//go:build linux || darwin

package fnet

import (
	"log/slog"
	"os"
	"runtime"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"github.com/linfeip/fnet/internal/units"
	"github.com/linfeip/fnet/poll"
)

// loop is a sub-reactor: a Poller watching the connections it owns, and the workers polling it (see worker). It has no
// goroutine of its own.
type loop struct {
	srv         *Server
	poller      *poll.Poller // the readiness of the loop's connections, see poll.NewBlocking
	listenerFds []int        // the listening sockets the Poller watches as well (see accept), set before Serve

	mu    sync.Mutex
	conns []*conn // the connections registered with this loop, see collectConns

	queue   connQueue    // connections whose task was handed over other than by their readiness, see submit
	blocked atomic.Int32 // the workers waiting in poller.Block; a connection queued meanwhile wakes one of them

	workersMu   sync.Mutex
	workers     []*worker // the loop's workers, read by the monitor; the extra ones remove themselves when they leave
	baseWorkers int       // the number of workers started with the loop
}

func newLoop(s *Server) (*loop, error) {
	p, err := poll.NewBlocking()
	if err != nil {
		return nil, err
	}
	return &loop{srv: s, poller: p}, nil
}

// close releases the Poller; it must be called after the workers have exited.
func (l *loop) close() { l.poller.Close() }

// watchListener has the loop's Poller watch the listening socket fd as well.
func (l *loop) watchListener(fd int) error {
	if err := l.poller.AddListener(fd); err != nil {
		return err
	}
	l.listenerFds = append(l.listenerFds, fd)
	return nil
}

// accept accepts the connections waiting on the listening socket fd until there are none left, handing them to the
// loops in round-robin order, whatever listener they arrived on, so that they spread evenly; the first task of each
// watches it (see watch), sets its socket options and invokes OnOpen. The listener is edge-triggered and a worker finds
// it like any connection: while one accepts, the connections arriving meanwhile bring in other workers, which accept
// at the same time, and no goroutine waits in the netpoller for the listener.
func (l *loop) accept(listenerFd int) {
	for c := l.srv.acceptConn(listenerFd); c != nil; c = l.srv.acceptConn(listenerFd) {
		c.schedule()
	}
}

// watch puts the new connection c in the table and its fd in the Poller, in this order so that the first event finds
// it, and adds it to the loop's list; it runs in c's first task. When that fails c is closed, without any callback.
// A connection added once the server has begun closing its connections (see Server.Serve) requests its own close.
func (l *loop) watch(c *conn) bool {
	if !connsByFd.store(c) { // an fd beyond the table's range
		c.abandon()
		return false
	}
	if err := l.poller.AddEdge(c.fd); err != nil {
		connsByFd.remove(c)
		c.abandon()
		return false
	}
	l.mu.Lock()
	l.conns = append(l.conns, c)
	draining := l.srv.draining.Load()
	l.mu.Unlock()
	if draining {
		c.requestClose(ErrServerClosed)
	}
	return true
}

// collectConns returns the connections still registered with the loop for which keep reports true; keep runs under
// the lock, so it must be quick and must not call into user code. The ones that have taken themselves out of
// connsByFd (see conn.close) are dropped from the list on the way, which is how the list catches up with them.
func (l *loop) collectConns(keep func(c *conn) bool) []*conn {
	var kept []*conn
	l.mu.Lock()
	defer l.mu.Unlock()
	live := l.conns[:0]
	for _, c := range l.conns {
		if connsByFd.lookup(c.fd) == c {
			live = append(live, c)
			if keep(c) {
				kept = append(kept, c)
			}
		}
	}
	clear(l.conns[len(live):]) // let the connections that were dropped be collected
	l.conns = live
	return kept
}

// checkDeadlines requests the close of connections whose deadline has passed; the Server calls it once every
// deadlineCheckInterval.
func (l *loop) checkDeadlines() {
	now := time.Now().UnixNano()
	expired := l.collectConns(func(c *conn) bool {
		d := c.deadline.Load()
		return d != 0 && d <= now
	})
	for _, c := range expired {
		c.requestClose(os.ErrDeadlineExceeded)
	}
}

// submit queues c for the loop's workers, for a task handed over other than by readiness: the first one of a
// connection, one requested from another goroutine (Write, Close, a deadline), or another round of a task that saw new
// events arrive while it ran. A worker waiting in the Poller is woken to take it.
func (l *loop) submit(c *conn) {
	l.queue.push(c)
	if l.blocked.Load() > 0 {
		l.poller.Wake()
	}
}

// take takes a queued connection. When more are left and workers are waiting, one of them is woken for those: the
// wakeup that brought this worker here was for all of them.
func (l *loop) take() *conn {
	c, more := l.queue.pop()
	if more && l.blocked.Load() > 0 {
		l.poller.Wake()
	}
	return c
}

// startWorkers starts the loop's workers: with the default executor they run the connections' tasks themselves, and
// there are enough of them for the loops to have one worker per P between them; with an Executor they only hand the
// tasks over, so one is enough.
func (l *loop) startWorkers() {
	l.workersMu.Lock()
	defer l.workersMu.Unlock()
	l.baseWorkers = 1
	if l.srv.opts.Executor == nil {
		l.baseWorkers = (runtime.GOMAXPROCS(0) + len(l.srv.loops) - 1) / len(l.srv.loops)
	}
	for range l.baseWorkers {
		l.startWorker(false)
	}
}

// startWorker starts a worker; the caller holds workersMu.
func (l *loop) startWorker(extra bool) {
	w := &worker{home: l, extra: extra}
	l.workers = append(l.workers, w)
	l.srv.workersWg.Add(1)
	go w.run()
}

// monitorTick is the interval at which the monitor looks for loops whose workers are all stuck.
const monitorTick = 10 * time.Millisecond

// relieve starts an extra worker when every worker of the loop has been in the same task since the monitor's previous
// tick, so that callbacks blocking for a long time do not hold up the loop's other connections. An extra worker leaves
// as soon as the loop has nothing for it; a loop has at most GOMAXPROCS of them.
func (l *loop) relieve() {
	l.workersMu.Lock()
	defer l.workersMu.Unlock()
	stuck := true
	for _, w := range l.workers {
		progress := w.progress.Load()
		if progress&1 == 0 || progress != w.lastProgress {
			stuck = false
		}
		w.lastProgress = progress
	}
	if stuck && len(l.workers) < l.baseWorkers+runtime.GOMAXPROCS(0) {
		l.startWorker(true)
	}
}

// leave takes the extra worker w off the loop.
func (l *loop) leave(w *worker) {
	l.workersMu.Lock()
	defer l.workersMu.Unlock()
	for i, other := range l.workers {
		if other == w {
			l.workers = slices.Delete(l.workers, i, i+1)
			return
		}
	}
}

// worker is a goroutine serving a loop: it takes the connections queued on the loop and polls the loop's Poller, and
// runs the tasks of those connections itself (with an Executor it hands them over instead). When the loop has nothing
// to do it helps the other loops in the same way, and only when none of them has anything either does it wait in its
// own loop's Poller, blocking its thread in the kernel: under load the workers fetch the events themselves, without
// waiting for the scheduler to run some goroutine for them, and an idle one costs nothing.
type worker struct {
	home      *loop
	extra     bool // started by the monitor (see loop.relieve): it does not wait, it leaves
	batch     poll.Batch
	next      int       // where helping the other loops starts next time, so that it spreads over them
	lastYield time.Time // when the worker last gave up its P, see yield

	lastProgress uint64 // progress at the monitor's previous tick, accessed only by the monitor
	_            [cacheLineSize]byte
	// progress is the number of tasks started, shifted left by one, with the low bit set while one runs; written only
	// by the worker, twice per task, so it keeps off the cache lines of other workers.
	progress atomic.Uint64
	_        [cacheLineSize - 8]byte
}

// cacheLineSize covers both the common 64B and 128B cache lines.
const cacheLineSize = 128

// run is the worker's loop until the server stops.
func (w *worker) run() {
	l := w.home
	exited := false
	defer func() {
		if !exited { // a task called runtime.Goexit: a replacement takes over this worker
			go w.run()
			return
		}
		l.srv.workersWg.Done()
	}()
	for !l.srv.stopping.Load() {
		if w.serve(l) {
			w.yield()
			continue
		}
		if w.extra {
			l.leave(w)
			exited = true
			return
		}
		if w.help() {
			w.yield()
			continue
		}
		// A connection queued after the check below finds this worker counted in blocked and wakes it, while one queued
		// before is seen by the check.
		l.blocked.Add(1)
		if l.queue.length.Load() > 0 || l.srv.stopping.Load() {
			l.blocked.Add(-1)
			continue
		}
		l.poller.Block(&w.batch)
		l.blocked.Add(-1)
		w.runBatch(l) // a wakeup is for the queue, which serve looks at next
	}
	// The wakeups for stopping may have been taken by workers of other loops: pass them on.
	for _, other := range l.srv.loops {
		if other.blocked.Load() > 0 {
			other.poller.Wake()
		}
	}
	exited = true
}

// yieldInterval is how long a busy worker keeps its P at most, see yield.
const yieldInterval = time.Millisecond

// yield gives up the P once every yieldInterval of work. A busy worker never waits, so the goroutines waiting for a P
// (fhttp's handlers, goroutines started by a callback, the runtime's own) would otherwise get its P only when the
// scheduler preempts it, after 10ms; a new goroutine waits on the P of the worker that started it.
func (w *worker) yield() {
	if time.Since(w.lastYield) >= yieldInterval {
		runtime.Gosched()
		w.lastYield = time.Now()
	}
}

// serve runs the tasks of the connections queued on l and polls l's Poller once, running the tasks of the connections
// it reports ready; it reports whether l had anything to do. A round takes as many queued connections as a poll
// retrieves at most, so that neither starves the other, and consuming the Poller's wakeup obliges it to look at the
// queue again: the wakeup was for a connection queued there (and take passes it on when more are queued).
func (w *worker) serve(l *loop) bool {
	busy := w.runQueued(l)
	woken := l.poller.Poll(&w.batch)
	if w.batch.Len() > 0 {
		w.runBatch(l)
		busy = true
	}
	if woken {
		w.runQueued(l)
	}
	return busy || woken
}

// runQueued runs the tasks of up to poll.BatchSize connections queued on l; it reports whether there were any.
func (w *worker) runQueued(l *loop) bool {
	ran := false
	for range poll.BatchSize {
		if l.queue.length.Load() == 0 {
			break
		}
		c := l.take()
		if c == nil {
			break
		}
		w.runConn(c)
		ran = true
	}
	return ran
}

// help serves each of the other loops once, until one has something to do; it reports whether one had. A loop with a
// worker waiting in its Poller is skipped: it has a worker to spare, which the kernel wakes when an event comes, so
// polling it would only cost an idle worker a syscall.
func (w *worker) help() bool {
	loops := w.home.srv.loops
	w.next++
	for i := range loops {
		if l := loops[(w.next+i)%len(loops)]; l != w.home && l.blocked.Load() == 0 && w.serve(l) {
			return true
		}
	}
	return false
}

// runBatch runs the tasks of the connections in the batch just retrieved from l's Poller.
//
// An event left over for a connection that has already closed finds no connection. The fd may even have been reused
// by now, by a connection of this loop (which then only gets a spurious event, and reading it finds nothing) or of
// another one (which must not be notified through this loop).
func (w *worker) runBatch(l *loop) {
	for i := range w.batch.Len() {
		fd, ev := w.batch.Event(i)
		w.runEvent(l, fd, ev)
	}
}

// runEvent runs the task of the connection with fd, reported ready by l's Poller, or accepts on the listening socket fd.
func (w *worker) runEvent(l *loop, fd int, ev poll.Event) {
	c := connsByFd.lookup(fd)
	if c == nil && slices.Contains(l.listenerFds, fd) {
		l.accept(fd)
		return
	}
	if c == nil || c.loop != l || !c.markEvents(uint32(ev)) {
		return
	}
	if executor := l.srv.opts.Executor; executor != nil {
		executor(c.task)
	} else {
		w.runConn(c)
	}
}

// runConn runs one round of c's task, queueing c again when events arrived meanwhile; a panic is logged, after the
// task has closed the connection.
func (w *worker) runConn(c *conn) {
	started := w.progress.Load() + 3 // one more task, running
	w.progress.Store(started)
	defer w.finish(started)
	if c.runRound() {
		c.loop.submit(c)
	}
}

func (w *worker) finish(started uint64) {
	if err := recover(); err != nil {
		buf := make([]byte, 64*units.KB)
		slog.Error("fnet: panic in a connection's task", "panic", err, "stack", string(buf[:runtime.Stack(buf, false)]))
	}
	w.progress.Store(started &^ 1)
}

// connQueue is a FIFO of connections; a connection is in it at most once, since only the holder of its task queues it.
type connQueue struct {
	mu     sync.Mutex
	conns  []*conn
	head   int
	length atomic.Int32 // len(conns)-head, read without the lock
}

func (q *connQueue) push(c *conn) {
	q.mu.Lock()
	q.conns = append(q.conns, c)
	q.length.Add(1)
	q.mu.Unlock()
}

// pop takes the connection at the head, nil when empty, and reports whether more are left.
func (q *connQueue) pop() (c *conn, more bool) {
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.head == len(q.conns) {
		return nil, false
	}
	c = q.conns[q.head]
	q.conns[q.head] = nil
	q.head++
	if q.head == len(q.conns) { // empty: start over at the front, reusing the array
		q.conns, q.head = q.conns[:0], 0
	}
	return c, q.length.Add(-1) > 0
}
