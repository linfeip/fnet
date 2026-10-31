//go:build linux || darwin

package fnet

import (
	"runtime"
	"slices"
	"sync/atomic"

	"github.com/linfeip/fnet/poll"
)

// loop is a sub-reactor: a Poller watching the connections it owns, and the worker polling it (see worker), which hands
// the tasks of the connections it finds ready to the Executor.
type loop struct {
	srv         *Server
	poller      *poll.Poller // the readiness of the loop's connections, see poll.New
	listenerFds []int        // the listening sockets the Poller watches as well (see accept), set before Serve
	active      atomic.Bool  // the last poll retrieved events; other loops' workers help only while it is set (see help)
}

func newLoop(s *Server) (*loop, error) {
	p, err := poll.New()
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

// acceptAll accepts on every listening socket of the loop, see accept.
func (l *loop) acceptAll() {
	for _, fd := range l.listenerFds {
		l.accept(fd)
	}
}

// accept accepts the connections waiting on the loop's listening socket fd until there are none left; they belong to the
// loop, or are spread over the loops in round-robin order when the loops share the listener (see
// Server.listenerPerLoop). The first task of each watches it (see watch) and invokes OnOpen. The listener is
// edge-triggered and a worker finds it like any connection: while one accepts, the connections arriving meanwhile bring
// in other workers polling the loop, which accept at the same time, and no goroutine waits in the netpoller for the
// listener.
func (l *loop) accept(listenerFd int) {
	for c := l.srv.acceptConn(listenerFd, l); c != nil; c = l.srv.acceptConn(listenerFd, l) {
		c.notify(evOpen)
	}
}

// watch puts the new connection c in the table and its fd in the Poller, in this order so that the first event finds
// it; it runs in c's first task. When that fails c is closed, without any callback.
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
	if l.srv.draining.Load() {
		c.notify(evShutdown)
	}
	return true
}

// startWorker starts the loop's worker.
func (l *loop) startWorker() {
	l.srv.workersWg.Add(1)
	w := &worker{home: l}
	go w.run()
}

// worker is the goroutine serving a loop: it polls the loop's Poller and hands the tasks of the connections it reports
// ready to the Executor, accepting itself on a ready listening socket. When the loop has nothing to do it helps the
// other loops in the same way, and only when none of them has anything either does it wait in its own loop's Poller,
// blocking its thread in the kernel: under load the worker fetches the events itself, without waiting for the
// scheduler to run some goroutine for it, and an idle one costs nothing. It never runs user code itself.
type worker struct {
	home  *loop
	batch poll.Batch
	next  int // where helping the other loops starts next time, so that it spreads over them
}

// run is the worker's loop until the server stops.
//
// After every round that had something to do the worker gives up its P. A goroutine that is woken waits on the P of
// the goroutine that woke it, so the Executor's goroutines woken for the tasks just handed over wait on this one, and a
// busy worker never waits: they would run only when another P steals them or the scheduler preempts the worker, after
// 10ms. Yielding runs them at once, and the events that arrive meanwhile are retrieved together by the next poll: with
// a yield only once a millisecond, 4096 echoing connections on 24 CPUs ran about 3% slower.
func (w *worker) run() {
	l := w.home
	defer l.srv.workersWg.Done()
	for !l.srv.stopping.Load() {
		if w.serve(l) || w.help() {
			runtime.Gosched()
			continue
		}
		// Serve wakes every loop after setting stopping: a wakeup that comes after the check below ends Block.
		if l.srv.stopping.Load() {
			break
		}
		l.poller.Block(&w.batch)
		w.runBatch(l)
	}
	// The wakeups for stopping may have been taken by workers of other loops: pass them on.
	for _, other := range l.srv.loops {
		other.poller.Wake()
	}
}

// serve polls l's Poller once and hands over the tasks of the connections it reports ready; it reports whether l had
// anything to do. A wakeup (sent only when the server stops) counts as something to do, so that run looks at stopping.
func (w *worker) serve(l *loop) bool {
	woken := l.poller.Poll(&w.batch)
	if w.batch.Len() == 0 {
		l.active.Store(false)
		return woken
	}
	w.runBatch(l)
	return true
}

// help serves the other loops that had events on their last poll, until one has something to do; it reports whether one
// had. A loop whose last poll found nothing is skipped, so idle workers do not contend on its Poller.
func (w *worker) help() bool {
	loops := w.home.srv.loops
	w.next++
	for i := range loops {
		l := loops[(w.next+i)%len(loops)]
		if l == w.home || !l.active.Load() {
			continue
		}
		if w.serve(l) {
			return true
		}
	}
	return false
}

// runBatch hands over the tasks of the connections in the batch just retrieved from l's Poller, then accepts if the
// batch reported a listening socket: accepting until none is left takes a while during a burst of connections, and the
// connections already reported ready need not wait for it.
func (w *worker) runBatch(l *loop) {
	if w.batch.Len() == 0 {
		return
	}
	l.active.Store(true)
	accepting := false
	for i := range w.batch.Len() {
		fd, ev := w.batch.Event(i)
		accepting = l.handleEvent(fd, ev) || accepting
	}
	if accepting {
		l.acceptAll()
	}
}

// handleEvent notifies the connection with fd, reported ready by l's Poller, which submits its task to the Executor; it
// reports whether fd is one of l's listening sockets instead, which runBatch accepts on.
//
// An event left over for a connection that has already closed finds no connection. The fd may even have been reused
// by now, by a connection of this loop (which then only gets a spurious event, and reading it finds nothing) or of
// another one (which must not be notified through this loop).
func (l *loop) handleEvent(fd int, ev poll.Event) (listener bool) {
	c := connsByFd.lookup(fd)
	if c == nil {
		return slices.Contains(l.listenerFds, fd)
	}
	if c.loop == l {
		c.notify(uint32(ev))
	}
	return false
}
