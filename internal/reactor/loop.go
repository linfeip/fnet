package reactor

import (
	"sync"
	"sync/atomic"
	"time"

	"github.com/linfeip/fnet/internal/netpoll"
)

const (
	readBufSize = 64 << 10 // per-loop read buffer shared by all its connections
	// maxReadsPerTurn caps back-to-back full reads from one connection before
	// the loop serves its other connections; the rest is read on a later turn.
	maxReadsPerTurn = 16
)

type taskKind uint8

const (
	taskResume taskKind = iota // offer buffered input, then read the socket
	taskClose                  // close the fd, then notify the handler
	taskNotify                 // notify a handler attached after the close
)

type task struct {
	kind taskKind
	c    *Conn
	h    Handler
	err  error
}

// Loop is one sub-reactor: a poller, a read buffer, and the goroutine that
// services them. It only moves bytes; protocols run in Handler callbacks.
type Loop struct {
	eng     *Engine
	poller  netpoll.Poller
	buf     []byte
	wheel   wheel   // close deadlines of this loop's connections
	expired []*Conn // scratch for expireDeadlines

	mu      sync.Mutex
	tasks   []task
	spare   []task
	stopped bool
	ntasks  atomic.Int32
	waiting atomic.Bool // blocked (or about to block) in poller.Wait
}

// post queues work for the loop goroutine. After the loop has stopped the work
// runs inline, so closes are never lost during shutdown.
func (l *Loop) post(t task) {
	if l == nil {
		return
	}
	l.mu.Lock()
	if l.stopped {
		l.mu.Unlock()
		l.run1(t)
		return
	}
	l.tasks = append(l.tasks, t)
	l.ntasks.Add(1)
	l.mu.Unlock()
	l.wake()
}

// wake interrupts the loop if it is blocked (or about to block) in Wait.
func (l *Loop) wake() {
	if l.waiting.Load() {
		_ = l.poller.Wake()
	}
}

func (l *Loop) run() {
	defer l.eng.wg.Done()
	l.buf = make([]byte, readBufSize)
	for !l.eng.closing.Load() {
		l.runTasks()
		l.waiting.Store(true)
		if l.ntasks.Load() > 0 { // work arrived after runTasks; don't block
			l.waiting.Store(false)
			continue
		}
		timeout := pollTimeout
		if d, ok := l.wheel.untilTick(time.Now().UnixNano()); ok {
			timeout = min(timeout, d)
		}
		events, err := l.poller.Wait(timeout)
		l.waiting.Store(false)
		if err != nil {
			continue
		}
		for _, ev := range events {
			c := l.eng.table.get(ev.Fd)
			if c == nil || c.loop != l {
				continue
			}
			if ev.Readable {
				l.onReadable(c, ev.Hup)
			}
			if ev.Writable {
				l.onWritable(c)
			}
		}
		l.expireDeadlines()
	}
	l.mu.Lock()
	l.stopped = true
	rest := l.tasks
	l.tasks = nil
	l.mu.Unlock()
	for _, t := range rest {
		l.run1(t)
	}
}

func (l *Loop) runTasks() {
	if l.ntasks.Load() == 0 {
		return
	}
	l.mu.Lock()
	tasks := l.tasks
	l.tasks, l.spare = l.spare[:0], tasks
	l.ntasks.Store(0)
	l.mu.Unlock()
	for i := range tasks {
		l.run1(tasks[i])
		tasks[i] = task{}
	}
}

func (l *Loop) run1(t task) {
	switch t.kind {
	case taskResume:
		if !l.eng.closing.Load() {
			l.resume(t.c)
		}
	case taskClose:
		_ = netpoll.Close(t.c.fd)
		if t.h != nil {
			t.h.OnClose(t.c, t.err)
		}
	case taskNotify:
		t.h.OnClose(t.c, t.err)
	}
}

func (l *Loop) expireDeadlines() {
	if l.wheel.n.Load() == 0 {
		return
	}
	l.expired = l.wheel.expire(time.Now().UnixNano(), l.expired[:0])
	for i, c := range l.expired {
		c.expire()
		l.expired[i] = nil
	}
}

// resume re-offers retained input (after Attach or ResumeRead) and then reads
// whatever arrived on the socket while nobody was reading it.
func (l *Loop) resume(c *Conn) {
	if c.state.Load()&stClosed != 0 {
		return
	}
	c.offer()
	if c.handlerSawEOF() {
		c.handlerEOF()
		return
	}
	l.onReadable(c, true) // edges that came while it was paused are gone
}

// onReadable reads c until its socket is drained. Readiness is edge-triggered,
// so a short read means drained, except when the peer's EOF or an error came
// with the same edge (hup), or no edge can be trusted (after a pause): then
// read on until the socket reports it.
func (l *Loop) onReadable(c *Conn, hup bool) {
	for reads := 1; ; reads++ {
		st := c.state.Load()
		if st&(stClosed|stPaused) != 0 {
			return // a paused connection is read again on resume
		}
		n, err := netpoll.Read(c.fd, l.buf)
		if n > 0 && st&stDraining == 0 {
			c.deliver(l.buf[:n])
		}
		if err != nil {
			if !netpoll.IsAgain(err) {
				c.abort(err)
			}
			return
		}
		if n == 0 {
			if st&stDraining != 0 {
				c.abort(nil) // closing anyway, and the peer is gone: stop draining
			} else {
				c.onPeerEOF()
			}
			return
		}
		if n < len(l.buf) && !hup {
			// Edge-triggered: a short read drained the socket, and new data
			// raises a new event, so skip the extra EAGAIN round trip.
			return
		}
		if reads == maxReadsPerTurn {
			// A flooding peer must not starve the loop's other connections.
			// No new edge will come for data already queued, so re-queue.
			l.post(task{kind: taskResume, c: c})
			return
		}
	}
}

func (l *Loop) onWritable(c *Conn) {
	if c.state.Load()&stClosed != 0 {
		return
	}
	pending, err := c.flush()
	if err != nil {
		c.abort(err)
		return
	}
	if pending {
		if c.state.Load()&stDraining != 0 {
			l.wheel.set(c, c.drainDeadline()) // the peer is still reading: keep draining
		}
		return // interest stays armed
	}
	_ = l.poller.DisableWrite(c.fd)
	st, _ := c.clearState(stWriteArmed)
	if st&stDraining != 0 {
		c.abort(nil) // the local Close finished draining
		return
	}
	if c.hasPendingOutput() { // a writer queued after flush saw the interest still armed
		c.armWrite()
	}
}
