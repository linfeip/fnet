//go:build linux || darwin

package fnet

import (
	"os"
	"sync"
	"time"

	"github.com/linfeip/fnet/poll"

	"golang.org/x/sys/unix"
)

// loop is a sub-reactor: one goroutine + one Poller, it waits only for the events of the connections it owns
// and notifies the connection's task (see conn.notify) to handle them.
type loop struct {
	srv     *Server
	poller  *poll.Poller
	onEvent func(fd int, ev poll.Event)

	// The connections registered with this loop, accessed only by the event loop. Looking a ready fd up goes through
	// connsByFd instead, so a connection's task can take itself out when it closes without any lock; the entries it
	// leaves behind here are dropped the next time the list is walked (see forEachConn).
	conns []*conn

	mu    sync.Mutex // guards tasks and dead
	tasks []func()
	dead  bool // the Poller is closed, no more tasks are accepted

	spare    []func() // used alternately with tasks to reduce allocations, accessed only by the event loop
	stopping bool     // accessed only by the event loop
}

func newLoop(s *Server) (*loop, error) {
	p, err := poll.New()
	if err != nil {
		return nil, err
	}
	l := &loop{
		srv:    s,
		poller: p,
	}
	l.onEvent = l.handleEvent // bind in advance to avoid allocating a closure on every Wait round
	return l, nil
}

// trigger submits a task for the event loop to run and may be called from any goroutine; tasks submitted
// after the loop has been closed are dropped.
func (l *loop) trigger(task func()) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.dead {
		return
	}
	l.tasks = append(l.tasks, task)
	// Wake up while holding the lock: this guarantees that once the Poller is closed, its released (and
	// possibly reused) fd is never written again.
	l.poller.Wake()
}

// run runs the event loop until it receives the stop task or the Poller fails; before exiting it requests the
// close of all connections, which is then carried out by each connection's task.
func (l *loop) run() error {
	var err error
	for !l.stopping {
		var woken bool
		if woken, err = l.poller.Wait(l.onEvent); err != nil {
			break
		}
		if woken {
			l.runTasks()
		}
	}
	l.forEachConn(func(c *conn) { c.requestClose(ErrServerClosed) })
	return err
}

func (l *loop) stop() { l.stopping = true }

// close releases the Poller; it must be called after run has returned.
func (l *loop) close() {
	l.mu.Lock()
	l.dead = true
	l.tasks = nil
	l.mu.Unlock()
	l.poller.Close()
}

func (l *loop) runTasks() {
	l.mu.Lock()
	tasks := l.tasks
	l.tasks = l.spare
	l.mu.Unlock()
	for i, task := range tasks {
		task()
		tasks[i] = nil
	}
	l.spare = tasks[:0]
}

// register takes over a new connection distributed by the main reactor; OnOpen is invoked by the connection's
// first task.
func (l *loop) register(c *conn) {
	if err := l.poller.AddEdge(c.fd); err != nil {
		unix.Close(c.fd)
		return
	}
	if !connsByFd.store(c) { // an fd beyond the table's range; closing it also drops its epoll registration
		unix.Close(c.fd)
		return
	}
	l.conns = append(l.conns, c)
	l.srv.openConnsWg.Add(1) // decremented again in close after the OnClose callback has finished
	c.notify(evOpen)
}

// handleEvent notifies the task of the connection that owns fd to handle the events; for how events are turned
// into state bits see evRead and evWrite.
//
// An event left over for a connection that has already closed in its task finds no connection. The fd may even have been
// reused by now, by a connection of this loop (which then only gets a spurious event, and reading it finds nothing) or
// of another one (which must not be notified through this loop).
func (l *loop) handleEvent(fd int, ev poll.Event) {
	if c := connsByFd.lookup(fd); c != nil && c.loop == l {
		c.notify(uint32(ev))
	}
}

// forEachConn calls fn for every connection still registered with the loop. The ones that have taken themselves out of
// connsByFd (see conn.close) are dropped from the list on the way, which is how the list catches up with them; it must
// only be called by the event loop.
// fn may call the user-supplied Executor (through requestClose); no lock is held while it runs.
func (l *loop) forEachConn(fn func(c *conn)) {
	live := l.conns[:0]
	for _, c := range l.conns {
		if connsByFd.lookup(c.fd) == c {
			live = append(live, c)
			fn(c)
		}
	}
	clear(l.conns[len(live):]) // let the connections that were dropped be collected
	l.conns = live
}

// checkDeadlines requests the close of connections whose deadline has passed; the Server submits it once every
// deadlineCheckInterval, to be run by the event loop.
func (l *loop) checkDeadlines() {
	now := time.Now().UnixNano()
	l.forEachConn(func(c *conn) {
		if d := c.deadline.Load(); d != 0 && d <= now {
			c.requestClose(os.ErrDeadlineExceeded)
		}
	})
}
