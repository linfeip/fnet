//go:build linux || darwin

package fnet

import (
	"maps"
	"os"
	"slices"
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

	connsMu sync.Mutex    // guards conns: the event loop looks up by fd, a connection is removed when its own task closes it
	conns   map[int]*conn // fd -> connection

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
		conns:  make(map[int]*conn),
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
	// Only copy the connection list while holding the lock: requestClose calls the user-supplied Executor,
	// which must not run while connsMu is held, otherwise once the Executor blocks, the event loop and the
	// connection tasks waiting for this lock (see remove) would wait for each other.
	l.connsMu.Lock()
	conns := slices.Collect(maps.Values(l.conns))
	l.connsMu.Unlock()
	for _, c := range conns {
		c.requestClose(ErrServerClosed)
	}
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
	l.connsMu.Lock()
	l.conns[c.fd] = c
	l.connsMu.Unlock()
	l.srv.openConnsWg.Add(1) // decremented again in close after the OnClose callback has finished
	c.notify(evOpen)
}

// remove takes the connection out of the event loop; it is called before the connection closes its fd, because
// a closed fd may immediately be reused by a new connection.
func (l *loop) remove(c *conn) {
	l.connsMu.Lock()
	delete(l.conns, c.fd)
	l.connsMu.Unlock()
}

// handleEvent notifies the task of the connection that owns fd to handle the events; for how events are turned
// into state bits see evRead and evWrite.
func (l *loop) handleEvent(fd int, ev poll.Event) {
	l.connsMu.Lock()
	c := l.conns[fd]
	l.connsMu.Unlock()
	if c != nil { // a leftover event for a connection already closed in its task has no matching connection
		c.notify(uint32(ev))
	}
}

// checkDeadlines requests the close of connections whose deadline has passed; the Server submits it once every
// deadlineCheckInterval.
func (l *loop) checkDeadlines() {
	now := time.Now().UnixNano()
	var expired []*conn
	l.connsMu.Lock()
	for _, c := range l.conns {
		if d := c.deadline.Load(); d != 0 && d <= now {
			expired = append(expired, c)
		}
	}
	l.connsMu.Unlock()
	for _, c := range expired { // request the close outside the lock, for the same reason as in run
		c.requestClose(os.ErrDeadlineExceeded)
	}
}
