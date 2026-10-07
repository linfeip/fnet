//go:build linux || darwin

package fnet

import (
	"os"
	"runtime"
	"sync"
	"time"

	"github.com/linfeip/fnet/poll"

	"golang.org/x/sys/unix"
)

const maxReadyTasks = 256

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

	spare                 []func() // used alternately with tasks to reduce allocations, accessed only by the event loop
	stopping              bool     // accessed only by the event loop
	readyTasks            []func() // 默认执行器的有界提交批次，仅事件循环访问
	readyEventsSinceYield int      // 计入已排队连接的事件，防止持续就绪时长期占住 P
	submitted             bool     // the current round of events submitted a task (see run), accessed only by the event loop
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
	if s.submitBatch != nil {
		l.readyTasks = make([]func(), 0, maxReadyTasks)
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
//
// 默认执行器按有界就绪事件数让出 P，避免小批次每轮都进行调度交接；即使事件都已合并进
// 现有任务，也会达到让出阈值。自定义执行器保留逐轮让出行为，单个事件循环仍不主动让出。
func (l *loop) run() error {
	yield := len(l.srv.loops) > 1
	var err error
	for !l.stopping {
		var woken bool
		woken, err = l.poller.Wait(l.onEvent)
		l.submitReadyTasks()
		if err != nil {
			break
		}
		if woken {
			l.runTasks()
		}
		if l.takeYield() && yield {
			runtime.Gosched()
		}
	}
	l.forEachConn(func(c *conn) { c.requestClose(ErrServerClosed) })
	return err
}

// takeYield 消费本轮让出状态；单循环也清零计数，避免计数无限增长。
func (l *loop) takeYield() bool {
	yield := l.submitted
	l.submitted = false
	if l.srv.submitBatch != nil {
		yield = l.readyEventsSinceYield >= maxReadyTasks
		if yield {
			l.readyEventsSinceYield = 0
		}
	}
	return yield
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

// register takes over a new connection distributed by the main reactor; its socket options are set and OnOpen is
// invoked by the connection's first task.
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
	c := connsByFd.lookup(fd)
	if c == nil || c.loop != l {
		return
	}
	if l.srv.submitBatch == nil {
		if c.notify(uint32(ev)) {
			l.submitted = true
		}
		return
	}
	l.readyEventsSinceYield++
	if c.markEvents(uint32(ev)) {
		l.readyTasks = append(l.readyTasks, c.task)
		if len(l.readyTasks) == maxReadyTasks {
			l.submitReadyTasks()
		}
	}
}

func (l *loop) submitReadyTasks() {
	if len(l.readyTasks) == 0 {
		return
	}
	l.srv.submitBatch(l.readyTasks)
	clear(l.readyTasks)
	l.readyTasks = l.readyTasks[:0]
	l.submitted = true
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
