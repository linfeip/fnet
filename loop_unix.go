//go:build linux || darwin

package fnet

import (
	"slices"

	"github.com/linfeip/fnet/poll"
)

// loop is a TCP sub-reactor: a Poller watching the connections it owns, paired with a reactorWorker (see reactor_unix.go)
// that hands ready connection tasks to the Executor and accepts on ready listening sockets.
type loop struct {
	srv         *Server
	poller      *poll.Poller // the readiness of the loop's connections, owned by the paired subReactor
	listenerFds []int        // the listening sockets the Poller watches as well (see accept), set before Serve
}

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

// runBatch hands over the tasks of the connections in the batch just retrieved from l's Poller, then accepts if the
// batch reported a listening socket: accepting until none is left takes a while during a burst of connections, and the
// connections already reported ready need not wait for it.
func (l *loop) runBatch(batch *poll.Batch) {
	accepting := false
	for i := range batch.Len() {
		fd, ev := batch.Event(i)
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
//
// A writable event alone matters only to a backlog, which Write records before it notifies the connection itself: kqueue
// reports one whenever the peer's ACKs free send space, and epoll one when the connection is watched.
func (l *loop) handleEvent(fd int, ev poll.Event) (listener bool) {
	c := connsByFd.lookup(fd)
	if c == nil {
		return slices.Contains(l.listenerFds, fd)
	}
	if c.loop == l && (ev != poll.EventWrite || c.backlogged.Load()) {
		c.notify(uint32(ev))
	}
	return false
}
