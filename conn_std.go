//go:build !linux && !darwin

package fnet

import (
	"net"
	"slices"
	"sync"
	"sync/atomic"
	"time"
)

const (
	closedByUser int32 = iota + 1
	closedByServer
	closedByDetach // taken out of the engine by Detach: the net.Conn now belongs to its caller and is left open
	closedByEngine // the read loop ended on its own: EOF, a read error, the deadline or a panic
)

type stdConn struct {
	netConnection net.Conn
	fd            int
	ctx           any
	closedBy      atomic.Int32 // who initiated the close, which decides the err reported to OnClose

	mu          sync.Mutex
	readPaused  bool
	resumed     bool          // ResumeRead was called since the read loop last looked, see waitResumed
	readResumed sync.Cond     // broadcast when reading resumes, the connection closes or is detached, with L being &mu
	detached    chan struct{} // set by Detach, closed by the read loop once it has stopped for good
	in          []byte        // the data OnData left unconsumed, handed over by the read loop when it stops
}

func (c *stdConn) LocalAddr() net.Addr  { return c.netConnection.LocalAddr() }
func (c *stdConn) RemoteAddr() net.Addr { return c.netConnection.RemoteAddr() }
func (c *stdConn) Fd() int              { return c.fd }
func (c *stdConn) Context() any         { return c.ctx }
func (c *stdConn) SetContext(ctx any)   { c.ctx = ctx }

func (c *stdConn) Write(b []byte) (int, error) {
	if c.closedBy.Load() != 0 {
		return 0, net.ErrClosed
	}
	n, err := c.netConnection.Write(b)
	if err != nil && c.closedBy.Load() != 0 {
		err = net.ErrClosed
	}
	return n, err
}

// Writev sends through net.Buffers, which also writes everything out at once with writev (WSASend on Windows) when the
// underlying connection supports it.
func (c *stdConn) Writev(bs [][]byte) (int, error) {
	if c.closedBy.Load() != 0 {
		return 0, net.ErrClosed
	}
	// net.Buffers.WriteTo modifies the elements of the slice, so copy it to avoid changing the caller's bs.
	buffers := net.Buffers(slices.Clone(bs))
	n, err := buffers.WriteTo(c.netConnection)
	if err != nil && c.closedBy.Load() != 0 {
		err = net.ErrClosed
	}
	return int(n), err
}

// Writable always reports true: Write is synchronous on this platform, so nothing waits in a send buffer.
func (c *stdConn) Writable() bool { return true }

// Close closes the connection. Write is synchronous on this platform, so by the time Close is called all data already
// written has been handed to the kernel.
func (c *stdConn) Close() error {
	c.closeWith(closedByUser)
	return nil
}

// SetDeadline sets the socket's read and write deadline: once it expires a blocked Read or Write returns a timeout error,
// and the read loop then closes the connection. It checks closedBy and sets the deadline while holding mu, against which
// closeWith and Detach take closedBy, so that it never puts off the wakeup they give the read loop.
func (c *stdConn) SetDeadline(t time.Time) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closedBy.Load() == 0 {
		c.netConnection.SetDeadline(t)
	}
}

// PauseRead makes the read loop wait before its next Read, until reading resumes or the connection closes.
func (c *stdConn) PauseRead() {
	c.mu.Lock()
	c.readPaused = true
	c.mu.Unlock()
}

func (c *stdConn) ResumeRead() {
	c.mu.Lock()
	c.readPaused, c.resumed = false, true
	c.readResumed.Broadcast()
	c.mu.Unlock()
}

// waitResumed blocks while reading is paused, until reading resumes, the connection closes or is detached; it reports
// whether the connection has been detached, and whether reading has been resumed (by ResumeRead) since the last call.
func (c *stdConn) waitResumed() (detached, resumed bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for c.readPaused && c.closedBy.Load() == 0 && c.detached == nil {
		c.readResumed.Wait()
	}
	resumed, c.resumed = c.resumed && c.closedBy.Load() == 0, false
	return c.detached != nil, resumed
}

// Detach stops the read loop, which hands over the data OnData left unconsumed. Taking closedBy keeps every later close
// (Close, the server shutting down) away from the net.Conn. Write is synchronous on this platform, so there is no send
// buffer to drain.
func (c *stdConn) Detach() (net.Conn, error) {
	c.mu.Lock()
	if !c.closedBy.CompareAndSwap(0, closedByDetach) {
		c.mu.Unlock()
		return nil, net.ErrClosed
	}
	done := make(chan struct{})
	c.detached = done
	c.readResumed.Broadcast()
	c.mu.Unlock()
	c.netConnection.SetReadDeadline(time.Unix(1, 0)) // wakes a Read in progress, in case reading was not paused
	<-done
	c.netConnection.SetDeadline(time.Time{})
	return &detachedConn{TCPConn: c.netConnection.(*net.TCPConn), in: c.in}, nil
}

// closeWith records who closes the connection, which decides the err reported to OnClose, and wakes the read loop up. A
// local close leaves the socket open, to be closed after OnClose; the server closes it at once, an OnClosing under way
// included.
func (c *stdConn) closeWith(by int32) {
	// Taken while holding mu, so that neither the check in waitResumed nor SetDeadline interleaves with it (see SetDeadline).
	c.mu.Lock()
	first := c.closedBy.CompareAndSwap(0, by)
	if first {
		c.readResumed.Broadcast()
	}
	c.mu.Unlock()
	if by == closedByServer && c.closedBy.Load() != closedByDetach {
		c.netConnection.Close()
	} else if first {
		c.netConnection.SetReadDeadline(time.Unix(1, 0)) // wakes a Read in progress
	}
}
