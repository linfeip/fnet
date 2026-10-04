//go:build linux || darwin

package fnet

import (
	"bytes"
	"net"
	"net/netip"
	"os"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"github.com/linfeip/fnet/internal/bytepool"

	"golang.org/x/sys/unix"
)

// conn is the Linux/macOS implementation of Conn.
//
// Concurrency model: reading, the callbacks and closing are all done serially in the connection's task (see
// notify and run), and the inbound buffer in is accessed only by the task; the send buffer and the close state
// are guarded by mu, because Write/Close may come from any goroutine. Closing the fd is done inside mu as
// well, which guarantees that whenever closed=false is seen while holding the lock the fd is valid, so no
// write goes to the wrong object after the fd has been reused by a new connection.
type conn struct {
	// Small fields are packed together, so that conn can stay in the 176B size class (see TestConnSize).
	state      atomic.Uint32 // pending events and scheduledBit, see notify
	readPaused atomic.Bool   // reading has been paused
	backlogged atomic.Bool   // out holds data; written while holding mu, read lock-free by the task (see handle)
	closing    bool          // a close has been requested, see requestClose; accessed while holding mu
	closeNow   bool          // close immediately, without draining the send buffer (error or deadline); accessed while holding mu
	closed     bool          // written only by the task while holding the lock, so the task itself can read it lock-free
	peerClosed bool          // the peer closed or an error occurred (see evHup), accessed only by the task
	loop       *loop
	fd         int
	task       func() // run bound once, so that handing the task to the Executor does not allocate a method value each time
	ctx        any
	in         bytepool.Buffer // unconsumed inbound data, accessed only by the task

	mu       sync.Mutex
	out      bytepool.Buffer // send buffer, the data to send is out.Bytes()[outPos:]; empty when nothing is backed up
	deadline atomic.Int64    // close deadline (UnixNano), 0 means no deadline; written by any goroutine, checked by the event loop

	outPos   int
	closeErr error          // close reason: nil means an explicit close, io.EOF means the peer closed its write direction; both drain the send buffer before closing
	remote   netip.AddrPort // kept by value, net.Addr is built on RemoteAddr: two fewer small objects resident per connection
}

func (c *conn) RemoteAddr() net.Addr { return addrPortToTCPAddr(c.remote) }
func (c *conn) Context() any         { return c.ctx }
func (c *conn) SetContext(ctx any)   { c.ctx = ctx }

func (c *conn) LocalAddr() net.Addr {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return nil
	}
	sa, err := unix.Getsockname(c.fd)
	if err != nil {
		return nil
	}
	return addrPortToTCPAddr(sockaddrToAddrPort(sa))
}

func (c *conn) Write(b []byte) (int, error) {
	buffered := false
	defer func() {
		if buffered {
			c.notify(evWrite) // notify after mu is released: the user-supplied Executor is not called under the lock
		}
	}()
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed || c.closing {
		return 0, net.ErrClosed
	}
	if len(b) == 0 {
		return 0, nil
	}
	n := 0
	if c.out.Len() == 0 {
		// No backed-up data: write to the socket directly, which in the vast majority of cases completes in
		// one go, without going through the connection's task.
		wn, err := sysWrite(c.fd, b)
		if n, err = afterDirectWrite(wn, err); err != nil || n == len(b) {
			return n, err
		}
	}
	c.compactOut(len(b) - n)
	c.out.Append(b[n:])
	c.backlogged.Store(true)
	buffered = true
	return len(b), nil
}

func (c *conn) Writev(bs [][]byte) (int, error) {
	buffered := false
	defer func() {
		if buffered {
			c.notify(evWrite) // notify after mu is released: the user-supplied Executor is not called under the lock
		}
	}()
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed || c.closing {
		return 0, net.ErrClosed
	}
	size := 0
	for _, b := range bs {
		size += len(b)
	}
	if size == 0 {
		return 0, nil
	}
	n := 0
	if c.out.Len() == 0 {
		// Segments beyond maxIovecs go into the send buffer, just like data that was not fully written.
		wn, err := sysWritev(c.fd, bs[:min(len(bs), maxIovecs)])
		if n, err = afterDirectWrite(wn, err); err != nil || n == size {
			return n, err
		}
	}
	// Skip the n bytes already written out; the remaining segments go into the send buffer in order.
	c.compactOut(size - n)
	for _, b := range bs {
		skip := min(n, len(b))
		c.out.Append(b[skip:])
		n -= skip
	}
	c.backlogged.Store(true)
	buffered = true
	return size, nil
}

// afterDirectWrite handles the result of writing directly to the socket when nothing is backed up and returns
// the number of bytes actually written out.
// When it was not fully written, the caller puts the remaining data into the send buffer for the connection's
// task to keep sending (see flush).
func afterDirectWrite(n int, err error) (int, error) {
	if err != nil {
		if err != unix.EAGAIN && err != unix.EINTR {
			// The connection has already failed: the task notices the error through a read event and closes
			// the connection.
			return 0, err
		}
		n = 0
	}
	return n, nil
}

// compactOut first reclaims the space of the already-sent part when the send buffer's capacity is not enough to
// append size bytes; the caller must hold mu.
func (c *conn) compactOut(size int) {
	if c.outPos > 0 && cap(c.out.Bytes())-c.out.Len() < size {
		c.out.Discard(c.outPos)
		c.outPos = 0
	}
}

// maxIovecs is the maximum number of data segments a single writev submits, i.e. IOV_MAX (1024 on both Linux
// and macOS); beyond that the kernel returns EINVAL.
const maxIovecs = 1024

func (c *conn) Close() error {
	c.requestClose(nil)
	return nil
}

func (c *conn) PauseRead() { c.readPaused.Store(true) }

// ResumeRead resumes reading. Edge-triggered notification does not notify again for data that arrived while
// paused, so the task is explicitly notified to read once.
func (c *conn) ResumeRead() {
	c.readPaused.Store(false)
	c.notify(evRead)
}

// Detach first takes over the connection's task: once state goes from 0 (no task queued or running) to scheduledBit,
// notify never submits the task again, so this goroutine owns everything the task owns, the inbound buffer included.
func (c *conn) Detach() (net.Conn, error) {
	for !c.state.CompareAndSwap(0, scheduledBit) {
		c.mu.Lock()
		closed := c.closed // a closed connection keeps scheduledBit set for good (see run)
		c.mu.Unlock()
		if closed {
			return nil, net.ErrClosed
		}
		runtime.Gosched() // a task is queued or running; callbacks return quickly
	}
	c.mu.Lock()
	if c.closing {
		c.mu.Unlock()
		if !c.state.CompareAndSwap(scheduledBit, 0) { // give the task back to carry out the close
			c.loop.srv.opts.Executor(c.task)
		}
		return nil, net.ErrClosed
	}
	c.closed = true // writes now fail; close does nothing, so OnClose is never called
	out := bytes.Clone(c.out.Bytes()[c.outPos:])
	c.out.Release()
	c.outPos = 0
	c.backlogged.Store(false)
	c.mu.Unlock()
	in := bytes.Clone(c.in.Bytes())
	c.in.Release()

	// Deregister explicitly: net.FileConn duplicates the fd, and an epoll registration lasts as long as any descriptor
	// of the socket stays open. The table entry goes before the fd is closed, which frees the fd number for reuse.
	c.loop.poller.Delete(c.fd)
	connsByFd.remove(c)
	c.loop.srv.openConnsWg.Done()
	f := os.NewFile(uintptr(c.fd), "")
	nc, err := net.FileConn(f)
	f.Close()
	if err != nil {
		return nil, err
	}
	if len(out) > 0 {
		if _, err := nc.Write(out); err != nil {
			nc.Close()
			return nil, err
		}
	}
	return &detachedConn{TCPConn: nc.(*net.TCPConn), in: in}, nil
}

// SetDeadline only records the deadline, which the event loop checks once per second (see
// loop.checkDeadlines).
func (c *conn) SetDeadline(t time.Time) {
	var d int64
	if !t.IsZero() {
		d = t.UnixNano()
	}
	c.deadline.Store(d)
}

// sockaddrToAddrPort converts a TCP socket address; it returns the zero value for unsupported address types.
func sockaddrToAddrPort(sa unix.Sockaddr) netip.AddrPort {
	switch sa := sa.(type) {
	case *unix.SockaddrInet4:
		return netip.AddrPortFrom(netip.AddrFrom4(sa.Addr), uint16(sa.Port))
	case *unix.SockaddrInet6:
		ip := netip.AddrFrom16(sa.Addr)
		if sa.ZoneId != 0 {
			if ifi, err := net.InterfaceByIndex(int(sa.ZoneId)); err == nil {
				ip = ip.WithZone(ifi.Name)
			}
		}
		return netip.AddrPortFrom(ip, uint16(sa.Port))
	}
	return netip.AddrPort{}
}

// addrPortToTCPAddr converts the result of sockaddrToAddrPort into a net.Addr; the zero value returns nil.
func addrPortToTCPAddr(ap netip.AddrPort) net.Addr {
	if !ap.IsValid() {
		return nil
	}
	return net.TCPAddrFromAddrPort(ap)
}
