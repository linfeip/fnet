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
// are guarded by mu, because Write/Close may come from any goroutine. closed is set inside mu before the fd is
// closed, which guarantees that whenever closed=false is seen while holding the lock the fd is valid, so no
// write goes to the wrong object after the fd has been reused by a new connection.
type conn struct {
	state      atomic.Uint32 // pending events and scheduledBit, see notify
	readPaused atomic.Bool   // reading has been paused
	backlogged atomic.Bool   // out holds data; written while holding mu, read lock-free by the task (see handle)
	closing    bool          // a close has been requested, see requestClose; accessed while holding mu
	closed     bool          // written only by the task while holding the lock, so the task itself can read it lock-free
	peerClosed bool          // the peer closed or an error occurred (see evHup), accessed only by the task
	readClosed bool          // EOF has been read and a HalfCloseHandler kept the connection open, so it is not read again; accessed only by the task
	loop       *loop
	fd         int
	task       func() // run bound once, so that handing the task over does not allocate a method value each time
	ctx        any
	in         bytepool.Buffer // unconsumed inbound data, accessed only by the task

	mu        sync.Mutex
	out       bytepool.Buffer // send buffer, the data to send is out.Bytes()[outPos:]; empty when nothing is backed up
	deadline  atomic.Int64    // close deadline (UnixNano), 0 means no deadline; written by any goroutine, checked by the event loop
	lingerEnd atomic.Int64    // when the linger of a connection closed locally ends (UnixNano), 0 while it does not linger; see beginLinger

	outPos     int
	closeErr   error          // close reason: nil means an explicit close, io.EOF means the peer closed its write direction; both drain the send buffer before closing, any other reason closes at once (see closesNow)
	remoteAddr netip.AddrPort // kept by value, net.Addr is built on RemoteAddr: two fewer small objects resident per connection
}

func (c *conn) RemoteAddr() net.Addr { return addrPortToTCPAddr(c.remoteAddr) }
func (c *conn) Fd() int              { return c.fd }
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
	c.mu.Lock()
	if c.closed || c.closing {
		c.mu.Unlock()
		return 0, net.ErrClosed
	}
	if len(b) == 0 {
		c.mu.Unlock()
		return 0, nil
	}
	n := 0
	if c.out.Len() == 0 {
		// No backed-up data: write to the socket directly, which in the vast majority of cases completes in
		// one go, without going through the connection's task.
		wn, err := sysWrite(c.fd, b)
		if n, err = afterDirectWrite(wn, err); err != nil || n == len(b) {
			c.mu.Unlock()
			return n, err
		}
	}
	c.compactOut(len(b) - n)
	c.out.Write(b[n:])
	c.backlogged.Store(true)
	c.mu.Unlock()
	c.notify(evWrite) // notify after mu is released: the user-supplied Executor is not called under the lock
	return len(b), nil
}

func (c *conn) Writev(bs [][]byte) (int, error) {
	c.mu.Lock()
	if c.closed || c.closing {
		c.mu.Unlock()
		return 0, net.ErrClosed
	}
	size := 0
	for _, b := range bs {
		size += len(b)
	}
	if size == 0 {
		c.mu.Unlock()
		return 0, nil
	}
	n := 0
	if c.out.Len() == 0 {
		// Segments beyond maxIovecs go into the send buffer, just like data that was not fully written.
		wn, err := sysWritev(c.fd, bs[:min(len(bs), maxIovecs)])
		if n, err = afterDirectWrite(wn, err); err != nil || n == size {
			c.mu.Unlock()
			return n, err
		}
	}
	// Skip the n bytes already written out; the remaining segments go into the send buffer in order.
	c.compactOut(size - n)
	for _, b := range bs {
		skip := min(n, len(b))
		c.out.Write(b[skip:])
		n -= skip
	}
	c.backlogged.Store(true)
	c.mu.Unlock()
	c.notify(evWrite) // notify after mu is released: the user-supplied Executor is not called under the lock
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

// Writable reports whether the send buffer holds no more than maxBacklog, see flush.
func (c *conn) Writable() bool { return !c.backlogged.Load() || c.backlog() <= maxBacklog }

// maxIovecs is the maximum number of data segments a single writev submits, i.e. IOV_MAX (1024 on both Linux
// and macOS); beyond that the kernel returns EINVAL.
const maxIovecs = 1024

func (c *conn) Close() error {
	c.requestClose(nil)
	return nil
}

func (c *conn) PauseRead() { c.readPaused.Store(true) }

// ResumeRead resumes reading. Edge-triggered notification does not notify again for data that arrived while
// paused, so the task is explicitly notified to read once, and to call ResumeHandler.OnResume.
func (c *conn) ResumeRead() {
	c.readPaused.Store(false)
	c.notify(evRead | evResume)
}

// Detach first takes over the connection's task (see takeOver), and with it everything the task owns, the inbound
// buffer included.
func (c *conn) Detach() (net.Conn, error) {
	for !c.takeOver() {
		c.mu.Lock()
		// A closed connection keeps its task for good (see run), unless it lingers. One that is closing may not become
		// idle for minutes: its peer takes none of the send buffer, and the events it keeps getting each run a task.
		closed := c.closed || c.closing
		c.mu.Unlock()
		if closed {
			return nil, net.ErrClosed
		}
		runtime.Gosched() // a task is queued or running; callbacks return quickly
	}
	c.mu.Lock()
	if c.closing {
		c.mu.Unlock()
		c.release() // the task carries out the close
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
		// This write blocks, and the peer may not be reading: the connection's deadline bounds it, see SetDeadline.
		if d := c.deadline.Load(); d > 0 {
			nc.SetWriteDeadline(time.Unix(0, d))
		}
		_, err := nc.Write(out)
		nc.SetWriteDeadline(time.Time{})
		if err != nil {
			nc.Close()
			return nil, err
		}
	}
	return &detachedConn{TCPConn: nc.(*net.TCPConn), in: in}, nil
}

// SetDeadline only records the deadline, which the server checks once per second (see Server.checkDeadlines). A
// lingering connection goes by the end of its linger instead, see expiredBy.
func (c *conn) SetDeadline(t time.Time) {
	if t.IsZero() {
		c.deadline.Store(0)
		return
	}
	d := max(int64(t.Sub(time.Unix(0, 0))), 1)
	c.deadline.Store(d)
	connsByFd.noteDeadline(c.fd, d)
}

// effectiveDeadline returns the connection's close deadline, or the end of the linger once the connection lingers (see
// beginLinger); 0 means no deadline.
func (c *conn) effectiveDeadline() int64 {
	if end := c.lingerEnd.Load(); end != 0 {
		return end
	}
	return c.deadline.Load()
}

// expiredBy reports whether the deadline, or the end of the linger once the connection lingers, has passed by now. The
// task checks it again on evExpired: Server.checkDeadlines found it so, but it may have been put off since, even by the
// callback of this very round.
func (c *conn) expiredBy(now int64) bool {
	d := c.effectiveDeadline()
	return d != 0 && d <= now
}

// sockaddrToAddrPort converts a TCP socket address; it returns the zero value for unsupported address types.
func sockaddrToAddrPort(sa unix.Sockaddr) netip.AddrPort {
	switch sa := sa.(type) {
	case *unix.SockaddrInet4:
		return netip.AddrPortFrom(netip.AddrFrom4(sa.Addr), uint16(sa.Port))
	case *unix.SockaddrInet6:
		return inet6AddrPort(sa.Addr, uint16(sa.Port), sa.ZoneId)
	}
	return netip.AddrPort{}
}

// inet6AddrPort builds an IPv6 address and port; a non-zero scope id becomes the zone, named after its interface.
func inet6AddrPort(addr [16]byte, port uint16, scopeID uint32) netip.AddrPort {
	ip := netip.AddrFrom16(addr)
	if scopeID != 0 {
		if ifi, err := net.InterfaceByIndex(int(scopeID)); err == nil {
			ip = ip.WithZone(ifi.Name)
		}
	}
	return netip.AddrPortFrom(ip, port)
}

// addrPortToTCPAddr converts the result of sockaddrToAddrPort into a net.Addr; the zero value returns nil.
func addrPortToTCPAddr(ap netip.AddrPort) net.Addr {
	if !ap.IsValid() {
		return nil
	}
	return net.TCPAddrFromAddrPort(ap)
}
