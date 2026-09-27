//go:build windows

package netpoll

import (
	"net"
	"net/netip"
	"sync"
	"syscall"
	"time"
)

// Windows has no readiness API the reactor can own sockets with, so this file
// emulates one with the net package. Reading needs the emulation: a read pump
// goroutine per socket buffers what it reads (or accepts) and marks the socket
// ready on the poller it is registered with. Writing does not: Write is a
// plain blocking net.Conn.Write, so a socket is always writable and never
// reports EAGAIN, and a peer that does not read holds the writer instead of
// filling the connection's queue. The reactor above runs unchanged; Windows is
// a development target, not a production one (it costs a goroutine per idle
// socket, and has no write backpressure).

const (
	pumpChunk   = 32 << 10  // bytes read per pump iteration
	pumpBacklog = 256 << 10 // the read pump pauses until the reactor drains below this
	// A write that makes no progress for writeStall fails, taking the
	// connection down: the reactor holds the connection's write lock while it
	// writes, so an unbounded write would hold Close too. writeChunk is the
	// progress it measures by.
	writeStall = 10 * time.Second
	writeChunk = 64 << 10
)

type sock struct {
	id   int
	ln   net.Listener
	conn net.Conn

	mu      sync.Mutex
	cond    sync.Cond // the read pump waits here while rbuf is full
	poller  *winPoller
	closed  bool
	eof     bool // the read pump hit EOF or an error
	writeOn bool // write readiness requested
	acceptQ []net.Conn
	rbuf    []byte

	queued bool // guarded by poller.mu: already on poller.ready
}

var socks struct {
	sync.RWMutex
	m    map[int]*sock
	free []int
	next int
}

func register(s *sock) int {
	s.cond.L = &s.mu
	socks.Lock()
	defer socks.Unlock()
	if socks.m == nil {
		socks.m = make(map[int]*sock)
	}
	if n := len(socks.free); n > 0 { // reuse ids like the OS reuses fds
		s.id = socks.free[n-1]
		socks.free = socks.free[:n-1]
	} else {
		socks.next++
		s.id = socks.next
	}
	socks.m[s.id] = s
	return s.id
}

func lookup(fd int) *sock {
	socks.RLock()
	s := socks.m[fd]
	socks.RUnlock()
	return s
}

func (s *sock) readableLocked() bool {
	return len(s.rbuf) > 0 || s.eof || len(s.acceptQ) > 0
}

func (s *sock) notify() {
	s.mu.Lock()
	p := s.poller
	s.mu.Unlock()
	if p != nil {
		p.push(s)
	}
}

func (s *sock) readPump() {
	buf := make([]byte, pumpChunk)
	for {
		n, err := s.conn.Read(buf)
		s.mu.Lock()
		if s.closed {
			s.mu.Unlock()
			return
		}
		s.rbuf = append(s.rbuf, buf[:n]...)
		if err != nil {
			s.eof = true
		}
		s.mu.Unlock()
		s.notify()
		if err != nil {
			return
		}
		s.mu.Lock()
		for len(s.rbuf) >= pumpBacklog && !s.closed {
			s.cond.Wait()
		}
		s.mu.Unlock()
	}
}

func (s *sock) acceptPump() {
	for {
		c, err := s.ln.Accept()
		s.mu.Lock()
		if s.closed || err != nil {
			s.mu.Unlock()
			if c != nil {
				_ = c.Close()
			}
			return
		}
		s.acceptQ = append(s.acceptQ, c)
		s.mu.Unlock()
		s.notify()
	}
}

// Listen opens a TCP listener and returns its emulated fd.
func Listen(network, address string) (int, net.Addr, error) {
	ln, err := net.Listen(network, address)
	if err != nil {
		return -1, nil, err
	}
	return FromListener(ln)
}

// FromListener takes over ln; it is closed when the returned fd is closed.
func FromListener(ln net.Listener) (int, net.Addr, error) {
	s := &sock{ln: ln}
	fd := register(s)
	go s.acceptPump()
	return fd, ln.Addr(), nil
}

// Accept pops one accepted connection, or returns EAGAIN.
func Accept(lnFD int) (int, netip.AddrPort, error) {
	ls := lookup(lnFD)
	if ls == nil {
		return -1, netip.AddrPort{}, net.ErrClosed
	}
	ls.mu.Lock()
	if len(ls.acceptQ) == 0 {
		ls.mu.Unlock()
		return -1, netip.AddrPort{}, syscall.EAGAIN
	}
	c := ls.acceptQ[0]
	ls.acceptQ[0] = nil
	ls.acceptQ = ls.acceptQ[1:]
	ls.mu.Unlock()

	s := &sock{conn: c}
	fd := register(s)
	go s.readPump()
	var ap netip.AddrPort
	if ta, ok := c.RemoteAddr().(*net.TCPAddr); ok {
		ap = ta.AddrPort()
		ap = netip.AddrPortFrom(ap.Addr().Unmap(), ap.Port())
	}
	return fd, ap, nil
}

// Read drains bytes buffered by the pump. (0, nil) means EOF.
func Read(fd int, b []byte) (int, error) {
	s := lookup(fd)
	if s == nil {
		return 0, net.ErrClosed
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.rbuf) == 0 {
		if s.eof {
			return 0, nil
		}
		return 0, syscall.EAGAIN
	}
	n := copy(b, s.rbuf)
	s.rbuf = s.rbuf[n:]
	if len(s.rbuf) == 0 {
		s.rbuf = nil
	}
	s.cond.Signal()
	return n, nil
}

// Write sends b, blocking until the kernel has taken all of it. It fails once
// the peer takes nothing for writeStall.
func Write(fd int, b []byte) (int, error) {
	s := lookup(fd)
	if s == nil || s.conn == nil {
		return 0, net.ErrClosed
	}
	n := 0
	for n < len(b) {
		_ = s.conn.SetWriteDeadline(time.Now().Add(writeStall))
		m, err := s.conn.Write(b[n:min(len(b), n+writeChunk)])
		n += m
		if err != nil {
			return n, err
		}
	}
	return n, nil
}

// Writev writes iovs in order, stopping at the first error.
func Writev(fd int, iovs [][]byte) (int, error) {
	total := 0
	for _, b := range iovs {
		n, err := Write(fd, b)
		total += n
		if err != nil {
			return total, err
		}
	}
	return total, nil
}

// KeepAliveInherited reports whether accepted sockets inherit their listener's
// keep-alive settings. The emulation sets them on every accepted connection.
const KeepAliveInherited = false

// SetKeepAlive applies ka to an accepted connection. The count of unanswered
// probes is left to Windows.
func SetKeepAlive(fd int, ka KeepAlive) error {
	s := lookup(fd)
	if s == nil {
		return net.ErrClosed
	}
	tc, ok := s.conn.(*net.TCPConn)
	if !ok {
		return nil // a listener: its connections are configured as they are accepted
	}
	if ka.Idle < 0 {
		return tc.SetKeepAlive(false)
	}
	if err := tc.SetKeepAlive(true); err != nil {
		return err
	}
	if ka.Idle > 0 {
		return tc.SetKeepAlivePeriod(ka.Idle)
	}
	return nil
}

// CloseWrite half-closes the connection: every write has already reached the
// kernel.
func CloseWrite(fd int) error {
	s := lookup(fd)
	if s == nil || s.conn == nil {
		return net.ErrClosed
	}
	if tc, ok := s.conn.(*net.TCPConn); ok {
		return tc.CloseWrite()
	}
	return nil
}

// LocalAddr returns the local address of the connection fd.
func LocalAddr(fd int) (netip.AddrPort, error) {
	s := lookup(fd)
	if s == nil || s.conn == nil {
		return netip.AddrPort{}, net.ErrClosed
	}
	ta, ok := s.conn.LocalAddr().(*net.TCPAddr)
	if !ok {
		return netip.AddrPort{}, nil
	}
	ap := ta.AddrPort()
	return netip.AddrPortFrom(ap.Addr().Unmap(), ap.Port()), nil
}

// Close closes the socket and releases its id.
func Close(fd int) error {
	socks.Lock()
	s := socks.m[fd]
	if s != nil {
		delete(socks.m, fd)
		socks.free = append(socks.free, fd)
	}
	socks.Unlock()
	if s == nil {
		return nil
	}
	s.mu.Lock()
	s.closed = true
	pending := s.acceptQ
	s.acceptQ = nil
	s.cond.Broadcast()
	s.mu.Unlock()
	for _, c := range pending {
		_ = c.Close()
	}
	if s.ln != nil {
		return s.ln.Close()
	}
	return s.conn.Close()
}
