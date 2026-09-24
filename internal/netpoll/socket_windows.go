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
// emulates one on top of the net package. A read pump goroutine per socket
// buffers what it reads (or accepts) and marks the socket ready on the poller
// it is registered with. Writes land in an emulated kernel send buffer that a
// write pump drains; a full buffer reports EAGAIN, exactly like a non-blocking
// socket. The reactor above runs unchanged; Windows is a development target,
// not a production one (it costs a goroutine per idle socket).

const (
	pumpChunk   = 32 << 10  // bytes read per pump iteration
	pumpBacklog = 256 << 10 // the read pump pauses until the reactor drains below this
	sendBuffer  = 256 << 10 // emulated kernel send buffer
	writeStall  = 30 * time.Second
)

type sock struct {
	id   int
	ln   net.Listener
	conn net.Conn

	mu       sync.Mutex
	cond     sync.Cond // the read pump waits here while rbuf is full
	poller   *winPoller
	closed   bool
	eof      bool // the read pump hit EOF or an error
	writeOn  bool // write readiness requested
	acceptQ  []net.Conn
	rbuf     []byte
	wbuf     []byte // accepted by Write, not yet handed to the write pump
	inflight int    // bytes the write pump is sending
	writing  bool   // a write pump is running
	werr     error  // the write pump failed

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

func (s *sock) writableLocked() bool {
	return s.conn != nil && (s.werr != nil || len(s.wbuf)+s.inflight < sendBuffer)
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

// Write copies b into the emulated send buffer. It returns a short count once
// the buffer fills and EAGAIN while it is full.
func Write(fd int, b []byte) (int, error) {
	s := lookup(fd)
	if s == nil || s.conn == nil {
		return 0, net.ErrClosed
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.werr != nil {
		return 0, s.werr
	}
	n := min(len(b), sendBuffer-len(s.wbuf)-s.inflight)
	if n <= 0 {
		return 0, syscall.EAGAIN
	}
	s.wbuf = append(s.wbuf, b[:n]...)
	if !s.writing {
		s.writing = true
		go s.writePump()
	}
	return n, nil
}

// Writev writes iovs in order, stopping at the first short write.
func Writev(fd int, iovs [][]byte) (int, error) {
	total := 0
	for _, b := range iovs {
		if len(b) == 0 {
			continue
		}
		n, err := Write(fd, b)
		total += n
		if err != nil || n < len(b) {
			if total > 0 && IsAgain(err) {
				err = nil
			}
			return total, err
		}
	}
	return total, nil
}

// writePump sends the send buffer, then closes the connection if Close was
// called meanwhile (so a response written just before Close still arrives).
func (s *sock) writePump() {
	for {
		s.mu.Lock()
		if len(s.wbuf) == 0 || s.werr != nil {
			s.writing = false
			closed := s.closed
			s.mu.Unlock()
			if closed {
				_ = s.conn.Close()
			}
			return
		}
		chunk := s.wbuf
		s.wbuf, s.inflight = nil, len(chunk)
		s.mu.Unlock()

		_ = s.conn.SetWriteDeadline(time.Now().Add(writeStall))
		_, err := s.conn.Write(chunk)

		s.mu.Lock()
		s.inflight = 0
		if err != nil {
			s.werr = err
		}
		s.mu.Unlock()
		s.notify()
	}
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
	flushing := s.writing // the write pump closes the conn once it is done
	s.cond.Broadcast()
	s.mu.Unlock()
	for _, c := range pending {
		_ = c.Close()
	}
	if s.ln != nil {
		return s.ln.Close()
	}
	if flushing {
		return nil
	}
	return s.conn.Close()
}
