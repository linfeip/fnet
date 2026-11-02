//go:build !linux && !darwin

package fnet

import (
	"errors"
	"io"
	"log/slog"
	"net"
	"os"
	"runtime/debug"
	"sync"
	"sync/atomic"
	"time"
)

// Server is a simple implementation on top of the standard library net package (for platforms such as Windows):
// one goroutine per connection, callbacks run serially in that goroutine, and Conn.Write is a blocking write.
type Server struct {
	handler   Handler
	listeners []net.Listener
	nextFd    atomic.Int32

	mu      sync.Mutex
	conns   map[*stdConn]struct{}
	serving bool
	closed  bool
	err     error          // the fatal accept error that made the server exit
	wg      sync.WaitGroup // connection goroutines
}

// NewServer creates a listener on addr; connections start being handled once Serve is called. To listen on several
// addresses with the one server, see NewServerAddrs. opts is ignored on this platform.
func NewServer(addr string, handler Handler, opts Options) (*Server, error) {
	return NewServerAddrs([]string{addr}, handler, opts)
}

// NewServerAddrs creates a listener on each of addrs; connections start being handled once Serve is called. At least
// one address is required, and the listeners share a fate: a fatal accept error on any one of them shuts the whole
// server down. opts is ignored on this platform.
func NewServerAddrs(addrs []string, handler Handler, _ Options) (*Server, error) {
	if len(addrs) == 0 {
		return nil, errors.New("fnet: NewServerAddrs needs at least one address")
	}
	s := &Server{handler: handler, conns: make(map[*stdConn]struct{})}
	for _, addr := range addrs {
		ln, err := net.Listen("tcp", addr)
		if err != nil {
			for _, open := range s.listeners {
				open.Close()
			}
			return nil, err
		}
		s.listeners = append(s.listeners, ln)
	}
	return s, nil
}

// Addr returns the first address actually being listened on; see Addrs for all of them.
func (s *Server) Addr() net.Addr { return s.listeners[0].Addr() }

// Addrs returns all the addresses actually being listened on, in the order they were given to NewServerAddrs.
func (s *Server) Addrs() []net.Addr {
	addrs := make([]net.Addr, 0, len(s.listeners))
	for _, ln := range s.listeners {
		addrs = append(addrs, ln.Addr())
	}
	return addrs
}

// Serve accepts on every listener and starts one goroutine per connection, blocking until Close is called (it then
// returns ErrServerClosed).
func (s *Server) Serve() error {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return ErrServerClosed
	}
	if s.serving {
		s.mu.Unlock()
		return errors.New("fnet: Serve called more than once")
	}
	s.serving = true
	s.mu.Unlock()

	var accepting sync.WaitGroup
	for _, ln := range s.listeners {
		accepting.Add(1)
		go func() {
			defer accepting.Done()
			if err := s.accept(ln); err != nil {
				s.shutdown(err) // stop the other listeners and all connections too
			}
		}()
	}
	accepting.Wait()
	s.wg.Wait() // the connection goroutines the accept loops started

	s.mu.Lock()
	defer s.mu.Unlock()
	if s.err != nil {
		return s.err
	}
	return ErrServerClosed
}

// accept accepts on one listener until the server is closed (returning nil) or a fatal error occurs.
func (s *Server) accept(ln net.Listener) error {
	var tempDelay time.Duration // backoff delay after accept hits a temporary error
	for {
		nc, err := ln.Accept()
		if err != nil {
			s.mu.Lock()
			closed := s.closed
			s.mu.Unlock()
			if closed {
				return nil
			}
			// Same as net/http: a temporary error (such as running out of file descriptors) is logged and retried after
			// a backoff that starts at 5ms and doubles each time up to 1s; it must not stop the server.
			if t, ok := err.(interface{ Temporary() bool }); ok && t.Temporary() {
				tempDelay = min(max(tempDelay*2, 5*time.Millisecond), time.Second)
				slog.Error("fnet: accept error", "err", err, "retryIn", tempDelay)
				time.Sleep(tempDelay)
				continue
			}
			return err
		}
		tempDelay = 0
		c := &stdConn{netConnection: nc, fd: int(s.nextFd.Add(1))}
		c.readResumed.L = &c.mu
		s.mu.Lock()
		if s.closed {
			s.mu.Unlock()
			nc.Close()
			return nil
		}
		s.conns[c] = struct{}{}
		s.wg.Add(1)
		s.mu.Unlock()
		go s.serveConn(c)
	}
}

// Close stops the server and closes all connections; by the time it returns every callback has finished.
// It must therefore not be called from a callback: it waits for the callbacks of all connections to finish, including the
// one calling it, which would deadlock; call it from a new goroutine when needed.
func (s *Server) Close() error {
	s.shutdown(nil)
	s.wg.Wait()
	return nil
}

// shutdown closes the listeners and all connections; err is the fatal error that caused the shutdown.
func (s *Server) shutdown(err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return
	}
	s.closed, s.err = true, err
	for _, ln := range s.listeners {
		ln.Close()
	}
	for c := range s.conns {
		c.closeWith(closedByServer)
	}
}

// serveConn is the goroutine of connection c: the read loop, then the close.
func (s *Server) serveConn(c *stdConn) {
	defer s.wg.Done()
	in, err := s.readLoop(c)
	// Unless something closed the connection first, the engine does. Detach may have woken the loop up, or come after it
	// ended: the connection is its caller's then.
	if !c.closedBy.CompareAndSwap(0, closedByEngine) {
		switch c.closedBy.Load() {
		case closedByDetach:
			s.handOver(c, in)
			return
		case closedByUser:
			err = nil
		case closedByServer:
			err = ErrServerClosed
		}
	}
	func() {
		defer recoverPanic(nil)
		s.handler.OnClose(c, err)
	}()
	c.netConnection.Close()
	s.mu.Lock()
	delete(s.conns, c)
	s.mu.Unlock()
}

// readBufferSize is the buffer size of a single Read in readLoop.
const readBufferSize = 4 << 10

// readLoop calls OnOpen, then reads and passes what it reads on to OnData until the connection is closed or detached,
// and calls OnClosing when it closes on its own. It returns the error that ended it, ErrHandlerPanic when a callback
// panicked (see recoverPanic), and the data OnData left unconsumed.
func (s *Server) readLoop(c *stdConn) (in []byte, err error) {
	defer recoverPanic(&err)
	s.handler.OnOpen(c)
	buf := make([]byte, readBufferSize)
	for err == nil {
		if s.waitRead(c) || c.closedBy.Load() != 0 {
			return in, nil
		}
		var n int
		n, err = c.netConnection.Read(buf)
		if n == 0 {
			continue
		}
		data := buf[:n]
		if len(in) > 0 {
			in = append(in, data...)
			data = in
		}
		// PauseRead may have been called while the Read was blocked: the data just read waits here until reading
		// resumes (it is not delivered meanwhile, as on Linux/macOS), or is handed over if the connection is detached.
		if s.waitRead(c) {
			return data, nil
		}
		if c.closedBy.Load() != 0 { // closed or detached meanwhile: OnData is not called again
			in = append(in[:0], data...)
			continue
		}
		consumed := min(max(s.handler.OnData(c, data), 0), len(data))
		in = append(in[:0], data[consumed:]...)
	}
	if errors.Is(err, os.ErrDeadlineExceeded) { // the SetDeadline deadline expired, unless a close or Detach woke the Read up
		err = os.ErrDeadlineExceeded
	}
	if h, ok := s.handler.(ClosingHandler); ok && c.closedBy.Load() == 0 && (err == io.EOF || err == os.ErrDeadlineExceeded) {
		c.netConnection.SetWriteDeadline(time.Now().Add(closingWriteTimeout))
		h.OnClosing(c, err)
		c.netConnection.SetWriteDeadline(time.Time{}) // it bounds OnClosing alone
	}
	return in, err
}

// recoverPanic, deferred, recovers the panic of a callback and logs it, as the executor does on Linux/macOS, and makes
// *err ErrHandlerPanic unless err is nil.
func recoverPanic(err *error) {
	if p := recover(); p != nil {
		slog.Error("fnet: panic in callback", "panic", p, "stack", string(debug.Stack()))
		if err != nil {
			*err = ErrHandlerPanic
		}
	}
}

// closingWriteTimeout bounds the writes of ClosingHandler.OnClosing: Write blocks on this platform, and a peer that does
// not read must not keep a closing connection open.
const closingWriteTimeout = time.Second

// waitRead waits while reading is paused (see waitResumed) and calls ResumeHandler.OnResume after ResumeRead, which may
// pause reading again; it reports whether the connection has been detached.
func (s *Server) waitRead(c *stdConn) bool {
	for {
		detached, resumed := c.waitResumed()
		if !resumed {
			return detached
		}
		if h, ok := s.handler.(ResumeHandler); ok {
			h.OnResume(c)
		}
	}
}

// handOver is the end of the read loop of a detached connection: it hands the unconsumed data over to Detach and leaves
// the connection open.
func (s *Server) handOver(c *stdConn, in []byte) {
	s.mu.Lock()
	delete(s.conns, c)
	s.mu.Unlock()
	c.mu.Lock() // Detach takes closedBy and sets detached under it: the read loop may end on its own meanwhile
	c.in = in
	close(c.detached)
	c.mu.Unlock()
}
