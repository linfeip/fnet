//go:build !linux && !darwin

package fnet

import (
	"errors"
	"io"
	"log/slog"
	"net"
	"os"
	"runtime/debug"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"github.com/linfeip/fnet/internal/units"
)

// Server is a simple implementation on top of the standard library net package (for platforms such as Windows):
// one goroutine per connection, callbacks run serially in that goroutine, and Conn.Write is a blocking write.
type Server struct {
	handler   Handler
	listeners []net.Listener

	mu     sync.Mutex
	conns  map[*stdConn]struct{}
	closed bool
	err    error          // the fatal accept error that made the server exit
	wg     sync.WaitGroup // connection goroutines
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
		c := &stdConn{netConnection: nc}
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
	if err == nil {
		c.linger()
	}
	c.netConnection.Close()
	s.mu.Lock()
	delete(s.conns, c)
	s.mu.Unlock()
}

// readLoop calls OnOpen, then reads and passes what it reads on to OnData until the connection is closed or detached,
// and calls OnClosing when it closes on its own. It returns the error that ended it, ErrHandlerPanic when a callback
// panicked (see recoverPanic), and the data OnData left unconsumed.
func (s *Server) readLoop(c *stdConn) (in []byte, err error) {
	defer recoverPanic(&err)
	s.handler.OnOpen(c)
	buf := make([]byte, 4*units.KB)
	for err == nil {
		if s.waitRead(c) {
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

// lingerTimeout is how long a connection closed locally keeps reading what the peer still sends, see linger.
const lingerTimeout = time.Second

// linger closes a connection closed locally in stages, as on Linux/macOS (see Conn.Close): closing a socket that is still
// receiving makes the kernel answer with an RST, which can make the peer drop what it has received but not read yet, such
// as the last response. So only the write direction is shut down, and what the peer still sends is read away until it
// closes too, lingerTimeout passes or the server closes the connection.
func (c *stdConn) linger() {
	c.netConnection.(*net.TCPConn).CloseWrite()
	c.netConnection.SetReadDeadline(time.Now().Add(lingerTimeout))
	io.Copy(io.Discard, c.netConnection)
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

const (
	closedByUser int32 = iota + 1
	closedByServer
	closedByDetach // taken out of the engine by Detach: the net.Conn now belongs to its caller and is left open
	closedByEngine // the read loop ended on its own: EOF, a read error, the deadline or a panic
)

type stdConn struct {
	netConnection net.Conn
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
// written has been handed to the kernel; the socket is closed in stages after OnClose, see linger.
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
// local close leaves the socket open, to be closed in stages after OnClose (see linger); the server closes it at once, a
// staged close or an OnClosing under way included.
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
