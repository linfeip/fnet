//go:build linux || darwin

package fnet

import (
	"errors"
	"net"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"golang.org/x/sys/unix"
)

// listener is one listening socket: the fd connections are accepted on, and the address it is bound to.
type listener struct {
	fd   int
	addr net.Addr
}

// Server is a TCP server based on the main/sub-reactor model.
type Server struct {
	handler   Handler
	opts      Options
	listeners []listener    // the listening sockets, watched by the first loop's Poller (see loop.accept)
	loops     []*loop       // sub-reactors
	nextLoop  atomic.Uint32 // round-robin over the loops for the accepted connections
	// acceptMu is held for reading while a connection is accepted and counted in openConnsWg, and taken once for
	// writing by Serve after setting draining, so that no connection is counted once it waits for them.
	acceptMu sync.RWMutex

	openConnsWg sync.WaitGroup // connections not yet closed: closed means OnClose returned; Serve waits for them before exiting
	workersWg   sync.WaitGroup // the loops' workers; Serve stops them once every connection has closed
	draining    atomic.Bool    // Serve is closing every connection: one being opened closes itself (see loop.watch)
	stopping    atomic.Bool    // the workers are to exit

	mu      sync.Mutex
	serving bool
	closed  bool
	err     error         // the fatal error that made the server exit
	closing chan struct{} // closed when the server is closed, after which Serve closes the connections and returns
	done    chan struct{} // closed after Serve has fully exited
}

// NewServer creates a listener on addr and a Poller for each sub-reactor. It starts handling connections
// after Serve is called. To listen on several addresses with the one server, see NewServerAddrs.
func NewServer(addr string, handler Handler, opts Options) (*Server, error) {
	return NewServerAddrs([]string{addr}, handler, opts)
}

// NewServerAddrs creates a listener on each of addrs and a Poller for each sub-reactor. It starts handling
// connections after Serve is called. At least one address is required; all the listeners share the one main
// reactor and the one set of sub-reactors, so the number of Pollers and goroutines is independent of how many
// addresses are listened on.
//
// The listeners share a fate: a fatal accept error on any one of them shuts the whole server down, closing the
// other listeners and all connections.
func NewServerAddrs(addrs []string, handler Handler, opts Options) (*Server, error) {
	if len(addrs) == 0 {
		return nil, errors.New("fnet: NewServerAddrs needs at least one address")
	}
	s := &Server{
		handler: handler,
		opts:    opts.withDefaults(),
		closing: make(chan struct{}),
		done:    make(chan struct{}),
	}
	// The loops come first: the first one watches the listeners.
	for range s.opts.NumLoops {
		l, err := newLoop(s)
		if err != nil {
			s.release()
			return nil, err
		}
		s.loops = append(s.loops, l)
	}
	for _, addr := range addrs {
		fd, laddr, err := listen(addr)
		if err != nil {
			s.release()
			return nil, err
		}
		if err := s.loops[0].watchListener(fd); err != nil {
			unix.Close(fd) // not in s.listeners yet, so release does not cover it
			s.release()
			return nil, err
		}
		s.listeners = append(s.listeners, listener{fd: fd, addr: laddr})
	}
	return s, nil
}

// Addr returns the first address actually being listened on; see Addrs for all of them.
func (s *Server) Addr() net.Addr { return s.listeners[0].addr }

// Addrs returns all the addresses actually being listened on, in the order they were given to NewServerAddrs.
func (s *Server) Addrs() []net.Addr {
	addrs := make([]net.Addr, 0, len(s.listeners))
	for _, ln := range s.listeners {
		addrs = append(addrs, ln.addr)
	}
	return addrs
}

// Serve starts the sub-reactors' workers, which accept the connections and hand their tasks to the Executor, and blocks
// until Close is called (returning ErrServerClosed) or a fatal error occurs.
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

	for _, l := range s.loops {
		l.startWorker()
	}
	go s.tick()
	<-s.closing
	// No connection is accepted from here on, and one accepted before is either in a loop's list already or closes
	// itself (see loop.watch).
	s.draining.Store(true)
	s.acceptMu.Lock()
	s.acceptMu.Unlock() //nolint:staticcheck // an empty critical section: it waits for the accepts under way
	for _, l := range s.loops {
		for _, c := range l.collectConns(func(*conn) bool { return true }) {
			c.requestClose(ErrServerClosed)
		}
	}
	s.openConnsWg.Wait() // the closes are carried out by the connections' tasks, which the Executor runs
	s.stopping.Store(true)
	for _, l := range s.loops {
		l.poller.Wake() // a worker that wakes up passes it on, see worker.run
	}
	s.workersWg.Wait()
	s.release()
	close(s.done)

	s.mu.Lock()
	defer s.mu.Unlock()
	if s.err != nil {
		return s.err
	}
	return ErrServerClosed
}

// Close stops the server and closes all connections (calling OnClose for each one); all callbacks have
// finished by the time it returns. It therefore must not be called from a callback: it waits for the callbacks
// of all connections to finish, including the one calling it, which deadlocks; call it from a new goroutine
// when needed.
func (s *Server) Close() error {
	first, serving := s.shutdown(nil)
	if !serving {
		if first {
			s.release()
		}
		return nil
	}
	<-s.done
	return nil
}

// shutdown marks the server as closed and makes Serve close the connections; err is the fatal error that caused the
// shutdown.
func (s *Server) shutdown(err error) (first, serving bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return false, s.serving
	}
	s.closed, s.err = true, err
	close(s.closing)
	return true, s.serving
}

// deadlineCheckInterval is the interval at which the event loops check connection deadlines, i.e. the
// precision of Conn.SetDeadline.
const deadlineCheckInterval = time.Second

// tick periodically checks the connection deadlines of every sub-reactor; it stops after Serve exits.
func (s *Server) tick() {
	ticker := time.NewTicker(deadlineCheckInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			for _, l := range s.loops {
				l.checkDeadlines()
			}
		case <-s.done:
			return
		}
	}
}

func (s *Server) release() {
	for _, ln := range s.listeners {
		unix.Close(ln.fd)
	}
	for _, l := range s.loops {
		l.close()
	}
}

// acceptConn accepts a connection on listenerFd for a loop chosen in round-robin order, with its open task pending
// (see loop.watch), and counts it in openConnsWg; it returns nil when there is none left to accept or the server is
// draining.
func (s *Server) acceptConn(listenerFd int) *conn {
	s.acceptMu.RLock()
	defer s.acceptMu.RUnlock()
	for !s.draining.Load() {
		fd, remote, err := accept(listenerFd)
		if err == nil {
			l := s.loops[s.nextLoop.Add(1)%uint32(len(s.loops))]
			c := &conn{fd: fd, loop: l, remote: remote}
			c.task = c.run
			c.state.Store(scheduledBit | evOpen)
			s.openConnsWg.Add(1) // decremented again in close after the OnClose callback has finished
			return c
		}
		switch err {
		case unix.EAGAIN:
			return nil
		case unix.EINTR, unix.ECONNABORTED:
		case unix.EPROTO, unix.ENETDOWN, unix.ENOPROTOOPT, unix.EHOSTDOWN, unix.EHOSTUNREACH, unix.EOPNOTSUPP, unix.ENETUNREACH:
			// Linux's accept returns a network error that already happened on the new connection as its error code; it
			// affects only that one connection, and accept(2) requires retrying as with EAGAIN, so the server must not
			// stop because of it.
		case unix.EMFILE, unix.ENFILE, unix.ENOBUFS, unix.ENOMEM:
			// Resource exhaustion: back off briefly. The listener is edge-triggered, so the connections left waiting are
			// accepted when the next one arrives.
			time.Sleep(10 * time.Millisecond)
			return nil
		default:
			s.shutdown(os.NewSyscallError("accept", err))
			return nil
		}
	}
	return nil
}

// listen creates the listening socket with the help of the standard library (address resolution, dual stack,
// SO_REUSEADDR and backlog are all handled by the standard library), then duplicates a separate fd for the
// server to manage, after which the standard library's listener is closed right away.
func listen(addr string) (int, net.Addr, error) {
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return -1, nil, err
	}
	defer ln.Close()
	rc, err := ln.(*net.TCPListener).SyscallConn()
	if err != nil {
		return -1, nil, err
	}
	fd, dupErr := -1, error(nil)
	if err := rc.Control(func(s uintptr) { fd, dupErr = unix.FcntlInt(s, unix.F_DUPFD_CLOEXEC, 0) }); err != nil {
		return -1, nil, err
	}
	if dupErr != nil {
		return -1, nil, os.NewSyscallError("fcntl", dupErr)
	}
	if err := unix.SetNonblock(fd, true); err != nil {
		unix.Close(fd)
		return -1, nil, os.NewSyscallError("setnonblock", err)
	}
	return fd, ln.Addr(), nil
}
