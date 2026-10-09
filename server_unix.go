//go:build linux || darwin

package fnet

import (
	"context"
	"errors"
	"net"
	"os"
	"slices"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"golang.org/x/sys/unix"
)

// Server is a TCP server based on the main/sub-reactor model.
type Server struct {
	handler     Handler
	opts        Options
	addrs       []net.Addr    // the addresses listened on, one per address given to NewServerAddrs
	listenerFds []int         // the listening sockets, one per address and loop or one per address (see listenerPerLoop)
	loops       []*loop       // sub-reactors
	nextLoop    atomic.Uint32 // round-robin over the loops for the connections of a listener they share
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
	// The loops come first: they watch the listeners.
	for range s.opts.NumLoops {
		l, err := newLoop(s)
		if err != nil {
			s.release()
			return nil, err
		}
		s.loops = append(s.loops, l)
	}
	sockets := 1
	if listenerPerLoop {
		sockets = len(s.loops)
	}
	for _, addr := range addrs {
		fds, laddr, err := listen(addr, sockets)
		if err != nil {
			s.release()
			return nil, err
		}
		s.addrs = append(s.addrs, laddr)
		s.listenerFds = append(s.listenerFds, fds...) // release closes them from here on
		for i, fd := range fds {
			if err := s.loops[i].watchListener(fd); err != nil {
				s.release()
				return nil, err
			}
		}
	}
	return s, nil
}

// Addr returns the first address actually being listened on; see Addrs for all of them.
func (s *Server) Addr() net.Addr { return s.addrs[0] }

// Addrs returns all the addresses actually being listened on, in the order they were given to NewServerAddrs.
func (s *Server) Addrs() []net.Addr { return slices.Clone(s.addrs) }

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
	for _, fd := range s.listenerFds {
		unix.Close(fd)
	}
	for _, l := range s.loops {
		l.close()
	}
}

// acceptConn accepts a connection on listenerFd, a listener of loop l, with its open task pending (see loop.watch), and
// counts it in openConnsWg; it returns nil when there is none left to accept or the server is draining. The connection
// belongs to l, or, when the loops share the listener (see listenerPerLoop), to a loop chosen in round-robin order.
func (s *Server) acceptConn(listenerFd int, l *loop) *conn {
	s.acceptMu.RLock()
	defer s.acceptMu.RUnlock()
	for !s.draining.Load() {
		fd, remote, err := accept(listenerFd)
		if err == nil {
			if !listenerPerLoop {
				l = s.loops[s.nextLoop.Add(1)%uint32(len(s.loops))]
			}
			c := &conn{fd: fd, loop: l, remote: remote}
			if s.opts.Executor != nil {
				c.task = c.run
			}
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

// listen creates n listening sockets on addr, sharing it through SO_REUSEPORT when n > 1, and returns the address
// actually listened on.
//
// The first socket is bound like net.Listen's, without the option, so that an address in use is reported as such rather
// than shared with another process listening on it with SO_REUSEPORT. It takes the option once it listens, which lets
// the others join it on the address it got (its port, for port 0).
func listen(addr string, n int) ([]int, net.Addr, error) {
	fd, laddr, err := listenSocket(addr, false)
	if err != nil {
		return nil, nil, err
	}
	fds := []int{fd}
	if n > 1 {
		err = os.NewSyscallError("setsockopt", unix.SetsockoptInt(fd, unix.SOL_SOCKET, unix.SO_REUSEPORT, 1))
	}
	for len(fds) < n && err == nil {
		if fd, _, err = listenSocket(laddr.String(), true); err == nil {
			fds = append(fds, fd)
		}
	}
	if err != nil {
		for _, fd := range fds {
			unix.Close(fd)
		}
		return nil, nil, err
	}
	return fds, laddr, nil
}

// listenSocket creates a listening socket with the help of the standard library (address resolution, dual stack,
// SO_REUSEADDR and backlog are all handled by the standard library), setting its options (see setListenerOptions) and,
// with reusePort, SO_REUSEPORT before it is bound; it then duplicates a separate fd for the server to manage, after which
// the standard library's listener is closed right away.
func listenSocket(addr string, reusePort bool) (int, net.Addr, error) {
	lc := net.ListenConfig{Control: func(_, _ string, rc syscall.RawConn) error {
		var err error
		if cerr := rc.Control(func(fd uintptr) {
			setListenerOptions(int(fd))
			if reusePort {
				err = os.NewSyscallError("setsockopt", unix.SetsockoptInt(int(fd), unix.SOL_SOCKET, unix.SO_REUSEPORT, 1))
			}
		}); cerr != nil {
			return cerr
		}
		return err
	}}
	ln, err := lc.Listen(context.Background(), "tcp", addr)
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
