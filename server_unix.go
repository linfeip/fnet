//go:build linux || darwin

package fnet

import (
	"errors"
	"net"
	"os"
	"sync"
	"time"

	"github.com/linfeip/fnet/poll"
	"github.com/linfeip/fnet/taskpool"

	"golang.org/x/sys/unix"
)

// listener is one listening socket: the fd the main reactor accepts on, and the address it is bound to.
type listener struct {
	fd   int
	addr net.Addr
}

// Server is a TCP server based on the main/sub-reactor model.
type Server struct {
	handler     Handler
	opts        Options
	listeners   []listener   // the listening sockets, all watched by the one acceptor
	acceptor    *poll.Poller // the main reactor's Poller, watches only the listeners
	loops       []*loop      // sub-reactors
	next        int          // round-robin index, accessed only by the main reactor
	onAccept    func(fd int, ev poll.Event)
	submitBatch func([]func()) // 仅默认执行器使用；自定义 Executor 保持逐任务调用

	openConnsWg sync.WaitGroup // connections not yet closed: closed means OnClose returned; Serve waits for them before exiting

	mu      sync.Mutex
	serving bool
	closed  bool
	err     error         // the fatal error that made the server exit
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
		done:    make(chan struct{}),
	}
	if opts.Executor == nil {
		s.submitBatch = taskpool.DefaultTaskPool.SubmitBatch
	}
	// The acceptor comes first so that release can clean up after any failure below.
	acceptor, err := poll.New()
	if err != nil {
		return nil, err
	}
	s.acceptor = acceptor
	for _, addr := range addrs {
		fd, laddr, err := listen(addr)
		if err != nil {
			s.release()
			return nil, err
		}
		if err := s.acceptor.AddRead(fd); err != nil {
			unix.Close(fd) // not in s.listeners yet, so release does not cover it
			s.release()
			return nil, err
		}
		s.listeners = append(s.listeners, listener{fd: fd, addr: laddr})
	}
	for range s.opts.NumLoops {
		l, err := newLoop(s)
		if err != nil {
			s.release()
			return nil, err
		}
		s.loops = append(s.loops, l)
	}
	s.onAccept = s.accept
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

// Serve starts all sub-reactors and runs the main reactor in the current goroutine, blocking until Close is
// called (returning ErrServerClosed) or a fatal error occurs.
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

	var wg sync.WaitGroup
	for _, l := range s.loops {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := l.run(); err != nil {
				s.shutdown(err)
			}
		}()
	}
	go s.tick()
	for {
		woken, err := s.acceptor.Wait(s.onAccept)
		if err != nil {
			s.shutdown(err)
			break
		}
		if woken && s.isClosed() {
			break
		}
	}
	for _, l := range s.loops {
		l.trigger(l.stop)
	}
	wg.Wait()
	s.openConnsWg.Wait() // event loops requested closing all connections before exiting, wait for their tasks to finish closing
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

// shutdown marks the server as closed and wakes the main reactor; err is the fatal error that caused the
// shutdown.
func (s *Server) shutdown(err error) (first, serving bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return false, s.serving
	}
	s.closed, s.err = true, err
	if s.serving {
		// Wake up while holding the lock: Serve only releases the acceptor after closed=true.
		s.acceptor.Wake()
	}
	return true, s.serving
}

// deadlineCheckInterval is the interval at which the event loops check connection deadlines, i.e. the
// precision of Conn.SetDeadline.
const deadlineCheckInterval = time.Second

// tick periodically makes every sub-reactor check connection deadlines; it stops after Serve exits.
func (s *Server) tick() {
	ticker := time.NewTicker(deadlineCheckInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			for _, l := range s.loops {
				l.trigger(l.checkDeadlines)
			}
		case <-s.done:
			return
		}
	}
}

func (s *Server) isClosed() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.closed
}

func (s *Server) release() {
	for _, ln := range s.listeners {
		unix.Close(ln.fd)
	}
	s.acceptor.Close()
	for _, l := range s.loops {
		l.close()
	}
}

// accept is the main reactor's readable callback: it accepts in batches on the ready listener until EAGAIN and
// distributes the connections to the sub-reactors in round-robin order. The connections of all the listeners
// go through the one round-robin, so they spread evenly over the sub-reactors no matter which address they
// arrived on.
func (s *Server) accept(listenerFd int, _ poll.Event) {
	for {
		fd, sa, err := accept(listenerFd)
		if err != nil {
			switch err {
			case unix.EAGAIN:
				return
			case unix.EINTR, unix.ECONNABORTED:
				continue
			case unix.EPROTO, unix.ENETDOWN, unix.ENOPROTOOPT, unix.EHOSTDOWN, unix.EHOSTUNREACH, unix.EOPNOTSUPP, unix.ENETUNREACH:
				// Linux's accept returns a network error that already happened on the new connection as its
				// error code; it affects only that one connection, and accept(2) requires retrying as with
				// EAGAIN, so the server must not stop because of it.
				continue
			case unix.EMFILE, unix.ENFILE, unix.ENOBUFS, unix.ENOMEM:
				// Resource exhaustion: back off briefly to avoid spinning under level-triggered mode.
				time.Sleep(10 * time.Millisecond)
				return
			default:
				s.shutdown(os.NewSyscallError("accept", err))
				return
			}
		}
		l := s.loops[s.next]
		s.next = (s.next + 1) % len(s.loops)
		c := &conn{fd: fd, loop: l, remote: sockaddrToAddrPort(sa)}
		c.task = c.run
		l.trigger(func() { l.register(c) })
	}
}

// listen creates the listening socket with the help of the standard library (address resolution, dual stack,
// SO_REUSEADDR and backlog are all handled by the standard library), then duplicates a separate fd and hands
// it to the main reactor to manage, after which the standard library's listener is closed right away.
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
