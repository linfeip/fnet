//go:build linux || darwin

package fnet

import (
	"errors"
	"net"
	"os"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"golang.org/x/sys/unix"
)

// Server is a TCP server based on the main/sub-reactor model.
type Server struct {
	reactor

	handler     Handler
	opts        Options
	addrs       []net.Addr    // the addresses listened on, one per address given to NewServerAddrs
	listenerFds []int         // the listening sockets, one per address and loop or one per address (see listenerPerLoop)
	loops       []*loop       // TCP sub-reactors, paired 1:1 with reactor.subReactors
	nextLoop    atomic.Uint32 // round-robin over the loops for the connections of a listener they share
	// listenerPerLoop: every loop listens on each address with a socket of its own, sharing the address through
	// SO_REUSEPORT, and serves the connections it accepts, so the loops accept in parallel instead of contending for one
	// listener and a connection stays with the worker that accepted it; otherwise the loops share one listener per
	// address, and the connections accepted from it are spread over them in round-robin order. Set with
	// Options.ReusePort where the platform's SO_REUSEPORT spreads connections (see reusePortSpreads).
	listenerPerLoop bool
	// acceptMu is held for reading while a connection is accepted and counted in openConnsWg, and taken once for
	// writing by Serve after setting draining, so that no connection is counted once it waits for them.
	acceptMu       sync.RWMutex
	acceptRetrying atomic.Bool // a retry of the accepts that ran out of resources is pending, see retryAccept
	draining       atomic.Bool // Serve is closing every connection: one being opened closes itself (see loop.watch)
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
	opts = opts.withDefaults()
	s := &Server{
		handler:         handler,
		opts:            opts,
		listenerPerLoop: reusePortSpreads && opts.ReusePort,
	}
	if err := s.initReactor(opts.NumLoops, opts.Executor); err != nil {
		return nil, err
	}
	s.opts.Executor = s.executor
	for _, sub := range s.subReactors {
		l := &loop{srv: s, poller: sub.poller}
		sub.onBatch = l.runBatch
		s.loops = append(s.loops, l)
	}
	sockets := 1
	if s.listenerPerLoop {
		sockets = len(s.loops)
	}
	for _, addr := range addrs {
		fds, laddr, err := listen("tcp", addr, sockets, s.opts.NoDelay)
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
	if err := s.beginServe(); err != nil {
		return err
	}
	s.startWorkers()
	go s.tick()
	<-s.closing
	// No connection is accepted from here on, and one accepted before is either in connsByFd already or closes
	// itself (see loop.watch).
	s.draining.Store(true)
	s.acceptMu.Lock()
	s.acceptMu.Unlock() //nolint:staticcheck // an empty critical section: it waits for the accepts under way
	s.closeListeners()  // a client connecting from now on is refused, instead of waiting in the listen queue until the server has gone
	connsByFd.each(func(c *conn) {
		if c.loop.srv == s {
			c.notify(evShutdown)
		}
	})
	s.openConnsWg.Wait() // the closes are carried out by the connections' tasks, which the Executor runs
	s.stopWorkers()
	s.release()
	return s.finishServe()
}

// Close stops the server and closes all connections (calling OnClose for each one); all callbacks have
// finished by the time it returns. It therefore must not be called from a callback: it waits for the callbacks
// of all connections to finish, including the one calling it, which deadlocks; call it from a new goroutine
// when needed.
func (s *Server) Close() error {
	return s.closeWith(s.release)
}

// deadlineCheckInterval is the interval at which the event loops check connection deadlines, i.e. the
// precision of Conn.SetDeadline.
const deadlineCheckInterval = time.Second

// tick periodically checks the connection deadlines of the server; it stops after Serve exits.
func (s *Server) tick() {
	ticker := time.NewTicker(deadlineCheckInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			s.checkDeadlines()
		case <-s.done:
			return
		}
	}
}

// checkDeadlines notifies the connections whose deadline has passed, which then close; tick calls it once every
// deadlineCheckInterval.
func (s *Server) checkDeadlines() {
	connsByFd.checkDeadlines(clockNow())
}

func (s *Server) release() {
	s.closeListeners()
	s.releaseReactor()
}

// closeListeners closes the listening sockets once no accept can run on them any more (see acceptConn), and only once:
// their fd numbers may be reused right after. An event already retrieved for one still finds it in its loop's
// listenerFds, and acceptConn then returns without accepting.
func (s *Server) closeListeners() {
	for _, fd := range s.listenerFds {
		unix.Close(fd)
	}
	s.listenerFds = nil
}

// acceptConn accepts a connection on listenerFd, a listener of loop l, and counts it in openConnsWg; it returns nil when
// there is none left to accept or the server is draining. The connection belongs to l, or, when the loops share the
// listener (see listenerPerLoop), to a loop chosen in round-robin order. Its first task, which the caller notifies with
// evOpen, watches it (see loop.watch).
func (s *Server) acceptConn(listenerFd int, l *loop) *conn {
	s.acceptMu.RLock()
	defer s.acceptMu.RUnlock()
	for !s.draining.Load() {
		fd, remoteAddr, err := accept(listenerFd)
		if err == nil {
			if !s.listenerPerLoop {
				l = s.loops[s.nextLoop.Add(1)%uint32(len(s.loops))]
			}
			c := &conn{fd: fd, loop: l, remoteAddr: remoteAddr}
			c.task = c.run
			s.openConnsWg.Add(1) // decremented again once OnClose has returned and the fd is closed
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
			// Resource exhaustion: the connection stays in the listen queue, and the edge-triggered listener raises no
			// new event for it when resources are freed, so accepting is retried later (see retryAccept), while the
			// worker goes on serving the connections it has.
			s.retryAccept()
			return nil
		default:
			s.shutdown(os.NewSyscallError("accept", err))
			return nil
		}
	}
	return nil
}

// acceptRetryDelay is how long accepting waits after running out of resources, see retryAccept.
const acceptRetryDelay = 10 * time.Millisecond

// retryAccept accepts on every listener again after acceptRetryDelay, unless such a retry is pending already: descriptors
// and memory are the process's, so whichever listener ran out, every one may have connections waiting. A retry that runs
// out again schedules the next one; after Serve has begun closing, acceptConn returns without accepting.
func (s *Server) retryAccept() {
	if s.acceptRetrying.Swap(true) {
		return
	}
	time.AfterFunc(acceptRetryDelay, func() {
		s.acceptRetrying.Store(false)
		if s.draining.Load() {
			return
		}
		for _, l := range s.loops {
			l.acceptAll()
		}
	})
}
