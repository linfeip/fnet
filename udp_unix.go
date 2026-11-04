//go:build linux || darwin

package fnet

import (
	"errors"
	"net"
	"net/netip"
	"os"
	"runtime"
	"sync"
	"sync/atomic"

	"github.com/linfeip/fnet/internal/bytepool"
	"github.com/linfeip/fnet/poll"

	"golang.org/x/sys/unix"
)

const (
	packetMaxPerRound      = 1024    // datagrams one task reads before re-queueing itself
	packetMaxBytesPerRound = 1 << 20 // and the bytes, so 64KB datagrams do not hold a worker for 64MB
)

// UDPServer is a UDP server based on the shared reactor foundation (see reactor_unix.go).
type UDPServer struct {
	reactor

	handler UDPHandler
	addr    net.Addr
	sockets []*udpSocket
}

// NewUDPServer creates one or more UDP sockets on addr and a Poller for each sub-reactor. It starts handling datagrams
// after Serve is called.
func NewUDPServer(addr string, handler UDPHandler, opts UDPOptions) (*UDPServer, error) {
	opts = opts.withDefaults()
	s := &UDPServer{handler: handler}
	if err := s.initReactor(opts.NumLoops, opts.Executor); err != nil {
		return nil, err
	}
	numSockets := 1
	if reusePortSpreads && opts.ReusePort {
		numSockets = runtime.GOMAXPROCS(0)
	}
	fds, laddr, err := listen("udp", addr, numSockets, false)
	if err != nil {
		s.release()
		return nil, err
	}
	s.addr = laddr

	loops := make([]*udpLoop, len(s.subReactors))
	for i, sub := range s.subReactors {
		ul := &udpLoop{}
		sub.onBatch = ul.runBatch
		loops[i] = ul
	}
	for i, fd := range fds {
		p, err := newUDPSocket(s, fd, laddr, opts)
		if err != nil {
			for _, rest := range fds[i:] {
				unix.Close(rest)
			}
			s.release()
			return nil, err
		}
		s.sockets = append(s.sockets, p)
	}
	for i, p := range s.sockets {
		idx := i % len(s.subReactors)
		if err := s.subReactors[idx].poller.AddListener(p.fd); err != nil {
			s.release()
			return nil, err
		}
		loops[idx].sockets = append(loops[idx].sockets, p)
	}
	return s, nil
}

// Addr returns the address actually bound by the server.
func (s *UDPServer) Addr() net.Addr { return s.addr }

// Conn returns one of the server's UDP sockets. All the sockets of a UDPServer share its address; this handle lets the
// application send datagrams before it has received anything, which hole punching needs.
func (s *UDPServer) Conn() UDPConn { return s.sockets[0] }

// Serve starts the sub-reactors' workers, which hand ready UDP socket tasks to the Executor, and blocks until Close is
// called (returning ErrServerClosed).
func (s *UDPServer) Serve() error {
	if err := s.beginServe(); err != nil {
		return err
	}
	for _, p := range s.sockets {
		p.counted.Store(true)
	}
	s.openConnsWg.Add(len(s.sockets))
	s.startWorkers()
	<-s.closing
	for _, p := range s.sockets {
		p.notify(evShutdown)
	}
	s.openConnsWg.Wait()
	s.stopWorkers()
	s.release()
	return s.finishServe()
}

// Close stops the server and closes its UDP sockets; all OnPacket callbacks have finished by the time it returns.
// It must therefore not be called from OnPacket: it waits for all callbacks to finish, including the one calling it,
// which deadlocks; call it from a new goroutine when needed.
func (s *UDPServer) Close() error {
	return s.closeWith(s.release)
}

func (s *UDPServer) release() {
	for _, p := range s.sockets {
		p.close()
	}
	s.releaseReactor()
}

type udpLoop struct {
	sockets []*udpSocket
}

func (l *udpLoop) runBatch(batch *poll.Batch) {
	for i := range batch.Len() {
		fd, _ := batch.Event(i)
		for _, p := range l.sockets {
			if p.fd == fd {
				p.notify(evRead)
				break
			}
		}
	}
}

type udpSocket struct {
	state     atomic.Uint32 // pending events and scheduledBit, see conn.notify
	counted   atomic.Bool   // openConnsWg has been incremented for this socket, see UDPServer.Serve
	srv       *UDPServer
	fd        int
	ipv4      bool     // the socket family is AF_INET
	localAddr net.Addr // cached, returned by LocalAddr
	task      func()   // run bound once, so scheduling does not allocate a method value

	mu     sync.RWMutex
	closed bool
}

func newUDPSocket(srv *UDPServer, fd int, localAddr net.Addr, opts UDPOptions) (*udpSocket, error) {
	if opts.ReadBufferSize > 0 {
		if err := setSocketBuffer(fd, unix.SOL_SOCKET, unix.SO_RCVBUF, opts.ReadBufferSize); err != nil {
			return nil, os.NewSyscallError("setsockopt", err)
		}
	}
	if opts.WriteBufferSize > 0 {
		if err := setSocketBuffer(fd, unix.SOL_SOCKET, unix.SO_SNDBUF, opts.WriteBufferSize); err != nil {
			return nil, os.NewSyscallError("setsockopt", err)
		}
	}
	sa, err := unix.Getsockname(fd)
	if err != nil {
		return nil, os.NewSyscallError("getsockname", err)
	}
	var ipv4 bool
	switch sa.(type) {
	case *unix.SockaddrInet4:
		ipv4 = true
	case *unix.SockaddrInet6:
	default:
		return nil, errors.New("fnet: unsupported UDP socket address family")
	}
	p := &udpSocket{
		srv:       srv,
		fd:        fd,
		ipv4:      ipv4,
		localAddr: localAddr,
	}
	p.task = p.run
	return p, nil
}

// setSocketBuffer sets SO_RCVBUF or SO_SNDBUF, stepping down on ENOBUFS/EINVAL where BSD kernels reject values above
// kern.ipc.maxsockbuf instead of clamping them as Linux does.
func setSocketBuffer(fd, level, opt, size int) error {
	for s := size; s >= 64<<10; s /= 2 {
		if err := unix.SetsockoptInt(fd, level, opt, s); err != unix.ENOBUFS && err != unix.EINVAL {
			return err
		}
	}
	return unix.SetsockoptInt(fd, level, opt, size)
}

func (p *udpSocket) notify(ev uint32) {
	if p.state.Or(ev|scheduledBit)&scheduledBit == 0 {
		p.schedule()
	}
}

func (p *udpSocket) schedule() { p.srv.executor(p.fd, p.task) }

func (p *udpSocket) release() {
	if !p.state.CompareAndSwap(scheduledBit, 0) {
		p.schedule()
	}
}

func (p *udpSocket) run() {
	ev := p.state.Swap(scheduledBit) &^ scheduledBit
	defer func() {
		if r := recover(); r != nil {
			// Put back the events this round took and queue exactly one more task. scheduledBit is still set,
			// so no notify can queue another one concurrently; the executor logs the panic and the socket lives on.
			p.state.Or(ev)
			p.schedule()
			panic(r)
		}
	}()
	p.handle(ev)
	// A closed socket keeps its task for good, so no leftover poll event submits another one after openConnsWg.Done.
	if ev&evShutdown == 0 {
		p.release()
	}
}

func (p *udpSocket) handle(ev uint32) {
	if ev&evShutdown != 0 {
		p.close()
		return
	}
	if p.closedForRead() { // a stale poll event for a socket that has already closed: its fd may be reused
		return
	}
	buf := bytepool.Get(maxDatagramSize)
	defer buf.Release()
	b := buf.Bytes()[:maxDatagramSize]
	for count, bytes := 0, 0; count < packetMaxPerRound && bytes < packetMaxBytesPerRound; count++ {
		n, from, err := recvDatagram(p.fd, b)
		if err != nil { // EAGAIN ends the round; a datagram error is not a socket error, so keep the socket
			return
		}
		bytes += n
		p.srv.handler.OnPacket(p, b[:n], from)
	}
	p.notify(evRead) // the round budget is spent and the edge was consumed; there may be more datagrams
}

func (p *udpSocket) closedForRead() bool {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.closed
}

func (p *udpSocket) LocalAddr() net.Addr { return p.localAddr }

func (p *udpSocket) WriteToUDPAddrPort(b []byte, addr netip.AddrPort) (int, error) {
	p.mu.RLock()
	defer p.mu.RUnlock()
	if p.closed {
		return 0, net.ErrClosed
	}
	if err := sendDatagram(p.fd, b, addr, p.ipv4); err != nil {
		return 0, err
	}
	return len(b), nil
}

func (p *udpSocket) close() {
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return
	}
	p.closed = true
	unix.Close(p.fd)
	p.mu.Unlock()
	if p.counted.Swap(false) {
		p.srv.openConnsWg.Done()
	}
}
