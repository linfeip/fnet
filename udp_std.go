//go:build !linux && !darwin

package fnet

import (
	"errors"
	"log/slog"
	"net"
	"net/netip"
	"sync"
)

// UDPServer is a simple UDP server implementation on top of the standard library net package (for platforms such as
// Windows): one goroutine reads datagrams and runs OnPacket serially, and WriteToUDPAddrPort is a blocking write.
type UDPServer struct {
	handler UDPHandler
	conn    *stdUDPConn

	mu      sync.Mutex
	serving bool
	closed  bool
	wg      sync.WaitGroup
}

type stdUDPConn struct {
	*net.UDPConn
	localAddr net.Addr
}

func (p *stdUDPConn) LocalAddr() net.Addr { return p.localAddr }

func (p *stdUDPConn) WriteToUDPAddrPort(b []byte, addr netip.AddrPort) (int, error) {
	n, err := p.UDPConn.WriteToUDPAddrPort(b, addr)
	if errors.Is(err, net.ErrClosed) {
		err = net.ErrClosed // the UDPConn contract is the exact error, as on Linux/macOS
	}
	return n, err
}

// NewUDPServer creates a UDP socket on addr; datagrams start being handled once Serve is called. opts is ignored on
// this platform.
func NewUDPServer(addr string, handler UDPHandler, _ UDPOptions) (*UDPServer, error) {
	pc, err := net.ListenPacket("udp", addr)
	if err != nil {
		return nil, err
	}
	conn, ok := pc.(*net.UDPConn)
	if !ok {
		pc.Close()
		return nil, errors.New("fnet: not a UDP socket")
	}
	return &UDPServer{
		handler: handler,
		conn:    &stdUDPConn{UDPConn: conn, localAddr: pc.LocalAddr()},
	}, nil
}

// Addr returns the address actually bound by the server.
func (s *UDPServer) Addr() net.Addr { return s.conn.LocalAddr() }

// Conn returns the server's UDP socket, letting the application send datagrams before it has received anything.
func (s *UDPServer) Conn() UDPConn { return s.conn }

// Serve reads datagrams from the socket until Close is called, returning ErrServerClosed.
func (s *UDPServer) Serve() error {
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
	s.wg.Add(1)
	s.mu.Unlock()

	defer s.wg.Done()
	buf := make([]byte, maxDatagramSize)
	for {
		n, from, err := s.conn.ReadFromUDPAddrPort(buf)
		if err != nil {
			s.mu.Lock()
			closed := s.closed
			s.mu.Unlock()
			if closed || errors.Is(err, net.ErrClosed) {
				return ErrServerClosed
			}
			slog.Error("fnet: udp read error", "err", err)
			return err
		}
		func() {
			defer recoverPanic(nil)
			s.handler.OnPacket(s.conn, buf[:n], from)
		}()
	}
}

// Close stops the server and closes its UDP socket; all OnPacket callbacks have finished by the time it returns.
func (s *UDPServer) Close() error {
	s.mu.Lock()
	if !s.closed {
		s.closed = true
		s.conn.Close()
	}
	s.mu.Unlock()
	s.wg.Wait()
	return nil
}
