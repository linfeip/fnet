// Package fnet serves HTTP/1.x, HTTPS and WebSocket from native event loops
// (epoll, kqueue, or the Windows emulation) behind the standard http.Handler
// API. Idle connections are parked on a poller and hold no goroutine.
//
// Layout: internal/netpoll is the platform layer, internal/reactor owns the
// event loops and connections, this package runs HTTP on top of them, and
// package websocket runs WebSocket on top of the same connections.
package fnet

import (
	"crypto/tls"
	"errors"
	"net"
	"net/http"
	"runtime"
	"sync"
	"time"

	"github.com/linfeip/fnet/internal/netpoll"
	"github.com/linfeip/fnet/internal/reactor"
)

// ErrServerClosed is returned by the ListenAndServe methods after Close.
var ErrServerClosed = errors.New("fnet: server closed")

const (
	// DefaultReadHeaderTimeout bounds the wait for a request header when neither
	// ReadHeaderTimeout nor ReadTimeout is set.
	DefaultReadHeaderTimeout = 30 * time.Second
	// DefaultIdleTimeout closes plaintext keep-alive connections idle this long
	// when IdleTimeout is not set.
	DefaultIdleTimeout = 2 * time.Minute
)

// ErrWriteBufferFull is returned by a connection write when its outbound queue
// (16 MiB) is full because the peer is not reading.
var ErrWriteBufferFull = reactor.ErrWriteBufferFull

// Server serves HTTP/1.x over native event loops. A single Server runs one
// accept loop and NumPollers event loops however many addresses it listens on.
type Server struct {
	// Addr is the TCP address to listen on (e.g. ":8080").
	Addr string
	// Addrs lists additional addresses; all share the accept loop and pollers.
	Addrs []string
	// Handler serves requests. Defaults to http.DefaultServeMux.
	Handler http.Handler
	// TLSConfig enables HTTPS when non-nil (or when ListenAndServeTLS is used).
	TLSConfig *tls.Config
	// ReadHeaderTimeout bounds the wait for a request header: for a new
	// connection from accept, for later requests from their first byte. It
	// also bounds the TLS handshake. 0 means ReadTimeout, or
	// DefaultReadHeaderTimeout if that is 0 too; negative means no limit.
	ReadHeaderTimeout time.Duration
	// ReadTimeout bounds reading each request, header and body.
	ReadTimeout time.Duration
	// WriteTimeout bounds writing each response, including flushing it when
	// the connection closes.
	WriteTimeout time.Duration
	// IdleTimeout bounds the wait for the next request on a keep-alive
	// connection. Plaintext idle connections wait on the poller and hold no
	// goroutine: 0 means DefaultIdleTimeout, negative means no limit. An idle
	// TLS connection holds a worker goroutine, so there keep-alive is opt-in:
	// 0 closes the connection after each response.
	IdleTimeout time.Duration
	// NumPollers is the number of event loops. Defaults to runtime.GOMAXPROCS(0).
	NumPollers int

	// Listen optionally creates the listeners (e.g. for socket activation).
	// By default sockets are opened with SO_REUSEADDR and SO_REUSEPORT.
	Listen func(network, addr string) (net.Listener, error)

	// WorkerPool runs request handlers, keyed by connection for affinity
	// (e.g. pool.SubmitConn, or fnet.AdaptPool(ants.Submit)). It is called on
	// an event loop and must not block. Defaults to DefaultWorkerPool.
	WorkerPool func(connID uint64, task func())

	mu     sync.Mutex
	closed bool
	engine *reactor.Engine
}

// ListenAndServe listens on addr and serves HTTP with handler.
func ListenAndServe(addr string, handler http.Handler) error {
	return (&Server{Addr: addr, Handler: handler}).ListenAndServe()
}

// ListenAndServeTLS listens on addr and serves HTTPS with handler.
func ListenAndServeTLS(addr, certFile, keyFile string, handler http.Handler) error {
	return (&Server{Addr: addr, Handler: handler}).ListenAndServeTLS(certFile, keyFile)
}

// ListenAndServe serves HTTP, or HTTPS if TLSConfig is set. It blocks until
// Close and then returns ErrServerClosed.
func (s *Server) ListenAndServe() error {
	return s.serve(s.TLSConfig)
}

// ListenAndServeTLS serves HTTPS with the given certificate on top of
// TLSConfig (which is left unmodified).
func (s *Server) ListenAndServeTLS(certFile, keyFile string) error {
	cert, err := tls.LoadX509KeyPair(certFile, keyFile)
	if err != nil {
		return err
	}
	cfg := &tls.Config{}
	if s.TLSConfig != nil {
		cfg = s.TLSConfig.Clone()
	}
	cfg.Certificates = []tls.Certificate{cert}
	return s.serve(cfg)
}

func (s *Server) serve(tlsConfig *tls.Config) error {
	if s.isClosed() {
		return ErrServerClosed
	}
	listeners, err := s.listen()
	if err != nil {
		return err
	}
	loops := s.NumPollers
	if loops <= 0 {
		loops = runtime.GOMAXPROCS(0)
	}
	eng, err := reactor.New(loops, listeners, newHTTPHandler(s, tlsConfig))
	if err != nil {
		return err
	}

	// The sockets are already accepting, so Close may have run meanwhile.
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		eng.Close()
		return ErrServerClosed
	}
	s.engine = eng
	s.mu.Unlock()

	if err := eng.Serve(); err != nil {
		return err
	}
	return ErrServerClosed
}

func (s *Server) listen() ([]reactor.Listener, error) {
	addrs := s.Addrs
	if s.Addr != "" || len(addrs) == 0 {
		addrs = append([]string{s.Addr}, addrs...)
	}
	var lns []reactor.Listener
	for _, addr := range addrs {
		var (
			fd    int
			laddr net.Addr
			err   error
		)
		if s.Listen != nil {
			var ln net.Listener
			if ln, err = s.Listen("tcp", addr); err == nil {
				fd, laddr, err = netpoll.FromListener(ln)
			}
		} else {
			fd, laddr, err = netpoll.Listen("tcp", addr)
		}
		if err != nil {
			for _, ln := range lns {
				_ = netpoll.Close(ln.FD)
			}
			return nil, err
		}
		lns = append(lns, reactor.Listener{FD: fd, Addr: laddr})
	}
	return lns, nil
}

func (s *Server) isClosed() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.closed
}

// Close closes the listeners and every connection immediately. Handlers that
// are still running see their connection closed. Close is idempotent.
func (s *Server) Close() error {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return nil
	}
	s.closed = true
	eng := s.engine
	s.mu.Unlock()
	if eng != nil {
		eng.Close()
	}
	return nil
}
