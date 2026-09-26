// Package fhttp serves HTTP/1.x and HTTPS from fnet's event loops behind the
// standard http.Handler API. Idle keep-alive connections are parked on a
// poller and hold no goroutine; package websocket upgrades them onto the same
// event loops.
package fhttp

import (
	"context"
	"crypto/tls"
	"errors"
	"log"
	"net"
	"net/http"
	"time"

	"github.com/linfeip/fnet/internal/reactor"
)

// ErrServerClosed is returned by the ListenAndServe methods after Close or
// Shutdown.
var ErrServerClosed = errors.New("fnet/fhttp: server closed")

const (
	// DefaultReadHeaderTimeout bounds the wait for a request header when neither
	// ReadHeaderTimeout nor ReadTimeout is set.
	DefaultReadHeaderTimeout = 30 * time.Second
	// DefaultIdleTimeout closes plaintext keep-alive connections idle this long
	// when IdleTimeout is not set.
	DefaultIdleTimeout = 2 * time.Minute
	// DefaultMaxHeaderBytes bounds a request line and header when
	// MaxHeaderBytes is not set.
	DefaultMaxHeaderBytes = 64 << 10
	// DefaultKeepAlive is the TCP keep-alive idle time when KeepAlive is not
	// set: a peer that vanished without closing is dropped about two minutes
	// later.
	DefaultKeepAlive = reactor.DefaultKeepAliveIdle
)

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
	// fhttp speaks HTTP/1.1 only, so "h2" is left out of its NextProtos.
	TLSConfig *tls.Config
	// ReadHeaderTimeout bounds the wait for a request header: for a new
	// connection from accept, for later requests from their first byte. It
	// also bounds the TLS handshake. 0 means ReadTimeout, or
	// DefaultReadHeaderTimeout if that is 0 too; negative means no limit.
	ReadHeaderTimeout time.Duration
	// ReadTimeout bounds reading each request, header and body.
	ReadTimeout time.Duration
	// WriteTimeout bounds writing each response.
	WriteTimeout time.Duration
	// IdleTimeout bounds the wait for the next request on a keep-alive
	// connection, which waits on the poller and holds no goroutine: 0 means
	// DefaultIdleTimeout, negative means no limit. Under TLS keep-alive is
	// opt-in, since an idle TLS connection keeps its TLS state (buffers of a
	// few tens of KiB): there 0 closes the connection after each response.
	IdleTimeout time.Duration
	// MaxHeaderBytes bounds the request line and header; a larger one is
	// refused with 431. 0 means DefaultMaxHeaderBytes.
	MaxHeaderBytes int
	// KeepAlive is the idle time before TCP keep-alive probes start, which
	// find peers that vanished without closing (event-driven WebSocket
	// connections have no idle timeout of their own). 0 means
	// DefaultKeepAlive, negative turns probes off.
	KeepAlive time.Duration
	// NumPollers is the number of event loops. Defaults to a third of
	// runtime.GOMAXPROCS(0), rounded up: the loops only read and frame, and the
	// workers do the rest.
	NumPollers int

	// Listen optionally creates the listeners (e.g. for socket activation, or
	// SO_REUSEPORT to run several servers on one port). By default they are
	// opened like net.Listen.
	Listen func(network, addr string) (net.Listener, error)

	// WorkerPool runs request handlers (e.g. p.SubmitConn for a *pool.Pool p,
	// or pool.Adapt(ants.Submit)). It is called on an event loop and must not
	// block; an error refuses the request and closes its connection. Defaults
	// to pool.Default().
	WorkerPool func(connID uint64, task func()) error

	// ErrorLog logs handler panics and invalid handler output, such as a
	// malformed Content-Length. nil means the log package's standard logger.
	ErrorLog *log.Logger

	run reactor.Runner
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
// Close or Shutdown and then returns ErrServerClosed.
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
	cfg := reactor.Config{Loops: s.NumPollers, KeepAlive: reactor.KeepAliveFor(s.KeepAlive)}
	if err := s.run.Run(cfg, s.Addr, s.Addrs, s.Listen, newHTTPHandler(s, tlsConfig)); err != nil {
		return err
	}
	return ErrServerClosed
}

// Close closes the listeners and every connection immediately. Handlers that
// are still running see their connection closed. Close is idempotent.
func (s *Server) Close() error {
	if eng := s.run.Stop(); eng != nil {
		eng.Close()
	}
	return nil
}

// Shutdown stops the server gracefully: it stops accepting, closes the
// keep-alive connections waiting for a request, and lets every request in
// progress finish, its response then closing the connection. It returns once
// no HTTP connection is left, or, if ctx ends first, closes every connection
// like Close and returns ctx.Err().
//
// Like net/http, Shutdown leaves hijacked and upgraded (WebSocket)
// connections alone; Close drops them.
func (s *Server) Shutdown(ctx context.Context) error {
	eng := s.run.Stop()
	if eng == nil {
		return nil
	}
	h := eng.Handler().(*httpHandler)
	h.shuttingDown.Store(true)
	eng.StopAccept()
	for wait := time.Millisecond; ; wait = min(2*wait, 500*time.Millisecond) {
		if h.closeIdle(eng) == 0 && h.active.Load() == 0 {
			others := 0
			eng.ForEach(func(*reactor.Conn) { others++ })
			if others == 0 {
				eng.Close() // nothing is left for the event loops
			}
			return nil
		}
		select {
		case <-ctx.Done():
			eng.Close()
			return ctx.Err()
		case <-time.After(wait):
		}
	}
}
