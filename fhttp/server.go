// Package fhttp serves HTTP/1.x and HTTPS from fnet's event loops behind the
// standard http.Handler API. Idle keep-alive connections are parked on a
// poller and hold no goroutine; package websocket upgrades them onto the same
// event loops.
package fhttp

import (
	"crypto/tls"
	"errors"
	"net"
	"net/http"
	"time"

	"github.com/linfeip/fnet/internal/reactor"
)

// ErrServerClosed is returned by the ListenAndServe methods after Close.
var ErrServerClosed = errors.New("fnet/fhttp: server closed")

const (
	// DefaultReadHeaderTimeout bounds the wait for a request header when neither
	// ReadHeaderTimeout nor ReadTimeout is set.
	DefaultReadHeaderTimeout = 30 * time.Second
	// DefaultIdleTimeout closes plaintext keep-alive connections idle this long
	// when IdleTimeout is not set.
	DefaultIdleTimeout = 2 * time.Minute
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
	// connection. Plaintext idle connections wait on the poller and hold no
	// goroutine: 0 means DefaultIdleTimeout, negative means no limit. An idle
	// TLS connection holds a worker goroutine, so there keep-alive is opt-in:
	// 0 closes the connection after each response.
	IdleTimeout time.Duration
	// KeepAlive is the idle time before TCP keep-alive probes start, which
	// find peers that vanished without closing (event-driven WebSocket
	// connections have no idle timeout of their own). 0 means
	// DefaultKeepAlive, negative turns probes off.
	KeepAlive time.Duration
	// NumPollers is the number of event loops. Defaults to runtime.GOMAXPROCS(0).
	NumPollers int

	// Listen optionally creates the listeners (e.g. for socket activation).
	// By default sockets are opened with SO_REUSEADDR and SO_REUSEPORT.
	Listen func(network, addr string) (net.Listener, error)

	// WorkerPool runs request handlers, keyed by connection for affinity
	// (e.g. p.SubmitConn for a *pool.Pool p, or pool.Adapt(ants.Submit)). It
	// is called on an event loop and must not block. Defaults to pool.Default().
	WorkerPool func(connID uint64, task func())

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
