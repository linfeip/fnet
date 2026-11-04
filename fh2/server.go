// Package fh2 is an HTTP/2 (RFC 9113) server built on the fnet engine.
//
// Architecture:
//   - Framing, HPACK decoding and connection-level control frames (SETTINGS, PING, WINDOW_UPDATE, RST_STREAM, GOAWAY)
//     run directly inside fnet's serial OnData callback without spawning a read or control goroutine per connection.
//     An idle HTTP/2 connection occupies zero goroutines and holds no read or write buffer.
//   - Each complete request opens a stream and submits its http.Handler task to Options.Executor (a private
//     taskpool.NewDefault() by default), spreading concurrent streams of a connection across executor shards.
//   - Response headers are encoded with golang.org/x/net/http2/hpack and written directly through fnet's non-blocking
//     Conn.Write / Writev under a per-connection write lock. Small responses buffer up to 64KB and go out in a single
//     Writev of HEADERS and DATA; larger or flushed responses stream DATA frames bounded by both HTTP/2 flow-control
//     windows and fnet.Conn.Writable.
//   - With Options.TLSConfig set, TLS and ALPN ("h2") run in-callback via package ftls; otherwise the server speaks
//     cleartext HTTP/2 with prior knowledge (h2c).
package fh2

import (
	"crypto/tls"
	"errors"
	"net"
	"net/http"
	"time"

	"github.com/linfeip/fnet"
	"github.com/linfeip/fnet/ftls"
	"github.com/linfeip/fnet/taskpool"
)

const (
	defaultMaxConcurrentStreams = 250
	defaultMaxHeaderBytes       = 64 << 10
	defaultStreamWindowSize     = 256 << 10
	defaultConnWindowSize       = 1 << 20
	defaultReadHeaderTimeout    = 10 * time.Second
	defaultIdleTimeout          = 120 * time.Second
)

// Options holds the HTTP/2 server parameters; the zero value is the default configuration.
type Options struct {
	// MaxConcurrentStreams is the maximum number of concurrent client-initiated streams per connection
	// (SETTINGS_MAX_CONCURRENT_STREAMS); when ==0 it is 250.
	MaxConcurrentStreams uint32
	// MaxHeaderBytes is the maximum uncompressed size of a request's header list in bytes
	// (SETTINGS_MAX_HEADER_LIST_SIZE, counting 32 bytes per field per RFC 9113 6.5.2); when <=0 it is 64KB.
	// A request exceeding this limit, or having more than 500 header fields, is answered with 431.
	MaxHeaderBytes int
	// MaxReadFrameSize is the largest frame payload the server accepts (SETTINGS_MAX_FRAME_SIZE), clamped to
	// [16KB, 16MB-1]; when <=0 it is 16KB.
	MaxReadFrameSize int
	// InitialWindowSize is the initial per-stream receive window in bytes (SETTINGS_INITIAL_WINDOW_SIZE);
	// when <=0 it is 256KB, capped at 2^31-1.
	InitialWindowSize int
	// ConnectionWindowSize is the connection-level receive window in bytes; when <=0 it is max(1MB, InitialWindowSize),
	// clamped to [65535, 2^31-1].
	ConnectionWindowSize int
	// ReadHeaderTimeout bounds the TLS handshake, the client connection preface and initial SETTINGS frame, and any
	// incomplete frame or multi-frame header block on an idle connection; when 0 it is 10s, when <0 it is unlimited.
	ReadHeaderTimeout time.Duration
	// IdleTimeout is the maximum time a connection with no active streams may remain idle, and the maximum time a
	// blocked response write waits for a slow client to grant flow-control window or drain TCP buffers; when 0 it is
	// 120s, when <0 it is unlimited.
	IdleTimeout time.Duration
	// Engine configures the underlying fnet engine.
	Engine fnet.Options
	// Executor runs request Handlers asynchronously. It must not block and must not run the task directly on the
	// caller's stack. When nil, NewServer creates a private taskpool.NewDefault() executor whose workers stop when the
	// server closes.
	Executor func(key int, task func())
	// TLSConfig, when non-nil, serves HTTPS over TLS (through package ftls) offering "h2" via ALPN. When nil, the
	// server accepts cleartext HTTP/2 connections with prior knowledge (h2c).
	TLSConfig *tls.Config
}

func (o Options) withDefaults() Options {
	if o.MaxConcurrentStreams == 0 {
		o.MaxConcurrentStreams = defaultMaxConcurrentStreams
	}
	if o.MaxHeaderBytes <= 0 {
		o.MaxHeaderBytes = defaultMaxHeaderBytes
	}
	if o.MaxReadFrameSize <= 0 {
		o.MaxReadFrameSize = minMaxFrameSize
	} else {
		o.MaxReadFrameSize = min(max(o.MaxReadFrameSize, minMaxFrameSize), maxFrameSizeLimit)
	}
	if o.InitialWindowSize <= 0 {
		o.InitialWindowSize = defaultStreamWindowSize
	} else {
		o.InitialWindowSize = min(o.InitialWindowSize, maxWindowSize)
	}
	if o.ConnectionWindowSize <= 0 {
		o.ConnectionWindowSize = max(defaultConnWindowSize, o.InitialWindowSize)
	}
	o.ConnectionWindowSize = min(max(o.ConnectionWindowSize, defaultInitialWindowSize), maxWindowSize)
	if o.ReadHeaderTimeout == 0 {
		o.ReadHeaderTimeout = defaultReadHeaderTimeout
	}
	if o.IdleTimeout == 0 {
		o.IdleTimeout = defaultIdleTimeout
	}
	return o
}

// Server is an HTTP/2 server.
type Server struct {
	handler    http.Handler
	opts       Options
	pool       *taskpool.Pool
	engine     *fnet.Server
	httpServer *http.Server // exposed via http.ServerContextKey in Request.Context
}

// NewServer creates an HTTP/2 server on addr; when handler is nil, http.DefaultServeMux is used.
func NewServer(addr string, handler http.Handler, opts Options) (*Server, error) {
	return NewServerAddrs([]string{addr}, handler, opts)
}

// NewServerAddrs creates an HTTP/2 server listening on all of addrs sharing a single fnet engine; when handler is nil,
// http.DefaultServeMux is used.
func NewServerAddrs(addrs []string, handler http.Handler, opts Options) (*Server, error) {
	if handler == nil {
		handler = http.DefaultServeMux
	}
	opts = opts.withDefaults()
	var pool *taskpool.Pool
	if opts.Executor == nil {
		pool = taskpool.NewDefault()
		opts.Executor = pool.SubmitTo
	}
	s := &Server{
		handler:    handler,
		opts:       opts,
		pool:       pool,
		httpServer: &http.Server{Handler: handler, MaxHeaderBytes: opts.MaxHeaderBytes},
	}
	var h fnet.Handler = engineHandler{server: s}
	if s.opts.TLSConfig != nil {
		h = ftls.NewHandler(h, h2TLSConfig(s.opts.TLSConfig), s.opts.ReadHeaderTimeout)
	}
	engine, err := fnet.NewServerAddrs(addrs, h, s.opts.Engine)
	if err != nil {
		s.closePool()
		return nil, err
	}
	s.engine = engine
	return s, nil
}

// h2TLSConfig returns a copy of config offering "h2" via ALPN (removing "http/1.1" while preserving auxiliary ALPN
// tokens such as "acme-tls/1") and enforcing at least TLS 1.2 as required by RFC 9113 9.2.
func h2TLSConfig(config *tls.Config) *tls.Config {
	config = config.Clone()
	if config.MinVersion < tls.VersionTLS12 {
		config.MinVersion = tls.VersionTLS12
	}
	protos := []string{"h2"}
	for _, p := range config.NextProtos {
		if p != "h2" && p != "http/1.1" {
			protos = append(protos, p)
		}
	}
	config.NextProtos = protos
	if get := config.GetConfigForClient; get != nil {
		config.GetConfigForClient = func(hello *tls.ClientHelloInfo) (*tls.Config, error) {
			c, err := get(hello)
			if c == nil || err != nil {
				return c, err
			}
			return h2TLSConfig(c), nil
		}
	}
	return config
}

// Addr returns the first address being listened on.
func (s *Server) Addr() net.Addr { return s.engine.Addr() }

// Addrs returns all addresses being listened on, in the order given to NewServerAddrs.
func (s *Server) Addrs() []net.Addr { return s.engine.Addrs() }

// Serve runs the server and blocks until Close is called (returning http.ErrServerClosed) or a fatal error occurs.
func (s *Server) Serve() error {
	err := s.engine.Serve()
	s.closePool()
	if errors.Is(err, fnet.ErrServerClosed) {
		return http.ErrServerClosed
	}
	return err
}

// Close closes all listeners and connections.
func (s *Server) Close() error {
	s.closePool()
	return s.engine.Close()
}

func (s *Server) closePool() {
	if s.pool != nil {
		s.pool.Close()
	}
}

type engineHandler struct {
	server *Server
}

func (h engineHandler) OnOpen(c fnet.Conn) {
	hc := newConn(h.server, c)
	c.SetContext(hc)
	hc.onOpen()
}

func (h engineHandler) OnData(c fnet.Conn, data []byte) int {
	return c.Context().(*conn).onData(data)
}

func (h engineHandler) OnWritable(c fnet.Conn) {
	c.Context().(*conn).onWritable()
}

func (h engineHandler) OnClosing(c fnet.Conn, err error) {
	c.Context().(*conn).onClosing(err)
}

func (h engineHandler) OnClose(c fnet.Conn, err error) {
	c.Context().(*conn).onClose(err)
}
