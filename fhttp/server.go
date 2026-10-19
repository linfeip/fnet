// Package fhttp is an HTTP/1.x server built on the fnet engine.
//
// The division of labor:
//   - fnet's OnData callback (on a goroutine of the executor) only finds where each request message ends: the
//     header terminator "\r\n\r\n", then a small body, with Content-Length or in chunks. It copies the complete
//     messages into the connection's request queue without parsing them;
//   - a worker goroutine of Options.Executor (taskpool.DefaultTaskPool by default; the Handlers may block, so
//     they are not run in OnData's task, which returns at once) takes the messages out in order, parses each (a
//     request in the plain forms of HTTP/1.x by fhttp itself, field for field the way the standard library's
//     http.ReadRequest would, any other through http.ReadRequest), runs the http.Handler, and writes the responses
//     out together whenever it finds the queue empty. The request body is read from memory.
//
// At most one worker processes a given connection at a time, so with pipelining the responses come in the same
// order as the requests. Written out together, a response also waits for the Handlers of the requests already queued
// behind it, as on fasthttp: a client pipelining a slow request behind one gets both answers once the slow one is
// done (net/http writes every response out as soon as its Handler returns). A request, body included, must be
// received in full within ReadHeaderTimeout, idle connections are closed after IdleTimeout, and the Handler's
// execution time is not limited (see Options).
// The response body is buffered in full before being written out, so http.Flusher and http.Hijacker are not
// supported, except for the requests served by net/http (see below). Protocol upgrades (such as WebSocket) are done
// through Upgrade.
//
// The request handed to a Handler is pooled, which saves it its allocations: it is valid only within that ServeHTTP
// call. A Handler, and anything it starts, must not keep the request (its header, URL and body included) past
// ServeHTTP: neither in a field or a cache, nor in a goroutine that is still running when ServeHTTP returns, nor
// through a wrapper that goes on running it after ServeHTTP returns, such as http.TimeoutHandler. The request object
// and the buffers behind its body are reused by later requests, so using them afterwards reads another request's
// data (a data race). The strings of a request, header values included, are copies and stay valid. On the fnet path,
// Request.Context is backed by the pooled request and is canceled when the client connection closes, or once Upgrade
// or net/http has taken the connection over (see below); a response Write after the close returns an error.
//
// A request whose body is not buffered (larger than MaxBufferedBodyBytes, or framed in an unusual way) is served by
// net/http instead: the connection is detached from fnet and handed to an internal http.Server running the same
// Handler, which streams the body (a large file upload costs constant memory with Request.MultipartReader) and closes
// the connection after that request. So is a request whose Accept field mentions text/event-stream, as EventSource's
// always does: there the Handler can send Server-Sent Events through http.Flusher, and the request's context ends when
// the client leaves. Such a connection costs what it does on net/http, a goroutine and its buffers, and has net/http's
// timeouts by default, none once its header has been read: a Handler reading a streamed body bounds the wait with
// http.ResponseController.SetReadDeadline, as on net/http. A server-wide limit would cut Server-Sent Events and slow
// uploads alike.
//
// A client that does not take the responses is not answered further: once fnet's send buffer holds more than 64KB (see
// fnet.Conn.Writable) the worker stops until the client has taken them, within IdleTimeout, so the queued requests of a
// client that pipelines without reading cost memory only as requests.
package fhttp

import (
	"crypto/tls"
	"errors"
	"net"
	"net/http"
	"time"

	"github.com/linfeip/fnet"
	"github.com/linfeip/fnet/ftls"
	"github.com/linfeip/fnet/internal/units"
	"github.com/linfeip/fnet/taskpool"
)

// maxLimit caps MaxHeaderBytes and MaxBufferedBodyBytes, far above anything buffered in practice, so that the offsets
// within a buffered message, header and body together, fit an int32 (see framer).
const maxLimit = 1<<30 - 1

// Options are the HTTP server parameters; the zero value is the default configuration.
type Options struct {
	// MaxHeaderBytes is the maximum number of bytes of the request line and headers; when <=0 it is 16KB, room for
	// the 8000-byte request lines RFC 9112 recommends supporting and the usual fields. A request whose request line is
	// longer gets 414 (URI Too Long); one whose header is longer, or has more than 500 field lines (net/http's limit),
	// gets 431 (Request Header Fields Too Large).
	MaxHeaderBytes int
	// MaxBufferedBodyBytes is the largest request body that is received in full before the Handler runs; when <=0
	// it is 1MB, and a chunked body counts with its framing. Such a request costs no more than one without a body: it
	// stays on fnet and its body is read from memory. A request with a larger body is served by net/http (see the
	// package documentation).
	MaxBufferedBodyBytes int
	// ReadHeaderTimeout is the maximum time from the first byte of a request until the request header, and a body
	// of up to MaxBufferedBodyBytes, are received in full; data arriving in the meantime does not extend it. When 0
	// it is 10s, when <0 it is unlimited. On timeout the connection is closed immediately.
	ReadHeaderTimeout time.Duration
	// IdleTimeout is the maximum time to wait for the next request while no request is being received or processed
	// (including on a newly established connection); when 0 it is 120s, when <0 it is unlimited.
	// The Handler's execution time is not limited.
	IdleTimeout time.Duration
	// Engine holds the parameters of the underlying fnet engine. Its Executor runs only the connections' own tasks
	// (reading and splitting the requests), which never block; the Handlers run on Executor.
	Engine fnet.Options
	// Executor runs the Handlers: it is called when a connection has requests queued and no such task of it is
	// running, with a task that processes the connection's queue and ends once the queue is empty. The key is the
	// connection's fnet.Conn.TaskKey, so on Linux/macOS, with both executors left at their default, a Handler usually
	// runs right after the connection's own task, on the same goroutine and CPU. It may be called from any
	// goroutine, must not block, must run the task asynchronously (not directly on the caller's stack) and must not
	// drop it; a Handler panic is recovered by fhttp. Unlike the connections' tasks the Handlers may block, so it has
	// to cope with tasks that block: one running them on a few goroutines would stall every connection behind a slow
	// Handler. When nil it is the SubmitTo of taskpool.DefaultTaskPool, which starts or wakes another worker for the
	// tasks queued behind a blocked one. Unlike Engine.Executor it is used on every platform.
	Executor func(key int, task func())
	// TLSConfig, when non-nil, has the server serve HTTPS (see package ftls), the handshake having to complete within
	// ReadHeaderTimeout. fhttp speaks HTTP/1.1 only, so h2 is taken out of the protocols offered through ALPN.
	TLSConfig *tls.Config
}

func (o Options) withDefaults() Options {
	if o.MaxHeaderBytes <= 0 {
		o.MaxHeaderBytes = 16 * units.KB
	}
	if o.MaxBufferedBodyBytes <= 0 {
		o.MaxBufferedBodyBytes = units.MB
	}
	o.MaxHeaderBytes, o.MaxBufferedBodyBytes = min(o.MaxHeaderBytes, maxLimit), min(o.MaxBufferedBodyBytes, maxLimit)
	if o.ReadHeaderTimeout == 0 {
		o.ReadHeaderTimeout = 10 * time.Second
	}
	if o.IdleTimeout == 0 {
		o.IdleTimeout = 120 * time.Second
	}
	if o.Executor == nil {
		o.Executor = taskpool.DefaultTaskPool.SubmitTo
	}
	return o
}

// Server is an HTTP server.
type Server struct {
	handler  http.Handler
	opts     Options
	engine   *fnet.Server
	streams  *http.Server     // serves the handed-off connections, see conn.handoff
	handoffs *handoffListener // what streams accepts from
}

// NewServer creates an HTTP server on addr; when handler is nil, http.DefaultServeMux is used. The Handler must not
// keep the request past ServeHTTP: it is pooled and reused (see the package documentation). To listen on several
// addresses with the one server, see NewServerAddrs.
func NewServer(addr string, handler http.Handler, opts Options) (*Server, error) {
	return NewServerAddrs([]string{addr}, handler, opts)
}

// NewServerAddrs creates an HTTP server on each of addrs; when handler is nil, http.DefaultServeMux is used. At
// least one address is required, and all of them share the one underlying fnet engine (see fnet.NewServerAddrs).
// The Handler must not keep the request past ServeHTTP: it is pooled and reused (see the package documentation).
func NewServerAddrs(addrs []string, handler http.Handler, opts Options) (*Server, error) {
	if handler == nil {
		handler = http.DefaultServeMux
	}
	s := &Server{handler: handler, opts: opts.withDefaults()}
	var h fnet.Handler = engineHandler{s}
	if s.opts.TLSConfig != nil {
		h = ftls.NewHandler(h, http1Config(s.opts.TLSConfig), s.opts.ReadHeaderTimeout)
	}
	engine, err := fnet.NewServerAddrs(addrs, h, s.opts.Engine)
	if err != nil {
		return nil, err
	}
	s.engine = engine
	// The header of a handed-off request has already arrived, so of net/http's limits only MaxHeaderBytes matters.
	s.streams = &http.Server{Handler: handler, MaxHeaderBytes: s.opts.MaxHeaderBytes}
	s.streams.SetKeepAlivesEnabled(false) // one request per handed-off connection, the next one comes back to fnet
	if s.opts.TLSConfig != nil {
		serveTLSState(s.streams)
	}
	s.handoffs = newHandoffListener(engine.Addr())
	return s, nil
}

// http1Config returns a copy of config offering HTTP/1.1 through ALPN in place of h2, which a client would otherwise
// pick when config offers it (as autocert's does); its other protocols, such as acme-tls/1, are kept. So does the config
// GetConfigForClient returns, which crypto/tls uses in place of this one.
//
// Dynamic record sizing is off as well: it cuts the first responses of a connection into records of about 1KB, each of
// which ftls writes to the socket on its own, a syscall each, and a small one may wait for an ACK (Nagle's algorithm).
func http1Config(config *tls.Config) *tls.Config {
	config = config.Clone()
	protos := []string{"http/1.1"}
	for _, p := range config.NextProtos {
		if p != "h2" && p != "http/1.1" {
			protos = append(protos, p)
		}
	}
	config.NextProtos = protos
	config.DynamicRecordSizingDisabled = true
	if get := config.GetConfigForClient; get != nil {
		config.GetConfigForClient = func(hello *tls.ClientHelloInfo) (*tls.Config, error) {
			c, err := get(hello)
			if c == nil || err != nil {
				return c, err
			}
			return http1Config(c), nil
		}
	}
	return config
}

// Addr returns the first address actually being listened on; see Addrs for all of them.
func (s *Server) Addr() net.Addr { return s.engine.Addr() }

// Addrs returns all the addresses actually being listened on, in the order they were given to NewServerAddrs.
func (s *Server) Addrs() []net.Addr { return s.engine.Addrs() }

// Serve runs the server and blocks until Close is called (returning http.ErrServerClosed) or a fatal error occurs.
func (s *Server) Serve() error {
	go s.streams.Serve(s.handoffs)
	err := s.engine.Serve()
	if errors.Is(err, fnet.ErrServerClosed) {
		return http.ErrServerClosed
	}
	return err
}

// Close closes the listener and all connections. Handlers that are already running are not interrupted, but their
// responses cannot be written out.
func (s *Server) Close() error {
	s.handoffs.Close()
	s.streams.Close()
	return s.engine.Close()
}

// engineHandler feeds fnet's connection events into the HTTP processing flow; it runs on a goroutine of fnet's
// executor and must return quickly.
type engineHandler struct{ server *Server }

func (h engineHandler) OnOpen(c fnet.Conn) {
	hc := &conn{srv: h.server, connection: c}
	hc.serveTask = hc.serve
	c.SetContext(hc)
	hc.setDeadline() // a new connection counts as idle; there is no concurrent access yet, so mu need not be held
}

// OnWritable resumes answering a client that has taken the responses waiting for it, see conn.serve.
func (h engineHandler) OnWritable(c fnet.Conn) {
	if hc, ok := c.Context().(*conn); ok {
		hc.onWritable()
	}
}

// OnData and OnClose find the connection's state in its context: the HTTP conn, or, once a protocol has taken the
// connection over, that protocol itself (see conn.onData), which leaves the HTTP state out of the path of every
// message the protocol receives.
func (h engineHandler) OnData(c fnet.Conn, data []byte) int {
	ctx := c.Context()
	if hc, ok := ctx.(*conn); ok {
		return hc.onData(data)
	}
	return ctx.(Protocol).OnData(data)
}

func (h engineHandler) OnClose(c fnet.Conn, err error) {
	ctx := c.Context()
	if hc, ok := ctx.(*conn); ok {
		hc.onClose(err)
		return
	}
	ctx.(Protocol).OnClose(err)
}
