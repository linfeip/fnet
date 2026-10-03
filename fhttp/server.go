// Package fhttp is an HTTP/1.x server built on the fnet engine.
//
// The division of labor:
//   - fnet's OnData callback (on a goroutine of the executor) only looks for the request header terminator
//     "\r\n\r\n" and copies the request line and headers into the connection's request queue, without parsing
//     anything;
//   - a worker goroutine takes the message out, hands it to the standard library's http.ReadRequest for parsing,
//     then runs the http.Handler and writes the response back.
//
// At most one worker processes a given connection at a time, so with pipelining the responses come in the same
// order as the requests. Request headers must be received in full within ReadHeaderTimeout, idle connections are
// closed after IdleTimeout, and the Handler's execution time is not limited (see Options).
// Currently only requests without a body are supported (a request with a body gets a 413 and the connection is
// closed); the response body is buffered in full before being written out, so http.Flusher and http.Hijacker are
// not supported. Protocol upgrades (such as WebSocket) are done through Upgrade.
package fhttp

import (
	"bufio"
	"bytes"
	"errors"
	"log/slog"
	"net"
	"net/http"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/linfeip/fnet"
	"github.com/linfeip/fnet/internal/bytepool"
	"github.com/linfeip/fnet/internal/units"
)

// maxPipeline is the upper bound on requests queued for processing on a single connection; going beyond it is
// treated as abuse and the connection is closed.
const maxPipeline = 128

// maxRetainedQueueCapacity is the maximum capacity retained once the request queue has been drained: the common
// case of a few requests needs no reallocation every time, while the large capacity left behind by a pipelining
// burst is released, so a large number of connections do not hold on to memory for long.
const maxRetainedQueueCapacity = 4

// Options are the HTTP server parameters; the zero value is the default configuration.
type Options struct {
	// MaxHeaderBytes is the maximum number of bytes of the request line and headers; when <=0 it is
	// http.DefaultMaxHeaderBytes (1MB).
	MaxHeaderBytes int
	// ReadHeaderTimeout is the maximum time from the first byte of a request until the request header is received
	// in full; data arriving in the meantime does not extend it. When 0 it is 10s, when <0 it is unlimited. On
	// timeout the connection is closed immediately.
	ReadHeaderTimeout time.Duration
	// IdleTimeout is the maximum time to wait for the next request while no request is being received or processed
	// (including on a newly established connection); when 0 it is 120s, when <0 it is unlimited.
	// The Handler's execution time is not limited.
	IdleTimeout time.Duration
	// Engine holds the parameters of the underlying fnet engine.
	Engine fnet.Options
}

func (o Options) withDefaults() Options {
	if o.MaxHeaderBytes <= 0 {
		o.MaxHeaderBytes = http.DefaultMaxHeaderBytes
	}
	if o.ReadHeaderTimeout == 0 {
		o.ReadHeaderTimeout = 10 * time.Second
	}
	if o.IdleTimeout == 0 {
		o.IdleTimeout = 120 * time.Second
	}
	return o
}

// Server is an HTTP server.
type Server struct {
	handler http.Handler
	opts    Options
	engine  *fnet.Server
}

// NewServer creates an HTTP server on addr; when handler is nil, http.DefaultServeMux is used. To listen on
// several addresses with the one server, see NewServerAddrs.
func NewServer(addr string, handler http.Handler, opts Options) (*Server, error) {
	return NewServerAddrs([]string{addr}, handler, opts)
}

// NewServerAddrs creates an HTTP server on each of addrs; when handler is nil, http.DefaultServeMux is used. At
// least one address is required, and all of them share the one underlying fnet engine (see fnet.NewServerAddrs).
func NewServerAddrs(addrs []string, handler http.Handler, opts Options) (*Server, error) {
	if handler == nil {
		handler = http.DefaultServeMux
	}
	s := &Server{handler: handler, opts: opts.withDefaults()}
	engine, err := fnet.NewServerAddrs(addrs, engineHandler{s}, s.opts.Engine)
	if err != nil {
		return nil, err
	}
	s.engine = engine
	return s, nil
}

// ListenAndServe starts an HTTP server on addr and blocks until an error occurs.
func ListenAndServe(addr string, handler http.Handler) error {
	s, err := NewServer(addr, handler, Options{})
	if err != nil {
		return err
	}
	return s.Serve()
}

// Addr returns the first address actually being listened on; see Addrs for all of them.
func (s *Server) Addr() net.Addr { return s.engine.Addr() }

// Addrs returns all the addresses actually being listened on, in the order they were given to NewServerAddrs.
func (s *Server) Addrs() []net.Addr { return s.engine.Addrs() }

// Serve runs the server and blocks until Close is called (returning http.ErrServerClosed) or a fatal error occurs.
func (s *Server) Serve() error {
	err := s.engine.Serve()
	if errors.Is(err, fnet.ErrServerClosed) {
		return http.ErrServerClosed
	}
	return err
}

// Close closes the listener and all connections. Handlers that are already running are not interrupted, but their
// responses cannot be written out.
func (s *Server) Close() error {
	return s.engine.Close()
}

// engineHandler feeds fnet's connection events into the HTTP processing flow; it runs on a goroutine of fnet's
// executor and must return quickly.
type engineHandler struct{ server *Server }

func (h engineHandler) OnOpen(c fnet.Conn) {
	hc := &conn{srv: h.server, connection: c, framer: framer{maxHeaderBytes: h.server.opts.MaxHeaderBytes}}
	c.SetContext(hc)
	hc.setDeadline() // a new connection counts as idle; there is no concurrent access yet, so mu need not be held
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

// request is one item in the request queue: either a complete request message, or an error status code to reply
// with.
type request struct {
	msg    bytepool.Buffer
	status int
}

// conn is the state of one HTTP connection.
type conn struct {
	srv        *Server
	connection fnet.Conn
	remoteAddr string // produced on the first request, accessed only by the worker (workers of one connection run serially)

	framer framer // accessed only by fnet's callbacks (callbacks of one connection run serially)
	broken bool   // a protocol error has occurred, all subsequent data is discarded; accessed only by fnet's callbacks

	protocol atomic.Pointer[Protocol] // the protocol that takes over the connection after Upgrade switches; written only while holding mu

	mu      sync.Mutex
	queue   []request
	busy    bool // handed to a worker, which is responsible for processing the queue until it is empty
	closed  bool
	partial bool // the connection buffer holds an incomplete request header; written only by fnet's callbacks
}

// setDeadline sets the deadline according to the connection's state and requires the caller to hold mu: while a
// request is being processed there is no limit; when there is an incomplete request header, ReadHeaderTimeout
// applies starting now; otherwise the connection is idle and IdleTimeout applies. It is called only when the state
// changes, so arriving data does not extend the deadline. After a protocol switch the deadline is managed by the
// new protocol.
func (c *conn) setDeadline() {
	if c.protocol.Load() != nil {
		return
	}
	var timeout time.Duration
	switch {
	case c.busy:
	case c.partial:
		timeout = c.srv.opts.ReadHeaderTimeout
	default:
		timeout = c.srv.opts.IdleTimeout
	}
	c.connection.SetDeadline(deadlineAfter(timeout))
}

// deadlineAfter returns the instant timeout from now; timeout<=0 means no limit and the zero value is returned.
func deadlineAfter(timeout time.Duration) time.Time {
	if timeout <= 0 {
		return time.Time{}
	}
	return time.Now().Add(timeout)
}

// setPartial records, inside fnet's callbacks, whether the connection buffer holds an incomplete request header,
// and updates the deadline when that changes.
func (c *conn) setPartial(partial bool) {
	if partial == c.partial { // only fnet's callbacks write partial, so it can be read here without a lock
		return
	}
	c.mu.Lock()
	c.partial = partial
	c.setDeadline()
	c.mu.Unlock()
}

// onData splits out complete request headers and enqueues them, returning the number of consumed bytes.
func (c *conn) onData(data []byte) int {
	if p := c.protocol.Load(); p != nil {
		// The protocol takes over the context from here on, which a callback may set. Nothing of the HTTP state is
		// needed any more: Upgrade released the queue, and closed only stops an upgrade that has already happened.
		c.connection.SetContext(*p)
		return (*p).OnData(data)
	}
	if c.broken {
		return len(data)
	}
	consumed := 0
	for consumed < len(data) {
		n, err := c.framer.next(data[consumed:])
		if err != nil {
			// framer only ever returns errHeaderTooLarge. The error response also goes out through the queue, which
			// guarantees it comes after the responses of earlier requests.
			c.broken = true
			c.push(nil, http.StatusRequestHeaderFieldsTooLarge)
			return len(data)
		}
		if n == 0 {
			break
		}
		if !c.push(data[consumed:consumed+n], 0) {
			c.broken = true
			return len(data)
		}
		consumed += n
	}
	c.setPartial(consumed < len(data))
	return consumed
}

// push puts a copy of the request message data (or, when data is nil, the error status code status) into the queue
// and hands it to a worker when needed; it closes the connection and returns false when too much is queued or the
// protocol has already been switched.
func (c *conn) push(data []byte, status int) bool {
	c.mu.Lock()
	// The protocol was switched while splitting: this data was sent by a client that did not wait for the upgrade
	// response, so it must not be executed as HTTP.
	if len(c.queue) >= maxPipeline || c.protocol.Load() != nil {
		c.mu.Unlock()
		c.connection.Close()
		return false
	}
	r := request{status: status}
	r.msg.Append(data) // borrow the buffer only after the checks pass; serve returns it on dequeue, onClose when the queue is dropped
	c.queue = append(c.queue, r)
	submit := !c.busy
	c.busy = true
	if submit {
		c.setDeadline() // processing of a request begins: no limit
	}
	c.mu.Unlock()
	if submit {
		go c.serve()
	}
	return true
}

func (c *conn) onClose(err error) {
	c.mu.Lock()
	c.closed = true
	for i := range c.queue {
		c.queue[i].msg.Release()
	}
	c.queue = nil
	c.mu.Unlock()
	// Once closed is set, Upgrade no longer switches the protocol, so what is read here is the final result.
	if p := c.protocol.Load(); p != nil {
		(*p).OnClose(err)
	}
}

// serve processes the queued requests in order on a worker, until the queue is empty or the connection needs to be
// closed.
func (c *conn) serve() {
	for {
		c.mu.Lock()
		if c.closed || len(c.queue) == 0 {
			c.busy = false
			c.setDeadline()
			c.mu.Unlock()
			return
		}
		r := c.queue[0]
		n := copy(c.queue, c.queue[1:])
		c.queue[n] = request{}
		c.queue = c.queue[:n]
		if n == 0 && cap(c.queue) > maxRetainedQueueCapacity {
			c.queue = nil
		}
		c.mu.Unlock()

		keepAlive := c.handle(r)
		r.msg.Release() // the dequeued message is owned by serve, handle only borrows it
		if !keepAlive {
			c.connection.Close() // busy stays true: the connection is about to close, so no new processing is scheduled
			return
		}
	}
}

// reader wraps a whole in-memory message into the *bufio.Reader that http.ReadRequest needs.
type reader struct {
	bufferedReader *bufio.Reader
	bytesReader    bytes.Reader
}

var readerPool = sync.Pool{New: func() any {
	rd := new(reader)
	rd.bufferedReader = bufio.NewReaderSize(&rd.bytesReader, 4*units.KB)
	return rd
}}

// handle parses and processes one request, and reports whether to keep the connection alive.
func (c *conn) handle(r request) bool {
	if r.status != 0 {
		c.writeError(r.status)
		return false
	}
	rd := readerPool.Get().(*reader)
	rd.bytesReader.Reset(r.msg.Bytes())
	rd.bufferedReader.Reset(&rd.bytesReader)
	defer func() {
		rd.bytesReader.Reset(nil)
		readerPool.Put(rd)
	}()

	req, err := http.ReadRequest(rd.bufferedReader)
	if err != nil {
		c.writeError(http.StatusBadRequest)
		return false
	}
	// The message is cut at the first "\r\n\r\n", but the standard library also accepts a bare LF as a line break:
	// when the message contains an empty line made of "\n\n", parsing ends early. The remainder must not be
	// silently discarded (an upstream proxy may treat it as the next request, misaligning the responses), so it is
	// handled as a bad request.
	if rd.bufferedReader.Buffered() > 0 || rd.bytesReader.Len() > 0 {
		c.writeError(http.StatusBadRequest)
		return false
	}
	// Same as the net/http server: only HTTP/1.x is supported, and it is rejected before Host is checked.
	if req.ProtoMajor != 1 {
		c.writeError(http.StatusHTTPVersionNotSupported)
		return false
	}
	// Same as the net/http server: an HTTP/1.1 request must carry Host (multiple Hosts are already rejected by
	// ReadRequest).
	if req.Host == "" && req.ProtoAtLeast(1, 1) && req.Method != http.MethodConnect {
		c.writeError(http.StatusBadRequest)
		return false
	}
	// Same as the net/http server: reject an invalid Host. The standard library's ReadRequest does not validate it,
	// while Handlers often use r.Host to build URLs or redirect addresses.
	if !validHost(req.Host) {
		c.writeError(http.StatusBadRequest)
		return false
	}
	// framer only cuts at the header terminator, so the request body is not part of the message; requests with a
	// body are not supported yet (ContentLength is -1 for chunked). They must be rejected and the connection closed:
	// the body has already been split and enqueued as subsequent requests, and closing keeps them from being
	// executed — otherwise this would be request smuggling.
	if req.ContentLength != 0 {
		c.writeError(http.StatusRequestEntityTooLarge)
		return false
	}
	if c.remoteAddr == "" {
		c.remoteAddr = c.connection.RemoteAddr().String()
	}
	req.RemoteAddr = c.remoteAddr

	w := newResponse(c, req)
	defer w.release()
	if !c.serveHTTP(w, req) {
		return false
	}
	if w.upgraded { // Upgrade already wrote the response: on success the connection goes to the new protocol, on failure it closes
		return c.protocol.Load() != nil
	}
	return w.finish()
}

// serveHTTP runs the Handler; when the Handler panics it logs the panic (except for http.ErrAbortHandler) and
// returns false.
func (c *conn) serveHTTP(w *response, req *http.Request) (ok bool) {
	defer func() {
		if err := recover(); err != nil {
			if err != http.ErrAbortHandler {
				buf := make([]byte, 64*units.KB)
				slog.Error("fhttp: panic serving request", "remoteAddr", c.remoteAddr, "panic", err,
					"stack", string(buf[:runtime.Stack(buf, false)]))
			}
			ok = false
		}
	}()
	c.srv.handler.ServeHTTP(w, req)
	return true
}

// validHost reports whether the Host value contains only characters allowed in host names, IPv4 and IPv6 literals
// (including the zone identifier) and ports, matching the net/http server's validation.
func validHost(host string) bool {
	for i := 0; i < len(host); i++ {
		switch c := host[i]; {
		case c >= '0' && c <= '9', c >= 'a' && c <= 'z', c >= 'A' && c <= 'Z':
		case strings.IndexByte("!$%&'()*+,-.:;=[]_~", c) >= 0:
		default:
			return false
		}
	}
	return true
}

func (c *conn) writeError(code int) {
	text := strconv.Itoa(code) + " " + http.StatusText(code)
	c.connection.Write([]byte("HTTP/1.1 " + text + "\r\nContent-Type: text/plain; charset=utf-8\r\nConnection: close\r\n" +
		"Content-Length: " + strconv.Itoa(len(text)) + "\r\n\r\n" + text))
}
