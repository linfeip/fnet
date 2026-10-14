// Package fhttp is an HTTP/1.x server built on the fnet engine.
//
// The division of labor:
//   - fnet's OnData callback (on a goroutine of the executor) only finds where each request message ends: the
//     header terminator "\r\n\r\n", then a small body, with Content-Length or in chunks. It copies the complete
//     messages into the connection's request queue without parsing them;
//   - a worker goroutine of Options.HandlerExecutor (taskpool.DefaultTaskPool by default; the Handlers may block, so
//     they are not run in OnData's task, which returns at once) takes the messages out in order, parses each (a
//     request in the plain forms of HTTP/1.x by fhttp itself, field for field the way the standard library's
//     http.ReadRequest would, any other through http.ReadRequest), runs the http.Handler, and writes the responses
//     out together whenever it finds the queue empty. The request body is read from memory.
//
// At most one worker processes a given connection at a time, so with pipelining the responses come in the same
// order as the requests. A request, body included, must be received in full within ReadHeaderTimeout, idle
// connections are closed after IdleTimeout, and the Handler's execution time is not limited (see Options).
// The response body is buffered in full before being written out, so http.Flusher and http.Hijacker are not
// supported. Protocol upgrades (such as WebSocket) are done through Upgrade.
//
// The request handed to a Handler is pooled, which saves it its allocations: it is valid only within that ServeHTTP
// call. A Handler, and anything it starts, must not keep the request (its header, URL and body included) past
// ServeHTTP: neither in a field or a cache, nor in a goroutine that is still running when ServeHTTP returns, nor
// through a wrapper that goes on running it after ServeHTTP returns, such as http.TimeoutHandler. The request object
// and the buffers behind its body are reused by later requests, so using them afterwards reads another request's
// data (a data race). The strings of a request, header values included, are copies and stay valid.
//
// A request whose body is not buffered (larger than MaxBufferedBodyBytes, or framed in an unusual way) is served by
// net/http instead: the connection is detached from fnet and handed to an internal http.Server running the same
// Handler, which streams the body (a large file upload costs constant memory with Request.MultipartReader) and closes
// the connection after that request.
package fhttp

import (
	"bufio"
	"bytes"
	"crypto/tls"
	"errors"
	"log/slog"
	"net"
	"net/http"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/linfeip/fnet"
	"github.com/linfeip/fnet/ftls"
	"github.com/linfeip/fnet/internal/bytepool"
	"github.com/linfeip/fnet/internal/units"
	"github.com/linfeip/fnet/taskpool"
)

// maxPipeline is the upper bound on requests queued for processing on a single connection; going beyond it is
// treated as abuse and the connection is closed.
const maxPipeline = 128

// maxLimit caps MaxHeaderBytes and MaxBufferedBodyBytes, far above anything buffered in practice, so that the offsets
// within a buffered message fit an int32 (see framer).
const maxLimit = 1 << 30

// maxRetainedQueueCapacity is the maximum capacity retained once the request queue has been drained: a connection
// pipelining a few requests at a time (16 is a common depth) needs no reallocation every time, a connection that sends
// them one by one keeps only one item, and the large capacity left behind by a pipelining burst is released, so a
// large number of connections do not hold on to memory for long.
const maxRetainedQueueCapacity = 16

// Options are the HTTP server parameters; the zero value is the default configuration.
type Options struct {
	// MaxHeaderBytes is the maximum number of bytes of the request line and headers; when <=0 it is
	// http.DefaultMaxHeaderBytes (1MB).
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
	// (reading and splitting the requests), which never block; the Handlers run on HandlerExecutor.
	Engine fnet.Options
	// HandlerExecutor runs the Handlers: it is called when a connection has requests queued and no such task of it is
	// running, with a task that processes the connection's queue and ends once the queue is empty. The key is random
	// (taskpool.RandomKey), so the tasks of a connection are not tied to one queue. It may be called from any
	// goroutine, must not block, must run the task asynchronously (not directly on the caller's stack) and must not
	// drop it; a Handler panic is recovered by fhttp. Unlike the connections' tasks the Handlers may block, so it has
	// to cope with tasks that block: one running them on a few goroutines would stall every connection behind a slow
	// Handler. When nil it is the SubmitTo of taskpool.DefaultTaskPool. Unlike Engine.Executor it is used on every
	// platform.
	HandlerExecutor func(key int, task func())
	// TLSConfig, when non-nil, has the server serve HTTPS (see package ftls), the handshake having to complete within
	// ReadHeaderTimeout. fhttp speaks HTTP/1.1 only, so h2 is taken out of the protocols offered through ALPN.
	TLSConfig *tls.Config
}

func (o Options) withDefaults() Options {
	if o.MaxHeaderBytes <= 0 {
		o.MaxHeaderBytes = http.DefaultMaxHeaderBytes
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
	if o.HandlerExecutor == nil {
		o.HandlerExecutor = taskpool.DefaultTaskPool.SubmitTo
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
// pick when config offers it (as autocert's does); its other protocols, such as acme-tls/1, are kept.
func http1Config(config *tls.Config) *tls.Config {
	config = config.Clone()
	protos := []string{"http/1.1"}
	for _, p := range config.NextProtos {
		if p != "h2" && p != "http/1.1" {
			protos = append(protos, p)
		}
	}
	config.NextProtos = protos
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

// request is one item in the request queue: either a complete request message, or a status code to reply with
// (100 Continue, or an error that closes the connection), or statusHandoff.
type request struct {
	msg     bytepool.Buffer
	header  int32 // the length of the message's request line and headers (see maxLimit)
	status  int16 // 0 for a message, otherwise the status code to reply with, or statusHandoff
	chunked bool  // the message's body is chunked
}

// statusHandoff is the queue item that hands the connection to net/http, see conn.handoff.
const statusHandoff = -1

// conn is the state of one HTTP connection.
type conn struct {
	srv        *Server
	connection fnet.Conn
	remoteAddr string       // produced on the first request, accessed only by the worker (workers of one connection run serially)
	last       *lastRequest // what parseRequest shares with the next request, made on the first one; accessed only by the worker
	serveTask  func()       // serve, bound once, so that handing it to a worker does not allocate each time

	framer framer // accessed only by fnet's callbacks (callbacks of one connection run serially)
	broken bool   // a protocol error has occurred, all subsequent data is discarded; accessed only by fnet's callbacks

	protocol atomic.Pointer[Protocol] // the protocol that takes over the connection after Upgrade switches; written only while holding mu

	mu      sync.Mutex
	queue   []request
	busy    bool // handed to a worker, which is responsible for processing the queue until it is empty
	closed  bool
	partial bool // the connection buffer holds an incomplete request; written only by fnet's callbacks
}

// setDeadline sets the deadline according to the connection's state and requires the caller to hold mu: while a
// request is being processed there is no limit; when there is an incomplete request, ReadHeaderTimeout applies
// starting now; otherwise the connection is idle and IdleTimeout applies. It is called only when the state changes, so
// arriving data does not extend the deadline. After a protocol switch the deadline is managed by the new protocol.
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

// onData splits out the complete request messages and enqueues them all at once, returning the number of consumed
// bytes.
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
	c.mu.Lock()
	queued := len(c.queue)
	for consumed < len(data) {
		m, expectContinue, err := c.framer.next(data[consumed:], &c.srv.opts)
		if err == errStreamBody {
			// The rest of the connection goes to net/http once the earlier requests are answered. Reading stops, so the
			// request stays unconsumed for it.
			c.connection.PauseRead()
			c.push(nil, message{}, statusHandoff)
			break
		}
		if err != nil {
			// framer only ever returns errHeaderTooLarge here. The error response also goes out through the queue,
			// which guarantees it comes after the responses of earlier requests.
			c.broken = true
			c.push(nil, message{}, http.StatusRequestHeaderFieldsTooLarge)
			consumed = len(data)
			break
		}
		if m.size == 0 {
			// The header is complete and the client waits for 100 Continue before sending the body. It goes through
			// the queue as well, so it is not written in the middle of earlier responses.
			if expectContinue {
				c.push(nil, message{}, http.StatusContinue)
			}
			break
		}
		c.push(data[consumed:consumed+m.size], m, 0)
		consumed += m.size
	}
	// Too much is queued, or the protocol was switched while splitting: this data was sent by a client that did not wait
	// for the upgrade response, so it must not be executed as HTTP.
	overflow := len(c.queue) > maxPipeline || c.protocol.Load() != nil
	submit := !overflow && len(c.queue) > queued && !c.busy
	if overflow {
		c.broken = true
		consumed = len(data)
	} else if submit {
		c.busy = true
	}
	partial := consumed < len(data)
	if submit || partial != c.partial {
		c.partial = partial
		c.setDeadline()
	}
	c.mu.Unlock()
	if overflow {
		c.connection.Close()
	} else if submit {
		c.srv.opts.HandlerExecutor(taskpool.RandomKey(), c.serveTask)
	}
	return consumed
}

// push puts a copy of the request message data framed as m (or, when data is nil, the status code status) into the
// queue; the caller holds mu.
func (c *conn) push(data []byte, m message, status int16) {
	r := request{header: int32(m.header), status: status, chunked: m.chunked}
	r.msg.Append(data) // serve returns the buffer on dequeue, onClose when the queue is dropped
	c.queue = append(c.queue, r)
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
// closed. The responses are collected and written out together whenever the queue is found empty, so pipelined
// requests are answered in one write.
func (c *conn) serve() {
	w := responsePool.Get().(*response)
	w.conn = c
	defer w.release()
	for {
		c.mu.Lock()
		if !c.closed && len(c.queue) == 0 && w.out.Len() > 0 {
			// Write out while still busy, so no other worker writes in between; then look again, as requests may have
			// been queued meanwhile.
			c.mu.Unlock()
			w.flush()
			continue
		}
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

		keepAlive := c.handle(w, r)
		r.msg.Release() // the dequeued message is owned by serve, handle only borrows it
		if !keepAlive {
			w.flush()
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

// unread returns the number of bytes of the message not read yet.
func (rd *reader) unread() int { return rd.bufferedReader.Buffered() + rd.bytesReader.Len() }

func (rd *reader) release() {
	rd.bytesReader.Reset(nil)
	readerPool.Put(rd)
}

// continueResponse is the interim response that asks the client to send the request body.
var continueResponse = []byte("HTTP/1.1 100 Continue\r\n\r\n")

// handle parses and processes one request, its response going into w's output, and reports whether to keep the
// connection alive.
func (c *conn) handle(w *response, r request) bool {
	switch r.status {
	case 0:
	case http.StatusContinue:
		w.out.Append(continueResponse)
		return true
	case statusHandoff:
		w.flush()
		c.handoff()
		return false // closing the detached connection does nothing
	default:
		w.writeError(int(r.status))
		return false
	}

	msg := r.msg.Bytes()
	m := message{size: len(msg), header: int(r.header), chunked: r.chunked}
	var req *http.Request
	if c.last == nil {
		c.last = new(lastRequest)
	}
	parsed := parseRequest(msg, m, c.last)
	if parsed != nil {
		req = &parsed.request
	} else {
		rd := readerPool.Get().(*reader)
		defer rd.release()
		rd.bytesReader.Reset(msg)
		rd.bufferedReader.Reset(&rd.bytesReader)
		var err error
		if req, err = http.ReadRequest(rd.bufferedReader); err != nil {
			w.writeError(http.StatusBadRequest)
			return false
		}
		// Same as the net/http server: only HTTP/1.x is supported, and it is rejected before Host is checked.
		if req.ProtoMajor != 1 {
			w.writeError(http.StatusHTTPVersionNotSupported)
			return false
		}
		// The message ends where framer said, so the standard library must have read the same header and must frame the
		// body the same way: a chunked body (whose form framer checked) as chunked, any other as exactly the rest of the
		// message. Otherwise the two disagree on the framing: a field framer did not recognize (see bodyLength), or an
		// empty line made of a bare LF, which the standard library accepts as a line break, ending the headers early.
		// The remainder must not be silently discarded or executed (an upstream proxy may frame it differently,
		// misaligning the responses), so it is handled as a bad request.
		body := m.size - m.header
		if rd.unread() != body || m.chunked != (req.TransferEncoding != nil) || !m.chunked && int64(body) != req.ContentLength {
			w.writeError(http.StatusBadRequest)
			return false
		}
	}
	// Same as the net/http server: an HTTP/1.1 request must carry Host (multiple Hosts are already rejected), and an
	// invalid Host is rejected: neither parser validates it, while Handlers often use r.Host to build URLs or redirect
	// addresses.
	if req.Host == "" && req.ProtoAtLeast(1, 1) && req.Method != http.MethodConnect || !validHost(req.Host) {
		w.writeError(http.StatusBadRequest)
		return false
	}
	if c.remoteAddr == "" {
		c.remoteAddr = c.connection.RemoteAddr().String()
	}
	req.RemoteAddr = c.remoteAddr
	if tc, ok := c.connection.(*ftls.Conn); ok {
		state := tc.ConnectionState()
		req.TLS = &state
	}

	w.start(req)
	reqBody := req.Body
	ok := c.serveHTTP(w, req)
	// Same as the net/http server: close the body, so a Handler that kept it cannot read the message buffer or the
	// pooled reader once they are reused, and remove the files ParseMultipartForm stored on disk.
	reqBody.Close()
	if req.MultipartForm != nil {
		req.MultipartForm.RemoveAll()
	}
	keepAlive := false
	switch {
	case !ok:
	case w.upgraded: // Upgrade already wrote the response: on success the connection goes to the new protocol, on failure it closes
		keepAlive = c.protocol.Load() != nil
	default:
		keepAlive = w.finish()
	}
	if parsed != nil {
		parsed.release() // the Handler has returned, see the package documentation
	}
	return keepAlive
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
