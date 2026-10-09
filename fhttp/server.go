// Package fhttp is an HTTP/1.x server built on the fnet engine.
//
// The division of labor:
//   - fnet's OnData callback (on a goroutine of the executor) only finds where each request message ends: the
//     header terminator "\r\n\r\n", then the Content-Length bytes of a small body. It copies the whole message
//     into the connection's request queue without parsing it;
//   - a worker goroutine takes the message out, hands it to the standard library's http.ReadRequest for parsing
//     (the request body is the standard library's own reader over the buffered bytes), then runs the http.Handler
//     and writes the response back.
//
// At most one worker processes a given connection at a time, so with pipelining the responses come in the same
// order as the requests. A request, small body included, must be received in full within ReadHeaderTimeout, idle
// connections are closed after IdleTimeout, and the Handler's execution time is not limited (see Options).
// The response body is buffered in full before being written out, so http.Flusher and http.Hijacker are not
// supported. Protocol upgrades (such as WebSocket) are done through Upgrade.
//
// A request whose body is not buffered (larger than MaxBufferedBodyBytes, chunked, or framed in an unusual way) is
// served by net/http instead: the connection is detached from fnet and handed to an internal http.Server running the
// same Handler, which streams the body (a large file upload costs constant memory with Request.MultipartReader) and
// closes the connection after that request.
package fhttp

import (
	"bufio"
	"bytes"
	"errors"
	"log/slog"
	"net"
	"net/http"
	"net/url"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/linfeip/fnet"
	"github.com/linfeip/fnet/internal/bytepool"
	"github.com/linfeip/fnet/internal/units"
	"github.com/linfeip/fnet/taskpool"
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
	// MaxBufferedBodyBytes is the largest request body that is received in full before the Handler runs; when <=0
	// it is 1MB. Such a request costs no more than one without a body: it stays on fnet and its body is read from
	// memory. A request with a larger body is served by net/http (see the package documentation).
	MaxBufferedBodyBytes int
	// ReadHeaderTimeout is the maximum time from the first byte of a request until the request header, and a body
	// of up to MaxBufferedBodyBytes, are received in full; data arriving in the meantime does not extend it. When 0
	// it is 10s, when <0 it is unlimited. On timeout the connection is closed immediately.
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
	if o.MaxBufferedBodyBytes <= 0 {
		o.MaxBufferedBodyBytes = units.MB
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
	handler  http.Handler
	opts     Options
	engine   *fnet.Server
	streams  *http.Server     // serves the handed-off connections, see conn.handoff
	handoffs *handoffListener // what streams accepts from
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
	// The header of a handed-off request has already arrived, so of net/http's limits only MaxHeaderBytes matters.
	s.streams = &http.Server{Handler: handler, MaxHeaderBytes: s.opts.MaxHeaderBytes}
	s.streams.SetKeepAlivesEnabled(false) // one request per handed-off connection, the next one comes back to fnet
	s.handoffs = newHandoffListener(engine.Addr())
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
	msg    bytepool.Buffer
	status int
}

// statusHandoff is the queue item that hands the connection to net/http, see conn.handoff.
const statusHandoff = -1

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

// onData splits out complete request messages and enqueues them, returning the number of consumed bytes.
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
		n, expectContinue, err := c.framer.next(data[consumed:], &c.srv.opts)
		if err == errStreamBody {
			// The rest of the connection goes to net/http once the earlier requests are answered. Reading stops, so
			// the request stays unconsumed for it.
			c.connection.PauseRead()
			if !c.push(nil, statusHandoff) {
				c.broken = true
				return len(data)
			}
			return consumed
		}
		if err != nil {
			// framer only ever returns errHeaderTooLarge here. The error response also goes out through the queue,
			// which guarantees it comes after the responses of earlier requests.
			c.broken = true
			c.push(nil, http.StatusRequestHeaderFieldsTooLarge)
			return len(data)
		}
		if n == 0 {
			// The header is complete and the client waits for 100 Continue before sending the body. It goes through
			// the queue as well, so it is not written in the middle of earlier responses.
			if expectContinue {
				if !c.push(nil, http.StatusContinue) {
					c.broken = true
					return len(data)
				}
			}
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
		taskpool.DefaultTaskPool.Submit(c.serve)
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

// continueResponse is the interim response that asks the client to send the request body.
var continueResponse = []byte("HTTP/1.1 100 Continue\r\n\r\n")

// fastUpgradeRequest pools the http.Request, url.URL and Header structures for WebSocket upgrade requests,
// avoiding the allocations of http.ReadRequest and its large MIMEHeader map.
type fastUpgradeRequest struct {
	request http.Request
	url     url.URL
	header  http.Header
	slots   [12][1]string
}

var fastUpgradePool = sync.Pool{
	New: func() any {
		f := &fastUpgradeRequest{header: make(http.Header, 8)}
		f.request.URL = &f.url
		f.request.Header = f.header
		return f
	},
}

func releaseFastUpgradeRequest(f *fastUpgradeRequest) {
	for key := range f.header {
		delete(f.header, key)
	}
	f.url = url.URL{}
	f.request = http.Request{Header: f.header, URL: &f.url}
	fastUpgradePool.Put(f)
}

func parseFastUpgradeRequest(msg []byte) *fastUpgradeRequest {
	end := bytes.Index(msg, headerTerminator)
	if end < 0 || end+len(headerTerminator) != len(msg) {
		return nil
	}
	firstLineEnd := bytes.Index(msg, []byte("\r\n"))
	if firstLineEnd <= 0 || firstLineEnd+2 >= end {
		return nil
	}
	firstLine := msg[:firstLineEnd]
	if !bytes.HasPrefix(firstLine, []byte("GET ")) || !bytes.HasSuffix(firstLine, []byte(" HTTP/1.1")) {
		return nil
	}
	uriBytes := firstLine[4 : len(firstLine)-9]
	if len(uriBytes) == 0 || uriBytes[0] != '/' || bytes.ContainsAny(uriBytes, " #%") {
		return nil
	}
	pathBytes := uriBytes
	var rawQuery string
	if i := bytes.IndexByte(uriBytes, '?'); i >= 0 {
		pathBytes = uriBytes[:i]
		rawQuery = string(uriBytes[i+1:])
	}
	path := string(pathBytes)

	f := fastUpgradePool.Get().(*fastUpgradeRequest)
	req := &f.request
	req.Method = http.MethodGet
	req.Proto = "HTTP/1.1"
	req.ProtoMajor = 1
	req.ProtoMinor = 1
	if rawQuery == "" {
		req.RequestURI = path
	} else {
		req.RequestURI = string(uriBytes)
	}
	f.url.Path = path
	f.url.RawQuery = rawQuery
	req.Body = http.NoBody
	req.ContentLength = 0
	req.Close = false

	headers := msg[firstLineEnd+2 : end]
	slotIndex := 0
	hasUpgrade := false
	hasConnection := false
	hasVersion := false
	var host string

	for len(headers) > 0 {
		lineEnd := bytes.Index(headers, []byte("\r\n"))
		var field []byte
		if lineEnd < 0 {
			field, headers = headers, nil
		} else {
			field, headers = headers[:lineEnd], headers[lineEnd+2:]
		}
		if len(field) == 0 {
			break
		}
		colon := bytes.IndexByte(field, ':')
		if colon <= 0 || slotIndex >= len(f.slots) {
			releaseFastUpgradeRequest(f)
			return nil
		}
		name := field[:colon]
		value := bytes.TrimSpace(field[colon+1:])

		var canonicalKey, val string
		switch {
		case bytes.EqualFold(name, []byte("Host")):
			if host != "" || len(value) == 0 || !validHostBytes(value) {
				releaseFastUpgradeRequest(f)
				return nil
			}
			host = string(value)
			req.Host = host
			continue
		case bytes.EqualFold(name, []byte("Upgrade")):
			canonicalKey = "Upgrade"
			hasUpgrade = bytes.EqualFold(value, []byte("websocket"))
			if hasUpgrade {
				val = "websocket"
			} else {
				val = string(value)
			}
		case bytes.EqualFold(name, []byte("Connection")):
			canonicalKey = "Connection"
			hasConnection = bytesHasToken(value, []byte("upgrade"))
			if bytes.EqualFold(value, []byte("Upgrade")) {
				val = "Upgrade"
			} else {
				val = string(value)
			}
		case bytes.EqualFold(name, []byte("Sec-WebSocket-Version")):
			canonicalKey = "Sec-Websocket-Version"
			hasVersion = bytes.Equal(value, []byte("13"))
			if hasVersion {
				val = "13"
			} else {
				val = string(value)
			}
		case bytes.EqualFold(name, []byte("Sec-WebSocket-Key")):
			if len(value) != 24 {
				releaseFastUpgradeRequest(f)
				return nil
			}
			canonicalKey = "Sec-Websocket-Key"
			val = string(value)
		case bytes.EqualFold(name, []byte("Sec-WebSocket-Protocol")):
			canonicalKey = "Sec-Websocket-Protocol"
			val = string(value)
		case bytes.EqualFold(name, []byte("Sec-WebSocket-Extensions")):
			canonicalKey = "Sec-Websocket-Extensions"
			val = string(value)
		case bytes.EqualFold(name, []byte("Origin")):
			canonicalKey = "Origin"
			val = string(value)
		case bytes.EqualFold(name, []byte("User-Agent")):
			canonicalKey = "User-Agent"
			val = string(value)
		case bytes.EqualFold(name, []byte("Cookie")):
			canonicalKey = "Cookie"
			val = string(value)
		default:
			releaseFastUpgradeRequest(f)
			return nil
		}

		if _, exists := f.header[canonicalKey]; exists {
			releaseFastUpgradeRequest(f)
			return nil
		}
		f.slots[slotIndex][0] = val
		f.header[canonicalKey] = f.slots[slotIndex][:1]
		slotIndex++
	}

	if host == "" || !hasUpgrade || !hasConnection || !hasVersion || f.header.Get("Sec-Websocket-Key") == "" {
		releaseFastUpgradeRequest(f)
		return nil
	}

	return f
}

func bytesHasToken(value []byte, token []byte) bool {
	for len(value) > 0 {
		var part []byte
		if i := bytes.IndexByte(value, ','); i >= 0 {
			part, value = value[:i], value[i+1:]
		} else {
			part, value = value, nil
		}
		if bytes.EqualFold(bytes.TrimSpace(part), token) {
			return true
		}
	}
	return false
}

func validHostBytes(host []byte) bool {
	for _, c := range host {
		switch {
		case c >= '0' && c <= '9', c >= 'a' && c <= 'z', c >= 'A' && c <= 'Z':
		case strings.IndexByte("!$%&'()*+,-.:;=[]_~", c) >= 0:
		default:
			return false
		}
	}
	return true
}

// handle parses and processes one request, and reports whether to keep the connection alive.
func (c *conn) handle(r request) bool {
	switch r.status {
	case 0:
	case http.StatusContinue:
		c.connection.Write(continueResponse)
		return true
	case statusHandoff:
		c.handoff()
		return false // closing the detached connection does nothing
	default:
		c.writeError(r.status)
		return false
	}

	var (
		req     *http.Request
		cleanup func()
	)
	msg := r.msg.Bytes()
	if fast := parseFastUpgradeRequest(msg); fast != nil {
		req = &fast.request
		cleanup = func() { releaseFastUpgradeRequest(fast) }
	} else {
		rd := readerPool.Get().(*reader)
		rd.bytesReader.Reset(msg)
		rd.bufferedReader.Reset(&rd.bytesReader)
		defer func() {
			rd.bytesReader.Reset(nil)
			readerPool.Put(rd)
		}()

		var err error
		req, err = http.ReadRequest(rd.bufferedReader)
		if err != nil {
			c.writeError(http.StatusBadRequest)
			return false
		}
		// Same as the net/http server: only HTTP/1.x is supported, and it is rejected before Host is checked.
		if req.ProtoMajor != 1 {
			c.writeError(http.StatusHTTPVersionNotSupported)
			return false
		}
		// The message ends where framer said, so what the standard library takes for the body must be exactly the rest
		// of it. Otherwise the two disagree on the framing: a field framer did not recognize (see bodyLength), or an empty
		// line made of a bare LF, which the standard library accepts as a line break, ending the headers early. The
		// remainder must not be silently discarded or executed (an upstream proxy may frame it differently, misaligning
		// the responses), so it is handled as a bad request.
		if int64(rd.bufferedReader.Buffered()+rd.bytesReader.Len()) != req.ContentLength {
			c.writeError(http.StatusBadRequest)
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
	}
	if cleanup != nil {
		defer cleanup()
	}
	if c.remoteAddr == "" {
		c.remoteAddr = c.connection.RemoteAddr().String()
	}
	req.RemoteAddr = c.remoteAddr

	w := newResponse(c, req)
	defer w.release()
	body := req.Body
	ok := c.serveHTTP(w, req)
	// Same as the net/http server: close the body, so a Handler that kept it cannot read the pooled reader once it
	// serves another request, and remove the files ParseMultipartForm stored on disk.
	body.Close()
	if req.MultipartForm != nil {
		req.MultipartForm.RemoveAll()
	}
	if !ok {
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
