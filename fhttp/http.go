package fhttp

import (
	"bufio"
	"bytes"
	"context"
	"crypto/tls"
	"errors"
	"io"
	"log"
	"net"
	"net/http"
	"runtime/debug"
	"slices"
	"strings"
	"sync/atomic"
	"time"

	"github.com/linfeip/fnet/internal/reactor"
	"github.com/linfeip/fnet/pool"
)

// maxBodyDrain is how much of a request body the handler left unread is
// discarded to keep the connection alive; larger leftovers close it.
const maxBodyDrain = 256 << 10

// errorHeaders completes the status line of the error responses sent to bad
// requests, like net/http's.
const errorHeaders = "\r\nContent-Type: text/plain; charset=utf-8\r\nConnection: close\r\n\r\n"

var headerTooLarge = []byte("HTTP/1.1 431 Request Header Fields Too Large" + errorHeaders + "431 Request Header Fields Too Large")

// headerComplete reports whether b holds the blank line that ends a request
// header ("\r\n\r\n" or "\n\n", as textproto reads it).
func headerComplete(b []byte) bool {
	for i := 0; ; {
		j := bytes.IndexByte(b[i:], '\n')
		if j < 0 {
			return false
		}
		i += j + 1
		if rest := b[i:]; len(rest) > 0 && rest[0] == '\n' || len(rest) > 1 && rest[0] == '\r' && rest[1] == '\n' {
			return true
		}
	}
}

// httpHandler is HTTP/1.x on reactor connections. On the event loop it only
// decides when a request is ready (a complete header, or any TLS record) and
// hands the connection to a worker, where net/http's parser and the user's
// Handler run with blocking reads. Between requests a connection returns to
// the event loop and holds no goroutine.
//
// Timeouts: while the event loop holds a connection, a close deadline bounds
// the wait for a request header (headerTimeout, from accept or from the
// request's first byte) and the keep-alive wait (idleTimeout). Once a worker
// holds it, read and write deadlines on the connection take over.
type httpHandler struct {
	handler       http.Handler
	tlsConfig     *tls.Config
	headerTimeout time.Duration // <= 0: none
	readTimeout   time.Duration
	writeTimeout  time.Duration
	idleTimeout   time.Duration // keep-alive; under TLS <= 0 closes after each response
	maxHeader     int
	submit        func(connID uint64, task func()) error
	errorLog      *log.Logger

	active       atomic.Int64 // connections handed to a worker and not yet back
	shuttingDown atomic.Bool
}

func newHTTPHandler(s *Server, tlsConfig *tls.Config) *httpHandler {
	h := &httpHandler{
		handler:       s.Handler,
		tlsConfig:     tlsConfig,
		headerTimeout: s.ReadHeaderTimeout,
		readTimeout:   s.ReadTimeout,
		writeTimeout:  s.WriteTimeout,
		idleTimeout:   s.IdleTimeout,
		maxHeader:     s.MaxHeaderBytes,
		submit:        s.WorkerPool,
		errorLog:      s.ErrorLog,
	}
	if h.handler == nil {
		h.handler = http.DefaultServeMux
	}
	if tlsConfig != nil {
		h.tlsConfig = http11Only(tlsConfig)
	}
	if h.headerTimeout == 0 {
		h.headerTimeout = s.ReadTimeout
	}
	if h.headerTimeout == 0 {
		h.headerTimeout = DefaultReadHeaderTimeout
	}
	if h.idleTimeout == 0 && tlsConfig == nil {
		h.idleTimeout = DefaultIdleTimeout
	}
	if h.maxHeader <= 0 {
		h.maxHeader = DefaultMaxHeaderBytes
	}
	return h
}

// http11Only drops h2 from cfg's ALPN protocols: a client that negotiated it
// would send HTTP/2 frames, and fhttp speaks HTTP/1.1.
func http11Only(cfg *tls.Config) *tls.Config {
	if !slices.Contains(cfg.NextProtos, "h2") {
		return cfg
	}
	cfg = cfg.Clone()
	cfg.NextProtos = slices.DeleteFunc(slices.Clone(cfg.NextProtos), func(p string) bool { return p == "h2" })
	return cfg
}

func (h *httpHandler) logf(format string, args ...any) {
	if h.errorLog != nil {
		h.errorLog.Printf(format, args...)
	} else {
		log.Printf(format, args...)
	}
}

// OnOpen gives a new connection headerTimeout to send its first request header.
func (h *httpHandler) OnOpen(c *reactor.Conn) {
	if h.headerTimeout > 0 {
		c.SetCloseDeadline(time.Now().Add(h.headerTimeout))
	}
}

// OnData runs on the event loop with everything buffered so far; only the
// bytes new since the last call are searched for the end of the header.
func (h *httpHandler) OnData(c *reactor.Conn, data []byte) int {
	if h.tlsConfig != nil || headerComplete(data[max(0, len(data)-c.NewBytes()-3):]) {
		h.dispatch(c, nil)
		return 0
	}
	if len(data) > h.maxHeader {
		_, _ = c.Write(headerTooLarge) // a header that never ends, slowly or not
		_ = c.Close()
		return 0
	}
	if h.headerTimeout > 0 {
		// A request has started: its header must be complete within
		// headerTimeout of its first byte, however slowly the rest trickles in.
		c.SetCloseDeadline(c.InputSince().Add(h.headerTimeout))
	}
	return 0
}

func (h *httpHandler) OnClose(*reactor.Conn, error) {}

// dispatch hands the connection to a worker, to start serving it, or with s
// to resume an idle TLS session. Event loop only.
func (h *httpHandler) dispatch(c *reactor.Conn, s *session) {
	h.active.Add(1) // first: closeIdle skips a detached connection as active
	c.Detach()
	if !pool.Dispatch(h.submit, uint64(c.Fd()), func() { h.serve(c, s) }) {
		h.active.Add(-1)
		_ = c.Close() // the custom pool refused the request
	}
}

// closeIdle closes the connections waiting for a request that has not begun,
// for Shutdown, and reports how many HTTP connections are still waiting.
func (h *httpHandler) closeIdle(eng *reactor.Engine) (waiting int) {
	eng.ForEach(func(c *reactor.Conn) {
		switch hh := c.Handler().(type) {
		case *httpHandler:
			if hh != h || c.Detached() {
				return // served by a worker (counted in active), or hijacked
			}
		case *session:
			if c.Detached() {
				return // its next request is being served (counted in active)
			}
		default:
			return // upgraded, e.g. to WebSocket
		}
		waiting++
		if c.Buffered() == 0 {
			_ = c.Close()
		}
	})
	return waiting
}

// after returns start+d, or the zero time (no deadline) when d <= 0.
func after(start time.Time, d time.Duration) time.Time {
	if d <= 0 {
		return time.Time{}
	}
	return start.Add(d)
}

// serve runs on a worker: it opens a session for a new connection (with its
// TLS handshake) or resumes an idle one, and serves requests.
func (h *httpHandler) serve(c *reactor.Conn, s *session) {
	defer h.active.Add(-1)
	if s == nil {
		s = &session{h: h, c: c, rw: c, raw: c}
		if h.tlsConfig != nil {
			tc := tls.Server(c, h.tlsConfig)
			_ = tc.SetReadDeadline(after(time.Now(), h.headerTimeout))
			if err := tc.Handshake(); err != nil {
				_ = c.Close()
				return
			}
			st := tc.ConnectionState()
			s.rw, s.raw, s.tlsState = tc, nil, &st
		}
	}
	s.run()
}

// session is a connection while its requests are served. A plaintext session
// ends when the connection goes idle; a TLS one outlives the requests, keeping
// the TLS state while its connection idles on the poller.
type session struct {
	h        *httpHandler
	c        *reactor.Conn
	rw       net.Conn      // c, or TLS on top of it
	raw      *reactor.Conn // c when plaintext; nil under TLS
	tlsState *tls.ConnectionState
	lr       limitReader   // under br: bounds a request header
	br       *bufio.Reader // lent from readerPool while a worker serves
}

// OnData resumes an idle TLS session on a worker when its next request begins.
func (s *session) OnData(c *reactor.Conn, _ []byte) int {
	s.h.dispatch(c, s)
	return 0
}

func (s *session) OnClose(*reactor.Conn, error) {}

// outcome is what a connection does after a request.
type outcome int

const (
	nextRequest outcome = iota // the next request is buffered already
	goIdle                     // wait for the next request on the poller
	closeConn                  // close the connection
	handedOff                  // hijacked: the handler owns the connection
	handedBack                 // hijacked, and already moved onto the event loop
)

// run serves requests until the connection idles, is hijacked, or closes.
func (s *session) run() {
	s.lr.r = s.rw
	s.br = readerPool.Get().(*bufio.Reader)
	s.br.Reset(&s.lr)
	for {
		switch s.serveOne() {
		case nextRequest:
			continue
		case goIdle:
			s.idle()
		case closeConn:
			s.releaseReader()
			_ = s.rw.Close()
		case handedBack:
			s.releaseReader()
		case handedOff:
			// The hijacker keeps the reader.
		}
		return
	}
}

func (s *session) releaseReader() {
	s.br.Reset(nil)
	readerPool.Put(s.br)
	s.br = nil
}

// idle returns the connection to the event loop until its next request.
func (s *session) idle() {
	h := s.h
	var next reactor.Handler = h
	if s.raw != nil {
		// Plaintext: bytes read ahead go back to the connection's input, where
		// the handler finds them again.
		if n := s.br.Buffered(); n > 0 {
			ahead, _ := s.br.Peek(n)
			s.c.Unread(ahead)
		}
	} else {
		_ = s.rw.SetReadDeadline(time.Time{})
		next = s // TLS: the session resumes with its state
	}
	s.releaseReader()
	if h.idleTimeout > 0 {
		s.c.SetCloseDeadline(time.Now().Add(h.idleTimeout))
	}
	s.c.Attach(next) // idle again: under poller custody, no goroutine
}

// serveOne reads and serves one request.
func (s *session) serveOne() outcome {
	h := s.h
	start := time.Now()
	_ = s.rw.SetReadDeadline(after(start, h.headerTimeout))
	s.lr.n = int64(h.maxHeader) + 4096 // the reader's buffer may run ahead of the header
	s.lr.hit = false
	req, err := http.ReadRequest(s.br)
	s.lr.n = -1
	if err != nil {
		s.refuse(err)
		return closeConn
	}
	if reason := checkHost(req); reason != "" {
		s.reply("400 Bad Request", reason)
		return closeConn
	}
	if exp := req.Header.Get("Expect"); exp != "" && !strings.EqualFold(exp, "100-continue") {
		s.reply("417 Expectation Failed", "") // as net/http does (RFC 9110 10.1.1)
		return closeConn
	}
	_ = s.rw.SetReadDeadline(after(start, h.readTimeout)) // the body
	_ = s.rw.SetWriteDeadline(after(start, h.writeTimeout))

	// The request's context ends when the client goes away mid-request, and
	// once the handler returns.
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	s.c.NotifyInputEnd(cancel)
	req = req.WithContext(ctx)
	req.RemoteAddr = s.c.RemoteAddrString()
	req.TLS = s.tlsState

	w := newResponseWriter(h, s.rw, s.br, s.raw)
	w.isHead = req.Method == http.MethodHead
	w.http10 = !req.ProtoAtLeast(1, 1)
	w.closeConn = req.Close || s.raw == nil && h.idleTimeout <= 0
	var body *requestBody
	if req.Body != http.NoBody {
		body = &requestBody{src: req.Body}
		if req.ProtoAtLeast(1, 1) && strings.EqualFold(req.Header.Get("Expect"), "100-continue") {
			body.expect, body.w = true, w
		}
		req.Body = body
	}
	ok := s.handle(w, req)
	s.c.NotifyInputEnd(nil)
	if body != nil {
		body.closed.Store(true) // a late read must not reach the next request
	}
	if w.hijacked {
		w.release()
		// If the hijacker already moved the stream onto the event loop, the
		// reader and writer are free again.
		if hc := w.hc; hc != nil && hc.recyclable() {
			hc.bw.Reset(nil)
			writerPool.Put(hc.bw)
			return handedBack
		}
		return handedOff
	}
	if !ok {
		w.release()
		return closeConn // a handler panic ends the connection, not the process
	}
	err = w.finish()
	closeAfter := err != nil || w.closeConn || h.shuttingDown.Load()
	w.release()
	if !closeAfter && body != nil {
		// A client that declared a body and stopped sending must not hold
		// this worker: the leftovers get headerTimeout at most.
		drainBy := after(time.Now(), h.headerTimeout)
		if d := after(start, h.readTimeout); !d.IsZero() && (drainBy.IsZero() || d.Before(drainBy)) {
			drainBy = d
		}
		_ = s.rw.SetReadDeadline(drainBy)
		closeAfter = !body.drain()
	}
	switch {
	case closeAfter:
		return closeConn
	case s.nextBuffered():
		return nextRequest
	}
	return goIdle
}

// handle runs the handler and reports whether it returned without panicking.
func (s *session) handle(w *responseWriter, req *http.Request) (ok bool) {
	defer func() {
		if r := recover(); r != nil {
			if r != http.ErrAbortHandler {
				s.h.logf("fhttp: panic serving %s: %v\n%s", req.RemoteAddr, r, debug.Stack())
			}
			ok = false
		}
	}()
	s.h.handler.ServeHTTP(w, req)
	return true
}

// nextBuffered reports whether the next request is already here, so the
// worker should serve it before the connection goes idle.
func (s *session) nextBuffered() bool {
	if n := s.br.Buffered(); n > 0 {
		if s.raw == nil {
			return true
		}
		ahead, _ := s.br.Peek(n)
		return headerComplete(ahead)
	}
	if s.raw != nil {
		return false
	}
	// TLS may hold decrypted input the reader has not asked for yet: the
	// event loop cannot see it, so look without waiting.
	_ = s.rw.SetReadDeadline(time.Now())
	_, err := s.br.Peek(1)
	return err == nil
}

// refuse answers a request that could not be read, like net/http: 431 for a
// header over the limit, 501 for an unknown transfer coding, 400 for the
// rest. A peer that left, or went quiet past the deadline, gets nothing.
func (s *session) refuse(err error) {
	var ne net.Error
	switch {
	case s.lr.hit:
		s.reply("431 Request Header Fields Too Large", "")
	case err == io.EOF, err == io.ErrUnexpectedEOF, errors.Is(err, net.ErrClosed), errors.As(err, &ne) && ne.Timeout():
	case strings.Contains(err.Error(), "unsupported transfer encoding"):
		s.reply("501 Not Implemented", "unsupported transfer encoding")
	default:
		s.reply("400 Bad Request", "")
	}
}

// reply sends an error response to a request that is not served.
func (s *session) reply(status, reason string) {
	body := status
	if reason != "" {
		body += ": " + reason
	}
	_ = s.rw.SetWriteDeadline(time.Now().Add(time.Second))
	_, _ = io.WriteString(s.rw, "HTTP/1.1 "+status+errorHeaders+body)
}

// checkHost applies the Host rules net/http's server adds to ReadRequest's
// (RFC 9112 3.2): an HTTP/1.1 request names its host, validly. ReadRequest
// has already refused several Host headers and moved the one into req.Host.
// It returns why the request is refused, or "".
func checkHost(req *http.Request) string {
	switch {
	case req.ProtoAtLeast(1, 1) && req.Host == "" && req.Method != http.MethodConnect:
		return "missing required Host header"
	case !validHost(req.Host):
		return "malformed Host header"
	}
	return ""
}

// validHost reports whether h is made of the bytes a Host header may hold: a
// reg-name, IP literal or IPv4 address and a port (RFC 3986 3.2.2).
func validHost(h string) bool {
	for i := 0; i < len(h); i++ {
		c := h[i]
		switch {
		case 'a' <= c && c <= 'z', 'A' <= c && c <= 'Z', '0' <= c && c <= '9':
		case strings.IndexByte("-._~!$&'()*+,;=:[]%@", c) >= 0:
		default:
			return false
		}
	}
	return true
}

// limitReader bounds what a request header may read from the connection.
type limitReader struct {
	r   io.Reader
	n   int64 // bytes left; < 0 means no limit
	hit bool  // the limit was reached
}

func (l *limitReader) Read(p []byte) (int, error) {
	if l.n == 0 {
		l.hit = true
		return 0, io.EOF
	}
	if l.n > 0 && int64(len(p)) > l.n {
		p = p[:l.n]
	}
	n, err := l.r.Read(p)
	if l.n > 0 {
		l.n -= int64(n)
	}
	return n, err
}

// requestBody is the Body a handler sees. On the first read it answers
// "Expect: 100-continue" (RFC 9110 10.1.1) with "100 Continue" if the client
// is still waiting for it. Its Close does not read the rest of the body: after
// the handler returns, drain decides whether the connection can be reused, and
// later reads fail.
type requestBody struct {
	src     io.ReadCloser
	w       *responseWriter
	expect  bool // the client sent Expect: 100-continue
	started bool // the handler has read from the body
	closed  atomic.Bool
}

func (b *requestBody) Read(p []byte) (int, error) {
	if b.closed.Load() {
		return 0, http.ErrBodyReadAfterClose
	}
	if !b.started {
		b.started = true
		if w := b.w; b.expect && w.awaitingBody() {
			w.sent100 = true
			if _, err := w.conn.Write(continue100); err != nil {
				return 0, err
			}
		}
	}
	return b.src.Read(p)
}

func (b *requestBody) Close() error {
	b.closed.Store(true)
	return nil
}

// drain discards what the handler left of the body so the next request can be
// read, and reports whether the connection may stay open. It gives up on
// bodies beyond maxBodyDrain, and on a body the client was never told to send
// (100-continue answered with a final response), which may never arrive.
func (b *requestBody) drain() bool {
	if b.expect && !b.started {
		return false
	}
	n, err := io.CopyN(io.Discard, b.src, maxBodyDrain+1)
	return n <= maxBodyDrain && (err == nil || err == io.EOF)
}
