package fhttp

import (
	"bufio"
	"bytes"
	"crypto/tls"
	"io"
	"net"
	"net/http"
	"strings"
	"time"

	"github.com/linfeip/fnet/internal/reactor"
	"github.com/linfeip/fnet/pool"
)

const (
	// maxHeaderBuffer bounds the bytes an idle connection may buffer without
	// completing a request header; beyond it the connection is dropped.
	maxHeaderBuffer = 64 << 10
	// maxBodyDrain is how much of a request body the handler left unread is
	// discarded to keep the connection alive; larger leftovers close it.
	maxBodyDrain = 256 << 10
)

var (
	crlfcrlf = []byte("\r\n\r\n")
	lflf     = []byte("\n\n")
)

func headerComplete(b []byte) bool {
	return bytes.Contains(b, crlfcrlf) || bytes.Contains(b, lflf)
}

// httpHandler is HTTP/1.x on reactor connections. On the event loop it only
// decides when a request is ready (a complete header, or any TLS record) and
// hands the connection to a worker, where net/http's parser and the user's
// Handler run with blocking reads. Between requests a plaintext connection
// returns to the event loop and holds no goroutine.
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
	idleTimeout   time.Duration // plaintext keep-alive; TLS: <= 0 closes after each response
	submit        func(connID uint64, task func())
}

func newHTTPHandler(s *Server, tlsConfig *tls.Config) *httpHandler {
	h := &httpHandler{
		handler:       s.Handler,
		tlsConfig:     tlsConfig,
		headerTimeout: s.ReadHeaderTimeout,
		readTimeout:   s.ReadTimeout,
		writeTimeout:  s.WriteTimeout,
		idleTimeout:   s.IdleTimeout,
		submit:        s.WorkerPool,
	}
	if h.handler == nil {
		h.handler = http.DefaultServeMux
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
	return h
}

// OnOpen gives a new connection headerTimeout to send its first request header.
func (h *httpHandler) OnOpen(c *reactor.Conn) {
	if h.headerTimeout > 0 {
		c.SetCloseDeadline(time.Now().Add(h.headerTimeout))
	}
}

// OnData runs on the event loop with everything buffered so far.
func (h *httpHandler) OnData(c *reactor.Conn, data []byte) int {
	if h.tlsConfig != nil || headerComplete(data) {
		c.Detach()
		h.dispatch(c)
		return 0
	}
	if len(data) > maxHeaderBuffer {
		_ = c.Close() // slow-loris: a header that never ends
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

func (h *httpHandler) dispatch(c *reactor.Conn) {
	if !pool.Dispatch(h.submit, uint64(c.Fd()), func() { h.serve(c) }) {
		_ = c.Close() // the custom pool rejected the task
	}
}

// after returns start+d, or the zero time (no deadline) when d <= 0.
func after(start time.Time, d time.Duration) time.Time {
	if d <= 0 {
		return time.Time{}
	}
	return start.Add(d)
}

// serve runs on a worker. It serves requests until the connection either goes
// idle (plaintext: back to the event loop), is hijacked, or must be closed.
func (h *httpHandler) serve(c *reactor.Conn) {
	var rw net.Conn = c
	defer func() {
		if recover() != nil { // a handler panic ends the connection, not the process
			_ = rw.Close()
		}
	}()

	raw := c // the unencrypted stream, if there is one
	var tlsState *tls.ConnectionState
	if h.tlsConfig != nil {
		tc := tls.Server(c, h.tlsConfig)
		_ = tc.SetReadDeadline(after(time.Now(), h.headerTimeout))
		if err := tc.Handshake(); err != nil {
			_ = c.Close()
			return
		}
		st := tc.ConnectionState()
		rw, raw, tlsState = tc, nil, &st
	}

	br := readerPool.Get().(*bufio.Reader)
	br.Reset(rw)
	defer func() {
		if br != nil {
			br.Reset(nil)
			readerPool.Put(br)
		}
	}()

	for {
		start := time.Now()
		_ = rw.SetReadDeadline(after(start, h.headerTimeout))
		req, err := http.ReadRequest(br)
		if err != nil {
			break // malformed, timed out, or the peer is gone: close silently
		}
		_ = rw.SetReadDeadline(after(start, h.readTimeout)) // the body
		// RFC 9112 3.2: an HTTP/1.1 request needs exactly one Host header.
		if req.ProtoAtLeast(1, 1) && (req.Host == "" || len(req.Header["Host"]) > 1) {
			break
		}
		req.RemoteAddr = c.RemoteAddrString()
		req.TLS = tlsState
		_ = rw.SetWriteDeadline(after(start, h.writeTimeout))

		w := newResponseWriter(rw, br, raw)
		w.isHead = req.Method == http.MethodHead
		w.closeConn = req.Close
		w.http10 = !req.ProtoAtLeast(1, 1)
		var body *requestBody
		if req.Body != http.NoBody {
			body = &requestBody{src: req.Body}
			if req.ProtoAtLeast(1, 1) && strings.EqualFold(req.Header.Get("Expect"), "100-continue") {
				body.expect, body.w = true, w
			}
			req.Body = body
		}
		h.handler.ServeHTTP(w, req)
		if body != nil {
			body.w = nil // w is recycled below; a late body read must not reach it
		}
		if w.hijacked {
			// The hijacker owns the connection now. If it already moved the
			// stream onto the event loop, the reader and writer are free again.
			if hc := w.hc; hc != nil && hc.recyclable() {
				hc.bw.Reset(nil)
				writerPool.Put(hc.bw)
			} else {
				br = nil // still in the hijacker's hands
			}
			w.release()
			return
		}
		err = w.finish()
		closeAfter := err != nil || w.closeConn
		if !closeAfter && body != nil {
			// A client that declared a body and stopped sending must not hold
			// this worker: the leftovers get headerTimeout at most.
			drainBy := after(time.Now(), h.headerTimeout)
			if d := after(start, h.readTimeout); !d.IsZero() && (drainBy.IsZero() || d.Before(drainBy)) {
				drainBy = d
			}
			_ = rw.SetReadDeadline(drainBy)
			closeAfter = !body.drain()
		}
		w.release()
		if closeAfter {
			break
		}

		if h.tlsConfig != nil {
			// TLS keeps its record state in the worker, so the worker stays
			// with the connection while it waits for the next request.
			if h.idleTimeout <= 0 {
				break
			}
			_ = rw.SetReadDeadline(time.Now().Add(h.idleTimeout))
			if _, err := br.Peek(1); err != nil {
				break
			}
			continue
		}

		// Plaintext: keep this worker only if the next request is already here.
		if n := br.Buffered(); n > 0 {
			ahead, _ := br.Peek(n)
			if headerComplete(ahead) {
				continue
			}
			c.Unread(ahead)
		}
		if h.idleTimeout > 0 {
			c.SetCloseDeadline(time.Now().Add(h.idleTimeout))
		}
		c.Attach(h) // idle again: under poller custody, no goroutine
		return
	}
	_ = rw.Close()
}

// requestBody is the Body a handler sees. On the first read it answers
// "Expect: 100-continue" (RFC 9110 10.1.1) with "100 Continue" if the client
// is still waiting for it. Its Close does not read the rest of the body: after
// the handler returns, drain decides whether the connection can be reused.
type requestBody struct {
	src     io.ReadCloser
	w       *responseWriter // nil once the handler has returned
	expect  bool            // the client sent Expect: 100-continue
	started bool            // the handler has read from the body
	closed  bool
}

func (b *requestBody) Read(p []byte) (int, error) {
	if b.closed {
		return 0, http.ErrBodyReadAfterClose
	}
	if !b.started {
		b.started = true
		if w := b.w; b.expect && w != nil && w.awaitingBody() {
			if _, err := w.conn.Write(continue100); err != nil {
				return 0, err
			}
		}
	}
	return b.src.Read(p)
}

func (b *requestBody) Close() error {
	b.closed = true
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
