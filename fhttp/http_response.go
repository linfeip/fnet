package fhttp

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/linfeip/fnet/internal/bufpool"
	"github.com/linfeip/fnet/internal/reactor"
)

// maxBufferedBody is the largest body sent in one piece with a computed
// Content-Length; a longer body switches to chunked encoding and streams.
const maxBufferedBody = 64 << 10

var (
	bodyPool   = sync.Pool{New: func() any { return new(bytes.Buffer) }}
	readerPool = sync.Pool{New: func() any { return bufio.NewReaderSize(nil, 4<<10) }}
	writerPool = sync.Pool{New: func() any { return bufio.NewWriterSize(nil, 4<<10) }}
	headPool   = sync.Pool{New: func() any { return new(bytes.Buffer) }}

	crlf            = []byte("\r\n")
	continue100     = []byte("HTTP/1.1 100 Continue\r\n\r\n")
	lastChunk       = []byte("0\r\n\r\n")
	errUnwrapped    = errors.New("fnet/fhttp: connection was moved to the event loop")
	errDoubleHijack = errors.New("fnet/fhttp: connection already hijacked")
	errFinished     = errors.New("fnet/fhttp: response used after the handler returned")

	// interimExcluded are the framing headers a 1xx response must not carry.
	interimExcluded = map[string]bool{"Content-Length": true, "Transfer-Encoding": true}
)

// responseWriter implements http.ResponseWriter, http.Flusher and
// http.Hijacker for one request, and the deadlines of http.ResponseController.
// It is not safe for concurrent use, like net/http's, and fails once the
// handler has returned.
type responseWriter struct {
	srv  *httpHandler
	conn net.Conn      // response stream: the reactor conn, or TLS on top of it
	br   *bufio.Reader // request reader, handed to a hijacker
	raw  *reactor.Conn // the unencrypted connection; nil under TLS
	hc   *hijackedConn

	header        http.Header
	status        int
	body          *bytes.Buffer // the body held back until its length is known; from bodyPool
	contentLength int64         // declared once the header is out; -1 when unknown
	written       int64         // body bytes sent, to check against contentLength
	discarded     int64         // body bytes a bodyless response swallowed, for HEAD's Content-Length
	done          atomic.Bool   // the handler returned
	wroteStatus   bool          // the first WriteHeader (or Write) fixed the status
	wroteHeader   bool          // the header block is on the wire
	chunked       bool          // the body goes out chunk-encoded
	hijacked      bool
	closeConn     bool // close after this response
	isHead        bool
	http10        bool // the client speaks HTTP/1.0: no chunked encoding, no 1xx
	sent100       bool // a 100 Continue went out
}

func newResponseWriter(srv *httpHandler, conn net.Conn, br *bufio.Reader, raw *reactor.Conn) *responseWriter {
	return &responseWriter{
		srv:           srv,
		conn:          conn,
		br:            br,
		raw:           raw,
		header:        make(http.Header, 4),
		status:        http.StatusOK,
		contentLength: -1,
	}
}

// release ends the handler's use of w and returns its pooled buffer. Any later
// call from a goroutine the handler left behind fails instead of reaching the
// buffer's next owner.
func (w *responseWriter) release() {
	w.done.Store(true)
	if b := w.body; b != nil {
		w.body = nil
		if b.Cap() <= 2*maxBufferedBody {
			b.Reset()
			bodyPool.Put(b)
		}
	}
}

func (w *responseWriter) buffered() int {
	if w.body == nil {
		return 0
	}
	return w.body.Len()
}

// bodyless reports whether the response must not carry a body: HEAD, 1xx, 204
// and 304 (RFC 9110 6.4.1).
func (w *responseWriter) bodyless() bool {
	return w.isHead || w.status < 200 || w.status == http.StatusNoContent || w.status == http.StatusNotModified
}

// allowsContentLength: RFC 9110 8.6 forbids Content-Length on 1xx and 204.
func (w *responseWriter) allowsContentLength() bool {
	return w.status >= 200 && w.status != http.StatusNoContent && w.status != http.StatusNotModified
}

// awaitingBody reports whether the client may still be waiting for permission
// to send the body: no response yet and no body byte received.
func (w *responseWriter) awaitingBody() bool {
	if w.wroteHeader || w.hijacked || w.sent100 || w.br.Buffered() > 0 {
		return false
	}
	return w.raw == nil || w.raw.Buffered() == 0
}

// declaredLength returns the Content-Length the handler set, or -1. A
// malformed one is logged and dropped, as net/http does.
func (w *responseWriter) declaredLength() int64 {
	cl := w.header.Get("Content-Length")
	if cl == "" {
		return -1
	}
	n, err := strconv.ParseInt(strings.TrimSpace(cl), 10, 64)
	if err != nil || n < 0 {
		w.srv.logf("fhttp: invalid Content-Length %q", cl)
		w.header.Del("Content-Length")
		return -1
	}
	return n
}

// hasFraming reports whether the handler chose the framing itself.
func (w *responseWriter) hasFraming() bool {
	return w.declaredLength() >= 0 || w.header.Get("Transfer-Encoding") != ""
}

// stream frames a body whose length is unknown: chunked, or for an HTTP/1.0
// client delimited by closing the connection.
func (w *responseWriter) stream() {
	if w.http10 {
		w.closeConn = true
	} else {
		w.header.Set("Transfer-Encoding", "chunked")
	}
}

func (w *responseWriter) Header() http.Header { return w.header }

// WriteHeader records the status; only the first call counts. With explicit
// framing the header block is sent at once, otherwise when the body is known.
// A 1xx status (but 101) goes out at once as an interim response, such as 103
// Early Hints, and the final status follows later.
func (w *responseWriter) WriteHeader(code int) {
	if w.done.Load() || w.hijacked || w.wroteStatus {
		return
	}
	if code < 100 || code > 999 {
		panic(fmt.Sprintf("invalid WriteHeader code %v", code)) // as net/http does
	}
	if code < 200 && code != http.StatusSwitchingProtocols {
		w.writeInterim(code)
		return
	}
	w.wroteStatus = true
	w.status = code
	if w.hasFraming() {
		_ = w.writeHeader(nil)
	}
}

// writeInterim sends a 1xx response with the header set so far, minus its
// framing. HTTP/1.0 clients do not get one (RFC 9110 15.2).
func (w *responseWriter) writeInterim(code int) {
	if w.http10 {
		return
	}
	if code == http.StatusContinue {
		w.sent100 = true
	}
	hb := headPool.Get().(*bytes.Buffer)
	hb.Reset()
	fmt.Fprintf(hb, "HTTP/1.1 %d %s\r\n", code, http.StatusText(code))
	_ = w.header.WriteSubset(hb, interimExcluded)
	hb.Write(crlf)
	_ = w.send(hb.Bytes())
	headPool.Put(hb)
}

func (w *responseWriter) Write(b []byte) (int, error) {
	if w.done.Load() {
		return 0, errFinished
	}
	if w.hijacked {
		return 0, http.ErrHijacked
	}
	w.wroteStatus = true
	if w.bodyless() {
		w.discarded += int64(len(b))
		return len(b), nil
	}
	if len(b) == 0 {
		return 0, nil
	}
	if !w.wroteHeader {
		if !w.hasFraming() {
			if w.buffered()+len(b) <= maxBufferedBody {
				if w.body == nil {
					w.body = bodyPool.Get().(*bytes.Buffer)
				}
				return w.body.Write(b)
			}
			w.stream() // outgrew the buffer: stream the rest
		}
		if err := w.writeHeader(nil); err != nil {
			return 0, err
		}
		if err := w.flushBuffered(); err != nil {
			return 0, err
		}
	}
	if w.contentLength >= 0 {
		if w.written += int64(len(b)); w.written > w.contentLength {
			w.closeConn = true
			return 0, http.ErrContentLength // more than declared: refuse it, as net/http does
		}
	}
	if err := w.writeBody(b); err != nil {
		return 0, err
	}
	return len(b), nil
}

// WriteString is Write for a string (io.StringWriter): a body still held back
// takes it without a conversion.
func (w *responseWriter) WriteString(s string) (int, error) {
	if !w.done.Load() && !w.hijacked && !w.wroteHeader && !w.bodyless() && len(s) > 0 &&
		w.buffered()+len(s) <= maxBufferedBody && !w.hasFraming() {
		w.wroteStatus = true
		if w.body == nil {
			w.body = bodyPool.Get().(*bytes.Buffer)
		}
		return w.body.WriteString(s)
	}
	return w.Write([]byte(s))
}

// Flush sends the header and the body written so far; later writes go out as
// they come (http.Flusher). A body of unknown length is then chunked.
func (w *responseWriter) Flush() { _ = w.FlushError() }

// FlushError is Flush that reports failure, for http.ResponseController.
func (w *responseWriter) FlushError() error {
	switch {
	case w.done.Load():
		return errFinished
	case w.hijacked:
		return http.ErrHijacked
	case w.wroteHeader:
		return nil // everything written after the header went out already
	}
	w.wroteStatus = true
	if !w.bodyless() && !w.hasFraming() {
		w.stream()
	}
	if err := w.writeHeader(nil); err != nil {
		return err
	}
	return w.flushBuffered()
}

// flushBuffered sends the body held back before the header went out.
func (w *responseWriter) flushBuffered() error {
	if w.buffered() == 0 {
		return nil
	}
	err := w.writeBody(w.body.Bytes())
	w.body.Reset()
	return err
}

// SetReadDeadline sets the deadline for reading the request body, for
// http.ResponseController.
func (w *responseWriter) SetReadDeadline(t time.Time) error { return w.conn.SetReadDeadline(t) }

// SetWriteDeadline sets the deadline for writing the response, for
// http.ResponseController.
func (w *responseWriter) SetWriteDeadline(t time.Time) error { return w.conn.SetWriteDeadline(t) }

// finish completes the response after the handler returns. A body shorter
// than its declared Content-Length marks the connection for closing: reusing
// it would make the client read the next response as the rest of this one.
func (w *responseWriter) finish() error {
	if w.hijacked {
		return nil
	}
	if !w.wroteHeader {
		// The whole response is known: header and body leave in one write.
		if w.allowsContentLength() && !w.hasFraming() {
			n := int64(w.buffered())
			if w.bodyless() {
				n = w.discarded
			}
			w.header.Set("Content-Length", strconv.FormatInt(n, 10))
		}
		var body []byte
		if !w.bodyless() && w.body != nil {
			body = w.body.Bytes()
			w.written = int64(len(body))
		}
		err := w.writeHeader(body)
		if w.body != nil {
			w.body.Reset()
		}
		if err != nil {
			return err
		}
	}
	if w.bodyless() {
		return nil
	}
	if w.chunked {
		if err := w.send(lastChunk); err != nil {
			return err
		}
	}
	if w.contentLength >= 0 && w.written != w.contentLength {
		w.closeConn = true
	}
	return nil
}

// writeHeader sends the status line and header block, followed by body.
func (w *responseWriter) writeHeader(body []byte) error {
	w.wroteHeader = true
	h := w.header
	w.contentLength = w.declaredLength()
	w.chunked = h.Get("Transfer-Encoding") == "chunked"
	w.settleFraming()
	if h.Get("Date") == "" {
		h.Set("Date", httpDate())
	}
	if w.srv.shuttingDown.Load() {
		w.closeConn = true
	}
	switch conn := h.Get("Connection"); {
	case strings.EqualFold(conn, "close"):
		w.closeConn = true
	case conn == "" && w.closeConn:
		h.Set("Connection", "close")
	case conn == "":
		h.Set("Connection", "keep-alive")
	}

	hb := headPool.Get().(*bytes.Buffer)
	hb.Reset()
	hb.WriteString("HTTP/1.1 ")
	hb.WriteString(strconv.Itoa(w.status))
	hb.WriteByte(' ')
	hb.WriteString(http.StatusText(w.status))
	hb.Write(crlf)
	_ = h.Write(hb)
	hb.Write(crlf)
	err := w.send(hb.Bytes(), body)
	headPool.Put(hb)
	return err
}

// settleFraming makes the framing the handler set consistent, as net/http
// does: a status without a body (1xx, 204, 304) carries no Content-Length or
// Transfer-Encoding (304 no Content-Type either); an HTTP/1.0 client knows no
// transfer coding; chunked wins over a Content-Length, which wins over any
// other coding; and without a length, a coding other than chunked (identity,
// once recommended for event streams) is delimited by closing the connection,
// since only chunked marks where a body ends.
func (w *responseWriter) settleFraming() {
	h := w.header
	te := h.Get("Transfer-Encoding")
	switch {
	case !w.allowsContentLength():
		h.Del("Content-Length")
		h.Del("Transfer-Encoding")
		if w.status == http.StatusNotModified {
			h.Del("Content-Type")
		}
		w.contentLength, w.chunked = -1, false
	case te == "":
	case w.http10:
		h.Del("Transfer-Encoding")
		w.chunked = false
		w.closeConn = w.closeConn || w.contentLength < 0
	case w.chunked:
		if w.contentLength >= 0 {
			w.srv.logf("fhttp: both Transfer-Encoding %q and Content-Length %d set; dropping the length", te, w.contentLength)
			h.Del("Content-Length")
			w.contentLength = -1
		}
	case w.contentLength >= 0:
		h.Del("Transfer-Encoding")
	case strings.EqualFold(te, "identity"):
		h.Del("Transfer-Encoding")
		w.closeConn = true
	default:
		h.Add("Transfer-Encoding", "chunked")
		w.chunked = true
	}
}

func (w *responseWriter) writeBody(b []byte) error {
	if !w.chunked {
		_, err := w.conn.Write(b)
		return err
	}
	var size [18]byte
	line := append(strconv.AppendInt(size[:0], int64(len(b)), 16), '\r', '\n')
	return w.send(line, b, crlf)
}

// send writes parts as one unit: one writev on a plaintext connection, one
// write (so one TLS record) otherwise, unless the parts are large enough that
// copying them together costs more than separate writes.
func (w *responseWriter) send(parts ...[]byte) error {
	if w.raw != nil {
		_, err := w.raw.Writev(parts)
		return err
	}
	total := 0
	for _, p := range parts {
		total += len(p)
	}
	if total > maxBufferedBody {
		for _, p := range parts {
			if len(p) > 0 {
				if _, err := w.conn.Write(p); err != nil {
					return err
				}
			}
		}
		return nil
	}
	buf := bufpool.Get(total)
	off := 0
	for _, p := range parts {
		off += copy(buf.B[off:], p)
	}
	_, err := w.conn.Write(buf.B)
	bufpool.Put(buf)
	return err
}

// Hijack implements http.Hijacker. Bytes the request reader buffered ahead are
// served first by the returned connection.
func (w *responseWriter) Hijack() (net.Conn, *bufio.ReadWriter, error) {
	if w.done.Load() {
		return nil, nil, errFinished
	}
	if w.hijacked {
		return nil, nil, errDoubleHijack
	}
	w.hijacked = true
	// As with net/http, the request's deadlines end with it: a protocol taken
	// over from here sets its own.
	_ = w.conn.SetDeadline(time.Time{})
	bw := writerPool.Get().(*bufio.Writer)
	bw.Reset(w.conn)
	w.hc = &hijackedConn{Conn: w.conn, br: w.br, bw: bw, raw: w.raw}
	return w.hc, bufio.NewReadWriter(w.br, bw), nil
}

const (
	notUnwrapped int32 = iota
	unwrapping
	unwrapped
)

// hijackedConn is the net.Conn returned by Hijack.
type hijackedConn struct {
	net.Conn
	br    *bufio.Reader
	bw    *bufio.Writer
	raw   *reactor.Conn // nil under TLS
	state atomic.Int32
}

func (hc *hijackedConn) Read(b []byte) (int, error) {
	if hc.state.Load() != notUnwrapped {
		return 0, errUnwrapped
	}
	return hc.br.Read(b)
}

// Writev writes iovs as one unit (one TLS record under TLS).
func (hc *hijackedConn) Writev(iovs [][]byte) (int, error) {
	if hc.raw != nil {
		return hc.raw.Writev(iovs)
	}
	total := 0
	for _, b := range iovs {
		total += len(b)
	}
	buf := bufpool.Get(total)
	off := 0
	for _, b := range iovs {
		off += copy(buf.B[off:], b)
	}
	n, err := hc.Conn.Write(buf.B)
	bufpool.Put(buf)
	return n, err
}

// Unwrap moves the connection onto the event loop for a protocol that drives
// it with a reactor.Handler from then on (event-driven WebSocket). Bytes the
// request reader buffered ahead are pushed back so the handler sees the whole
// stream. It returns nil under TLS, whose records are decrypted on a worker.
func (hc *hijackedConn) Unwrap() *reactor.Conn {
	if hc.raw == nil || !hc.state.CompareAndSwap(notUnwrapped, unwrapping) {
		return nil
	}
	if n := hc.br.Buffered(); n > 0 {
		ahead, _ := hc.br.Peek(n)
		hc.raw.Unread(ahead)
		_, _ = hc.br.Discard(n)
	}
	hc.state.Store(unwrapped)
	return hc.raw
}

// recyclable reports whether the reader and writer lent to the hijacker are
// free again: the stream moved to the event loop before the handler returned.
func (hc *hijackedConn) recyclable() bool { return hc.state.Load() == unwrapped }

// HTTP dates have one-second resolution (RFC 9110 5.6.7), so the formatted
// Date header is shared across responses within a second.
type dateEntry struct {
	sec int64
	s   string
}

var dateCache atomic.Pointer[dateEntry]

func httpDate() string {
	now := time.Now()
	if d := dateCache.Load(); d != nil && d.sec == now.Unix() {
		return d.s
	}
	d := &dateEntry{sec: now.Unix(), s: now.UTC().Format(http.TimeFormat)}
	dateCache.Store(d)
	return d.s
}

var (
	_ http.ResponseWriter = (*responseWriter)(nil)
	_ http.Flusher        = (*responseWriter)(nil)
	_ http.Hijacker       = (*responseWriter)(nil)
	_ io.Reader           = (*hijackedConn)(nil)
)
