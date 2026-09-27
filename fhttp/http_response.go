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

// sniffLen is how much of a body http.DetectContentType looks at.
const sniffLen = 512

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

	// badTrailer are the fields a trailer section must not carry (RFC 9110
	// 6.5.1): the ones that frame, route or authenticate the message. It is
	// the list net/http checks declared trailers against.
	badTrailer = map[string]bool{
		"Authorization": true, "Cache-Control": true, "Connection": true, "Content-Encoding": true,
		"Content-Length": true, "Content-Range": true, "Content-Type": true, "Expect": true,
		"Host": true, "Keep-Alive": true, "Max-Forwards": true, "Pragma": true,
		"Proxy-Authenticate": true, "Proxy-Authorization": true, "Proxy-Connection": true,
		"Range": true, "Realm": true, "Te": true, "Trailer": true, "Transfer-Encoding": true,
		"Www-Authenticate": true,
	}
)

// responseWriter implements http.ResponseWriter, http.Flusher and
// http.Hijacker for one request, and the deadlines and full duplex of
// http.ResponseController. It is not safe for concurrent use, like net/http's,
// and fails once the handler has returned.
//
// Like net/http's, it sends nothing until it must: the header leaves with the
// first body bytes, when a Flush asks for it, or when the handler returns, so a
// small response is one write with a computed Content-Length. A body is held
// back up to maxBufferedBody unless the handler framed it itself (set
// Content-Length or Transfer-Encoding), which streams every Write.
type responseWriter struct {
	srv  *httpHandler
	conn net.Conn      // response stream: the reactor conn, or TLS on top of it
	br   *bufio.Reader // request reader, handed to a hijacker
	raw  *reactor.Conn // the unencrypted connection; nil under TLS
	hc   *hijackedConn

	header        http.Header
	frozen        http.Header // header's snapshot once the status is fixed; see Header
	status        int
	body          *bytes.Buffer // the body held back until the header goes out; from bodyPool
	trailers      []string      // the trailer fields the header declared
	contentLength int64         // declared once the header is out; -1 when unknown
	written       int64         // body bytes sent, to check against contentLength
	discarded     int64         // body bytes a HEAD response swallowed, for its Content-Length
	done          atomic.Bool   // the handler returned
	wroteStatus   bool          // the first WriteHeader (or Write) fixed the status
	wroteHeader   bool          // the header block is on the wire
	streaming     bool          // the body's length is unknown: chunk it, or close after it
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

// held returns the body held back so far.
func (w *responseWriter) held() []byte {
	if w.body == nil {
		return nil
	}
	return w.body.Bytes()
}

func (w *responseWriter) hold(b []byte) {
	if w.body == nil {
		w.body = bodyPool.Get().(*bytes.Buffer)
	}
	w.body.Write(b)
}

// bodyAllowed reports whether the status lets a response carry a body: not
// 1xx, 204 or 304 (RFC 9110 6.4.1).
func (w *responseWriter) bodyAllowed() bool {
	return w.status >= 200 && w.status != http.StatusNoContent && w.status != http.StatusNotModified
}

// bodyless reports whether no body goes on the wire: the status allows none,
// or the request is HEAD, whose response is framed as its GET's would be.
func (w *responseWriter) bodyless() bool { return w.isHead || !w.bodyAllowed() }

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
	cl := w.hdr().Get("Content-Length")
	if cl == "" {
		return -1
	}
	n, err := strconv.ParseInt(strings.TrimSpace(cl), 10, 64)
	if err != nil || n < 0 {
		w.srv.logf("fhttp: invalid Content-Length %q", cl)
		w.hdr().Del("Content-Length")
		return -1
	}
	return n
}

// hasFraming reports whether the handler chose the framing itself.
func (w *responseWriter) hasFraming() bool {
	return w.declaredLength() >= 0 || w.hdr().Get("Transfer-Encoding") != ""
}

// hasTrailers reports whether the handler asked for trailer fields, by
// declaring them in a Trailer header or with http.TrailerPrefix keys (set at
// any time, so in the handler's own header).
func (w *responseWriter) hasTrailers() bool {
	if len(w.hdr()["Trailer"]) > 0 {
		return true
	}
	for k := range w.header {
		if strings.HasPrefix(k, http.TrailerPrefix) {
			return true
		}
	}
	return false
}

// Header returns the header map. As with net/http, changing it once the status
// is fixed (by WriteHeader or Write) has no effect, but for trailers: the
// first access after that takes a snapshot, which the response goes out with.
func (w *responseWriter) Header() http.Header {
	if w.wroteStatus && !w.wroteHeader && w.frozen == nil {
		w.frozen = w.header.Clone()
	}
	return w.header
}

// hdr returns the header the response goes out with: the handler's, or its
// snapshot (see Header).
func (w *responseWriter) hdr() http.Header {
	if w.frozen != nil {
		return w.frozen
	}
	return w.header
}

// WriteHeader records the status; only the first call counts. The header
// block goes out with the body, or on Flush. A 1xx status (but 101) goes out
// at once as an interim response, such as 103 Early Hints, and the final
// status follows later.
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
		if w.isHead && w.bodyAllowed() && !w.wroteHeader {
			// What GET would send: its length, and its start for the
			// Content-Type.
			w.discarded += int64(len(b))
			if n := sniffLen - len(w.held()); n > 0 {
				w.hold(b[:min(n, len(b))])
			}
		}
		return len(b), nil
	}
	if len(b) == 0 {
		return 0, nil
	}
	if w.wroteHeader {
		if w.contentLength >= 0 && w.written+int64(len(b)) > w.contentLength {
			w.closeConn = true
			return 0, http.ErrContentLength // more than declared: refuse it, as net/http does
		}
		if err := w.writeBody(b); err != nil {
			return 0, err
		}
		return len(b), nil
	}
	framed := w.hasFraming()
	if !framed && len(w.held())+len(b) <= maxBufferedBody {
		w.hold(b)
		return len(b), nil
	}
	if cl := w.declaredLength(); cl >= 0 && int64(len(w.held())+len(b)) > cl {
		w.closeConn = true
		return 0, http.ErrContentLength
	}
	w.streaming = !framed // outgrew the buffer: stream the rest
	err := w.writeHeader(w.held(), b)
	if w.body != nil {
		w.body.Reset()
	}
	if err != nil {
		return 0, err
	}
	return len(b), nil
}

// WriteString is Write for a string (io.StringWriter): a body still held back
// takes it without a conversion.
func (w *responseWriter) WriteString(s string) (int, error) {
	if !w.done.Load() && !w.hijacked && !w.wroteHeader && !w.bodyless() && len(s) > 0 &&
		len(w.held())+len(s) <= maxBufferedBody && !w.hasFraming() {
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
	w.streaming = !w.hasFraming()
	err := w.writeHeader(w.held())
	if w.body != nil {
		w.body.Reset()
	}
	return err
}

// SetReadDeadline sets the deadline for reading the request body, for
// http.ResponseController.
func (w *responseWriter) SetReadDeadline(t time.Time) error { return w.conn.SetReadDeadline(t) }

// SetWriteDeadline sets the deadline for writing the response, for
// http.ResponseController.
func (w *responseWriter) SetWriteDeadline(t time.Time) error { return w.conn.SetWriteDeadline(t) }

// EnableFullDuplex is a no-op for http.ResponseController: the request body
// stays readable while the response is written.
func (w *responseWriter) EnableFullDuplex() error { return nil }

// finish completes the response after the handler returns. A body shorter
// than its declared Content-Length marks the connection for closing: reusing
// it would make the client read the next response as the rest of this one.
func (w *responseWriter) finish() error {
	if w.hijacked {
		return nil
	}
	if !w.wroteHeader {
		// The whole response is known: header and body leave in one write,
		// with its length, unless trailers follow the body, which takes
		// chunked encoding. A HEAD response states the length of the body its
		// handler wrote; one that wrote nothing may have left it out because
		// it saw HEAD, so no length is claimed then (RFC 9110 8.6).
		if w.bodyAllowed() && !w.hasFraming() {
			n := int64(len(w.held()))
			if w.isHead {
				n = w.discarded
			}
			switch {
			case w.hasTrailers() && !w.http10:
				w.streaming = true
			case !w.isHead || n > 0:
				w.hdr().Set("Content-Length", strconv.FormatInt(n, 10))
			}
		} else if cl := w.declaredLength(); cl > int64(len(w.held())) && !w.bodyless() {
			w.closeConn = true // a body shorter than declared: say so up front
		}
		if err := w.writeHeader(w.held()); err != nil {
			return err
		}
	}
	if w.bodyless() {
		return nil
	}
	if w.chunked {
		if err := w.writeLastChunk(); err != nil {
			return err
		}
	}
	if w.contentLength >= 0 && w.written != w.contentLength {
		w.closeConn = true
	}
	return nil
}

// writeHeader sends the status line and header block, followed in the same
// write by body, the first bytes of the body: what was held back, and the
// write that set it going. A HEAD response only looks at them, for its
// Content-Type.
func (w *responseWriter) writeHeader(body ...[]byte) error {
	w.wroteHeader = true
	h := w.hdr()
	te := h.Get("Transfer-Encoding") // the handler's
	if _, ok := h["Content-Type"]; !ok && te == "" && w.bodyAllowed() {
		// As net/http does; a nil Content-Type value opts out.
		for _, b := range body {
			if len(b) > 0 {
				h.Set("Content-Type", http.DetectContentType(b))
				break
			}
		}
	}
	w.contentLength = w.declaredLength()
	w.settleFraming(te)
	total := 0
	if !w.bodyless() {
		for i, b := range body {
			if w.contentLength >= 0 && int64(total+len(b)) > w.contentLength {
				// A Content-Length set after the body was held back, and
				// smaller: what goes beyond it is not sent, as net/http
				// refuses it, and the connection ends with the response.
				body[i], body = b[:w.contentLength-int64(total)], body[:i+1]
				total = int(w.contentLength)
				w.closeConn = true
				break
			}
			total += len(b)
		}
	}
	if _, ok := h["Date"]; !ok { // a nil Date value opts out
		h.Set("Date", httpDate())
	}
	w.settleConnection()

	hb := headPool.Get().(*bytes.Buffer)
	hb.Reset()
	hb.WriteString("HTTP/1.1 ")
	hb.WriteString(strconv.Itoa(w.status))
	hb.WriteByte(' ')
	hb.WriteString(http.StatusText(w.status))
	hb.Write(crlf)
	_ = h.WriteSubset(hb, w.declareTrailers())
	hb.Write(crlf)

	parts := [6][]byte{hb.Bytes()}
	np := 1
	if total > 0 {
		var size [18]byte
		if w.chunked {
			parts[np] = append(strconv.AppendInt(size[:0], int64(total), 16), '\r', '\n')
			np++
		}
		for _, b := range body {
			parts[np] = b
			np++
		}
		if w.chunked {
			parts[np] = crlf
			np++
		}
		w.written += int64(total)
	}
	err := w.send(parts[:np]...)
	headPool.Put(hb)
	return err
}

// settleFraming makes the framing the handler set consistent, as net/http
// does, te being the Transfer-Encoding it set: a status without a body (1xx,
// 204, 304) carries no Content-Length or Transfer-Encoding (304 no
// Content-Type either); an HTTP/1.0 client knows no transfer coding; chunked
// wins over a Content-Length, which wins over any other coding; identity (once
// recommended for event streams) means no coding. A body of unknown length is
// chunked, or, where that cannot be, delimited by closing the connection.
func (w *responseWriter) settleFraming(te string) {
	h := w.hdr()
	switch {
	case !w.bodyAllowed():
		h.Del("Content-Length")
		h.Del("Transfer-Encoding")
		if w.status == http.StatusNotModified {
			h.Del("Content-Type")
		}
		w.contentLength = -1
		return
	case w.http10:
		h.Del("Transfer-Encoding")
	case te == "":
		if w.streaming && w.contentLength < 0 {
			h.Set("Transfer-Encoding", "chunked")
			w.chunked = true
		}
	case strings.EqualFold(te, "chunked"):
		if w.contentLength >= 0 {
			w.srv.logf("fhttp: both Transfer-Encoding %q and Content-Length %d set; dropping the length", te, w.contentLength)
			h.Del("Content-Length")
			w.contentLength = -1
		}
		w.chunked = true
	case w.contentLength >= 0, strings.EqualFold(te, "identity"):
		h.Del("Transfer-Encoding")
	default:
		h.Add("Transfer-Encoding", "chunked")
		w.chunked = true
	}
	if !w.isHead && !w.hijacked && !w.chunked && w.contentLength < 0 {
		w.closeConn = true // nothing else marks where the body ends
	}
}

// settleConnection makes the Connection header say what happens next: close
// after this response, or keep the connection open. A handler's keep-alive
// does not survive a close it cannot see (a closing request, a server
// shutting down, a body the connection's end delimits); a 101, or a header a
// hijacker goes on from (a 2xx to CONNECT, say), keeps the handler's.
func (w *responseWriter) settleConnection() {
	h := w.hdr()
	conn := h.Get("Connection")
	if w.srv.shuttingDown.Load() || hasToken(conn, "close") {
		w.closeConn = true
	}
	switch {
	case w.status == http.StatusSwitchingProtocols, w.hijacked:
	case w.closeConn:
		if !hasToken(conn, "close") {
			h.Set("Connection", "close")
		}
	case conn == "":
		h.Set("Connection", "keep-alive")
	}
}

// declareTrailers notes the trailer fields the header declares, and returns
// the header keys to leave out of the header block, nil when there are none:
// the http.TrailerPrefix ones, and, when a trailer section follows, the
// declared ones. The header goes out late, so a trailer value the handler set
// after writing the body is already in the map; it belongs in the trailer
// section alone.
func (w *responseWriter) declareTrailers() map[string]bool {
	var exclude map[string]bool
	skip := func(k string) {
		if exclude == nil {
			exclude = make(map[string]bool)
		}
		exclude[k] = true
	}
	for k := range w.hdr() {
		if strings.HasPrefix(k, http.TrailerPrefix) {
			skip(k)
		}
	}
	for _, v := range w.hdr()["Trailer"] {
		for name := range strings.SplitSeq(v, ",") {
			if name = http.CanonicalHeaderKey(strings.TrimSpace(name)); name != "" && !badTrailer[name] {
				w.trailers = append(w.trailers, name)
				if w.chunked {
					skip(name)
				}
			}
		}
	}
	return exclude
}

// writeLastChunk ends a chunked body, with the trailer section if the handler
// set trailer fields: the declared ones, and the http.TrailerPrefix ones but
// those a trailer must not carry.
func (w *responseWriter) writeLastChunk() error {
	var t http.Header
	for k, vv := range w.header {
		if name, ok := strings.CutPrefix(k, http.TrailerPrefix); ok {
			if name = http.CanonicalHeaderKey(name); badTrailer[name] {
				continue
			}
			if t == nil {
				t = make(http.Header)
			}
			t[name] = vv
		}
	}
	for _, k := range w.trailers {
		for _, v := range w.header[k] {
			if t == nil {
				t = make(http.Header)
			}
			t.Add(k, v)
		}
	}
	if t == nil {
		return w.send(lastChunk)
	}
	hb := headPool.Get().(*bytes.Buffer)
	hb.Reset()
	hb.WriteString("0\r\n")
	_ = t.Write(hb)
	hb.Write(crlf)
	err := w.send(hb.Bytes())
	headPool.Put(hb)
	return err
}

func (w *responseWriter) writeBody(b []byte) error {
	w.written += int64(len(b))
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

// Hijack implements http.Hijacker. As with net/http, a status the handler set
// goes out first, with the header (a 101, say, or a 200 to CONNECT, for a
// protocol the hijacker then speaks); the hijacker owns what follows, so the
// header gets no framing and no Connection of fhttp's (RFC 9110 9.3.6). A
// body not sent yet is dropped. Bytes the request reader buffered ahead are
// served first by the returned connection.
func (w *responseWriter) Hijack() (net.Conn, *bufio.ReadWriter, error) {
	if w.done.Load() {
		return nil, nil, errFinished
	}
	if w.hijacked {
		return nil, nil, errDoubleHijack
	}
	w.hijacked = true
	if w.wroteStatus && !w.wroteHeader {
		if err := w.writeHeader(); err != nil {
			w.hijacked = false // not taken over: the connection is closed as usual
			return nil, nil, err
		}
	}
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

// hasToken reports whether the comma-separated list v holds token, in any case.
func hasToken(v, token string) bool {
	for t := range strings.SplitSeq(v, ",") {
		if strings.EqualFold(strings.TrimSpace(t), token) {
			return true
		}
	}
	return false
}

var (
	_ http.ResponseWriter = (*responseWriter)(nil)
	_ http.Flusher        = (*responseWriter)(nil)
	_ http.Hijacker       = (*responseWriter)(nil)
	_ io.Reader           = (*hijackedConn)(nil)
)
