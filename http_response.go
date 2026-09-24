package fnet

import (
	"bufio"
	"bytes"
	"errors"
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
	responseWriterPool = sync.Pool{New: func() any {
		return &responseWriter{header: make(http.Header, 8)}
	}}
	readerPool = sync.Pool{New: func() any { return bufio.NewReaderSize(nil, 4<<10) }}
	writerPool = sync.Pool{New: func() any { return bufio.NewWriterSize(nil, 4<<10) }}
	headPool   = sync.Pool{New: func() any { return new(bytes.Buffer) }}

	crlf            = []byte("\r\n")
	continue100     = []byte("HTTP/1.1 100 Continue\r\n\r\n")
	lastChunk       = []byte("0\r\n\r\n")
	errUnwrapped    = errors.New("fnet: connection was moved to the event loop")
	errDoubleHijack = errors.New("fnet: connection already hijacked")
)

// responseWriter implements http.ResponseWriter and http.Hijacker for one
// request. It is not safe for concurrent use, like net/http's.
type responseWriter struct {
	conn net.Conn      // response stream: the reactor conn, or TLS on top of it
	br   *bufio.Reader // request reader, handed to a hijacker
	raw  *reactor.Conn // the unencrypted connection; nil under TLS
	hc   *hijackedConn

	header      http.Header
	status      int
	body        bytes.Buffer
	discarded   int64 // body bytes a bodyless response swallowed, for HEAD's Content-Length
	wroteStatus bool  // the first WriteHeader (or Write) fixed the status
	wroteHeader bool  // the header block is on the wire
	chunked     bool  // the body goes out chunk-encoded
	hijacked    bool
	closeConn   bool // close after this response
	isHead      bool
	http10      bool // the client speaks HTTP/1.0: no chunked encoding
}

func newResponseWriter(conn net.Conn, br *bufio.Reader, raw *reactor.Conn) *responseWriter {
	w := responseWriterPool.Get().(*responseWriter)
	w.conn, w.br, w.raw = conn, br, raw
	w.status = http.StatusOK
	return w
}

func (w *responseWriter) release() {
	h, body := w.header, w.body
	clear(h)
	body.Reset()
	if body.Cap() > 2*maxBufferedBody {
		body = bytes.Buffer{}
	}
	*w = responseWriter{header: h, body: body}
	responseWriterPool.Put(w)
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
	if w.wroteHeader || w.hijacked || w.br.Buffered() > 0 {
		return false
	}
	return w.raw == nil || w.raw.Buffered() == 0
}

// hasFraming reports whether the handler chose the framing itself.
func (w *responseWriter) hasFraming() bool {
	return w.header.Get("Content-Length") != "" || w.header.Get("Transfer-Encoding") != ""
}

func (w *responseWriter) Header() http.Header { return w.header }

// WriteHeader records the status; only the first call counts. With explicit
// framing the header block is sent at once, otherwise when the body is known.
func (w *responseWriter) WriteHeader(code int) {
	if w.hijacked || w.wroteStatus {
		return
	}
	w.wroteStatus = true
	w.status = code
	if w.hasFraming() {
		_ = w.writeHeader(nil)
	}
}

func (w *responseWriter) Write(b []byte) (int, error) {
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
			if w.body.Len()+len(b) <= maxBufferedBody {
				return w.body.Write(b)
			}
			// Outgrew the buffer: stream the rest, chunked, or delimited by
			// closing the connection for an HTTP/1.0 client.
			if w.http10 {
				w.closeConn = true
			} else {
				w.header.Set("Transfer-Encoding", "chunked")
			}
		}
		if err := w.writeHeader(nil); err != nil {
			return 0, err
		}
		if w.body.Len() > 0 {
			err := w.writeBody(w.body.Bytes())
			w.body.Reset()
			if err != nil {
				return 0, err
			}
		}
	}
	if err := w.writeBody(b); err != nil {
		return 0, err
	}
	return len(b), nil
}

// finish completes the response after the handler returns.
func (w *responseWriter) finish() error {
	if w.hijacked {
		return nil
	}
	if !w.wroteHeader {
		// The whole response is known: header and body leave in one write.
		if w.allowsContentLength() && !w.hasFraming() {
			n := int64(w.body.Len())
			if w.bodyless() {
				n = w.discarded
			}
			w.header.Set("Content-Length", strconv.FormatInt(n, 10))
		}
		var body []byte
		if !w.bodyless() {
			body = w.body.Bytes()
		}
		err := w.writeHeader(body)
		w.body.Reset()
		if err != nil {
			return err
		}
	}
	if w.chunked && !w.bodyless() {
		return w.send(lastChunk)
	}
	return nil
}

// writeHeader sends the status line and header block, followed by body.
func (w *responseWriter) writeHeader(body []byte) error {
	w.wroteHeader = true
	h := w.header
	w.chunked = h.Get("Transfer-Encoding") == "chunked"
	if h.Get("Date") == "" {
		h.Set("Date", httpDate())
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
	if w.hijacked {
		return nil, nil, errDoubleHijack
	}
	w.hijacked = true
	if w.br == nil {
		w.br = bufio.NewReader(w.conn)
	}
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
	_ http.Hijacker       = (*responseWriter)(nil)
	_ io.Reader           = (*hijackedConn)(nil)
)
