package fhttp

import (
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/linfeip/fnet/internal/bytepool"
	"github.com/linfeip/fnet/internal/units"
)

// response implements http.ResponseWriter. The response body is buffered in full first, then written out all at
// once with Content-Length after the Handler returns (Flusher/Hijacker are not supported).
type response struct {
	conn        *conn
	req         *http.Request
	header      http.Header
	status      int
	wroteHeader bool
	upgraded    bool // Upgrade has been called, so finish no longer writes the response
	body        bytepool.Buffer
	buf         bufWriter // serializes the status line and headers
}

var responsePool = sync.Pool{New: func() any { return &response{header: make(http.Header)} }}

func newResponse(c *conn, req *http.Request) *response {
	w := responsePool.Get().(*response)
	w.conn, w.req = c, req
	return w
}

func (w *response) release() {
	w.body.Release()
	clear(w.header)
	*w = response{header: w.header}
	responsePool.Put(w)
}

func (w *response) Header() http.Header { return w.header }

func (w *response) WriteHeader(code int) {
	if code < 100 || code > 999 {
		panic(fmt.Sprintf("fhttp: invalid WriteHeader code %v", code))
	}
	if w.wroteHeader || code < 200 { // 1xx informational responses are not supported yet, so ignore them
		return
	}
	w.wroteHeader, w.status = true, code
}

func (w *response) Write(p []byte) (int, error) {
	if err := w.beforeWrite(); err != nil {
		return 0, err
	}
	w.body.Append(p)
	return len(p), nil
}

func (w *response) WriteString(s string) (int, error) {
	if err := w.beforeWrite(); err != nil {
		return 0, err
	}
	w.body.AppendString(s)
	return len(s), nil
}

// beforeWrite fills in the default 200 status code before the response body is written; it returns
// http.ErrBodyNotAllowed when the status code does not allow a body.
func (w *response) beforeWrite() error {
	if !w.wroteHeader {
		w.WriteHeader(http.StatusOK)
	}
	if !bodyAllowedForStatus(w.status) {
		return http.ErrBodyNotAllowed
	}
	return nil
}

// Fields that, in each case, are not emitted from the Handler's headers but are decided by finish or must be
// dropped.
var (
	excludeTE     = map[string]bool{"Transfer-Encoding": true}
	excludeCLTE   = map[string]bool{"Content-Length": true, "Transfer-Encoding": true}
	excludeCTCLTE = map[string]bool{"Content-Type": true, "Content-Length": true, "Transfer-Encoding": true}
)

// finish serializes and writes out the response after the Handler returns, and reports whether to keep the
// connection alive.
func (w *response) finish() (keepAlive bool) {
	if !w.wroteHeader {
		w.WriteHeader(http.StatusOK)
	}
	req, h := w.req, w.header
	http11 := req.ProtoAtLeast(1, 1)
	isHEAD := req.Method == http.MethodHead
	bodyAllowed := bodyAllowedForStatus(w.status)
	keepAlive = !req.Close && !hasToken(h["Connection"], "close")

	b := &w.buf
	b.buffer = bytepool.Get(units.KB / 2)
	if http11 {
		b.WriteString("HTTP/1.1 ")
	} else {
		b.WriteString("HTTP/1.0 ")
	}
	b.writeInt(w.status)
	b.WriteString(" ")
	if text := http.StatusText(w.status); text != "" {
		b.WriteString(text)
	} else {
		b.WriteString("status code " + strconv.Itoa(w.status))
	}
	b.WriteString("\r\n")

	var exclude map[string]bool
	switch {
	case w.status == http.StatusNotModified:
		exclude = excludeCTCLTE
	case !bodyAllowed:
		exclude = excludeCLTE
	case isHEAD: // HEAD keeps the Content-Length declared by the Handler
		exclude = excludeTE
		if _, ok := h["Content-Length"]; !ok && w.body.Len() > 0 {
			b.writeContentLength(w.body.Len())
		}
	default: // the response body is fully buffered, so always use its actual length
		exclude = excludeCLTE
		b.writeContentLength(w.body.Len())
	}
	if _, ok := h["Content-Type"]; !ok && bodyAllowed && w.body.Len() > 0 && h.Get("Content-Encoding") == "" {
		b.WriteString("Content-Type: ")
		b.WriteString(http.DetectContentType(w.body.Bytes()))
		b.WriteString("\r\n")
	}
	if _, ok := h["Date"]; !ok {
		b.WriteString("Date: ")
		appendDate(&b.buffer)
		b.WriteString("\r\n")
	}
	if _, ok := h["Connection"]; !ok {
		if !keepAlive {
			b.WriteString("Connection: close\r\n")
		} else if !http11 {
			b.WriteString("Connection: keep-alive\r\n")
		}
	}
	h.WriteSubset(b, exclude)
	b.WriteString("\r\n")

	body := w.body.Bytes()
	if isHEAD || !bodyAllowed {
		body = nil
	}
	// The headers and the response body are written with a single writev, so the body needs no copy.
	w.conn.connection.Writev([][]byte{b.buffer.Bytes(), body})
	b.buffer.Release()
	return keepAlive
}

// bufWriter appends to a byte slice and implements io.Writer and io.StringWriter for use by Header.WriteSubset.
type bufWriter struct{ buffer bytepool.Buffer }

func (w *bufWriter) Write(p []byte) (int, error) {
	w.buffer.Append(p)
	return len(p), nil
}

func (w *bufWriter) WriteString(s string) (int, error) {
	w.buffer.AppendString(s)
	return len(s), nil
}

func (w *bufWriter) writeContentLength(n int) {
	w.WriteString("Content-Length: ")
	w.writeInt(n)
	w.WriteString("\r\n")
}

// writeInt writes the decimal representation of n.
func (w *bufWriter) writeInt(n int) {
	var digits [20]byte
	w.Write(strconv.AppendInt(digits[:0], int64(n), 10))
}

func bodyAllowedForStatus(status int) bool {
	return status >= 200 && status != http.StatusNoContent && status != http.StatusNotModified
}

// hasToken reports whether the comma-separated header values contain token (case-insensitive).
func hasToken(values []string, token string) bool {
	for _, v := range values {
		for v != "" {
			var t string
			t, v, _ = strings.Cut(v, ",")
			if strings.EqualFold(strings.TrimSpace(t), token) {
				return true
			}
		}
	}
	return false
}

// The Date header is formatted only once per second.
type dateValue struct {
	unixSeconds int64
	text        []byte
}

var dateCache atomic.Pointer[dateValue]

func appendDate(b *bytepool.Buffer) {
	now := time.Now()
	d := dateCache.Load()
	if d == nil || d.unixSeconds != now.Unix() {
		d = &dateValue{unixSeconds: now.Unix(), text: now.UTC().AppendFormat(nil, http.TimeFormat)}
		dateCache.Store(d)
	}
	b.Append(d.text)
}
