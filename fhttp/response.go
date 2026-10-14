package fhttp

import (
	"fmt"
	"net/http"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/linfeip/fnet/internal/bytepool"
	"github.com/linfeip/fnet/internal/units"
)

// maxCopiedBody is the largest response body copied next to its header into the connection's output; a larger one is
// written right away with a single writev of the output and the body, so it is not copied.
const maxCopiedBody = 4 * units.KB

// initialOutput is the capacity the output starts with: a run of pipelined responses to small requests fits, so it
// seldom has to grow.
const initialOutput = 2 * units.KB

// maxBufferedOutput is how much output is collected before it is written out without waiting for the queue to be
// found empty (see conn.serve).
const maxBufferedOutput = 64 * units.KB

// response implements http.ResponseWriter. The response body is buffered in full first, then serialized with
// Content-Length after the Handler returns (Flusher/Hijacker are not supported). A worker uses one response for the
// requests it takes from a connection's queue in turn, and its output collects their responses (see conn.serve).
type response struct {
	conn        *conn
	req         *http.Request
	header      http.Header
	status      int
	wroteHeader bool
	upgraded    bool // Upgrade has been called, so finish no longer writes the response
	body        bytepool.Buffer
	out         bytepool.Buffer // the responses serialized and not written out yet, see flush
}

var responsePool = sync.Pool{New: func() any { return &response{header: make(http.Header)} }}

// start readies w for the response to req.
func (w *response) start(req *http.Request) {
	clear(w.header)
	w.req, w.status, w.wroteHeader, w.upgraded = req, 0, false, false
	w.body.Reset()
}

func (w *response) release() {
	w.body.Release()
	w.out.Release()
	clear(w.header)
	*w = response{header: w.header}
	responsePool.Put(w)
}

// flush writes out the responses collected so far in one write.
func (w *response) flush() {
	if w.out.Len() > 0 {
		w.conn.connection.Write(w.out.Bytes())
	}
	w.out.Release()
}

// writeError puts a response with code, which closes the connection, into the output.
func (w *response) writeError(code int) {
	text := strconv.Itoa(code) + " " + http.StatusText(code)
	w.out.AppendString("HTTP/1.1 " + text + "\r\nContent-Type: text/plain; charset=utf-8\r\nConnection: close\r\n" +
		"Content-Length: " + strconv.Itoa(len(text)) + "\r\n\r\n" + text)
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

// finish serializes the response into the output after the Handler returns, and reports whether to keep the
// connection alive.
func (w *response) finish() (keepAlive bool) {
	if !w.wroteHeader {
		w.WriteHeader(http.StatusOK)
	}
	req := w.req
	http11 := req.ProtoAtLeast(1, 1)
	isHEAD := req.Method == http.MethodHead
	bodyAllowed := bodyAllowedForStatus(w.status)
	keepLength := isHEAD && bodyAllowed // HEAD keeps the Content-Length declared by the Handler
	// One pass over the Handler's fields finds those finish decides on, and the names it writes out: Transfer-Encoding
	// and Content-Length are decided here (the body is buffered in full, so its actual length is used), and a 304 has no
	// Content-Type.
	var stack [16]field
	fields := stack[:0]
	var hasLength, hasType, hasDate, hasConnection, encoded, closing bool
	for name, values := range w.header {
		switch name {
		case "Transfer-Encoding":
			continue
		case "Content-Length":
			if hasLength = true; !keepLength {
				continue
			}
		case "Content-Type":
			if hasType = true; w.status == http.StatusNotModified {
				continue
			}
		case "Content-Encoding":
			encoded = len(values) > 0 && values[0] != ""
		case "Date":
			hasDate = true
		case "Connection":
			hasConnection, closing = true, hasToken(values, "close")
		}
		if validName(name) {
			fields = append(fields, field{name, values})
		}
	}
	keepAlive = !req.Close && !closing

	b := &w.out
	if cap(b.Bytes()) == 0 {
		*b = bytepool.Get(initialOutput)
	}
	if http11 && w.status == http.StatusOK {
		b.AppendString("HTTP/1.1 200 OK\r\n") // the usual status line, in one piece
	} else {
		appendStatusLine(b, http11, w.status)
	}
	if bodyAllowed && (!isHEAD || !hasLength && w.body.Len() > 0) {
		appendContentLength(b, w.body.Len())
	}
	if !hasType && bodyAllowed && w.body.Len() > 0 && !encoded {
		b.AppendString("Content-Type: ")
		b.AppendString(http.DetectContentType(w.body.Bytes()))
		b.AppendString("\r\n")
	}
	if !hasDate {
		appendDate(b)
	}
	if !hasConnection {
		if !keepAlive {
			b.AppendString("Connection: close\r\n")
		} else if !http11 {
			b.AppendString("Connection: keep-alive\r\n")
		}
	}
	appendFields(b, fields)
	b.AppendString("\r\n")

	body := w.body.Bytes()
	if isHEAD || !bodyAllowed {
		body = nil
	}
	if len(body) > maxCopiedBody { // written right away after the output so far, without being copied
		w.conn.connection.Writev([][]byte{b.Bytes(), body})
		b.Release()
	} else {
		b.Append(body)
	}
	if b.Len() >= maxBufferedOutput {
		w.flush()
	}
	return keepAlive
}

// field is a name of a header with its values, see appendFields.
type field struct {
	name   string
	values []string
}

// appendHeader appends the fields of h the way http.Header.Write writes them, see appendFields.
func appendHeader(b *bytepool.Buffer, h http.Header) {
	var stack [16]field
	fields := stack[:0]
	for name, values := range h {
		if validName(name) {
			fields = append(fields, field{name, values})
		}
	}
	appendFields(b, fields)
}

// appendFields appends fields, whose names are tokens, the way http.Header.WriteSubset writes them: sorted by name, and
// with the CR and LF of a value turned into spaces (after trimming the surrounding whitespace), so that a Handler cannot
// inject fields.
func appendFields(b *bytepool.Buffer, fields []field) {
	if len(fields) > 1 {
		slices.SortFunc(fields, func(a, b field) int { return strings.Compare(a.name, b.name) })
	}
	for _, f := range fields {
		for _, v := range f.values {
			b.AppendString(f.name)
			b.AppendString(": ")
			for v != "" && isFieldSpace(v[0]) {
				v = v[1:]
			}
			for v != "" && isFieldSpace(v[len(v)-1]) {
				v = v[:len(v)-1]
			}
			for i := 0; i < len(v); i++ {
				if v[i] == '\r' || v[i] == '\n' {
					b.AppendString(v[:i])
					b.AppendString(" ")
					v, i = v[i+1:], -1
				}
			}
			b.AppendString(v)
			b.AppendString("\r\n")
		}
	}
}

// isFieldSpace reports whether c is one of the bytes http.Header.Write trims off a value.
func isFieldSpace(c byte) bool { return c == ' ' || c == '\t' || c == '\r' || c == '\n' }

// appendStatusLine appends the status line of a response with code status to a request of HTTP/1.1, or else 1.0.
func appendStatusLine(b *bytepool.Buffer, http11 bool, status int) {
	if http11 {
		b.AppendString("HTTP/1.1 ")
	} else {
		b.AppendString("HTTP/1.0 ")
	}
	appendInt(b, status)
	b.AppendString(" ")
	if text := http.StatusText(status); text != "" {
		b.AppendString(text)
	} else {
		b.AppendString("status code " + strconv.Itoa(status))
	}
	b.AppendString("\r\n")
}

func appendContentLength(b *bytepool.Buffer, n int) {
	b.AppendString("Content-Length: ")
	appendInt(b, n)
	b.AppendString("\r\n")
}

// appendInt appends the decimal representation of n.
func appendInt(b *bytepool.Buffer, n int) {
	var digits [20]byte
	b.Append(strconv.AppendInt(digits[:0], int64(n), 10))
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

// The Date field line, "Date: ...\r\n", is formatted at the start of every second by a goroutine started with the first
// response (see keepDate), so that a response only appends it.
var (
	dateField atomic.Pointer[[]byte]
	dateOnce  sync.Once
)

// appendDate appends the Date field line of the current second.
func appendDate(b *bytepool.Buffer) {
	dateOnce.Do(keepDate)
	b.Append(*dateField.Load())
}

// keepDate formats the Date field line now, then has it formatted again at the start of every second.
func keepDate() {
	refresh := func() time.Time {
		now := time.Now()
		field := append(now.UTC().AppendFormat([]byte("Date: "), http.TimeFormat), "\r\n"...)
		dateField.Store(&field)
		return now
	}
	now := refresh()
	go func() {
		for {
			time.Sleep(time.Second - time.Duration(now.Nanosecond()))
			now = refresh()
		}
	}()
}
