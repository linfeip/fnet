package fhttp

import (
	"bufio"
	"bytes"
	"fmt"
	"net"
	"net/http"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/linfeip/fnet/internal/bytepool"
	"github.com/linfeip/fnet/internal/httpx"
)

// maxCopiedBody is the largest response body copied next to its header into the connection's output; a larger one is
// written right away with a single writev of the output and the body, so it is not copied.
const maxCopiedBody = 4 << 10

// initialOutput is the capacity the output starts with: a run of pipelined responses to small requests fits, so it
// seldom has to grow.
const initialOutput = 2 << 10

// maxBufferedOutput is how much output is collected before it is written out without waiting for the queue to be
// found empty (see conn.serve).
const maxBufferedOutput = 64 << 10

// maxBufferedBody is the largest response body buffered until the Handler returns, to go out with its actual
// Content-Length; a larger one goes out in pieces of this size as the Handler writes it (see stream), so that no more
// than this of it waits in the response, and twice as much in fnet (see conn.waitWritable). Not to be confused with
// Options.MaxBufferedBodyBytes, which is about request bodies.
const maxBufferedBody = 64 << 10

// crlf ends the data of a chunk, and lastChunk the data of the last one, followed by the zero-size chunk and the empty
// trailer section that end a chunked body (RFC 9112 7.1).
var (
	crlf      = []byte("\r\n")
	lastChunk = []byte("\r\n0\r\n\r\n")
)

// response implements http.ResponseWriter. A body of up to maxBufferedBody is buffered, then serialized with its
// Content-Length after the Handler returns; a larger one is streamed as the Handler writes it (see stream).
// Flusher/Hijacker are not supported. A Write after the connection closes returns net.ErrClosed. A worker uses one
// response for the requests it takes from a connection's queue in turn, and its output collects their responses (see
// conn.serve).
type response struct {
	conn           *conn
	req            http.Request
	bufferedReader *bufio.Reader
	bytesReader    bytes.Reader
	header         http.Header
	status         int
	writeErr       error
	body           bytepool.Buffer
	out            bytepool.Buffer // the responses serialized and not written out yet, see flush
	writeBuffers   [3][]byte       // scratch slice backing for Writev in send and finish, avoiding an allocation per call
	outStart       int             // the length of out before the current response
	remaining      int             // what the Handler may still write of the Content-Length it declared, -1 for none
	committed      committedHeader // what commitHeader took of the Handler's header
	bodyWriting    bodyWriting     // how the body goes out, decided with the header by writeHeader
	wroteHeader    bool
	upgraded       bool // Upgrade has been called, so finish no longer writes the response
	streamed       bool // the header has gone out, the body following in pieces as the Handler writes it (see stream)
}

// committedHeader is what commitHeader takes of the Handler's header when the status is written; later changes to the
// header do not count.
type committedHeader struct {
	connection     []string // the values of the Connection field, which goes out only if the body leaves the connection open
	declaredLength int      // the Content-Length the Handler declared, -1 for none
	hasType        bool     // the header has a Content-Type
	hasDate        bool     // the header has a Date
	encoded        bool     // the header has a Content-Encoding
	closing        bool     // the Connection field says close
}

// bodyWriting is how writeHeader decides the body goes out.
type bodyWriting struct {
	chunked     bool // the streamed body goes out in chunks
	discardBody bool // the response has no content (HEAD, 205, a tunnel): what is written of the body is dropped
	keepAlive   bool // whether to keep the connection alive
}

var responsePool = sync.Pool{New: func() any {
	w := &response{header: make(http.Header)}
	w.bufferedReader = bufio.NewReaderSize(&w.bytesReader, 4<<10)
	return w
}}

// start readies w for the response to w.req.
func (w *response) start() {
	if len(w.header) > 0 {
		clear(w.header)
	}
	w.status, w.wroteHeader, w.upgraded, w.streamed, w.writeErr = 0, false, false, false, nil
	w.outStart = w.out.Len()
	w.body.Reset()
}

func (w *response) release() {
	resetOrRelease(&w.body)
	resetOrRelease(&w.out)
	if len(w.header) > 0 {
		clear(w.header)
	}
	w.conn = nil
	w.req = http.Request{}
	w.writeErr = nil
	w.committed = committedHeader{}
	responsePool.Put(w)
}

// resetOrRelease keeps b on w while its capacity is within maxBufferedOutput, and returns a larger buffer to bytepool.
func resetOrRelease(b *bytepool.Buffer) {
	if cap(b.Bytes()) <= maxBufferedOutput {
		b.Reset()
	} else {
		b.Release()
	}
}

// flush writes out the responses collected so far in one write.
func (w *response) flush() error {
	var err error
	if w.out.Len() > 0 {
		_, err = w.conn.connection.Write(w.out.Bytes()) // net.ErrClosed once the connection is closing
	}
	resetOrRelease(&w.out)
	if err != nil && w.writeErr == nil {
		w.writeErr = err
	}
	return err
}

// writeError puts a response with code, which closes the connection, into the output. It has a Date as well, which RFC
// 9110 6.6.1 requires in a 4xx response.
func (w *response) writeError(code int) {
	b := &w.out
	text := http.StatusText(code)
	appendStatusLine(b, code)
	b.WriteString("Content-Type: text/plain; charset=utf-8\r\nConnection: close\r\n")
	appendContentLength(b, 4+len(text))
	appendDate(b)
	b.WriteString("\r\n")
	appendInt(b, code)
	b.WriteString(" ")
	b.WriteString(text)
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
	w.commitHeader()
}

func (w *response) Write(p []byte) (int, error) {
	if err := w.beforeWrite(len(p)); err != nil {
		return 0, err
	}
	if w.body.Len()+len(p) > maxBufferedBody {
		return w.stream(p)
	}
	w.body.Write(p)
	return len(p), nil
}

func (w *response) WriteString(s string) (int, error) {
	if err := w.beforeWrite(len(s)); err != nil {
		return 0, err
	}
	if w.body.Len()+len(s) > maxBufferedBody {
		return w.streamString(s)
	}
	w.body.WriteString(s)
	return len(s), nil
}

// beforeWrite readies the response for n more bytes of its body. It fills in the default 200 status code, and returns
// http.ErrBodyNotAllowed when the status code does not allow a body, and http.ErrContentLength, as on net/http, when the
// n bytes would go past the Content-Length the Handler declared: none of them are written then.
func (w *response) beforeWrite(n int) error {
	if w.writeErr != nil {
		return w.writeErr
	}
	if w.conn.closed.Load() {
		w.writeErr = net.ErrClosed
		return w.writeErr
	}
	if !w.wroteHeader {
		w.WriteHeader(http.StatusOK)
	}
	if !bodyAllowedForStatus(w.status) {
		return http.ErrBodyNotAllowed
	}
	if w.remaining >= 0 {
		if n > w.remaining {
			return http.ErrContentLength
		}
		w.remaining -= n
	}
	return nil
}

// stream writes out the body buffered so far and p in pieces of maxBufferedBody, the way bufio.Writer does: the buffer is
// filled up first, whole pieces of p go out without being copied, and the rest of p, at least a byte, stays buffered,
// so that finish has a last piece to end the body with.
func (w *response) stream(p []byte) (int, error) {
	n := len(p)
	for w.body.Len()+len(p) > maxBufferedBody {
		var piece []byte
		if w.body.Len() == 0 {
			piece, p = p[:maxBufferedBody], p[maxBufferedBody:]
		} else {
			fill := maxBufferedBody - w.body.Len()
			w.body.Write(p[:fill])
			piece, p = w.body.Bytes(), p[fill:]
		}
		err := w.send(piece)
		w.body.Reset()
		if err != nil {
			return n - len(p), err
		}
	}
	w.body.Write(p)
	return n, nil
}

// streamString is stream for a string: copied into the buffer a piece at a time without allocating a slice for the whole string.
func (w *response) streamString(s string) (int, error) {
	n := len(s)
	for w.body.Len()+len(s) > maxBufferedBody {
		fill := maxBufferedBody - w.body.Len()
		w.body.WriteString(s[:fill])
		s = s[fill:]
		err := w.send(w.body.Bytes())
		w.body.Reset()
		if err != nil {
			return n - len(s), err
		}
	}
	w.body.WriteString(s)
	return n, nil
}

// send writes out a piece of the streamed body, after the header when it is the first piece, once the client has taken
// enough of what went before (see conn.waitWritable).
func (w *response) send(piece []byte) error {
	if !w.streamed {
		w.writeHeader(piece, true) // into the output, after the responses collected before it
	}
	if w.bodyWriting.discardBody {
		return nil
	}
	if err := w.conn.waitWritable(); err != nil {
		w.writeErr = err
		return err
	}
	var end []byte
	if w.bodyWriting.chunked {
		appendChunkSize(&w.out, len(piece))
		end = crlf
	}
	err := w.writev(w.out.Bytes(), piece, end)
	w.out.Release()
	if err != nil {
		w.writeErr = err
	}
	return err
}

// writev sends the segments in order with a single Writev, reusing w.writeBuffers so no slice is allocated.
func (w *response) writev(first, second, third []byte) error {
	w.writeBuffers = [3][]byte{first, second, third}
	_, err := w.conn.connection.Writev(w.writeBuffers[:])
	w.writeBuffers = [3][]byte{}
	return err
}

// finish puts the rest of the response into the output after the Handler returns, the header first unless it went out
// already, and reports whether to keep the connection alive.
func (w *response) finish() bool {
	if w.writeErr != nil {
		return false
	}
	if !w.streamed {
		w.writeHeader(w.body.Bytes(), false)
	}
	body, end, keepAlive := w.body.Bytes(), []byte(nil), w.bodyWriting.keepAlive
	switch {
	case w.bodyWriting.discardBody:
		body = nil
	case w.streamed && w.remaining > 0:
		// The Handler wrote less than the Content-Length it declared: what goes out ends there, and so does the connection.
		keepAlive = false
	case w.bodyWriting.chunked: // body holds at least a byte, see stream
		appendChunkSize(&w.out, len(body))
		end = lastChunk
	}
	b := &w.out
	if len(body) > maxCopiedBody { // written right away after the output so far, without being copied
		if err := w.writev(b.Bytes(), body, end); err != nil {
			w.writeErr = err
		}
		b.Release()
	} else {
		b.Write(body)
		b.Write(end)
	}
	if b.Len() >= maxBufferedOutput {
		w.flush()
	}
	return keepAlive && w.writeErr == nil
}

// bodyAllowed reports whether the response has content. A 2xx response to CONNECT turns the connection into a tunnel
// right after its header (RFC 9110 9.3.6), which fhttp does not provide: it has no content and no Content-Length (8.6).
func (w *response) bodyAllowed() bool {
	return bodyAllowedForStatus(w.status) && !(w.req.Method == http.MethodConnect && w.status/100 == 2)
}

// commitHeader fixes the Handler's header as it is when the status is written, writing the status line and the
// Handler's fields directly into w.out.
func (w *response) commitHeader() {
	b := &w.out
	if cap(b.Bytes()) == 0 {
		*b = bytepool.Get(initialOutput)
	}
	appendStatusLine(b, w.status)
	committed := committedHeader{declaredLength: -1}
	if len(w.header) > 0 {
		for name, values := range w.header {
			switch name {
			case "Transfer-Encoding":
				continue
			case "Content-Length":
				committed.declaredLength = declaredLength(values)
				continue
			case "Content-Type":
				if w.status == http.StatusNotModified {
					continue
				}
				committed.hasType = true
			case "Date":
				committed.hasDate = true
			case "Content-Encoding":
				committed.encoded = len(values) > 0 && values[0] != ""
			case "Connection":
				committed.closing, committed.connection = httpx.HasToken(values, "close"), slices.Clone(values)
				continue
			default:
				if !validName(name) {
					continue
				}
			}
			appendHeaderValues(b, name, values)
		}
	}
	w.committed, w.remaining = committed, committed.declaredLength
}

// appendHeaderValues appends the field lines for name and values, trimming surrounding whitespace and turning any
// CR or LF inside a value into a space, the way http.Header.Write does, so a Handler cannot inject fields.
func appendHeaderValues(b *bytepool.Buffer, name string, values []string) {
	for _, v := range values {
		v = strings.Trim(v, " \t\r\n")
		b.WriteString(name)
		b.WriteString(": ")
		for i := strings.IndexAny(v, "\r\n"); i >= 0; i = strings.IndexAny(v, "\r\n") {
			b.WriteString(v[:i])
			b.WriteString(" ")
			v = v[i+1:]
		}
		b.WriteString(v)
		b.WriteString("\r\n")
	}
}

// writeHeader finishes serializing the response header into the output and decides how the body goes out and whether to
// keep the connection alive; content is the start of the body, to detect its type from. A streamed body (see stream) has
// the Content-Length the Handler declared, if any; otherwise its length is not known, and it goes out in chunks on
// HTTP/1.1, and until the connection closes on HTTP/1.0.
func (w *response) writeHeader(content []byte, streamed bool) {
	if !w.wroteHeader {
		w.WriteHeader(http.StatusOK)
	}
	req := &w.req
	http11 := req.ProtoAtLeast(1, 1)
	isHEAD := req.Method == http.MethodHead
	bodyAllowed := w.bodyAllowed()
	if w.status == http.StatusResetContent {
		w.body.Reset() // a 205 has no content (RFC 9110 15.3.6), so it goes out with Content-Length: 0
		content = nil
	}
	committed := &w.committed
	writing := bodyWriting{discardBody: isHEAD || !bodyAllowed || w.status == http.StatusResetContent}
	closeDelimited := false
	if streamed && !writing.discardBody {
		writing.chunked = committed.declaredLength < 0 && http11
		closeDelimited = committed.declaredLength < 0 && !http11 // HTTP/1.0 has no chunks
	}
	// Whatever its status, the response to a CONNECT closes the connection: after a rejection, a client or an intermediary
	// that takes the request for one that opens a tunnel goes on sending the data of that tunnel, which the server would
	// read as requests of their own.
	mustClose := req.Close || req.Method == http.MethodConnect || closeDelimited
	writing.keepAlive = !mustClose && !committed.closing
	w.streamed, w.bodyWriting = streamed, writing
	handlerConnection := committed.connection != nil && (committed.closing || !mustClose)

	b := &w.out
	switch {
	case !bodyAllowed:
	case w.status == http.StatusResetContent:
		appendContentLength(b, 0)
	case committed.declaredLength >= 0 && (isHEAD || streamed):
		appendContentLength(b, committed.declaredLength)
	case !streamed:
		if !isHEAD || w.body.Len() > 0 {
			appendContentLength(b, w.body.Len())
		}
	case writing.chunked:
		b.WriteString("Transfer-Encoding: chunked\r\n")
	}
	if !committed.hasType && bodyAllowed && len(content) > 0 && !committed.encoded {
		b.WriteString("Content-Type: ")
		b.WriteString(http.DetectContentType(content))
		b.WriteString("\r\n")
	}
	if !committed.hasDate {
		appendDate(b)
	}
	if !handlerConnection {
		if !writing.keepAlive {
			b.WriteString("Connection: close\r\n")
		} else if !http11 {
			b.WriteString("Connection: keep-alive\r\n")
		}
	} else {
		appendHeaderValues(b, "Connection", committed.connection)
	}
	b.WriteString("\r\n")
}

// appendStatusLine appends the status line of a response with code status. It is HTTP/1.1 for an HTTP/1.0 request as
// well, the highest version fhttp conforms to (RFC 9110 6.2), where the net/http server answers with HTTP/1.0.
func appendStatusLine(b *bytepool.Buffer, status int) {
	if status == http.StatusOK {
		b.WriteString("HTTP/1.1 200 OK\r\n")
		return
	}
	b.WriteString("HTTP/1.1 ")
	appendInt(b, status)
	b.WriteString(" ")
	if text := http.StatusText(status); text != "" {
		b.WriteString(text)
	} else {
		b.WriteString("status code " + strconv.Itoa(status))
	}
	b.WriteString("\r\n")
}

func appendContentLength(b *bytepool.Buffer, n int) {
	b.WriteString("Content-Length: ")
	appendInt(b, n)
	b.WriteString("\r\n")
}

// appendChunkSize appends the line that starts a chunk of size bytes, its size in hexadecimal (RFC 9112 7.1).
func appendChunkSize(b *bytepool.Buffer, size int) {
	var digits [16]byte
	b.Write(strconv.AppendInt(digits[:0], int64(size), 16))
	b.WriteString("\r\n")
}

// declaredLength returns the Content-Length the Handler declared in the values of its field, or -1 when it declared it not
// as a single number.
func declaredLength(values []string) int {
	if len(values) == 1 {
		if n, err := strconv.Atoi(values[0]); err == nil && n >= 0 {
			return n
		}
	}
	return -1
}

// appendInt appends the decimal representation of n.
func appendInt(b *bytepool.Buffer, n int) {
	var digits [20]byte
	b.Write(strconv.AppendInt(digits[:0], int64(n), 10))
}

func bodyAllowedForStatus(status int) bool {
	return status >= 200 && status != http.StatusNoContent && status != http.StatusNotModified
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
	b.Write(*dateField.Load())
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
