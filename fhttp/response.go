package fhttp

import (
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

// maxBufferedBody is the largest response body buffered until the Handler returns, to go out with its actual
// Content-Length; a larger one goes out in pieces of this size as the Handler writes it (see stream), so that no more
// than this of it waits in the response, and twice as much in fnet (see conn.waitWritable). Not to be confused with
// Options.MaxBufferedBodyBytes, which is about request bodies.
const maxBufferedBody = 64 * units.KB

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
	conn        *conn
	req         *http.Request
	header      http.Header
	status      int
	writeErr    error
	body        bytepool.Buffer
	out         bytepool.Buffer // the responses serialized and not written out yet, see flush
	fields      bytepool.Buffer // the fields commitHeader serialized, but for Connection
	remaining   int             // what the Handler may still write of the Content-Length it declared, -1 for none
	committed   committedHeader // what commitHeader took of the Handler's header
	bodyWriting bodyWriting     // how the body goes out, decided with the header by writeHeader
	wroteHeader bool
	upgraded    bool // Upgrade has been called, so finish no longer writes the response
	streamed    bool // the header has gone out, the body following in pieces as the Handler writes it (see stream)
}

// committedHeader is what commitHeader takes of the Handler's header when the status is written; later changes to the
// header do not count.
type committedHeader struct {
	connection     []string // the values of the Connection field, which goes out only if the body leaves the connection open
	declaredLength int      // the Content-Length the Handler declared, -1 for none
	hasLength      bool     // the header has a Content-Length
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

var responsePool = sync.Pool{New: func() any { return &response{header: make(http.Header)} }}

// start readies w for the response to req.
func (w *response) start(req *http.Request) {
	clear(w.header)
	w.req, w.status, w.wroteHeader, w.upgraded, w.streamed, w.writeErr = req, 0, false, false, false, nil
	w.body.Reset()
}

func (w *response) release() {
	w.body.Release()
	w.out.Release()
	w.fields.Release()
	clear(w.header)
	*w = response{header: w.header}
	responsePool.Put(w)
}

// flush writes out the responses collected so far in one write.
func (w *response) flush() error {
	var err error
	if w.out.Len() > 0 {
		_, err = w.conn.connection.Write(w.out.Bytes()) // net.ErrClosed once the connection is closing
	}
	w.out.Release()
	if err != nil && w.writeErr == nil {
		w.writeErr = err
	}
	return err
}

// writeError puts a response with code, which closes the connection, into the output. It has a Date as well, which RFC
// 9110 6.6.1 requires in a 4xx response (the net/http server leaves it out of its own).
func (w *response) writeError(code int) {
	text := strconv.Itoa(code) + " " + http.StatusText(code)
	w.out.AppendString("HTTP/1.1 " + text + "\r\nContent-Type: text/plain; charset=utf-8\r\nConnection: close\r\n" +
		"Content-Length: " + strconv.Itoa(len(text)) + "\r\n")
	appendDate(&w.out)
	w.out.AppendString("\r\n" + text)
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
	w.body.Append(p)
	return len(p), nil
}

func (w *response) WriteString(s string) (int, error) {
	if err := w.beforeWrite(len(s)); err != nil {
		return 0, err
	}
	if w.body.Len()+len(s) > maxBufferedBody {
		return w.streamString(s)
	}
	w.body.AppendString(s)
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
			w.body.Append(p[:fill])
			piece, p = w.body.Bytes(), p[fill:]
		}
		if err := w.sendPiece(piece); err != nil {
			return n - len(p), err
		}
	}
	w.body.Append(p)
	return n, nil
}

// streamString is stream for a string, which cannot be sent from as it is: it is copied into the buffer a piece at a
// time, so that however long it is no more than a piece of it is held in addition. It is not generic with stream: a
// generic instance has no escape information, which would have every string written to a response escape.
func (w *response) streamString(s string) (int, error) {
	n := len(s)
	for w.body.Len()+len(s) > maxBufferedBody {
		fill := maxBufferedBody - w.body.Len()
		w.body.AppendString(s[:fill])
		s = s[fill:]
		if err := w.sendPiece(w.body.Bytes()); err != nil {
			return n - len(s), err
		}
	}
	w.body.AppendString(s)
	return n, nil
}

// sendPiece sends a piece of the streamed body (see send) and empties the buffer, which the piece may be.
func (w *response) sendPiece(piece []byte) error {
	err := w.send(piece)
	w.body.Reset()
	return err
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
	_, err := w.conn.connection.Writev([][]byte{w.out.Bytes(), piece, end})
	w.out.Release()
	if err != nil {
		w.writeErr = err
	}
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
		if _, err := w.conn.connection.Writev([][]byte{b.Bytes(), body, end}); err != nil {
			w.writeErr = err
		}
		b.Release()
	} else {
		b.Append(body)
		b.Append(end)
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

// commitHeader fixes the Handler's header as it is when the status is written, as net/http does: changes to the header
// after that do not count. One pass over the fields serializes those to send, but for Connection, which goes out only if
// the body leaves the connection open (see writeHeader), and notes the few that writeHeader decides on: Transfer-Encoding
// and Content-Length are decided there (the actual length when the whole body is at hand), and a 304 has no Content-Type.
func (w *response) commitHeader() {
	keepLength := w.req.Method == http.MethodHead && w.bodyAllowed() && w.status != http.StatusResetContent // HEAD keeps the Content-Length declared by the Handler
	var stack [16]field
	fields := stack[:0]
	committed := committedHeader{declaredLength: -1}
	for name, values := range w.header {
		switch name {
		case "Transfer-Encoding":
			continue
		case "Content-Length":
			committed.declaredLength = declaredLength(values)
			if committed.hasLength = true; !keepLength {
				continue
			}
		case "Content-Type":
			if committed.hasType = true; w.status == http.StatusNotModified {
				continue
			}
		case "Content-Encoding":
			committed.encoded = len(values) > 0 && values[0] != ""
		case "Date":
			committed.hasDate = true
		case "Connection":
			committed.closing, committed.connection = httpx.HasToken(values, "close"), slices.Clone(values)
			continue
		}
		if validName(name) {
			fields = append(fields, field{name, values})
		}
	}
	w.committed, w.remaining = committed, committed.declaredLength
	w.fields.Reset()
	appendFields(&w.fields, fields)
}

// writeHeader serializes the response header into the output, after the responses collected before it, and decides how
// the body goes out and whether to keep the connection alive; content is the start of the body, to detect its type
// from. A streamed body (see stream) has the Content-Length the Handler declared, if any; otherwise its length is not
// known, and it goes out in chunks on HTTP/1.1, and until the connection closes on HTTP/1.0.
func (w *response) writeHeader(content []byte, streamed bool) {
	if !w.wroteHeader {
		w.WriteHeader(http.StatusOK)
	}
	req := w.req
	http11 := req.ProtoAtLeast(1, 1)
	isHEAD := req.Method == http.MethodHead
	bodyAllowed := w.bodyAllowed()
	if w.status == http.StatusResetContent {
		w.body.Reset() // a 205 has no content (RFC 9110 15.3.6), so it goes out with Content-Length: 0
		content = nil
	}
	keepLength := isHEAD && bodyAllowed && w.status != http.StatusResetContent // HEAD keeps the Content-Length declared by the Handler
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
	// The Handler's Connection field goes out unless the request, the tunnel or the end of the body closes the connection
	// and it does not say so: "Connection: close" goes out in its place.
	handlerConnection := committed.connection != nil && (committed.closing || !mustClose)

	b := &w.out
	if cap(b.Bytes()) == 0 {
		*b = bytepool.Get(initialOutput)
	}
	if w.status == http.StatusOK {
		b.AppendString("HTTP/1.1 200 OK\r\n") // the usual status line, in one piece
	} else {
		appendStatusLine(b, w.status)
	}
	switch {
	case !bodyAllowed:
	case !streamed || w.status == http.StatusResetContent: // the whole body is at hand
		if !keepLength || !committed.hasLength && w.body.Len() > 0 {
			appendContentLength(b, w.body.Len())
		}
	case committed.declaredLength >= 0:
		appendContentLength(b, committed.declaredLength)
	case writing.chunked:
		b.AppendString("Transfer-Encoding: chunked\r\n")
	}
	if !committed.hasType && bodyAllowed && len(content) > 0 && !committed.encoded {
		b.AppendString("Content-Type: ")
		b.AppendString(http.DetectContentType(content))
		b.AppendString("\r\n")
	}
	if !committed.hasDate {
		appendDate(b)
	}
	if !handlerConnection {
		if !writing.keepAlive {
			b.AppendString("Connection: close\r\n")
		} else if !http11 {
			b.AppendString("Connection: keep-alive\r\n")
		}
	}
	b.Append(w.fields.Bytes())
	if handlerConnection {
		appendFields(b, []field{{"Connection", committed.connection}})
	}
	b.AppendString("\r\n")
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

// appendStatusLine appends the status line of a response with code status. It is HTTP/1.1 for an HTTP/1.0 request as
// well, the highest version fhttp conforms to (RFC 9110 6.2), where the net/http server answers with HTTP/1.0.
func appendStatusLine(b *bytepool.Buffer, status int) {
	b.AppendString("HTTP/1.1 ")
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

// appendChunkSize appends the line that starts a chunk of size bytes, its size in hexadecimal (RFC 9112 7.1).
func appendChunkSize(b *bytepool.Buffer, size int) {
	var digits [16]byte
	b.Append(strconv.AppendInt(digits[:0], int64(size), 16))
	b.AppendString("\r\n")
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
	b.Append(strconv.AppendInt(digits[:0], int64(n), 10))
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
