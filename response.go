package fnet

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"strconv"
	"sync"

	"github.com/gobwas/ws"
)

// responseWriter implements http.ResponseWriter and http.Hijacker backed by a
// net.Conn (typically a *tls.Conn or *VirtualConn).
type responseWriter struct {
	c             *conn
	conn          net.Conn
	bufr          *bufio.Reader
	bufw          *bufio.Writer
	bufwPooled    bool
	header        http.Header
	status        int
	wroteHeader   bool
	wroteBody     bool
	hijacked      bool
	contentLength int64
	chunked       bool
	closeConn     bool
	isHead        bool  // request method was HEAD: emit headers but no body
	discardedLen  int64 // bytes a bodyless response swallowed, used for Content-Length
	bodyBuf       bytes.Buffer
	mu            sync.Mutex
}

var writerPool = sync.Pool{
	New: func() any { return bufio.NewWriterSize(nil, 4096) },
}

// WSHandler receives WebSocket events on an event-driven connection.
type WSHandler interface {
	OnOpen()
	OnMessage(opcode byte, payload []byte)
	OnClose(err error)
}

// WSFrameHandler is an optional interface extending WSHandler to deliver
// full frame headers (e.g. RSV bits for compression extensions).
type WSFrameHandler interface {
	WSHandler
	OnFrame(h ws.Header, payload []byte)
}

// WSAttacher is implemented by http.ResponseWriter (and hijacked net.Conn) in fnet
// to transition the connection from the HTTP request goroutine into the Poller-driven
// zero-goroutine WebSocket state.
type WSAttacher interface {
	AttachWS(handler WSHandler) (*VirtualConn, error)
}

var responseWriterPool = sync.Pool{
	New: func() any {
		return &responseWriter{
			header:        make(http.Header, 8),
			status:        http.StatusOK,
			contentLength: -1,
		}
	},
}

func acquireResponseWriter(c *conn, conn net.Conn, bufr ...*bufio.Reader) *responseWriter {
	w := responseWriterPool.Get().(*responseWriter)
	var r *bufio.Reader
	if len(bufr) > 0 {
		r = bufr[0]
	}
	if r == nil {
		r = bufio.NewReader(conn)
	}
	w.c = c
	w.conn = conn
	w.bufr = r
	w.bufw = nil
	w.bufwPooled = false
	w.status = http.StatusOK
	w.wroteHeader = false
	w.wroteBody = false
	w.hijacked = false
	w.contentLength = -1
	w.chunked = false
	w.closeConn = false
	w.isHead = false
	w.discardedLen = 0
	w.bodyBuf.Reset()
	if w.header == nil {
		w.header = make(http.Header, 8)
	} else {
		clear(w.header)
	}
	return w
}

func releaseResponseWriter(w *responseWriter) {
	w.releaseBuffers()
	responseWriterPool.Put(w)
}

func newResponseWriter(conn net.Conn, bufr ...*bufio.Reader) *responseWriter {
	return acquireResponseWriter(nil, conn, bufr...)
}

func (w *responseWriter) SetClose(close bool) {
	w.mu.Lock()
	w.closeConn = close
	w.mu.Unlock()
}

// SetHead marks the response as answering a HEAD request. Headers are produced
// exactly as they would be for GET, but no message body is sent (RFC 9110 9.3.2).
func (w *responseWriter) SetHead(head bool) {
	w.mu.Lock()
	w.isHead = head
	w.mu.Unlock()
}

// bodylessLocked reports whether this response must not carry a message body:
// HEAD requests plus 1xx, 204 and 304 statuses (RFC 9110 6.4.1).
func (w *responseWriter) bodylessLocked() bool {
	return w.isHead || w.status < 200 ||
		w.status == http.StatusNoContent || w.status == http.StatusNotModified
}

// allowsContentLengthLocked reports whether Content-Length may be sent for the
// current status (RFC 9110 8.6 forbids it on 1xx and 204).
func (w *responseWriter) allowsContentLengthLocked() bool {
	return w.status >= 200 &&
		w.status != http.StatusNoContent && w.status != http.StatusNotModified
}

func (w *responseWriter) Header() http.Header {
	return w.header
}

func (w *responseWriter) WriteHeader(statusCode int) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.wroteHeader {
		return
	}
	w.status = statusCode
	if w.header.Get("Content-Length") != "" || w.header.Get("Transfer-Encoding") == "chunked" {
		w.writeHeaderLocked()
	}
}

func (w *responseWriter) writeHeaderLocked() {
	if w.wroteHeader {
		return
	}
	w.wroteHeader = true

	if cl := w.header.Get("Content-Length"); cl != "" {
		if n, err := strconv.ParseInt(cl, 10, 64); err == nil {
			w.contentLength = n
		}
	}
	if w.header.Get("Transfer-Encoding") == "chunked" {
		w.chunked = true
	}
	if w.header.Get("Date") == "" {
		w.header.Set("Date", httpDateNow())
	}
	if w.closeConn {
		if w.header.Get("Connection") == "" {
			w.header.Set("Connection", "close")
		}
	} else {
		if w.header.Get("Connection") == "" {
			w.header.Set("Connection", "keep-alive")
		}
	}

	buf := bufio.NewWriter(w.conn)
	fmt.Fprintf(buf, "HTTP/1.1 %d %s\r\n", w.status, http.StatusText(w.status))
	_ = w.header.Write(buf)
	_, _ = buf.WriteString("\r\n")
	_ = buf.Flush()
}

func (w *responseWriter) Write(b []byte) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.hijacked {
		return 0, errors.New("fnet: connection hijacked")
	}

	w.wroteBody = true

	if w.bodylessLocked() {
		// Swallow the body but keep counting it, so a HEAD response can still
		// advertise the Content-Length the matching GET would have returned.
		w.discardedLen += int64(len(b))
		return len(b), nil
	}

	if w.wroteHeader {
		if w.chunked {
			if _, err := fmt.Fprintf(w.conn, "%x\r\n", len(b)); err != nil {
				return 0, err
			}
			if _, err := w.conn.Write(b); err != nil {
				return 0, err
			}
			_, err := io.WriteString(w.conn, "\r\n")
			return len(b), err
		}
		return w.conn.Write(b)
	}

	if w.header.Get("Content-Length") != "" || w.header.Get("Transfer-Encoding") == "chunked" {
		w.writeHeaderLocked()
		if w.chunked {
			if _, err := fmt.Fprintf(w.conn, "%x\r\n", len(b)); err != nil {
				return 0, err
			}
			if _, err := w.conn.Write(b); err != nil {
				return 0, err
			}
			_, err := io.WriteString(w.conn, "\r\n")
			return len(b), err
		}
		return w.conn.Write(b)
	}

	const maxBuffer = 64 * 1024
	if w.bodyBuf.Len()+len(b) <= maxBuffer {
		return w.bodyBuf.Write(b)
	}

	w.chunked = true
	w.header.Set("Transfer-Encoding", "chunked")
	w.writeHeaderLocked()

	if w.bodyBuf.Len() > 0 {
		old := w.bodyBuf.Bytes()
		if _, err := fmt.Fprintf(w.conn, "%x\r\n", len(old)); err != nil {
			return 0, err
		}
		if _, err := w.conn.Write(old); err != nil {
			return 0, err
		}
		if _, err := io.WriteString(w.conn, "\r\n"); err != nil {
			return 0, err
		}
		w.bodyBuf.Reset()
	}

	if _, err := fmt.Fprintf(w.conn, "%x\r\n", len(b)); err != nil {
		return 0, err
	}
	if _, err := w.conn.Write(b); err != nil {
		return 0, err
	}
	_, err := io.WriteString(w.conn, "\r\n")
	return len(b), err
}

func (w *responseWriter) finish() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.hijacked {
		return nil
	}
	bodyless := w.bodylessLocked()
	if !w.wroteHeader {
		if w.allowsContentLengthLocked() {
			// Never emit both framing headers: RFC 9112 6.1 forbids
			// Content-Length alongside Transfer-Encoding. w.chunked is only
			// set once headers are flushed, so consult the header too.
			chunked := w.chunked || w.header.Get("Transfer-Encoding") == "chunked"
			if w.header.Get("Content-Length") == "" && !chunked {
				n := int64(w.bodyBuf.Len())
				if bodyless {
					n = w.discardedLen
				}
				w.header.Set("Content-Length", strconv.FormatInt(n, 10))
			}
		}
		w.writeHeaderLocked()
	}

	if bodyless {
		// Headers are on the wire; the body (and any chunked terminator) is not.
		w.bodyBuf.Reset()
		return nil
	}

	if w.bodyBuf.Len() > 0 {
		_, err := w.conn.Write(w.bodyBuf.Bytes())
		w.bodyBuf.Reset()
		if err != nil {
			return err
		}
	}

	if w.chunked {
		_, err := io.WriteString(w.conn, "0\r\n\r\n")
		return err
	}
	return nil
}

func (w *responseWriter) releaseBuffers() {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.bufw != nil {
		if w.bufwPooled {
			w.bufw.Reset(nil)
			writerPool.Put(w.bufw)
		}
		w.bufw = nil
		w.bufwPooled = false
	}
	w.conn = nil
	w.bufr = nil
	if w.header != nil {
		clear(w.header)
	}
	w.c = nil
}

// AttachWS marks the connection for event-driven WebSocket processing and attaches
// the event handler. Any read-ahead bytes buffered by the HTTP reader are restored
// to the VirtualConn input buffer so no frame bytes are lost. Frame dispatch starts
// once the HTTP handler returns, so OnOpen-style setup always precedes OnMessage.
//
// The reactor parses raw socket bytes, so this is unavailable on TLS connections
// and returns ErrWSAttachUnsupported; callers should fall back to a goroutine loop.
func (w *responseWriter) AttachWS(handler WSHandler) (*VirtualConn, error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.c == nil {
		return nil, errors.New("fnet: no underlying fnet conn")
	}
	if w.c.server != nil && w.c.server.TLSConfig != nil {
		return nil, ErrWSAttachUnsupported
	}

	w.hijacked = true

	// Drain any read-ahead bytes in w.bufr back to VirtualConn
	if w.bufr != nil && w.bufr.Buffered() > 0 {
		rem := make([]byte, w.bufr.Buffered())
		_, _ = io.ReadFull(w.bufr, rem)
		w.c.vc.UnshiftInput(rem)
	}

	c := w.c
	c.mu.Lock()
	c.wsHandler = handler
	c.state.Store(connStateWSAttached)
	c.mu.Unlock()

	if w.bufw != nil {
		if w.bufwPooled {
			w.bufw.Reset(nil)
			writerPool.Put(w.bufw)
		}
		w.bufw = nil
		w.bufwPooled = false
	}
	w.bufr = nil
	w.header = nil

	return c.vc, nil
}

// hijackedConn wraps the underlying net.Conn so that any bytes buffered in
// the HTTP reader are consumed first before reading directly from the network.
type hijackedConn struct {
	net.Conn
	r io.Reader
	w *responseWriter
}

func (c *hijackedConn) Read(b []byte) (int, error) {
	if c.r != nil {
		return c.r.Read(b)
	}
	return c.Conn.Read(b)
}

func (c *hijackedConn) WriteVector(iovs [][]byte) (int, error) {
	if wv, ok := c.Conn.(interface{ WriteVector([][]byte) (int, error) }); ok {
		return wv.WriteVector(iovs)
	}
	total := 0
	for _, b := range iovs {
		n, err := c.Conn.Write(b)
		total += n
		if err != nil {
			return total, err
		}
	}
	return total, nil
}

func (c *hijackedConn) AttachWS(handler WSHandler) (*VirtualConn, error) {
	if c.w != nil {
		w := c.w
		c.w = nil
		c.r = nil
		return w.AttachWS(handler)
	}
	if attacher, ok := c.Conn.(WSAttacher); ok {
		return attacher.AttachWS(handler)
	}
	return nil, errors.New("fnet: underlying connection is not a WSAttacher")
}

// Hijack implements http.Hijacker.
func (w *responseWriter) Hijack() (net.Conn, *bufio.ReadWriter, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.hijacked {
		return nil, nil, errors.New("fnet: connection already hijacked")
	}
	w.hijacked = true
	if w.bufw == nil {
		bw := writerPool.Get().(*bufio.Writer)
		bw.Reset(w.conn)
		w.bufw = bw
		w.bufwPooled = true
	}
	conn := &hijackedConn{
		Conn: w.conn,
		r:    w.bufr,
		w:    w,
	}
	return conn, bufio.NewReadWriter(w.bufr, w.bufw), nil
}

// Hijacked reports whether the connection was hijacked.
func (w *responseWriter) Hijacked() bool {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.hijacked
}

// Ensure responseWriter satisfies http.ResponseWriter, http.Hijacker, and WSAttacher.
var (
	_ http.ResponseWriter = (*responseWriter)(nil)
	_ http.Hijacker       = (*responseWriter)(nil)
	_ WSAttacher          = (*responseWriter)(nil)
	_ WSAttacher          = (*hijackedConn)(nil)
)
