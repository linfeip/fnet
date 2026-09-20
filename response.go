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
)

// responseWriter implements http.ResponseWriter and http.Hijacker backed by a
// net.Conn (typically a *tls.Conn or *VirtualConn).
type responseWriter struct {
	c             *conn
	conn          net.Conn
	bufr          *bufio.Reader
	bufw          *bufio.Writer
	header        http.Header
	status        int
	wroteHeader   bool
	wroteBody     bool
	hijacked      bool
	contentLength int64
	chunked       bool
	closeConn     bool
	bodyBuf       bytes.Buffer
	mu            sync.Mutex
}

// WSHandler receives WebSocket events on an event-driven connection.
type WSHandler interface {
	OnOpen()
	OnMessage(opcode byte, payload []byte)
	OnClose(err error)
}

// WSAttacher is implemented by http.ResponseWriter (and hijacked net.Conn) in fnet
// to transition the connection from the HTTP request goroutine into the Poller-driven
// zero-goroutine WebSocket state.
type WSAttacher interface {
	AttachWS(handler WSHandler) (*VirtualConn, error)
}

func newResponseWriter(conn net.Conn, bufr ...*bufio.Reader) *responseWriter {
	var r *bufio.Reader
	if len(bufr) > 0 {
		r = bufr[0]
	}
	if r == nil {
		r = bufio.NewReader(conn)
	}
	return &responseWriter{
		conn:          conn,
		bufr:          r,
		header:        make(http.Header),
		status:        http.StatusOK,
		contentLength: -1,
	}
}

func (w *responseWriter) SetClose(close bool) {
	w.mu.Lock()
	w.closeConn = close
	w.mu.Unlock()
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
	if !w.wroteHeader {
		if w.status != http.StatusNoContent && w.status != http.StatusNotModified {
			if w.header.Get("Content-Length") == "" && !w.chunked {
				w.header.Set("Content-Length", strconv.Itoa(w.bodyBuf.Len()))
			}
		}
		w.writeHeaderLocked()
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

// AttachWS marks the connection for event-driven WebSocket processing and attaches
// the event handler. Any read-ahead bytes buffered by the HTTP reader are restored
// to the VirtualConn input buffer so no frame bytes are lost.
func (w *responseWriter) AttachWS(handler WSHandler) (*VirtualConn, error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.c == nil {
		return nil, errors.New("fnet: no underlying fnet conn")
	}

	w.hijacked = true

	// Drain any read-ahead bytes in w.bufr back to VirtualConn
	if w.bufr != nil && w.bufr.Buffered() > 0 {
		rem := make([]byte, w.bufr.Buffered())
		_, _ = io.ReadFull(w.bufr, rem)
		w.c.vc.UnshiftInput(rem)
	}

	w.c.mu.Lock()
	w.c.state = connStateWSEventDriven
	w.c.wsHandler = handler
	w.c.mu.Unlock()

	return w.c.vc, nil
}

// hijackedConn wraps the underlying net.Conn so that any bytes buffered in
// the HTTP reader are consumed first before reading directly from the network.
type hijackedConn struct {
	net.Conn
	r io.Reader
	w *responseWriter
}

func (c *hijackedConn) Read(b []byte) (int, error) {
	return c.r.Read(b)
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
		return c.w.AttachWS(handler)
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
		w.bufw = bufio.NewWriter(w.conn)
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
