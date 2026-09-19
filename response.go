package fnet

import (
	"bufio"
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
	mu            sync.Mutex
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
	w.writeHeaderLocked()
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
	if w.header.Get("Connection") == "" {
		w.header.Set("Connection", "close")
	}

	buf := bufio.NewWriter(w.conn)
	fmt.Fprintf(buf, "HTTP/1.1 %d %s\r\n", w.status, http.StatusText(w.status))
	_ = w.header.Write(buf)
	_, _ = buf.WriteString("\r\n")
	_ = buf.Flush()
}

func (w *responseWriter) Write(b []byte) (int, error) {
	w.mu.Lock()
	if !w.wroteHeader {
		w.writeHeaderLocked()
	}
	w.wroteBody = true
	chunked := w.chunked
	w.mu.Unlock()

	if chunked {
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

func (w *responseWriter) finish() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.hijacked {
		return nil
	}
	if !w.wroteHeader {
		w.writeHeaderLocked()
	}
	if w.chunked {
		_, err := io.WriteString(w.conn, "0\r\n\r\n")
		return err
	}
	return nil
}

// hijackedConn wraps the underlying net.Conn so that any bytes buffered in
// the HTTP reader are consumed first before reading directly from the network.
type hijackedConn struct {
	net.Conn
	r io.Reader
}

func (c *hijackedConn) Read(b []byte) (int, error) {
	return c.r.Read(b)
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
	}
	return conn, bufio.NewReadWriter(w.bufr, w.bufw), nil
}

// Hijacked reports whether the connection was hijacked.
func (w *responseWriter) Hijacked() bool {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.hijacked
}

// Ensure responseWriter satisfies http.ResponseWriter and http.Hijacker.
var (
	_ http.ResponseWriter = (*responseWriter)(nil)
	_ http.Hijacker       = (*responseWriter)(nil)
)
