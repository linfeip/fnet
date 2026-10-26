package fhttp

import (
	"log/slog"
	"net/http"
	"runtime"

	"github.com/linfeip/fnet/ftls"
	"github.com/linfeip/fnet/internal/units"
)

// continueResponse is the interim response that asks the client to send the request body.
var continueResponse = []byte("HTTP/1.1 100 Continue\r\n\r\n")

// handle parses and processes one request, its response going into w's output, and reports whether to keep the
// connection alive. It owns the dequeued message and returns its buffer before exiting.
func (c *conn) handle(w *response, r request) bool {
	defer r.msg.Release()
	switch r.status {
	case 0:
	case http.StatusContinue:
		w.out.Append(continueResponse)
		return true
	case statusHandoff:
		if w.flush() != nil {
			c.connection.Close()
			return false
		}
		c.handoff()
		return false // closing the detached connection does nothing
	case statusClose:
		return false
	default:
		w.writeError(int(r.status))
		return false
	}

	if c.last == nil {
		last := new(lastRequest)
		c.mu.Lock() // onClose reads it while holding mu
		c.last = last
		c.mu.Unlock()
	}
	req, parsed, status := c.parse(r)
	if status != 0 {
		w.writeError(status)
		return false
	}
	defer parsed.release()
	parsed.bind(c)
	if c.remoteAddr == "" {
		c.remoteAddr = c.connection.RemoteAddr().String()
	}
	req.RemoteAddr = c.remoteAddr
	if tc, ok := c.connection.(*ftls.Conn); ok {
		if c.last.tls == nil { // the requests of a connection share it, as on net/http
			state := tc.ConnectionState()
			c.last.tls = &state
		}
		req.TLS = c.last.tls
	}

	w.start(req)
	ok := c.serveHTTP(w, req)
	keepAlive := false
	switch {
	case !ok:
	case w.upgraded: // Upgrade already wrote the response: on success the connection goes to the new protocol, on failure it closes
		keepAlive = c.protocol.Load() != nil && w.writeErr == nil
		c.endContext()
	default:
		keepAlive = w.finish()
	}
	return keepAlive
}

// serveHTTP runs the Handler; when the Handler panics it logs the panic (except for http.ErrAbortHandler) and
// returns false.
func (c *conn) serveHTTP(w *response, req *http.Request) (ok bool) {
	reqBody := req.Body
	defer func() {
		reqBody.Close()
		if req.MultipartForm != nil {
			req.MultipartForm.RemoveAll()
		}
		if err := recover(); err != nil {
			if err != http.ErrAbortHandler {
				buf := make([]byte, 64*units.KB)
				slog.Error("fhttp: panic serving request", "remoteAddr", c.remoteAddr, "panic", err,
					"stack", string(buf[:runtime.Stack(buf, false)]))
			}
			ok = false
		}
	}()
	handler := c.srv.handler
	if req.RequestURI == "*" && req.Method == http.MethodOptions {
		handler = serverOptions
	}
	handler.ServeHTTP(w, req)
	return true
}

// serverOptions answers "OPTIONS *", which asks about the server as a whole rather than a resource (RFC 9110 9.3.7), the
// way the net/http server does: 200 with no content, where http.ServeMux would answer 400.
var serverOptions = http.HandlerFunc(func(http.ResponseWriter, *http.Request) {})
