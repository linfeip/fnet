package fhttp

import (
	"bufio"
	"crypto/tls"
	"errors"
	"io"
	"net"
	"net/http"
	"os"
	"runtime"
	"testing"
	"time"

	"github.com/linfeip/fnet/internal/testcert"
)

// Shared helpers for the HTTP and HTTPS suites. Every server started here is
// bound to a loopback port and torn down by t.Cleanup.

const (
	testDialTimeout = 5 * time.Second
	testIOTimeout   = 5 * time.Second
)

// freeAddr reserves a loopback port and releases it again. The window between
// release and the server binding is racy in theory but reliable on loopback.
func freeAddr(t *testing.T) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("reserve port: %v", err)
	}
	addr := ln.Addr().String()
	_ = ln.Close()
	return addr
}

// startServer runs srv in the background and blocks until it accepts connections.
func startServer(t *testing.T, srv *Server) string {
	t.Helper()
	if srv.Addr == "" {
		srv.Addr = freeAddr(t)
	}
	go func() { _ = srv.ListenAndServe() }()
	t.Cleanup(func() { _ = srv.Close() })
	waitReady(t, srv.Addr)
	return srv.Addr
}

// startHTTP is the common case: a plain HTTP server for the given handler.
func startHTTP(t *testing.T, h http.Handler) string {
	t.Helper()
	return startServer(t, &Server{Handler: h})
}

func startHTTPFunc(t *testing.T, fn http.HandlerFunc) string {
	t.Helper()
	return startHTTP(t, fn)
}

func waitReady(t *testing.T, addr string) {
	t.Helper()
	deadline := time.Now().Add(testDialTimeout)
	for {
		c, err := net.DialTimeout("tcp", addr, time.Second)
		if err == nil {
			_ = c.Close()
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("server at %s never became ready: %v", addr, err)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// ---------------------------------------------------------------------------
// Raw wire client
// ---------------------------------------------------------------------------

// rawConn is a hand-driven HTTP client. It writes exact bytes and reads
// responses one at a time, which the keep-alive, pipelining and malformed
// message tests need — net/http's client would normalise all of that away.
type rawConn struct {
	t  *testing.T
	c  net.Conn
	br *bufio.Reader
}

func dialRaw(t *testing.T, addr string) *rawConn {
	t.Helper()
	c, err := net.DialTimeout("tcp", addr, testDialTimeout)
	if err != nil {
		t.Fatalf("dial %s: %v", addr, err)
	}
	t.Cleanup(func() { _ = c.Close() })
	return &rawConn{t: t, c: c, br: bufio.NewReader(c)}
}

func dialRawTLS(t *testing.T, addr string, cfg *tls.Config) *rawConn {
	t.Helper()
	d := &net.Dialer{Timeout: testDialTimeout}
	c, err := tls.DialWithDialer(d, "tcp", addr, cfg)
	if err != nil {
		t.Fatalf("tls dial %s: %v", addr, err)
	}
	t.Cleanup(func() { _ = c.Close() })
	return &rawConn{t: t, c: c, br: bufio.NewReader(c)}
}

func (r *rawConn) write(s string) {
	r.t.Helper()
	_ = r.c.SetWriteDeadline(time.Now().Add(testIOTimeout))
	if _, err := io.WriteString(r.c, s); err != nil {
		r.t.Fatalf("write: %v", err)
	}
}

// writeByteByByte sends s one byte per Write to exercise fragmented delivery.
func (r *rawConn) writeByteByByte(s string) {
	r.t.Helper()
	_ = r.c.SetWriteDeadline(time.Now().Add(testIOTimeout))
	for i := 0; i < len(s); i++ {
		if _, err := r.c.Write([]byte{s[i]}); err != nil {
			r.t.Fatalf("write byte %d: %v", i, err)
		}
		time.Sleep(100 * time.Microsecond)
	}
}

// readResponse parses the next response. method drives body framing, so HEAD
// responses are read correctly.
func (r *rawConn) readResponse(method string) *http.Response {
	r.t.Helper()
	resp, err := r.readResponseErr(method)
	if err != nil {
		r.t.Fatalf("read %s response: %v", method, err)
	}
	return resp
}

func (r *rawConn) readResponseErr(method string) (*http.Response, error) {
	_ = r.c.SetReadDeadline(time.Now().Add(testIOTimeout))
	return http.ReadResponse(r.br, &http.Request{Method: method})
}

// readAll drains everything the server sends until it closes the connection.
// Read errors are not fatal: a reset is a legitimate answer to a rejected
// request, and callers assert on the bytes that did arrive.
func (r *rawConn) readAll(within time.Duration) string {
	_ = r.c.SetReadDeadline(time.Now().Add(within))
	b, _ := io.ReadAll(r.br)
	return string(b)
}

func (r *rawConn) close() { _ = r.c.Close() }

// rawExchange sends req verbatim on a fresh connection and returns everything
// the server writes back. Use `Connection: close` in req to avoid waiting out
// the deadline on a keep-alive connection.
func rawExchange(t *testing.T, addr, req string) string {
	t.Helper()
	c := dialRaw(t, addr)
	defer c.close()
	c.write(req)
	return c.readAll(testIOTimeout)
}

// ---------------------------------------------------------------------------
// net/http clients
// ---------------------------------------------------------------------------

// newClient returns an http.Client with a private Transport, so keep-alive
// state never leaks between tests.
func newClient(t *testing.T, tlsCfg *tls.Config) *http.Client {
	t.Helper()
	tr := &http.Transport{
		TLSClientConfig:     tlsCfg,
		MaxIdleConnsPerHost: 32,
		DisableCompression:  true,
	}
	t.Cleanup(tr.CloseIdleConnections)
	return &http.Client{Transport: tr, Timeout: 20 * time.Second}
}

func readBody(t *testing.T, resp *http.Response) string {
	t.Helper()
	defer resp.Body.Close()
	b, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read body: %v", err)
	}
	return string(b)
}

func isTimeout(err error) bool {
	if errors.Is(err, os.ErrDeadlineExceeded) {
		return true
	}
	var ne net.Error
	return errors.As(err, &ne) && ne.Timeout()
}

// testCertificate returns a self-signed localhost certificate.
func testCertificate() (tls.Certificate, error) {
	certPEM, keyPEM, err := testcert.Generate()
	if err != nil {
		return tls.Certificate{}, err
	}
	return tls.X509KeyPair(certPEM, keyPEM)
}

// skipOnPumpEmulation skips tests that count goroutines or bytes per idle
// connection: the Windows emulation in internal/netpoll parks a read-pump
// goroutine on every socket, which the native epoll/kqueue pollers do not.
func skipOnPumpEmulation(t *testing.T) {
	t.Helper()
	if runtime.GOOS == "windows" {
		t.Skip("the Windows socket emulation holds a pump goroutine per connection")
	}
}
