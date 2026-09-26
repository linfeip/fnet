package fhttp

import (
	"bufio"
	"bytes"
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"
)

// A download larger than any output cap, to a client that reads slower than
// the handler writes: the handler's writes wait for the client, as on a
// blocking socket, and the whole body arrives.
func TestHTTPLargeDownloadToSlowReader(t *testing.T) {
	const total = 24 << 20
	werr := make(chan error, 1)
	addr := startHTTPFunc(t, func(w http.ResponseWriter, r *http.Request) {
		chunk := bytes.Repeat([]byte("d"), 32<<10)
		var err error
		for n := 0; n < total && err == nil; n += len(chunk) {
			_, err = w.Write(chunk)
		}
		werr <- err
	})
	c := dialRaw(t, addr)
	defer c.close()
	_, _ = io.WriteString(c.c, "GET / HTTP/1.1\r\nHost: x\r\n\r\n")
	_ = c.c.SetReadDeadline(time.Now().Add(30 * time.Second))
	resp, err := http.ReadResponse(bufio.NewReader(slowReader{c.c}), nil)
	if err != nil {
		t.Fatal(err)
	}
	n, err := io.Copy(io.Discard, resp.Body)
	if err != nil || n != total {
		t.Fatalf("read %d of %d bytes (%v)", n, total, err)
	}
	if err := <-werr; err != nil {
		t.Fatalf("handler write: %v", err)
	}
}

// slowReader reads in small steps with a pause, like a client on a slow link.
type slowReader struct{ r io.Reader }

func (s slowReader) Read(p []byte) (int, error) {
	time.Sleep(time.Millisecond)
	return s.r.Read(p[:min(len(p), 64<<10)])
}

// An upload to a handler still busy with something else is held back by TCP
// flow control instead of being read into memory.
func TestHTTPUploadIsNotBufferedWithoutBound(t *testing.T) {
	const bodySize = 256 << 20
	release := make(chan struct{})
	addr := startHTTPFunc(t, func(w http.ResponseWriter, r *http.Request) {
		<-release // a slow downstream call before the body is read
		n, _ := io.Copy(io.Discard, r.Body)
		fmt.Fprint(w, n)
	})
	c := dialRaw(t, addr)
	defer c.close()
	fmt.Fprintf(c.c, "POST / HTTP/1.1\r\nHost: x\r\nContent-Length: %d\r\n\r\n", bodySize)
	chunk := make([]byte, 1<<20)
	sent := 0
	_ = c.c.SetWriteDeadline(time.Now().Add(time.Second))
	for sent < bodySize {
		n, err := c.c.Write(chunk)
		sent += n
		if err != nil {
			break
		}
	}
	close(release)
	if sent > 64<<20 {
		t.Fatalf("the server took %d MiB while its handler was not reading", sent>>20)
	}
	t.Logf("the client could push %d MiB ahead of the handler", sent>>20)
}

// The request's context ends when the client goes away, so a handler waiting
// on a database or a downstream call can stop.
func TestHTTPRequestContextCanceledOnDisconnect(t *testing.T) {
	canceled := make(chan error, 1)
	addr := startHTTPFunc(t, func(w http.ResponseWriter, r *http.Request) {
		select {
		case <-r.Context().Done():
			canceled <- r.Context().Err()
		case <-time.After(3 * time.Second):
			canceled <- nil
		}
	})
	c := dialRaw(t, addr)
	_, _ = io.WriteString(c.c, "GET / HTTP/1.1\r\nHost: x\r\n\r\n")
	time.Sleep(50 * time.Millisecond)
	c.close()
	if err := <-canceled; !errors.Is(err, context.Canceled) {
		t.Fatalf("context not canceled after the client left: %v", err)
	}
}

// Server-Sent Events: Flush sends what was written at once.
func TestHTTPFlushStreamsEvents(t *testing.T) {
	addr := startHTTPFunc(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = io.WriteString(w, "data: first\n\n")
		if err := http.NewResponseController(w).Flush(); err != nil {
			t.Errorf("Flush: %v", err)
		}
		time.Sleep(time.Second)
		_, _ = io.WriteString(w, "data: second\n\n")
	})
	start := time.Now()
	resp, err := newClient(t, nil).Get("http://" + addr)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	line, err := bufio.NewReader(resp.Body).ReadString('\n')
	if err != nil || line != "data: first\n" {
		t.Fatalf("first event %q (%v)", line, err)
	}
	if d := time.Since(start); d > 500*time.Millisecond {
		t.Fatalf("first event arrived after %v, held back until the handler returned", d)
	}
}

// A handler that writes less, or more, than its Content-Length must not throw
// a keep-alive connection out of step with its client.
func TestHTTPContentLengthMismatchKeepsFraming(t *testing.T) {
	longErr := make(chan error, 1)
	addr := startHTTPFunc(t, func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/short":
			w.Header().Set("Content-Length", "10")
			_, _ = io.WriteString(w, "hello")
		case "/long":
			w.Header().Set("Content-Length", "2")
			_, err := io.WriteString(w, "hello")
			longErr <- err
		default:
			_, _ = io.WriteString(w, "second")
		}
	})
	for _, first := range []string{"/short", "/long"} {
		c := dialRaw(t, addr)
		fmt.Fprintf(c.c, "GET %s HTTP/1.1\r\nHost: x\r\n\r\nGET /next HTTP/1.1\r\nHost: x\r\n\r\n", first)
		out := c.readAll(2 * time.Second)
		c.close()
		if strings.Contains(out, "second") {
			t.Fatalf("%s: the connection was reused after a broken body: %q", first, out)
		}
	}
	if err := <-longErr; !errors.Is(err, http.ErrContentLength) {
		t.Fatalf("writing past Content-Length: %v, want http.ErrContentLength", err)
	}
}

// 103 Early Hints go out first, and the final response still follows.
func TestHTTPEarlyHints(t *testing.T) {
	addr := startHTTPFunc(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Link", "</style.css>; rel=preload; as=style")
		w.WriteHeader(http.StatusEarlyHints)
		w.WriteHeader(http.StatusOK)
		_, _ = io.WriteString(w, "ok")
	})
	out := rawExchange(t, addr, "GET / HTTP/1.1\r\nHost: x\r\nConnection: close\r\n\r\n")
	if !strings.HasPrefix(out, "HTTP/1.1 103 Early Hints\r\n") || !strings.Contains(out, "HTTP/1.1 200 OK\r\n") || !strings.HasSuffix(out, "ok") {
		t.Fatalf("response %q", out)
	}
}

// A client that half-closes right after its request gets the whole response,
// however much of it had to be queued.
func TestHTTPHalfCloseLargeResponse(t *testing.T) {
	const size = 8 << 20
	addr := startHTTPFunc(t, func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write(bytes.Repeat([]byte("r"), size))
	})
	c := dialRaw(t, addr)
	defer c.close()
	_, _ = io.WriteString(c.c, "GET / HTTP/1.1\r\nHost: x\r\nConnection: close\r\n\r\n")
	_ = c.c.(*net.TCPConn).CloseWrite()
	time.Sleep(100 * time.Millisecond)
	_ = c.c.SetReadDeadline(time.Now().Add(10 * time.Second))
	resp, err := http.ReadResponse(bufio.NewReader(c.c), nil)
	if err != nil {
		t.Fatal(err)
	}
	if n, err := io.Copy(io.Discard, resp.Body); err != nil || n != size {
		t.Fatalf("body %d of %d bytes (%v)", n, size, err)
	}
}

// Without keep-alive a TLS response says Connection: close, so a client does
// not send its next request (a POST it cannot retry) on a closing connection.
func TestHTTPSWithoutKeepAliveAnnouncesClose(t *testing.T) {
	ca := newTestCA(t)
	addr := startTLS(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		_, _ = io.WriteString(w, r.Method)
	}), &tls.Config{Certificates: []tls.Certificate{ca.serverCert(t)}}, 0)
	client := newClient(t, clientTLSConfig(ca, "localhost"))
	for i := range 20 {
		resp, err := client.Get("https://" + addr + "/")
		if err != nil {
			t.Fatalf("round %d GET: %v", i, err)
		}
		if !resp.Close {
			t.Fatalf("round %d: response did not announce Connection: close", i)
		}
		readBody(t, resp)
		if resp, err = client.Post("https://"+addr+"/", "text/plain", strings.NewReader("x")); err != nil {
			t.Fatalf("round %d POST: %v", i, err)
		}
		readBody(t, resp)
	}
}

// Keep-alive over TLS: the session idles on the poller and resumes, TLS state
// intact, for the next request.
func TestHTTPSKeepAliveResumesIdleSession(t *testing.T) {
	ca := newTestCA(t)
	addr := startTLS(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.WriteString(w, r.URL.Path)
	}), &tls.Config{Certificates: []tls.Certificate{ca.serverCert(t)}}, time.Minute)
	c, err := tls.Dial("tcp", addr, clientTLSConfig(ca, "localhost"))
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	br := bufio.NewReader(c)
	for i := range 3 {
		fmt.Fprintf(c, "GET /r%d HTTP/1.1\r\nHost: x\r\n\r\n", i)
		resp, err := http.ReadResponse(br, nil)
		if err != nil {
			t.Fatalf("request %d: %v", i, err)
		}
		if body := readBody(t, resp); body != fmt.Sprintf("/r%d", i) {
			t.Fatalf("request %d: body %q", i, body)
		}
		time.Sleep(50 * time.Millisecond) // idle on the poller between requests
	}
}

// Over TLS a header that never ends is cut off at MaxHeaderBytes, as it is in
// plaintext, instead of being read into memory without bound.
func TestHTTPSHeaderIsBounded(t *testing.T) {
	ca := newTestCA(t)
	addr := startTLS(t, http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}),
		&tls.Config{Certificates: []tls.Certificate{ca.serverCert(t)}}, 0)
	c, err := tls.Dial("tcp", addr, clientTLSConfig(ca, "localhost"))
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	go func() {
		_, _ = io.WriteString(c, "GET / HTTP/1.1\r\nHost: x\r\nX-Big: ")
		chunk := bytes.Repeat([]byte("a"), 64<<10)
		_ = c.SetWriteDeadline(time.Now().Add(5 * time.Second))
		for range 256 {
			if _, err := c.Write(chunk); err != nil {
				return
			}
		}
	}()
	_ = c.SetReadDeadline(time.Now().Add(5 * time.Second))
	resp, err := http.ReadResponse(bufio.NewReader(c), nil)
	if err != nil {
		t.Fatalf("no answer to a 16 MiB header: %v", err)
	}
	if resp.StatusCode != http.StatusRequestHeaderFieldsTooLarge {
		t.Fatalf("status %d, want 431", resp.StatusCode)
	}
}

// Shutdown lets a request in progress finish and closes idle connections.
func TestHTTPShutdown(t *testing.T) {
	started := make(chan struct{})
	release := make(chan struct{})
	srv := &Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/slow" {
			close(started)
			<-release
		}
		_, _ = io.WriteString(w, "done")
	})}
	addr := startServer(t, srv)

	idle := dialRaw(t, addr)
	defer idle.close()
	_, _ = io.WriteString(idle.c, "GET / HTTP/1.1\r\nHost: x\r\n\r\n")
	if resp := idle.readResponse(http.MethodGet); readBody(t, resp) != "done" {
		t.Fatal("warm-up request failed")
	}

	busy := dialRaw(t, addr)
	defer busy.close()
	_, _ = io.WriteString(busy.c, "GET /slow HTTP/1.1\r\nHost: x\r\n\r\n")
	<-started

	done := make(chan error, 1)
	go func() { done <- srv.Shutdown(context.Background()) }()
	time.Sleep(100 * time.Millisecond)
	if out := idle.readAll(time.Second); out != "" {
		t.Fatalf("idle connection got %q instead of being closed", out)
	}
	select {
	case err := <-done:
		t.Fatalf("Shutdown returned (%v) with a request in progress", err)
	default:
	}
	close(release)
	resp := busy.readResponse(http.MethodGet)
	if !resp.Close || readBody(t, resp) != "done" {
		t.Fatal("the request in progress did not finish with Connection: close")
	}
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Shutdown: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Shutdown did not return after the last request")
	}
}

// A body the handler hands to a goroutine cannot be read once the handler has
// returned: the connection's reader may serve another request by then.
func TestHTTPBodyReadAfterHandlerReturns(t *testing.T) {
	late := make(chan error, 1)
	addr := startHTTPFunc(t, func(w http.ResponseWriter, r *http.Request) {
		go func() {
			time.Sleep(50 * time.Millisecond)
			_, err := r.Body.Read(make([]byte, 8))
			late <- err
		}()
	})
	rawExchange(t, addr, "POST / HTTP/1.1\r\nHost: x\r\nContent-Length: 4\r\nConnection: close\r\n\r\nbody")
	if err := <-late; !errors.Is(err, http.ErrBodyReadAfterClose) {
		t.Fatalf("late body read: %v", err)
	}
}

// Handler panics are logged, with the stack, and end only that connection.
func TestHTTPPanicIsLogged(t *testing.T) {
	var buf syncBuffer
	addr := startServer(t, &Server{
		ErrorLog: log.New(&buf, "", 0),
		Handler:  http.HandlerFunc(func(http.ResponseWriter, *http.Request) { panic("boom") }),
	})
	rawExchange(t, addr, "GET / HTTP/1.1\r\nHost: x\r\n\r\n")
	waitFor := time.Now().Add(2 * time.Second)
	for !strings.Contains(buf.String(), "panic serving") && time.Now().Before(waitFor) {
		time.Sleep(10 * time.Millisecond)
	}
	if s := buf.String(); !strings.Contains(s, "boom") || !strings.Contains(s, "goroutine") {
		t.Fatalf("panic not logged with its stack: %q", s)
	}
}

type syncBuffer struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (s *syncBuffer) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.Write(p)
}

func (s *syncBuffer) String() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.String()
}

// The same with keep-alive: the connection goes back to the event loop with
// the end of the response still queued, finds the client's EOF there, and must
// deliver that end before closing.
func TestHTTPHalfCloseKeepAliveLargeResponse(t *testing.T) {
	const size = 16 << 20
	addr := startHTTPFunc(t, func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write(bytes.Repeat([]byte("r"), size))
	})
	c := dialRaw(t, addr)
	defer c.close()
	_, _ = io.WriteString(c.c, "GET / HTTP/1.1\r\nHost: x\r\n\r\n")
	_ = c.c.(*net.TCPConn).CloseWrite()
	_ = c.c.SetReadDeadline(time.Now().Add(20 * time.Second))
	// A slow reader keeps the socket buffers full, so the handler returns with
	// output still queued.
	resp, err := http.ReadResponse(bufio.NewReader(slowReader{c.c}), nil)
	if err != nil {
		t.Fatal(err)
	}
	if n, err := io.Copy(io.Discard, resp.Body); err != nil || n != size {
		t.Fatalf("body %d of %d bytes (%v)", n, size, err)
	}
}

// A hijacked connection has no deadlines, as with net/http: the request's
// ReadTimeout and WriteTimeout end with the request, and a WebSocket or other
// protocol taken over from it must not inherit them.
func TestHijackClearsDeadlines(t *testing.T) {
	res := make(chan error, 1)
	srv := &Server{
		ReadTimeout:  100 * time.Millisecond,
		WriteTimeout: 100 * time.Millisecond,
		Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			conn, rw, err := w.(http.Hijacker).Hijack()
			if err != nil {
				res <- err
				return
			}
			go func() {
				defer conn.Close()
				time.Sleep(300 * time.Millisecond) // past both deadlines
				line, err := rw.ReadString('\n')
				if err == nil {
					_, err = conn.Write([]byte(line))
				}
				res <- err
			}()
		}),
	}
	addr := startServer(t, srv)
	c := dialRaw(t, addr)
	defer c.close()
	_, _ = io.WriteString(c.c, "GET / HTTP/1.1\r\nHost: x\r\n\r\n")
	time.Sleep(200 * time.Millisecond)
	_, _ = io.WriteString(c.c, "ping\n")
	out := c.readAll(2 * time.Second)
	if err := <-res; err != nil || out != "ping\n" {
		t.Fatalf("after the deadlines the hijacked connection got %q (%v)", out, err)
	}
}

// Shutdown lets a request in progress on an idle-then-resumed TLS session
// finish: a session a worker is serving is not idle.
func TestHTTPSShutdownLetsSessionRequestFinish(t *testing.T) {
	ca := newTestCA(t)
	started := make(chan struct{}, 1)
	release := make(chan struct{})
	srv := &Server{
		Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.URL.Path == "/slow" {
				started <- struct{}{}
				<-release
			}
			_, _ = io.WriteString(w, "done")
		}),
		TLSConfig:   &tls.Config{Certificates: []tls.Certificate{ca.serverCert(t)}},
		IdleTimeout: time.Minute,
	}
	addr := startServer(t, srv)
	c, err := tls.Dial("tcp", addr, clientTLSConfig(ca, "localhost"))
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	br := bufio.NewReader(c)
	_, _ = io.WriteString(c, "GET / HTTP/1.1\r\nHost: x\r\n\r\n")
	resp, err := http.ReadResponse(br, nil)
	if err != nil || readBody(t, resp) != "done" {
		t.Fatalf("warm-up request: %v", err)
	}
	time.Sleep(50 * time.Millisecond) // the session idles on the poller
	_, _ = io.WriteString(c, "GET /slow HTTP/1.1\r\nHost: x\r\n\r\n")
	<-started

	done := make(chan error, 1)
	go func() { done <- srv.Shutdown(context.Background()) }()
	time.Sleep(100 * time.Millisecond)
	close(release)
	_ = c.SetReadDeadline(time.Now().Add(5 * time.Second))
	if resp, err = http.ReadResponse(br, nil); err != nil {
		t.Fatalf("the request in progress lost its response: %v", err)
	}
	if !resp.Close || readBody(t, resp) != "done" {
		t.Fatal("the request in progress did not finish with Connection: close")
	}
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Shutdown: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Shutdown did not return after the last request")
	}
}

// The framing a handler sets is made consistent as net/http makes it: only
// chunked marks where a body ends, so without a Content-Length another coding
// (identity, once recommended for event streams) is delimited by closing the
// connection; a body-less status carries no framing headers.
func TestHTTPHandlerFramingIsConsistent(t *testing.T) {
	addr := startHTTPFunc(t, func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/identity":
			w.Header().Set("Transfer-Encoding", "identity")
			_, _ = io.WriteString(w, "hello")
		case "/nocontent":
			w.Header().Set("Content-Length", "5")
			w.WriteHeader(http.StatusNoContent)
		case "/both":
			w.Header().Set("Transfer-Encoding", "chunked")
			w.Header().Set("Content-Length", "5")
			_, _ = io.WriteString(w, "hello")
		}
	})
	// Keep-alive requests: a response the client cannot delimit would hang it.
	out := rawExchange(t, addr, "GET /identity HTTP/1.1\r\nHost: x\r\n\r\n")
	head, body, _ := strings.Cut(out, "\r\n\r\n")
	if strings.Contains(head, "Transfer-Encoding") || !strings.Contains(head, "Connection: close") || body != "hello" {
		t.Fatalf("identity: %q", out)
	}
	out = rawExchange(t, addr, "GET /nocontent HTTP/1.1\r\nHost: x\r\nConnection: close\r\n\r\n")
	if !strings.HasPrefix(out, "HTTP/1.1 204") || strings.Contains(out, "Content-Length") {
		t.Fatalf("204: %q", out)
	}
	out = rawExchange(t, addr, "GET /both HTTP/1.1\r\nHost: x\r\nConnection: close\r\n\r\n")
	resp, err := http.ReadResponse(bufio.NewReader(strings.NewReader(out)), nil)
	if err != nil || strings.Contains(out, "Content-Length") || readBody(t, resp) != "hello" {
		t.Fatalf("chunked and a length: %q (%v)", out, err)
	}
}

// An expectation other than 100-continue is refused with 417, as net/http
// does (RFC 9110 10.1.1).
func TestHTTPUnknownExpectation(t *testing.T) {
	addr := startHTTPFunc(t, func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.WriteString(w, "served")
	})
	out := rawExchange(t, addr, "POST / HTTP/1.1\r\nHost: x\r\nExpect: 200-ok\r\nContent-Length: 1\r\n\r\nx")
	if !strings.HasPrefix(out, "HTTP/1.1 417") || strings.Contains(out, "served") {
		t.Fatalf("response %q", out)
	}
}
