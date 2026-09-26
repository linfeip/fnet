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
