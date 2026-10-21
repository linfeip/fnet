package fhttp

import (
	"bufio"
	"bytes"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"io"
	"log"
	"log/slog"
	"math/big"
	"net"
	"net/http"
	"os"
	"reflect"
	"runtime"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
	"unsafe"

	"github.com/linfeip/fnet"
	"github.com/linfeip/fnet/internal/bytepool"
	"github.com/linfeip/fnet/internal/units"
	"github.com/linfeip/fnet/taskpool"
)

// tlsServerConfig returns a server configuration with a self-signed certificate, which the clients do not verify.
func tlsServerConfig(t *testing.T) *tls.Config {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	template := &x509.Certificate{SerialNumber: big.NewInt(1)}
	der, err := x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	return &tls.Config{Certificates: []tls.Certificate{{Certificate: [][]byte{der}, PrivateKey: key}}}
}

func testMux() *http.ServeMux {
	mux := http.NewServeMux()
	mux.HandleFunc("/hello", func(w http.ResponseWriter, r *http.Request) {
		io.WriteString(w, "hello world")
	})
	mux.HandleFunc("/status", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("X-Custom", "v")
		w.WriteHeader(http.StatusCreated)
	})
	mux.HandleFunc("/remote", func(w http.ResponseWriter, r *http.Request) {
		io.WriteString(w, r.RemoteAddr)
	})
	mux.HandleFunc("/big", func(w http.ResponseWriter, r *http.Request) {
		w.Write(bytes.Repeat([]byte("0123456789abcdef"), 256*units.KB)) // 4MB
	})
	mux.HandleFunc("/panic", func(w http.ResponseWriter, r *http.Request) {
		panic("boom")
	})
	mux.HandleFunc("/echo", func(w http.ResponseWriter, r *http.Request) {
		io.Copy(w, r.Body)
	})
	return mux
}

func newTestServer(t *testing.T, opts Options) *Server {
	t.Helper()
	return serve(t, testMux(), opts)
}

func serve(t *testing.T, handler http.Handler, opts Options) *Server {
	t.Helper()
	s, err := NewServer("127.0.0.1:0", handler, opts)
	if err != nil {
		t.Fatal(err)
	}
	served := make(chan error, 1)
	go func() { served <- s.Serve() }()
	t.Cleanup(func() {
		s.Close()
		if err := <-served; !errors.Is(err, http.ErrServerClosed) {
			t.Errorf("Serve returned %v", err)
		}
	})
	return s
}

func get(t *testing.T, client *http.Client, url string) (*http.Response, string) {
	t.Helper()
	resp, err := client.Get(url)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	return resp, string(body)
}

func TestClient(t *testing.T) {
	s := newTestServer(t, Options{})
	base := "http://" + s.Addr().String()
	client := &http.Client{Transport: &http.Transport{}}
	defer client.CloseIdleConnections()

	resp, body := get(t, client, base+"/hello")
	if resp.StatusCode != 200 || body != "hello world" || resp.ContentLength != 11 ||
		resp.Header.Get("Content-Type") != "text/plain; charset=utf-8" || resp.Header.Get("Date") == "" {
		t.Fatalf("GET /hello: %d %q %v", resp.StatusCode, body, resp.Header)
	}

	// keep-alive: the two requests reuse the same connection.
	_, addr1 := get(t, client, base+"/remote")
	_, addr2 := get(t, client, base+"/remote")
	if addr1 != addr2 {
		t.Fatalf("connection was not reused: %s vs %s", addr1, addr2)
	}

	// A small body, with Content-Length or chunked (hiding the concrete type keeps the client from knowing the length),
	// is buffered and served on fnet, keeping the connection alive.
	for _, body := range []io.Reader{strings.NewReader("ping"), struct{ io.Reader }{strings.NewReader("ping")}} {
		resp, err := client.Post(base+"/echo", "text/plain", body)
		if err != nil {
			t.Fatal(err)
		}
		got, err := io.ReadAll(resp.Body)
		resp.Body.Close()
		if err != nil || resp.StatusCode != 200 || string(got) != "ping" || resp.Close {
			t.Fatalf("POST with a body: %d %q close=%v err=%v", resp.StatusCode, got, resp.Close, err)
		}
	}

	if resp, err := client.Head(base + "/hello"); err != nil || resp.ContentLength != 11 {
		t.Fatalf("HEAD: %v %v", resp, err)
	}
	if resp, _ := get(t, client, base+"/status"); resp.StatusCode != 201 || resp.Header.Get("X-Custom") != "v" {
		t.Fatalf("GET /status: %d %v", resp.StatusCode, resp.Header)
	}
	if resp, _ := get(t, client, base+"/missing"); resp.StatusCode != 404 {
		t.Fatalf("GET /missing: %d", resp.StatusCode)
	}
	if _, body := get(t, client, base+"/big"); len(body) != 4*units.MB {
		t.Fatalf("GET /big: %d bytes", len(body))
	}

	// Silence the log from the Handler panic. slog.SetDefault also rewrites the output and flags of the
	// log package, so both have to be restored at the end; otherwise every later log written through the
	// default slog (which goes through the log package underneath) would be discarded.
	defer func(w io.Writer, flags int) { log.SetOutput(w); log.SetFlags(flags) }(log.Writer(), log.Flags())
	defer slog.SetDefault(slog.Default())
	slog.SetDefault(slog.New(slog.NewTextHandler(io.Discard, nil)))
	if _, err := client.Get(base + "/panic"); err == nil {
		t.Fatal("the connection should be closed after a Handler panic")
	}
}

// runOnExecutor is the frame in which the Executor of TestExecutor runs its tasks; the Handler looks for it in its own
// stack.
//
//go:noinline
func runOnExecutor(task func()) { task() }

// TestExecutor checks that the Handlers run as tasks of Options.Executor, those of two requests sent one after the other
// on one connection included.
func TestExecutor(t *testing.T) {
	onExecutor := make(chan bool, 1)
	s := serve(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		stack := make([]byte, 16*units.KB)
		onExecutor <- strings.Contains(string(stack[:runtime.Stack(stack, false)]), "runOnExecutor")
		io.WriteString(w, "ok")
	}), Options{Executor: func(_ int, task func()) { go runOnExecutor(task) }})
	c, err := net.Dial("tcp", s.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	c.SetDeadline(time.Now().Add(5 * time.Second))
	br := bufio.NewReader(c)
	for range 2 {
		io.WriteString(c, "GET / HTTP/1.1\r\nHost: a\r\n\r\n")
		resp, err := http.ReadResponse(br, nil)
		if err != nil {
			t.Fatal(err)
		}
		if body, _ := io.ReadAll(resp.Body); string(body) != "ok" {
			t.Fatalf("GET /: %q", body)
		}
		if !<-onExecutor {
			t.Fatal("the Handler did not run as a task of Options.Executor")
		}
	}
}

func dialRaw(t *testing.T, s *Server, data string) *bufio.Reader {
	t.Helper()
	c, err := net.Dial("tcp", s.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { c.Close() })
	c.SetDeadline(time.Now().Add(5 * time.Second))
	if _, err := io.WriteString(c, data); err != nil {
		t.Fatal(err)
	}
	return bufio.NewReader(c)
}

func readResp(t *testing.T, br *bufio.Reader) (*http.Response, string) {
	t.Helper()
	resp, err := http.ReadResponse(br, nil)
	if err != nil {
		t.Fatal(err)
	}
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	return resp, string(body)
}

func expectClosed(t *testing.T, br *bufio.Reader) {
	t.Helper()
	if b, err := br.ReadByte(); err != io.EOF {
		t.Fatalf("want the connection to be closed, got %q %v", b, err)
	}
}

func TestPipelining(t *testing.T) {
	s := newTestServer(t, Options{})
	br := dialRaw(t, s, "GET /hello HTTP/1.1\r\nHost: a\r\n\r\n"+
		"GET /status HTTP/1.1\r\nHost: a\r\n\r\n")
	for _, want := range []string{"200 hello world", "201 "} {
		resp, body := readResp(t, br)
		if got := fmt.Sprintf("%d %s", resp.StatusCode, body); got != want {
			t.Fatalf("got %q, want %q", got, want)
		}
	}
}

// TestEmptyLineBeforeRequest checks that empty lines before a request are ignored (RFC 9112 2.2), such as the CRLF some
// clients send after a request body.
func TestEmptyLineBeforeRequest(t *testing.T) {
	s := newTestServer(t, Options{})
	br := dialRaw(t, s, "\r\nPOST /echo HTTP/1.1\r\nHost: a\r\nContent-Length: 2\r\n\r\nhi\r\n\n"+
		"GET /hello HTTP/1.1\r\nHost: a\r\n\r\n")
	for _, want := range []string{"200 hi", "200 hello world"} {
		resp, body := readResp(t, br)
		if got := fmt.Sprintf("%d %s", resp.StatusCode, body); got != want {
			t.Fatalf("got %q, want %q", got, want)
		}
	}

	// An empty line split between two arrivals.
	c, err := net.Dial("tcp", s.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	c.Write([]byte("\r"))
	time.Sleep(50 * time.Millisecond)
	c.Write([]byte("\nGET /hello HTTP/1.1\r\nHost: a\r\n\r\n"))
	c.SetReadDeadline(time.Now().Add(5 * time.Second))
	if resp, body := readResp(t, bufio.NewReader(c)); resp.StatusCode != http.StatusOK || body != "hello world" {
		t.Fatalf("split empty line: got %d %q", resp.StatusCode, body)
	}
}

// recordConn is a fnet.Conn that records what is written to it; it only implements the methods used while handling
// a request.
type recordConn struct {
	fnet.Conn
	writes []string
	paused bool
	full   bool // the send buffer is over what Writable allows
	closed bool
	err    error // what Write returns
}

func (c *recordConn) Close() error   { c.closed = true; return nil }
func (c *recordConn) PauseRead()     { c.paused = true }
func (c *recordConn) ResumeRead()    { c.paused = false }
func (c *recordConn) Writable() bool { return !c.full }

func (c *recordConn) Write(b []byte) (int, error) {
	if c.err != nil {
		return 0, c.err
	}
	c.writes = append(c.writes, string(b))
	return len(b), nil
}

func (c *recordConn) Writev(bs [][]byte) (int, error) {
	var b []byte
	for _, s := range bs {
		b = append(b, s...)
	}
	return c.Write(b)
}

func (*recordConn) SetDeadline(time.Time) {}
func (*recordConn) RemoteAddr() net.Addr  { return &net.TCPAddr{} }
func (*recordConn) LocalAddr() net.Addr   { return &net.TCPAddr{} }

// queueConn returns a conn writing to rc whose requests the test serves itself: serving, so onData starts no worker.
func queueConn(rc *recordConn) *conn {
	return &conn{srv: &Server{handler: testMux(), opts: Options{}.withDefaults()}, connection: rc, worker: workerServing}
}

func TestResponseWriteAfterClose(t *testing.T) {
	c := &conn{connection: &recordConn{}}
	c.closed.Store(true)
	w := &response{conn: c, header: make(http.Header)}
	if n, err := w.Write([]byte("body")); n != 0 || !errors.Is(err, net.ErrClosed) {
		t.Fatalf("Write after close: n=%d err=%v", n, err)
	}
}

// TestQueueCapacity verifies that once the queue has been drained the large capacity left behind by a
// pipelining burst is released, while the capacity for a small number of requests is retained so that
// each request does not have to allocate again.
func TestQueueCapacity(t *testing.T) {
	const get = "GET /hello HTTP/1.1\r\nHost: a\r\n\r\n"
	c := queueConn(&recordConn{})
	c.onData([]byte(strings.Repeat(get, 4*maxRetainedQueueCapacity)))
	c.serve()
	if cap(c.queue) > maxRetainedQueueCapacity {
		t.Fatalf("queue capacity after a burst %d, want no more than %d", cap(c.queue), maxRetainedQueueCapacity)
	}

	c.worker = workerServing
	c.onData([]byte(get))
	c.serve()
	if cap(c.queue) != 1 {
		t.Fatalf("queue capacity after a single request %d, want to retain 1", cap(c.queue))
	}
}

// TestQueueBackpressure checks that a connection with too much queued, by number or by size, is not read until its
// worker has answered everything, and that a client pipelining more than that is answered in full. Every request at
// hand is queued before reading pauses: the engine does not pass the data left unconsumed on again when reading
// resumes, so a request left there would wait for data the client, awaiting its answer, never sends.
func TestQueueBackpressure(t *testing.T) {
	const get = "GET /hello HTTP/1.1\r\nHost: a\r\n\r\n"
	post := fmt.Sprintf("POST /echo HTTP/1.1\r\nHost: a\r\nContent-Length: %d\r\n\r\n%s", maxQueuedBytes,
		strings.Repeat("x", maxQueuedBytes))
	for _, tt := range []struct {
		name     string
		data     string
		requests int
		paused   bool
	}{
		{"below the limits", strings.Repeat(get, maxPipeline-1), maxPipeline - 1, false},
		{"maxPipeline requests", strings.Repeat(get, maxPipeline), maxPipeline, true},
		{"maxQueuedBytes of request", post, 1, true},
		{"past maxPipeline", strings.Repeat(get, maxPipeline+1), maxPipeline + 1, true},
		{"a request after maxQueuedBytes", post + get, 2, true},
	} {
		rc := &recordConn{}
		c := queueConn(rc)
		if consumed := c.onData([]byte(tt.data)); consumed != len(tt.data) {
			t.Fatalf("%s: consumed %d of %d bytes", tt.name, consumed, len(tt.data))
		}
		if rc.paused != tt.paused {
			t.Fatalf("%s: paused=%v, want %v", tt.name, rc.paused, tt.paused)
		}
		c.serve()
		if got := strings.Count(strings.Join(rc.writes, ""), "HTTP/1.1 200 OK"); rc.paused || got != tt.requests {
			t.Fatalf("%s: after serving %d responses and paused=%v, want %d and false", tt.name, got, rc.paused, tt.requests)
		}
	}

	s := newTestServer(t, Options{})
	const count = 3 * maxPipeline
	br := dialRaw(t, s, strings.Repeat(get, count))
	for i := range count {
		if resp, body := readResp(t, br); resp.StatusCode != http.StatusOK || body != "hello world" {
			t.Fatalf("response %d: %d %q", i, resp.StatusCode, body)
		}
	}
	br = dialRaw(t, s, post+get)
	for i, want := range []string{strings.Repeat("x", maxQueuedBytes), "hello world"} {
		if resp, body := readResp(t, br); resp.StatusCode != http.StatusOK || body != want {
			t.Fatalf("response %d after a full queue: %d, %d bytes", i, resp.StatusCode, len(body))
		}
	}
}

// TestWaitWritable checks that the worker stops answering a client that does not take the responses (fnet's send buffer
// is full), with IdleTimeout as its deadline, and answers the rest once fnet reports the buffer back down.
func TestWaitWritable(t *testing.T) {
	const get = "GET /hello HTTP/1.1\r\nHost: a\r\n\r\n"
	rc := &recordConn{full: true}
	c := queueConn(rc)
	c.serveTask = c.serve
	c.srv.opts.Executor = func(_ int, task func()) { task() }
	c.onData([]byte(strings.Repeat(get, 3)))
	c.serve()
	if len(rc.writes) != 0 || c.worker != workerWaiting {
		t.Fatalf("answered %d writes while the client took nothing, worker state %d", len(rc.writes), c.worker)
	}
	rc.full = false
	c.onWritable()
	if got := strings.Count(strings.Join(rc.writes, ""), "HTTP/1.1 200 OK"); got != 3 || c.worker != workerIdle {
		t.Fatalf("%d responses once writable, worker state %d; want 3, idle", got, c.worker)
	}
}

// TestWorkerState follows a connection's worker through its states: one that finds the client no longer taking the
// responses writes out what it has collected and waits, a request arriving meanwhile submits no other worker, onWritable
// resumes it, and once a worker closes the connection a request submits none either.
func TestWorkerState(t *testing.T) {
	const get = "GET / HTTP/1.1\r\nHost: a\r\n\r\n"
	rc := &recordConn{}
	c := queueConn(rc)
	c.serveTask = c.serve
	submitted := 0
	c.srv.opts.Executor = func(_ int, task func()) { submitted++; task() }
	c.srv.handler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/stall" {
			rc.full = true // the client takes nothing from this response on
		}
		io.WriteString(w, "ok")
	})
	responses := func() int { return strings.Count(strings.Join(rc.writes, ""), "HTTP/1.1 200 OK") }

	c.onData([]byte("GET /stall HTTP/1.1\r\nHost: a\r\n\r\n" + get))
	c.serve() // the worker queueConn stands for
	if responses() != 1 || c.worker != workerWaiting {
		t.Fatalf("stalled with %d responses written, worker state %d; want the collected one, waiting", responses(), c.worker)
	}
	c.onData([]byte(get))
	if submitted != 0 {
		t.Fatal("a request submitted a worker while one was waiting")
	}
	rc.full = false
	c.onWritable()
	if responses() != 3 || submitted != 1 || c.worker != workerIdle {
		t.Fatalf("resumed: %d responses, %d workers, worker state %d; want 3, 1, idle", responses(), submitted, c.worker)
	}

	c.onData([]byte("GET / HTTP/1.1\r\nHost: a\r\nConnection: close\r\n\r\n"))
	if responses() != 4 || !rc.closed || c.worker != workerClosing {
		t.Fatalf("closing: %d responses, closed=%v, worker state %d; want 4, true, closing", responses(), rc.closed, c.worker)
	}
	c.onData([]byte(get))
	if submitted != 2 {
		t.Fatal("a request submitted a worker while one was closing the connection")
	}
}

// TestFlushFailure checks that a worker whose write fails closes the connection and stops for good, as one answering a
// request that closes the connection does.
func TestFlushFailure(t *testing.T) {
	const get = "GET / HTTP/1.1\r\nHost: a\r\n\r\n"
	rc := &recordConn{err: net.ErrClosed}
	c := queueConn(rc)
	c.serveTask = c.serve
	submitted := 0
	c.srv.opts.Executor = func(_ int, task func()) { submitted++; task() }
	c.onData([]byte(get))
	c.serve() // the worker queueConn stands for
	if !rc.closed || c.worker != workerClosing {
		t.Fatalf("after a failed write: closed=%v, worker state %d; want true, closing", rc.closed, c.worker)
	}
	c.onData([]byte(get))
	if submitted != 0 {
		t.Fatal("a request submitted a worker while one was closing the connection")
	}
}

// TestSlowReader has a client pipeline requests for large responses and read none: the server answers only as many as
// the socket and fnet's send buffer take, rather than piling every response up in memory.
func TestSlowReader(t *testing.T) {
	var answered atomic.Int32
	big := bytes.Repeat([]byte("x"), 4*units.MB)
	s := serve(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		answered.Add(1)
		w.Write(big)
	}), Options{})
	const requests = 32
	dialRaw(t, s, strings.Repeat("GET / HTTP/1.1\r\nHost: a\r\n\r\n", requests))
	time.Sleep(500 * time.Millisecond)
	if n := answered.Load(); n >= requests/2 {
		t.Fatalf("answered %d of %d requests to a client that reads nothing", n, requests)
	}
}

// TestReuseRequests checks that the requests fhttp parsed are released for reuse once their Handler has returned,
// without anything of one request showing in the next. Whether the pool hands the same one out again is not checked:
// under the race detector sync.Pool drops some of what it is given.
func TestReuseRequests(t *testing.T) {
	var seen []*http.Request
	var views []string
	c := queueConn(&recordConn{})
	c.srv.handler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		seen = append(seen, r)
		views = append(views, fmt.Sprintf("%s %s %v %q", r.Method, r.URL, r.Header, body))
	})
	c.onData([]byte("POST /a?x=1 HTTP/1.1\r\nHost: h\r\nX-A: 1\r\nContent-Length: 2\r\n\r\nhi" +
		"GET /b HTTP/1.1\r\nHost: h\r\n\r\n"))
	c.serve()
	want := []string{`POST /a?x=1 map[Content-Length:[2] X-A:[1]] "hi"`, `GET /b map[] ""`}
	if !slices.Equal(views, want) {
		t.Errorf("requests %q, want %q", views, want)
	}
	for i, r := range seen {
		if r.Method != "" || len(r.Header) != 0 {
			t.Errorf("request %d was not released: %q %v", i, r.Method, r.Header)
		}
	}
}

// TestBatchedResponses checks that the responses to queued requests go out in a single write, and that the largest body
// still buffered is written without being copied, after what was collected before it.
func TestBatchedResponses(t *testing.T) {
	const get = "GET /hello HTTP/1.1\r\nHost: a\r\n\r\n"
	rc := &recordConn{}
	c := queueConn(rc)
	if n := c.onData([]byte(strings.Repeat(get, 16) + "GET /hel")); n != 16*len(get) {
		t.Fatalf("consumed %d bytes, want %d", n, 16*len(get))
	}
	c.serve()
	if len(rc.writes) != 1 || strings.Count(rc.writes[0], "hello world") != 16 {
		t.Fatalf("got %d writes, want one with 16 responses: %q", len(rc.writes), rc.writes)
	}

	rc = &recordConn{}
	c = queueConn(rc)
	full := strings.Repeat("x", maxBufferedBody)
	c.srv.handler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/full" {
			io.WriteString(w, full)
			return
		}
		io.WriteString(w, "hello world")
	})
	c.onData([]byte(get + "GET /full HTTP/1.1\r\nHost: a\r\n\r\n" + get))
	c.serve()
	if len(rc.writes) != 2 || !strings.HasPrefix(rc.writes[0], "HTTP/1.1 200 OK") || !strings.Contains(rc.writes[0], "hello world") ||
		!strings.Contains(rc.writes[0], fmt.Sprintf("Content-Length: %d\r\n", maxBufferedBody)) || !strings.HasSuffix(rc.writes[0], full) ||
		strings.Count(rc.writes[1], "hello world") != 1 {
		t.Fatalf("got %d writes, want the first response together with /full, then the last response", len(rc.writes))
	}
}

// TestStreamedResponse checks how a body over maxBufferedBody goes out while the Handler writes it: in chunks of
// maxBufferedBody after the responses collected before it, with the Content-Length the Handler declared, or until the
// connection closes on HTTP/1.0, and not at all for a response without content.
func TestStreamedResponse(t *testing.T) {
	const get = "GET /hello HTTP/1.1\r\nHost: a\r\n\r\n"
	rc := &recordConn{}
	c := queueConn(rc)
	c.onData([]byte(get + "GET /big HTTP/1.1\r\nHost: a\r\n\r\n" + get))
	c.serve()
	if want := 4*units.MB/maxBufferedBody + 1; len(rc.writes) != want || !strings.Contains(rc.writes[0], "hello world") {
		t.Fatalf("got %d writes, want %d, the first with the response collected before /big", len(rc.writes), want)
	}
	br := bufio.NewReader(strings.NewReader(strings.Join(rc.writes, "")))
	for i, want := range []int{11, 4 * units.MB, 11} {
		if resp, body := readResp(t, br); len(body) != want || (i == 1) != (resp.ContentLength == -1) {
			t.Fatalf("response %d: %d bytes, Content-Length %d", i, len(body), resp.ContentLength)
		}
	}

	// Small writes go out in chunks of maxBufferedBody, but for the last one.
	rc = &recordConn{}
	c = queueConn(rc)
	c.srv.handler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		for range 300 {
			io.WriteString(w, strings.Repeat("y", 1000))
		}
	})
	c.onData([]byte(get))
	c.serve()
	wire := strings.Join(rc.writes, "")
	if n := strings.Count(wire, fmt.Sprintf("\n%x\r\n", maxBufferedBody)); n != 300*1000/maxBufferedBody {
		t.Fatalf("%d chunks of maxBufferedBody, want %d", n, 300*1000/maxBufferedBody)
	}
	if _, body := readResp(t, bufio.NewReader(strings.NewReader(wire))); body != strings.Repeat("y", 300*1000) {
		t.Fatalf("small writes: %d bytes", len(body))
	}

	// Silence the log from the Handler panic, as TestClient does.
	defer func(w io.Writer, flags int) { log.SetOutput(w); log.SetFlags(flags) }(log.Writer(), log.Flags())
	defer slog.SetDefault(slog.Default())
	slog.SetDefault(slog.New(slog.NewTextHandler(io.Discard, nil)))
	payload := strings.Repeat("x", 100*units.KB)
	write := func(w http.ResponseWriter) error {
		_, err := io.WriteString(w, payload)
		return err
	}
	declare := func(length string) func(http.ResponseWriter) error {
		return func(w http.ResponseWriter) error {
			w.Header().Set("Content-Length", length)
			return write(w)
		}
	}
	const head, http10 = "HEAD /hello HTTP/1.1\r\nHost: a\r\n\r\n", "GET /hello HTTP/1.0\r\nConnection: keep-alive\r\n\r\n"
	for _, tt := range []struct {
		name      string
		request   string
		handle    func(http.ResponseWriter) error
		err       error    // what handle returns
		fields    []string // field lines the header has
		absent    []string // field names the header has not
		body      string   // what the client reads of the body
		truncated bool     // the body ends before its length
		closed    bool     // the connection is closed after the response
	}{
		{name: "chunked", request: get, handle: write, fields: []string{"Transfer-Encoding: chunked"}, absent: []string{"Content-Length"}, body: payload},
		{name: "declared length", request: get, handle: declare("102400"), fields: []string{"Content-Length: 102400"},
			absent: []string{"Transfer-Encoding"}, body: payload},
		{name: "declared length short of a piece", request: get, handle: declare("30000"), err: http.ErrContentLength,
			fields: []string{"Content-Length: 30000"}, truncated: true, closed: true},
		{name: "declared length short of the last piece", request: get, handle: declare("70000"), fields: []string{"Content-Length: 70000"},
			body: payload[:70000], closed: true},
		{name: "declared length not reached", request: get, handle: declare("200000"), fields: []string{"Content-Length: 200000"},
			body: payload, truncated: true, closed: true},
		{name: "HTTP/1.0", request: http10, handle: write, fields: []string{"Connection: close"},
			absent: []string{"Content-Length", "Transfer-Encoding"}, body: payload, closed: true},
		{name: "HTTP/1.0 declared length", request: http10, handle: declare("102400"),
			fields: []string{"Content-Length: 102400", "Connection: keep-alive"}, body: payload},
		{name: "HEAD", request: head, handle: write, absent: []string{"Content-Length", "Transfer-Encoding"}},
		{name: "HEAD declared length", request: head, handle: declare("102400"), fields: []string{"Content-Length: 102400"},
			absent: []string{"Transfer-Encoding"}},
		{name: "205", request: get, handle: func(w http.ResponseWriter) error {
			w.WriteHeader(http.StatusResetContent)
			return write(w)
		}, fields: []string{"Content-Length: 0"}, absent: []string{"Transfer-Encoding"}},
		{name: "upgrade after the body started", request: get, handle: func(w http.ResponseWriter) error {
			write(w)
			return Upgrade(w, func(fnet.Conn) Protocol { panic("upgraded") })
		}, err: http.ErrNotSupported, fields: []string{"Transfer-Encoding: chunked"}, body: payload},
		{name: "panic", request: get, handle: func(w http.ResponseWriter) error {
			write(w)
			panic("boom")
		}, fields: []string{"Transfer-Encoding: chunked"}, body: payload[:maxBufferedBody], truncated: true, closed: true},
	} {
		rc := &recordConn{}
		c := queueConn(rc)
		var err error
		c.srv.handler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { err = tt.handle(w) })
		c.onData([]byte(tt.request))
		c.serve()
		wire := strings.Join(rc.writes, "")
		header, _, _ := strings.Cut(wire, "\r\n\r\n")
		for _, field := range tt.fields {
			if !strings.Contains(header+"\r\n", "\r\n"+field+"\r\n") {
				t.Errorf("%s: no %q in the header %q", tt.name, field, header)
			}
		}
		for _, name := range tt.absent {
			if strings.Contains(header, "\r\n"+name+":") {
				t.Errorf("%s: %s in the header %q", tt.name, name, header)
			}
		}
		method, _, _ := strings.Cut(tt.request, " ")
		resp, readErr := http.ReadResponse(bufio.NewReader(strings.NewReader(wire)), &http.Request{Method: method})
		if readErr != nil {
			t.Fatalf("%s: %v", tt.name, readErr)
		}
		body, readErr := io.ReadAll(resp.Body)
		if err != tt.err || string(body) != tt.body || (readErr != nil) != tt.truncated || rc.closed != tt.closed {
			t.Errorf("%s: Write returned %v, read %d bytes (%v), closed=%v; want %v, %d bytes, truncated=%v, closed=%v",
				tt.name, err, len(body), readErr, rc.closed, tt.err, len(tt.body), tt.truncated, tt.closed)
		}
	}
}

// TestStreamBackpressure has a Handler write a large body to a client that reads none of it for a while: the Handler is
// held back once fnet has a piece or two waiting, rather than piling the body up in memory, and goes on once the client
// reads, which gets all of it and the answer to the request pipelined behind it.
func TestStreamBackpressure(t *testing.T) {
	const size = 64 * units.MB
	var written atomic.Int64
	s := serve(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/stream" {
			io.WriteString(w, "hello world")
			return
		}
		piece := make([]byte, 32*units.KB)
		for range size / len(piece) {
			if _, err := w.Write(piece); err != nil {
				return
			}
			written.Add(int64(len(piece)))
		}
	}), Options{})
	br := dialRaw(t, s, "GET /stream HTTP/1.1\r\nHost: a\r\n\r\nGET /hello HTTP/1.1\r\nHost: a\r\n\r\n")
	time.Sleep(500 * time.Millisecond)
	if n := written.Load(); n >= 16*units.MB { // the socket buffers take a few MB, fnet and the response 192KB
		t.Fatalf("the Handler wrote %dMB to a client that reads nothing", n/units.MB)
	}
	resp, err := http.ReadResponse(br, nil)
	if err != nil {
		t.Fatal(err)
	}
	if n, err := io.Copy(io.Discard, resp.Body); err != nil || n != size || resp.ContentLength != -1 {
		t.Fatalf("streamed body: %d bytes, Content-Length %d, %v", n, resp.ContentLength, err)
	}
	if resp, body := readResp(t, br); resp.StatusCode != http.StatusOK || body != "hello world" {
		t.Fatalf("pipelined response: %d %q", resp.StatusCode, body)
	}
}

// endlessBody is a Handler writing to its client until a Write fails, which it reports on errs.
func endlessBody(errs chan<- error) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		piece := make([]byte, 32*units.KB)
		for {
			if _, err := w.Write(piece); err != nil {
				errs <- err
				return
			}
		}
	}
}

// TestStreamIdleTimeout checks that a Handler writing to a client that takes nothing gives up after IdleTimeout.
func TestStreamIdleTimeout(t *testing.T) {
	if runtime.GOOS != "linux" && runtime.GOOS != "darwin" {
		t.Skip("elsewhere a write blocks in the kernel, with no time limit")
	}
	errs := make(chan error, 1)
	s := serve(t, endlessBody(errs), Options{IdleTimeout: 300 * time.Millisecond})
	dialRaw(t, s, "GET / HTTP/1.1\r\nHost: a\r\n\r\n")
	select {
	case err := <-errs:
		if !errors.Is(err, os.ErrDeadlineExceeded) {
			t.Fatalf("Write returned %v, want %v", err, os.ErrDeadlineExceeded)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Write still waits for a client that takes nothing")
	}
}

// TestStreamClientGone checks that a Handler writing a large body learns soon that its client went away.
func TestStreamClientGone(t *testing.T) {
	errs := make(chan error, 1)
	s := serve(t, endlessBody(errs), Options{})
	c, err := net.Dial("tcp", s.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	io.WriteString(c, "GET / HTTP/1.1\r\nHost: a\r\n\r\n")
	if _, err := io.CopyN(io.Discard, c, units.MB); err != nil {
		t.Fatal(err)
	}
	c.Close()
	select {
	case <-errs:
	case <-time.After(2 * time.Second):
		t.Fatal("Write still goes on after the client closed")
	}
}

// TestStreamSaturatedExecutor has Handlers waiting in Write hold every goroutine of an executor the connections' tasks
// share. Once the Writes give up after IdleTimeout, the executor serves the connections again; a wait ended only by
// fnet's task reporting the client took something would have stalled it for good.
func TestStreamSaturatedExecutor(t *testing.T) {
	if runtime.GOOS != "linux" && runtime.GOOS != "darwin" {
		t.Skip("elsewhere the engine has no executor, and a write blocks in the kernel")
	}
	pool := taskpool.New(1, 2, 64)
	started, errs := make(chan struct{}, 2), make(chan error, 2)
	stream := endlessBody(errs)
	s := serve(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/hello" {
			io.WriteString(w, "hello world")
			return
		}
		started <- struct{}{}
		stream(w, r)
	}), Options{IdleTimeout: 500 * time.Millisecond, Executor: pool.SubmitTo, Engine: fnet.Options{Executor: pool.SubmitTo}})
	for range 2 { // as many as the executor has goroutines
		dialRaw(t, s, "GET /stream HTTP/1.1\r\nHost: a\r\n\r\n")
		select {
		case <-started:
		case <-time.After(5 * time.Second):
			t.Fatal("the download did not start")
		}
	}
	time.Sleep(100 * time.Millisecond) // the Handlers fill what the sockets take, then wait
	br := dialRaw(t, s, "GET /hello HTTP/1.1\r\nHost: a\r\n\r\n")
	if resp, body := readResp(t, br); resp.StatusCode != http.StatusOK || body != "hello world" {
		t.Fatalf("GET /hello: %d %q", resp.StatusCode, body)
	}
	if len(errs) == 0 { // a Handler reports its failed Write before it frees its goroutine
		t.Fatal("another connection was served while both downloads held the executor")
	}
}

// TestConnSize keeps the state every HTTP connection holds in its 128-byte size class.
func TestConnSize(t *testing.T) {
	if size := unsafe.Sizeof(conn{}); size > 128 {
		t.Fatalf("conn is %d bytes, want no more than 128", size)
	}
}

func TestConnectionManagement(t *testing.T) {
	s := newTestServer(t, Options{})

	// HTTP/1.0 closes the connection by default. The response is HTTP/1.1, the highest version fhttp conforms to
	// (RFC 9110 6.2), so it says it closes.
	br := dialRaw(t, s, "GET /hello HTTP/1.0\r\n\r\n")
	if resp, body := readResp(t, br); resp.Proto != "HTTP/1.1" || !resp.Close || body != "hello world" {
		t.Fatalf("HTTP/1.0: %s close=%v %q", resp.Proto, resp.Close, body)
	}
	expectClosed(t, br)

	// HTTP/1.0 with an explicit keep-alive.
	br = dialRaw(t, s, "GET /hello HTTP/1.0\r\nConnection: keep-alive\r\n\r\nGET /status HTTP/1.0\r\n\r\n")
	if resp, _ := readResp(t, br); resp.Header.Get("Connection") != "keep-alive" {
		t.Fatalf("HTTP/1.0 keep-alive: %v", resp.Header)
	}
	if resp, _ := readResp(t, br); resp.Proto != "HTTP/1.1" || resp.StatusCode != http.StatusCreated {
		t.Fatalf("HTTP/1.0 after keep-alive: %s %d", resp.Proto, resp.StatusCode)
	}
	expectClosed(t, br)

	// HTTP/1.1 Connection: close.
	br = dialRaw(t, s, "GET /hello HTTP/1.1\r\nHost: a\r\nConnection: close\r\n\r\n")
	if resp, _ := readResp(t, br); !resp.Close {
		t.Fatal("response should carry Connection: close")
	}
	expectClosed(t, br)

	// A Connection field of the Handler's that does not say close gives way to the close the request asks for.
	s = serve(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Connection", "keep-alive")
	}), Options{})
	br = dialRaw(t, s, "GET / HTTP/1.1\r\nHost: a\r\nConnection: close\r\n\r\n")
	if resp, _ := readResp(t, br); !resp.Close {
		t.Fatalf("response should carry Connection: close, got %v", resp.Header)
	}
	expectClosed(t, br)
}

func TestBadRequests(t *testing.T) {
	s := newTestServer(t, Options{MaxHeaderBytes: units.KB})
	tests := []struct {
		req    string
		status int
	}{
		{"BLAH\r\n\r\n", 400},
		{"GET / HTTP/1.1\r\n\r\n", 400},              // missing Host
		{"GET http://a/hello HTTP/1.1\r\n\r\n", 400}, // missing Host, which the target's authority does not stand in for
		{"GET / HTTP/1.1\r\nHost: a\r\nX: " + strings.Repeat("a", 2*units.KB) + "\r\n\r\n", 431},
		// Framed by fhttp, but the standard library reads a different body out of the message: an obs-fold line
		// continuing Content-Length, and an empty line made of a bare LF ending the headers early.
		{"POST /echo HTTP/1.1\r\nHost: a\r\nContent-Length: 1\r\n 0\r\n\r\nx", 400},
		{"POST /echo HTTP/1.1\r\nHost: a\r\nContent-Length: 3\n\nabc\r\n\r\nxyz", 400},
		{"POST /echo HTTP/1.1\r\nHost: a\r\nTransfer-Encoding: chunked\r\n x\r\n\r\n0\r\n\r\n", 400},
		// A space before the colon (RFC 9112 5.1), which http.ReadRequest lets through: what an intermediary reading it as
		// Content-Length or Transfer-Encoding takes for the body must not be answered as a request of its own.
		{"POST /echo HTTP/1.1\r\nHost: a\r\nContent-Length : 32\r\n\r\nGET /hello HTTP/1.1\r\nHost: a\r\n\r\n", 400},
		{"POST /echo HTTP/1.1\r\nHost: a\r\nTransfer-Encoding : chunked\r\n\r\n0\r\n\r\n", 400},
		// A header ending in an empty line made of a bare LF, answered rather than left waiting for CRLF CRLF.
		{"GET /hello HTTP/1.1\nHost: a\n\n", 400},
		{"GET /hello HTTP/1.1\r\nHost: a\n\r\n", 400},
		// A bare CR before the request line is no empty line (RFC 9112 2.2).
		{"\r\rGET /hello HTTP/1.1\r\nHost: a\r\n\r\n", 400},
		// Unless chunked is the final transfer coding the body's length is unknown (RFC 9112 6.3), where the net/http
		// server answers 501.
		{"POST / HTTP/1.1\r\nHost: a\r\nTransfer-Encoding: gzip\r\n\r\n", 501},
		{"POST / HTTP/1.1\r\nHost: a\r\nTransfer-Encoding: chunked, gzip\r\n\r\n", 501},
		// An invalid Host field, which the host of an absolute-form target stands in for, and a CONNECT to an empty or
		// invalid port (RFC 9110 9.3.6).
		{"GET http://a/hello HTTP/1.1\r\nHost: b c\r\n\r\n", 400},
		{"CONNECT a HTTP/1.1\r\nHost: a\r\n\r\n", 400},
		{"CONNECT a: HTTP/1.1\r\nHost: a:\r\n\r\n", 400},
		{"CONNECT a:65536 HTTP/1.1\r\nHost: a:65536\r\n\r\n", 400},
		// Not buffered by fhttp, so net/http answers them, the same as the net/http server would.
		{"POST / HTTP/1.1\r\nHost: a\r\nTransfer-Encoding: gzip, chunked\r\n\r\n", 501},
		{"POST / HTTP/1.1\r\nHost: a\r\nContent-Length: 1\r\nContent-Length: 2\r\n\r\n", 400},
		{"POST /echo HTTP/1.1\r\nHost: a\r\nExpect: foo\r\nContent-Length: 1\r\n\r\nx", 417},
		{"GET /hello HTTP/9.9\r\nHost: a\r\n\r\n", 505},
		{"PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n", 505},
		// A blank line made of a bare LF makes the standard library stop parsing early: what
		// follows must not be silently discarded, otherwise the request count differs from what
		// the front proxy saw.
		{"GET /hello HTTP/1.1\nHost: a\n\nGET /hello HTTP/1.1\r\nHost: a\r\n\r\n", 400},
		// The net/http server rejects an invalid Host.
		{"GET /hello HTTP/1.1\r\nHost: a b\r\n\r\n", 400},
		{"GET /hello HTTP/1.1\r\nHost: a\"b\r\n\r\n", 400},
		{"GET /hello HTTP/1.1\r\nHost: evil.com/path\r\n\r\n", 400},
		{"GET /hello HTTP/1.1\r\nHost: <script>\r\n\r\n", 400},
	}
	for _, tt := range tests {
		br := dialRaw(t, s, tt.req)
		if resp, _ := readResp(t, br); resp.StatusCode != tt.status {
			t.Fatalf("%q: got %d, want %d", tt.req, resp.StatusCode, tt.status)
		}
		expectClosed(t, br)
	}

	// The error response has to come after the response to the valid request that preceded it, also when it comes
	// from net/http.
	for _, tt := range []struct {
		req    string
		status int
	}{
		{"GET / HTTP/1.1\r\nHost: a\r\nX: " + strings.Repeat("a", 2*units.KB) + "\r\n\r\n", 431},
		{"POST / HTTP/1.1\r\nTransfer-Encoding: gzip, chunked\r\n\r\n", 501},
	} {
		br := dialRaw(t, s, "GET /hello HTTP/1.1\r\nHost: a\r\n\r\n"+tt.req)
		if resp, _ := readResp(t, br); resp.StatusCode != 200 {
			t.Fatalf("first response: %d", resp.StatusCode)
		}
		if resp, _ := readResp(t, br); resp.StatusCode != tt.status {
			t.Fatalf("second response: got %d, want %d", resp.StatusCode, tt.status)
		}
		expectClosed(t, br)
	}
}

// TestHeaderLimits checks the default limits on the request header: a request line of the 8000 bytes RFC 9112 3
// recommends supporting, with a cookie of the 4096 bytes RFC 6265 6.1 does, and maxHeaderValueCount field lines are
// served; a longer request line gets 414, a longer header or one with more field lines 431, with a Date as every 4xx
// response has (RFC 9110 6.6.1).
func TestHeaderLimits(t *testing.T) {
	s := newTestServer(t, Options{})
	fields := func(n int) string { return strings.Repeat("X: y\r\n", n) }
	for _, tt := range []struct {
		name   string
		req    string
		status int
	}{
		{"8000-byte request line", "GET /hello?" + strings.Repeat("a", 8000-len("GET /hello? HTTP/1.1")) + " HTTP/1.1\r\n" +
			"Host: a\r\nCookie: " + strings.Repeat("c", 4096) + "\r\n\r\n", 200},
		{"most field lines", "GET /hello HTTP/1.1\r\nHost: a\r\n" + fields(maxHeaderValueCount-1) + "\r\n", 200},
		{"too many field lines", "GET /hello HTTP/1.1\r\nHost: a\r\n" + fields(maxHeaderValueCount) + "\r\n", 431},
		{"request line too long", "GET /" + strings.Repeat("a", 16*units.KB) + " HTTP/1.1\r\nHost: a\r\n\r\n", 414},
		{"header too large", "GET /hello HTTP/1.1\r\nHost: a\r\nX: " + strings.Repeat("a", 16*units.KB) + "\r\n\r\n", 431},
	} {
		br := dialRaw(t, s, tt.req)
		if resp, _ := readResp(t, br); resp.StatusCode != tt.status || resp.Header.Get("Date") == "" {
			t.Fatalf("%s: got %d %v, want %d with a Date", tt.name, resp.StatusCode, resp.Header, tt.status)
		}
		if tt.status != http.StatusOK {
			expectClosed(t, br)
		}
	}
}

// TestResponseSemantics checks what RFC 9110 requires of a response whatever the Handler does: a 2xx to CONNECT has no
// content and closes the connection, as fhttp provides no tunnel (9.3.6, 8.6); a 205 has no content (15.3.6); and
// "OPTIONS *" is answered for the server as a whole, as the net/http server does (9.3.7).
func TestResponseSemantics(t *testing.T) {
	mux := testMux()
	mux.HandleFunc("/reset", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Length", "7")
		w.WriteHeader(http.StatusResetContent)
		io.WriteString(w, "content")
	})
	s := serve(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodConnect {
			w.Header().Set("Connection", "keep-alive")
			io.WriteString(w, "content")
			return
		}
		mux.ServeHTTP(w, r)
	}), Options{})

	out, err := io.ReadAll(dialRaw(t, s, "CONNECT a:443 HTTP/1.1\r\nHost: a:443\r\n\r\nGET /hello HTTP/1.1\r\nHost: a\r\n\r\n"))
	if err != nil || !strings.HasPrefix(string(out), "HTTP/1.1 200 OK\r\n") || !strings.HasSuffix(string(out), "\r\n\r\n") ||
		strings.Contains(string(out), "Content-Length") || !strings.Contains(string(out), "\r\nConnection: close\r\n") ||
		strings.Contains(string(out), "keep-alive") {
		t.Fatalf("CONNECT: %q %v", out, err)
	}

	// A HEAD gets the same Content-Length as a GET, whatever the Handler declared.
	br := dialRaw(t, s, "GET /reset HTTP/1.1\r\nHost: a\r\n\r\nHEAD /reset HTTP/1.1\r\nHost: a\r\n\r\nOPTIONS * HTTP/1.1\r\nHost: a\r\n\r\n")
	if resp, body := readResp(t, br); resp.StatusCode != http.StatusResetContent || resp.ContentLength != 0 || body != "" {
		t.Fatalf("205: got %d, Content-Length %d, %q", resp.StatusCode, resp.ContentLength, body)
	}
	if resp, err := http.ReadResponse(br, &http.Request{Method: http.MethodHead}); err != nil ||
		resp.StatusCode != http.StatusResetContent || resp.Header.Get("Content-Length") != "0" {
		t.Fatalf("205 to HEAD: %v, %v", resp, err)
	}
	if resp, body := readResp(t, br); resp.StatusCode != http.StatusOK || resp.ContentLength != 0 || resp.Close || body != "" {
		t.Fatalf("OPTIONS *: got %d, Content-Length %d, close=%v, %q", resp.StatusCode, resp.ContentLength, resp.Close, body)
	}
}

// TestValidHost checks that a Host may be a hostname, an IPv4 address or an IPv6 literal with a zone
// identifier, optionally with a port; any other character is rejected.
func TestValidHost(t *testing.T) {
	for _, host := range []string{
		"", "localhost", "example.com", "example.com:8080", "127.0.0.1:80", "[::1]", "[::1]:8080", "[fe80::1%25en0]:80",
		"a_b-c.d~e", "a+b", "a,b", "a;b", "a=b", "a'b", "a!b", "a$b", "a(b)", "a*b", "a&b",
	} {
		if !validHost(host) {
			t.Errorf("validHost(%q) = false, want true", host)
		}
	}
	for _, host := range []string{
		"a b", "a\tb", "a\"b", "a/b", "a\\b", "a<b>", "a@b", "a?b", "a#b", "a{b}", "a|b", "a^b", "a`b", "a\x00b", "a\x7fb", "aéb",
	} {
		if validHost(host) {
			t.Errorf("validHost(%q) = true, want false", host)
		}
	}
}

// TestH1Spec ports all 33 cases of h1spec (https://github.com/uNetworking/h1spec, commit f0a5650); the
// sub-test names keep h1spec's case descriptions so they are easy to compare against.
func TestH1Spec(t *testing.T) {
	s := serve(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		io.Copy(w, r.Body) // h1spec requires echoing the request body for any method and any path
	}), Options{})

	// Arriving in fragments: no prefix of a complete request should get a response. h1spec only
	// checks that there is no response within 500ms; here the request is also completed, to check
	// that it is then handled normally.
	const full = "GET /hello HTTP/1.1\r\nHost: localhost\r\n\r\n"
	for _, tt := range []struct{ name, prefix string }{
		{"Fragmented method", "G"},
		{"Fragmented URL 1", "GET "},
		{"Fragmented URL 2", "GET /hello"},
		{"Fragmented URL 3", "GET /hello "},
		{"Fragmented HTTP version", "GET /hello HTTP"},
		{"Fragmented request line", "GET /hello HTTP/1.1"},
		{"Fragmented request line newline 1", "GET /hello HTTP/1.1\r"},
		{"Fragmented request line newline 2", "GET /hello HTTP/1.1\r\n"},
		{"Fragmented field name", "GET /hello HTTP/1.1\r\nHos"},
		{"Fragmented field value 1", "GET /hello HTTP/1.1\r\nHost:"},
		{"Fragmented field value 2", "GET /hello HTTP/1.1\r\nHost: "},
		{"Fragmented field value 3", "GET /hello HTTP/1.1\r\nHost: localhost"},
		{"Fragmented field value 4", "GET /hello HTTP/1.1\r\nHost: localhost\r"},
		{"Fragmented request", "GET /hello HTTP/1.1\r\nHost: localhost\r\n"},
		{"Fragmented request termination", "GET /hello HTTP/1.1\r\nHost: localhost\r\n\r"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			c, err := net.Dial("tcp", s.Addr().String())
			if err != nil {
				t.Fatal(err)
			}
			defer c.Close()
			io.WriteString(c, tt.prefix)
			c.SetReadDeadline(time.Now().Add(100 * time.Millisecond))
			if n, err := c.Read(make([]byte, 1)); !errors.Is(err, os.ErrDeadlineExceeded) {
				t.Fatalf("no response should arrive while the request is incomplete: n=%d err=%v", n, err)
			}
			io.WriteString(c, full[len(tt.prefix):])
			c.SetReadDeadline(time.Now().Add(5 * time.Second))
			if resp, _ := readResp(t, bufio.NewReader(c)); resp.StatusCode != http.StatusOK {
				t.Fatalf("after completing the request: got %d, want 200", resp.StatusCode)
			}
		})
	}

	// Complete requests: the status code has to fall into one of the ranges; when the status code is
	// 200 and a body is given, that request body also has to be echoed (same as h1spec).
	for _, tt := range []struct {
		name   string
		req    string
		status [][2]int
		body   string
	}{
		{"Request without HTTP version", "GET / \r\n\r\n", [][2]int{{400, 599}}, ""},
		{"Request with Expect header", "GET / HTTP/1.1\r\nHost: example.com\r\nExpect: 100-continue\r\n\r\n", [][2]int{{100, 100}, {200, 299}}, ""},
		{"Valid GET request", "GET / HTTP/1.1\r\nHost: example.com\r\n\r\n", [][2]int{{200, 299}}, ""},
		{"Valid GET request with edge cases", "GET / HTTP/1.1\r\nhoSt:\texample.com\r\nempty:\r\n\r\n", [][2]int{{200, 299}}, ""},
		{"Empty Host header", "GET / HTTP/1.1\r\nHost:\r\n\r\n", [][2]int{{200, 299}}, ""},
		{"Invalid header characters", "GET / HTTP/1.1\r\nHost: example.com\r\nX-Invalid[]: test\r\n\r\n", [][2]int{{400, 499}}, ""},
		// fhttp handles a request once its body has arrived, so unlike h1spec the 5 bytes are sent as well.
		{"Missing Host header", "GET / HTTP/1.1\r\nContent-Length: 5\r\n\r\nhello", [][2]int{{400, 499}}, ""},
		{"Multiple Host headers", "GET / HTTP/1.1\r\nHost: example.com\r\nHost: example.org\r\n\r\n", [][2]int{{400, 499}}, ""},
		{"Overflowing negative Content-Length header", "GET / HTTP/1.1\r\nHost: example.com\r\nContent-Length: -123456789123456789123456789\r\n\r\n", [][2]int{{400, 499}}, ""},
		{"Negative Content-Length header", "GET / HTTP/1.1\r\nHost: example.com\r\nContent-Length: -1234\r\n\r\n", [][2]int{{400, 499}}, ""},
		{"Non-numeric Content-Length header", "GET / HTTP/1.1\r\nHost: example.com\r\nContent-Length: abc\r\n\r\n", [][2]int{{400, 499}}, ""},
		{"Empty header value", "GET / HTTP/1.1\r\nHost: example.com\r\nX-Empty-Header: \r\n\r\n", [][2]int{{200, 299}}, ""},
		{"Header containing invalid control character", "GET / HTTP/1.1\r\nHost: example.com\r\nX-Bad-Control-Char: test\x07\r\n\r\n", [][2]int{{400, 499}}, ""},
		{"Invalid HTTP version", "GET / HTTP/9.9\r\nHost: example.com\r\n\r\n", [][2]int{{400, 499}, {500, 599}}, ""},
		{"Invalid prefix of request", "Extra lineGET / HTTP/1.1\r\nHost: example.com\r\n\r\n", [][2]int{{400, 499}, {500, 599}}, ""},
		{"Invalid line ending", "GET / HTTP/1.1\r\nHost: example.com\r\n\rSome-Header: Test\r\n\r\n", [][2]int{{400, 499}}, ""},
		{"Valid POST request with body", "POST / HTTP/1.1\r\nHost: example.com\r\nContent-Length: 5\r\n\r\nhello", [][2]int{{200, 299}, {404, 404}}, "hello"},
		{"Chunked Transfer-Encoding", "POST / HTTP/1.1\r\nHost: example.com\r\nTransfer-Encoding: chunked\r\n\r\nc\r\nHellO world1\r\n0\r\n\r\n", [][2]int{{200, 299}}, "HellO world1"},
		{"Conflicting Transfer-Encoding and Content-Length in varying case", "POST / HTTP/1.1\r\nHost: example.com\r\ncontent-LengtH: 5\r\nTransFer-Encoding: chunked\r\n\r\nc\r\nHellO world1\r\n0\r\n\r\n", [][2]int{{400, 499}, {200, 299}}, "HellO world1"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			resp, body := readResp(t, dialRaw(t, s, tt.req))
			if !slices.ContainsFunc(tt.status, func(r [2]int) bool { return r[0] <= resp.StatusCode && resp.StatusCode <= r[1] }) {
				t.Fatalf("got %d, want %v", resp.StatusCode, tt.status)
			}
			if resp.StatusCode == http.StatusOK && tt.body != "" && body != tt.body {
				t.Fatalf("body: got %q, want %q", body, tt.body)
			}
		})
	}
}

func TestConcurrentClients(t *testing.T) {
	s := newTestServer(t, Options{})
	base := "http://" + s.Addr().String()
	var wg sync.WaitGroup
	for range 100 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			client := &http.Client{Transport: &http.Transport{}}
			defer client.CloseIdleConnections()
			for range 20 {
				resp, err := client.Get(base + "/hello")
				if err != nil {
					t.Error(err)
					return
				}
				body, _ := io.ReadAll(resp.Body)
				resp.Body.Close()
				if string(body) != "hello world" {
					t.Errorf("got %q", body)
					return
				}
			}
		}()
	}
	wg.Wait()
}

// TestTimeouts covers the timeouts: they are checked with a precision of about 1 second and the scenarios
// run in parallel; when a timeout closes the connection the client may read either EOF or RST, so all that
// is required is that the connection is dropped and there is no response.
func TestTimeouts(t *testing.T) {
	mux := testMux()
	mux.HandleFunc("/sleep", func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(1500 * time.Millisecond) // exceeds both timeouts: Handler run time is not limited
		io.WriteString(w, "done")
	})
	s := serve(t, mux, Options{ReadHeaderTimeout: 200 * time.Millisecond, IdleTimeout: 300 * time.Millisecond})
	unlimited := serve(t, mux, Options{ReadHeaderTimeout: -1, IdleTimeout: -1})

	expectDropped := func(t *testing.T, br *bufio.Reader) {
		t.Helper()
		if b, err := br.ReadByte(); err == nil || errors.Is(err, os.ErrDeadlineExceeded) {
			t.Fatalf("want the server to close the connection, got %q %v", b, err)
		}
	}
	t.Run("idle new connection", func(t *testing.T) {
		t.Parallel()
		expectDropped(t, dialRaw(t, s, ""))
	})
	t.Run("idle keep-alive", func(t *testing.T) {
		t.Parallel()
		br := dialRaw(t, s, "GET /hello HTTP/1.1\r\nHost: a\r\n\r\n")
		readResp(t, br)
		expectDropped(t, br)
	})
	t.Run("slow request header", func(t *testing.T) {
		t.Parallel()
		c, err := net.Dial("tcp", s.Addr().String())
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { c.Close() })
		c.SetDeadline(time.Now().Add(5 * time.Second))
		go func() { // sending data continuously does not extend the deadline either
			io.WriteString(c, "GET /hello HTTP/1.1\r\nHost: a\r\nX: ")
			for {
				time.Sleep(50 * time.Millisecond)
				if _, err := io.WriteString(c, "a"); err != nil {
					return
				}
			}
		}()
		expectDropped(t, bufio.NewReader(c))
	})
	t.Run("slow request body", func(t *testing.T) { // a buffered body is part of the request, under ReadHeaderTimeout
		t.Parallel()
		expectDropped(t, dialRaw(t, s, "POST /echo HTTP/1.1\r\nHost: a\r\nContent-Length: 5\r\n\r\nhel"))
	})
	t.Run("incomplete request header received while handling", func(t *testing.T) {
		t.Parallel()
		br := dialRaw(t, s, "GET /sleep HTTP/1.1\r\nHost: a\r\n\r\nGET /hel")
		if _, body := readResp(t, br); body != "done" {
			t.Fatalf("got %q", body)
		}
		expectDropped(t, br)
	})
	t.Run("unlimited", func(t *testing.T) {
		t.Parallel()
		c, err := net.Dial("tcp", unlimited.Addr().String())
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { c.Close() })
		c.SetDeadline(time.Now().Add(5 * time.Second))
		io.WriteString(c, "GET /hel")
		time.Sleep(1500 * time.Millisecond) // header and idle both exceed one check interval
		io.WriteString(c, "lo HTTP/1.1\r\nHost: a\r\n\r\n")
		if _, body := readResp(t, bufio.NewReader(c)); body != "hello world" {
			t.Fatalf("got %q", body)
		}
	})
}

// TestHTTP1Config checks that a config GetConfigForClient returns, which crypto/tls uses in place of the server's, does
// not negotiate h2 either.
func TestHTTP1Config(t *testing.T) {
	perClient := tlsServerConfig(t)
	perClient.NextProtos = []string{"h2", "http/1.1"}
	config := &tls.Config{GetConfigForClient: func(*tls.ClientHelloInfo) (*tls.Config, error) { return perClient, nil }}
	s := serve(t, testMux(), Options{TLSConfig: config})
	c, err := tls.Dial("tcp", s.Addr().String(), &tls.Config{InsecureSkipVerify: true, NextProtos: []string{"h2", "http/1.1"}})
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	if p := c.ConnectionState().NegotiatedProtocol; p != "http/1.1" {
		t.Fatalf("negotiated %q, want http/1.1", p)
	}
}

// TestHTTPS serves over TLS, on fnet and, for a streamed body, on net/http after the handoff. The server config offers
// h2, which fhttp must not negotiate.
func TestHTTPS(t *testing.T) {
	mux := testMux()
	mux.HandleFunc("/tls", func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		if r.TLS == nil {
			http.Error(w, "no TLS state", http.StatusInternalServerError)
			return
		}
		fmt.Fprintf(w, "%s %d", r.TLS.NegotiatedProtocol, len(body))
	})
	config := tlsServerConfig(t)
	config.NextProtos = []string{"h2", "http/1.1"}
	s := serve(t, mux, Options{TLSConfig: config, MaxBufferedBodyBytes: 8})
	if !slices.Equal(config.NextProtos, []string{"h2", "http/1.1"}) {
		t.Fatalf("NewServer modified the caller's config: %q", config.NextProtos)
	}
	base := "https://" + s.Addr().String()
	client := &http.Client{Transport: &http.Transport{TLSClientConfig: &tls.Config{InsecureSkipVerify: true}, ForceAttemptHTTP2: true}}
	defer client.CloseIdleConnections()

	resp, body := get(t, client, base+"/tls")
	if resp.ProtoMajor != 1 || body != "http/1.1 0" {
		t.Fatalf("GET /tls: %s %q", resp.Proto, body)
	}
	_, addr1 := get(t, client, base+"/remote")
	_, addr2 := get(t, client, base+"/remote")
	if addr1 != addr2 {
		t.Fatalf("connection not kept alive: %s, %s", addr1, addr2)
	}
	if _, body := get(t, client, base+"/big"); len(body) != 4*units.MB { // streamed through the TLS connection
		t.Fatalf("GET /big: %d bytes", len(body))
	}

	// A body over MaxBufferedBodyBytes is not buffered: net/http serves it on the connection detached from fnet.
	resp, err := client.Post(base+"/tls", "text/plain", strings.NewReader("hello world"))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if b, _ := io.ReadAll(resp.Body); string(b) != "http/1.1 11" || !resp.Close {
		t.Fatalf("POST /tls: %d %q close=%v", resp.StatusCode, b, resp.Close)
	}

	// Far more pipelined requests than are queued at once (see maxPipeline), spanning several TLS records, are all
	// answered.
	tc, err := tls.Dial("tcp", s.Addr().String(), &tls.Config{InsecureSkipVerify: true})
	if err != nil {
		t.Fatal(err)
	}
	defer tc.Close()
	tc.SetDeadline(time.Now().Add(5 * time.Second))
	const count = 8 * maxPipeline
	io.WriteString(tc, strings.Repeat("GET /hello HTTP/1.1\r\nHost: a\r\n\r\n", count))
	br := bufio.NewReader(tc)
	for i := range count {
		if resp, body := readResp(t, br); resp.StatusCode != http.StatusOK || body != "hello world" {
			t.Fatalf("pipelined response %d: %d %q", i, resp.StatusCode, body)
		}
	}
}

// TestDate checks that the Date field line is that of the current second.
func TestDate(t *testing.T) {
	var b bytepool.Buffer
	appendDate(&b)
	line := string(b.Bytes())
	b.Release()
	date, err := http.ParseTime(strings.TrimSuffix(strings.TrimPrefix(line, "Date: "), "\r\n"))
	if err != nil || !strings.HasSuffix(line, "\r\n") || time.Since(date).Abs() > 2*time.Second {
		t.Fatalf("Date field %q: %v", line, err)
	}
}

// TestAppendHeader checks that the fields come out the way http.Header.Write writes them: sorted, without names that
// are not tokens, and with no CR or LF that could inject a field.
func TestAppendHeader(t *testing.T) {
	h := http.Header{
		"X-B": {"2", " two \t"}, "X-A": {"a\r\nX-Injected: 1", "\r\nb\n"}, "Bad Name": {"v"}, "X-Empty": {""},
		"Content-Length": {"5"}, "Set-Cookie": {"a=1", "b=2"}, "X~`|^": {"tok"}, "Bad\"Quote": {"v"},
	}
	var want strings.Builder
	h.Write(&want)
	var got bytepool.Buffer
	appendHeader(&got, h)
	if string(got.Bytes()) != want.String() {
		t.Errorf("got %q, want %q", got.Bytes(), want.String())
	}
	got.Release()
}

// parseCases are requests parseRequest has to read field for field like http.ReadRequest (fast), and requests it has
// to leave to http.ReadRequest.
var parseCases = struct{ fast, slow []string }{
	fast: []string{
		"GET / HTTP/1.1\r\nHost: a\r\n\r\n",
		"GET /baseline11?a=13&b=42 HTTP/1.1\r\nHost: localhost:8080\r\n\r\n",
		"GET /a/b;c,d=e@f:g$h&i+j~k_l.m-n//o HTTP/1.0\r\n\r\n",
		"GET /q? HTTP/1.1\r\nHost: a\r\n\r\n",
		"GET /q??x?y HTTP/1.1\r\nHost: a\r\n\r\n",
		"GET /q?a=%41#f\"<> HTTP/1.1\r\nHost: a\r\n\r\n",
		"HEAD /x HTTP/1.0\r\nConnection: Keep-Alive\r\n\r\n",
		"OPTIONS /x HTTP/1.1\r\nhost:\tb:80 \r\nCONNECTION: foo, close\r\n\r\n",
		"DELETE /x HTTP/1.1\r\nHost: a\r\nempty:\r\nX-A: 1\r\nx-a: 2\r\nX-B:  sp  ace  \r\nx-lower-case: v\r\n" +
			"Accept: text/html,application/xhtml+xml\r\nUser-Agent: Mozilla/5.0 (X11)\r\nCookie: a=b; c=d\r\n" +
			"X-Many: 1\r\nX-Many: 2\r\nX-Many: 3\r\n\r\n",
		"POST /echo HTTP/1.1\r\nHost: a\r\nContent-Length: 5\r\n\r\nhello",
		"PUT /echo HTTP/1.1\r\nHost: a\r\ncontent-length: 0\r\n\r\n",
		"PATCH /echo HTTP/1.1\r\nHost: a\r\nExpect: 100-continue\r\nContent-Length: 2\r\n\r\nhi",
		"POST /echo HTTP/1.1\r\nHost: a\r\nTransfer-Encoding: chunked\r\n\r\n5\r\nhello\r\nB\r\n world 0123\r\n0\r\n\r\n",
		"POST /echo HTTP/1.1\r\nHost: a\r\ntransfer-encoding: Chunked\r\n\r\n0\r\n\r\n",
		"GET /ws HTTP/1.1\r\nHost: 127.0.0.1:18080\r\nUpgrade: websocket\r\nConnection: Upgrade\r\n" +
			"Sec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\nSec-WebSocket-Version: 13\r\n\r\n",
	},
	slow: []string{
		"GET /%41 HTTP/1.1\r\nHost: a\r\n\r\n",
		"GET /a!b HTTP/1.1\r\nHost: a\r\n\r\n",
		"GET http://a/b HTTP/1.1\r\nHost: a\r\n\r\n",
		"OPTIONS * HTTP/1.1\r\nHost: a\r\n\r\n",
		"CONNECT a:443 HTTP/1.1\r\nHost: a\r\n\r\n",
		"BREW /pot HTTP/1.1\r\nHost: a\r\n\r\n",
		"GET / HTTP/1.2\r\nHost: a\r\n\r\n",
		"GET / HTTP/1.1\r\nHost: a\r\nHost: b\r\n\r\n",
		"GET / HTTP/1.1\r\nHost: a\r\nPragma: no-cache\r\n\r\n",
		"GET / HTTP/1.1\r\nHost: a\r\nX-Bad: \x80\r\n\r\n",
		"GET / HTTP/1.1\r\nHost: a\r\nX : v\r\n\r\n",
		"GET / HTTP/1.1\r\nHost: a\r\nX-Fold: a\r\n b\r\n\r\n",
		"GET / HTTP/1.1\r\nHost: a\r\nX: a\rb\r\n\r\n",
		"GET / HTTP/1.1\nHost: a\r\n\r\n",
		"POST / HTTP/1.1\r\nHost: a\r\nTrailer: X\r\nTransfer-Encoding: chunked\r\n\r\n0\r\n\r\n",
	},
}

// readMessage frames raw like a connection would and returns the message.
func readMessage(t testing.TB, raw string) ([]byte, message) {
	var f framer
	opts := Options{}.withDefaults()
	m, _, err := f.next([]byte(raw), &opts)
	if err != nil || m.size != len(raw) {
		t.Fatalf("%q: framed %+v, %v", raw, m, err)
	}
	return []byte(raw), m
}

// sameRequest reports how got differs from want, the request http.ReadRequest read; "" when it does not.
func sameRequest(got, want *http.Request) string {
	gotBody, _ := io.ReadAll(got.Body)
	wantBody, _ := io.ReadAll(want.Body)
	switch {
	case got.Method != want.Method || got.RequestURI != want.RequestURI || got.Host != want.Host:
		return fmt.Sprintf("request line or Host: %q %q %q", got.Method, got.RequestURI, got.Host)
	case got.Proto != want.Proto || got.ProtoMajor != want.ProtoMajor || got.ProtoMinor != want.ProtoMinor:
		return fmt.Sprintf("protocol: %q", got.Proto)
	case !reflect.DeepEqual(*got.URL, *want.URL):
		return fmt.Sprintf("URL: %#v, want %#v", *got.URL, *want.URL)
	case !reflect.DeepEqual(got.Header, want.Header):
		return fmt.Sprintf("Header: %q, want %q", got.Header, want.Header)
	case got.Close != want.Close || got.ContentLength != want.ContentLength:
		return fmt.Sprintf("Close=%v ContentLength=%d", got.Close, got.ContentLength)
	case !slices.Equal(got.TransferEncoding, want.TransferEncoding) || got.Trailer != nil:
		return fmt.Sprintf("TransferEncoding=%q Trailer=%q", got.TransferEncoding, got.Trailer)
	case (got.Body == http.NoBody) != (want.Body == http.NoBody) || !bytes.Equal(gotBody, wantBody):
		return fmt.Sprintf("body %q, want %q", gotBody, wantBody)
	}
	return ""
}

// TestParseRequest compares parseRequest with http.ReadRequest; the parsed requests are released after use, so that
// later cases reuse them, as fhttp does once the Handler has returned.
func TestParseRequest(t *testing.T) {
	var last lastRequest // shared by all the requests, as by those of one connection
	for _, raw := range parseCases.fast {
		// The second time the request shares the target and the fields copied the first time.
		for round := range 2 {
			msg, m := readMessage(t, raw)
			before := struct{ target, fields string }{last.target, last.fields}
			got := parseRequest(msg, m, &last)
			want, err := http.ReadRequest(bufio.NewReader(strings.NewReader(raw)))
			if got == nil || err != nil {
				t.Fatalf("%q: parseRequest %v, http.ReadRequest %v", raw, got, err)
			}
			if diff := sameRequest(&got.request, want); diff != "" {
				t.Errorf("%q round %d: %s", raw, round, diff)
			}
			if round == 1 && unsafe.StringData(last.target) != unsafe.StringData(before.target) {
				t.Errorf("%q: the same target was copied again", raw)
			}
			if round == 1 && unsafe.StringData(last.fields) != unsafe.StringData(before.fields) {
				t.Errorf("%q: the same fields were copied again", raw)
			}
			got.release()
		}
	}
	for _, raw := range parseCases.slow {
		if msg, m := readMessage(t, raw); parseRequest(msg, m, new(lastRequest)) != nil {
			t.Errorf("%q: parseRequest should leave it to http.ReadRequest", raw)
		}
	}
}

// FuzzParseRequest checks that whatever parseRequest reads, http.ReadRequest reads the same way.
func FuzzParseRequest(f *testing.F) {
	for _, raw := range slices.Concat(parseCases.fast, parseCases.slow) {
		f.Add(raw)
	}
	opts := Options{}.withDefaults()
	f.Fuzz(func(t *testing.T, raw string) {
		var fr framer
		m, _, err := fr.next([]byte(raw), &opts)
		if err != nil || m.size == 0 {
			return
		}
		raw = raw[:m.size]
		got := parseRequest([]byte(raw), m, new(lastRequest))
		if got == nil {
			return
		}
		want, err := http.ReadRequest(bufio.NewReader(strings.NewReader(raw)))
		if err != nil {
			t.Fatalf("%q: parseRequest read it, http.ReadRequest: %v", raw, err)
		}
		if diff := sameRequest(&got.request, want); diff != "" {
			t.Fatalf("%q: %s", raw, diff)
		}
		got.release()
	})
}

// BenchmarkParseRequest measures the requests of one connection, whose client sends the same header fields every
// time: each is parsed, then released for reuse as fhttp does once the Handler has returned.
func BenchmarkParseRequest(b *testing.B) {
	for _, raw := range []string{
		"GET /baseline11?a=13&b=42 HTTP/1.1\r\nHost: localhost:8080\r\n\r\n",
		"GET /ws HTTP/1.1\r\nHost: 127.0.0.1:18080\r\nUpgrade: websocket\r\nConnection: Upgrade\r\n" +
			"Sec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\nSec-WebSocket-Version: 13\r\n\r\n",
	} {
		msg, m := readMessage(b, raw)
		b.Run(strings.Fields(raw)[1], func(b *testing.B) {
			b.ReportAllocs()
			var last lastRequest
			for range b.N {
				p := parseRequest(msg, m, &last)
				if p == nil {
					b.Fatal("nil")
				}
				p.release()
			}
		})
	}
}
