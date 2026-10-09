package fhttp

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"io"
	"log"
	"log/slog"
	"net"
	"net/http"
	"os"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"
	"unsafe"

	"github.com/linfeip/fnet"
	"github.com/linfeip/fnet/internal/units"
)

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
			t.Errorf("Serve 返回 %v", err)
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
		t.Fatalf("连接未复用: %s vs %s", addr1, addr2)
	}

	// A small body with Content-Length is buffered and served on fnet, keeping the connection alive; a chunked one
	// (hiding the concrete type keeps the client from knowing the length) goes to net/http, which closes it.
	for _, tt := range []struct {
		body  io.Reader
		close bool
	}{
		{strings.NewReader("ping"), false},
		{struct{ io.Reader }{strings.NewReader("ping")}, true},
	} {
		resp, err := client.Post(base+"/echo", "text/plain", tt.body)
		if err != nil {
			t.Fatal(err)
		}
		body, err := io.ReadAll(resp.Body)
		resp.Body.Close()
		if err != nil || resp.StatusCode != 200 || string(body) != "ping" || resp.Close != tt.close {
			t.Fatalf("带请求体的 POST: %d %q close=%v err=%v", resp.StatusCode, body, resp.Close, err)
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
		t.Fatalf("GET /big: %d 字节", len(body))
	}

	// Silence the log from the Handler panic. slog.SetDefault also rewrites the output and flags of the
	// log package, so both have to be restored at the end; otherwise every later log written through the
	// default slog (which goes through the log package underneath) would be discarded.
	defer func(w io.Writer, flags int) { log.SetOutput(w); log.SetFlags(flags) }(log.Writer(), log.Flags())
	defer slog.SetDefault(slog.Default())
	slog.SetDefault(slog.New(slog.NewTextHandler(io.Discard, nil)))
	if _, err := client.Get(base + "/panic"); err == nil {
		t.Fatal("Handler panic 后连接应被关闭")
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
		t.Fatalf("期望连接被关闭, got %q %v", b, err)
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

// discardConn is a fnet.Conn that discards whatever is written to it; it only implements the methods used
// while handling a request.
type discardConn struct{ fnet.Conn }

func (discardConn) Write(b []byte) (int, error) { return len(b), nil }
func (discardConn) Writev(bs [][]byte) (int, error) {
	n := 0
	for _, b := range bs {
		n += len(b)
	}
	return n, nil
}
func (discardConn) SetDeadline(time.Time) {}
func (discardConn) RemoteAddr() net.Addr  { return &net.TCPAddr{} }

// TestConnSize checks that conn and the request queue item stay within their size classes: every connection holds a
// conn, and its queue keeps a backing array (see maxRetainedQueueCapacity), so a few bytes more cost every idle
// connection a whole size class.
func TestConnSize(t *testing.T) {
	if unsafe.Sizeof(uintptr(0)) != 8 {
		t.Skip("尺寸按 64 位平台检查")
	}
	if size := unsafe.Sizeof(conn{}); size > 112 {
		t.Fatalf("conn 为 %dB，超出 112B 的内存分级", size)
	}
	if size := unsafe.Sizeof(request{}); size > 32 {
		t.Fatalf("request 为 %dB，超出 32B", size)
	}
}

// TestQueueCapacity verifies that once the queue has been drained the large capacity left behind by a
// pipelining burst is released, while the capacity for a small number of requests is retained so that
// each request does not have to allocate again.
func TestQueueCapacity(t *testing.T) {
	req := "GET /hello HTTP/1.1\r\nHost: a\r\n\r\n"
	s := &Server{handler: testMux(), opts: Options{}.withDefaults()}
	c := &conn{srv: s, connection: discardConn{}, busy: true} // busy: push starts no worker; the test calls serve
	for range 64 {
		c.push([]byte(req), 0)
	}
	c.serve()
	if cap(c.queue) > maxRetainedQueueCapacity {
		t.Fatalf("突发后队列容量 %d, 期望不超过 %d", cap(c.queue), maxRetainedQueueCapacity)
	}

	c.busy = true
	c.push([]byte(req), 0)
	c.serve()
	if cap(c.queue) != 1 {
		t.Fatalf("单个请求后队列容量 %d, 期望保留 1", cap(c.queue))
	}
}

func TestConnectionManagement(t *testing.T) {
	s := newTestServer(t, Options{})

	// HTTP/1.0 closes the connection by default.
	br := dialRaw(t, s, "GET /hello HTTP/1.0\r\n\r\n")
	if resp, body := readResp(t, br); resp.Proto != "HTTP/1.0" || body != "hello world" {
		t.Fatalf("HTTP/1.0: %s %q", resp.Proto, body)
	}
	expectClosed(t, br)

	// HTTP/1.0 with an explicit keep-alive.
	br = dialRaw(t, s, "GET /hello HTTP/1.0\r\nConnection: keep-alive\r\n\r\nGET /hello HTTP/1.0\r\n\r\n")
	if resp, _ := readResp(t, br); resp.Header.Get("Connection") != "keep-alive" {
		t.Fatalf("HTTP/1.0 keep-alive: %v", resp.Header)
	}
	readResp(t, br)
	expectClosed(t, br)

	// HTTP/1.1 Connection: close.
	br = dialRaw(t, s, "GET /hello HTTP/1.1\r\nHost: a\r\nConnection: close\r\n\r\n")
	if resp, _ := readResp(t, br); !resp.Close {
		t.Fatal("响应应带 Connection: close")
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
		{"GET / HTTP/1.1\r\n\r\n", 400}, // missing Host
		{"GET / HTTP/1.1\r\nHost: a\r\nX: " + strings.Repeat("a", 2*units.KB) + "\r\n\r\n", 431},
		// Framed by fhttp, but the standard library reads a different body out of the message: an obs-fold line
		// continuing Content-Length, and an empty line made of a bare LF ending the headers early.
		{"POST /echo HTTP/1.1\r\nHost: a\r\nContent-Length: 1\r\n 0\r\n\r\nx", 400},
		{"POST /echo HTTP/1.1\r\nHost: a\r\nContent-Length: 3\n\nabc\r\n\r\nxyz", 400},
		// Not buffered by fhttp, so net/http answers them, the same as the net/http server would.
		{"POST / HTTP/1.1\r\nHost: a\r\nTransfer-Encoding: gzip\r\n\r\n", 501},
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
		{"POST / HTTP/1.1\r\nTransfer-Encoding: gzip\r\n\r\n", 501},
	} {
		br := dialRaw(t, s, "GET /hello HTTP/1.1\r\nHost: a\r\n\r\n"+tt.req)
		if resp, _ := readResp(t, br); resp.StatusCode != 200 {
			t.Fatalf("第一个响应: %d", resp.StatusCode)
		}
		if resp, _ := readResp(t, br); resp.StatusCode != tt.status {
			t.Fatalf("第二个响应: got %d, want %d", resp.StatusCode, tt.status)
		}
		expectClosed(t, br)
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
			t.Errorf("validHost(%q) = false, 期望 true", host)
		}
	}
	for _, host := range []string{
		"a b", "a\tb", "a\"b", "a/b", "a\\b", "a<b>", "a@b", "a?b", "a#b", "a{b}", "a|b", "a^b", "a`b", "a\x00b", "a\x7fb", "aéb",
	} {
		if validHost(host) {
			t.Errorf("validHost(%q) = true, 期望 false", host)
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
				t.Fatalf("请求不完整时不应有响应: n=%d err=%v", n, err)
			}
			io.WriteString(c, full[len(tt.prefix):])
			c.SetReadDeadline(time.Now().Add(5 * time.Second))
			if resp, _ := readResp(t, bufio.NewReader(c)); resp.StatusCode != http.StatusOK {
				t.Fatalf("补齐请求后: got %d, want 200", resp.StatusCode)
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
			t.Fatalf("期望连接被服务端关闭, got %q %v", b, err)
		}
	}
	t.Run("新连接空闲", func(t *testing.T) {
		t.Parallel()
		expectDropped(t, dialRaw(t, s, ""))
	})
	t.Run("keep-alive 空闲", func(t *testing.T) {
		t.Parallel()
		br := dialRaw(t, s, "GET /hello HTTP/1.1\r\nHost: a\r\n\r\n")
		readResp(t, br)
		expectDropped(t, br)
	})
	t.Run("慢速发送请求头", func(t *testing.T) {
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
	t.Run("慢速发送请求体", func(t *testing.T) { // a buffered body is part of the request, under ReadHeaderTimeout
		t.Parallel()
		expectDropped(t, dialRaw(t, s, "POST /echo HTTP/1.1\r\nHost: a\r\nContent-Length: 5\r\n\r\nhel"))
	})
	t.Run("处理期间收到的不完整请求头", func(t *testing.T) {
		t.Parallel()
		br := dialRaw(t, s, "GET /sleep HTTP/1.1\r\nHost: a\r\n\r\nGET /hel")
		if _, body := readResp(t, br); body != "done" {
			t.Fatalf("got %q", body)
		}
		expectDropped(t, br)
	})
	t.Run("不限制", func(t *testing.T) {
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

func TestParseFastUpgradeRequest(t *testing.T) {
	msg := []byte("GET /ws HTTP/1.1\r\nHost: 127.0.0.1:18080\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\nSec-WebSocket-Version: 13\r\n\r\n")
	fast := parseFastUpgradeRequest(msg)
	if fast == nil {
		t.Fatal("expected non-nil fastUpgradeRequest")
	}
	defer releaseFastUpgradeRequest(fast)

	req := &fast.request
	if req.Method != "GET" || req.Proto != "HTTP/1.1" || req.ProtoMajor != 1 || req.ProtoMinor != 1 {
		t.Fatalf("unexpected request line: %+v", req)
	}
	if req.Host != "127.0.0.1:18080" || req.URL.Path != "/ws" {
		t.Fatalf("unexpected host or path: host=%q path=%q", req.Host, req.URL.Path)
	}
	if req.Header.Get("Upgrade") != "websocket" || req.Header.Get("Connection") != "Upgrade" ||
		req.Header.Get("Sec-WebSocket-Version") != "13" || req.Header.Get("Sec-WebSocket-Key") != "dGhlIHNhbXBsZSBub25jZQ==" {
		t.Fatalf("unexpected headers: %+v", req.Header)
	}

	// Missing Host
	if fast2 := parseFastUpgradeRequest([]byte("GET /ws HTTP/1.1\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\nSec-WebSocket-Version: 13\r\n\r\n")); fast2 != nil {
		t.Fatal("expected nil for missing Host")
	}

	// Plain GET request
	if fast3 := parseFastUpgradeRequest([]byte("GET /hello HTTP/1.1\r\nHost: a\r\n\r\n")); fast3 != nil {
		t.Fatal("expected nil for plain GET")
	}
}

func BenchmarkParseFastUpgradeRequest(b *testing.B) {
	msg := []byte("GET /ws HTTP/1.1\r\nHost: 127.0.0.1:18080\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\nSec-WebSocket-Version: 13\r\n\r\n")
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		fast := parseFastUpgradeRequest(msg)
		if fast == nil {
			b.Fatal("nil")
		}
		releaseFastUpgradeRequest(fast)
	}
}
