package fhttp

import (
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"regexp"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/linfeip/fnet/internal/reactor"
)

// Conformance with RFC 9112 (HTTP/1.1 messaging) and the parts of RFC 9110
// (HTTP semantics) a server's framing layer owns, driven with raw bytes: an
// HTTP client would normalise exactly what is under test. Test names carry the
// section. Where the RFC leaves a choice, the comment says which one fhttp
// makes (net/http's, unless noted).
//
// Every rejection the RFC pairs with closing the connection is checked with a
// request pipelined behind the bad one: it must never reach the handler, or
// the server could be made to answer a request a front end never saw
// (request smuggling, RFC 9112 11.2).

// wireExchange writes raw on a new connection and returns what the server
// sends back, and whether it closed the connection within wait.
func wireExchange(t *testing.T, addr, raw string, wait time.Duration) (out string, closed bool) {
	t.Helper()
	c, err := net.DialTimeout("tcp", addr, testDialTimeout)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer c.Close()
	_ = c.SetDeadline(time.Now().Add(wait))
	if _, err := io.WriteString(c, raw); err != nil {
		t.Fatalf("write: %v", err)
	}
	b, err := io.ReadAll(c)
	return string(b), err == nil || !isTimeout(err)
}

// statusOf returns the status code of the first response in out, or 0.
func statusOf(out string) int {
	var code int
	if _, err := fmt.Sscanf(out, "HTTP/1.1 %d", &code); err != nil {
		return 0
	}
	return code
}

// smuggleProbe serves /smuggled by counting it: a request that must never be
// answered. Every other path echoes the request as the handler saw it.
type smuggleProbe struct{ smuggled atomic.Int32 }

func (p *smuggleProbe) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path == "/smuggled" {
		p.smuggled.Add(1)
	}
	body, err := io.ReadAll(r.Body)
	fmt.Fprintf(w, "%s %s %s host=%q body=%q err=%v", r.Method, r.RequestURI, r.Proto, r.Host, body, err)
}

const smuggled = "GET /smuggled HTTP/1.1\r\nHost: a\r\nConnection: close\r\n\r\n"

// ---------------------------------------------------------------------------
// 2.2 Message parsing, 2.3 HTTP version, 3 Request line
// ---------------------------------------------------------------------------

func TestRFC9112RequestLine(t *testing.T) {
	p := &smuggleProbe{}
	addr := startHTTP(t, p)
	long := "/" + strings.Repeat("a", 7985) // an 8000-octet request line (3 RECOMMENDED minimum)

	cases := []struct {
		name, line string
		want       int
		wantBody   string
	}{
		{"origin-form", "GET /p?q=1 HTTP/1.1", 200, `GET /p?q=1 HTTP/1.1 host="a"`},
		{"absolute-form-3.2.2", "GET http://real.example/p HTTP/1.1", 200, `GET http://real.example/p HTTP/1.1 host="real.example"`},
		{"asterisk-form-3.2.4", "OPTIONS * HTTP/1.1", 200, `OPTIONS * HTTP/1.1`},
		{"8000-octet-line-3", "GET " + long + " HTTP/1.1", 200, "GET " + long},
		{"higher-minor-version-9110-6.2", "GET / HTTP/1.9", 200, "GET / HTTP/1.9"},
		{"lowercase-version-2.3", "GET / http/1.1", 400, ""},
		{"two-digit-minor-2.3", "GET / HTTP/1.10", 400, ""},
		{"double-space-3", "GET  / HTTP/1.1", 400, ""},
		{"tab-separators-3", "GET\t/\tHTTP/1.1", 400, ""},
		{"whitespace-in-target-3.2", "GET /a b HTTP/1.1", 400, ""},
		{"relative-target-3.2.1", "GET a/b HTTP/1.1", 400, ""},
		{"missing-version-3", "GET /", 400, ""},
		{"http2-9110-15.6.6", "GET / HTTP/2.0", 505, "505 HTTP Version Not Supported"},
		{"http0.9-9110-15.6.6", "GET / HTTP/0.9", 505, "505 HTTP Version Not Supported"},
		{"http3-9110-15.6.6", "GET / HTTP/3.0", 505, "505 HTTP Version Not Supported"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			host := "Host: a\r\n"
			if strings.HasPrefix(tc.line, "GET http://") {
				host = "Host: ignored.example\r\n" // the target's authority wins
			}
			out, _ := wireExchange(t, addr, tc.line+"\r\n"+host+"Connection: close\r\n\r\n", testIOTimeout)
			if got := statusOf(out); got != tc.want {
				t.Fatalf("status %d, want %d: %q", got, tc.want, truncate(out, 200))
			}
			if !strings.Contains(out, tc.wantBody) {
				t.Fatalf("response lacks %q: %q", truncate(tc.wantBody, 60), truncate(out, 300))
			}
		})
	}

	t.Run("rejection-closes-3", func(t *testing.T) {
		out, closed := wireExchange(t, addr, "GET / http/1.1\r\nHost: a\r\n\r\n"+smuggled, testIOTimeout)
		if statusOf(out) != 400 || !closed || !strings.Contains(out, "Connection: close") {
			t.Fatalf("closed=%v, response %q", closed, truncate(out, 200))
		}
	})
	if n := p.smuggled.Load(); n != 0 {
		t.Fatalf("a request behind a rejected one was served %d times", n)
	}
}

// TestRFC9112RequestTargetTooLong: a request-target longer than the server
// parses gets 414 (3 MUST), a header section over the limit 431 (RFC 6585 5),
// on the event loop's path and on the worker's (a pipelined request).
func TestRFC9112RequestTargetTooLong(t *testing.T) {
	addr := startServer(t, &Server{MaxHeaderBytes: 4 << 10, Handler: &smuggleProbe{}})
	longTarget := "GET /" + strings.Repeat("a", 16<<10) + " HTTP/1.1\r\nHost: a\r\n\r\n"
	longHeader := "GET / HTTP/1.1\r\nHost: a\r\nX-Big: " + strings.Repeat("b", 16<<10) + "\r\n\r\n"
	first := "GET /first HTTP/1.1\r\nHost: a\r\n\r\n"

	cases := []struct {
		name, raw string
		want      int
	}{
		{"target-first-request", longTarget, 414},
		{"header-first-request", longHeader, 431},
		{"target-pipelined", first + longTarget, 414},
		{"header-pipelined", first + longHeader, 431},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			out, closed := wireExchange(t, addr, tc.raw, testIOTimeout)
			if strings.HasPrefix(tc.raw, first) {
				_, out, _ = strings.Cut(out, `body=""`) // past the first response
				out = strings.TrimPrefix(out, " err=<nil>")
			}
			if got := statusOf(out); got != tc.want || !closed {
				t.Fatalf("status %d (want %d), closed=%v: %q", got, tc.want, closed, truncate(out, 200))
			}
		})
	}
}

// TestHTTPEmptyLinesBeforeRequestLine covers 2.2: a server SHOULD ignore at
// least one empty line before a request line; old clients send one after a
// request body. fhttp ignores any number, before any request; empty lines
// alone neither start a request nor move its deadlines.
func TestHTTPEmptyLinesBeforeRequestLine(t *testing.T) {
	p := &smuggleProbe{}
	addr := startServer(t, &Server{Handler: p, ReadHeaderTimeout: 300 * time.Millisecond})

	t.Run("before-first-request", func(t *testing.T) {
		out, _ := wireExchange(t, addr, "\r\n\n\r\nGET /first HTTP/1.1\r\nHost: a\r\nConnection: close\r\n\r\n", testIOTimeout)
		if statusOf(out) != 200 || !strings.Contains(out, "GET /first") {
			t.Fatalf("%q", truncate(out, 200))
		}
	})
	t.Run("after-a-body-pipelined", func(t *testing.T) {
		out, _ := wireExchange(t, addr, "POST /a HTTP/1.1\r\nHost: a\r\nContent-Length: 1\r\n\r\nx\r\n"+
			"GET /b HTTP/1.1\r\nHost: a\r\nConnection: close\r\n\r\n", testIOTimeout)
		if strings.Count(out, "HTTP/1.1 200 OK") != 2 || !strings.Contains(out, "GET /b") {
			t.Fatalf("%q", truncate(out, 400))
		}
	})
	t.Run("in-separate-segments", func(t *testing.T) {
		c := dialRaw(t, addr)
		c.write("POST /a HTTP/1.1\r\nHost: a\r\nContent-Length: 1\r\n\r\nx")
		if got := readBody(t, c.readResponse("POST")); !strings.Contains(got, `body="x"`) {
			t.Fatalf("first response %q", got)
		}
		c.write("\r\n")
		time.Sleep(50 * time.Millisecond)
		c.write("GET /b HTTP/1.1\r\nHost: a\r\n\r\n")
		if got := readBody(t, c.readResponse("GET")); !strings.HasPrefix(got, "GET /b") {
			t.Fatalf("second response %q", got)
		}
	})
	t.Run("alone-is-not-a-request", func(t *testing.T) {
		// Empty lines alone get no 400: the connection just times out, as
		// one that sent nothing would.
		out, closed := wireExchange(t, addr, "\r\n\r\n\r\n", testIOTimeout)
		if out != "" || !closed {
			t.Fatalf("closed=%v, answered %q", closed, out)
		}
	})
}

// ---------------------------------------------------------------------------
// 3.2 Host
// ---------------------------------------------------------------------------

func TestRFC9112HostHeader(t *testing.T) {
	addr := startHTTPFunc(t, func(w http.ResponseWriter, r *http.Request) {
		_, inHeader := r.Header["Host"]
		fmt.Fprintf(w, "host=%q inHeader=%v", r.Host, inHeader)
	})
	cases := []struct {
		name, raw string
		want      int
		wantBody  string
	}{
		{"present", "GET / HTTP/1.1\r\nHost: a.example:8080\r\n", 200, `host="a.example:8080" inHeader=false`},
		// An empty Host is what a client sends when the target has no
		// authority (3.2); net/http accepts it too.
		{"empty", "GET / HTTP/1.1\r\nHost:\r\n", 200, `host=""`},
		{"missing-1.1", "GET / HTTP/1.1\r\n", 400, "missing required Host header"},
		{"missing-absolute-form", "GET http://a.example/ HTTP/1.1\r\n", 400, "missing required Host header"},
		{"absolute-form-wins", "GET http://a.example/ HTTP/1.1\r\nHost: b.example\r\n", 200, `host="a.example"`},
		{"missing-1.0", "GET / HTTP/1.0\r\n", 200, `host=""`},
		{"twice", "GET / HTTP/1.1\r\nHost: a\r\nHost: b\r\n", 400, ""},
		{"invalid-value", "GET / HTTP/1.1\r\nHost: a b\r\n", 400, "malformed Host header"},
		{"invalid-bracket", "GET / HTTP/1.1\r\nHost: a<b\r\n", 400, "malformed Host header"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			out, _ := wireExchange(t, addr, tc.raw+"Connection: close\r\n\r\n", testIOTimeout)
			if got := statusOf(out); got != tc.want || !strings.Contains(out, tc.wantBody) {
				t.Fatalf("status %d (want %d, body with %q): %q", got, tc.want, tc.wantBody, truncate(out, 300))
			}
		})
	}
}

// ---------------------------------------------------------------------------
// 5 Field syntax
// ---------------------------------------------------------------------------

func TestRFC9112FieldSyntax(t *testing.T) {
	p := &smuggleProbe{}
	echo := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/smuggled" {
			p.ServeHTTP(w, r)
			return
		}
		fmt.Fprintf(w, "x-a=%q n=%d", r.Header.Get("X-A"), len(r.Header["X-A"]))
	})
	addr := startHTTP(t, echo)
	cases := []struct {
		name, fields string
		want         int
		wantBody     string
	}{
		{"ows-trimmed-5.1", "X-A: \t value \t\r\n", 200, `x-a="value"`},
		{"unknown-field-ignored-9110-5.1", "X-Unknown-42: v\r\n", 200, `x-a=""`},
		{"repeated-field-9110-5.3", "X-A: 1\r\nX-A: 2\r\n", 200, "n=2"},
		// obs-fold: 5.2 lets a server reject it or replace it with SP; net/http
		// and fhttp replace it.
		{"obs-fold-replaced-5.2", "X-A: one\r\n two\r\n", 200, `x-a="one two"`},
		{"space-before-colon-5.1", "X-A : v\r\n", 400, "invalid header name"},
		{"tab-before-colon-5.1", "X-A\t: v\r\n", 400, ""},
		{"framing-field-space-before-colon-5.1", "Transfer-Encoding : chunked\r\n", 400, "invalid header name"},
		{"no-colon-5", "X-A v\r\n", 400, ""},
		{"bad-name-byte-5", "X(A): v\r\n", 400, ""},
		{"bare-cr-in-value-2.2", "X-A: b\rc\r\n", 400, ""},
		{"nul-in-value-9110-5.5", "X-A: v\x00v\r\n", 400, ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			next := smuggled // must not be served behind a rejected request
			if tc.want == 200 {
				next = ""
				tc.fields += "Connection: close\r\n"
			}
			out, closed := wireExchange(t, addr, "GET / HTTP/1.1\r\nHost: a\r\n"+tc.fields+"\r\n"+next, testIOTimeout)
			if got := statusOf(out); got != tc.want || !strings.Contains(out, tc.wantBody) {
				t.Fatalf("status %d (want %d, body with %q): %q", got, tc.want, tc.wantBody, truncate(out, 300))
			}
			if tc.want != 200 && !closed {
				t.Fatal("a rejected request left the connection open")
			}
		})
	}

	t.Run("whitespace-line-after-request-line-2.2", func(t *testing.T) {
		out, closed := wireExchange(t, addr, "GET / HTTP/1.1\r\n Host: evil\r\nHost: a\r\n\r\n"+smuggled, testIOTimeout)
		if statusOf(out) != 400 || !closed {
			t.Fatalf("closed=%v: %q", closed, truncate(out, 200))
		}
	})
	t.Run("bare-lf-line-endings-2.2", func(t *testing.T) {
		// A recipient MAY take a lone LF as a line end; net/http does.
		out, _ := wireExchange(t, addr, "GET / HTTP/1.1\nHost: a\nX-A: lf\nConnection: close\n\n", testIOTimeout)
		if statusOf(out) != 200 || !strings.Contains(out, `x-a="lf"`) {
			t.Fatalf("%q", truncate(out, 200))
		}
	})
	if n := p.smuggled.Load(); n != 0 {
		t.Fatalf("a request behind a rejected one was served %d times", n)
	}
}

// ---------------------------------------------------------------------------
// 6 Message body length, 7 Chunked transfer coding
// ---------------------------------------------------------------------------

func TestRFC9112RequestBodyLength(t *testing.T) {
	p := &smuggleProbe{}
	addr := startHTTP(t, p)

	cases := []struct {
		name, raw string
		want      int
		wantBody  string
		wantClose bool
	}{
		{"chunked-6.1", "POST / HTTP/1.1\r\nHost: a\r\nTransfer-Encoding: chunked\r\n\r\n5\r\nhello\r\n0\r\n\r\n",
			200, `body="hello" err=<nil>`, false},
		{"chunked-any-case-7", "POST / HTTP/1.1\r\nHost: a\r\nTransfer-Encoding: ChUnKeD\r\n\r\n5\r\nhello\r\n0\r\n\r\n",
			200, `body="hello"`, false},
		{"chunk-extensions-ignored-7.1.1", "POST / HTTP/1.1\r\nHost: a\r\nTransfer-Encoding: chunked\r\n\r\n5;foo=bar;baz=\"a;b\"\r\nhello\r\n00000;x\r\n\r\n",
			200, `body="hello"`, false},
		{"content-length-6.2", "POST / HTTP/1.1\r\nHost: a\r\nContent-Length: 5\r\n\r\nhello",
			200, `body="hello"`, false},
		{"repeated-equal-lengths-6.3", "POST / HTTP/1.1\r\nHost: a\r\nContent-Length: 5\r\nContent-Length: 5\r\n\r\nhello",
			200, `body="hello"`, false},
		// 6.3 rule 3 and 6.1: Transfer-Encoding overrides a Content-Length,
		// and the connection MUST close after the response.
		{"te-and-cl-6.1", "POST / HTTP/1.1\r\nHost: a\r\nContent-Length: 6\r\nTransfer-Encoding: chunked\r\n\r\n0\r\n\r\n",
			200, `body=""`, true},
		// 6.1: an HTTP/1.0 message with Transfer-Encoding has faulty framing;
		// close after it. net/http frames it by Content-Length.
		{"te-in-http1.0-6.1", "POST / HTTP/1.0\r\nHost: a\r\nConnection: keep-alive\r\nContent-Length: 5\r\nTransfer-Encoding: chunked\r\n\r\nhello",
			200, `body="hello"`, true},
		// 6.3 rule 4: chunked not last. 6.1 has an unknown coding answered
		// with 501, as net/http does.
		{"chunked-not-final-6.3", "POST / HTTP/1.1\r\nHost: a\r\nTransfer-Encoding: chunked, gzip\r\n\r\n0\r\n\r\n", 501, "", true},
		{"unknown-coding-6.1", "POST / HTTP/1.1\r\nHost: a\r\nTransfer-Encoding: xchunked\r\n\r\n0\r\n\r\n", 501, "", true},
		{"two-te-lines-6.1", "POST / HTTP/1.1\r\nHost: a\r\nTransfer-Encoding: chunked\r\nTransfer-Encoding: identity\r\n\r\n0\r\n\r\n", 501, "", true},
		// 6.3 rule 5: an invalid Content-Length is an unrecoverable error.
		{"different-lengths-6.3", "POST / HTTP/1.1\r\nHost: a\r\nContent-Length: 3\r\nContent-Length: 4\r\n\r\nabcd", 400, "", true},
		{"length-list-6.3", "POST / HTTP/1.1\r\nHost: a\r\nContent-Length: 3, 4\r\n\r\nabcd", 400, "", true},
		{"signed-length-6.2", "POST / HTTP/1.1\r\nHost: a\r\nContent-Length: +3\r\n\r\nabc", 400, "", true},
		{"hex-length-6.2", "POST / HTTP/1.1\r\nHost: a\r\nContent-Length: 0x3\r\n\r\nabc", 400, "", true},
		{"huge-length-9110-8.6", "POST / HTTP/1.1\r\nHost: a\r\nContent-Length: 99999999999999999999999\r\n\r\n", 400, "", true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			out, closed := wireExchange(t, addr, tc.raw+smuggled, time.Second)
			if got := statusOf(out); got != tc.want || !strings.Contains(out, tc.wantBody) {
				t.Fatalf("status %d (want %d, body with %q): %q", got, tc.want, tc.wantBody, truncate(out, 300))
			}
			if tc.wantClose {
				if !closed || !strings.Contains(out, "Connection: close") {
					t.Fatalf("closed=%v; want Connection: close and the close itself: %q", closed, truncate(out, 300))
				}
			} else if !strings.Contains(out, "GET /smuggled") {
				t.Fatalf("the connection was not reusable after the body: %q", truncate(out, 400))
			}
		})
	}
	if n := p.smuggled.Load(); n != 5 { // the five framings that keep the connection
		t.Fatalf("/smuggled served %d times, want 5", n)
	}

	t.Run("no-length-means-no-body-6.3", func(t *testing.T) {
		// Rule 7: a request without Content-Length or Transfer-Encoding has
		// no body; what follows is the next request, here a bad one.
		out, closed := wireExchange(t, addr, "POST / HTTP/1.1\r\nHost: a\r\n\r\nXYZ\r\n\r\n", time.Second)
		if !strings.Contains(out, `body="" err=<nil>`) || !strings.Contains(out, "400 Bad Request") || !closed {
			t.Fatalf("closed=%v: %q", closed, truncate(out, 400))
		}
	})
}

// TestRFC9112IncompleteBody covers 6.3 rule 6 and 8: a body cut short by the
// client's close is an error for the handler, never a complete body.
func TestRFC9112IncompleteBody(t *testing.T) {
	type result struct {
		body string
		err  error
	}
	got := make(chan result, 2)
	addr := startHTTPFunc(t, func(w http.ResponseWriter, r *http.Request) {
		b, err := io.ReadAll(r.Body)
		got <- result{string(b), err}
	})
	for _, raw := range []string{
		"POST / HTTP/1.1\r\nHost: a\r\nContent-Length: 10\r\n\r\nabc",
		"POST / HTTP/1.1\r\nHost: a\r\nTransfer-Encoding: chunked\r\n\r\n5\r\nhello\r\n", // no last chunk
	} {
		c := dialRaw(t, addr)
		c.write(raw)
		_ = c.c.(*net.TCPConn).CloseWrite()
		select {
		case r := <-got:
			if r.err == nil {
				t.Fatalf("truncated body %q read without an error", r.body)
			}
		case <-time.After(testIOTimeout):
			t.Fatal("handler never finished reading the truncated body")
		}
	}
}

// TestRFC9112ChunkSizeOverflow covers 7.1: a chunk size too large for an
// integer fails the body read; nothing wraps, panics or allocates it.
func TestRFC9112ChunkSizeOverflow(t *testing.T) {
	errc := make(chan error, 1)
	addr := startHTTPFunc(t, func(w http.ResponseWriter, r *http.Request) {
		_, err := io.Copy(io.Discard, r.Body)
		errc <- err
	})
	out, _ := wireExchange(t, addr, "POST / HTTP/1.1\r\nHost: a\r\nTransfer-Encoding: chunked\r\nConnection: close\r\n\r\n"+
		"10000000000000000\r\nx\r\n0\r\n\r\n", testIOTimeout)
	if err := <-errc; err == nil {
		t.Fatalf("a 65-bit chunk size was read without an error (response %q)", truncate(out, 100))
	}
}

// TestRFC9112RequestTrailers covers 7.1.2 and RFC 9110 6.5: trailer fields
// reach the handler in Request.Trailer once the body is read, declared or
// not, and never merge into the header.
func TestRFC9112RequestTrailers(t *testing.T) {
	addr := startHTTPFunc(t, func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		fmt.Fprintf(w, "trailer=%v header-x-t=%q cl=%q", r.Trailer, r.Header.Get("X-T"), r.Header.Get("Content-Length"))
	})
	for _, declare := range []string{"", "Trailer: X-T\r\n"} {
		out, _ := wireExchange(t, addr, "POST / HTTP/1.1\r\nHost: a\r\nTransfer-Encoding: chunked\r\nConnection: close\r\n"+declare+"\r\n"+
			"5\r\nhello\r\n0\r\nX-T: 1\r\nX-T: 2\r\n\r\n", testIOTimeout)
		if !strings.Contains(out, `trailer=map[X-T:[1 2]] header-x-t="" cl=""`) {
			t.Fatalf("declared=%v: %q", declare != "", truncate(out, 300))
		}
	}
}

// ---------------------------------------------------------------------------
// Responses: 4 Status line, 6 framing, RFC 9110 6.4.1, 6.6.1, 8.6, 9.3.2
// ---------------------------------------------------------------------------

var imfFixdate = regexp.MustCompile(`\r\nDate: (Mon|Tue|Wed|Thu|Fri|Sat|Sun), \d\d (Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec) \d{4} \d\d:\d\d:\d\d GMT\r\n`)

func TestRFC9112ResponseFraming(t *testing.T) {
	addr := startHTTPFunc(t, func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/sized":
			_, _ = io.WriteString(w, "0123456789")
		case "/head-aware":
			if r.Method == http.MethodHead {
				return // a handler that skips the body for HEAD
			}
			_, _ = io.WriteString(w, "full body")
		case "/204", "/304":
			w.Header().Set("Content-Length", "5")
			w.Header().Set("Transfer-Encoding", "chunked")
			w.WriteHeader(map[string]int{"/204": 204, "/304": 304}[r.URL.Path])
			_, _ = io.WriteString(w, "never")
		case "/599":
			w.WriteHeader(599)
		case "/stream":
			_, _ = io.WriteString(w, "a")
			w.(http.Flusher).Flush()
			_, _ = io.WriteString(w, "b")
		}
	})

	t.Run("head-states-gets-length-9110-8.6", func(t *testing.T) {
		out, _ := wireExchange(t, addr, "HEAD /sized HTTP/1.1\r\nHost: a\r\n\r\nGET /sized HTTP/1.1\r\nHost: a\r\nConnection: close\r\n\r\n", testIOTimeout)
		head, rest, _ := strings.Cut(out, "\r\n\r\n")
		if !strings.Contains(head, "Content-Length: 10\r\n") || !strings.HasPrefix(rest, "HTTP/1.1 200 OK") {
			t.Fatalf("HEAD response %q, then %q", head, truncate(rest, 60)) // a body would sit before the next status line
		}
	})
	t.Run("head-claims-no-length-it-does-not-know-9110-8.6", func(t *testing.T) {
		out, _ := wireExchange(t, addr, "HEAD /head-aware HTTP/1.1\r\nHost: a\r\nConnection: close\r\n\r\n", testIOTimeout)
		if statusOf(out) != 200 || strings.Contains(out, "Content-Length") || !strings.HasSuffix(out, "\r\n\r\n") {
			t.Fatalf("%q", out)
		}
	})
	for _, path := range []string{"/204", "/304"} {
		t.Run("no-content-no-framing"+path+"-6.1-9110-8.6", func(t *testing.T) {
			out, _ := wireExchange(t, addr, "GET "+path+" HTTP/1.1\r\nHost: a\r\n\r\nGET /sized HTTP/1.1\r\nHost: a\r\nConnection: close\r\n\r\n", testIOTimeout)
			head, rest, _ := strings.Cut(out, "\r\n\r\n")
			if strings.Contains(head, "Content-Length") || strings.Contains(head, "Transfer-Encoding") ||
				!strings.HasPrefix(rest, "HTTP/1.1 200 OK") {
				t.Fatalf("response %q, then %q", head, truncate(rest, 60))
			}
		})
	}
	t.Run("reason-phrase-space-kept-4", func(t *testing.T) {
		out, _ := wireExchange(t, addr, "GET /599 HTTP/1.1\r\nHost: a\r\nConnection: close\r\n\r\n", testIOTimeout)
		if !strings.HasPrefix(out, "HTTP/1.1 599 \r\n") {
			t.Fatalf("%q", truncate(out, 60))
		}
	})
	t.Run("http1.0-never-chunked-6.1", func(t *testing.T) {
		out, closed := wireExchange(t, addr, "GET /stream HTTP/1.0\r\nConnection: keep-alive\r\n\r\n", testIOTimeout)
		head, body, _ := strings.Cut(out, "\r\n\r\n")
		if strings.Contains(head, "Transfer-Encoding") || !strings.Contains(head, "Connection: close") || body != "ab" || !closed {
			t.Fatalf("closed=%v: %q", closed, out)
		}
	})
	t.Run("status-line-and-date-4-9110-6.6.1", func(t *testing.T) {
		statusLine := regexp.MustCompile(`^HTTP/1\.1 \d{3} [^\r\n]*\r\n`)
		for _, raw := range []string{
			"GET /sized HTTP/1.1\r\nHost: a\r\nConnection: close\r\n\r\n", // 2xx
			"GET / HTTP/1.1\r\nConnection: close\r\n\r\n",                 // 4xx from the server itself
			"GET / HTTP/2.0\r\nHost: a\r\n\r\n",                           // 5xx from the server itself
		} {
			out, _ := wireExchange(t, addr, raw, testIOTimeout)
			if !statusLine.MatchString(out) || !imfFixdate.MatchString(out) {
				t.Fatalf("status line or IMF-fixdate Date missing: %q", truncate(out, 200))
			}
		}
	})
}

// TestHTTPResponseHeaderSanitised covers 2.2 and 11.1: whatever a handler puts
// in a header, no CR or LF of its own reaches the header section.
func TestHTTPResponseHeaderSanitised(t *testing.T) {
	addr := startHTTPFunc(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("X-Split", "a\r\nX-Injected: 1")
		w.Header().Set("X-Bare-CR", "b\rc")
		w.Header()["Bad Name"] = []string{"v"}
		_, _ = io.WriteString(w, "ok")
	})
	out, _ := wireExchange(t, addr, "GET / HTTP/1.1\r\nHost: a\r\nConnection: close\r\n\r\n", testIOTimeout)
	head, _, _ := strings.Cut(out, "\r\n\r\n")
	for line := range strings.SplitSeq(head, "\r\n") {
		if strings.ContainsAny(line, "\r\n") || strings.HasPrefix(line, "X-Injected") || strings.HasPrefix(line, "Bad Name") {
			t.Fatalf("header section carries a line the handler smuggled in: %q", head)
		}
	}
}

// TestHTTPResponseTrailers covers 7.1.2 and RFC 9110 6.5: declared trailers
// and http.TrailerPrefix ones follow a chunked body, the framing fields among
// them never do, and a response whose length would otherwise be computed is
// chunked to carry them.
func TestHTTPResponseTrailers(t *testing.T) {
	addr := startHTTPFunc(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Trailer", "X-Sum")
		_, _ = io.WriteString(w, "body")
		w.Header().Set("X-Sum", "42")
		w.Header().Set(http.TrailerPrefix+"X-Late", "yes")
		w.Header().Set(http.TrailerPrefix+"Content-Length", "999") // never a trailer
	})
	c := dialRaw(t, addr)
	c.write("GET / HTTP/1.1\r\nHost: a\r\n\r\n")
	resp := c.readResponse("GET")
	if got := readBody(t, resp); got != "body" {
		t.Fatalf("body %q", got)
	}
	if len(resp.TransferEncoding) == 0 || resp.TransferEncoding[0] != "chunked" {
		t.Fatalf("trailers need chunked encoding; got %v", resp.TransferEncoding)
	}
	if got := resp.Trailer.Get("X-Sum"); got != "42" {
		t.Errorf("X-Sum trailer = %q", got)
	}
	if got := resp.Trailer.Get("X-Late"); got != "yes" {
		t.Errorf("X-Late trailer = %q", got)
	}
	if got := resp.Trailer.Get("Content-Length"); got != "" {
		t.Errorf("framing field sent as a trailer: %q", got)
	}
	if got := resp.Header.Get("X-Sum"); got != "" {
		t.Errorf("trailer value also in the header: %q", got)
	}
	// The connection is intact for the next request.
	c.write("GET / HTTP/1.1\r\nHost: a\r\nConnection: close\r\n\r\n")
	if got := readBody(t, c.readResponse("GET")); got != "body" {
		t.Fatalf("second body %q", got)
	}
}

// TestHTTPContentLengthMismatch covers RFC 9110 8.6: the framing on the wire
// always matches the bytes sent. A handler writing past its Content-Length is
// refused; one writing less has the connection closed, announced when known.
func TestHTTPContentLengthMismatch(t *testing.T) {
	werr := make(chan error, 1)
	addr := startHTTPFunc(t, func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/long":
			w.Header().Set("Content-Length", "3")
			_, err := io.WriteString(w, "toolong")
			werr <- err
		case "/short":
			w.Header().Set("Content-Length", "10")
			_, _ = io.WriteString(w, "abc") // streams: the header is out before the shortfall shows
		case "/nothing":
			w.Header().Set("Content-Length", "10")
		case "/set-late":
			_, _ = io.WriteString(w, "abcdef") // held back, then a smaller length
			w.Header().Set("Content-Length", "2")
		}
	})
	t.Run("longer", func(t *testing.T) {
		out, closed := wireExchange(t, addr, "GET /long HTTP/1.1\r\nHost: a\r\n\r\n"+smuggled, testIOTimeout)
		if err := <-werr; !errors.Is(err, http.ErrContentLength) {
			t.Fatalf("Write past Content-Length returned %v", err)
		}
		if !closed || strings.Contains(out, "toolong") {
			t.Fatalf("closed=%v: %q", closed, out)
		}
	})
	t.Run("shorter", func(t *testing.T) {
		out, closed := wireExchange(t, addr, "GET /short HTTP/1.1\r\nHost: a\r\n\r\n", testIOTimeout)
		if !closed || !strings.HasSuffix(out, "\r\n\r\nabc") {
			t.Fatalf("closed=%v: %q", closed, out)
		}
	})
	t.Run("shorter-known-up-front", func(t *testing.T) {
		out, closed := wireExchange(t, addr, "GET /nothing HTTP/1.1\r\nHost: a\r\n\r\n", testIOTimeout)
		if !closed || !strings.Contains(out, "Connection: close\r\n") || !strings.HasSuffix(out, "\r\n\r\n") {
			t.Fatalf("closed=%v: %q", closed, out)
		}
	})
	t.Run("length-set-after-body", func(t *testing.T) {
		out, closed := wireExchange(t, addr, "GET /set-late HTTP/1.1\r\nHost: a\r\nConnection: close\r\n\r\n", testIOTimeout)
		// The header set after the body was fixed has no effect (net/http):
		// the body goes out with its own length.
		if !strings.Contains(out, "Content-Length: 6\r\n") || !strings.HasSuffix(out, "abcdef") {
			t.Fatalf("closed=%v: %q", closed, out)
		}
	})
}

// TestHTTPContentTypeSniffed matches net/http: a body without a Content-Type
// gets the sniffed one, unless the handler set it to nil; so does HEAD, from
// the body its handler wrote. Date opts out the same way.
func TestHTTPContentTypeSniffed(t *testing.T) {
	addr := startHTTPFunc(t, func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/none" {
			w.Header()["Content-Type"] = nil
			w.Header()["Date"] = nil
		}
		_, _ = io.WriteString(w, "<!DOCTYPE html><html><body>hi</body></html>")
	})
	for _, tc := range []struct{ method, path, want string }{
		{"GET", "/", "text/html; charset=utf-8"},
		{"HEAD", "/", "text/html; charset=utf-8"},
		{"GET", "/none", ""},
	} {
		c := dialRaw(t, addr)
		c.write(tc.method + " " + tc.path + " HTTP/1.1\r\nHost: a\r\nConnection: close\r\n\r\n")
		resp := c.readResponse(tc.method)
		if got := resp.Header.Get("Content-Type"); got != tc.want {
			t.Errorf("%s %s: Content-Type %q, want %q", tc.method, tc.path, got, tc.want)
		}
		if _, ok := resp.Header["Date"]; ok == (tc.path == "/none") {
			t.Errorf("%s %s: Date present=%v", tc.method, tc.path, ok)
		}
		c.close()
	}
}

// ---------------------------------------------------------------------------
// 9 Connection management
// ---------------------------------------------------------------------------

func TestRFC9112ConnectionClose(t *testing.T) {
	p := &smuggleProbe{}
	mux := http.NewServeMux()
	mux.Handle("/", p)
	mux.HandleFunc("/keep-alive-header", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Connection", "keep-alive") // overruled: the request asked to close
		_, _ = io.WriteString(w, "x")
	})
	mux.HandleFunc("/ignores-body", func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.WriteString(w, "did not read the body")
	})
	addr := startHTTP(t, mux)

	for _, option := range []string{"close", "CLOSE", "Keep-Alive, Close"} {
		t.Run("request-option-"+option+"-9.6", func(t *testing.T) {
			// 9.6: the response says close, the server closes, and the
			// request pipelined behind it is never processed.
			out, closed := wireExchange(t, addr, "GET /keep-alive-header HTTP/1.1\r\nHost: a\r\nConnection: "+option+"\r\n\r\n"+smuggled, testIOTimeout)
			if statusOf(out) != 200 || !closed || strings.Count(out, "HTTP/1.1") != 1 ||
				!strings.Contains(out, "Connection: close\r\n") || strings.Contains(out, "keep-alive") {
				t.Fatalf("closed=%v: %q", closed, out)
			}
		})
	}
	t.Run("unread-body-drained-9.3", func(t *testing.T) {
		// 9.3: the server reads the whole body or closes; the body's bytes
		// are never taken for the next request.
		out, _ := wireExchange(t, addr, fmt.Sprintf("POST /ignores-body HTTP/1.1\r\nHost: a\r\nContent-Length: %d\r\n\r\n", len(smuggled))+smuggled+
			"GET /next HTTP/1.1\r\nHost: a\r\nConnection: close\r\n\r\n", testIOTimeout)
		if strings.Count(out, "HTTP/1.1 200 OK") != 2 || !strings.Contains(out, "GET /next") {
			t.Fatalf("%q", truncate(out, 400))
		}
	})
	t.Run("http1.0-closes-by-default-9.3", func(t *testing.T) {
		out, closed := wireExchange(t, addr, "GET / HTTP/1.0\r\n\r\n"+smuggled, testIOTimeout)
		if statusOf(out) != 200 || !closed || strings.Count(out, "HTTP/1.1") != 1 {
			t.Fatalf("closed=%v: %q", closed, out)
		}
	})
	if n := p.smuggled.Load(); n != 0 {
		t.Fatalf("a request behind a close was served %d times", n)
	}
}

// TestRFC9112PipelinedResponsesInOrder covers 9.3.2: responses leave in
// request order, a slow first one holding back the fast ones behind it.
func TestRFC9112PipelinedResponsesInOrder(t *testing.T) {
	addr := startHTTPFunc(t, func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/slow" {
			time.Sleep(100 * time.Millisecond)
		}
		_, _ = io.WriteString(w, r.URL.Path)
	})
	c := dialRaw(t, addr)
	c.write("GET /slow HTTP/1.1\r\nHost: a\r\n\r\nGET /fast1 HTTP/1.1\r\nHost: a\r\n\r\nGET /fast2 HTTP/1.1\r\nHost: a\r\n\r\n")
	for _, want := range []string{"/slow", "/fast1", "/fast2"} {
		if got := readBody(t, c.readResponse("GET")); got != want {
			t.Fatalf("response %q, want %q", got, want)
		}
	}
}

// ---------------------------------------------------------------------------
// RFC 9110 10.1.1 Expect
// ---------------------------------------------------------------------------

func TestRFC9110Expect(t *testing.T) {
	addr := startHTTPFunc(t, func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		fmt.Fprintf(w, "body=%q", b)
	})
	t.Run("case-insensitive-10.1.1", func(t *testing.T) {
		c := dialRaw(t, addr)
		c.write("PUT / HTTP/1.1\r\nHost: a\r\nExpect: 100-Continue\r\nContent-Length: 4\r\n\r\n")
		_ = c.c.SetReadDeadline(time.Now().Add(testIOTimeout))
		line, err := c.br.ReadString('\n')
		if err != nil || line != "HTTP/1.1 100 Continue\r\n" {
			t.Fatalf("interim response %q, %v", line, err)
		}
		if blank, _ := c.br.ReadString('\n'); blank != "\r\n" {
			t.Fatalf("100 Continue carried more than its status line: %q", blank)
		}
		c.write("data")
		if got := readBody(t, c.readResponse("PUT")); got != `body="data"` {
			t.Fatalf("final response %q", got)
		}
	})
	t.Run("ignored-in-http1.0-10.1.1", func(t *testing.T) {
		// No 1xx to an HTTP/1.0 client (RFC 9110 15.2): it just sends the body.
		c := dialRaw(t, addr)
		c.write("PUT / HTTP/1.0\r\nExpect: 100-continue\r\nContent-Length: 4\r\n\r\n")
		time.Sleep(50 * time.Millisecond)
		c.write("data")
		out := c.readAll(testIOTimeout)
		if strings.Contains(out, " 100 ") || !strings.HasSuffix(out, `body="data"`) {
			t.Fatalf("%q", out)
		}
	})
}

// ---------------------------------------------------------------------------
// Hijack
// ---------------------------------------------------------------------------

// TestHTTPHijackAfterWriteHeader matches net/http: the status and header a
// handler set before Hijack go out first, and the hijacker speaks after them.
func TestHTTPHijackAfterWriteHeader(t *testing.T) {
	addr := startHTTPFunc(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Upgrade", "raw")
		w.Header().Set("Connection", "Upgrade")
		w.WriteHeader(http.StatusSwitchingProtocols)
		conn, brw, err := w.(http.Hijacker).Hijack()
		if err != nil {
			t.Errorf("Hijack: %v", err)
			return
		}
		defer conn.Close()
		_, _ = brw.WriteString("raw bytes")
		_ = brw.Flush()
	})
	c := dialRaw(t, addr)
	c.write("GET / HTTP/1.1\r\nHost: a\r\nConnection: Upgrade\r\nUpgrade: raw\r\n\r\n")
	resp := c.readResponse("GET")
	if resp.StatusCode != http.StatusSwitchingProtocols || resp.Header.Get("Upgrade") != "raw" ||
		resp.Header.Get("Connection") != "Upgrade" {
		t.Fatalf("status %d, header %v", resp.StatusCode, resp.Header)
	}
	if rest, _ := io.ReadAll(c.br); string(rest) != "raw bytes" {
		t.Fatalf("after the 101: %q", rest)
	}
}

// TestHTTPConnectTunnel covers RFC 9110 9.3.6 and RFC 9112 6.3: a 2xx to
// CONNECT turns the connection into a tunnel, so its header carries no
// Transfer-Encoding or Content-Length, and no Connection of the server's.
// (net/http sends Transfer-Encoding: chunked here.)
func TestHTTPConnectTunnel(t *testing.T) {
	addr := startHTTPFunc(t, func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodConnect || r.Host != "example.com:443" {
			http.Error(w, "not CONNECT", http.StatusBadRequest)
			return
		}
		w.WriteHeader(http.StatusOK)
		conn, brw, err := w.(http.Hijacker).Hijack()
		if err != nil {
			t.Errorf("Hijack: %v", err)
			return
		}
		defer conn.Close()
		line, _ := brw.ReadString('\n') // echo one line through the tunnel
		_, _ = brw.WriteString("tunnel:" + line)
		_ = brw.Flush()
	})
	c := dialRaw(t, addr)
	c.write("CONNECT example.com:443 HTTP/1.1\r\nHost: example.com:443\r\n\r\n")
	_ = c.c.SetReadDeadline(time.Now().Add(testIOTimeout))
	var head []string
	for {
		line, err := c.br.ReadString('\n')
		if err != nil {
			t.Fatalf("header: %v (so far %q)", err, head)
		}
		if line == "\r\n" {
			break
		}
		head = append(head, strings.TrimSpace(line))
	}
	if head[0] != "HTTP/1.1 200 OK" {
		t.Fatalf("status line %q", head[0])
	}
	for _, f := range head[1:] {
		if name, _, _ := strings.Cut(f, ":"); name != "Date" {
			t.Errorf("tunnel header carries %q", f)
		}
	}
	c.write("hello\n")
	if got, _ := c.br.ReadString('\n'); got != "tunnel:hello\n" {
		t.Fatalf("through the tunnel: %q", got)
	}
}

// TestHTTPShutdownFlushesLastResponse: Shutdown returns only once no HTTP
// connection is left, the ones closing after their last response included, so
// exiting right after it (Close here) loses none of that response. Here the
// response closes its connection (Connection: close) and the client reads it
// slowly: it is still on its way when Shutdown starts.
func TestHTTPShutdownFlushesLastResponse(t *testing.T) {
	const size = 16 << 20
	wrote := make(chan struct{})
	srv := &Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write(make([]byte, size))
		close(wrote)
	})}
	addr := startServer(t, srv)
	c := dialRaw(t, addr)
	_ = c.c.(*net.TCPConn).SetReadBuffer(64 << 10)
	c.write("GET / HTTP/1.1\r\nHost: a\r\nConnection: close\r\n\r\n")

	type result struct {
		err  error
		left int // connections the engine still had
	}
	done := make(chan result, 1)
	go func() {
		<-wrote
		err := srv.Shutdown(t.Context())
		left := 0
		if eng := srv.run.Stop(); eng != nil {
			eng.ForEach(func(*reactor.Conn) { left++ })
		}
		_ = srv.Close() // what exiting the process would do
		done <- result{err, left}
	}()
	resp := c.readResponse("GET")
	var n int64
	buf := make([]byte, 32<<10)
	for {
		time.Sleep(time.Millisecond) // a slow reader
		m, err := resp.Body.Read(buf)
		n += int64(m)
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatalf("response cut short after %d of %d bytes: %v", n, size, err)
		}
	}
	r := <-done
	if r.err != nil || r.left != 0 || n != size {
		t.Fatalf("Shutdown: %v, returning with %d connections left; read %d of %d", r.err, r.left, n, size)
	}
}
