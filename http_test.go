package fnet

import (
	"bufio"
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"mime/multipart"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// ---------------------------------------------------------------------------
// Request line: methods and target forms
// ---------------------------------------------------------------------------

func TestHTTPMethods(t *testing.T) {
	addr := startHTTPFunc(t, func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(r.Body)
		if err != nil {
			t.Errorf("%s: read body: %v", r.Method, err)
		}
		fmt.Fprintf(w, "%s|%d", r.Method, len(body))
	})

	// GET/HEAD/DELETE/OPTIONS/TRACE carry no body; the rest do. PURGE covers
	// the extension-method path, which must not be special-cased anywhere.
	cases := []struct {
		method string
		body   string
	}{
		{"GET", ""},
		{"POST", "payload"},
		{"PUT", "put-body"},
		{"PATCH", "{}"},
		{"DELETE", ""},
		{"OPTIONS", ""},
		{"TRACE", ""},
		{"PURGE", ""},
		{"PROPFIND", "<xml/>"},
	}
	for _, tc := range cases {
		t.Run(tc.method, func(t *testing.T) {
			c := dialRaw(t, addr)
			defer c.close()
			req := tc.method + " /m HTTP/1.1\r\nHost: example.com\r\nConnection: close\r\n"
			if tc.body != "" {
				req += "Content-Length: " + strconv.Itoa(len(tc.body)) + "\r\n"
			}
			req += "\r\n" + tc.body
			c.write(req)

			resp := c.readResponse(tc.method)
			if resp.StatusCode != 200 {
				t.Fatalf("status = %d, want 200", resp.StatusCode)
			}
			got := readBody(t, resp)
			// HEAD is excluded above precisely because it has no body to compare.
			want := fmt.Sprintf("%s|%d", tc.method, len(tc.body))
			if got != want {
				t.Fatalf("body = %q, want %q", got, want)
			}
		})
	}
}

func TestHTTPRequestTargetForms(t *testing.T) {
	addr := startHTTPFunc(t, func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintf(w, "uri=%s host=%s path=%s", r.RequestURI, r.Host, r.URL.Path)
	})

	cases := []struct {
		name    string
		reqLine string
		host    string
		want    string
	}{
		{
			// origin-form (RFC 9112 3.2.1)
			name:    "origin",
			reqLine: "GET /a/b?q=1 HTTP/1.1",
			host:    "example.com",
			want:    "uri=/a/b?q=1 host=example.com path=/a/b",
		},
		{
			// absolute-form: the request target's authority wins over Host.
			name:    "absolute",
			reqLine: "GET http://upstream.example/a/b?q=1 HTTP/1.1",
			host:    "ignored.example",
			want:    "uri=http://upstream.example/a/b?q=1 host=upstream.example path=/a/b",
		},
		{
			// asterisk-form, only valid for OPTIONS.
			name:    "asterisk",
			reqLine: "OPTIONS * HTTP/1.1",
			host:    "example.com",
			want:    "uri=* host=example.com path=*",
		},
		{
			// authority-form, only valid for CONNECT.
			name:    "authority",
			reqLine: "CONNECT tunnel.example:443 HTTP/1.1",
			host:    "tunnel.example:443",
			want:    "uri=tunnel.example:443 host=tunnel.example:443 path=",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			raw := tc.reqLine + "\r\nHost: " + tc.host + "\r\nConnection: close\r\n\r\n"
			out := rawExchange(t, addr, raw)
			if !strings.Contains(out, tc.want) {
				t.Fatalf("response %q does not contain %q", out, tc.want)
			}
		})
	}
}

func TestHTTPLongRequestTarget(t *testing.T) {
	addr := startHTTPFunc(t, func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintf(w, "path=%d query=%d", len(r.URL.Path), len(r.URL.Query()))
	})

	target := "/" + strings.Repeat("segment/", 1000) + "?" + strings.Repeat("k=v&", 500) + "last=1"
	out := rawExchange(t, addr, "GET "+target+" HTTP/1.1\r\nHost: x\r\nConnection: close\r\n\r\n")
	if !strings.Contains(out, "path=8001 query=2") {
		t.Fatalf("long target mishandled: %q", out)
	}
}

func TestHTTPPercentEncodedAndUnicodeTargets(t *testing.T) {
	addr := startHTTPFunc(t, func(w http.ResponseWriter, r *http.Request) {
		// EscapedPath preserves the wire form; Path is the decoded value.
		fmt.Fprintf(w, "path=%s escaped=%s q=%s", r.URL.Path, r.URL.EscapedPath(), r.URL.Query().Get("q"))
	})

	cases := []struct{ name, target, want string }{
		{"encoded-slash", "/a%2Fb", "path=/a/b escaped=/a%2Fb"},
		{"space-plus", "/x?q=a+b", "q=a b"},
		{"space-encoded", "/x?q=a%20b", "q=a b"},
		{"unicode", "/" + url.PathEscape("日本語"), "path=/日本語"},
		{"reserved", "/p?q=" + url.QueryEscape("a&b=c;d"), "q=a&b=c;d"},
		{"empty-query", "/p?", "path=/p"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			out := rawExchange(t, addr, "GET "+tc.target+" HTTP/1.1\r\nHost: x\r\nConnection: close\r\n\r\n")
			if !strings.Contains(out, tc.want) {
				t.Fatalf("response %q does not contain %q", out, tc.want)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// Protocol versions and connection management
// ---------------------------------------------------------------------------

func TestHTTPProtocolVersions(t *testing.T) {
	addr := startHTTPFunc(t, func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintf(w, "proto=%s close=%v", r.Proto, r.Close)
	})

	cases := []struct {
		name      string
		raw       string
		wantBody  string
		wantClose bool
	}{
		{
			// HTTP/1.0 defaults to close.
			name:      "1.0-default",
			raw:       "GET / HTTP/1.0\r\n\r\n",
			wantBody:  "proto=HTTP/1.0 close=true",
			wantClose: true,
		},
		{
			// HTTP/1.0 opts into persistence explicitly.
			name:     "1.0-keep-alive",
			raw:      "GET / HTTP/1.0\r\nConnection: keep-alive\r\n\r\n",
			wantBody: "proto=HTTP/1.0 close=false",
		},
		{
			// HTTP/1.1 is persistent by default.
			name:     "1.1-default",
			raw:      "GET / HTTP/1.1\r\nHost: x\r\n\r\n",
			wantBody: "proto=HTTP/1.1 close=false",
		},
		{
			name:      "1.1-close",
			raw:       "GET / HTTP/1.1\r\nHost: x\r\nConnection: close\r\n\r\n",
			wantBody:  "proto=HTTP/1.1 close=true",
			wantClose: true,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			c := dialRaw(t, addr)
			defer c.close()
			c.write(tc.raw)
			resp := c.readResponse("GET")
			// http.ReadResponse consumes `Connection: close` into resp.Close,
			// so check that rather than the header. `keep-alive` survives.
			if resp.Close != tc.wantClose {
				t.Errorf("resp.Close = %v, want %v (Connection=%q)",
					resp.Close, tc.wantClose, resp.Header.Get("Connection"))
			}
			if !tc.wantClose {
				if got := resp.Header.Get("Connection"); got != "keep-alive" {
					t.Errorf("Connection = %q, want keep-alive", got)
				}
			}
			if got := readBody(t, resp); got != tc.wantBody {
				t.Errorf("body = %q, want %q", got, tc.wantBody)
			}
			if !tc.wantClose {
				return
			}
			// The server must actually hang up, not just advertise it.
			if _, err := c.readResponseErr("GET"); err == nil {
				t.Fatal("connection stayed open after Connection: close")
			}
		})
	}
}

// TestHTTPConnectionCloseOnTheWire asserts the literal `Connection: close`
// header bytes, which http.ReadResponse hides by folding it into resp.Close.
func TestHTTPConnectionCloseOnTheWire(t *testing.T) {
	addr := startHTTPFunc(t, func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.WriteString(w, "bye")
	})
	out := rawExchange(t, addr, "GET / HTTP/1.1\r\nHost: x\r\nConnection: close\r\n\r\n")
	if !strings.Contains(out, "\r\nConnection: close\r\n") {
		t.Fatalf("response is missing Connection: close:\n%q", out)
	}
}

func TestHTTPKeepAliveManyRequests(t *testing.T) {
	addr := startHTTPFunc(t, func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintf(w, "n=%s", strings.TrimPrefix(r.URL.Path, "/"))
	})

	c := dialRaw(t, addr)
	defer c.close()
	const n = 50
	for i := 0; i < n; i++ {
		c.write(fmt.Sprintf("GET /%d HTTP/1.1\r\nHost: x\r\n\r\n", i))
		resp := c.readResponse("GET")
		if got, want := readBody(t, resp), fmt.Sprintf("n=%d", i); got != want {
			t.Fatalf("request %d: body = %q, want %q", i, got, want)
		}
	}
}

// TestHTTPKeepAliveAcrossIdleGaps covers the hand-off back to the poller: after
// each response the worker goroutine exits and the connection returns to idle
// custody, so the next request has to re-dispatch a fresh worker.
func TestHTTPKeepAliveAcrossIdleGaps(t *testing.T) {
	addr := startHTTPFunc(t, func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.WriteString(w, "ok")
	})

	c := dialRaw(t, addr)
	defer c.close()
	for i := 0; i < 5; i++ {
		c.write("GET / HTTP/1.1\r\nHost: x\r\n\r\n")
		if got := readBody(t, c.readResponse("GET")); got != "ok" {
			t.Fatalf("round %d: body = %q", i, got)
		}
		time.Sleep(60 * time.Millisecond) // let the worker retire
	}
}

func TestHTTPPipelinedRequests(t *testing.T) {
	addr := startHTTPFunc(t, func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintf(w, "p=%s", r.URL.Path)
	})

	c := dialRaw(t, addr)
	defer c.close()
	// All three requests arrive in a single segment; responses must come back
	// in request order.
	c.write("GET /1 HTTP/1.1\r\nHost: x\r\n\r\n" +
		"GET /2 HTTP/1.1\r\nHost: x\r\n\r\n" +
		"GET /3 HTTP/1.1\r\nHost: x\r\nConnection: close\r\n\r\n")

	for i := 1; i <= 3; i++ {
		resp := c.readResponse("GET")
		if got, want := readBody(t, resp), fmt.Sprintf("p=/%d", i); got != want {
			t.Fatalf("response %d: body = %q, want %q", i, got, want)
		}
	}
}

func TestHTTPPipelinedRequestsWithBodies(t *testing.T) {
	addr := startHTTPFunc(t, func(w http.ResponseWriter, r *http.Request) {
		b, err := io.ReadAll(r.Body)
		if err != nil {
			t.Errorf("read body: %v", err)
		}
		fmt.Fprintf(w, "%s:%s", r.URL.Path, b)
	})

	c := dialRaw(t, addr)
	defer c.close()
	var sb strings.Builder
	bodies := []string{"alpha", "bravo-body", "c"}
	for i, b := range bodies {
		fmt.Fprintf(&sb, "POST /%d HTTP/1.1\r\nHost: x\r\nContent-Length: %d\r\n\r\n%s", i, len(b), b)
	}
	sb.WriteString("GET /done HTTP/1.1\r\nHost: x\r\nConnection: close\r\n\r\n")
	c.write(sb.String())

	for i, b := range bodies {
		resp := c.readResponse("POST")
		if got, want := readBody(t, resp), fmt.Sprintf("/%d:%s", i, b); got != want {
			t.Fatalf("response %d: body = %q, want %q", i, got, want)
		}
	}
	if got := readBody(t, c.readResponse("GET")); got != "/done:" {
		t.Fatalf("trailing response body = %q", got)
	}
}

// TestHTTPPipelinedMixedWithUnreadBody checks that a handler leaving the request
// body unread does not desynchronise the next pipelined request.
func TestHTTPPipelinedMixedWithUnreadBody(t *testing.T) {
	addr := startHTTPFunc(t, func(w http.ResponseWriter, r *http.Request) {
		// Deliberately never touch r.Body.
		fmt.Fprintf(w, "p=%s", r.URL.Path)
	})

	c := dialRaw(t, addr)
	defer c.close()
	c.write("POST /1 HTTP/1.1\r\nHost: x\r\nContent-Length: 11\r\n\r\nhello world" +
		"POST /2 HTTP/1.1\r\nHost: x\r\nTransfer-Encoding: chunked\r\n\r\n3\r\nabc\r\n0\r\n\r\n" +
		"GET /3 HTTP/1.1\r\nHost: x\r\nConnection: close\r\n\r\n")

	for i := 1; i <= 3; i++ {
		resp := c.readResponse("POST")
		if got, want := readBody(t, resp), fmt.Sprintf("p=/%d", i); got != want {
			t.Fatalf("response %d: body = %q, want %q", i, got, want)
		}
	}
}

// ---------------------------------------------------------------------------
// Header parsing
// ---------------------------------------------------------------------------

func TestHTTPHeaderEdgeCases(t *testing.T) {
	addr := startHTTPFunc(t, func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintf(w, "single=%q multi=%v empty=%q|%v case=%q pad=%d",
			r.Header.Get("X-Single"),
			r.Header.Values("X-Multi"),
			r.Header.Get("X-Empty"),
			r.Header["X-Empty"] != nil,
			r.Header.Get("x-MiXeD-cAsE"),
			len(r.Header.Get("X-Padded")),
		)
	})

	raw := "GET / HTTP/1.1\r\n" +
		"Host: x\r\n" +
		"X-Single: one\r\n" +
		"X-Multi: a\r\n" +
		"X-Multi: b\r\n" +
		"X-Multi: c\r\n" +
		"X-Empty:\r\n" +
		"X-Mixed-Case: MiXeD\r\n" +
		"X-Padded: \t  spaced  \t\r\n" + // OWS around the value must be stripped
		"Connection: close\r\n\r\n"

	out := rawExchange(t, addr, raw)
	for _, want := range []string{
		`single="one"`,
		`multi=[a b c]`,
		`empty=""|true`,
		`case="MiXeD"`,
		`pad=6`, // surrounding tabs and spaces trimmed, leaving "spaced"
	} {
		if !strings.Contains(out, want) {
			t.Errorf("response %q does not contain %q", out, want)
		}
	}
}

func TestHTTPManyHeaders(t *testing.T) {
	addr := startHTTPFunc(t, func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintf(w, "count=%d first=%q last=%q",
			len(r.Header), r.Header.Get("X-H-0"), r.Header.Get("X-H-399"))
	})

	var sb strings.Builder
	sb.WriteString("GET / HTTP/1.1\r\nHost: x\r\n")
	for i := 0; i < 400; i++ {
		fmt.Fprintf(&sb, "X-H-%d: v%d\r\n", i, i)
	}
	sb.WriteString("Connection: close\r\n\r\n")

	// 400 X-H-* plus Connection; Host is moved to r.Host and is not in r.Header.
	out := rawExchange(t, addr, sb.String())
	if !strings.Contains(out, `count=401 first="v0" last="v399"`) {
		t.Fatalf("many-header request mishandled: %q", out)
	}
}

func TestHTTPLargeHeaderValue(t *testing.T) {
	addr := startHTTPFunc(t, func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintf(w, "len=%d", len(r.Header.Get("X-Big")))
	})

	// A single 32 KiB header value: large, but still under maxHeaderBuffer.
	big := strings.Repeat("A", 32*1024)
	raw := "GET / HTTP/1.1\r\nHost: x\r\nX-Big: " + big + "\r\nConnection: close\r\n\r\n"
	out := rawExchange(t, addr, raw)
	if !strings.Contains(out, fmt.Sprintf("len=%d", len(big))) {
		t.Fatalf("large header value mishandled: %q", truncate(out, 200))
	}
}

// TestHTTPHeaderBombDropped covers the slow-loris defence: a connection that
// keeps buffering header bytes without ever sending CRLFCRLF is dropped once it
// exceeds maxHeaderBuffer, and never reaches the handler.
func TestHTTPHeaderBombDropped(t *testing.T) {
	var reached bool
	var mu sync.Mutex
	addr := startHTTPFunc(t, func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		reached = true
		mu.Unlock()
	})

	c := dialRaw(t, addr)
	defer c.close()
	_ = c.c.SetWriteDeadline(time.Now().Add(testIOTimeout))
	_, _ = io.WriteString(c.c, "GET / HTTP/1.1\r\nHost: x\r\n")

	junk := "X-Pad: " + strings.Repeat("a", 8*1024) + "\r\n"
	var wrote int
	var writeErr error
	// 40 * 8 KiB = 320 KiB, comfortably past the 64 KiB cap.
	for i := 0; i < 40; i++ {
		n, err := io.WriteString(c.c, junk)
		wrote += n
		if err != nil {
			writeErr = err
			break
		}
	}

	// Either the server already reset us mid-write, or it drops the connection
	// without producing a response.
	out := c.readAll(2 * time.Second)
	if strings.Contains(out, "HTTP/1.1") {
		t.Fatalf("server answered an oversized header (wrote %d bytes): %q", wrote, truncate(out, 200))
	}
	mu.Lock()
	defer mu.Unlock()
	if reached {
		t.Fatal("handler was invoked for an oversized header")
	}
	t.Logf("header bomb rejected after %d bytes (writeErr=%v)", wrote, writeErr)
}

func TestHTTPMalformedRequestsRejected(t *testing.T) {
	var reached int
	var mu sync.Mutex
	addr := startHTTPFunc(t, func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		reached++
		mu.Unlock()
		_, _ = io.WriteString(w, "should-not-happen")
	})

	// Requests the parser must refuse outright. The server answers by closing
	// the connection rather than emitting 400, so assert on "no response".
	cases := []struct{ name, raw string }{
		{"garbage-request-line", "TOTALLY BROKEN\r\n\r\n"},
		{"missing-version", "GET /\r\n\r\n"},
		{"duplicate-content-length", "POST / HTTP/1.1\r\nHost: x\r\nContent-Length: 1\r\nContent-Length: 2\r\n\r\nab"},
		{"negative-content-length", "POST / HTTP/1.1\r\nHost: x\r\nContent-Length: -1\r\n\r\n"},
		{"non-numeric-content-length", "POST / HTTP/1.1\r\nHost: x\r\nContent-Length: abc\r\n\r\n"},
		{"binary-junk", "\x00\x01\x02\x03\xff\xfe\r\n\r\n"},
		{"bare-crlf", "\r\n\r\n\r\n"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			out := rawExchange(t, addr, tc.raw)
			if strings.Contains(out, "HTTP/1.1") {
				t.Fatalf("server responded to a malformed request: %q", truncate(out, 200))
			}
		})
	}

	mu.Lock()
	defer mu.Unlock()
	if reached != 0 {
		t.Fatalf("handler ran for %d malformed requests", reached)
	}
}

// TestHTTPByteAtATimeRequest delivers a complete request one byte per TCP write.
// The reactor must buffer it and only dispatch a worker once CRLFCRLF arrives.
func TestHTTPByteAtATimeRequest(t *testing.T) {
	addr := startHTTPFunc(t, func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		fmt.Fprintf(w, "path=%s body=%s", r.URL.Path, b)
	})

	c := dialRaw(t, addr)
	defer c.close()
	c.writeByteByByte("POST /drip HTTP/1.1\r\nHost: x\r\nContent-Length: 5\r\nConnection: close\r\n\r\nhello")
	resp := c.readResponse("POST")
	if got := readBody(t, resp); got != "path=/drip body=hello" {
		t.Fatalf("body = %q", got)
	}
}

// TestHTTPHeaderSplitAcrossSegments splits the request at every plausible
// boundary, including inside the CRLFCRLF terminator.
func TestHTTPHeaderSplitAcrossSegments(t *testing.T) {
	addr := startHTTPFunc(t, func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.WriteString(w, "ok:"+r.URL.Path)
	})

	raw := "GET /split HTTP/1.1\r\nHost: example.com\r\nX-A: 1\r\nConnection: close\r\n\r\n"
	for _, split := range []int{1, 10, len(raw) - 4, len(raw) - 3, len(raw) - 2, len(raw) - 1} {
		t.Run(fmt.Sprintf("at-%d", split), func(t *testing.T) {
			c := dialRaw(t, addr)
			defer c.close()
			c.write(raw[:split])
			time.Sleep(40 * time.Millisecond) // force a separate read event
			c.write(raw[split:])
			resp := c.readResponse("GET")
			if got := readBody(t, resp); got != "ok:/split" {
				t.Fatalf("body = %q", got)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// Request bodies
// ---------------------------------------------------------------------------

func TestHTTPChunkedRequestBody(t *testing.T) {
	addr := startHTTPFunc(t, func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(r.Body)
		if err != nil {
			t.Errorf("read chunked body: %v", err)
		}
		fmt.Fprintf(w, "te=%v body=%q trailer=%q cl=%d",
			r.TransferEncoding, body, r.Trailer.Get("X-Checksum"), r.ContentLength)
	})

	// Multiple chunks, a chunk extension, an uppercase hex size, and a trailer.
	raw := "POST /u HTTP/1.1\r\nHost: x\r\n" +
		"Transfer-Encoding: chunked\r\n" +
		"Trailer: X-Checksum\r\n" +
		"Connection: close\r\n\r\n" +
		"5;name=value\r\nhello\r\n" +
		"1\r\n \r\n" +
		"A\r\nworld12345\r\n" +
		"0\r\n" +
		"X-Checksum: deadbeef\r\n\r\n"

	out := rawExchange(t, addr, raw)
	for _, want := range []string{
		`te=[chunked]`,
		`body="hello world12345"`,
		`trailer="deadbeef"`,
		`cl=-1`, // chunked bodies have unknown length
	} {
		if !strings.Contains(out, want) {
			t.Errorf("response %q does not contain %q", out, want)
		}
	}
}

// TestHTTPChunkedRequestFragmented delivers the chunked body in fragments that
// deliberately split chunk-size lines and payloads.
func TestHTTPChunkedRequestFragmented(t *testing.T) {
	addr := startHTTPFunc(t, func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(r.Body)
		if err != nil {
			t.Errorf("read chunked body: %v", err)
		}
		fmt.Fprintf(w, "body=%s", body)
	})

	c := dialRaw(t, addr)
	defer c.close()
	c.write("POST /u HTTP/1.1\r\nHost: x\r\nTransfer-Encoding: chunked\r\nConnection: close\r\n\r\n")
	for _, frag := range []string{"4", "\r\n", "ab", "cd", "\r\n", "3\r\nef", "g\r\n", "0\r\n", "\r\n"} {
		c.write(frag)
		time.Sleep(15 * time.Millisecond)
	}
	if got := readBody(t, c.readResponse("POST")); got != "body=abcdefg" {
		t.Fatalf("body = %q", got)
	}
}

// TestHTTPChunkedOverridesContentLength covers RFC 9112 6.3: when both framing
// headers are present, Transfer-Encoding wins and Content-Length is ignored.
func TestHTTPChunkedOverridesContentLength(t *testing.T) {
	addr := startHTTPFunc(t, func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		fmt.Fprintf(w, "body=%q cl=%d te=%v", body, r.ContentLength, r.TransferEncoding)
	})

	raw := "POST / HTTP/1.1\r\nHost: x\r\n" +
		"Content-Length: 99\r\n" +
		"Transfer-Encoding: chunked\r\n" +
		"Connection: close\r\n\r\n" +
		"4\r\nbody\r\n0\r\n\r\n"

	out := rawExchange(t, addr, raw)
	if !strings.Contains(out, `body="body"`) || !strings.Contains(out, "cl=-1") {
		t.Fatalf("Transfer-Encoding did not override Content-Length: %q", out)
	}
}

func TestHTTPLargeRequestBody(t *testing.T) {
	const size = 4 << 20 // 4 MiB
	addr := startHTTPFunc(t, func(w http.ResponseWriter, r *http.Request) {
		sum := sha256.New()
		n, err := io.Copy(sum, r.Body)
		if err != nil {
			t.Errorf("copy body: %v", err)
		}
		fmt.Fprintf(w, "n=%d sha=%s", n, hex.EncodeToString(sum.Sum(nil)))
	})

	payload := make([]byte, size)
	for i := range payload {
		payload[i] = byte(i * 7)
	}
	want := sha256.Sum256(payload)

	client := newClient(t, nil)
	resp, err := client.Post("http://"+addr+"/upload", "application/octet-stream", bytes.NewReader(payload))
	if err != nil {
		t.Fatalf("POST: %v", err)
	}
	got := readBody(t, resp)
	expect := fmt.Sprintf("n=%d sha=%s", size, hex.EncodeToString(want[:]))
	if got != expect {
		t.Fatalf("body = %q, want %q", got, expect)
	}
}

func TestHTTPChunkedRequestLargeBody(t *testing.T) {
	const size = 1 << 20
	addr := startHTTPFunc(t, func(w http.ResponseWriter, r *http.Request) {
		sum := sha256.New()
		n, _ := io.Copy(sum, r.Body)
		fmt.Fprintf(w, "n=%d sha=%s te=%v", n, hex.EncodeToString(sum.Sum(nil)), r.TransferEncoding)
	})

	payload := bytes.Repeat([]byte("0123456789abcdef"), size/16)
	want := sha256.Sum256(payload)

	// An unknown-length body makes net/http use Transfer-Encoding: chunked.
	req, err := http.NewRequest("POST", "http://"+addr+"/chunked", io.NopCloser(bytes.NewReader(payload)))
	if err != nil {
		t.Fatal(err)
	}
	resp, err := newClient(t, nil).Do(req)
	if err != nil {
		t.Fatalf("POST: %v", err)
	}
	got := readBody(t, resp)
	if !strings.Contains(got, "te=[chunked]") {
		t.Fatalf("request was not chunked: %q", got)
	}
	if want := fmt.Sprintf("n=%d sha=%s", len(payload), hex.EncodeToString(want[:])); !strings.HasPrefix(got, want) {
		t.Fatalf("body = %q, want prefix %q", got, want)
	}
}

// TestHTTPExpect100Continue documents current behaviour: the server does not
// emit an interim 100 Continue, but a client that sends the body anyway (as
// RFC 9110 10.1.1 requires after a timeout) is served normally.
func TestHTTPExpect100Continue(t *testing.T) {
	addr := startHTTPFunc(t, func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		fmt.Fprintf(w, "expect=%q body=%s", r.Header.Get("Expect"), b)
	})

	raw := "POST / HTTP/1.1\r\nHost: x\r\n" +
		"Content-Length: 4\r\nExpect: 100-continue\r\nConnection: close\r\n\r\nabcd"

	c := dialRaw(t, addr)
	defer c.close()
	c.write(raw)
	resp := c.readResponse("POST")
	if resp.StatusCode != 200 {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}
	if got := readBody(t, resp); got != `expect="100-continue" body=abcd` {
		t.Fatalf("body = %q", got)
	}
}

func TestHTTPFormURLEncoded(t *testing.T) {
	addr := startHTTPFunc(t, func(w http.ResponseWriter, r *http.Request) {
		if err := r.ParseForm(); err != nil {
			t.Errorf("ParseForm: %v", err)
		}
		fmt.Fprintf(w, "a=%s b=%s multi=%v query=%s",
			r.PostForm.Get("a"), r.PostForm.Get("b"), r.PostForm["m"], r.URL.Query().Get("q"))
	})

	form := url.Values{"a": {"hello world"}, "b": {"x&y=z"}, "m": {"1", "2"}}
	resp, err := newClient(t, nil).PostForm("http://"+addr+"/f?q=inquery", form)
	if err != nil {
		t.Fatalf("PostForm: %v", err)
	}
	got := readBody(t, resp)
	if got != "a=hello world b=x&y=z multi=[1 2] query=inquery" {
		t.Fatalf("body = %q", got)
	}
}

func TestHTTPMultipartFormData(t *testing.T) {
	addr := startHTTPFunc(t, func(w http.ResponseWriter, r *http.Request) {
		if err := r.ParseMultipartForm(1 << 20); err != nil {
			t.Errorf("ParseMultipartForm: %v", err)
			return
		}
		f, hdr, err := r.FormFile("file")
		if err != nil {
			t.Errorf("FormFile: %v", err)
			return
		}
		defer f.Close()
		content, _ := io.ReadAll(f)
		fmt.Fprintf(w, "field=%s name=%s size=%d content=%s",
			r.FormValue("field"), hdr.Filename, len(content), content)
	})

	var body bytes.Buffer
	mw := multipart.NewWriter(&body)
	if err := mw.WriteField("field", "value"); err != nil {
		t.Fatal(err)
	}
	fw, err := mw.CreateFormFile("file", "report.txt")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := fw.Write([]byte("file-contents")); err != nil {
		t.Fatal(err)
	}
	if err := mw.Close(); err != nil {
		t.Fatal(err)
	}

	resp, err := newClient(t, nil).Post("http://"+addr+"/m", mw.FormDataContentType(), &body)
	if err != nil {
		t.Fatalf("POST: %v", err)
	}
	if got := readBody(t, resp); got != "field=value name=report.txt size=13 content=file-contents" {
		t.Fatalf("body = %q", got)
	}
}

func TestHTTPCookies(t *testing.T) {
	addr := startHTTPFunc(t, func(w http.ResponseWriter, r *http.Request) {
		in, err := r.Cookie("session")
		if err != nil {
			t.Errorf("read cookie: %v", err)
			return
		}
		http.SetCookie(w, &http.Cookie{Name: "issued", Value: "abc123", Path: "/", HttpOnly: true})
		http.SetCookie(w, &http.Cookie{Name: "second", Value: "v2", MaxAge: 60})
		fmt.Fprintf(w, "session=%s count=%d", in.Value, len(r.Cookies()))
	})

	c := dialRaw(t, addr)
	defer c.close()
	c.write("GET / HTTP/1.1\r\nHost: x\r\nCookie: session=s3cr3t; other=1\r\nConnection: close\r\n\r\n")
	resp := c.readResponse("GET")

	cookies := resp.Cookies()
	if len(cookies) != 2 {
		t.Fatalf("got %d Set-Cookie headers, want 2: %v", len(cookies), resp.Header.Values("Set-Cookie"))
	}
	if cookies[0].Name != "issued" || cookies[0].Value != "abc123" || !cookies[0].HttpOnly {
		t.Errorf("first cookie = %+v", cookies[0])
	}
	if got := readBody(t, resp); got != "session=s3cr3t count=2" {
		t.Fatalf("body = %q", got)
	}
}

// ---------------------------------------------------------------------------
// Response generation
// ---------------------------------------------------------------------------

func TestHTTPStatusCodes(t *testing.T) {
	addr := startHTTPFunc(t, func(w http.ResponseWriter, r *http.Request) {
		code, err := strconv.Atoi(strings.TrimPrefix(r.URL.Path, "/"))
		if err != nil {
			t.Errorf("bad path %q", r.URL.Path)
			return
		}
		w.WriteHeader(code)
		_, _ = io.WriteString(w, "body")
	})

	// 599 has no registered reason phrase: the status line must still parse.
	codes := []int{200, 201, 202, 204, 206, 301, 302, 304, 307, 308,
		400, 401, 403, 404, 405, 409, 410, 418, 422, 429,
		500, 501, 502, 503, 504, 599}

	for _, code := range codes {
		t.Run(strconv.Itoa(code), func(t *testing.T) {
			c := dialRaw(t, addr)
			defer c.close()
			c.write(fmt.Sprintf("GET /%d HTTP/1.1\r\nHost: x\r\nConnection: close\r\n\r\n", code))
			resp := c.readResponse("GET")
			if resp.StatusCode != code {
				t.Fatalf("status = %d, want %d", resp.StatusCode, code)
			}
			body := readBody(t, resp)
			// 204 and 304 must never carry a body (RFC 9110 6.4.1).
			if code == 204 || code == 304 {
				if body != "" {
					t.Fatalf("status %d carried a body: %q", code, body)
				}
				if cl := resp.Header.Get("Content-Length"); cl != "" {
					t.Fatalf("status %d advertised Content-Length: %q", code, cl)
				}
				return
			}
			if body != "body" {
				t.Fatalf("status %d: body = %q, want %q", code, body, "body")
			}
		})
	}
}

// TestHTTPHeadHasNoBody covers RFC 9110 9.3.2: a HEAD response mirrors the GET
// headers (including Content-Length) but must not include a message body. If it
// did, the stray bytes would be parsed as the start of the next response.
func TestHTTPHeadHasNoBody(t *testing.T) {
	addr := startHTTPFunc(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/plain")
		_, _ = io.WriteString(w, "hello")
	})

	c := dialRaw(t, addr)
	defer c.close()
	c.write("HEAD /res HTTP/1.1\r\nHost: x\r\n\r\n")
	resp := c.readResponse("HEAD")

	if got := resp.Header.Get("Content-Length"); got != "5" {
		t.Errorf("Content-Length = %q, want %q", got, "5")
	}
	if got := resp.Header.Get("Content-Type"); got != "text/plain" {
		t.Errorf("Content-Type = %q, want text/plain", got)
	}
	if body := readBody(t, resp); body != "" {
		t.Errorf("HEAD response carried a body: %q", body)
	}

	// The connection must still be usable: a HEAD body would corrupt this read.
	c.write("GET /res HTTP/1.1\r\nHost: x\r\nConnection: close\r\n\r\n")
	next := c.readResponse("GET")
	if got := readBody(t, next); got != "hello" {
		t.Fatalf("follow-up GET after HEAD: body = %q, want %q", got, "hello")
	}
}

func TestHTTPHeadWithExplicitFraming(t *testing.T) {
	addr := startHTTPFunc(t, func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/preset-length":
			w.Header().Set("Content-Length", "11")
			_, _ = io.WriteString(w, "hello world")
		case "/chunked":
			w.Header().Set("Transfer-Encoding", "chunked")
			_, _ = io.WriteString(w, "streamed")
		case "/huge":
			// Past the 64 KiB response buffer, where a GET would switch to chunked.
			_, _ = w.Write(bytes.Repeat([]byte("x"), 200*1024))
		}
	})

	for _, path := range []string{"/preset-length", "/chunked", "/huge"} {
		t.Run(path, func(t *testing.T) {
			c := dialRaw(t, addr)
			defer c.close()
			c.write("HEAD " + path + " HTTP/1.1\r\nHost: x\r\n\r\n")
			resp := c.readResponse("HEAD")
			if body := readBody(t, resp); body != "" {
				t.Fatalf("HEAD %s carried a body: %q", path, truncate(body, 80))
			}
			// Connection integrity is the real assertion: any leaked byte
			// (raw body or a chunk terminator) breaks the next response.
			c.write("GET /preset-length HTTP/1.1\r\nHost: x\r\nConnection: close\r\n\r\n")
			if got := readBody(t, c.readResponse("GET")); got != "hello world" {
				t.Fatalf("connection desynchronised after HEAD %s: %q", path, got)
			}
		})
	}
}

func TestHTTPResponseFraming(t *testing.T) {
	const big = 200 * 1024
	addr := startHTTPFunc(t, func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/empty":
		case "/small":
			_, _ = io.WriteString(w, "tiny")
		case "/explicit-length":
			w.Header().Set("Content-Length", "5")
			_, _ = io.WriteString(w, "fixed")
		case "/explicit-chunked":
			w.Header().Set("Transfer-Encoding", "chunked")
			_, _ = io.WriteString(w, "chunk-a")
			_, _ = io.WriteString(w, "chunk-b")
		case "/auto-chunked":
			// Exceeds the 64 KiB buffer, so the writer switches to chunked.
			_, _ = w.Write(bytes.Repeat([]byte("Z"), big))
		case "/many-small-writes":
			for i := 0; i < 10000; i++ {
				_, _ = io.WriteString(w, "0123456789")
			}
		}
	})

	cases := []struct {
		path        string
		wantBody    string
		wantLen     string // expected Content-Length, "" if chunked/absent
		wantChunked bool
	}{
		{path: "/empty", wantBody: "", wantLen: "0"},
		{path: "/small", wantBody: "tiny", wantLen: "4"},
		{path: "/explicit-length", wantBody: "fixed", wantLen: "5"},
		{path: "/explicit-chunked", wantBody: "chunk-achunk-b", wantChunked: true},
		{path: "/auto-chunked", wantBody: strings.Repeat("Z", big), wantChunked: true},
		{path: "/many-small-writes", wantBody: strings.Repeat("0123456789", 10000), wantChunked: true},
	}
	for _, tc := range cases {
		t.Run(tc.path, func(t *testing.T) {
			c := dialRaw(t, addr)
			defer c.close()
			c.write("GET " + tc.path + " HTTP/1.1\r\nHost: x\r\nConnection: close\r\n\r\n")
			resp := c.readResponse("GET")

			chunked := len(resp.TransferEncoding) > 0 && resp.TransferEncoding[0] == "chunked"
			if chunked != tc.wantChunked {
				t.Errorf("chunked = %v, want %v (TE=%v)", chunked, tc.wantChunked, resp.TransferEncoding)
			}
			if !tc.wantChunked {
				if got := resp.Header.Get("Content-Length"); got != tc.wantLen {
					t.Errorf("Content-Length = %q, want %q", got, tc.wantLen)
				}
			}
			if got := readBody(t, resp); got != tc.wantBody {
				t.Fatalf("body length %d, want %d", len(got), len(tc.wantBody))
			}
		})
	}
}

// TestHTTPChunkedResponseStreams verifies that an explicitly chunked response is
// written incrementally rather than buffered until the handler returns. The
// handler blocks until the client has actually received the first chunk, so a
// buffering implementation deadlocks into the read deadline instead of passing.
func TestHTTPChunkedResponseStreams(t *testing.T) {
	gotFirst := make(chan struct{})
	addr := startHTTPFunc(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Transfer-Encoding", "chunked")
		if _, err := io.WriteString(w, "first"); err != nil {
			t.Errorf("write first chunk: %v", err)
			return
		}
		select {
		case <-gotFirst:
		case <-time.After(testIOTimeout):
			t.Error("client never received the first chunk")
			return
		}
		_, _ = io.WriteString(w, "second")
	})

	c := dialRaw(t, addr)
	defer c.close()
	c.write("GET /stream HTTP/1.1\r\nHost: x\r\nConnection: close\r\n\r\n")
	resp := c.readResponse("GET")
	defer resp.Body.Close()

	buf := make([]byte, 5)
	if _, err := io.ReadFull(resp.Body, buf); err != nil {
		t.Fatalf("read first chunk: %v", err)
	}
	if string(buf) != "first" {
		t.Fatalf("first chunk = %q", buf)
	}
	close(gotFirst)

	rest, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read remainder: %v", err)
	}
	if string(rest) != "second" {
		t.Fatalf("remainder = %q", rest)
	}
}

// TestHTTPNeverSendsBothFramingHeaders covers RFC 9112 6.1: Content-Length and
// Transfer-Encoding must never appear on the same response. A message carrying
// both is ambiguous and is the classic request-smuggling primitive.
func TestHTTPNeverSendsBothFramingHeaders(t *testing.T) {
	addr := startHTTPFunc(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Transfer-Encoding", "chunked")
		if r.URL.Path == "/write" {
			_, _ = io.WriteString(w, "streamed")
		}
		// /nowrite deliberately produces an empty chunked body.
	})

	for _, method := range []string{"GET", "HEAD"} {
		for _, path := range []string{"/write", "/nowrite"} {
			t.Run(method+path, func(t *testing.T) {
				out := rawExchange(t, addr,
					method+" "+path+" HTTP/1.1\r\nHost: x\r\nConnection: close\r\n\r\n")
				head, _, _ := strings.Cut(out, "\r\n\r\n")
				hasCL := strings.Contains(head, "Content-Length:")
				hasTE := strings.Contains(head, "Transfer-Encoding:")
				if hasCL && hasTE {
					t.Fatalf("response carries both framing headers:\n%q", head)
				}
				if !hasCL && !hasTE {
					t.Fatalf("response carries no framing header at all:\n%q", head)
				}
			})
		}
	}
}

func TestHTTPResponseHeaders(t *testing.T) {
	addr := startHTTPFunc(t, func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/custom":
			w.Header().Set("Content-Type", "application/json; charset=utf-8")
			w.Header().Set("X-Single", "one")
			w.Header().Add("X-Multi", "a")
			w.Header().Add("X-Multi", "b")
			w.Header().Set("Etag", `"abc"`)
			_, _ = io.WriteString(w, `{"ok":true}`)
		case "/own-date":
			w.Header().Set("Date", "Thu, 01 Jan 1970 00:00:00 GMT")
		case "/handler-close":
			// A handler-set Connection: close must be honoured even though the
			// request itself asked for keep-alive.
			w.Header().Set("Connection", "close")
			_, _ = io.WriteString(w, "bye")
		}
	})

	t.Run("custom", func(t *testing.T) {
		c := dialRaw(t, addr)
		defer c.close()
		c.write("GET /custom HTTP/1.1\r\nHost: x\r\nConnection: close\r\n\r\n")
		resp := c.readResponse("GET")
		if got := resp.Header.Get("Content-Type"); got != "application/json; charset=utf-8" {
			t.Errorf("Content-Type = %q", got)
		}
		if got := resp.Header.Values("X-Multi"); len(got) != 2 || got[0] != "a" || got[1] != "b" {
			t.Errorf("X-Multi = %v", got)
		}
		if got := resp.Header.Get("Etag"); got != `"abc"` {
			t.Errorf("Etag = %q", got)
		}
		if resp.Header.Get("Date") == "" {
			t.Error("Date header missing")
		}
		if got := readBody(t, resp); got != `{"ok":true}` {
			t.Errorf("body = %q", got)
		}
	})

	t.Run("handler-date-wins", func(t *testing.T) {
		c := dialRaw(t, addr)
		defer c.close()
		c.write("GET /own-date HTTP/1.1\r\nHost: x\r\nConnection: close\r\n\r\n")
		resp := c.readResponse("GET")
		if got := resp.Header.Values("Date"); len(got) != 1 || got[0] != "Thu, 01 Jan 1970 00:00:00 GMT" {
			t.Fatalf("Date = %v, want the handler's value only", got)
		}
	})

	t.Run("handler-close", func(t *testing.T) {
		c := dialRaw(t, addr)
		defer c.close()
		c.write("GET /handler-close HTTP/1.1\r\nHost: x\r\n\r\n")
		resp := c.readResponse("GET")
		if !resp.Close {
			t.Errorf("resp.Close = false, want true (Connection=%q)", resp.Header.Get("Connection"))
		}
		if got := readBody(t, resp); got != "bye" {
			t.Errorf("body = %q", got)
		}
		if _, err := c.readResponseErr("GET"); err == nil {
			t.Fatal("connection stayed open after a handler-set Connection: close")
		}
	})
}

func TestHTTPWriteHeaderIsIdempotent(t *testing.T) {
	addr := startHTTPFunc(t, func(w http.ResponseWriter, r *http.Request) {
		// The first status to reach the wire wins; later calls are ignored.
		w.Header().Set("Content-Length", "2")
		w.WriteHeader(http.StatusCreated)
		w.WriteHeader(http.StatusInternalServerError)
		w.Header().Set("X-Too-Late", "1")
		_, _ = io.WriteString(w, "ok")
	})

	c := dialRaw(t, addr)
	defer c.close()
	c.write("GET / HTTP/1.1\r\nHost: x\r\nConnection: close\r\n\r\n")
	resp := c.readResponse("GET")
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("status = %d, want 201", resp.StatusCode)
	}
	if got := resp.Header.Get("X-Too-Late"); got != "" {
		t.Errorf("header set after WriteHeader leaked to the wire: %q", got)
	}
	if got := readBody(t, resp); got != "ok" {
		t.Fatalf("body = %q", got)
	}
}

func TestHTTPLargeResponseIntegrity(t *testing.T) {
	const size = 8 << 20 // 8 MiB, well past both the 64 KiB response and socket buffers
	payload := make([]byte, size)
	for i := range payload {
		payload[i] = byte(i%251 + 1)
	}
	want := sha256.Sum256(payload)

	addr := startHTTPFunc(t, func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write(payload)
	})

	resp, err := newClient(t, nil).Get("http://" + addr + "/big")
	if err != nil {
		t.Fatalf("GET: %v", err)
	}
	defer resp.Body.Close()

	sum := sha256.New()
	n, err := io.Copy(sum, resp.Body)
	if err != nil {
		t.Fatalf("read body: %v", err)
	}
	if n != size {
		t.Fatalf("read %d bytes, want %d", n, size)
	}
	if got := sum.Sum(nil); !bytes.Equal(got, want[:]) {
		t.Fatalf("payload corrupted: sha=%s want=%s", hex.EncodeToString(got), hex.EncodeToString(want[:]))
	}
}

// ---------------------------------------------------------------------------
// Hijack
// ---------------------------------------------------------------------------

func TestHTTPHijack(t *testing.T) {
	addr := startHTTPFunc(t, func(w http.ResponseWriter, r *http.Request) {
		hj, ok := w.(http.Hijacker)
		if !ok {
			t.Error("ResponseWriter is not an http.Hijacker")
			return
		}
		conn, brw, err := hj.Hijack()
		if err != nil {
			t.Errorf("Hijack: %v", err)
			return
		}
		defer conn.Close()

		// Speak a trivial line protocol after the upgrade handshake.
		_, _ = brw.WriteString("HTTP/1.1 101 Switching Protocols\r\nUpgrade: echo\r\nConnection: Upgrade\r\n\r\n")
		if err := brw.Flush(); err != nil {
			t.Errorf("flush handshake: %v", err)
			return
		}
		line, err := brw.ReadString('\n')
		if err != nil {
			t.Errorf("read client line: %v", err)
			return
		}
		_, _ = brw.WriteString("echo:" + line)
		_ = brw.Flush()
	})

	c := dialRaw(t, addr)
	defer c.close()
	c.write("GET /upgrade HTTP/1.1\r\nHost: x\r\nUpgrade: echo\r\nConnection: Upgrade\r\n\r\n")

	resp := c.readResponse("GET")
	if resp.StatusCode != http.StatusSwitchingProtocols {
		t.Fatalf("status = %d, want 101", resp.StatusCode)
	}
	if got := resp.Header.Get("Upgrade"); got != "echo" {
		t.Errorf("Upgrade = %q", got)
	}

	c.write("ping\n")
	_ = c.c.SetReadDeadline(time.Now().Add(testIOTimeout))
	line, err := c.br.ReadString('\n')
	if err != nil {
		t.Fatalf("read echo: %v", err)
	}
	if line != "echo:ping\n" {
		t.Fatalf("echo = %q", line)
	}
}

// TestHTTPHijackWithBufferedPipeline checks that bytes already read ahead by the
// HTTP reader are handed to the hijacker rather than dropped.
func TestHTTPHijackWithBufferedPipeline(t *testing.T) {
	addr := startHTTPFunc(t, func(w http.ResponseWriter, r *http.Request) {
		conn, brw, err := w.(http.Hijacker).Hijack()
		if err != nil {
			t.Errorf("Hijack: %v", err)
			return
		}
		defer conn.Close()
		buf := make([]byte, len("TRAILING-BYTES"))
		if _, err := io.ReadFull(brw, buf); err != nil {
			t.Errorf("read buffered bytes: %v", err)
			return
		}
		_, _ = brw.WriteString("HTTP/1.1 200 OK\r\nContent-Length: " +
			strconv.Itoa(len(buf)) + "\r\n\r\n" + string(buf))
		_ = brw.Flush()
	})

	c := dialRaw(t, addr)
	defer c.close()
	// The extra bytes ride along in the same segment as the request.
	c.write("GET /h HTTP/1.1\r\nHost: x\r\n\r\nTRAILING-BYTES")
	resp := c.readResponse("GET")
	if got := readBody(t, resp); got != "TRAILING-BYTES" {
		t.Fatalf("hijacker saw %q, want %q", got, "TRAILING-BYTES")
	}
}

// ---------------------------------------------------------------------------
// Concurrency, multiple listeners, timeouts
// ---------------------------------------------------------------------------

func TestHTTPConcurrentRequests(t *testing.T) {
	addr := startHTTPFunc(t, func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(r.Body)
		if err != nil {
			t.Errorf("read body: %v", err)
			return
		}
		w.Header().Set("X-Echo-Path", r.URL.Path)
		_, _ = w.Write(body)
	})

	client := newClient(t, nil)
	const (
		workers   = 16
		perWorker = 25
	)
	var wg sync.WaitGroup
	errs := make(chan error, workers*perWorker)
	for wk := 0; wk < workers; wk++ {
		wg.Add(1)
		go func(wk int) {
			defer wg.Done()
			for i := 0; i < perWorker; i++ {
				path := fmt.Sprintf("/w%d/r%d", wk, i)
				want := fmt.Sprintf("payload-%d-%d", wk, i)
				resp, err := client.Post("http://"+addr+path, "text/plain", strings.NewReader(want))
				if err != nil {
					errs <- fmt.Errorf("%s: %w", path, err)
					return
				}
				got, err := io.ReadAll(resp.Body)
				resp.Body.Close()
				if err != nil {
					errs <- fmt.Errorf("%s: read: %w", path, err)
					return
				}
				if string(got) != want {
					errs <- fmt.Errorf("%s: body = %q, want %q", path, got, want)
					return
				}
				if hp := resp.Header.Get("X-Echo-Path"); hp != path {
					errs <- fmt.Errorf("%s: response was crossed with %q", path, hp)
					return
				}
			}
		}(wk)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Error(err)
	}
}

// TestHTTPConcurrentPipeliningPerConnection hammers a single connection with
// pipelined requests to confirm responses stay ordered and uncorrupted.
func TestHTTPConcurrentPipeliningPerConnection(t *testing.T) {
	addr := startHTTPFunc(t, func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintf(w, "%s", strings.TrimPrefix(r.URL.Path, "/"))
	})

	c := dialRaw(t, addr)
	defer c.close()
	const n = 200
	var sb strings.Builder
	for i := 0; i < n; i++ {
		fmt.Fprintf(&sb, "GET /%d HTTP/1.1\r\nHost: x\r\n\r\n", i)
	}
	c.write(sb.String())
	for i := 0; i < n; i++ {
		resp := c.readResponse("GET")
		if got, want := readBody(t, resp), strconv.Itoa(i); got != want {
			t.Fatalf("response %d = %q, want %q", i, got, want)
		}
	}
}

func TestHTTPMultipleListenAddrs(t *testing.T) {
	addrs := []string{freeAddr(t), freeAddr(t), freeAddr(t)}
	srv := &Server{
		Addr:  addrs[0],
		Addrs: addrs[1:],
		Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			fmt.Fprintf(w, "served-by=%s", r.Host)
		}),
	}
	go func() { _ = srv.ListenAndServe() }()
	t.Cleanup(func() { _ = srv.Close() })

	for _, addr := range addrs {
		waitReady(t, addr)
		out := rawExchange(t, addr, "GET / HTTP/1.1\r\nHost: "+addr+"\r\nConnection: close\r\n\r\n")
		if !strings.Contains(out, "served-by="+addr) {
			t.Fatalf("listener %s returned %q", addr, truncate(out, 120))
		}
	}
}

func TestHTTPWithReadAndWriteTimeouts(t *testing.T) {
	// Deadlines must not disturb well-behaved traffic, including keep-alive.
	srv := &Server{
		Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			b, _ := io.ReadAll(r.Body)
			fmt.Fprintf(w, "n=%d", len(b))
		}),
		ReadTimeout:  2 * time.Second,
		WriteTimeout: 2 * time.Second,
		IdleTimeout:  2 * time.Second,
	}
	addr := startServer(t, srv)

	c := dialRaw(t, addr)
	defer c.close()
	for i := 0; i < 3; i++ {
		body := strings.Repeat("x", i+1)
		c.write(fmt.Sprintf("POST / HTTP/1.1\r\nHost: x\r\nContent-Length: %d\r\n\r\n%s", len(body), body))
		if got, want := readBody(t, c.readResponse("POST")), fmt.Sprintf("n=%d", i+1); got != want {
			t.Fatalf("round %d: body = %q, want %q", i, got, want)
		}
	}
}

func TestHTTPNumPollersOne(t *testing.T) {
	// A single reactor must still serve concurrent connections correctly.
	srv := &Server{
		NumPollers: 1,
		Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			_, _ = io.WriteString(w, "single")
		}),
	}
	addr := startServer(t, srv)

	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			out := rawExchange(t, addr, "GET / HTTP/1.1\r\nHost: x\r\nConnection: close\r\n\r\n")
			if !strings.Contains(out, "single") {
				t.Errorf("response = %q", truncate(out, 120))
			}
		}()
	}
	wg.Wait()
}

func TestHTTPClientAbortMidRequest(t *testing.T) {
	// Half-sent requests and abrupt disconnects must not wedge the reactor.
	addr := startHTTPFunc(t, func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.WriteString(w, "ok")
	})

	for i := 0; i < 20; i++ {
		c := dialRaw(t, addr)
		c.write("GET / HTTP/1.1\r\nHost: x\r\nContent-Len")
		c.close() // vanish mid-header
	}

	// The server must still answer normally afterwards.
	out := rawExchange(t, addr, "GET / HTTP/1.1\r\nHost: x\r\nConnection: close\r\n\r\n")
	if !strings.Contains(out, "ok") {
		t.Fatalf("server unhealthy after aborted requests: %q", truncate(out, 120))
	}
}

func TestHTTPServerCloseIsIdempotent(t *testing.T) {
	srv := &Server{
		Addr:    freeAddr(t),
		Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}),
	}
	go func() { _ = srv.ListenAndServe() }()
	waitReady(t, srv.Addr)

	if err := srv.Close(); err != nil {
		t.Fatalf("first Close: %v", err)
	}
	if err := srv.Close(); err != nil {
		t.Fatalf("second Close: %v", err)
	}
	assertPortClosed(t, srv.Addr)
}

// TestHTTPCloseRacingStartup closes the server the instant it starts accepting.
// The listening socket is bound before ListenAndServe publishes its state, so a
// Close landing in that window must still tear the listener down rather than
// leaving a socket that accepts connections and never answers.
func TestHTTPCloseRacingStartup(t *testing.T) {
	for i := 0; i < 25; i++ {
		srv := &Server{
			Addr:    freeAddr(t),
			Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}),
		}
		started := make(chan struct{})
		go func() {
			close(started)
			_ = srv.ListenAndServe()
		}()
		<-started
		if err := srv.Close(); err != nil {
			t.Fatalf("iteration %d: Close: %v", i, err)
		}
		assertPortClosed(t, srv.Addr)
	}
}

// assertPortClosed fails unless connecting to addr is refused, or the connection
// is immediately reset. A socket that accepts and then stalls is the failure
// mode this guards against.
func assertPortClosed(t *testing.T, addr string) {
	t.Helper()
	c, err := net.DialTimeout("tcp", addr, 2*time.Second)
	if err != nil {
		return // refused: the listener is gone
	}
	defer c.Close()
	_ = c.SetDeadline(time.Now().Add(time.Second))
	if _, err := io.WriteString(c, "GET / HTTP/1.1\r\nHost: x\r\n\r\n"); err != nil {
		return // reset
	}
	n, err := c.Read(make([]byte, 64))
	if err == nil || isTimeout(err) {
		t.Fatalf("%s still has a live listener after Close (read %d bytes, err=%v)", addr, n, err)
	}
}

// defaultMuxOnce keeps the global registration idempotent under -count>1.
var defaultMuxOnce sync.Once

func TestHTTPDefaultHandlerIsServeMux(t *testing.T) {
	// A Server with no Handler falls back to http.DefaultServeMux.
	pattern := "/fnet-default-mux-test"
	defaultMuxOnce.Do(func() {
		http.HandleFunc(pattern, func(w http.ResponseWriter, r *http.Request) {
			_, _ = io.WriteString(w, "from-default-mux")
		})
	})

	srv := &Server{}
	addr := startServer(t, srv)
	out := rawExchange(t, addr, "GET "+pattern+" HTTP/1.1\r\nHost: x\r\nConnection: close\r\n\r\n")
	if !strings.Contains(out, "from-default-mux") {
		t.Fatalf("DefaultServeMux not used: %q", truncate(out, 200))
	}
}

// ---------------------------------------------------------------------------
// responseWriter unit tests (no sockets involved)
// ---------------------------------------------------------------------------

func TestResponseWriterContentLengthAuto(t *testing.T) {
	var buf bytes.Buffer
	w := newResponseWriter(&bufferConn{buf: &buf}, nil, nil)
	_, _ = io.WriteString(w, "hello")
	if err := w.finish(); err != nil {
		t.Fatalf("finish: %v", err)
	}
	out := buf.String()
	if !strings.Contains(out, "Content-Length: 5\r\n") {
		t.Errorf("missing auto Content-Length:\n%s", out)
	}
	if !strings.HasSuffix(out, "\r\n\r\nhello") {
		t.Errorf("body not appended after the header block:\n%q", out)
	}
}

func TestResponseWriterSwitchesToChunkedPastBuffer(t *testing.T) {
	var buf bytes.Buffer
	w := newResponseWriter(&bufferConn{buf: &buf}, nil, nil)

	// Stay inside the 64 KiB buffer, then cross it: the writer must abandon
	// Content-Length and flush what it had as chunks.
	first := bytes.Repeat([]byte("a"), 60*1024)
	second := bytes.Repeat([]byte("b"), 10*1024)
	if _, err := w.Write(first); err != nil {
		t.Fatalf("first write: %v", err)
	}
	if buf.Len() != 0 {
		t.Fatalf("writer flushed %d bytes before exceeding the buffer", buf.Len())
	}
	if _, err := w.Write(second); err != nil {
		t.Fatalf("second write: %v", err)
	}
	if err := w.finish(); err != nil {
		t.Fatalf("finish: %v", err)
	}

	out := buf.String()
	if !strings.Contains(out, "Transfer-Encoding: chunked\r\n") {
		t.Fatalf("expected chunked framing:\n%s", truncate(out, 300))
	}
	if strings.Contains(out, "Content-Length:") {
		t.Errorf("chunked response also advertised Content-Length:\n%s", truncate(out, 300))
	}
	if !strings.HasSuffix(out, "0\r\n\r\n") {
		t.Errorf("missing terminating chunk: ...%q", out[max(0, len(out)-32):])
	}
	// Chunk sizes are hex: 60 KiB = 0xf000, 10 KiB = 0x2800.
	for _, want := range []string{"f000\r\n", "2800\r\n"} {
		if !strings.Contains(out, want) {
			t.Errorf("missing chunk size %q", want)
		}
	}
}

func TestResponseWriterBodylessStatuses(t *testing.T) {
	for _, status := range []int{http.StatusNoContent, http.StatusNotModified, http.StatusContinue} {
		t.Run(http.StatusText(status), func(t *testing.T) {
			var buf bytes.Buffer
			w := newResponseWriter(&bufferConn{buf: &buf}, nil, nil)
			w.WriteHeader(status)
			if _, err := io.WriteString(w, "must-be-dropped"); err != nil {
				t.Fatalf("write: %v", err)
			}
			if err := w.finish(); err != nil {
				t.Fatalf("finish: %v", err)
			}
			out := buf.String()
			if strings.Contains(out, "must-be-dropped") {
				t.Errorf("status %d emitted a body:\n%q", status, out)
			}
			if strings.Contains(out, "Content-Length:") {
				t.Errorf("status %d emitted Content-Length:\n%q", status, out)
			}
			if !strings.HasSuffix(out, "\r\n\r\n") {
				t.Errorf("status %d header block not terminated:\n%q", status, out)
			}
		})
	}
}

func TestResponseWriterHeadKeepsContentLength(t *testing.T) {
	var buf bytes.Buffer
	w := newResponseWriter(&bufferConn{buf: &buf}, nil, nil)
	w.isHead = true
	if _, err := io.WriteString(w, "hello world"); err != nil {
		t.Fatalf("write: %v", err)
	}
	if err := w.finish(); err != nil {
		t.Fatalf("finish: %v", err)
	}
	out := buf.String()
	if !strings.Contains(out, "Content-Length: 11\r\n") {
		t.Errorf("HEAD response lost Content-Length:\n%q", out)
	}
	if strings.Contains(out, "hello world") {
		t.Errorf("HEAD response emitted a body:\n%q", out)
	}
}

func TestResponseWriterWriteAfterHijack(t *testing.T) {
	var buf bytes.Buffer
	w := newResponseWriter(&bufferConn{buf: &buf}, nil, nil)
	if _, _, err := w.Hijack(); err != nil {
		t.Fatalf("Hijack: %v", err)
	}
	if !w.hijacked {
		t.Fatal("Hijacked() = false after Hijack()")
	}
	if _, err := w.Write([]byte("x")); err == nil {
		t.Fatal("Write after Hijack succeeded, want an error")
	}
	if _, _, err := w.Hijack(); err == nil {
		t.Fatal("second Hijack succeeded, want an error")
	}
	if err := w.finish(); err != nil {
		t.Fatalf("finish after Hijack: %v", err)
	}
}

func TestResponseWriterCloseHeader(t *testing.T) {
	for _, closeConn := range []bool{false, true} {
		var buf bytes.Buffer
		w := newResponseWriter(&bufferConn{buf: &buf}, nil, nil)
		w.closeConn = closeConn
		_ = w.finish()
		want := "Connection: keep-alive\r\n"
		if closeConn {
			want = "Connection: close\r\n"
		}
		if !strings.Contains(buf.String(), want) {
			t.Errorf("SetClose(%v) produced:\n%q", closeConn, buf.String())
		}
	}
}

// ---------------------------------------------------------------------------
// Advanced HTTP Robustness & Edge Cases
// ---------------------------------------------------------------------------

func TestHTTPMissingHostHeaderRFC9112(t *testing.T) {
	var reached atomic.Int32
	addr := startHTTPFunc(t, func(w http.ResponseWriter, r *http.Request) {
		reached.Add(1)
		_, _ = io.WriteString(w, "ok:"+r.Proto)
	})

	t.Run("http1.1-missing-host-rejected", func(t *testing.T) {
		out := rawExchange(t, addr, "GET / HTTP/1.1\r\nConnection: close\r\n\r\n")
		if strings.Contains(out, "ok:") {
			t.Fatalf("server answered HTTP/1.1 request lacking Host: %q", out)
		}
	})

	t.Run("http1.0-missing-host-accepted", func(t *testing.T) {
		out := rawExchange(t, addr, "GET / HTTP/1.0\r\n\r\n")
		if !strings.Contains(out, "ok:HTTP/1.0") {
			t.Fatalf("server rejected valid HTTP/1.0 request lacking Host: %q", out)
		}
	})
}

func TestHTTPTransferEncodingUnsupported(t *testing.T) {
	var reached atomic.Int32
	addr := startHTTPFunc(t, func(w http.ResponseWriter, r *http.Request) {
		reached.Add(1)
		_, _ = io.WriteString(w, "should-not-reach")
	})

	cases := []struct{ name, header string }{
		{"unsupported-compress", "Transfer-Encoding: compress"},
		{"unsupported-deflate", "Transfer-Encoding: deflate"},
		{"unsupported-unknown", "Transfer-Encoding: bogus"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			raw := "POST / HTTP/1.1\r\nHost: x\r\n" + tc.header + "\r\nConnection: close\r\n\r\nbody"
			out := rawExchange(t, addr, raw)
			if strings.Contains(out, "should-not-reach") {
				t.Fatalf("server accepted unsupported %s: %q", tc.name, out)
			}
		})
	}
}

func TestHTTPReadTimeoutDuringBody(t *testing.T) {
	readErrCh := make(chan error, 1)
	srv := &Server{
		ReadTimeout: 150 * time.Millisecond,
		Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			_, err := io.ReadAll(r.Body)
			readErrCh <- err
		}),
	}
	addr := startServer(t, srv)

	c := dialRaw(t, addr)
	defer c.close()

	// Send complete header declaring 100 bytes of body, but send only 5 bytes
	c.write("POST /slow HTTP/1.1\r\nHost: x\r\nContent-Length: 100\r\n\r\n12345")

	select {
	case err := <-readErrCh:
		if err == nil {
			t.Fatal("expected read timeout error, got nil")
		}
	case <-time.After(3 * time.Second):
		t.Fatal("handler never returned or timed out")
	}
}

func TestHTTPWriteTimeoutSlowClient(t *testing.T) {
	writeErrCh := make(chan error, 1)
	srv := &Server{
		WriteTimeout: 100 * time.Millisecond,
		Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			big := bytes.Repeat([]byte("A"), 64*1024)
			var writeErr error
			for i := 0; i < 500; i++ {
				if _, err := w.Write(big); err != nil {
					writeErr = err
					break
				}
			}
			writeErrCh <- writeErr
		}),
	}
	addr := startServer(t, srv)

	c, err := net.DialTimeout("tcp", addr, testDialTimeout)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	// Write request, then stop reading altogether so socket buffer fills up
	_, _ = io.WriteString(c, "GET / HTTP/1.1\r\nHost: x\r\nConnection: close\r\n\r\n")

	select {
	case err := <-writeErrCh:
		if err == nil {
			t.Log("all writes completed before socket buffer filled")
		} else {
			t.Logf("WriteTimeout triggered as expected: %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("handler hung or WriteTimeout was ignored")
	}
}

func TestHTTPSlowClientDoesNotBlockFastClient(t *testing.T) {
	addr := startHTTPFunc(t, func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.WriteString(w, "fast-ok")
	})

	// Start a slow client that sends one byte every 20ms
	slowConn, err := net.DialTimeout("tcp", addr, testDialTimeout)
	if err != nil {
		t.Fatal(err)
	}
	defer slowConn.Close()

	stopSlow := make(chan struct{})
	go func() {
		msg := "GET /slow HTTP/1.1\r\nHost: x\r\n"
		for i := 0; i < len(msg); i++ {
			select {
			case <-stopSlow:
				return
			default:
				_, _ = slowConn.Write([]byte{msg[i]})
				time.Sleep(15 * time.Millisecond)
			}
		}
	}()
	defer close(stopSlow)

	// While the slow client is sending, normal clients must be served quickly
	client := newClient(t, nil)
	for i := 0; i < 5; i++ {
		start := time.Now()
		resp, err := client.Get("http://" + addr + "/fast")
		if err != nil {
			t.Fatalf("fast client %d failed: %v", i, err)
		}
		body := readBody(t, resp)
		if body != "fast-ok" {
			t.Fatalf("body = %q, want fast-ok", body)
		}
		if dur := time.Since(start); dur > 500*time.Millisecond {
			t.Fatalf("fast client took %v, reactor may be blocked by slow client", dur)
		}
	}
}

func TestHTTPKeepAliveHighLoad(t *testing.T) {
	addr := startHTTPFunc(t, func(w http.ResponseWriter, r *http.Request) {
		seq := r.Header.Get("X-Seq")
		w.Header().Set("X-Echo-Seq", seq)
		fmt.Fprintf(w, "ack-%s", seq)
	})

	c := dialRaw(t, addr)
	defer c.close()

	const count = 300
	for i := 0; i < count; i++ {
		seqStr := strconv.Itoa(i)
		req := fmt.Sprintf("GET /seq HTTP/1.1\r\nHost: x\r\nX-Seq: %s\r\n\r\n", seqStr)
		c.write(req)
		resp := c.readResponse("GET")
		if got := resp.Header.Get("X-Echo-Seq"); got != seqStr {
			t.Fatalf("request %d: header seq = %q, want %q", i, got, seqStr)
		}
		if got := readBody(t, resp); got != "ack-"+seqStr {
			t.Fatalf("request %d: body = %q, want ack-%s", i, got, seqStr)
		}
	}
}

func TestHTTPRangeRequest(t *testing.T) {
	content := []byte("0123456789abcdefghijklmnopqrstuvwxyz")
	addr := startHTTPFunc(t, func(w http.ResponseWriter, r *http.Request) {
		http.ServeContent(w, r, "test.txt", time.Time{}, bytes.NewReader(content))
	})

	c := dialRaw(t, addr)
	defer c.close()

	c.write("GET /test.txt HTTP/1.1\r\nHost: x\r\nRange: bytes=0-9\r\nConnection: close\r\n\r\n")
	resp := c.readResponse("GET")
	if resp.StatusCode != http.StatusPartialContent {
		t.Fatalf("status = %d, want 206", resp.StatusCode)
	}
	if got := resp.Header.Get("Content-Range"); got != fmt.Sprintf("bytes 0-9/%d", len(content)) {
		t.Errorf("Content-Range = %q, want bytes 0-9/%d", got, len(content))
	}
	if got := readBody(t, resp); got != "0123456789" {
		t.Fatalf("body = %q, want 0123456789", got)
	}
}

func TestHTTPMultipleSetCookieHeaders(t *testing.T) {
	addr := startHTTPFunc(t, func(w http.ResponseWriter, r *http.Request) {
		http.SetCookie(w, &http.Cookie{
			Name:     "auth",
			Value:    "token123",
			Path:     "/",
			HttpOnly: true,
			Secure:   true,
			SameSite: http.SameSiteStrictMode,
		})
		http.SetCookie(w, &http.Cookie{
			Name:   "theme",
			Value:  "dark",
			Path:   "/ui",
			MaxAge: 3600,
		})
		http.SetCookie(w, &http.Cookie{
			Name:  "pref",
			Value: "lang=zh",
			Path:  "/",
		})
		_, _ = io.WriteString(w, "cookie-test")
	})

	resp, err := newClient(t, nil).Get("http://" + addr + "/cookie")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()

	cookies := resp.Cookies()
	if len(cookies) != 3 {
		t.Fatalf("got %d cookies, want 3", len(cookies))
	}
	cookieMap := make(map[string]*http.Cookie)
	for _, ck := range cookies {
		cookieMap[ck.Name] = ck
	}

	auth := cookieMap["auth"]
	if auth == nil || auth.Value != "token123" || !auth.HttpOnly || !auth.Secure || auth.SameSite != http.SameSiteStrictMode {
		t.Errorf("auth cookie mismatch: %+v", auth)
	}
	theme := cookieMap["theme"]
	if theme == nil || theme.Value != "dark" || theme.MaxAge != 3600 || theme.Path != "/ui" {
		t.Errorf("theme cookie mismatch: %+v", theme)
	}
	pref := cookieMap["pref"]
	if pref == nil || pref.Value != "lang=zh" {
		t.Errorf("pref cookie mismatch: %+v", pref)
	}
}

func TestHTTPHandlerPanicRecovery(t *testing.T) {
	var panicCount atomic.Int32
	addr := startHTTPFunc(t, func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/panic" {
			panicCount.Add(1)
			panic("intentional test panic")
		}
		_, _ = io.WriteString(w, "healthy")
	})

	// 1. Send request that causes a panic; connection must be closed without crashing server
	out := rawExchange(t, addr, "GET /panic HTTP/1.1\r\nHost: x\r\nConnection: close\r\n\r\n")
	if strings.Contains(out, "healthy") {
		t.Fatalf("unexpected response from panicking handler: %q", out)
	}

	if panicCount.Load() != 1 {
		t.Fatalf("panic handler was not executed")
	}

	// 2. Normal requests must still be served normally
	c := dialRaw(t, addr)
	defer c.close()
	c.write("GET /health HTTP/1.1\r\nHost: x\r\nConnection: close\r\n\r\n")
	resp := c.readResponse("GET")
	if got := readBody(t, resp); got != "healthy" {
		t.Fatalf("server unhealthy after handler panic: got %q, want healthy", got)
	}
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n] + fmt.Sprintf("...(%d bytes total)", len(s))
}

// TestHTTPClientHalfCloseStillAnswered: a client may send its request and shut
// down its write side (half-close); the response must still arrive.
func TestHTTPClientHalfCloseStillAnswered(t *testing.T) {
	addr := startHTTPFunc(t, func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(50 * time.Millisecond) // the FIN lands while the handler runs
		_, _ = io.WriteString(w, "still-here")
	})
	c, err := net.DialTimeout("tcp", addr, testDialTimeout)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	_, _ = io.WriteString(c, "GET / HTTP/1.1\r\nHost: x\r\n\r\n")
	if err := c.(*net.TCPConn).CloseWrite(); err != nil {
		t.Fatal(err)
	}
	_ = c.SetReadDeadline(time.Now().Add(testIOTimeout))
	resp, err := http.ReadResponse(bufio.NewReader(c), &http.Request{Method: http.MethodGet})
	if err != nil {
		t.Fatalf("no response after half-close: %v", err)
	}
	if got := readBody(t, resp); got != "still-here" {
		t.Fatalf("body = %q", got)
	}
}

// TestHTTPLargeUnreadBodyClosesConnection: a handler that ignores a large body
// must not make the server read it all to keep the connection alive.
func TestHTTPLargeUnreadBodyClosesConnection(t *testing.T) {
	addr := startHTTPFunc(t, func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.WriteString(w, "ignored")
	})
	c := dialRaw(t, addr)
	defer c.close()
	const size = 4 << 20
	c.write(fmt.Sprintf("POST / HTTP/1.1\r\nHost: x\r\nContent-Length: %d\r\n\r\n", size))
	go func() { _, _ = c.c.Write(make([]byte, size)) }()
	resp := c.readResponse("POST")
	if got := readBody(t, resp); got != "ignored" {
		t.Fatalf("body = %q", got)
	}
	_ = c.c.SetReadDeadline(time.Now().Add(testIOTimeout))
	if _, err := c.br.ReadByte(); err == nil || isTimeout(err) {
		t.Fatalf("connection stayed open behind a %d-byte unread body (err=%v)", size, err)
	}
}

// TestHTTP10LargeResponseNotChunked: HTTP/1.0 has no chunked encoding, so a
// body too large to buffer is delimited by closing the connection.
func TestHTTP10LargeResponseNotChunked(t *testing.T) {
	payload := strings.Repeat("z", 200<<10)
	addr := startHTTPFunc(t, func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.WriteString(w, payload)
	})
	out := rawExchange(t, addr, "GET / HTTP/1.0\r\nConnection: keep-alive\r\n\r\n")
	head, body, _ := strings.Cut(out, "\r\n\r\n")
	if strings.Contains(head, "Transfer-Encoding") || !strings.Contains(head, "Connection: close") {
		t.Fatalf("bad HTTP/1.0 framing:\n%s", head)
	}
	if body != payload {
		t.Fatalf("body length %d, want %d", len(body), len(payload))
	}
}

// ---------------------------------------------------------------------------
// Timeouts on the event loop and 100-continue
// ---------------------------------------------------------------------------

// expectClosed fails unless the server closes c within limit.
func expectClosed(t *testing.T, c *rawConn, limit time.Duration, what string) {
	t.Helper()
	start := time.Now()
	_ = c.c.SetReadDeadline(time.Now().Add(limit))
	if _, err := c.br.ReadByte(); err == nil || isTimeout(err) {
		t.Fatalf("%s: connection still open after %v (err=%v)", what, time.Since(start), err)
	}
}

func TestHTTPIdleTimeoutClosesPlaintextKeepAlive(t *testing.T) {
	addr := startServer(t, &Server{
		IdleTimeout: 300 * time.Millisecond,
		Handler:     http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { _, _ = io.WriteString(w, "ok") }),
	})
	c := dialRaw(t, addr)
	defer c.close()
	for i := 0; i < 3; i++ { // requests inside the idle window keep the connection
		c.write("GET / HTTP/1.1\r\nHost: x\r\n\r\n")
		if got := readBody(t, c.readResponse("GET")); got != "ok" {
			t.Fatalf("request %d: body = %q", i, got)
		}
		time.Sleep(150 * time.Millisecond)
	}
	expectClosed(t, c, 2*time.Second, "idle keep-alive")
}

// A header dripped byte by byte keeps the connection busy but must still be
// complete within ReadHeaderTimeout of its first byte (slow-loris).
func TestHTTPHeaderTimeoutBoundsSlowHeader(t *testing.T) {
	var reached atomic.Int32
	addr := startServer(t, &Server{
		ReadHeaderTimeout: 300 * time.Millisecond,
		Handler:           http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { reached.Add(1) }),
	})
	c := dialRaw(t, addr)
	defer c.close()
	go func() {
		for _, b := range []byte("GET / HTTP/1.1\r\nHost: x\r\nX-Slow: " + strings.Repeat("a", 100)) {
			if _, err := c.c.Write([]byte{b}); err != nil {
				return
			}
			time.Sleep(30 * time.Millisecond)
		}
	}()
	expectClosed(t, c, 2*time.Second, "slow header")
	if reached.Load() != 0 {
		t.Fatal("handler ran for an incomplete header")
	}
}

func TestHTTPHeaderTimeoutClosesSilentConnection(t *testing.T) {
	addr := startServer(t, &Server{
		ReadHeaderTimeout: 300 * time.Millisecond,
		Handler:           http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}),
	})
	c := dialRaw(t, addr)
	defer c.close()
	expectClosed(t, c, 2*time.Second, "connection that never sends")
}

// A slow request header does not affect the next request's clock: the
// deadline starts at each request's first byte.
func TestHTTPHeaderTimeoutPerRequest(t *testing.T) {
	addr := startServer(t, &Server{
		ReadHeaderTimeout: 400 * time.Millisecond,
		Handler:           http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { _, _ = io.WriteString(w, "ok") }),
	})
	c := dialRaw(t, addr)
	defer c.close()
	for i := 0; i < 3; i++ {
		time.Sleep(250 * time.Millisecond) // idle, then a header split over 200ms
		c.write("GET / HTTP/1.1\r\n")
		time.Sleep(200 * time.Millisecond)
		c.write("Host: x\r\n\r\n")
		if got := readBody(t, c.readResponse("GET")); got != "ok" {
			t.Fatalf("request %d: body = %q", i, got)
		}
	}
}

// Like curl, the client waits for "100 Continue" before sending the body.
func TestHTTPExpectContinueSendsInterimResponse(t *testing.T) {
	addr := startHTTPFunc(t, func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		_, _ = io.WriteString(w, "got="+string(b))
	})
	c := dialRaw(t, addr)
	defer c.close()
	c.write("POST / HTTP/1.1\r\nHost: x\r\nContent-Length: 5\r\nExpect: 100-continue\r\n\r\n")
	_ = c.c.SetReadDeadline(time.Now().Add(testIOTimeout))
	status, err := c.br.ReadString('\n')
	if err != nil || status != "HTTP/1.1 100 Continue\r\n" {
		t.Fatalf("interim status %q (%v)", status, err)
	}
	if blank, _ := c.br.ReadString('\n'); blank != "\r\n" {
		t.Fatalf("interim response not terminated: %q", blank)
	}
	c.write("hello")
	if got := readBody(t, c.readResponse("POST")); got != "got=hello" {
		t.Fatalf("body = %q", got)
	}
}

// A handler that answers from the headers alone sends no 100, and the
// connection closes: the client may or may not send the body now.
func TestHTTPExpectContinueUnreadBodyCloses(t *testing.T) {
	addr := startHTTPFunc(t, func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "too large", http.StatusRequestEntityTooLarge)
	})
	c := dialRaw(t, addr)
	defer c.close()
	c.write("POST / HTTP/1.1\r\nHost: x\r\nContent-Length: 5\r\nExpect: 100-continue\r\n\r\n")
	resp := c.readResponse("POST")
	if resp.StatusCode != http.StatusRequestEntityTooLarge {
		t.Fatalf("status = %d (a 100 Continue leaked?)", resp.StatusCode)
	}
	_ = readBody(t, resp)
	expectClosed(t, c, 2*time.Second, "unread expect-continue body")
}

// net/http's client waits ExpectContinueTimeout for the 100 before sending
// the body; the server must not make it wait that long.
func TestHTTPExpectContinueWithGoClient(t *testing.T) {
	addr := startHTTPFunc(t, func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		_, _ = w.Write(b)
	})
	client := &http.Client{Transport: &http.Transport{ExpectContinueTimeout: 5 * time.Second}}
	defer client.CloseIdleConnections()
	body := strings.Repeat("x", 4096)
	req, _ := http.NewRequest(http.MethodPost, "http://"+addr+"/", strings.NewReader(body))
	req.Header.Set("Expect", "100-continue")
	start := time.Now()
	resp, err := client.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	if got := readBody(t, resp); got != body {
		t.Fatalf("echoed %d bytes", len(got))
	}
	if el := time.Since(start); el > 2*time.Second {
		t.Fatalf("request took %v: the client waited for a 100 Continue that never came", el)
	}
}

// A handler may close a body the client declared but never sends: the response
// still goes out, and the leftover body cannot hold the worker forever.
func TestHTTPBodyNeverSentDoesNotHoldWorker(t *testing.T) {
	addr := startServer(t, &Server{
		ReadHeaderTimeout: 300 * time.Millisecond, // also bounds draining leftovers
		Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			_ = r.Body.Close()
			_, _ = io.WriteString(w, "done")
		}),
	})
	c := dialRaw(t, addr)
	defer c.close()
	c.write("POST / HTTP/1.1\r\nHost: x\r\nContent-Length: 100\r\n\r\nonly-10-by")
	if got := readBody(t, c.readResponse("POST")); got != "done" {
		t.Fatalf("body = %q", got)
	}
	expectClosed(t, c, 2*time.Second, "connection with a body that never arrives")
}
