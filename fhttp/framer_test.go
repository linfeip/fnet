package fhttp

import (
	"errors"
	"slices"
	"strings"
	"testing"

	"github.com/linfeip/fnet/internal/units"
)

// frameAll simulates the engine: data arrives step bytes at a time. It returns the messages that were
// split out, the error it ran into, and whether a 100 Continue was asked for.
func frameAll(in string, step int) ([]string, error, bool) {
	var f framer
	opts := Options{MaxHeaderBytes: units.KB, MaxBufferedBodyBytes: 16}
	var msgs []string
	var buf []byte
	expectContinue := false
	for i := 0; i < len(in); i += step {
		buf = append(buf, in[i:min(i+step, len(in))]...)
		for {
			n, expect, err := f.next(buf, &opts)
			if err != nil {
				return msgs, err, expectContinue
			}
			if expect {
				if expectContinue {
					return msgs, errors.New("100 Continue was requested twice"), true
				}
				expectContinue = true
			}
			if n == 0 {
				break
			}
			msgs = append(msgs, string(buf[:n]))
			buf = buf[n:]
		}
	}
	return msgs, nil, expectContinue
}

func TestFramer(t *testing.T) {
	const (
		get  = "GET / HTTP/1.1\r\nHost: a\r\n\r\n"
		post = "POST / HTTP/1.1\r\nHost: a\r\nContent-Length: 5\r\n\r\nhello"
	)
	tests := []struct {
		name string
		in   string
		want []string
		err  error
	}{
		{"single request", get, []string{get}, nil},
		{"pipelining", get + get + get, []string{get, get, get}, nil},
		{"body split out by Content-Length", post + get + post, []string{post, get, post}, nil},
		{"body not complete", post[:len(post)-1], nil, nil},
		{"field name case-insensitive", "POST / HTTP/1.1\r\ncontent-LENGTH:\t3 \r\n\r\nabc", []string{"POST / HTTP/1.1\r\ncontent-LENGTH:\t3 \r\n\r\nabc"}, nil},
		{"Content-Length: 0", "POST / HTTP/1.1\r\nContent-Length: 0\r\n\r\n" + get, []string{"POST / HTTP/1.1\r\nContent-Length: 0\r\n\r\n", get}, nil},
		// Not recognized as Content-Length: the message ends at the header, and conn.handle rejects it once the
		// standard library reads a body length out of it.
		{"space before the colon", "POST / HTTP/1.1\r\nContent-Length : 5\r\n\r\n", []string{"POST / HTTP/1.1\r\nContent-Length : 5\r\n\r\n"}, nil},
		{"Expect: 100-continue", "POST / HTTP/1.1\r\nExpect: 100-Continue\r\nContent-Length: 1\r\n\r\nx", []string{"POST / HTTP/1.1\r\nExpect: 100-Continue\r\nContent-Length: 1\r\n\r\nx"}, nil},
		// Earlier messages are split out before the one that goes to net/http.
		{"body too large", get + "POST / HTTP/1.1\r\nContent-Length: 17\r\n\r\n", []string{get}, errStreamBody},
		{"chunked", "POST / HTTP/1.1\r\nTransfer-Encoding: chunked\r\n\r\n", nil, errStreamBody},
		{"invalid Content-Length", "POST / HTTP/1.1\r\nContent-Length: -1\r\n\r\n", nil, errStreamBody},
		{"empty Content-Length", "POST / HTTP/1.1\r\nContent-Length: \r\n\r\n", nil, errStreamBody},
		{"duplicate Content-Length", "POST / HTTP/1.1\r\nContent-Length: 1\r\nContent-Length: 1\r\n\r\nx", nil, errStreamBody},
		{"other Expect", "POST / HTTP/1.1\r\nExpect: foo\r\n\r\n", nil, errStreamBody},
		{"header too large and not ended", "GET / HTTP/1.1\r\nX: " + strings.Repeat("a", 1100), nil, errHeaderTooLarge},
		{"header too large", "GET / HTTP/1.1\r\nX: " + strings.Repeat("a", 1100) + "\r\n\r\n", nil, errHeaderTooLarge},
	}
	for _, tt := range tests {
		for _, step := range []int{len(tt.in), 1, 7} {
			got, err, _ := frameAll(tt.in, step)
			if err != tt.err || !slices.Equal(got, tt.want) {
				t.Errorf("%s(step=%d): got %q, err=%v; want %q, err=%v", tt.name, step, got, err, tt.want, tt.err)
			}
		}
	}
}

// TestFramerExpectContinue checks that 100 Continue is asked for only while the body has not arrived, and never for
// HTTP/1.0 or a request without a body.
func TestFramerExpectContinue(t *testing.T) {
	const header = "POST / HTTP/1.1\r\nExpect: 100-continue\r\nContent-Length: 1\r\n\r\n"
	tests := []struct {
		name string
		in   string
		step int
		want bool
	}{
		{"body not arrived", header, len(header), true},
		{"body arrives separately", header + "x", len(header), true},
		{"body arrives with the header", header + "x", len(header) + 1, false},
		{"HTTP/1.0", strings.Replace(header, "HTTP/1.1", "HTTP/1.0", 1), len(header), false},
		{"no request body", "GET / HTTP/1.1\r\nExpect: 100-continue\r\n\r\n", 100, false},
	}
	for _, tt := range tests {
		if _, _, got := frameAll(tt.in, tt.step); got != tt.want {
			t.Errorf("%s: expectContinue=%v, want %v", tt.name, got, tt.want)
		}
	}
}

func TestParseLength(t *testing.T) {
	for _, tt := range []struct {
		value string
		max   int
		n     int
		ok    bool
	}{
		{"0", 10, 0, true},
		{" 10\t\r", 10, 10, true},
		{"11", 10, 0, false},
		{"1a", 10, 0, false},
		{"+1", 10, 0, false},
		{"", 10, 0, false},
		{"99999999999999999999999", 1<<63 - 1, 0, false}, // overflow
		{"9223372036854775807", 1<<63 - 1, 1<<63 - 1, true},
	} {
		if n, ok := parseLength([]byte(tt.value), tt.max); ok != tt.ok || ok && n != tt.n { // n means nothing when !ok
			t.Errorf("parseLength(%q, %d) = %d, %v; want %d, %v", tt.value, tt.max, n, ok, tt.n, tt.ok)
		}
	}
}

// BenchmarkFramer measures splitting a typical browser GET, the hot path every request takes.
func BenchmarkFramer(b *testing.B) {
	req := []byte("GET /index.html HTTP/1.1\r\nHost: example.com\r\nConnection: keep-alive\r\nCache-Control: max-age=0\r\n" +
		"User-Agent: Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/129.0 Safari/537.36\r\n" +
		"Accept: text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8\r\n" +
		"Accept-Encoding: gzip, deflate, br\r\nAccept-Language: en-US,en;q=0.9\r\nCookie: session=0123456789abcdef\r\n\r\n")
	opts := Options{}.withDefaults()
	var f framer
	b.SetBytes(int64(len(req)))
	for range b.N {
		if n, _, _ := f.next(req, &opts); n != len(req) {
			b.Fatal(n)
		}
	}
}
