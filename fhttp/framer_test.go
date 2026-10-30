package fhttp

import (
	"errors"
	"net/http"
	"slices"
	"strings"
	"testing"
)

// frameAll simulates the engine: data arrives step bytes at a time, and what split leaves is passed again with what
// arrives next, the way conn.onData has it. It returns the messages that were split out, the error a status item stands
// for, and whether a 100 Continue was asked for.
func frameAll(in string, step int) ([]string, error, bool) {
	var f framer
	opts := Options{MaxHeaderBytes: 1 << 10, MaxBufferedBodyBytes: 32}
	var msgs []string
	var buf []byte
	continues := 0
	var stopped error
	push := func(msg []byte, _ message, status int16) {
		switch status {
		case 0:
			msgs = append(msgs, string(msg))
		case http.StatusContinue:
			continues++
		case statusHandoff:
			stopped = errHandoff
		case http.StatusRequestHeaderFieldsTooLarge:
			stopped = errHeaderTooLarge
		case http.StatusRequestURITooLong:
			stopped = errURITooLong
		default:
			stopped = errBadRequest
		}
	}
	for i := 0; i < len(in) && stopped == nil; i += step {
		buf = append(buf, in[i:min(i+step, len(in))]...)
		consumed, _ := f.split(buf, &opts, push)
		buf = buf[consumed:]
	}
	if continues > 1 {
		return msgs, errors.New("100 Continue was requested twice"), true
	}
	return msgs, stopped, continues == 1
}

func TestFramer(t *testing.T) {
	const (
		get     = "GET / HTTP/1.1\r\nHost: a\r\n\r\n"
		post    = "POST / HTTP/1.1\r\nHost: a\r\nContent-Length: 5\r\n\r\nhello"
		chunked = "POST / HTTP/1.1\r\nTransfer-Encoding: chunked\r\n\r\n"
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
		{"body too large", get + "POST / HTTP/1.1\r\nContent-Length: 33\r\n\r\n", []string{get}, errHandoff},
		// A chunked body counts with its framing.
		{"chunked", chunked + "5\r\nhello\r\nB\r\n world 0123\r\n0\r\n\r\n" + get,
			[]string{chunked + "5\r\nhello\r\nB\r\n world 0123\r\n0\r\n\r\n", get}, nil},
		{"chunked empty body", chunked + "0\r\n\r\n", []string{chunked + "0\r\n\r\n"}, nil},
		{"chunked field case-insensitive", "POST / HTTP/1.1\r\ntransfer-encoding:  Chunked\r\n\r\n0\r\n\r\n",
			[]string{"POST / HTTP/1.1\r\ntransfer-encoding:  Chunked\r\n\r\n0\r\n\r\n"}, nil},
		{"chunked not complete", chunked + "5\r\nhello\r\n0\r\n", nil, nil},
		// Earlier messages are split out before the one that goes to net/http, which judges the body itself.
		{"chunked too large", get + chunked + "21\r\n", []string{get}, errHandoff},
		{"chunked too large in total", chunked + "10\r\n0123456789abcdef\r\n10\r\n0123456789abcdef\r\n", nil, errHandoff},
		// Chunk extensions are handed to net/http.
		{"chunk extension", chunked + "5;a=b\r\nhello\r\n0\r\n\r\n", nil, errHandoff},
		{"chunk extension with blanks", chunked + "5 ; a = b\r\nhello\r\n0\r\n\r\n", nil, errHandoff},
		{"chunk extension on the last chunk", chunked + "5\r\nhello\r\n0;x\r\n\r\n", nil, errHandoff},
		{"chunk extension of no end", chunked + "5;" + strings.Repeat("a", 300) + "\r\nhello\r\n0\r\n\r\n", nil, errHandoff},
		{"chunk size with spaces", chunked + "5 \r\nhello\r\n0\r\n\r\n", nil, errHandoff},
		{"chunk without CRLF", chunked + "5\r\nhelloX\r\n0\r\n\r\n", nil, errHandoff},
		{"bare LF after chunk size", chunked + "5\nhello\r\n0\r\n\r\n", nil, errHandoff},
		{"trailer fields", chunked + "0\r\nX: y\r\n\r\n", nil, errHandoff},
		{"chunk size of 7 digits", chunked + "0000005\r\nhello\r\n0\r\n\r\n", []string{chunked + "0000005\r\nhello\r\n0\r\n\r\n"}, nil},
		{"chunk size too long", chunked + "00000005\r\nhello\r\n0\r\n\r\n", nil, errHandoff},
		{"chunked and Content-Length", "POST / HTTP/1.1\r\nContent-Length: 1\r\nTransfer-Encoding: chunked\r\n\r\n", nil, errHandoff},
		{"chunked twice", "POST / HTTP/1.1\r\nTransfer-Encoding: chunked\r\nTransfer-Encoding: chunked\r\n\r\n", nil, errHandoff},
		{"other transfer coding", "POST / HTTP/1.1\r\nTransfer-Encoding: gzip, chunked\r\n\r\n", nil, errHandoff},
		// Unsupported or ambiguous transfer codings are handed to net/http, which validates them and reports the protocol
		// error without fhttp trying to guess the body boundary.
		{"chunked on HTTP/1.0", "POST / HTTP/1.0\r\nTransfer-Encoding: chunked\r\n\r\n0\r\n\r\n", nil, errHandoff},
		{"chunked and Content-Length on HTTP/1.0", "POST / HTTP/1.0\r\nTransfer-Encoding: chunked\r\nContent-Length: 5\r\n\r\nhello", nil, errHandoff},
		{"chunked in the last line only", "POST / HTTP/1.1\r\nTransfer-Encoding: gzip\r\nTransfer-Encoding: chunked\r\n\r\n", nil, errHandoff},
		// A Transfer-Encoding whose last coding is not chunked leaves the length of the body unknown: 400 (RFC 9112 6.3), and
		// not net/http's 501, whatever else would have the request handed over.
		{"no chunked", "POST / HTTP/1.1\r\nTransfer-Encoding: gzip\r\n\r\n", nil, errBadRequest},
		{"chunked not last", "POST / HTTP/1.1\r\nTransfer-Encoding: chunked, gzip\r\n\r\n", nil, errBadRequest},
		{"chunked not in the last line", "POST / HTTP/1.1\r\nTransfer-Encoding: chunked\r\nTransfer-Encoding: gzip\r\n\r\n", nil, errBadRequest},
		{"no chunked on HTTP/1.0", "POST / HTTP/1.0\r\nTransfer-Encoding: gzip\r\n\r\n", nil, errBadRequest},
		{"no chunked and an event stream", "GET / HTTP/1.1\r\nAccept: text/event-stream\r\nTransfer-Encoding: gzip\r\n\r\n", nil, errBadRequest},
		// A Content-Length that is no number, or differs from another one, is answered by fhttp, with a Date.
		{"invalid Content-Length", "POST / HTTP/1.1\r\nContent-Length: -1\r\n\r\n", nil, errBadRequest},
		{"empty Content-Length", "POST / HTTP/1.1\r\nContent-Length: \r\n\r\n", nil, errBadRequest},
		{"Content-Length list", "POST / HTTP/1.1\r\nContent-Length: 1, 1\r\n\r\nx", nil, errBadRequest},
		{"differing Content-Length", "POST / HTTP/1.1\r\nContent-Length: 1\r\nContent-Length: 2\r\n\r\nxy", nil, errBadRequest},
		{"duplicate Content-Length", "POST / HTTP/1.1\r\nContent-Length: 1\r\nContent-Length: 1\r\n\r\nx", nil, errHandoff},
		{"duplicate large Content-Length", "POST / HTTP/1.1\r\nContent-Length: 99\r\nContent-Length: 99\r\n\r\n", nil, errHandoff},
		{"other Expect", "POST / HTTP/1.1\r\nExpect: foo\r\n\r\n", nil, errHandoff},
		// An event stream is answered by net/http, which sends it as it is written.
		{"event stream", get + "GET /events HTTP/1.1\r\naccept: text/html, text/event-stream;q=0.9\r\n\r\n", []string{get}, errHandoff},
		{"event stream in another case", "GET / HTTP/1.1\r\nAccept: a-b, TEXT/Event-Stream\r\n\r\n", nil, errHandoff},
		{"other stream", "GET / HTTP/1.1\r\nAccept: application/octet-stream\r\nX: text/event-stream\r\n\r\n",
			[]string{"GET / HTTP/1.1\r\nAccept: application/octet-stream\r\nX: text/event-stream\r\n\r\n"}, nil},
		{"header too large and not ended", "GET / HTTP/1.1\r\nX: " + strings.Repeat("a", 1100), nil, errHeaderTooLarge},
		{"header too large", "GET / HTTP/1.1\r\nX: " + strings.Repeat("a", 1100) + "\r\n\r\n", nil, errHeaderTooLarge},
		{"request line too long and not ended", "GET /" + strings.Repeat("a", 1100), nil, errURITooLong},
		{"request line too long", "GET /" + strings.Repeat("a", 1100) + " HTTP/1.1\r\n\r\n", nil, errURITooLong},
		// The header must end in CRLF CRLF: one ending in an empty line made of a bare LF is rejected once it arrives.
		{"header ended by bare LFs", get + "GET / HTTP/1.1\nHost: a\n\n", []string{get}, errBadRequest},
		{"header ended by CRLF after a bare LF", "GET / HTTP/1.1\r\nHost: a\n\r\n", nil, errBadRequest},
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

// TestChunkHeader checks the size line of a plain chunk.
func TestChunkHeader(t *testing.T) {
	for _, line := range []string{"5\r\n", "a\r\n", "A0\r\n", "0\r\n"} {
		size, n, ok := chunkHeader([]byte(line + "data"))
		if wantSize := map[byte]int{'5': 5, 'a': 10, 'A': 160, '0': 0}[line[0]]; !ok || n != len(line) || size != wantSize {
			t.Errorf("%q: size %d, length %d, ok=%v; want %d, %d, true", line, size, n, ok, wantSize, len(line))
		}
		for i := range len(line) {
			if _, n, ok := chunkHeader([]byte(line[:i])); n != 0 || !ok {
				t.Errorf("%q: length %d, ok=%v; want a line not in full yet", line[:i], n, ok)
			}
		}
	}
	for _, line := range []string{"\r\n", ";a\r\n", "x\r\n", "5x\r\n", "5 \r\n", "5\t\r\n", "5;a=b\r\n", "000000005\r\n"} {
		if _, n, ok := chunkHeader([]byte(line)); ok && n != 0 {
			t.Errorf("%q: read as a line of %d bytes, want it refused", line, n)
		}
	}
}

// TestFramerExpectContinue checks that 100 Continue is asked for only while the body has not arrived, and never for
// HTTP/1.0 or a request without a body.
func TestFramerExpectContinue(t *testing.T) {
	const (
		header        = "POST / HTTP/1.1\r\nExpect: 100-continue\r\nContent-Length: 1\r\n\r\n"
		chunkedHeader = "POST / HTTP/1.1\r\nExpect: 100-continue\r\nTransfer-Encoding: chunked\r\n\r\n"
	)
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
		{"HTTP/1.0 with a bare LF", strings.Replace(header, "HTTP/1.1\r\n", "HTTP/1.0\n", 1), len(header), false},
		{"no request body", "GET / HTTP/1.1\r\nExpect: 100-continue\r\n\r\n", 100, false},
		{"chunked body not arrived", chunkedHeader, len(chunkedHeader), true},
		{"chunked body arrives with the header", chunkedHeader + "0\r\n\r\n", 100, false},
	}
	for _, tt := range tests {
		if _, _, got := frameAll(tt.in, tt.step); got != tt.want {
			t.Errorf("%s: expectContinue=%v, want %v", tt.name, got, tt.want)
		}
	}
}

// TestFramerFieldLines checks that a header may have maxHeaderValueCount field lines, and no more.
func TestFramerFieldLines(t *testing.T) {
	opts := Options{}.withDefaults()
	for _, tt := range []struct {
		lines int
		err   error
	}{
		{maxHeaderValueCount, nil},
		{maxHeaderValueCount + 1, errHeaderTooLarge},
	} {
		req := []byte("GET / HTTP/1.1\r\n" + strings.Repeat("A:\r\n", tt.lines) + "\r\n")
		var f framer
		if m, _, err := f.next(req, &opts); err != tt.err || err == nil && m.size != len(req) {
			t.Errorf("%d field lines: got size %d, err=%v; want size %d, err=%v", tt.lines, m.size, err, len(req), tt.err)
		}
	}
}

func TestParseLength(t *testing.T) {
	for _, tt := range []struct {
		value string
		n     uint64
		ok    bool
	}{
		{"0", 0, true},
		{" 10\t\r", 10, true},
		{"1a", 0, false},
		{"+1", 0, false},
		{"-1", 0, false},
		{"", 0, false},
		{"99999999999999999999999", 0, false}, // overflow
		{"9223372036854775807", 1<<63 - 1, true},
	} {
		if n, ok := parseLength([]byte(tt.value)); ok != tt.ok || ok && n != tt.n { // n means nothing when !ok
			t.Errorf("parseLength(%q) = %d, %v; want %d, %v", tt.value, n, ok, tt.n, tt.ok)
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
		if m, _, _ := f.next(req, &opts); m.size != len(req) {
			b.Fatal(m)
		}
	}
}
