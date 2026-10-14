package fhttp

import (
	"bytes"
	"errors"
	"strconv"
)

var (
	// errHeaderTooLarge means the request line and headers exceed MaxHeaderBytes.
	errHeaderTooLarge = errors.New("fhttp: request header too large")
	// errStreamBody means the request body is not buffered by fhttp: it is larger than MaxBufferedBodyBytes, or framed
	// in a way only the standard library should judge; the connection is handed over to net/http.
	errStreamBody = errors.New("fhttp: request body is streamed")
)

// headerTerminator is the terminator of the request headers.
var headerTerminator = []byte("\r\n\r\n")

// message is a complete request message the framer split out.
type message struct {
	size    int  // the length of the whole message: request line, headers and body
	header  int  // the length of the request line and headers, terminator included
	chunked bool // the body is chunked, and size includes its framing
}

// framer splits request messages inside fnet's callbacks. It only finds where a message ends: the header terminator,
// then a body of Content-Length bytes or of chunks. The message is handed as a whole to parseRequest (or the standard
// library's http.ReadRequest), which parses it and decodes the body. Every connection holds one, so the limits are not
// copied in, and the offsets are int32 (see maxLimit).
type framer struct {
	header int32 // the length of the current message's header once it has arrived, 0 before
	pos    int32 // before the header has arrived: the bytes known to lack its terminator; in a chunked body: the next chunk
	size   int32 // the length of a message with a Content-Length body, -1 while a chunked body arrives
}

// next looks for the end of the message starting at the beginning of data, within the limits of opts: m.size>0 means
// data[:m.size] is a complete message, m.size==0 means there is not enough data and next must be called again with
// the same message start once more data arrives. expectContinue is reported once, when the header is complete and the
// client waits for 100 Continue before sending the body. Errors leave the framer at the start of that message.
func (f *framer) next(data []byte, opts *Options) (m message, expectContinue bool, err error) {
	if f.header == 0 {
		h, err := f.findHeader(data, opts.MaxHeaderBytes)
		if h == 0 || err != nil {
			return message{}, false, err
		}
		length, chunked, expect, ok := bodyLength(data[:h], opts.MaxBufferedBodyBytes)
		if !ok {
			return message{}, false, errStreamBody
		}
		f.header = int32(h)
		if chunked {
			f.pos, f.size = int32(h), -1
			expectContinue = expect && len(data) == h
		} else {
			f.size = int32(h + length)
			expectContinue = expect && len(data) < h+length
		}
	}
	m = message{header: int(f.header)}
	if f.size < 0 {
		end, err := f.chunks(data, m.header+opts.MaxBufferedBodyBytes)
		if err != nil {
			*f = framer{}
			return message{}, false, err
		}
		if end == 0 {
			return message{}, expectContinue, nil
		}
		m.size, m.chunked = end, true
	} else if m.size = int(f.size); len(data) < m.size {
		return message{}, expectContinue, nil
	}
	*f = framer{}
	return m, false, nil
}

// findHeader returns the length of the request line and headers at the start of data, terminator included, or 0 when
// the terminator has not arrived yet.
func (f *framer) findHeader(data []byte, maxHeaderBytes int) (int, error) {
	// Back up by len-1 bytes, in case the terminator is split across two arrivals of data.
	from := max(int(f.pos)-len(headerTerminator)+1, 0)
	i := bytes.Index(data[from:], headerTerminator)
	if i < 0 {
		if len(data) > maxHeaderBytes {
			return 0, errHeaderTooLarge
		}
		f.pos = int32(len(data))
		return 0, nil
	}
	n := from + i + len(headerTerminator)
	if n > maxHeaderBytes {
		return 0, errHeaderTooLarge
	}
	return n, nil
}

// chunks goes over the chunks of the body that have arrived, from f.pos on, and returns the end of the message once the
// last chunk is there, 0 before. Only the plain form is buffered, up to limit: hexadecimal sizes, no chunk extensions
// and no trailer fields; anything else is errStreamBody, for the standard library to judge.
func (f *framer) chunks(data []byte, limit int) (int, error) {
	for {
		pos := int(f.pos)
		size, n, ok := chunkHeader(data[pos:])
		if !ok {
			return 0, errStreamBody
		}
		if n == 0 {
			return 0, nil
		}
		end := pos + n + size + 2 // the chunk's data and its CRLF; the last chunk has the empty line ending the message
		if end > limit {
			return 0, errStreamBody
		}
		if len(data) < end {
			return 0, nil
		}
		if data[end-2] != '\r' || data[end-1] != '\n' {
			return 0, errStreamBody
		}
		if size == 0 {
			return end, nil
		}
		f.pos = int32(end)
	}
}

// chunkHeader reads the size line at the start of b, a hexadecimal size and CRLF, and returns the size and the line's
// length; the length is 0 when the line has not arrived in full. ok=false for anything else, and for sizes of more than
// 8 digits, far above any buffered body.
func chunkHeader(b []byte) (size, n int, ok bool) {
	for i, c := range b {
		switch {
		case '0' <= c && c <= '9':
			size = size<<4 | int(c-'0')
		case 'a' <= c && c <= 'f':
			size = size<<4 | int(c-'a'+10)
		case 'A' <= c && c <= 'F':
			size = size<<4 | int(c-'A'+10)
		case c == '\r' && i > 0:
			if i+1 == len(b) {
				return 0, 0, true
			}
			return size, i + 2, b[i+1] == '\n'
		default:
			return 0, 0, false
		}
		if i == 8 {
			return 0, 0, false
		}
	}
	return 0, 0, true
}

// dechunk decodes in place a chunked body the framer delimited (see framer.chunks) and returns the data.
func dechunk(body []byte) []byte {
	r, w := 0, 0
	for {
		size, n, _ := chunkHeader(body[r:])
		if size == 0 {
			return body[:w]
		}
		w += copy(body[w:], body[r+n:r+n+size])
		r += n + size + 2
	}
}

// bodyLength looks at the fields of header that decide how the body is framed: it returns the length of a
// Content-Length body, whether the body is chunked instead, and whether the client sent "Expect: 100-continue".
// ok=false means fhttp does not buffer the body: a Content-Length above maxBody, a Transfer-Encoding other than a
// single chunked on HTTP/1.1 (or one with a Content-Length), or anything unusual (a repeated or invalid Content-Length,
// another Expect), all of which the standard library judges instead.
//
// A field this scan misses (say "Content-Length :" with a space) cannot smuggle a request: the message ends where this
// scan says, and conn.handle rejects it when the standard library frames it differently.
func bodyLength(header []byte, maxBody int) (length int, chunked, expectContinue, ok bool) {
	seenLength := false
	end := bytes.IndexByte(header, '\n')
	requestLine, lines := header[:end], header[end+1:]
	for len(lines) > 0 {
		i := bytes.IndexByte(lines, '\n') // the header ends with "\r\n\r\n", so every line has one
		line := lines[:i]
		lines = lines[i+1:]
		switch {
		case hasField(line, "Content-Length"):
			if seenLength {
				return 0, false, false, false
			}
			seenLength = true
			if length, ok = parseLength(line[len("Content-Length:"):], maxBody); !ok {
				return 0, false, false, false
			}
		case hasField(line, "Transfer-Encoding"):
			if chunked || !bytes.HasSuffix(requestLine, []byte("HTTP/1.1\r")) ||
				!bytes.EqualFold(fieldValue(line, "Transfer-Encoding"), []byte("chunked")) {
				return 0, false, false, false
			}
			chunked = true
		case hasField(line, "Expect"):
			if !bytes.EqualFold(fieldValue(line, "Expect"), []byte("100-continue")) {
				return 0, false, false, false
			}
			expectContinue = !bytes.HasSuffix(requestLine, []byte("HTTP/1.0\r")) // no 1xx response goes to an HTTP/1.0 client
		}
	}
	if chunked && seenLength {
		return 0, false, false, false
	}
	return length, chunked, expectContinue, true
}

// hasField reports whether line is the header field name (case-insensitive) followed directly by a colon.
func hasField(line []byte, name string) bool {
	return len(line) > len(name) && line[len(name)] == ':' && bytes.EqualFold(line[:len(name)], []byte(name))
}

// fieldValue returns the value of the field line hasField matched with name, without the surrounding whitespace and the
// line's trailing CR.
func fieldValue(line []byte, name string) []byte {
	return bytes.Trim(line[len(name)+1:], " \t\r")
}

// parseLength parses a Content-Length value the way the standard library does (see parseContentLength in net/http):
// the surrounding whitespace and the line's trailing CR are trimmed, and what is left must be a decimal number.
// ok=false for anything else or for a length above maxBody.
func parseLength(value []byte, maxBody int) (int, bool) {
	n, err := strconv.ParseUint(string(bytes.Trim(value, " \t\r")), 10, 63)
	return int(n), err == nil && n <= uint64(maxBody)
}
