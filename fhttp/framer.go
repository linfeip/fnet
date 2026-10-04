package fhttp

import (
	"bytes"
	"errors"
	"strconv"
)

var (
	// errHeaderTooLarge means the request line and headers exceed MaxHeaderBytes.
	errHeaderTooLarge = errors.New("fhttp: request header too large")
	// errStreamBody means the request body is not buffered by fhttp: it is larger than MaxBufferedBodyBytes, chunked, or
	// declared in a way only the standard library should judge; the connection is handed over to net/http.
	errStreamBody = errors.New("fhttp: request body is streamed")
)

// headerTerminator is the terminator of the request headers.
var headerTerminator = []byte("\r\n\r\n")

// framer splits request messages inside fnet's callbacks. It only finds where a message ends: the header terminator,
// then the Content-Length bytes of a small body. The message is handed as a whole to the standard library's
// http.ReadRequest, which parses it and decodes the body. Every connection holds one, so the limits are not copied in.
type framer struct {
	scanned int // bytes of this message known to lack the terminator; scanning resumes here, so slow clients cause no rescans
	size    int // the length of a message whose header is complete but whose body has not fully arrived, 0 otherwise
}

// next looks for the end of the message starting at the beginning of data, within the limits of opts: n>0 means
// data[:n] is a complete message (request line, headers and body), n==0 means there is not enough data and next must
// be called again with the same message start once more data arrives. expectContinue is reported once, when the header
// is complete and the client waits for 100 Continue before sending the body. errStreamBody leaves the framer at the
// start of that message.
func (f *framer) next(data []byte, opts *Options) (n int, expectContinue bool, err error) {
	if f.size == 0 {
		h, err := f.header(data, opts.MaxHeaderBytes)
		if h == 0 || err != nil {
			return 0, false, err
		}
		length, expect, ok := bodyLength(data[:h], opts.MaxBufferedBodyBytes)
		if !ok {
			return 0, false, errStreamBody
		}
		f.size = h + length
		expectContinue = expect && len(data) < f.size
	}
	if len(data) < f.size {
		return 0, expectContinue, nil
	}
	n, f.size = f.size, 0
	return n, false, nil
}

// header returns the length of the request line and headers at the start of data, terminator included, or 0 when the
// terminator has not arrived yet.
func (f *framer) header(data []byte, maxHeaderBytes int) (int, error) {
	// Back up by len-1 bytes, in case the terminator is split across two arrivals of data.
	from := max(f.scanned-len(headerTerminator)+1, 0)
	i := bytes.Index(data[from:], headerTerminator)
	if i < 0 {
		if len(data) > maxHeaderBytes {
			return 0, errHeaderTooLarge
		}
		f.scanned = len(data)
		return 0, nil
	}
	n := from + i + len(headerTerminator)
	if n > maxHeaderBytes {
		return 0, errHeaderTooLarge
	}
	f.scanned = 0
	return n, nil
}

// bodyLength looks at the three fields of header that decide how the body is framed and returns the length of the body
// and whether the client sent "Expect: 100-continue". ok=false means fhttp does not buffer the body: a Transfer-Encoding,
// a Content-Length above maxBody, or anything unusual (a repeated or invalid Content-Length, another Expect), all of
// which the standard library judges instead.
//
// A field this scan misses (say "Content-Length :" with a space) cannot smuggle a request: the message ends where this
// scan says, and conn.handle rejects it when the standard library reads a different body length out of it.
func bodyLength(header []byte, maxBody int) (length int, expectContinue, ok bool) {
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
				return 0, false, false
			}
			seenLength = true
			if length, ok = parseLength(line[len("Content-Length:"):], maxBody); !ok {
				return 0, false, false
			}
		case hasField(line, "Transfer-Encoding"):
			return 0, false, false
		case hasField(line, "Expect"):
			if !bytes.EqualFold(bytes.Trim(line[len("Expect:"):], " \t\r"), []byte("100-continue")) {
				return 0, false, false
			}
			expectContinue = !bytes.HasSuffix(requestLine, []byte("HTTP/1.0\r")) // no 1xx response goes to an HTTP/1.0 client
		}
	}
	return length, expectContinue, true
}

// hasField reports whether line is the header field name (case-insensitive) followed directly by a colon.
func hasField(line []byte, name string) bool {
	return len(line) > len(name) && line[len(name)] == ':' && bytes.EqualFold(line[:len(name)], []byte(name))
}

// parseLength parses a Content-Length value the way the standard library does (see parseContentLength in net/http):
// the surrounding whitespace and the line's trailing CR are trimmed, and what is left must be a decimal number. ok=false
// for anything else or for a length above maxBody.
func parseLength(value []byte, maxBody int) (int, bool) {
	n, err := strconv.ParseUint(string(bytes.Trim(value, " \t\r")), 10, 63)
	return int(n), err == nil && n <= uint64(maxBody)
}
