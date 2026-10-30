package fhttp

import (
	"bytes"
	"errors"
	"net/http"
	"strconv"
)

var (
	// errHeaderTooLarge means the request line and headers exceed MaxHeaderBytes, or have more than
	// maxHeaderValueCount field lines; it is answered with 431 (Request Header Fields Too Large).
	errHeaderTooLarge = errors.New("fhttp: request header too large")
	// errURITooLong means the request line alone exceeds MaxHeaderBytes; RFC 9112 3 requires a 414 (URI Too Long) for a
	// request target longer than the server parses.
	errURITooLong = errors.New("fhttp: request URI too long")
	// errBadRequest means the request cannot be framed: its header ends in an empty line made of a bare LF, its Content-Length
	// is no number or differs from another, or its Transfer-Encoding does not end in chunked (RFC 9112 6.3). It is answered
	// with 400 (Bad Request), with the Date the net/http server leaves out of its own; valid but unsupported body framing
	// is handed to net/http instead.
	errBadRequest = errors.New("fhttp: request cannot be framed")
	// errHandoff means fhttp cannot safely handle the request on its buffered path; the connection is handed to net/http.
	errHandoff = errors.New("fhttp: request is handed to net/http")
)

// maxHeaderValueCount is the maximum number of field lines in a request header, net/http's DefaultMaxHeaderValueCount
// (Go 1.27): MaxHeaderBytes alone lets a header hold thousands of tiny fields ("A:\r\n" takes 4 bytes).
const maxHeaderValueCount = 500

// headerTerminator is the terminator of the request headers.
var headerTerminator = []byte("\r\n\r\n")

// message is a complete request message the framer split out.
type message struct {
	size    int  // the length of the whole message: request line, headers and body
	header  int  // the length of the request line and headers, terminator included
	chunked bool // the body is chunked, and size includes its framing
}

// framer splits request messages inside fnet's callbacks. It only finds where a message ends: the header terminator,
// then a body of Content-Length bytes or of plain chunks. The message is handed as a whole to http.ReadRequest, which
// parses it and decodes the body. Every connection holds one, so the limits are not copied in, and the offsets are
// int32 (see maxLimit).
type framer struct {
	header int32 // the length of the current message's header once it has arrived, 0 before
	pos    int32 // before the header has arrived: the bytes known to lack its terminator; in a chunked body: the next chunk
	size   int32 // the length of a message with a Content-Length body, -1 while a chunked body arrives
}

// split frames the request messages at the start of data, within the limits of opts, and hands push the items of the
// request queue they make, in order: a complete message (status 0, msg being the message and m its framing), the 100
// Continue a client waits for before it sends the body, the status code answering a request that cannot be framed, or
// statusHandoff. It returns how much of data it took, the messages and the empty lines before them, and end, the status of
// the item that stopped it: an error status, after which it takes all of data, or statusHandoff, the request that goes
// to net/http being left in data. What else is left is a message still to arrive, to be passed again, with what arrives
// next, from its start.
func (f *framer) split(data []byte, opts *Options, push func(msg []byte, m message, status int16)) (consumed int, end int16) {
	for consumed < len(data) {
		rest := data[consumed:]
		if *f == (framer{}) {
			// An empty line before a request, CRLF or a bare LF, is ignored, as some clients send one after a body (RFC
			// 9112 2.2); a bare CR is not one, and is left to make the request line invalid.
			if rest[0] == '\n' {
				consumed++
				continue
			} else if rest[0] == '\r' && len(rest) == 1 {
				return consumed, 0 // its LF may be on its way
			} else if rest[0] == '\r' && rest[1] == '\n' {
				consumed += 2
				continue
			}
		}
		m, expectContinue, err := f.next(rest, opts)
		switch {
		case err == errHandoff:
			push(nil, message{}, statusHandoff)
			return consumed, statusHandoff
		case err != nil:
			status := errorStatus(err)
			push(nil, message{}, status)
			return len(data), status
		case m.size == 0:
			// The header is complete and the client waits for 100 Continue before sending the body. It goes through the
			// queue as well, so it is not written in the middle of earlier responses.
			if expectContinue {
				push(nil, message{}, http.StatusContinue)
			}
			return consumed, 0
		}
		push(rest[:m.size], m, 0)
		consumed += m.size
	}
	return consumed, 0
}

// errorStatus returns the status code answering a request the framer rejects with err.
func errorStatus(err error) int16 {
	switch err {
	case errHeaderTooLarge:
		return http.StatusRequestHeaderFieldsTooLarge
	case errURITooLong:
		return http.StatusRequestURITooLong
	}
	return http.StatusBadRequest
}

// next looks for the end of the message starting at the beginning of data, within the limits of opts: m.size>0 means
// data[:m.size] is a complete message, m.size==0 means there is not enough data and next must be called again with
// the same message start once more data arrives. expectContinue is reported once, when the header is complete and the
// client waits for 100 Continue before sending the body. After an error the framer is not used again.
func (f *framer) next(data []byte, opts *Options) (m message, expectContinue bool, err error) {
	if f.header == 0 {
		h, err := f.findHeader(data, opts.MaxHeaderBytes)
		if h == 0 || err != nil {
			return message{}, false, err
		}
		maxBody := min(opts.MaxBufferedBodyBytes, maxLimit-h)
		length, chunked, expect, err := inspectHeader(data[:h], maxBody)
		if err != nil {
			return message{}, false, err
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
		end, err := f.chunks(data, m.header+min(opts.MaxBufferedBodyBytes, maxLimit-m.header))
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
// the terminator has not arrived yet. A header over MaxHeaderBytes is errURITooLong or errHeaderTooLarge; inspectHeader
// counts its field lines.
func (f *framer) findHeader(data []byte, maxHeaderBytes int) (int, error) {
	// Back up by len-1 bytes, in case the terminator is split across two arrivals of data.
	from := max(int(f.pos)-len(headerTerminator)+1, 0)
	i := bytes.Index(data[from:], headerTerminator)
	if i < 0 {
		// RFC 9112 2.2 lets a recipient take a bare LF for a line break, which fhttp does not: a header ending in an empty
		// line made of one is rejected rather than left waiting for a terminator that is not coming.
		if rest := data[from:]; bytes.Contains(rest, []byte("\n\n")) || bytes.Contains(rest, []byte("\n\r\n")) {
			return 0, errBadRequest
		}
		if len(data) > maxHeaderBytes {
			return 0, tooLarge(data, maxHeaderBytes)
		}
		f.pos = int32(len(data))
		return 0, nil
	}
	n := from + i + len(headerTerminator)
	if n > maxHeaderBytes {
		return 0, tooLarge(data, maxHeaderBytes)
	}
	return n, nil
}

// tooLarge returns the error for data, whose header goes beyond maxHeaderBytes: errURITooLong when the request line does
// not end within the first maxHeaderBytes bytes, errHeaderTooLarge when it does.
func tooLarge(data []byte, maxHeaderBytes int) error {
	if bytes.IndexByte(data[:maxHeaderBytes], '\n') < 0 {
		return errURITooLong
	}
	return errHeaderTooLarge
}

// chunks goes over the chunks of the body that have arrived, from f.pos on, and returns the end of the message once the
// last chunk is there, 0 before. Only plain chunks (hexadecimal sizes and no chunk extensions or trailer fields) are
// buffered, up to limit; anything else is errHandoff, for the standard library to handle.
func (f *framer) chunks(data []byte, limit int) (int, error) {
	for {
		pos := int(f.pos)
		size, n, ok := chunkHeader(data[pos:])
		if !ok {
			return 0, errHandoff
		}
		if n == 0 {
			return 0, nil
		}
		// The chunk's data and its CRLF; the last chunk has the empty line ending the message. Compared before it is
		// added up, as pos+size could overflow an int on 32-bit platforms with limits near maxLimit.
		if n+size+2 > limit-pos {
			return 0, errHandoff
		}
		end := pos + n + size + 2
		if len(data) < end {
			return 0, nil
		}
		if data[end-2] != '\r' || data[end-1] != '\n' {
			return 0, errHandoff
		}
		if size == 0 {
			return end, nil
		}
		f.pos = int32(end)
	}
}

// chunkHeader reads the size line at the start of b, a hexadecimal size and CRLF, and returns the size and the line's
// length; the length is 0 when the line has not arrived in full. ok=false for anything else (including chunk extensions),
// and for sizes of more than 7 digits: 256MB and up, far above any buffered body, so that a size never overflows an int,
// even on 32-bit platforms (RFC 9112 7.1).
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
		if i == 7 {
			return 0, 0, false
		}
	}
	return 0, 0, true
}

// inspectHeader goes once over the field lines of header, deciding how fhttp takes the message in: it returns the length
// of a Content-Length body, whether the body is chunked instead, and whether the client sent "Expect: 100-continue".
// errHeaderTooLarge means too many field lines. errBadRequest means a message whose body length cannot be told: a
// Content-Length that is no number, or differs from another, and a Transfer-Encoding whose last coding is not chunked (RFC
// 9112 6.3). errHandoff means fhttp does not serve the request itself: a Content-Length above maxBody, an unsupported
// Transfer-Encoding, or anything unusual (a repeated Content-Length, another Expect), all of which the standard library
// judges instead; or a client asking for an event stream (Server-Sent Events), which needs the http.Flusher fhttp does
// not provide.
//
// A field this scan misses cannot smuggle a request, although the message ends where this scan says: readRequest
// rejects it, as a name that is no token (say "Content-Length :", see validStdRequest) or because the standard library
// frames the message differently.
func inspectHeader(header []byte, maxBody int) (length int, chunked, expectContinue bool, err error) {
	var lengths, codings int // the field lines of Content-Length and of Transfer-Encoding
	var declared uint64      // the first Content-Length
	finalChunked, handoff := false, false
	end := bytes.IndexByte(header, '\n')
	requestLine, lines := bytes.TrimSuffix(header[:end], []byte("\r")), header[end+1:]
	for fieldLines := 0; len(lines) > 0; fieldLines++ {
		i := bytes.IndexByte(lines, '\n') // the header ends with "\r\n\r\n", so every line has one
		line := lines[:i]
		lines = lines[i+1:]
		if fieldLines == maxHeaderValueCount && len(lines) > 0 { // a field line past the limit, not the empty line ending them
			return 0, false, false, errHeaderTooLarge
		}
		switch {
		case hasField(line, "Content-Length"):
			n, ok := parseLength(line[len("Content-Length:"):])
			if !ok || lengths > 0 && n != declared {
				return 0, false, false, errBadRequest
			}
			if lengths, declared = lengths+1, n; n > uint64(maxBody) {
				handoff = true
			} else {
				length = int(n)
			}
		case hasField(line, "Transfer-Encoding"):
			value := fieldValue(line, "Transfer-Encoding")
			// Only a single field line of just "chunked" has a boundary fhttp frames itself.
			chunked = codings == 0 && bytes.EqualFold(value, []byte("chunked"))
			finalChunked = bytes.EqualFold(bytes.Trim(value[bytes.LastIndexByte(value, ',')+1:], " \t"), []byte("chunked"))
			codings++
		case hasField(line, "Expect"):
			if !bytes.EqualFold(fieldValue(line, "Expect"), []byte("100-continue")) {
				handoff = true
			}
			expectContinue = !bytes.HasSuffix(requestLine, []byte("HTTP/1.0")) // no 1xx response goes to an HTTP/1.0 client
		case hasField(line, "Accept"):
			handoff = handoff || hasEventStream(fieldValue(line, "Accept"))
		}
	}
	if codings > 0 && !finalChunked {
		return 0, false, false, errBadRequest
	}
	// A chunked body is framed here only on HTTP/1.1 (RFC 9112 6.1) and without a Content-Length (6.2); net/http judges
	// the others.
	if handoff || codings > 1 || lengths > 1 || codings == 1 && !chunked || chunked && (lengths > 0 || !bytes.HasSuffix(requestLine, []byte("HTTP/1.1"))) {
		return 0, false, false, errHandoff
	}
	return length, chunked, expectContinue, nil
}

// hasEventStream reports whether an Accept value mentions text/event-stream, the media type EventSource asks for, in
// any letter case as media types go. It is looked for at each '-' of the value, which no letter case changes and few
// Accept values have; a false positive only costs a handoff to net/http.
func hasEventStream(value []byte) bool {
	const mediaType, dash = "text/event-stream", len("text/event")
	for i := dash; i < len(value); i++ {
		j := bytes.IndexByte(value[i:], '-')
		if j < 0 {
			break
		}
		i += j
		if s := value[i-dash:]; len(s) >= len(mediaType) && bytes.EqualFold(s[:len(mediaType)], []byte(mediaType)) {
			return true
		}
	}
	return false
}

// hasField reports whether line is the header field name (case-insensitive) followed directly by a colon.
func hasField(line []byte, name string) bool {
	return len(line) > len(name) && line[len(name)] == ':' && line[0]|0x20 == name[0]|0x20 && bytes.EqualFold(line[:len(name)], []byte(name))
}

// fieldValue returns the value of the field line hasField matched with name, without the surrounding whitespace and the
// line's trailing CR.
func fieldValue(line []byte, name string) []byte {
	return bytes.Trim(line[len(name)+1:], " \t\r")
}

// parseLength parses a Content-Length value the way the standard library does (see parseContentLength in net/http):
// the surrounding whitespace and the line's trailing CR are trimmed, and what is left must be a decimal number. ok=false
// for anything else.
func parseLength(value []byte) (n uint64, ok bool) {
	n, err := strconv.ParseUint(string(bytes.Trim(value, " \t\r")), 10, 63)
	return n, err == nil
}
