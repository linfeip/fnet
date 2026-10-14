package fhttp

import (
	"bytes"
	"io"
	"net/http"
	"net/textproto"
	"net/url"
	"strings"
	"sync"
)

// parsedRequest is everything parseRequest builds for one request, held in one object.
type parsedRequest struct {
	request  http.Request
	url      url.URL
	body     body
	values   [8]string // the header values, when there are no more
	encoding [1]string // Request.TransferEncoding of a chunked request
}

// lastRequest holds the strings of a connection's previous request that parseRequest shares with the next one when it
// has the same bytes there: a client usually sends the same header fields every time, and often requests the same
// target again (polling, reporting, calling the same API).
type lastRequest struct {
	target string // the request target
	fields string // the header fields, up to the empty line ending them
}

// parsedPool holds the parsedRequests; a request taken from it is valid only within the ServeHTTP call it is handed
// to, and release puts it back here for the next request once that call returns (see the package documentation).
var parsedPool = sync.Pool{New: func() any { return new(parsedRequest) }}

// release returns p, with its header map, to parsedPool once nothing refers to the request any more. The values and
// encoding are left as they are: only the cleared header and the reset request referred to them.
func (p *parsedRequest) release() {
	header := p.request.Header
	if len(header) > 0 { // clearing costs even an empty map, as most requests leave it (Host has a field of its own)
		clear(header)
	}
	p.request, p.url, p.body = http.Request{Header: header}, url.URL{}, body{}
	parsedPool.Put(p)
}

// parseRequest builds the request for msg, the complete message m the framer split out, when the message keeps to the
// plain forms of HTTP/1.x that http.ReadRequest reads in only one way: a request line "METHOD /path?query HTTP/1.x" with
// one of the common methods and a path the URL needs no escaping for, and header fields of a token name, a colon and a
// value of visible characters, spaces and tabs, every line ending in CRLF. The result is what http.ReadRequest would
// return, field for field. Anything else returns nil, for http.ReadRequest to read or reject.
//
// The method and the protocol are constants, and the target and the header fields are copies, the latter split into
// substrings: last holds those of the connection's previous request, each shared when it is the same bytes. A chunked
// body is decoded in place in msg. The result comes from parsedPool, for the caller to release once the Handler has
// returned.
func parseRequest(msg []byte, m message, last *lastRequest) *parsedRequest {
	end := bytes.IndexByte(msg, '\n') // the header ends in "\r\n\r\n", so the request line has an LF
	if end < 1 || msg[end-1] != '\r' {
		return nil
	}
	methodBytes, line, _ := bytes.Cut(msg[:end-1], []byte(" "))
	target, protoBytes, _ := bytes.Cut(line, []byte(" "))
	method, ok := methodName(methodBytes)
	var minor int
	switch string(protoBytes) {
	case "HTTP/1.1":
		minor = 1
	case "HTTP/1.0":
	default:
		ok = false
	}
	if !ok {
		return nil
	}
	uri := last.target
	if string(target) != uri {
		uri = string(target)
		last.target = uri
	}
	path, query, hasQuery := strings.Cut(uri, "?")
	if !validPath(path) || !validQuery(query) {
		return nil
	}
	fields := last.fields
	if string(msg[end+1:m.header]) != fields {
		fields = string(msg[end+1 : m.header])
		last.fields = fields
	}

	count := strings.Count(fields, "\n") - 1 // the last line is the empty one ending the header
	p := parsedPool.Get().(*parsedRequest)
	r := &p.request
	if r.Header == nil {
		r.Header = make(http.Header, count)
	}
	header, values := r.Header, p.values[:]
	if count > len(values) {
		values = make([]string, count)
	}
	host, hosts := "", 0
	for {
		field, rest, ok := cutLine(fields)
		if !ok {
			p.release()
			return nil
		}
		if field == "" {
			break
		}
		fields = rest
		name, value, ok := strings.Cut(field, ":")
		if !ok || !validName(name) || !validValue(value) {
			p.release()
			return nil
		}
		value = strings.Trim(value, " \t")
		switch name = canonicalKey(name); name {
		case "Host": // http.ReadRequest moves it to Request.Host
			host, hosts = value, hosts+1
			continue
		case "Transfer-Encoding": // the framer let only a single chunked through, which becomes Request.TransferEncoding
			continue
		case "Pragma", "Trailer": // these change other fields
			p.release()
			return nil
		}
		if vv, ok := header[name]; ok {
			header[name] = append(vv, value)
			continue
		}
		values[0] = value
		header[name], values = values[:1:1], values[1:]
	}
	if hosts > 1 {
		p.release()
		return nil
	}

	r.Method, r.RequestURI, r.Proto, r.ProtoMajor, r.ProtoMinor = method, uri, protos[minor], 1, minor
	r.URL, r.Host = &p.url, host
	p.url.Path, p.url.RawQuery, p.url.ForceQuery = path, query, hasQuery && query == ""
	r.Close = hasToken(header["Connection"], "close")
	if minor == 0 {
		r.Close = r.Close || !hasToken(header["Connection"], "keep-alive")
	}
	body := msg[m.header:m.size]
	switch {
	case m.chunked:
		p.body.data = dechunk(body)
		p.encoding[0] = "chunked"
		r.Body, r.ContentLength, r.TransferEncoding = &p.body, -1, p.encoding[:]
	case len(body) > 0:
		p.body.data = body
		r.Body, r.ContentLength = &p.body, int64(len(body))
	default:
		r.Body = http.NoBody
	}
	return p
}

// methods are the request methods parseRequest reads, and protos the protocols by minor version.
var (
	methods = [...]string{http.MethodGet, http.MethodHead, http.MethodPost, http.MethodPut, http.MethodDelete, http.MethodOptions, http.MethodPatch}
	protos  = [...]string{"HTTP/1.0", "HTTP/1.1"}
)

// methodName returns the one of methods that b spells, so that a request refers to the constant rather than a copy.
func methodName(b []byte) (string, bool) {
	for _, name := range methods {
		if string(b) == name {
			return name, true
		}
	}
	return "", false
}

// cutLine cuts s after its first line, which must end in CRLF: ok is false when the first LF has no CR before it.
func cutLine(s string) (line, rest string, ok bool) {
	i := strings.IndexByte(s, '\n')
	if i < 1 || s[i-1] != '\r' {
		return "", "", false
	}
	return s[:i-1], s[i+1:], true
}

// canonicalKey returns the canonical form of the field name, a token: name itself when it already is, as it usually is.
func canonicalKey(name string) string {
	upper := true
	for i := 0; i < len(name); i++ {
		c := name[i]
		if upper && 'a' <= c && c <= 'z' || !upper && 'A' <= c && c <= 'Z' {
			if key, ok := knownKeys[name]; ok {
				return key
			}
			return textproto.CanonicalMIMEHeaderKey(name)
		}
		upper = c == '-'
	}
	return name
}

// knownKeys maps the usual spelling of the WebSocket handshake fields to their canonical form, which saves every
// handshake a copy of each name.
var knownKeys = map[string]string{
	"Sec-WebSocket-Key":        "Sec-Websocket-Key",
	"Sec-WebSocket-Version":    "Sec-Websocket-Version",
	"Sec-WebSocket-Protocol":   "Sec-Websocket-Protocol",
	"Sec-WebSocket-Extensions": "Sec-Websocket-Extensions",
}

// byteClasses marks the bytes of a token (RFC 9110 5.6.2), such as a field name, and those a URL path keeps unescaped.
var byteClasses = func() (t [256]uint8) {
	for _, c := range []byte("!#$%&'*+-.^_`|~0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz") {
		t[c] |= tokenByte
	}
	for _, c := range []byte("$&+,-./0123456789:;=@ABCDEFGHIJKLMNOPQRSTUVWXYZ_abcdefghijklmnopqrstuvwxyz~") {
		t[c] |= pathByte
	}
	return t
}()

const (
	tokenByte = 1 << iota
	pathByte
)

// validName reports whether name is a token, as a field name must be; http.Header.Write leaves out the fields whose
// names are not.
func validName(name string) bool {
	for i := 0; i < len(name); i++ {
		if byteClasses[name[i]]&tokenByte == 0 {
			return false
		}
	}
	return name != ""
}

// validPath reports whether path is absolute and made of bytes url.ParseRequestURI keeps as they are: no escapes, so
// that URL.RawPath stays empty.
func validPath(path string) bool {
	for i := 0; i < len(path); i++ {
		if byteClasses[path[i]]&pathByte == 0 {
			return false
		}
	}
	return path != "" && path[0] == '/'
}

// validQuery reports whether query is made of visible ASCII characters.
func validQuery(query string) bool {
	for i := 0; i < len(query); i++ {
		if c := query[i]; c <= ' ' || c >= 0x7f {
			return false
		}
	}
	return true
}

// validValue reports whether value is made of visible ASCII characters, spaces and tabs.
func validValue(value string) bool {
	for i := 0; i < len(value); i++ {
		if c := value[i]; (c < ' ' || c >= 0x7f) && c != '\t' {
			return false
		}
	}
	return true
}

// body is the Body of a request parseRequest built: the buffered body, which the Handler must not read once it has
// returned (as with net/http), since the message buffer is reused.
type body struct {
	data   []byte
	closed bool
}

func (b *body) Read(p []byte) (int, error) {
	if b.closed {
		return 0, http.ErrBodyReadAfterClose
	}
	if len(b.data) == 0 {
		return 0, io.EOF
	}
	n := copy(p, b.data)
	b.data = b.data[n:]
	return n, nil
}

// WriteTo lets io.Copy take the body without a buffer of its own.
func (b *body) WriteTo(w io.Writer) (int64, error) {
	if b.closed {
		return 0, http.ErrBodyReadAfterClose
	}
	n, err := w.Write(b.data)
	b.data = b.data[n:]
	return int64(n), err
}

func (b *body) Close() error {
	b.data, b.closed = nil, true
	return nil
}
