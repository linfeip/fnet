package fhttp

import (
	"bufio"
	"bytes"
	"crypto/tls"
	"io"
	"net/http"
	"net/textproto"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/linfeip/fnet/internal/httpx"
	"github.com/linfeip/fnet/internal/units"
)

// parsedRequest is everything parseRequest builds for one request, held in one object.
type parsedRequest struct {
	request        http.Request
	baseRequest    http.Request // request with the pooled context, used to reset the unexported Request fields
	requestContext pooledContext
	url            url.URL
	body           body
	values         [8]string // the header values, when there are no more
	encoding       [1]string // Request.TransferEncoding of a chunked request
	reader         *reader   // what the body of a request http.ReadRequest read reads from, see readRequest
}

// maxRetainedBytes is the longest request target or header block a connection keeps for the next request to share (see
// lastRequest). Longer ones are copied again each time: held by every idle connection, a header of the 16KB allowed would
// add up to gigabytes at a million connections.
const maxRetainedBytes = units.KB

// lastRequest holds what a connection's requests share, made on its first one: the strings of the previous request, up to
// maxRetainedBytes, that parseRequest shares with the next one when it has the same bytes there (a client usually sends
// the same header fields every time, and often requests the same target again: polling, reporting, calling the same API),
// the TLS state and the context.
type lastRequest struct {
	target string                      // the request target
	fields string                      // the header fields, up to the empty line ending them
	tls    *tls.ConnectionState        // Request.TLS, made on the first request of a TLS connection
	ctx    atomic.Pointer[connContext] // the context of the requests, made when a Handler first asks for it (see conn.context)
}

// parsedPool holds the parsedRequests; a request taken from it is valid only within the ServeHTTP call it is handed
// to, and release puts it back here for the next request once that call returns (see the package documentation).
var parsedPool = sync.Pool{New: func() any {
	p := new(parsedRequest)
	p.baseRequest = *p.request.WithContext(&p.requestContext)
	p.request = p.baseRequest
	return p
}}

// release returns p, with its header map, to parsedPool once nothing refers to the request any more, and its reader to
// readerPool. The values and encoding are left as they are: only the cleared header and the reset request referred to
// them.
func (p *parsedRequest) release() {
	header := p.request.Header
	if len(header) > 0 { // clearing costs even an empty map, as most requests leave it (Host has a field of its own)
		clear(header)
	}
	p.request = p.baseRequest
	p.request.Header = header
	p.requestContext.connection.Store(nil)
	p.url, p.body = url.URL{}, body{}
	if p.reader != nil {
		p.reader.release()
		p.reader = nil
	}
	parsedPool.Put(p)
}

// bind gives the request the context of c's requests (see pooledContext), until release.
func (p *parsedRequest) bind(c *conn) { p.requestContext.connection.Store(c) }

// reader wraps a whole in-memory message into the *bufio.Reader that http.ReadRequest needs.
type reader struct {
	bufferedReader *bufio.Reader
	bytesReader    bytes.Reader
}

var readerPool = sync.Pool{New: func() any {
	rd := new(reader)
	rd.bufferedReader = bufio.NewReaderSize(&rd.bytesReader, 4*units.KB)
	return rd
}}

// RequestBody returns the buffered body bytes of r if available and not yet consumed.
// When the request body is buffered in memory by fhttp, it returns the slice directly without allocation.
// Otherwise it falls back to reading r.Body.
func RequestBody(r *http.Request) ([]byte, error) {
	if b, ok := r.Body.(interface{ Bytes() []byte }); ok {
		return b.Bytes(), nil
	}
	return io.ReadAll(r.Body)
}

// unread returns the number of bytes of the message not read yet.
func (rd *reader) unread() int { return rd.bufferedReader.Buffered() + rd.bytesReader.Len() }

func (rd *reader) release() {
	rd.bytesReader.Reset(nil)
	readerPool.Put(rd)
}

// parse builds the request for the message r holds, through parseRequest when the message keeps to its plain forms and
// http.ReadRequest otherwise. It returns the status code to reply with instead when the request is invalid, 0 otherwise;
// the request is valid until parsed.release.
//
// Same as the net/http server, an invalid Host is rejected, since neither parser validates it while Handlers often use
// r.Host to build URLs or redirect addresses.
func (c *conn) parse(r request) (req *http.Request, parsed *parsedRequest, status int) {
	msg := r.msg.Bytes()
	m := message{size: len(msg), header: int(r.header), chunked: r.chunked}
	if parsed = parseRequest(msg, m, c.last); parsed == nil {
		return readRequest(msg, m)
	}
	if !validHost(parsed.request.Host) { // parseRequest builds only requests with one Host field, which r.Host holds
		parsed.release()
		return nil, nil, http.StatusBadRequest
	}
	return &parsed.request, parsed, 0
}

// readRequest builds the request for msg, the message m, through http.ReadRequest, see parse.
func readRequest(msg []byte, m message) (*http.Request, *parsedRequest, int) {
	rd := readerPool.Get().(*reader)
	rd.bytesReader.Reset(msg)
	rd.bufferedReader.Reset(&rd.bytesReader)
	req, err := http.ReadRequest(rd.bufferedReader)
	status := 0
	switch body := m.size - m.header; {
	case err != nil:
		status = http.StatusBadRequest
	case req.ProtoMajor != 1: // same as the net/http server: only HTTP/1.x is supported, rejected before Host is checked
		status = http.StatusHTTPVersionNotSupported
	case rd.unread() != body || m.chunked != (req.TransferEncoding != nil) || !m.chunked && int64(body) != req.ContentLength:
		// The message ends where framer said, so the standard library must have read the same header and must frame the
		// body the same way: a chunked body (whose form framer checked) as chunked, any other as exactly the rest of the
		// message. Otherwise the two disagree on the framing: a field framer did not recognize (see inspectHeader), or an
		// empty line made of a bare LF, which the standard library accepts as a line break, ending the headers early.
		// The remainder must not be silently discarded or executed (an upstream proxy may frame it differently,
		// misaligning the responses), so it is handled as a bad request.
		status = http.StatusBadRequest
	case !validStdRequest(msg[:m.header], req):
		status = http.StatusBadRequest
	}
	if status != 0 {
		rd.release()
		return nil, nil, status
	}
	parsed := parsedPool.Get().(*parsedRequest) // for its context, which the request takes as well (see pooledContext)
	parsed.reader = rd                          // the request's body reads from it, so it goes back to its pool with the request
	if m.chunked {
		// The chunks are decoded here, as parseRequest does, since the standard library reads fewer chunk extensions than
		// the framer let through.
		parsed.body.data = dechunk(msg[m.header:m.size])
		req.Body = &parsed.body
	}
	return req.WithContext(&parsed.requestContext), parsed, 0
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
		last.target = retained(uri)
	}
	path, query, hasQuery := strings.Cut(uri, "?")
	if !validPath(path) || !validQuery(query) {
		return nil
	}
	fields := last.fields
	if string(msg[end+1:m.header]) != fields {
		fields = string(msg[end+1 : m.header])
		last.fields = retained(fields)
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
		value = textproto.TrimString(value) // spaces and tabs: validValue let no CR or LF through
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
	if hosts > 1 || hosts == 0 && minor == 1 { // rejected by http.ReadRequest and conn.handle
		p.release()
		return nil
	}

	r.Method, r.RequestURI, r.Proto, r.ProtoMajor, r.ProtoMinor = method, uri, protos[minor], 1, minor
	r.URL, r.Host = &p.url, host
	p.url.Path, p.url.RawQuery, p.url.ForceQuery = path, query, hasQuery && query == ""
	r.Close = httpx.HasToken(header["Connection"], "close")
	if minor == 0 {
		r.Close = r.Close || !httpx.HasToken(header["Connection"], "keep-alive")
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

// retained returns s if a connection keeps it for its next request, "" if it is longer than maxRetainedBytes.
func retained(s string) string {
	if len(s) > maxRetainedBytes {
		return ""
	}
	return s
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

// validHost reports whether the Host value is empty or a host name, an IPv4 address or an IPv6 literal in brackets
// (including the zone identifier), optionally followed by a colon and a port of digits (RFC 9110 7.2, RFC 3986 3.2.2).
// The characters of a name are those the net/http server allows, so that what it serves is served here as well.
func validHost(host string) bool {
	for i := 0; i < len(host); i++ {
		switch c := host[i]; {
		case c >= '0' && c <= '9', c >= 'a' && c <= 'z', c >= 'A' && c <= 'Z':
		case strings.IndexByte("!$%&'()*+,-.:;=[]_~", c) >= 0:
		default:
			return false
		}
	}
	rest := host // what follows the host: nothing, or a colon and the port
	if strings.HasPrefix(host, "[") {
		end := strings.IndexByte(host, ']')
		if end < 2 || strings.IndexByte(host[1:end], '[') >= 0 { // not closed, empty, or opened again
			return false
		}
		rest = host[end+1:]
	} else if i := strings.IndexAny(host, ":[]"); i < 0 {
		return true
	} else if host[i] != ':' { // brackets enclose an IP literal only
		return false
	} else {
		rest = host[i:]
	}
	if rest == "" {
		return true
	}
	if rest[0] != ':' {
		return false
	}
	for i := 1; i < len(rest); i++ {
		if rest[i] < '0' || rest[i] > '9' {
			return false
		}
	}
	return true
}

// validConnectTarget reports whether a CONNECT request names a port, as its target, a host and a port, must (RFC 9110
// 9.3.6); any other request passes.
func validConnectTarget(req *http.Request) bool {
	if req.Method != http.MethodConnect || req.URL.Path != "" {
		return true
	}
	_, err := strconv.ParseUint(req.URL.Port(), 10, 16)
	return err == nil
}

// validStdRequest makes the checks the net/http server makes on a request http.ReadRequest read out of header (see
// readRequest in net/http's server.go), which http.ReadRequest itself leaves out; it has checked the field values.
//   - Every field name is a token: http.ReadRequest keeps a name with a space before its colon, for which RFC 9112 5.1
//     requires a 400. Accepted, "Content-Length : 5" frames no body for fhttp, but a body for an intermediary that reads
//     it as Content-Length, and so smuggles a request in.
//   - Request.Host is valid, and an HTTP/1.1 request has a valid Host field, even an empty one and even with an
//     absolute-form target (RFC 9112 3.2). http.ReadRequest drops the field, with an absolute-form target's host in
//     Request.Host in its place, so it is looked up in header, the request line and the field lines after it.
//   - Unlike on the net/http server, a CONNECT needs a Host field as well, and one to an empty or invalid port is
//     rejected (RFC 9110 9.3.6).
func validStdRequest(header []byte, req *http.Request) bool {
	for name := range req.Header {
		if !validName(name) {
			return false
		}
	}
	if !validHost(req.Host) || !validConnectTarget(req) {
		return false
	}
	for i := bytes.IndexByte(header, '\n'); i >= 0; i = bytes.IndexByte(header, '\n') {
		if header = header[i+1:]; hasField(header, "Host") {
			return validHost(string(fieldValue(header[:bytes.IndexByte(header, '\n')], "Host")))
		}
	}
	return !req.ProtoAtLeast(1, 1)
}

// body is the Body of a request parseRequest built: the buffered body, which the Handler must not read once it has
// returned (as with net/http), since the message buffer is reused.
type body struct {
	data   []byte
	closed bool
}

func (b *body) Bytes() []byte {
	if b.closed {
		return nil
	}
	return b.data
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

// QueryParam returns the first value associated with key in r.URL.RawQuery, as r.URL.Query().Get(key) does, but without
// allocating a map: nor a string, unless there is something in the query to unescape.
func QueryParam(r *http.Request, key string) (value string) {
	ForEachQuery(r, func(k, v string) bool {
		if k == key {
			value = v
			return false
		}
		return true
	})
	return value
}

// ForEachQuery calls fn for each key-value pair in r.URL.RawQuery, in order, until fn returns false. The pairs are those
// r.URL.Query() holds, unescaped the same way (a pair with an invalid escape or a semicolon is left out), but no map is
// allocated: nor a string, unless there is something in the query to unescape.
func ForEachQuery(r *http.Request, fn func(key, value string) bool) {
	for query := r.URL.RawQuery; query != ""; {
		var pair string
		pair, query, _ = strings.Cut(query, "&")
		if pair == "" || strings.Contains(pair, ";") {
			continue
		}
		key, value, _ := strings.Cut(pair, "=")
		key, keyErr := url.QueryUnescape(key)
		value, valueErr := url.QueryUnescape(value)
		if keyErr != nil || valueErr != nil {
			continue
		}
		if !fn(key, value) {
			return
		}
	}
}
