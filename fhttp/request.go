package fhttp

import (
	"bytes"
	"net/http"
	"strconv"
	"strings"
)

// readRequest builds the request for the message r holds into w.req through http.ReadRequest. It returns the status
// code to reply with instead when the request is invalid, 0 otherwise.
func (c *conn) readRequest(w *response, r request) int {
	msg := r.msg.Bytes()
	m := message{size: len(msg), header: int(r.header), chunked: r.chunked}
	w.bytesReader.Reset(msg)
	w.bufferedReader.Reset(&w.bytesReader)
	req, err := http.ReadRequest(w.bufferedReader)
	status := 0
	switch body := m.size - m.header; {
	case err != nil:
		status = http.StatusBadRequest
	case req.ProtoMajor != 1: // same as the net/http server: only HTTP/1.x is supported, rejected before Host is checked
		status = http.StatusHTTPVersionNotSupported
	case w.bufferedReader.Buffered()+w.bytesReader.Len() != body || m.chunked != (req.TransferEncoding != nil) || !m.chunked && int64(body) != req.ContentLength:
		// The message ends where framer said, so the standard library must have read the same header and must frame the
		// body the same way: a chunked body as chunked, any other as exactly the rest of the message. Otherwise the two
		// disagree on the framing: a field framer did not recognize (see inspectHeader), or an empty line made of a bare
		// LF, which the standard library accepts as a line break, ending the headers early.
		status = http.StatusBadRequest
	case !validStdRequest(msg[:m.header], req):
		status = http.StatusBadRequest
	}
	if status != 0 {
		w.bytesReader.Reset(nil)
		return status
	}
	// The copy WithContext makes stays on the stack: the Handler gets the request http.ReadRequest allocated, its own to
	// keep after it returns, as on net/http.
	*req = *req.WithContext(c.state)
	w.req = req
	return 0
}

// tokenBytes marks the bytes of a token (RFC 9110 5.6.2), such as a header field name.
var tokenBytes = func() (t [256]bool) {
	for _, c := range []byte("!#$%&'*+-.^_`|~0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz") {
		t[c] = true
	}
	return t
}()

// validName reports whether name is a token, as a header field name must be.
func validName(name string) bool {
	for i := 0; i < len(name); i++ {
		if !tokenBytes[name[i]] {
			return false
		}
	}
	return name != ""
}

// hostBytes marks the bytes allowed in a host name or inside an IPv6 literal (RFC 9110 7.2, RFC 3986 3.2.2).
var hostBytes = func() (t [256]bool) {
	for _, c := range []byte("!$%&'()*+,-.:;=_~0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz") {
		t[c] = true
	}
	return t
}()

// validHost reports whether the Host value is empty or a host name, an IPv4 address or an IPv6 literal in brackets
// (including the zone identifier), optionally followed by a colon and a port of digits (RFC 9110 7.2, RFC 3986 3.2.2).
// The characters of a name are those the net/http server allows, so that what it serves is served here as well.
func validHost(host string) bool {
	rest := "" // what follows the host: nothing, or a colon and the port
	if strings.HasPrefix(host, "[") {
		end := strings.IndexByte(host, ']')
		if end < 2 { // not closed, or empty
			return false
		}
		host, rest = host[1:end], host[end+1:]
	} else if i := strings.IndexByte(host, ':'); i >= 0 {
		host, rest = host[:i], host[i:]
	}
	for i := 0; i < len(host); i++ {
		if !hostBytes[host[i]] {
			return false
		}
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
	if req.Host != "" && req.URL.Host == "" {
		return true
	}
	for i := bytes.IndexByte(header, '\n'); i >= 0; i = bytes.IndexByte(header, '\n') {
		if header = header[i+1:]; hasField(header, "Host") {
			return validHost(string(fieldValue(header[:bytes.IndexByte(header, '\n')], "Host")))
		}
	}
	return !req.ProtoAtLeast(1, 1)
}
