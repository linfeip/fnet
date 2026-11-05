package http2

import (
	"net/http"
	"net/textproto"
	"net/url"
	"strconv"
	"strings"

	"github.com/linfeip/fnet/internal/httpx"
	"golang.org/x/net/http2/hpack"
)

// maxHeaderValueCount caps the number of header fields in a single header block, matching fhttp and net/http.
const maxHeaderValueCount = 500

// Pseudo-header bitmask flags in headerBuilder.seenPseudos.
const (
	pseudoMethod uint8 = 1 << iota
	pseudoScheme
	pseudoAuthority
	pseudoPath
	pseudoProtocol
)

// lowerTokenBytes marks valid lowercase HTTP token characters allowed in HTTP/2 field names (RFC 9113 8.2).
var lowerTokenBytes = func() (t [256]bool) {
	for _, c := range []byte("!#$%&'*+-.^_`|~0123456789abcdefghijklmnopqrstuvwxyz") {
		t[c] = true
	}
	return t
}()

// tokenBytes marks valid HTTP token characters (case-insensitive) for response/trailer validation.
var tokenBytes = func() (t [256]bool) {
	for _, c := range []byte("!#$%&'*+-.^_`|~0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz") {
		t[c] = true
	}
	return t
}()

// hostBytes marks characters allowed in a host name or IPv6 literal (RFC 9110 7.2, RFC 3986 3.2.2).
var hostBytes = func() (t [256]bool) {
	for _, c := range []byte("!$%&'()*+,-.:;=_~0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz") {
		t[c] = true
	}
	return t
}()

// commonCanonicalHeaders avoids a heap allocation in textproto.CanonicalMIMEHeaderKey for standard lowercase HTTP/2
// field names.
var commonCanonicalHeaders = map[string]string{
	"accept":               "Accept",
	"accept-charset":       "Accept-Charset",
	"accept-encoding":      "Accept-Encoding",
	"accept-language":      "Accept-Language",
	"authorization":        "Authorization",
	"cache-control":        "Cache-Control",
	"connection":           "Connection",
	"content-encoding":     "Content-Encoding",
	"content-length":       "Content-Length",
	"content-type":         "Content-Type",
	"cookie":               "Cookie",
	"date":                 "Date",
	"etag":                 "Etag",
	"expect":               "Expect",
	"grpc-accept-encoding": "Grpc-Accept-Encoding",
	"grpc-encoding":        "Grpc-Encoding",
	"grpc-message":         "Grpc-Message",
	"grpc-status":          "Grpc-Status",
	"grpc-timeout":         "Grpc-Timeout",
	"host":                 "Host",
	"if-match":             "If-Match",
	"if-modified-since":    "If-Modified-Since",
	"if-none-match":        "If-None-Match",
	"if-unmodified-since":  "If-Unmodified-Since",
	"keep-alive":           "Keep-Alive",
	"origin":               "Origin",
	"pragma":               "Pragma",
	"proxy-authorization":  "Proxy-Authorization",
	"proxy-connection":     "Proxy-Connection",
	"range":                "Range",
	"referer":              "Referer",
	"te":                   "Te",
	"trailer":              "Trailer",
	"transfer-encoding":    "Transfer-Encoding",
	"upgrade":              "Upgrade",
	"user-agent":           "User-Agent",
	"via":                  "Via",
	"x-forwarded-for":      "X-Forwarded-For",
	"x-forwarded-proto":    "X-Forwarded-Proto",
	"x-request-id":         "X-Request-Id",
}

func canonicalHeaderKey(lower string) string {
	if c, ok := commonCanonicalHeaders[lower]; ok {
		return c
	}
	return textproto.CanonicalMIMEHeaderKey(lower)
}

func validLowerFieldName(name string) bool {
	for i := 0; i < len(name); i++ {
		if !lowerTokenBytes[name[i]] {
			return false
		}
	}
	return name != ""
}

func validFieldName(name string) bool {
	for i := 0; i < len(name); i++ {
		if !tokenBytes[name[i]] {
			return false
		}
	}
	return name != ""
}

// validFieldValue checks RFC 9113 8.2.1: no NUL/CR/LF or other control characters (except HTAB), and no leading or
// trailing SP/HTAB.
func validFieldValue(v string) bool {
	if len(v) == 0 {
		return true
	}
	if v[0] == ' ' || v[0] == '\t' || v[len(v)-1] == ' ' || v[len(v)-1] == '\t' {
		return false
	}
	for i := 0; i < len(v); i++ {
		b := v[i]
		if (b < 0x20 && b != '\t') || b == 0x7f {
			return false
		}
	}
	return true
}

func validHost(host string) bool {
	rest := ""
	if strings.HasPrefix(host, "[") {
		end := strings.IndexByte(host, ']')
		if end < 2 {
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

var forbiddenTrailers = map[string]bool{
	"Authorization":       true,
	"Cache-Control":       true,
	"Connection":          true,
	"Content-Encoding":    true,
	"Content-Length":      true,
	"Content-Range":       true,
	"Content-Type":        true,
	"Expect":              true,
	"Host":                true,
	"Keep-Alive":          true,
	"Max-Forwards":        true,
	"Pragma":              true,
	"Proxy-Authenticate":  true,
	"Proxy-Authorization": true,
	"Proxy-Connection":    true,
	"Range":               true,
	"Realm":               true,
	"Te":                  true,
	"Trailer":             true,
	"Transfer-Encoding":   true,
	"Upgrade":             true,
	"Www-Authenticate":    true,
}

func validTrailerName(canonical string) bool {
	return !strings.HasPrefix(canonical, "If-") && !forbiddenTrailers[canonical]
}

// headerBuilder accumulates one HTTP/2 header block (initial request headers or trailers) in a single pass as
// hpack.Decoder emits fields.
type headerBuilder struct {
	streamID       uint32
	method         string
	scheme         string
	authority      string
	path           string
	protocol       string
	header         http.Header
	remainingBytes uint32
	fieldCount     int
	seenPseudos    uint8
	sawRegular     bool
	isTrailer      bool
	truncated      bool
	invalid        bool
	streamEnded    bool
	selfDependency bool
}

func (b *headerBuilder) reset(streamID, maxBytes uint32, isTrailer, streamEnded, selfDependency bool) {
	*b = headerBuilder{
		streamID:       streamID,
		remainingBytes: maxBytes,
		isTrailer:      isTrailer,
		streamEnded:    streamEnded,
		selfDependency: selfDependency,
	}
}

// emit processes one decoded HPACK field and reports whether decoding should keep emitting fields.
func (b *headerBuilder) emit(hf hpack.HeaderField) bool {
	b.fieldCount++
	if b.fieldCount > maxHeaderValueCount {
		b.truncated = true
		b.remainingBytes = 0
		return false
	}
	if !validFieldValue(hf.Value) {
		b.invalid = true
		return false
	}
	size := hf.Size()
	if size > b.remainingBytes {
		b.truncated = true
		b.remainingBytes = 0
		return false
	}
	b.remainingBytes -= size

	if strings.HasPrefix(hf.Name, ":") {
		if b.sawRegular || b.isTrailer {
			b.invalid = true
			return false
		}
		var bit uint8
		switch hf.Name {
		case ":method":
			bit, b.method = pseudoMethod, hf.Value
		case ":scheme":
			bit, b.scheme = pseudoScheme, hf.Value
		case ":authority":
			bit, b.authority = pseudoAuthority, hf.Value
		case ":path":
			bit, b.path = pseudoPath, hf.Value
		case ":protocol":
			bit, b.protocol = pseudoProtocol, hf.Value
		default:
			b.invalid = true
			return false
		}
		if b.seenPseudos&bit != 0 {
			b.invalid = true
			return false
		}
		b.seenPseudos |= bit
		return true
	}

	b.sawRegular = true
	if !validLowerFieldName(hf.Name) {
		b.invalid = true
		return false
	}
	if b.header == nil {
		b.header = make(http.Header, 8)
	}
	key := canonicalHeaderKey(hf.Name)
	b.header[key] = append(b.header[key], hf.Value)
	return true
}

type parsedRequest struct {
	req            http.Request
	declaredLength int64
	expectContinue bool
	status         int // non-zero HTTP error status to reply immediately without calling Handler (431, 417)
}

// buildRequest validates the completed request header block in b and builds an http.Request. ok=false means the
// request is malformed and must be rejected with RST_STREAM(PROTOCOL_ERROR) (RFC 9113 8.1.1).
func (b *headerBuilder) buildRequest() (parsedRequest, bool) {
	if b.invalid || b.selfDependency {
		return parsedRequest{}, false
	}
	if b.truncated {
		return parsedRequest{status: http.StatusRequestHeaderFieldsTooLarge}, true
	}

	header := b.header
	if header == nil {
		header = make(http.Header)
	}
	if !validRequestConnectionHeaders(header) {
		return parsedRequest{}, false
	}

	authority := b.authority
	if hostValues, hasHost := header["Host"]; hasHost {
		if len(hostValues) != 1 || (authority != "" && !strings.EqualFold(authority, hostValues[0])) {
			return parsedRequest{}, false
		}
		if authority == "" {
			authority = hostValues[0]
		}
		delete(header, "Host")
	}
	if !validHost(authority) {
		return parsedRequest{}, false
	}

	if b.method == "" || !validFieldName(b.method) {
		return parsedRequest{}, false
	}
	var parsedURL *url.URL
	var requestURI string
	isPlainConnect := b.method == http.MethodConnect && b.protocol == ""
	if isPlainConnect {
		if b.seenPseudos&(pseudoScheme|pseudoPath) != 0 || authority == "" {
			return parsedRequest{}, false
		}
		if _, err := strconv.ParseUint((&url.URL{Host: authority}).Port(), 10, 16); err != nil {
			return parsedRequest{}, false
		}
		parsedURL = &url.URL{Host: authority}
		requestURI = authority
	} else {
		if b.protocol != "" && b.method != http.MethodConnect {
			return parsedRequest{}, false
		}
		if b.method == "" || b.scheme == "" || b.path == "" {
			return parsedRequest{}, false
		}
		if (b.scheme == "http" || b.scheme == "https") && strings.IndexByte(authority, '@') >= 0 {
			return parsedRequest{}, false
		}
		if b.path[0] != '/' && !(b.path == "*" && b.method == http.MethodOptions) {
			return parsedRequest{}, false
		}
		u, err := url.ParseRequestURI(b.path)
		if err != nil {
			return parsedRequest{}, false
		}
		parsedURL = u
		requestURI = b.path
		if b.protocol != "" {
			header.Set(":protocol", b.protocol)
		}
	}

	declaredLength := int64(0)
	if !b.streamEnded {
		declaredLength = -1
	}
	if lengths, ok := header["Content-Length"]; ok {
		n, err := strconv.ParseUint(lengths[0], 10, 63)
		if err != nil {
			return parsedRequest{}, false
		}
		for _, dup := range lengths[1:] {
			if dup != lengths[0] {
				return parsedRequest{}, false
			}
		}
		if len(lengths) > 1 {
			header["Content-Length"] = lengths[:1]
		}
		if b.streamEnded && n != 0 {
			return parsedRequest{}, false
		}
		declaredLength = int64(n)
	}

	expectContinue := false
	if expects, ok := header["Expect"]; ok {
		if !httpx.HasToken(expects, "100-continue") {
			return parsedRequest{status: http.StatusExpectationFailed}, true
		}
		expectContinue = !b.streamEnded
		delete(header, "Expect")
	}

	if cookies := header["Cookie"]; len(cookies) > 1 {
		header["Cookie"] = []string{strings.Join(cookies, "; ")}
	}

	var trailer http.Header
	if trailers, ok := header["Trailer"]; ok {
		for _, v := range trailers {
			for v != "" {
				var part string
				part, v, _ = strings.Cut(v, ",")
				key := textproto.CanonicalMIMEHeaderKey(strings.TrimSpace(part))
				if key != "" && validFieldName(key) && validTrailerName(key) {
					if trailer == nil {
						trailer = make(http.Header)
					}
					trailer[key] = nil
				}
			}
		}
		delete(header, "Trailer")
	}

	req := http.Request{
		Method:        b.method,
		URL:           parsedURL,
		Proto:         "HTTP/2.0",
		ProtoMajor:    2,
		ProtoMinor:    0,
		Header:        header,
		ContentLength: declaredLength,
		Host:          authority,
		RequestURI:    requestURI,
		Trailer:       trailer,
	}
	return parsedRequest{
		req:            req,
		declaredLength: declaredLength,
		expectContinue: expectContinue,
	}, true
}

// validRequestConnectionHeaders rejects connection-specific headers prohibited by RFC 9113 8.2.2.
func validRequestConnectionHeaders(header http.Header) bool {
	for _, k := range [...]string{"Connection", "Keep-Alive", "Proxy-Connection", "Transfer-Encoding", "Upgrade"} {
		if _, exists := header[k]; exists {
			return false
		}
	}
	if te := header["Te"]; len(te) > 0 && (len(te) > 1 || (te[0] != "trailers" && te[0] != "")) {
		return false
	}
	return true
}

// buildTrailers validates a trailing HEADERS block and merges valid trailer fields into dst.
func (b *headerBuilder) buildTrailers(dst http.Header) bool {
	if b.invalid || b.truncated || b.selfDependency || !b.streamEnded || b.seenPseudos != 0 {
		return false
	}
	for k, vv := range b.header {
		if !validTrailerName(k) {
			return false
		}
		if dst != nil {
			dst[k] = append(dst[k], vv...)
		}
	}
	return true
}
