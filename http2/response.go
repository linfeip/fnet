package http2

import (
	"fmt"
	"net/http"
	"net/textproto"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/linfeip/fnet/internal/bytepool"
	"github.com/linfeip/fnet/internal/httpx"
)

const (
	maxBufferedBody       = 64 << 10
	maxRetainedHeaderList = 64
)

type headerPair struct {
	name  string
	value string
}

var commonLowerHeaders = map[string]string{
	"Accept-Ranges":               "accept-ranges",
	"Access-Control-Allow-Origin": "access-control-allow-origin",
	"Allow":                       "allow",
	"Cache-Control":               "cache-control",
	"Content-Disposition":         "content-disposition",
	"Content-Encoding":            "content-encoding",
	"Content-Language":            "content-language",
	"Content-Location":            "content-location",
	"Content-Range":               "content-range",
	"Content-Type":                "content-type",
	"Etag":                        "etag",
	"Expires":                     "expires",
	"Grpc-Encoding":               "grpc-encoding",
	"Grpc-Message":                "grpc-message",
	"Grpc-Status":                 "grpc-status",
	"Last-Modified":               "last-modified",
	"Location":                    "location",
	"Retry-After":                 "retry-after",
	"Server":                      "server",
	"Set-Cookie":                  "set-cookie",
	"Strict-Transport-Security":   "strict-transport-security",
	"Vary":                        "vary",
	"Www-Authenticate":            "www-authenticate",
	"X-Content-Type-Options":      "x-content-type-options",
	"X-Frame-Options":             "x-frame-options",
	"X-Request-Id":                "x-request-id",
}

func lowerHeaderName(name string) string {
	if l, ok := commonLowerHeaders[name]; ok {
		return l
	}
	return strings.ToLower(name)
}

var statusStrings = map[int]string{
	http.StatusContinue:                    "100",
	http.StatusEarlyHints:                  "103",
	http.StatusOK:                          "200",
	http.StatusCreated:                     "201",
	http.StatusAccepted:                    "202",
	http.StatusNoContent:                   "204",
	http.StatusPartialContent:              "206",
	http.StatusMovedPermanently:            "301",
	http.StatusFound:                       "302",
	http.StatusSeeOther:                    "303",
	http.StatusNotModified:                 "304",
	http.StatusTemporaryRedirect:           "307",
	http.StatusPermanentRedirect:           "308",
	http.StatusBadRequest:                  "400",
	http.StatusUnauthorized:                "401",
	http.StatusForbidden:                   "403",
	http.StatusNotFound:                    "404",
	http.StatusMethodNotAllowed:            "405",
	http.StatusConflict:                    "409",
	http.StatusPreconditionFailed:          "412",
	http.StatusRequestEntityTooLarge:       "413",
	http.StatusExpectationFailed:           "417",
	http.StatusTooManyRequests:             "429",
	http.StatusRequestHeaderFieldsTooLarge: "431",
	http.StatusInternalServerError:         "500",
	http.StatusNotImplemented:              "501",
	http.StatusBadGateway:                  "502",
	http.StatusServiceUnavailable:          "503",
	http.StatusGatewayTimeout:              "504",
}

func statusCodeString(code int) string {
	if s, ok := statusStrings[code]; ok {
		return s
	}
	return strconv.Itoa(code)
}

// response implements http.ResponseWriter and http.Flusher for one HTTP/2 stream.
type response struct {
	stream          *stream
	header          http.Header
	committedFields []headerPair
	trailers        []string
	body            bytepool.Buffer
	status          int
	declaredLength  int64
	remaining       int64
	writeErr        error
	wroteHeader     bool
	sentHeader      bool
	sentEndStream   bool
	hasContentType  bool
	hasDate         bool
	hasEncoding     bool
	closing         bool
}

var responsePool = sync.Pool{New: func() any {
	return &response{header: make(http.Header)}
}}

func acquireResponse(s *stream) *response {
	w := responsePool.Get().(*response)
	w.stream = s
	w.status = 0
	w.declaredLength = -1
	w.remaining = -1
	w.writeErr = nil
	w.wroteHeader = false
	w.sentHeader = false
	w.sentEndStream = false
	w.hasContentType = false
	w.hasDate = false
	w.hasEncoding = false
	w.closing = false
	return w
}

func (w *response) release() {
	if cap(w.body.Bytes()) <= maxBufferedBody {
		w.body.Reset()
	} else {
		w.body.Release()
	}
	if len(w.header) > 0 {
		clear(w.header)
	}
	if cap(w.committedFields) <= maxRetainedHeaderList {
		clear(w.committedFields)
		w.committedFields = w.committedFields[:0]
	} else {
		w.committedFields = nil
	}
	if len(w.trailers) > 0 {
		clear(w.trailers)
		w.trailers = w.trailers[:0]
	}
	w.stream = nil
	w.writeErr = nil
	responsePool.Put(w)
}

func (w *response) Header() http.Header { return w.header }

func (w *response) WriteHeader(code int) {
	if code < 100 || code > 999 {
		panic(fmt.Sprintf("http2: invalid WriteHeader code %v", code))
	}
	if w.wroteHeader {
		return
	}
	if code < 200 {
		if code != http.StatusSwitchingProtocols {
			_ = w.sendInformational(code)
		}
		return
	}
	w.wroteHeader = true
	w.status = code
	w.commitHeader()
}

func (w *response) sendInformational(code int) error {
	var fields []headerPair
	for name, values := range w.header {
		if strings.HasPrefix(name, http.TrailerPrefix) || !validFieldName(name) || isForbiddenResponseHeader(name) || name == "Content-Length" {
			continue
		}
		lower := lowerHeaderName(name)
		for _, v := range values {
			fields = append(fields, headerPair{name: lower, value: sanitizeHeaderValue(v)})
		}
	}
	return w.stream.conn.writeHeaders(w.stream, code, fields, "", "", "", false, true)
}

func isForbiddenResponseHeader(canonical string) bool {
	switch canonical {
	case "Connection", "Keep-Alive", "Proxy-Connection", "Transfer-Encoding", "Upgrade":
		return true
	}
	return false
}

func sanitizeHeaderValue(v string) string {
	v = strings.Trim(v, " \t\r\n")
	if strings.IndexAny(v, "\r\n\x00") < 0 {
		return v
	}
	var b strings.Builder
	b.Grow(len(v))
	for i := 0; i < len(v); i++ {
		c := v[i]
		if c == '\r' || c == '\n' || c == 0 {
			b.WriteByte(' ')
		} else {
			b.WriteByte(c)
		}
	}
	return strings.Trim(b.String(), " \t")
}

// commitHeader snapshots w.header at the moment the status code is fixed so subsequent mutations to w.Header() only
// affect trailers, matching net/http.
func (w *response) commitHeader() {
	w.declaredLength = -1
	w.remaining = -1
	for name, values := range w.header {
		if strings.HasPrefix(name, http.TrailerPrefix) {
			continue
		}
		switch name {
		case "Transfer-Encoding", "Keep-Alive", "Proxy-Connection", "Upgrade":
			continue
		case "Connection":
			if httpx.HasToken(values, "close") {
				w.closing = true
			}
			continue
		case "Trailer":
			for _, v := range values {
				w.declareTrailers(v)
			}
			continue
		case "Content-Length":
			if len(values) == 1 {
				if n, err := strconv.ParseInt(strings.TrimSpace(values[0]), 10, 63); err == nil && n >= 0 {
					w.declaredLength = n
					w.remaining = n
				}
			}
			continue
		case "Content-Type":
			if w.status == http.StatusNotModified {
				continue
			}
			w.hasContentType = true
		case "Date":
			w.hasDate = true
		case "Content-Encoding":
			w.hasEncoding = len(values) > 0 && values[0] != ""
		default:
			if !validFieldName(name) {
				continue
			}
		}
		lower := lowerHeaderName(name)
		for _, v := range values {
			w.committedFields = append(w.committedFields, headerPair{
				name:  lower,
				value: sanitizeHeaderValue(v),
			})
		}
	}
}

func (w *response) declareTrailers(raw string) {
	for raw != "" {
		var part string
		part, raw, _ = strings.Cut(raw, ",")
		key := textproto.CanonicalMIMEHeaderKey(strings.TrimSpace(part))
		if key != "" && validFieldName(key) && validTrailerName(key) && !slices.Contains(w.trailers, key) {
			w.trailers = append(w.trailers, key)
		}
	}
}

func (w *response) promoteUndeclaredTrailers() {
	for k := range w.header {
		if !strings.HasPrefix(k, http.TrailerPrefix) {
			continue
		}
		key := textproto.CanonicalMIMEHeaderKey(strings.TrimPrefix(k, http.TrailerPrefix))
		if key != "" && validFieldName(key) && validTrailerName(key) && !slices.Contains(w.trailers, key) {
			w.trailers = append(w.trailers, key)
		}
	}
}

func (w *response) collectTrailerFields() []headerPair {
	w.promoteUndeclaredTrailers()
	if len(w.trailers) == 0 {
		return nil
	}
	var fields []headerPair
	for _, key := range w.trailers {
		values := w.header[key]
		if len(values) == 0 {
			values = w.header[http.TrailerPrefix+key]
		}
		if len(values) == 0 {
			for k, vv := range w.header {
				if strings.HasPrefix(k, http.TrailerPrefix) && textproto.CanonicalMIMEHeaderKey(strings.TrimPrefix(k, http.TrailerPrefix)) == key {
					values = vv
					break
				}
			}
		}
		if len(values) == 0 {
			continue
		}
		lower := lowerHeaderName(key)
		for _, v := range values {
			fields = append(fields, headerPair{name: lower, value: sanitizeHeaderValue(v)})
		}
	}
	return fields
}

func (w *response) Write(p []byte) (int, error) {
	if err := w.beforeWrite(len(p)); err != nil {
		return 0, err
	}
	if w.discardBody() {
		if w.stream.req.Method == http.MethodHead && w.declaredLength < 0 && !w.sentHeader && w.body.Len()+len(p) <= maxBufferedBody {
			w.body.Write(p)
		} else if !w.sentHeader && w.body.Len() < 512 {
			w.body.Write(p[:min(len(p), 512-w.body.Len())])
		}
		return len(p), nil
	}
	if !w.sentHeader && w.body.Len()+len(p) <= maxBufferedBody {
		w.body.Write(p)
		return len(p), nil
	}
	return w.streamWrite(p)
}

func (w *response) WriteString(s string) (int, error) {
	if err := w.beforeWrite(len(s)); err != nil {
		return 0, err
	}
	if w.discardBody() {
		if w.stream.req.Method == http.MethodHead && w.declaredLength < 0 && !w.sentHeader && w.body.Len()+len(s) <= maxBufferedBody {
			w.body.WriteString(s)
		} else if !w.sentHeader && w.body.Len() < 512 {
			w.body.WriteString(s[:min(len(s), 512-w.body.Len())])
		}
		return len(s), nil
	}
	if !w.sentHeader && w.body.Len()+len(s) <= maxBufferedBody {
		w.body.WriteString(s)
		return len(s), nil
	}
	return w.streamWrite([]byte(s))
}

func (w *response) beforeWrite(n int) error {
	if w.writeErr != nil {
		return w.writeErr
	}
	if err := w.stream.conn.streamWriteError(w.stream); err != nil {
		w.writeErr = err
		return err
	}
	if !w.wroteHeader {
		w.WriteHeader(http.StatusOK)
	}
	if !bodyAllowedForStatus(w.status) {
		return http.ErrBodyNotAllowed
	}
	if w.remaining >= 0 {
		if int64(n) > w.remaining {
			return http.ErrContentLength
		}
		w.remaining -= int64(n)
	}
	return nil
}

func (w *response) discardBody() bool {
	req := w.stream.req
	return req.Method == http.MethodHead || !bodyAllowedForStatus(w.status) || w.status == http.StatusResetContent ||
		(req.Method == http.MethodConnect && w.status/100 == 2)
}

func (w *response) streamWrite(p []byte) (int, error) {
	if !w.sentHeader {
		sniff := w.body.Bytes()
		if len(sniff) == 0 {
			sniff = p
		}
		if err := w.flushHeader(sniff, false); err != nil {
			return 0, err
		}
	}
	if w.body.Len() > 0 {
		err := w.stream.conn.writeData(w.stream, w.body.Bytes(), false)
		w.body.Release()
		if err != nil {
			w.writeErr = err
			return 0, err
		}
	}
	if len(p) == 0 {
		return 0, nil
	}
	if err := w.stream.conn.writeData(w.stream, p, false); err != nil {
		w.writeErr = err
		return 0, err
	}
	return len(p), nil
}

// Flush implements http.Flusher, immediately sending the response headers (if not yet sent) and any buffered body
// bytes.
func (w *response) Flush() {
	_ = w.FlushError()
}

// FlushError is used by http.ResponseController.Flush.
func (w *response) FlushError() error {
	if w.writeErr != nil {
		return w.writeErr
	}
	if !w.wroteHeader {
		w.WriteHeader(http.StatusOK)
	}
	if !w.sentHeader {
		endStream := w.discardBody() && len(w.trailers) == 0
		if err := w.flushHeader(w.body.Bytes(), endStream); err != nil {
			return err
		}
		if w.discardBody() {
			w.body.Reset()
			return nil
		}
	}
	if w.body.Len() > 0 && !w.discardBody() {
		err := w.stream.conn.writeData(w.stream, w.body.Bytes(), false)
		w.body.Reset()
		if err != nil {
			w.writeErr = err
			return err
		}
	}
	return w.stream.conn.streamWriteError(w.stream)
}

// flushHeader sends the committed response HEADERS frame.
func (w *response) flushHeader(sample []byte, endStream bool) error {
	w.sentHeader = true
	contentType, contentLength, date := w.autoHeaders(sample, false)
	if w.closing {
		w.stream.conn.sendGoAway(true)
	}
	err := w.stream.conn.writeHeaders(w.stream, w.status, w.committedFields, contentType, contentLength, date, endStream, false)
	if err != nil {
		w.writeErr = err
	} else if endStream {
		w.sentEndStream = true
	}
	return err
}

func (w *response) autoHeaders(body []byte, handlerDone bool) (contentType, contentLength, date string) {
	bodyAllowed := bodyAllowedForStatus(w.status) && !(w.stream.req.Method == http.MethodConnect && w.status/100 == 2)
	isHEAD := w.stream.req.Method == http.MethodHead
	switch {
	case !bodyAllowed:
	case w.status == http.StatusResetContent:
		contentLength = "0"
	case w.declaredLength >= 0 && len(w.trailers) == 0:
		contentLength = strconv.FormatInt(w.declaredLength, 10)
	case handlerDone && len(w.trailers) == 0 && (!isHEAD || len(body) > 0):
		contentLength = strconv.Itoa(len(body))
	}
	if !w.hasContentType && !w.hasEncoding && bodyAllowed && len(body) > 0 && w.status != http.StatusResetContent {
		contentType = http.DetectContentType(body)
	}
	if !w.hasDate {
		date = currentDate()
	}
	return contentType, contentLength, date
}

// finish completes the stream's response once ServeHTTP returns normally.
func (w *response) finish() {
	if w.writeErr != nil {
		return
	}
	if !w.wroteHeader {
		w.WriteHeader(http.StatusOK)
	}
	isHEAD := w.stream.req.Method == http.MethodHead
	if !isHEAD && w.remaining > 0 {
		// Handler wrote fewer bytes than the Content-Length it declared: reset the stream so the client detects the
		// truncated response.
		w.stream.conn.resetStream(w.stream, errCodeInternal)
		return
	}

	trailerFields := w.collectTrailerFields()
	hasTrailers := len(trailerFields) > 0
	discard := w.discardBody()
	body := w.body.Bytes()
	if discard {
		body = nil
	}

	if !w.sentHeader {
		w.sentHeader = true
		sample := w.body.Bytes()
		contentType, contentLength, date := w.autoHeaders(sample, !hasTrailers)
		if w.closing {
			w.stream.conn.sendGoAway(true)
		}
		if len(body) == 0 && !hasTrailers {
			_ = w.stream.conn.writeHeaders(w.stream, w.status, w.committedFields, contentType, contentLength, date, true, false)
			return
		}
		if len(body) > 0 && !hasTrailers {
			_ = w.stream.conn.writeHeadersAndBody(w.stream, w.status, w.committedFields, contentType, contentLength, date, body)
			return
		}
		if err := w.stream.conn.writeHeaders(w.stream, w.status, w.committedFields, contentType, contentLength, date, false, false); err != nil {
			return
		}
	}

	if len(body) > 0 || (!hasTrailers && !w.sentEndStream) {
		if err := w.stream.conn.writeData(w.stream, body, !hasTrailers); err != nil {
			return
		}
		if !hasTrailers {
			w.sentEndStream = true
		}
	}
	if hasTrailers {
		_ = w.stream.conn.writeHeaders(w.stream, 0, trailerFields, "", "", "", true, false)
	}
}

func bodyAllowedForStatus(status int) bool {
	return status >= 200 && status != http.StatusNoContent && status != http.StatusNotModified
}

var (
	cachedDate atomic.Pointer[string]
	dateOnce   sync.Once
)

func currentDate() string {
	dateOnce.Do(startDateClock)
	return *cachedDate.Load()
}

func startDateClock() {
	refresh := func() time.Time {
		now := time.Now()
		s := now.UTC().Format(http.TimeFormat)
		cachedDate.Store(&s)
		return now
	}
	now := refresh()
	go func() {
		for {
			time.Sleep(time.Second - time.Duration(now.Nanosecond()))
			now = refresh()
		}
	}()
}
