//go:build go1.24

package http2

import (
	"bufio"
	"bytes"
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"log"
	"log/slog"
	"math/big"
	"net"
	"net/http"
	"net/http/httptrace"
	"net/textproto"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/linfeip/fnet/internal/bytepool"
	"golang.org/x/net/http2/hpack"
)

func tlsServerConfig(t *testing.T) *tls.Config {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	template := &x509.Certificate{
		SerialNumber: big.NewInt(1),
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(24 * time.Hour),
		DNSNames:     []string{"localhost"},
		IPAddresses:  []net.IP{net.IPv4(127, 0, 0, 1), net.IPv6loopback},
	}
	der, err := x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	return &tls.Config{Certificates: []tls.Certificate{{Certificate: [][]byte{der}, PrivateKey: key}}}
}

func startServer(t *testing.T, handler http.Handler, opts Options) *Server {
	t.Helper()
	s, err := NewServer("127.0.0.1:0", handler, opts)
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- s.Serve() }()
	t.Cleanup(func() {
		_ = s.Close()
		if err := <-done; !errors.Is(err, http.ErrServerClosed) {
			t.Errorf("Serve returned %v", err)
		}
	})
	return s
}

func newTLSClient(t *testing.T) *http.Client {
	t.Helper()
	tr := &http.Transport{
		TLSClientConfig:   &tls.Config{InsecureSkipVerify: true},
		ForceAttemptHTTP2: true,
	}
	c := &http.Client{Transport: tr, Timeout: 10 * time.Second}
	t.Cleanup(c.CloseIdleConnections)
	return c
}

func newH2CClient(t *testing.T) *http.Client {
	t.Helper()
	var protos http.Protocols
	protos.SetUnencryptedHTTP2(true)
	tr := &http.Transport{Protocols: &protos}
	c := &http.Client{Transport: tr, Timeout: 10 * time.Second}
	t.Cleanup(c.CloseIdleConnections)
	return c
}

func testMux() *http.ServeMux {
	mux := http.NewServeMux()
	mux.HandleFunc("/hello", func(w http.ResponseWriter, r *http.Request) {
		if r.ProtoMajor != 2 || r.Proto != "HTTP/2.0" {
			http.Error(w, "wrong proto", http.StatusBadRequest)
			return
		}
		_, _ = io.WriteString(w, "hello h2")
	})
	mux.HandleFunc("/echo", func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(w, r.Body)
	})
	mux.HandleFunc("/status", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("X-Custom", "value")
		w.WriteHeader(http.StatusCreated)
	})
	mux.HandleFunc("/big", func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write(bytes.Repeat([]byte("0123456789abcdef"), 256<<10)) // 4MB
	})
	mux.HandleFunc("/cookies", func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.WriteString(w, r.Header.Get("Cookie"))
	})
	return mux
}

func TestTLSBasicAndMultiplex(t *testing.T) {
	srv := startServer(t, testMux(), Options{TLSConfig: tlsServerConfig(t)})
	base := "https://" + srv.Addr().String()
	client := newTLSClient(t)

	resp, err := client.Get(base + "/hello")
	if err != nil {
		t.Fatal(err)
	}
	body, err := io.ReadAll(resp.Body)
	resp.Body.Close()
	if err != nil || resp.StatusCode != 200 || resp.Proto != "HTTP/2.0" || string(body) != "hello h2" ||
		resp.ContentLength != 8 || resp.Header.Get("Content-Type") != "text/plain; charset=utf-8" || resp.Header.Get("Date") == "" {
		t.Fatalf("GET /hello: status=%d proto=%s len=%d body=%q hdr=%v err=%v",
			resp.StatusCode, resp.Proto, resp.ContentLength, body, resp.Header, err)
	}

	headResp, err := client.Head(base + "/hello")
	if err != nil {
		t.Fatal(err)
	}
	headResp.Body.Close()
	if headResp.StatusCode != 200 || headResp.ContentLength != 8 {
		t.Fatalf("HEAD /hello: status=%d cl=%d", headResp.StatusCode, headResp.ContentLength)
	}

	stResp, err := client.Get(base + "/status")
	if err != nil {
		t.Fatal(err)
	}
	stResp.Body.Close()
	if stResp.StatusCode != http.StatusCreated || stResp.Header.Get("X-Custom") != "value" {
		t.Fatalf("GET /status: %d %v", stResp.StatusCode, stResp.Header)
	}

	req, _ := http.NewRequest(http.MethodGet, base+"/cookies", nil)
	req.AddCookie(&http.Cookie{Name: "a", Value: "1"})
	req.AddCookie(&http.Cookie{Name: "b", Value: "2"})
	ckResp, err := client.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	ckBody, _ := io.ReadAll(ckResp.Body)
	ckResp.Body.Close()
	if string(ckBody) != "a=1; b=2" {
		t.Fatalf("Cookie coalescing: got %q", ckBody)
	}

	const concurrency = 32
	payload := bytes.Repeat([]byte("multiplex-stream-data-"), 24<<10)
	var wg sync.WaitGroup
	errCh := make(chan error, concurrency)
	for i := range concurrency {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			msg := append([]byte(fmt.Sprintf("[%d]", idx)), payload...)
			r, err := client.Post(base+"/echo", "application/octet-stream", bytes.NewReader(msg))
			if err != nil {
				errCh <- err
				return
			}
			got, err := io.ReadAll(r.Body)
			r.Body.Close()
			if err != nil {
				errCh <- err
				return
			}
			if !bytes.Equal(got, msg) {
				errCh <- fmt.Errorf("stream %d mismatch: got %d bytes, want %d", idx, len(got), len(msg))
			}
		}(i)
	}
	wg.Wait()
	close(errCh)
	for err := range errCh {
		t.Fatal(err)
	}

	bigResp, err := client.Get(base + "/big")
	if err != nil {
		t.Fatal(err)
	}
	bigBody, err := io.ReadAll(bigResp.Body)
	bigResp.Body.Close()
	if err != nil || len(bigBody) != 4<<20 {
		t.Fatalf("GET /big: len=%d err=%v", len(bigBody), err)
	}
}

func TestH2CPriorKnowledge(t *testing.T) {
	srv := startServer(t, testMux(), Options{})
	base := "http://" + srv.Addr().String()
	client := newH2CClient(t)

	for range 10 {
		resp, err := client.Post(base+"/echo", "text/plain", strings.NewReader("cleartext-h2c"))
		if err != nil {
			t.Fatal(err)
		}
		got, err := io.ReadAll(resp.Body)
		resp.Body.Close()
		if err != nil || resp.Proto != "HTTP/2.0" || string(got) != "cleartext-h2c" {
			t.Fatalf("h2c echo: proto=%s got=%q err=%v", resp.Proto, got, err)
		}
	}
}

//go:noinline
func runOnH2Executor(task func()) { task() }

func TestExecutorAndKeySpread(t *testing.T) {
	var mu sync.Mutex
	keys := make(map[int]bool)
	onExecutor := atomic.Bool{}
	onExecutor.Store(true)

	srv := startServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		stack := make([]byte, 16<<10)
		if !strings.Contains(string(stack[:runtime.Stack(stack, false)]), "runOnH2Executor") {
			onExecutor.Store(false)
		}
		if r.Context().Value(http.ServerContextKey) == nil || r.Context().Value(http.LocalAddrContextKey) == nil {
			t.Error("missing standard context keys")
		}
		_, _ = io.WriteString(w, "ok")
	}), Options{
		Executor: func(key int, task func()) {
			mu.Lock()
			keys[key] = true
			mu.Unlock()
			go runOnH2Executor(task)
		},
	})

	client := newH2CClient(t)
	base := "http://" + srv.Addr().String()
	for range 4 {
		resp, err := client.Get(base + "/test")
		if err != nil {
			t.Fatal(err)
		}
		_, _ = io.ReadAll(resp.Body)
		resp.Body.Close()
	}
	if !onExecutor.Load() {
		t.Fatal("Handler did not execute on Options.Executor")
	}
	mu.Lock()
	distinctKeys := len(keys)
	mu.Unlock()
	if distinctKeys != 4 {
		t.Fatalf("expected 4 distinct executor keys across 4 streams on one connection, got %d", distinctKeys)
	}
}

func TestFlusherAndSSE(t *testing.T) {
	step := make(chan struct{})
	srv := startServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		flusher, ok := w.(http.Flusher)
		if !ok {
			t.Error("ResponseWriter does not implement http.Flusher")
			return
		}
		w.Header().Set("Content-Type", "text/event-stream")
		for i := range 3 {
			fmt.Fprintf(w, "data: chunk-%d\n\n", i)
			flusher.Flush()
			<-step
		}
	}), Options{})

	client := newH2CClient(t)
	resp, err := client.Get("http://" + srv.Addr().String() + "/events")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()

	reader := bufio.NewReader(resp.Body)
	for i := range 3 {
		line, err := reader.ReadString('\n')
		if err != nil {
			t.Fatal(err)
		}
		want := fmt.Sprintf("data: chunk-%d\n", i)
		if line != want {
			t.Fatalf("chunk %d: got %q, want %q", i, line, want)
		}
		_, _ = reader.ReadString('\n')
		step <- struct{}{}
	}
}

func TestTrailersDeclaredAndPrefixed(t *testing.T) {
	srv := startServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Trailer", "X-Declared")
		w.WriteHeader(http.StatusOK)
		body, _ := io.ReadAll(r.Body)
		_, _ = w.Write(body)
		w.Header().Set("X-Declared", "dec-"+r.Trailer.Get("X-Req-Trailer"))
		w.Header().Set(http.TrailerPrefix+"X-Prefixed", "prefixed-ok")
	}), Options{TLSConfig: tlsServerConfig(t)})

	client := newTLSClient(t)
	pr, pw := io.Pipe()
	req, err := http.NewRequest(http.MethodPost, "https://"+srv.Addr().String()+"/trailers", pr)
	if err != nil {
		t.Fatal(err)
	}
	req.Trailer = http.Header{"X-Req-Trailer": nil}
	go func() {
		_, _ = pw.Write([]byte("payload"))
		req.Trailer.Set("X-Req-Trailer", "client-val")
		_ = pw.Close()
	}()

	resp, err := client.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	gotBody, err := io.ReadAll(resp.Body)
	resp.Body.Close()
	if err != nil || string(gotBody) != "payload" {
		t.Fatalf("body: %q err=%v", gotBody, err)
	}
	if got := resp.Trailer.Get("X-Declared"); got != "dec-client-val" {
		t.Fatalf("X-Declared trailer: got %q", got)
	}
	if got := resp.Trailer.Get("X-Prefixed"); got != "prefixed-ok" {
		t.Fatalf("X-Prefixed trailer: got %q", got)
	}
}

func TestExpect100ContinueAndEarlyHints(t *testing.T) {
	srv := startServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/reject":
			http.Error(w, "denied", http.StatusForbidden)
		case "/hints":
			w.Header().Add("Link", "</app.css>; rel=preload")
			w.WriteHeader(http.StatusEarlyHints)
			w.Header().Del("Link")
			_, _ = io.WriteString(w, "final")
		default:
			data, err := io.ReadAll(r.Body)
			if err != nil {
				t.Errorf("ReadAll: %v", err)
				return
			}
			_, _ = w.Write(data)
		}
	}), Options{TLSConfig: tlsServerConfig(t)})

	client := newTLSClient(t)
	base := "https://" + srv.Addr().String()

	req, _ := http.NewRequest(http.MethodPost, base+"/accept", strings.NewReader("expected-body"))
	req.Header.Set("Expect", "100-continue")
	resp, err := client.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	got, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != 200 || string(got) != "expected-body" {
		t.Fatalf("Expect 100 accept: %d %q", resp.StatusCode, got)
	}

	// Unread large POST rejected immediately with 403 -> RST_STREAM(NO_ERROR) lets client finish cleanly.
	slowReader, slowWriter := io.Pipe()
	defer slowWriter.Close()
	go func() {
		_, _ = slowWriter.Write([]byte("part1"))
	}()
	req2, _ := http.NewRequest(http.MethodPost, base+"/reject", slowReader)
	resp2, err := client.Do(req2)
	if err != nil {
		t.Fatal(err)
	}
	resp2.Body.Close()
	if resp2.StatusCode != http.StatusForbidden {
		t.Fatalf("early reject: got %d", resp2.StatusCode)
	}

	var got103 int
	traceCtx := httptrace.WithClientTrace(context.Background(), &httptrace.ClientTrace{
		Got1xxResponse: func(code int, _ textproto.MIMEHeader) error {
			got103 = code
			return nil
		},
	})
	req3, _ := http.NewRequestWithContext(traceCtx, http.MethodGet, base+"/hints", nil)
	resp3, err := client.Do(req3)
	if err != nil {
		t.Fatal(err)
	}
	body3, _ := io.ReadAll(resp3.Body)
	resp3.Body.Close()
	if got103 != http.StatusEarlyHints || string(body3) != "final" {
		t.Fatalf("103 Early Hints: got1xx=%d body=%q", got103, body3)
	}
}

type syncBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *syncBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *syncBuffer) Len() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Len()
}

func TestHandlerPanicAndAbortHandler(t *testing.T) {
	defer func(w io.Writer, flags int) { log.SetOutput(w); log.SetFlags(flags) }(log.Writer(), log.Flags())
	defer slog.SetDefault(slog.Default())
	var logBuf syncBuffer
	slog.SetDefault(slog.New(slog.NewTextHandler(&logBuf, nil)))

	srv := startServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/panic":
			panic("boom")
		case "/abort":
			panic(http.ErrAbortHandler)
		default:
			_, _ = io.WriteString(w, "alive")
		}
	}), Options{TLSConfig: tlsServerConfig(t)})

	client := newTLSClient(t)
	base := "https://" + srv.Addr().String()

	if _, err := client.Get(base + "/abort"); err == nil {
		t.Fatal("expected ErrAbortHandler stream to fail")
	}
	if logBuf.Len() != 0 {
		t.Fatal("ErrAbortHandler should not log stack trace")
	}

	if _, err := client.Get(base + "/panic"); err == nil {
		t.Fatal("expected panicking stream to fail")
	}
	if logBuf.Len() == 0 {
		t.Fatal("expected panic to be logged")
	}

	resp, err := client.Get(base + "/ok")
	if err != nil {
		t.Fatal(err)
	}
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if string(body) != "alive" {
		t.Fatalf("subsequent stream failed: %q", body)
	}
}

func TestResponseContentLengthEnforcement(t *testing.T) {
	srv := startServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/overflow":
			w.Header().Set("Content-Length", "4")
			_, err := w.Write([]byte("12345"))
			if !errors.Is(err, http.ErrContentLength) {
				t.Errorf("expected ErrContentLength, got %v", err)
			}
			_, _ = w.Write([]byte("1234"))
		case "/short":
			w.Header().Set("Content-Length", "10")
			_, _ = w.Write([]byte("1234"))
		}
	}), Options{})

	client := newH2CClient(t)
	base := "http://" + srv.Addr().String()

	resp, err := client.Get(base + "/overflow")
	if err != nil {
		t.Fatal(err)
	}
	got, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if string(got) != "1234" {
		t.Fatalf("overflow recovery: got %q", got)
	}

	if resp2, err := client.Get(base + "/short"); err == nil {
		_, err = io.ReadAll(resp2.Body)
		resp2.Body.Close()
		if err == nil {
			t.Fatal("expected truncated Content-Length response to fail")
		}
	}
}

func TestStreamCancelCancelsRequestContext(t *testing.T) {
	started := make(chan struct{})
	canceled := make(chan error, 1)

	srv := startServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		close(started)
		<-r.Context().Done()
		canceled <- r.Context().Err()
	}), Options{})

	client := newH2CClient(t)
	ctx, cancel := context.WithCancel(context.Background())
	req, _ := http.NewRequestWithContext(ctx, http.MethodGet, "http://"+srv.Addr().String()+"/slow", nil)

	go func() {
		<-started
		cancel()
	}()
	_, err := client.Do(req)
	if err == nil {
		t.Fatal("expected canceled request to fail")
	}

	select {
	case ctxErr := <-canceled:
		if !errors.Is(ctxErr, context.Canceled) {
			t.Fatalf("unexpected handler context error: %v", ctxErr)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for server stream context cancellation")
	}
}

// rawH2Conn drives low-level wire-protocol conformance tests against http2.
type rawH2Conn struct {
	test          *testing.T
	netConnection net.Conn
	hpackEncoder  *hpack.Encoder
	encodeBuf     bytes.Buffer
}

func dialRawH2(t *testing.T, addr string) *rawH2Conn {
	t.Helper()
	nc, err := net.DialTimeout("tcp", addr, 3*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = nc.Close() })
	_ = nc.SetDeadline(time.Now().Add(5 * time.Second))

	rc := &rawH2Conn{test: t, netConnection: nc}
	rc.hpackEncoder = hpack.NewEncoder(&rc.encodeBuf)
	return rc
}

func (r *rawH2Conn) greet() {
	r.test.Helper()
	var out bytepool.Buffer
	defer out.Release()
	out.WriteString(clientPreface)
	appendSettings(&out)
	if _, err := r.netConnection.Write(out.Bytes()); err != nil {
		r.test.Fatal(err)
	}
	for {
		fh, _ := r.readFrame()
		if fh.frameType == frameSettings && fh.flags&flagAck == 0 {
			var ack bytepool.Buffer
			appendSettingsAck(&ack)
			_, _ = r.netConnection.Write(ack.Bytes())
			ack.Release()
		}
		if fh.frameType == frameSettings && fh.flags&flagAck != 0 {
			return
		}
	}
}

func (r *rawH2Conn) readFrame() (frameHeader, []byte) {
	r.test.Helper()
	var hdr [frameHeaderLen]byte
	if _, err := io.ReadFull(r.netConnection, hdr[:]); err != nil {
		r.test.Fatalf("readFrame header: %v", err)
	}
	fh := parseFrameHeader(hdr[:])
	payload := make([]byte, fh.length)
	if _, err := io.ReadFull(r.netConnection, payload); err != nil {
		r.test.Fatalf("readFrame payload (%+v): %v", fh, err)
	}
	return fh, payload
}

func (r *rawH2Conn) encodeFields(kvs ...string) []byte {
	r.encodeBuf.Reset()
	for i := 0; i < len(kvs); i += 2 {
		_ = r.hpackEncoder.WriteField(hpack.HeaderField{Name: kvs[i], Value: kvs[i+1]})
	}
	return bytes.Clone(r.encodeBuf.Bytes())
}

func TestProtocolValidationAndLimits(t *testing.T) {
	var active atomic.Int32
	release := make(chan struct{})
	defer close(release)

	srv := startServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/hold":
			active.Add(1)
			<-release
		case "/big-resp-hdr":
			w.Header().Set("X-Big-Resp", strings.Repeat("r", 32<<10))
			_, _ = io.WriteString(w, "ok")
		case "/close":
			w.Header().Set("Connection", "close")
			_, _ = io.WriteString(w, "bye")
		default:
			_, _ = io.WriteString(w, "ok")
		}
	}), Options{
		MaxConcurrentStreams: 1,
		MaxHeaderBytes:       2048,
	})

	t.Run("PingAndSplitFrames", func(t *testing.T) {
		rc := dialRawH2(t, srv.Addr().String())
		var initBuf bytepool.Buffer
		initBuf.WriteString(clientPreface)
		appendSettings(&initBuf)
		for _, b := range initBuf.Bytes() {
			if _, err := rc.netConnection.Write([]byte{b}); err != nil {
				t.Fatal(err)
			}
		}
		initBuf.Release()

		for {
			fh, _ := rc.readFrame()
			if fh.frameType == frameSettings && fh.flags&flagAck != 0 {
				break
			}
		}

		var pingBuf bytepool.Buffer
		appendPing(&pingBuf, false, []byte("12345678"))
		_, _ = rc.netConnection.Write(pingBuf.Bytes())
		pingBuf.Release()

		fh, payload := rc.readFrame()
		if fh.frameType != framePing || fh.flags != flagAck || string(payload) != "12345678" {
			t.Fatalf("PING ACK: fh=%+v payload=%q", fh, payload)
		}
	})

	t.Run("ContinuationBothDirectionsAndOptionsStar", func(t *testing.T) {
		rc := dialRawH2(t, srv.Addr().String())
		rc.greet()

		// Stream 1: OPTIONS * split across HEADERS + 2 CONTINUATION frames.
		optBlock := rc.encodeFields(
			":method", "OPTIONS",
			":scheme", "http",
			":authority", "localhost",
			":path", "*",
			"x-pad", strings.Repeat("p", 100),
		)
		var out bytepool.Buffer
		appendHeaders(&out, 1, optBlock, true, 12) // split into 12-byte fragments
		_, _ = rc.netConnection.Write(out.Bytes())
		out.Release()

		fh, payload := rc.readFrame()
		if fh.frameType != frameHeaders || fh.streamID != 1 || fh.flags&flagEndStream == 0 {
			t.Fatalf("OPTIONS * expected HEADERS(END_STREAM), got %+v", fh)
		}
		var gotResp []hpack.HeaderField
		dec := hpack.NewDecoder(4096, func(hf hpack.HeaderField) {
			gotResp = append(gotResp, hf)
		})
		fields, err := dec.DecodeFull(payload)
		if err != nil {
			t.Fatal(err)
		}
		if len(fields) == 0 || fields[0].Name != ":status" || fields[0].Value != "200" {
			t.Fatalf("OPTIONS * status: %v", fields)
		}

		// Stream 3: response headers > 16KB split by server across HEADERS + CONTINUATION.
		reqBlock := rc.encodeFields(":method", "GET", ":scheme", "http", ":authority", "localhost", ":path", "/big-resp-hdr")
		appendHeaders(&out, 3, reqBlock, true, minMaxFrameSize)
		_, _ = rc.netConnection.Write(out.Bytes())
		out.Release()

		fh1, p1 := rc.readFrame()
		if fh1.frameType != frameHeaders || fh1.streamID != 3 || fh1.flags&flagEndHeaders != 0 {
			t.Fatalf("expected initial HEADERS without END_HEADERS, got %+v", fh1)
		}
		fh2, p2 := rc.readFrame()
		if fh2.frameType != frameContinuation || fh2.streamID != 3 || fh2.flags&flagEndHeaders == 0 {
			t.Fatalf("expected CONTINUATION with END_HEADERS, got %+v", fh2)
		}
		gotResp = nil
		if _, err := dec.Write(p1); err != nil {
			t.Fatal(err)
		}
		if _, err := dec.Write(p2); err != nil {
			t.Fatal(err)
		}
		if err := dec.Close(); err != nil {
			t.Fatal(err)
		}
		foundBig := false
		for _, f := range gotResp {
			if f.Name == "x-big-resp" && len(f.Value) == 32<<10 {
				foundBig = true
			}
		}
		if !foundBig {
			t.Fatalf("did not decode 32KB x-big-resp across CONTINUATION: %d fields", len(gotResp))
		}
	})

	t.Run("RejectUppercaseAndConnectionHeaders", func(t *testing.T) {
		rc := dialRawH2(t, srv.Addr().String())
		rc.greet()

		hdrBlock := rc.encodeFields(
			":method", "GET",
			":scheme", "http",
			":authority", "localhost",
			":path", "/",
			"connection", "keep-alive",
		)
		var out bytepool.Buffer
		appendHeaders(&out, 1, hdrBlock, true, minMaxFrameSize)
		_, _ = rc.netConnection.Write(out.Bytes())
		out.Release()

		fh, payload := rc.readFrame()
		if fh.frameType != frameRSTStream || fh.streamID != 1 || errCode(binary.BigEndian.Uint32(payload)) != errCodeProtocol {
			t.Fatalf("expected RST_STREAM(PROTOCOL_ERROR), got %+v payload=%x", fh, payload)
		}

		// Stream 3: uppercase header name -> RST_STREAM(PROTOCOL_ERROR).
		upperBlock := rc.encodeFields(
			":method", "GET",
			":scheme", "http",
			":authority", "localhost",
			":path", "/",
			"X-Upper", "bad",
		)
		appendHeaders(&out, 3, upperBlock, true, minMaxFrameSize)
		_, _ = rc.netConnection.Write(out.Bytes())
		out.Release()

		fh, payload = rc.readFrame()
		if fh.frameType != frameRSTStream || fh.streamID != 3 || errCode(binary.BigEndian.Uint32(payload)) != errCodeProtocol {
			t.Fatalf("expected RST_STREAM(PROTOCOL_ERROR) for uppercase header, got %+v", fh)
		}

		// Stream 5: header list exceeding MaxHeaderBytes (2048B) -> 431 response.
		bigBlock := rc.encodeFields(
			":method", "GET",
			":scheme", "http",
			":authority", "localhost",
			":path", "/",
			"x-large", strings.Repeat("a", 2200),
		)
		appendHeaders(&out, 5, bigBlock, true, minMaxFrameSize)
		_, _ = rc.netConnection.Write(out.Bytes())
		out.Release()

		fh, payload = rc.readFrame()
		if fh.frameType != frameHeaders || fh.streamID != 5 {
			t.Fatalf("expected 431 HEADERS on stream 5, got %+v", fh)
		}
		dec := hpack.NewDecoder(4096, nil)
		fields, err := dec.DecodeFull(payload)
		if err != nil {
			t.Fatal(err)
		}
		got431 := false
		for _, f := range fields {
			if f.Name == ":status" && f.Value == "431" {
				got431 = true
			}
		}
		if !got431 {
			t.Fatalf("expected :status 431, got %v", fields)
		}
	})

	t.Run("MaxConcurrentStreamsRefused", func(t *testing.T) {
		rc := dialRawH2(t, srv.Addr().String())
		rc.greet()

		holdBlock := rc.encodeFields(":method", "GET", ":scheme", "http", ":authority", "localhost", ":path", "/hold")
		var out bytepool.Buffer
		appendHeaders(&out, 1, holdBlock, true, minMaxFrameSize)
		_, _ = rc.netConnection.Write(out.Bytes())
		out.Release()

		for active.Load() == 0 {
			time.Sleep(time.Millisecond)
		}

		block2 := rc.encodeFields(":method", "GET", ":scheme", "http", ":authority", "localhost", ":path", "/")
		appendHeaders(&out, 3, block2, true, minMaxFrameSize)
		_, _ = rc.netConnection.Write(out.Bytes())
		out.Release()

		fh, payload := rc.readFrame()
		if fh.frameType != frameRSTStream || fh.streamID != 3 || errCode(binary.BigEndian.Uint32(payload)) != errCodeRefusedStream {
			t.Fatalf("expected REFUSED_STREAM on stream 3, got %+v code=%v", fh, binary.BigEndian.Uint32(payload))
		}
	})

	t.Run("ConnectionCloseSendsGoAway", func(t *testing.T) {
		rc := dialRawH2(t, srv.Addr().String())
		rc.greet()

		block := rc.encodeFields(":method", "GET", ":scheme", "http", ":authority", "localhost", ":path", "/close")
		var out bytepool.Buffer
		appendHeaders(&out, 1, block, true, minMaxFrameSize)
		_, _ = rc.netConnection.Write(out.Bytes())
		out.Release()

		sawGoAway := false
		sawEnd := false
		for !(sawGoAway && sawEnd) {
			fh, _ := rc.readFrame()
			if fh.frameType == frameGoAway {
				sawGoAway = true
			}
			if (fh.frameType == frameData || fh.frameType == frameHeaders) && fh.streamID == 1 && fh.flags&flagEndStream != 0 {
				sawEnd = true
			}
		}
		var one [1]byte
		if _, err := rc.netConnection.Read(one[:]); !errors.Is(err, io.EOF) {
			t.Fatalf("expected EOF after graceful GOAWAY close, got %v", err)
		}
	})

	t.Run("RapidResetEnhanceYourCalm", func(t *testing.T) {
		rc := dialRawH2(t, srv.Addr().String())
		rc.greet()

		var batch bytepool.Buffer
		defer batch.Release()
		for i := range maxResetBurst + 5 {
			streamID := uint32(2*i + 1)
			block := rc.encodeFields(":method", "GET", ":scheme", "http", ":authority", "localhost", ":path", "/hold")
			appendHeaders(&batch, streamID, block, false, minMaxFrameSize)
			appendRSTStream(&batch, streamID, errCodeCancel)
		}
		_, _ = rc.netConnection.Write(batch.Bytes())

		gotEnhance := false
		for {
			var hdr [frameHeaderLen]byte
			if _, err := io.ReadFull(rc.netConnection, hdr[:]); err != nil {
				break
			}
			fh := parseFrameHeader(hdr[:])
			payload := make([]byte, fh.length)
			if _, err := io.ReadFull(rc.netConnection, payload); err != nil {
				break
			}
			if fh.frameType == frameGoAway && errCode(binary.BigEndian.Uint32(payload[4:8])) == errCodeEnhanceYourCalm {
				gotEnhance = true
				break
			}
		}
		if !gotEnhance {
			t.Fatal("expected GOAWAY(ENHANCE_YOUR_CALM) under rapid reset flood")
		}
	})

	t.Run("FrameMidContinuationClosesConnection", func(t *testing.T) {
		rc := dialRawH2(t, srv.Addr().String())
		rc.greet()

		block := rc.encodeFields(":method", "GET", ":scheme", "http", ":authority", "localhost", ":path", "/")
		var out bytepool.Buffer
		appendFrameHeader(&out, len(block), frameHeaders, flagEndStream, 1)
		out.Write(block)
		appendPing(&out, false, []byte("badorder"))
		_, _ = rc.netConnection.Write(out.Bytes())
		out.Release()

		fh, payload := rc.readFrame()
		if fh.frameType != frameGoAway || errCode(binary.BigEndian.Uint32(payload[4:8])) != errCodeProtocol {
			t.Fatalf("expected GOAWAY(PROTOCOL_ERROR), got %+v", fh)
		}
	})
}

func TestReadHeaderAndIdleTimeouts(t *testing.T) {
	srv := startServer(t, testMux(), Options{
		ReadHeaderTimeout: 150 * time.Millisecond,
		IdleTimeout:       150 * time.Millisecond,
	})

	// 1. Stalled connection that never finishes client preface is closed by ReadHeaderTimeout.
	stalled, err := net.DialTimeout("tcp", srv.Addr().String(), 2*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer stalled.Close()
	_ = stalled.SetDeadline(time.Now().Add(3 * time.Second))
	_, _ = stalled.Write([]byte("PRI * "))
	if _, err := io.Copy(io.Discard, stalled); err != nil {
		t.Fatalf("expected clean close on ReadHeaderTimeout, got %v", err)
	}

	// 2. Idle connection after SETTINGS handshake receives GOAWAY(NO_ERROR) when IdleTimeout expires.
	rc := dialRawH2(t, srv.Addr().String())
	rc.greet()
	fh, payload := rc.readFrame()
	if fh.frameType != frameGoAway || errCode(binary.BigEndian.Uint32(payload[4:8])) != errCodeNo {
		t.Fatalf("expected GOAWAY(NO_ERROR) on IdleTimeout, got %+v", fh)
	}
}
