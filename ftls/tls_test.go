package ftls

import (
	"bytes"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"io"
	"math/big"
	"net"
	"os"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/linfeip/fnet"
)

// tlsServerConfig returns a server configuration with a self-signed certificate, which the clients do not verify.
func tlsServerConfig(t *testing.T) *tls.Config {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	template := &x509.Certificate{SerialNumber: big.NewInt(1)}
	der, err := x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	return &tls.Config{Certificates: []tls.Certificate{{Certificate: [][]byte{der}, PrivateKey: key}}}
}

// testHandler echoes what it receives unless onData says otherwise, and reports the connections it opens and closes.
type testHandler struct {
	opened chan *Conn
	closed chan error
	onOpen func(c fnet.Conn)
	onData func(c fnet.Conn, data []byte) int
}

func newTestHandler() *testHandler {
	return &testHandler{opened: make(chan *Conn, 16), closed: make(chan error, 16)}
}

func (h *testHandler) OnOpen(c fnet.Conn) {
	h.opened <- c.(*Conn)
	if h.onOpen != nil {
		h.onOpen(c)
	}
}

func (h *testHandler) OnData(c fnet.Conn, data []byte) int {
	if h.onData != nil {
		return h.onData(c, data)
	}
	c.Write(data)
	return len(data)
}

func (h *testHandler) OnClose(c fnet.Conn, err error) { h.closed <- err }

func serve(t *testing.T, h fnet.Handler, handshakeTimeout time.Duration) *fnet.Server {
	t.Helper()
	srv, err := fnet.NewServer("127.0.0.1:0", NewHandler(h, tlsServerConfig(t), handshakeTimeout), fnet.Options{})
	if err != nil {
		t.Fatal(err)
	}
	go srv.Serve()
	t.Cleanup(func() { srv.Close() })
	return srv
}

func dial(t *testing.T, srv *fnet.Server) *tls.Conn {
	t.Helper()
	c, err := tls.Dial("tcp", srv.Addr().String(), &tls.Config{InsecureSkipVerify: true})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { c.Close() })
	return c
}

func wait[T any](t *testing.T, ch <-chan T) T {
	t.Helper()
	select {
	case v := <-ch:
		return v
	case <-time.After(5 * time.Second):
		t.Fatal("timed out")
		panic("unreachable")
	}
}

func readN(t *testing.T, r io.Reader, n int) []byte {
	t.Helper()
	b := make([]byte, n)
	if _, err := io.ReadFull(r, b); err != nil {
		t.Fatal(err)
	}
	return b
}

func TestEcho(t *testing.T) {
	h := newTestHandler()
	c := dial(t, serve(t, h, 0))
	wait(t, h.opened)

	c.Write([]byte("hello"))
	if got := readN(t, c, 5); string(got) != "hello" {
		t.Fatalf("got %q", got)
	}

	// Larger than a record and than the engine's read buffer, written while the echo comes back.
	payload := make([]byte, 512*1024)
	for i := range payload {
		payload[i] = byte(i % 251)
	}
	go c.Write(payload)
	if got := readN(t, c, len(payload)); !bytes.Equal(got, payload) {
		t.Fatal("payload mismatch")
	}

	c.Close() // close_notify
	if err := wait(t, h.closed); err != io.EOF {
		t.Fatalf("OnClose err = %v, want io.EOF", err)
	}
}

// TestSlowOnOpen sends data while OnOpen is still running: the engine holds it back meanwhile, and all of it is passed
// on once OnOpen returns.
func TestSlowOnOpen(t *testing.T) {
	h := newTestHandler()
	h.onOpen = func(fnet.Conn) { time.Sleep(200 * time.Millisecond) }
	c := dial(t, serve(t, h, 0))
	payload := bytes.Repeat([]byte("0123456789abcdef"), 64*1024) // 1MB
	go c.Write(payload)
	if got := readN(t, c, len(payload)); !bytes.Equal(got, payload) {
		t.Fatal("payload mismatch")
	}
}

func TestServerClose(t *testing.T) {
	h := newTestHandler()
	h.onData = func(c fnet.Conn, data []byte) int {
		c.Close()
		return len(data)
	}
	c := dial(t, serve(t, h, 0))
	c.Write([]byte("bye"))
	if n, err := c.Read(make([]byte, 1)); err != io.EOF {
		t.Fatalf("client read %d, %v, want io.EOF from close_notify", n, err)
	}
	if err := wait(t, h.closed); err != nil {
		t.Fatalf("OnClose err = %v, want nil", err)
	}
}

func TestShutdown(t *testing.T) {
	h := newTestHandler()
	srv := serve(t, h, 0)
	dial(t, srv)
	wait(t, h.opened)
	srv.Close()
	if err := wait(t, h.closed); err != fnet.ErrServerClosed {
		t.Fatalf("OnClose err = %v, want ErrServerClosed", err)
	}
}

// recordingConn keeps what the client reads from the server.
type recordingConn struct {
	net.Conn
	read bytes.Buffer
}

func (c *recordingConn) Read(b []byte) (int, error) {
	n, err := c.Conn.Read(b)
	c.read.Write(b[:n])
	return n, err
}

// TestCloseNotify checks that a connection the engine closes on its own still sends close_notify (RFC 9112 9.8): after the
// handshake the client reads an alert record, then EOF. crypto/tls takes a bare EOF for a close as well, so the record
// is looked at directly, over TLS 1.2, which leaves the record type of an alert in the clear.
func TestCloseNotify(t *testing.T) {
	cases := []struct {
		closedBy string
		reason   error
	}{
		{"peer", io.EOF}, // without close_notify of its own
		{"deadline", os.ErrDeadlineExceeded},
		{"server", fnet.ErrServerClosed},
	}
	for _, tc := range cases {
		t.Run(tc.closedBy, func(t *testing.T) {
			if tc.closedBy == "server" && runtime.GOOS == "windows" {
				t.Skip("the standard library implementation closes at once when the server closes")
			}
			h := newTestHandler()
			if tc.closedBy == "deadline" {
				h.onOpen = func(c fnet.Conn) { c.SetDeadline(time.Now().Add(50 * time.Millisecond)) }
			}
			srv := serve(t, h, 0)
			raw, err := net.Dial("tcp", srv.Addr().String())
			if err != nil {
				t.Fatal(err)
			}
			defer raw.Close()
			recorded := &recordingConn{Conn: raw}
			c := tls.Client(recorded, &tls.Config{InsecureSkipVerify: true, MaxVersion: tls.VersionTLS12})
			if err := c.Handshake(); err != nil {
				t.Fatal(err)
			}
			wait(t, h.opened)
			handshake := recorded.read.Len()
			switch tc.closedBy {
			case "peer":
				raw.(*net.TCPConn).CloseWrite()
			case "server":
				srv.Close()
			}
			c.SetReadDeadline(time.Now().Add(5 * time.Second))
			if _, err := c.Read(make([]byte, 1)); err != io.EOF {
				t.Fatalf("client read %v, want io.EOF", err)
			}
			const alert = 21 // the content type of an alert record
			if rest := recorded.read.Bytes()[handshake:]; len(rest) == 0 || rest[0] != alert {
				t.Fatalf("the server sent %x after the handshake, want close_notify", rest)
			}
			if err := wait(t, h.closed); err != tc.reason {
				t.Fatalf("OnClose err = %v, want %v", err, tc.reason)
			}
		})
	}
}

// TestWritev writes frames from several goroutines at once: each Writev must arrive whole.
func TestWritev(t *testing.T) {
	const writers, size = 8, 20 * 1024 // a frame takes more than one record
	h := newTestHandler()
	h.onData = func(c fnet.Conn, data []byte) int {
		for i := range writers {
			go func() {
				c.Writev([][]byte{{byte(i)}, bytes.Repeat([]byte{byte(i)}, size), {'\n'}})
			}()
		}
		return len(data)
	}
	c := dial(t, serve(t, h, 0))
	c.Write([]byte("go"))
	seen := map[byte]bool{}
	for range writers {
		frame := readN(t, c, size+2)
		id := frame[0]
		if !bytes.Equal(frame[1:size+1], bytes.Repeat([]byte{id}, size)) || frame[size+1] != '\n' || seen[id] {
			t.Fatalf("frame %d interleaved with another write", id)
		}
		seen[id] = true
	}
}

func TestHandshakeFailure(t *testing.T) {
	h := newTestHandler()
	srv := serve(t, h, 100*time.Millisecond)
	// A client that stays silent, one that stops halfway through its ClientHello, and one that is no TLS client.
	for _, hello := range []string{"", "\x16\x03\x01\x01\x00\x01", "not a client hello"} {
		c, err := net.Dial("tcp", srv.Addr().String())
		if err != nil {
			t.Fatal(err)
		}
		c.Write([]byte(hello))
		c.SetReadDeadline(time.Now().Add(5 * time.Second)) // the engine checks deadlines once per second
		if _, err := io.ReadAll(c); err != nil {
			t.Fatalf("hello %q: connection not closed by the server: %v", hello, err)
		}
		c.Close()
	}
	select {
	case <-h.opened:
		t.Fatal("handler opened a connection whose handshake failed")
	default:
	}
	// The handshake coroutines are gone, the one stopped halfway through included.
	for deadline := time.Now().Add(5 * time.Second); ; time.Sleep(10 * time.Millisecond) {
		buf := make([]byte, 1<<20)
		if !bytes.Contains(buf[:runtime.Stack(buf, true)], []byte("ftls.(*Conn).handshake")) {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("handshake coroutine left behind:\n%s", buf)
		}
	}
}

func TestPanic(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("the standard library implementation does not recover a panic in a callback (see fnet.ErrHandlerPanic)")
	}
	for _, in := range []string{"OnOpen", "OnData"} {
		t.Run(in, func(t *testing.T) {
			var panicked atomic.Bool // only the first connection panics
			h := newTestHandler()
			h.onOpen = func(fnet.Conn) {
				if in == "OnOpen" && panicked.CompareAndSwap(false, true) {
					panic("test panic")
				}
			}
			h.onData = func(c fnet.Conn, data []byte) int {
				if in == "OnData" && panicked.CompareAndSwap(false, true) {
					panic("test panic")
				}
				c.Write(data)
				return len(data)
			}
			srv := serve(t, h, 0)
			c := dial(t, srv)
			serverConn := wait(t, h.opened)
			c.Write([]byte("x"))
			if err := wait(t, h.closed); err != fnet.ErrHandlerPanic {
				t.Fatalf("OnClose err = %v, want ErrHandlerPanic", err)
			}
			if serverConn.ciphertext != nil {
				t.Error("ciphertext still references the engine's read buffer after a callback panic")
			}
			c = dial(t, srv) // the server keeps serving
			c.Write([]byte("ok"))
			if got := readN(t, c, 2); string(got) != "ok" {
				t.Fatalf("got %q", got)
			}
		})
	}
}

func TestPauseRead(t *testing.T) {
	var mu sync.Mutex
	var received []string
	paused := make(chan struct{})
	h := newTestHandler()
	h.onData = func(c fnet.Conn, data []byte) int {
		mu.Lock()
		defer mu.Unlock()
		received = append(received, string(data))
		if len(received) == 1 {
			c.PauseRead()
			close(paused)
		}
		return len(data)
	}
	c := dial(t, serve(t, h, 0))
	conn := wait(t, h.opened)
	c.Write([]byte("a"))
	wait(t, paused)
	c.Write([]byte("b"))
	time.Sleep(200 * time.Millisecond)
	mu.Lock()
	if len(received) != 1 {
		t.Fatalf("OnData while paused: %q", received)
	}
	mu.Unlock()
	conn.ResumeRead()
	for deadline := time.Now().Add(5 * time.Second); ; time.Sleep(10 * time.Millisecond) {
		mu.Lock()
		got := received
		mu.Unlock()
		if len(got) == 2 && got[1] == "b" {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("received %q after ResumeRead", got)
		}
	}
}

// heldConn holds back what the client writes once it has read from the server, until flush: over TLS 1.3 the client's
// Finished and its first data then arrive together.
type heldConn struct {
	net.Conn
	read bool
	held []byte
}

func (c *heldConn) Read(b []byte) (int, error) {
	c.read = true
	return c.Conn.Read(b)
}

func (c *heldConn) Write(b []byte) (int, error) {
	if !c.read {
		return c.Conn.Write(b)
	}
	c.held = append(c.held, b...)
	return len(b), nil
}

func (c *heldConn) flush() error {
	_, err := c.Conn.Write(c.held)
	c.held = nil
	return err
}

// TestResumeDeliversKept has the handler pause reading in OnOpen while the client's first data arrives with the end of
// the handshake: that data, decrypted while paused, comes as soon as reading resumes, though the client, waiting for an
// answer, sends nothing more.
func TestResumeDeliversKept(t *testing.T) {
	received := make(chan string, 1)
	h := newTestHandler()
	h.onOpen = func(c fnet.Conn) { c.PauseRead() }
	h.onData = func(c fnet.Conn, data []byte) int {
		received <- string(data)
		return len(data)
	}
	srv := serve(t, h, 0)
	raw, err := net.Dial("tcp", srv.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer raw.Close()
	held := &heldConn{Conn: raw}
	c := tls.Client(held, &tls.Config{InsecureSkipVerify: true, MinVersion: tls.VersionTLS13})
	if err := c.Handshake(); err != nil {
		t.Fatal(err)
	}
	c.Write([]byte("hello"))
	if err := held.flush(); err != nil {
		t.Fatal(err)
	}
	conn := wait(t, h.opened)
	time.Sleep(100 * time.Millisecond) // "hello" has been decrypted meanwhile, and kept
	select {
	case got := <-received:
		t.Fatalf("OnData while paused: %q", got)
	default:
	}
	conn.ResumeRead()
	if got := wait(t, received); got != "hello" {
		t.Fatalf("OnData after ResumeRead got %q, want %q", got, "hello")
	}
}

// TestResumeRedeliversUnconsumed has the handler leave its data unconsumed and pause reading, which is resumed before
// OnData returns, as fhttp's worker may do: the data comes again as soon as reading resumes, though the client, waiting
// for an answer, sends nothing more.
func TestResumeRedeliversUnconsumed(t *testing.T) {
	received := make(chan string, 2)
	h := newTestHandler()
	first := true // accessed only by the connection's callbacks, which run serially
	h.onData = func(c fnet.Conn, data []byte) int {
		received <- string(data)
		if first {
			first = false
			c.PauseRead()
			c.ResumeRead()
			return 0
		}
		return len(data)
	}
	c := dial(t, serve(t, h, 0))
	c.Write([]byte("hello"))
	for range 2 {
		if got := wait(t, received); got != "hello" {
			t.Fatalf("OnData got %q, want %q", got, "hello")
		}
	}
}

// halfCloseHandler is a testHandler that keeps the connections open for a peer that closed its write direction, and
// counts how often it is asked to.
type halfCloseHandler struct {
	*testHandler
	asked atomic.Int32
}

func (h *halfCloseHandler) OnHalfClose(fnet.Conn) bool { h.asked.Add(1); return true }

// TestHalfCloseAskedOnce has the peer send close_notify, go on sending at the TCP level, which no longer reaches the
// handler, and then close its write direction: the handler is asked to keep the connection open once, however much
// follows, as fnet asks once on a FIN.
func TestHalfCloseAskedOnce(t *testing.T) {
	h := &halfCloseHandler{testHandler: newTestHandler()}
	srv := serve(t, h, 0)
	raw, err := net.Dial("tcp", srv.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer raw.Close()
	c := tls.Client(raw, &tls.Config{InsecureSkipVerify: true})
	if err := c.Handshake(); err != nil {
		t.Fatal(err)
	}
	wait(t, h.opened)
	if err := c.CloseWrite(); err != nil {
		t.Fatal(err)
	}
	raw.SetWriteDeadline(time.Time{}) // CloseWrite sets it to now
	for range 20 {
		if _, err := raw.Write([]byte{23, 3, 3, 0, 1, 0}); err != nil { // a record header, each a segment of its own
			t.Fatal(err)
		}
		time.Sleep(5 * time.Millisecond)
	}
	raw.(*net.TCPConn).CloseWrite()
	time.Sleep(100 * time.Millisecond)
	if n := h.asked.Load(); n != 1 {
		t.Fatalf("OnHalfClose was called %d times, want 1", n)
	}
}

// TestWriteAfterPeerClose checks that a Write after the engine closed the connection on its own (the peer closed it
// without close_notify, and OnClosing sent one) returns net.ErrClosed, as on fnet, rather than crypto/tls's own error.
func TestWriteAfterPeerClose(t *testing.T) {
	h := newTestHandler()
	c := dial(t, serve(t, h, 0))
	conn := wait(t, h.opened)
	c.NetConn().Close()
	wait(t, h.closed)
	if _, err := conn.Write([]byte("x")); !errors.Is(err, net.ErrClosed) {
		t.Fatalf("Write after the engine closed the connection: %v, want net.ErrClosed", err)
	}
}

func TestDetach(t *testing.T) {
	h := newTestHandler()
	h.onData = func(c fnet.Conn, data []byte) int {
		c.PauseRead()
		return len("hello ")
	}
	c := dial(t, serve(t, h, 0))
	conn := wait(t, h.opened)
	c.Write([]byte("hello world"))
	var nc net.Conn
	for deadline := time.Now().Add(5 * time.Second); nc == nil; time.Sleep(10 * time.Millisecond) {
		if conn.readPaused.Load() {
			var err error
			if nc, err = conn.Detach(); err != nil {
				t.Fatal(err)
			}
		} else if time.Now().After(deadline) {
			t.Fatal("reading was not paused")
		}
	}
	defer nc.Close()
	if _, err := conn.Write([]byte("x")); !errors.Is(err, net.ErrClosed) {
		t.Fatalf("Write after Detach: %v, want net.ErrClosed", err)
	}

	if got := readN(t, nc, 5); string(got) != "world" {
		t.Fatalf("detached read %q, want the unconsumed plaintext", got)
	}
	c.Write([]byte("more"))
	if got := readN(t, nc, 4); string(got) != "more" {
		t.Fatalf("detached read %q", got)
	}
	nc.Write([]byte("ok"))
	if got := readN(t, c, 2); string(got) != "ok" {
		t.Fatalf("client read %q", got)
	}
	nc.Close()
	if n, err := c.Read(make([]byte, 1)); err != io.EOF {
		t.Fatalf("client read %d, %v after the detached connection closed, want io.EOF", n, err)
	}
	select {
	case err := <-h.closed:
		t.Fatalf("OnClose(%v) after Detach", err)
	default:
	}
}

// openConn is the little of an fnet.Conn that Handler.OnOpen and OnClose ask for.
type openConn struct {
	fnet.Conn
	ctx any
}

func (c *openConn) Context() any          { return c.ctx }
func (c *openConn) SetContext(ctx any)    { c.ctx = ctx }
func (c *openConn) SetDeadline(time.Time) {}
func (c *openConn) RemoteAddr() net.Addr  { return &net.TCPAddr{} }
func (c *openConn) LocalAddr() net.Addr   { return &net.TCPAddr{} }

// TestSilentConnections checks that a connection that has sent nothing costs no goroutine: the handshake's coroutine
// starts with the first data, so peers that connect and keep quiet do not hold a stack each until the handshake times out.
// One closed before that has no coroutine to end.
func TestSilentConnections(t *testing.T) {
	h := NewHandler(newTestHandler(), tlsServerConfig(t), time.Minute)
	const conns = 200
	before := runtime.NumGoroutine()
	opened := make([]*openConn, conns)
	for i := range opened {
		opened[i] = &openConn{}
		h.OnOpen(opened[i])
	}
	if grown := runtime.NumGoroutine() - before; grown > conns/10 {
		t.Fatalf("%d silent connections started %d goroutines", conns, grown)
	}
	for _, c := range opened {
		h.OnClose(c, nil) // the handshake never started: nothing to end
	}
}
