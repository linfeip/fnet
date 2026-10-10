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
			c.Write([]byte("x"))
			if err := wait(t, h.closed); err != fnet.ErrHandlerPanic {
				t.Fatalf("OnClose err = %v, want ErrHandlerPanic", err)
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
