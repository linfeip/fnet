package reactor

import (
	"bytes"
	"errors"
	"io"
	"net"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/linfeip/fnet/internal/netpoll"
)

// detachedConn is a socketless Conn in blocking-reader mode: writes queue and
// input is fed by hand.
func detachedConn() *Conn {
	return &Conn{fd: -1, detached: true}
}

func (c *Conn) feed(b []byte) { c.deliver(b) }

// drain removes up to len(dst) queued output bytes.
func (c *Conn) drain(dst []byte) int {
	c.wmu.Lock()
	defer c.wmu.Unlock()
	n := copy(dst, c.out.B[c.outR:c.outW])
	c.outR += n
	if c.outR == c.outW {
		c.releaseOutLocked()
	}
	if c.outW-c.outR <= lowWatermark {
		c.clearState(stPausedOut)
	}
	return n
}

func TestConnReadWrite(t *testing.T) {
	c := detachedConn()
	got := make(chan string, 1)
	go func() {
		buf := make([]byte, 64)
		n, err := c.Read(buf)
		if err != nil {
			t.Errorf("Read: %v", err)
		}
		got <- string(buf[:n])
	}()
	time.Sleep(20 * time.Millisecond)
	c.feed([]byte("hello"))
	if s := <-got; s != "hello" {
		t.Fatalf("got %q", s)
	}

	if n, err := c.Write([]byte("wor")); n != 3 || err != nil {
		t.Fatalf("Write: %d %v", n, err)
	}
	if n, err := c.Writev([][]byte{[]byte("l"), nil, []byte("d")}); n != 2 || err != nil {
		t.Fatalf("Writev: %d %v", n, err)
	}
	out := make([]byte, 16)
	if n := c.drain(out); string(out[:n]) != "world" {
		t.Fatalf("queued %q", out[:n])
	}
}

func TestConnPeerEOF(t *testing.T) {
	c := detachedConn()
	c.feed([]byte("ab"))
	c.onPeerEOF()
	buf := make([]byte, 8)
	if n, err := c.Read(buf); err != nil || string(buf[:n]) != "ab" {
		t.Fatalf("first read: %q %v", buf[:n], err)
	}
	if n, err := c.Read(buf); err != io.EOF || n != 0 {
		t.Fatalf("second read: %d %v", n, err)
	}
	if n, err := c.Write([]byte("reply")); err != nil || n != 5 {
		t.Fatalf("write after peer EOF (half-close): %d %v", n, err)
	}
}

func TestConnCloseUnblocksRead(t *testing.T) {
	c := detachedConn()
	done := make(chan error, 1)
	go func() {
		_, err := c.Read(make([]byte, 8))
		done <- err
	}()
	time.Sleep(20 * time.Millisecond)
	_ = c.Close()
	select {
	case err := <-done:
		if err != io.EOF {
			t.Fatalf("want EOF, got %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Read did not unblock")
	}
	if _, err := c.Write([]byte("x")); !errors.Is(err, net.ErrClosed) {
		t.Fatalf("write after Close: %v", err)
	}
}

func TestConnReadDeadline(t *testing.T) {
	c := detachedConn()
	_ = c.SetReadDeadline(time.Now().Add(30 * time.Millisecond))
	if _, err := c.Read(make([]byte, 8)); !errors.Is(err, os.ErrDeadlineExceeded) {
		t.Fatalf("want deadline error, got %v", err)
	}
}

func TestConnReadRequiresDetach(t *testing.T) {
	c := &Conn{fd: -1}
	if _, err := c.Read(make([]byte, 8)); !errors.Is(err, errHandlerOwned) {
		t.Fatalf("Read in handler mode: %v", err)
	}
}

func TestConnUnread(t *testing.T) {
	c := detachedConn()
	c.feed([]byte("world"))
	c.Unread([]byte("hello "))
	buf := make([]byte, 32)
	n, _ := c.Read(buf)
	if string(buf[:n]) != "hello world" {
		t.Fatalf("got %q", buf[:n])
	}
}

func TestConnWriteBufferLimit(t *testing.T) {
	c := detachedConn()
	chunk := make([]byte, 1<<20)
	for i := 0; i < DefaultMaxOutbound>>20; i++ {
		if _, err := c.Write(chunk); err != nil {
			t.Fatalf("write %d: %v", i, err)
		}
	}
	if _, err := c.Write(chunk); !errors.Is(err, ErrWriteBufferFull) {
		t.Fatalf("want ErrWriteBufferFull, got %v", err)
	}
}

func TestConnOutboundBackpressure(t *testing.T) {
	c := detachedConn()
	_, _ = c.Write(make([]byte, 32<<10))
	if c.state.Load()&stPausedOut != 0 {
		t.Fatal("paused below the high watermark")
	}
	_, _ = c.Write(make([]byte, 40<<10))
	if c.state.Load()&stPausedOut == 0 {
		t.Fatal("not paused above the high watermark")
	}
	c.drain(make([]byte, 60<<10))
	if c.state.Load()&stPausedOut != 0 {
		t.Fatal("still paused below the low watermark")
	}
}

// ---------------------------------------------------------------------------
// Engine
// ---------------------------------------------------------------------------

type funcHandler struct {
	data  func(c *Conn, b []byte) int
	close func(c *Conn, err error)
}

func (h *funcHandler) OnData(c *Conn, b []byte) int { return h.data(c, b) }
func (h *funcHandler) OnClose(c *Conn, err error) {
	if h.close != nil {
		h.close(c, err)
	}
}

func startEngine(t *testing.T, h Handler) string {
	t.Helper()
	fd, addr, err := netpoll.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	e, err := New(Config{Loops: 2}, []Listener{{FD: fd, Addr: addr}}, h)
	if err != nil {
		t.Fatal(err)
	}
	go func() { _ = e.Serve() }()
	t.Cleanup(e.Close)
	return addr.String()
}

func dial(t *testing.T, addr string) net.Conn {
	t.Helper()
	c, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = c.Close() })
	_ = c.SetDeadline(time.Now().Add(5 * time.Second))
	return c
}

func TestEngineEventModeLineEcho(t *testing.T) {
	var closed atomic.Int32
	addr := startEngine(t, &funcHandler{
		// Echo complete lines; keep partial lines until the rest arrives.
		data: func(c *Conn, b []byte) int {
			i := bytes.LastIndexByte(b, '\n')
			if i < 0 {
				return 0
			}
			_, _ = c.Write(b[:i+1])
			return i + 1
		},
		close: func(*Conn, error) { closed.Add(1) },
	})
	c := dial(t, addr)
	for _, part := range []string{"he", "llo\nwor", "ld\n"} {
		_, _ = c.Write([]byte(part))
		time.Sleep(10 * time.Millisecond)
	}
	buf := make([]byte, 12)
	if _, err := io.ReadFull(c, buf); err != nil || string(buf) != "hello\nworld\n" {
		t.Fatalf("echo %q %v", buf, err)
	}
	_ = c.Close()
	deadline := time.Now().Add(3 * time.Second)
	for closed.Load() == 0 && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	if closed.Load() != 1 {
		t.Fatalf("OnClose calls = %d", closed.Load())
	}
}

// A detached reader keeps its connection through a peer half-close, answers,
// and hands the connection back with Attach.
func TestEngineDetachHalfCloseAndAttach(t *testing.T) {
	var h *funcHandler
	h = &funcHandler{data: func(c *Conn, b []byte) int {
		if !bytes.Contains(b, []byte("\n")) {
			return 0
		}
		c.Detach()
		go func() {
			buf := make([]byte, 64)
			var got []byte
			for !bytes.Contains(got, []byte("\n")) {
				n, err := c.Read(buf)
				if err != nil {
					return
				}
				got = append(got, buf[:n]...)
			}
			_, _ = c.Write(append([]byte("got:"), got...))
			if bytes.HasPrefix(got, []byte("bye")) {
				_ = c.Close()
				return
			}
			c.Attach(h)
		}()
		return 0
	}}
	addr := startEngine(t, h)
	c := dial(t, addr)
	br := make([]byte, 64)

	_, _ = c.Write([]byte("one\n"))
	if n, _ := io.ReadAtLeast(c, br, len("got:one\n")); string(br[:n]) != "got:one\n" {
		t.Fatalf("first reply %q", br[:n])
	}
	_, _ = c.Write([]byte("bye\n"))
	_ = c.(*net.TCPConn).CloseWrite() // half-close: the reply must still arrive
	all, err := io.ReadAll(c)
	if err != nil || string(all) != "got:bye\n" {
		t.Fatalf("reply after half-close %q %v", all, err)
	}
}

func TestEngineInboundPauseResume(t *testing.T) {
	var mu sync.Mutex
	var got []byte
	var paused *Conn
	addr := startEngine(t, &funcHandler{data: func(c *Conn, b []byte) int {
		mu.Lock()
		got = append(got, b...)
		if paused == nil {
			paused = c
			c.PauseRead()
		}
		mu.Unlock()
		return len(b)
	}})
	c := dial(t, addr)
	_, _ = c.Write([]byte("first"))
	time.Sleep(50 * time.Millisecond)
	_, _ = c.Write([]byte("second"))
	time.Sleep(50 * time.Millisecond)
	mu.Lock()
	if string(got) != "first" {
		mu.Unlock()
		t.Fatalf("read while paused: %q", got)
	}
	p := paused
	mu.Unlock()
	p.ResumeRead()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		mu.Lock()
		s := string(got)
		mu.Unlock()
		if s == "firstsecond" {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("data after resume: %q", got)
}

func TestEngineCloseFlushesQueuedOutput(t *testing.T) {
	payload := bytes.Repeat([]byte("x"), 8<<20) // far more than the socket buffer
	addr := startEngine(t, &funcHandler{data: func(c *Conn, b []byte) int {
		_, _ = c.Write(payload)
		_ = c.Close()
		return len(b)
	}})
	c := dial(t, addr)
	_, _ = c.Write([]byte("go"))
	time.Sleep(100 * time.Millisecond) // let the server queue before we read
	_ = c.SetDeadline(time.Now().Add(10 * time.Second))
	all, err := io.ReadAll(c)
	if err != nil || len(all) != len(payload) {
		t.Fatalf("read %d bytes (%v), want %d", len(all), err, len(payload))
	}
}

func TestEngineCloseIsIdempotentBeforeServe(t *testing.T) {
	fd, addr, err := netpoll.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	e, err := New(Config{Loops: 1}, []Listener{{FD: fd, Addr: addr}}, &funcHandler{})
	if err != nil {
		t.Fatal(err)
	}
	e.Close()
	e.Close()
	if err := e.Serve(); err != nil {
		t.Fatalf("Serve after Close: %v", err)
	}
	if c, err := net.DialTimeout("tcp", addr.String(), time.Second); err == nil {
		_ = c.Close()
		t.Fatal("listener still open after Close")
	}
}

type openHandler struct {
	funcHandler
	open func(c *Conn)
}

func (h *openHandler) OnOpen(c *Conn) { h.open(c) }

// A close deadline closes a connection whose handler still owns it; Detach
// cancels it.
func TestEngineCloseDeadline(t *testing.T) {
	closed := make(chan error, 4)
	addr := startEngine(t, &openHandler{
		funcHandler: funcHandler{
			data:  func(c *Conn, b []byte) int { c.Detach(); return 0 },
			close: func(_ *Conn, err error) { closed <- err },
		},
		open: func(c *Conn) { c.SetCloseDeadline(time.Now().Add(200 * time.Millisecond)) },
	})

	silent := dial(t, addr)
	start := time.Now()
	select {
	case err := <-closed:
		if !errors.Is(err, os.ErrDeadlineExceeded) {
			t.Fatalf("OnClose err = %v, want deadline exceeded", err)
		}
		if el := time.Since(start); el < 150*time.Millisecond {
			t.Fatalf("closed after %v, before its deadline", el)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("close deadline did not fire")
	}
	if _, err := silent.Read(make([]byte, 1)); err == nil {
		t.Fatal("peer still connected after the close deadline")
	}

	detached := dial(t, addr)
	_, _ = detached.Write([]byte("x"))
	select {
	case err := <-closed:
		t.Fatalf("closed a detached connection: %v", err)
	case <-time.After(600 * time.Millisecond):
	}
}

func shortenDrain(t *testing.T, d time.Duration) {
	old := drainStall
	t.Cleanup(func() { drainStall = old }) // runs after the engine's own cleanup
	drainStall = d
}

// Close with output queued for a peer that never reads again ends after
// drainStall instead of holding the connection forever.
func TestEngineDrainStopsWhenPeerStopsReading(t *testing.T) {
	shortenDrain(t, 300*time.Millisecond)
	closed := make(chan error, 1)
	payload := make([]byte, 15<<20) // more than the kernel buffers take
	addr := startEngine(t, &funcHandler{
		data: func(c *Conn, b []byte) int {
			_, _ = c.Write(payload)
			_ = c.Close()
			return len(b)
		},
		close: func(_ *Conn, err error) { closed <- err },
	})
	c := dial(t, addr)
	_, _ = c.Write([]byte("go")) // and never read the reply
	select {
	case err := <-closed:
		if !errors.Is(err, os.ErrDeadlineExceeded) {
			t.Fatalf("OnClose err = %v, want deadline exceeded", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("drain never gave up on a peer that stopped reading")
	}
}

// A slow peer that keeps reading is not cut off: each bit of progress extends
// the drain.
func TestEngineDrainKeepsGoingWhilePeerReads(t *testing.T) {
	shortenDrain(t, 300*time.Millisecond)
	closed := make(chan error, 1)
	payload := bytes.Repeat([]byte("y"), 4<<20)
	addr := startEngine(t, &funcHandler{
		data: func(c *Conn, b []byte) int {
			_, _ = c.Write(payload)
			_ = c.Close()
			return len(b)
		},
		close: func(_ *Conn, err error) { closed <- err },
	})
	c := dial(t, addr)
	_ = c.SetDeadline(time.Now().Add(30 * time.Second))
	_, _ = c.Write([]byte("go"))
	got, buf := 0, make([]byte, 64<<10)
	start := time.Now()
	for {
		time.Sleep(20 * time.Millisecond) // ~1.3s in total, far beyond drainStall
		n, err := c.Read(buf)
		got += n
		if err != nil {
			break
		}
	}
	if got != len(payload) {
		t.Fatalf("received %d of %d bytes in %v", got, len(payload), time.Since(start))
	}
	if err := <-closed; err != nil {
		t.Fatalf("OnClose err = %v, want nil", err)
	}
}

// tls.Conn.Close sets the write deadline to "now" before closing the
// connection; output already queued must still be flushed.
func TestEngineDrainIgnoresPassedWriteDeadline(t *testing.T) {
	payload := bytes.Repeat([]byte("z"), 4<<20)
	addr := startEngine(t, &funcHandler{data: func(c *Conn, b []byte) int {
		_, _ = c.Write(payload)
		_ = c.SetWriteDeadline(time.Now())
		_ = c.Close()
		return len(b)
	}})
	c := dial(t, addr)
	_, _ = c.Write([]byte("go"))
	time.Sleep(100 * time.Millisecond) // let the server queue and close first
	_ = c.SetDeadline(time.Now().Add(10 * time.Second))
	all, err := io.ReadAll(c)
	if err != nil || len(all) != len(payload) {
		t.Fatalf("read %d of %d bytes (%v)", len(all), len(payload), err)
	}
}

// One write larger than the cap still goes out whole when nothing is queued,
// so a message is never cut short on the wire; the cap then holds.
func TestConnWriteLargerThanCapIntoEmptyQueue(t *testing.T) {
	c := detachedConn()
	big := make([]byte, DefaultMaxOutbound+1)
	if n, err := c.Write(big); err != nil || n != len(big) {
		t.Fatalf("Write(big) = %d, %v", n, err)
	}
	if _, err := c.Write([]byte("x")); !errors.Is(err, ErrWriteBufferFull) {
		t.Fatalf("write over the cap: %v", err)
	}
}

// Data and the peer's FIN often arrive on one edge; the EOF must still be seen
// after the short read that drains the data.
func TestEngineSeesEOFThatCameWithData(t *testing.T) {
	closed := make(chan error, 64)
	addr := startEngine(t, &funcHandler{
		data:  func(_ *Conn, b []byte) int { return len(b) },
		close: func(_ *Conn, err error) { closed <- err },
	})
	for i := 0; i < 50; i++ {
		c := dial(t, addr)
		_, _ = c.Write([]byte("last words"))
		_ = c.(*net.TCPConn).CloseWrite()
		select {
		case err := <-closed:
			if err != io.EOF {
				t.Fatalf("OnClose err = %v, want io.EOF", err)
			}
		case <-time.After(3 * time.Second):
			t.Fatalf("round %d: the EOF was never seen", i)
		}
	}
}

type eofHandler struct {
	funcHandler
	eof func(c *Conn, rest []byte)
}

func (h *eofHandler) OnEOF(c *Conn, rest []byte) { h.eof(c, rest) }

// An EOFHandler gets the input it left unconsumed, keeps writing after the
// peer's EOF, and closes when it is done.
func TestEngineEOFHandlerServesHalfClose(t *testing.T) {
	addr := startEngine(t, &eofHandler{
		funcHandler: funcHandler{data: func(*Conn, []byte) int { return 0 }},
		eof: func(c *Conn, rest []byte) {
			_, _ = c.Write(append([]byte("rest:"), rest...))
			_ = c.Close()
		},
	})
	c := dial(t, addr)
	_, _ = c.Write([]byte("abc"))
	time.Sleep(20 * time.Millisecond)
	_, _ = c.Write([]byte("def"))
	_ = c.(*net.TCPConn).CloseWrite()
	all, err := io.ReadAll(c)
	if err != nil || string(all) != "rest:abcdef" {
		t.Fatalf("read %q (%v)", all, err)
	}
}

func TestEngineStopAcceptKeepsConnections(t *testing.T) {
	fd, addr, err := netpoll.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	e, err := New(Config{Loops: 1}, []Listener{{FD: fd, Addr: addr}}, &funcHandler{
		data: func(c *Conn, b []byte) int {
			_, _ = c.Write(b)
			return len(b)
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	served := make(chan error, 1)
	go func() { served <- e.Serve() }()
	t.Cleanup(e.Close)

	c := dial(t, addr.String())
	buf := make([]byte, 5)
	_, _ = c.Write([]byte("hi"))
	if _, err := io.ReadFull(c, buf[:2]); err != nil {
		t.Fatal(err)
	}
	e.StopAccept()
	if err := <-served; err != nil {
		t.Fatalf("Serve after StopAccept: %v", err)
	}
	_, _ = c.Write([]byte("again"))
	if _, err := io.ReadFull(c, buf); err != nil || string(buf) != "again" {
		t.Fatalf("echo after StopAccept: %q %v", buf, err)
	}
	if nc, err := net.DialTimeout("tcp", addr.String(), time.Second); err == nil {
		_ = nc.Close()
		t.Fatal("still accepting after StopAccept")
	}
	n := 0
	e.ForEach(func(*Conn) { n++ })
	if n != 1 {
		t.Fatalf("ForEach saw %d connections, want 1", n)
	}
}

func TestEngineMaxOutbound(t *testing.T) {
	fd, addr, err := netpoll.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	results := make(chan [2]error, 1)
	e, err := New(Config{Loops: 1, MaxOutbound: 64 << 10}, []Listener{{FD: fd, Addr: addr}}, &funcHandler{
		data: func(c *Conn, b []byte) int {
			_, err1 := c.Write(make([]byte, 32<<20)) // more than any kernel buffer takes
			_, err2 := c.Write(make([]byte, 1024))
			results <- [2]error{err1, err2}
			return len(b)
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	go func() { _ = e.Serve() }()
	t.Cleanup(e.Close)
	c := dial(t, addr.String()) // and never read
	_, _ = c.Write([]byte("go"))
	select {
	case errs := <-results:
		if errs[0] != nil || !errors.Is(errs[1], ErrWriteBufferFull) {
			t.Fatalf("writes returned %v, want nil then ErrWriteBufferFull", errs)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("handler did not run")
	}
}
