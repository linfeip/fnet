package fnet

import (
	"bufio"
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// The tests speak a length-prefixed protocol: | len uint32 BE | payload |.
var lengthPrefixed = LengthField{Size: 4, Strip: 4}.Split

func frame(payload []byte) []byte {
	b := make([]byte, 4+len(payload))
	binary.BigEndian.PutUint32(b, uint32(len(payload)))
	copy(b[4:], payload)
	return b
}

func frames(payloads ...string) []byte {
	var b []byte
	for _, p := range payloads {
		b = append(b, frame([]byte(p))...)
	}
	return b
}

func readFrame(t *testing.T, r io.Reader) []byte {
	t.Helper()
	var hdr [4]byte
	if _, err := io.ReadFull(r, hdr[:]); err != nil {
		t.Fatalf("read header: %v", err)
	}
	b := make([]byte, binary.BigEndian.Uint32(hdr[:]))
	if _, err := io.ReadFull(r, b); err != nil {
		t.Fatalf("read payload: %v", err)
	}
	return b
}

func echo(c *Conn, msg []byte) { _, _ = c.Write(frame(msg)) }

// serve starts s on a loopback port and closes it when the test ends.
func serve(t *testing.T, s *Server) string {
	t.Helper()
	addrc := make(chan string, 1)
	s.Listen = func(network, _ string) (net.Listener, error) {
		ln, err := net.Listen(network, "127.0.0.1:0")
		if err == nil {
			addrc <- ln.Addr().String()
		}
		return ln, err
	}
	errc := make(chan error, 1)
	go func() { errc <- s.ListenAndServe() }()
	var addr string
	select {
	case addr = <-addrc:
	case err := <-errc:
		t.Fatalf("ListenAndServe: %v", err)
	case <-time.After(5 * time.Second):
		t.Fatal("server did not start")
	}
	t.Cleanup(func() {
		_ = s.Close()
		if err := <-errc; err != ErrServerClosed {
			t.Errorf("ListenAndServe returned %v, want ErrServerClosed", err)
		}
	})
	return addr
}

func dial(t *testing.T, addr string) net.Conn {
	t.Helper()
	c, err := net.DialTimeout("tcp", addr, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = c.Close() })
	_ = c.SetDeadline(time.Now().Add(10 * time.Second))
	return c
}

func recv[T any](t *testing.T, ch <-chan T) T {
	t.Helper()
	select {
	case v := <-ch:
		return v
	case <-time.After(5 * time.Second):
		t.Fatal("timed out")
		panic("unreachable")
	}
}

func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// releaser returns a channel that blocks handlers until release is called.
// Cleanup releases it too, so a failing test never leaves a worker stuck.
func releaser(t *testing.T) (ch <-chan struct{}, release func()) {
	c := make(chan struct{})
	var once sync.Once
	release = func() { once.Do(func() { close(c) }) }
	t.Cleanup(release)
	return c, release
}

func expectClosed(t *testing.T, c net.Conn) {
	t.Helper()
	if n, err := c.Read(make([]byte, 1)); err == nil {
		t.Fatalf("connection still open (read %d bytes)", n)
	}
}

// ---------------------------------------------------------------------------
// Framing
// ---------------------------------------------------------------------------

// Messages that arrive glued together or in pieces reach OnMessage whole.
func TestServerCoalescedAndSplitMessages(t *testing.T) {
	addr := serve(t, &Server{Split: lengthPrefixed, OnMessage: echo})
	c := dial(t, addr)

	if _, err := c.Write(frames("alpha", "beta", "gamma")); err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{"alpha", "beta", "gamma"} {
		if got := readFrame(t, c); string(got) != want {
			t.Fatalf("got %q, want %q", got, want)
		}
	}
	for _, b := range frame([]byte("trickle")) {
		if _, err := c.Write([]byte{b}); err != nil {
			t.Fatal(err)
		}
		time.Sleep(2 * time.Millisecond)
	}
	if got := readFrame(t, c); string(got) != "trickle" {
		t.Fatalf("got %q", got)
	}
}

func TestServerEmptyMessage(t *testing.T) {
	sizes := make(chan int, 2)
	addr := serve(t, &Server{
		Split: lengthPrefixed,
		OnMessage: func(c *Conn, msg []byte) {
			sizes <- len(msg)
			echo(c, msg)
		},
	})
	c := dial(t, addr)
	_, _ = c.Write(frames("", "x"))
	if n := recv(t, sizes); n != 0 {
		t.Fatalf("first message has %d bytes, want 0", n)
	}
	if n := recv(t, sizes); n != 1 {
		t.Fatalf("second message has %d bytes, want 1", n)
	}
	if got := readFrame(t, c); len(got) != 0 {
		t.Fatalf("empty echo came back as %q", got)
	}
	if got := readFrame(t, c); string(got) != "x" {
		t.Fatalf("got %q", got)
	}
}

// bufio.ErrFinalToken delivers its message and ends the input.
func TestServerFinalToken(t *testing.T) {
	split := func(data []byte, atEOF bool) (int, []byte, error) {
		adv, tok, err := bufio.ScanLines(data, atEOF)
		if err == nil && string(tok) == "bye" {
			return adv, tok, bufio.ErrFinalToken
		}
		return adv, tok, err
	}
	got := make(chan string, 4)
	closed := make(chan error, 1)
	addr := serve(t, &Server{
		Split: split,
		OnMessage: func(c *Conn, line []byte) {
			got <- string(line)
			_, _ = c.Write(append(line, '\n'))
		},
		OnClose: func(_ *Conn, err error) { closed <- err },
	})
	c := dial(t, addr)
	_, _ = c.Write([]byte("hi\nbye\nignored\n"))
	all, err := io.ReadAll(c)
	if err != nil || string(all) != "hi\nbye\n" {
		t.Fatalf("read %q (%v)", all, err)
	}
	if err := recv(t, closed); err != nil {
		t.Fatalf("OnClose err = %v, want nil", err)
	}
	if a, b := recv(t, got), recv(t, got); a != "hi" || b != "bye" {
		t.Fatalf("messages %q %q", a, b)
	}
	select {
	case m := <-got:
		t.Fatalf("%q delivered after the final token", m)
	default:
	}
}

// ---------------------------------------------------------------------------
// Callback order and isolation
// ---------------------------------------------------------------------------

func TestServerCallbacksRunInOrderOneAtATime(t *testing.T) {
	type session struct{ n int }
	var (
		mu      sync.Mutex
		events  []string
		active  atomic.Int32
		overlap atomic.Bool
	)
	record := func(s string) {
		mu.Lock()
		events = append(events, s)
		mu.Unlock()
	}
	done := make(chan struct{})
	addr := serve(t, &Server{
		Split: lengthPrefixed,
		OnOpen: func(c *Conn) {
			c.SetContext(&session{})
			record("open")
		},
		OnMessage: func(c *Conn, msg []byte) {
			if active.Add(1) > 1 {
				overlap.Store(true)
			}
			defer active.Add(-1)
			c.Context().(*session).n++
			time.Sleep(time.Millisecond)
			record(string(msg))
		},
		OnClose: func(c *Conn, err error) {
			record(fmt.Sprintf("close:%d", c.Context().(*session).n))
			close(done)
		},
	})
	c := dial(t, addr)
	want := []string{"open"}
	var all []byte
	for i := 0; i < 50; i++ {
		want = append(want, strconv.Itoa(i))
		all = append(all, frame([]byte(strconv.Itoa(i)))...)
	}
	want = append(want, "close:50")
	for len(all) > 0 { // several writes: several reads, several batches
		n := min(len(all), 37)
		_, _ = c.Write(all[:n])
		all = all[n:]
		time.Sleep(time.Millisecond)
	}
	waitFor(t, "all messages", func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(events) == len(want)-1
	})
	_ = c.Close()
	recv(t, done)
	mu.Lock()
	defer mu.Unlock()
	if fmt.Sprint(events) != fmt.Sprint(want) {
		t.Fatalf("events\n got %v\nwant %v", events, want)
	}
	if overlap.Load() {
		t.Fatal("OnMessage ran concurrently for one connection")
	}
}

// A handler blocked on one connection must not hold up the event loop.
func TestServerBlockedHandlerDoesNotBlockOthers(t *testing.T) {
	release, unblock := releaser(t)
	addr := serve(t, &Server{
		Split:      lengthPrefixed,
		NumPollers: 1, // every connection shares the one loop
		OnMessage: func(c *Conn, msg []byte) {
			if string(msg) == "block" {
				<-release
			}
			echo(c, msg)
		},
	})
	a := dial(t, addr)
	_, _ = a.Write(frame([]byte("block")))
	time.Sleep(50 * time.Millisecond)
	b := dial(t, addr)
	for i := 0; i < 10; i++ {
		_, _ = b.Write(frame([]byte("ping")))
		if got := readFrame(t, b); string(got) != "ping" {
			t.Fatalf("got %q", got)
		}
	}
	unblock()
	if got := readFrame(t, a); string(got) != "block" {
		t.Fatalf("got %q", got)
	}
}

// A connection stuck in the middle of a message must not hold up the others.
func TestServerPartialMessageDoesNotBlockOthers(t *testing.T) {
	addr := serve(t, &Server{Split: lengthPrefixed, OnMessage: echo, NumPollers: 1})
	msg := frame([]byte("finished much later"))
	a := dial(t, addr)
	_, _ = a.Write(msg[:7])
	b := dial(t, addr)
	for i := 0; i < 10; i++ {
		_, _ = b.Write(frame([]byte("ping")))
		if got := readFrame(t, b); string(got) != "ping" {
			t.Fatalf("got %q", got)
		}
	}
	_, _ = a.Write(msg[7:])
	if got := readFrame(t, a); string(got) != "finished much later" {
		t.Fatalf("got %q", got)
	}
}

// Writes from several goroutines never interleave within a message.
func TestServerConcurrentWritesStayWhole(t *testing.T) {
	opened := make(chan *Conn, 1)
	addr := serve(t, &Server{
		Split:     lengthPrefixed,
		OnOpen:    func(c *Conn) { opened <- c },
		OnMessage: func(*Conn, []byte) {},
	})
	c := dial(t, addr)
	sc := recv(t, opened)
	const writers, each = 4, 200
	var wg sync.WaitGroup
	for w := 0; w < writers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			payload := bytes.Repeat([]byte{byte('a' + w)}, 3000)
			var hdr [4]byte
			binary.BigEndian.PutUint32(hdr[:], uint32(len(payload)))
			for i := 0; i < each; i++ {
				var err error
				if i%2 == 0 {
					_, err = sc.Write(frame(payload))
				} else {
					_, err = sc.Writev([][]byte{hdr[:], payload})
				}
				if err != nil {
					t.Errorf("write: %v", err)
					return
				}
			}
		}(w)
	}
	counts := map[byte]int{}
	for i := 0; i < writers*each; i++ {
		m := readFrame(t, c)
		if len(m) != 3000 || bytes.Count(m, m[:1]) != len(m) {
			t.Fatalf("message %d is mixed or cut: %d bytes", i, len(m))
		}
		counts[m[0]]++
	}
	wg.Wait()
	for w := 0; w < writers; w++ {
		if counts[byte('a'+w)] != each {
			t.Fatalf("writer %d: %d messages", w, counts[byte('a'+w)])
		}
	}
}

// ---------------------------------------------------------------------------
// Closing
// ---------------------------------------------------------------------------

// A half-closing peer still gets the replies to everything it sent, including
// a last message that only EOF completes.
func TestServerHalfCloseHandlesRemainingMessages(t *testing.T) {
	closed := make(chan error, 1)
	addr := serve(t, &Server{
		Split: bufio.ScanLines,
		OnMessage: func(c *Conn, line []byte) {
			time.Sleep(20 * time.Millisecond) // still busy when the FIN arrives
			_, _ = c.Write([]byte("got " + string(line) + "\n"))
		},
		OnClose: func(_ *Conn, err error) { closed <- err },
	})
	c := dial(t, addr)
	_, _ = c.Write([]byte("one\ntwo\nthree"))
	_ = c.(*net.TCPConn).CloseWrite()
	all, err := io.ReadAll(c)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if want := "got one\ngot two\ngot three\n"; string(all) != want {
		t.Fatalf("got %q, want %q", all, want)
	}
	if err := recv(t, closed); err != io.EOF {
		t.Fatalf("OnClose err = %v, want io.EOF", err)
	}
}

func TestServerTruncatedMessageAtEOF(t *testing.T) {
	got := make(chan string, 4)
	closed := make(chan error, 1)
	addr := serve(t, &Server{
		Split:     lengthPrefixed,
		OnMessage: func(_ *Conn, msg []byte) { got <- string(msg) },
		OnClose:   func(_ *Conn, err error) { closed <- err },
	})
	c := dial(t, addr)
	_, _ = c.Write(append(frame([]byte("whole")), frame([]byte("cut off"))[:6]...))
	_ = c.(*net.TCPConn).CloseWrite()
	if m := recv(t, got); m != "whole" {
		t.Fatalf("got %q", m)
	}
	if err := recv(t, closed); !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Fatalf("OnClose err = %v, want io.ErrUnexpectedEOF", err)
	}
	select {
	case m := <-got:
		t.Fatalf("delivered the truncated message %q", m)
	default:
	}
}

// Close drops the messages not handled yet but flushes the replies written.
func TestServerCloseDropsUnhandledMessages(t *testing.T) {
	got := make(chan string, 8)
	closed := make(chan error, 1)
	addr := serve(t, &Server{
		Split: lengthPrefixed,
		OnMessage: func(c *Conn, msg []byte) {
			got <- string(msg)
			echo(c, msg)
			if string(msg) == "quit" {
				_ = c.Close()
			}
		},
		OnClose: func(_ *Conn, err error) { closed <- err },
	})
	c := dial(t, addr)
	_, _ = c.Write(frames("a", "quit", "b", "c"))
	for _, want := range []string{"a", "quit"} {
		if m := readFrame(t, c); string(m) != want {
			t.Fatalf("reply %q, want %q", m, want)
		}
	}
	expectClosed(t, c)
	if err := recv(t, closed); err != nil {
		t.Fatalf("OnClose err = %v, want nil", err)
	}
	if a, b := recv(t, got), recv(t, got); a != "a" || b != "quit" {
		t.Fatalf("handled %q %q", a, b)
	}
	select {
	case m := <-got:
		t.Fatalf("%q handled after Close", m)
	case <-time.After(50 * time.Millisecond):
	}
}

func TestServerMaxMessageSize(t *testing.T) {
	closed := make(chan error, 2)
	addr := serve(t, &Server{
		Split:          lengthPrefixed,
		MaxMessageSize: 64,
		OnMessage:      echo,
		OnClose:        func(_ *Conn, err error) { closed <- err },
	})
	a := dial(t, addr)
	_, _ = a.Write(frame(make([]byte, 65))) // complete, one byte over
	if err := recv(t, closed); !errors.Is(err, ErrMessageTooLarge) {
		t.Fatalf("complete message: OnClose err = %v", err)
	}
	b := dial(t, addr)
	_, _ = b.Write(frame(make([]byte, 10000))[:200]) // incomplete, already over
	if err := recv(t, closed); !errors.Is(err, ErrMessageTooLarge) {
		t.Fatalf("partial message: OnClose err = %v", err)
	}
	c := dial(t, addr)
	_, _ = c.Write(frame(make([]byte, 64)))
	if got := readFrame(t, c); len(got) != 64 {
		t.Fatalf("a message at the limit came back with %d bytes", len(got))
	}
}

func TestServerSplitFailuresCloseOnlyThatConnection(t *testing.T) {
	errBoom := errors.New("boom")
	cases := []struct {
		name  string
		split func([]byte, bool) (int, []byte, error)
		want  func(error) bool
	}{
		{"error", func([]byte, bool) (int, []byte, error) { return 0, nil, errBoom },
			func(err error) bool { return errors.Is(err, errBoom) }},
		{"panic", func([]byte, bool) (int, []byte, error) { panic("split bug") },
			func(err error) bool {
				var pe *PanicError
				return errors.As(err, &pe) && pe.Value == "split bug" && len(pe.Stack) > 0
			}},
		{"no progress", func([]byte, bool) (int, []byte, error) { return 0, []byte{}, nil },
			func(err error) bool { return errors.Is(err, errNoProgress) }},
		{"bad advance", func(d []byte, _ bool) (int, []byte, error) { return len(d) + 1, nil, nil },
			func(err error) bool { return errors.Is(err, errBadAdvance) }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			closed := make(chan error, 1)
			addr := serve(t, &Server{
				Split:     tc.split,
				OnMessage: func(*Conn, []byte) { t.Error("OnMessage ran") },
				OnClose:   func(_ *Conn, err error) { closed <- err },
			})
			c := dial(t, addr)
			_, _ = c.Write([]byte("x"))
			if err := recv(t, closed); !tc.want(err) {
				t.Fatalf("OnClose err = %v", err)
			}
			expectClosed(t, c)
			other := dial(t, addr) // the server is still up
			_, _ = other.Write([]byte("y"))
			recv(t, closed)
		})
	}
}

func TestServerOnMessagePanicClosesOnlyThatConnection(t *testing.T) {
	got := make(chan string, 8)
	closed := make(chan error, 2)
	addr := serve(t, &Server{
		Split: lengthPrefixed,
		OnMessage: func(c *Conn, msg []byte) {
			if string(msg) == "boom" {
				panic("handler bug")
			}
			got <- string(msg)
			echo(c, msg)
		},
		OnClose: func(_ *Conn, err error) { closed <- err },
	})
	a := dial(t, addr)
	_, _ = a.Write(frames("before", "boom", "after"))
	if m := recv(t, got); m != "before" {
		t.Fatalf("got %q", m)
	}
	var pe *PanicError
	if err := recv(t, closed); !errors.As(err, &pe) || pe.Value != "handler bug" || len(pe.Stack) == 0 {
		t.Fatalf("OnClose err = %v, want *PanicError", err)
	}
	select {
	case m := <-got:
		t.Fatalf("%q handled after the panic", m)
	case <-time.After(50 * time.Millisecond):
	}
	b := dial(t, addr)
	_, _ = b.Write(frame([]byte("still up")))
	if m := readFrame(t, b); string(m) != "still up" {
		t.Fatalf("got %q", m)
	}
}

func TestServerOnOpenPanic(t *testing.T) {
	closed := make(chan error, 1)
	addr := serve(t, &Server{
		Split:     lengthPrefixed,
		OnOpen:    func(*Conn) { panic("open bug") },
		OnMessage: func(*Conn, []byte) { t.Error("OnMessage ran after OnOpen panicked") },
		OnClose:   func(_ *Conn, err error) { closed <- err },
	})
	c := dial(t, addr)
	_, _ = c.Write(frame([]byte("x")))
	var pe *PanicError
	if err := recv(t, closed); !errors.As(err, &pe) || pe.Value != "open bug" {
		t.Fatalf("OnClose err = %v", err)
	}
	expectClosed(t, c)
}

// A custom pool that refuses work sheds the connection; OnClose still runs.
func TestServerPoolRejection(t *testing.T) {
	closed := make(chan error, 1)
	addr := serve(t, &Server{
		Split:      lengthPrefixed,
		WorkerPool: func(uint64, func()) { panic("pool full") },
		OnMessage:  func(*Conn, []byte) { t.Error("OnMessage ran on a rejected connection") },
		OnClose:    func(_ *Conn, err error) { closed <- err },
	})
	c := dial(t, addr)
	_, _ = c.Write(frame([]byte("x")))
	if err := recv(t, closed); !errors.Is(err, errPoolRejected) {
		t.Fatalf("OnClose err = %v", err)
	}
	expectClosed(t, c)
}

// ---------------------------------------------------------------------------
// Backpressure and limits
// ---------------------------------------------------------------------------

func TestServerBackpressureBoundsPendingMessages(t *testing.T) {
	const maxPending = 8 << 10
	opened := make(chan *Conn, 1)
	release, unblock := releaser(t)
	var handled atomic.Int32
	addr := serve(t, &Server{
		Split:                  lengthPrefixed,
		MaxPendingMessageBytes: maxPending,
		OnOpen:                 func(c *Conn) { opened <- c },
		OnMessage: func(*Conn, []byte) {
			<-release
			handled.Add(1)
		},
	})
	c := dial(t, addr)
	sc := recv(t, opened)
	const n = 2000
	msg := frame(make([]byte, 1000))
	go func() {
		for i := 0; i < n; i++ {
			if _, err := c.Write(msg); err != nil {
				return
			}
		}
	}()
	time.Sleep(300 * time.Millisecond) // OnMessage is stuck; the client keeps sending
	sc.mu.Lock()
	pending, paused := sc.pending, sc.state&csPaused != 0
	sc.mu.Unlock()
	// One read (64 KiB) can land before the pause takes effect.
	if limit := maxPending + 64<<10 + len(msg); pending > limit || !paused {
		t.Fatalf("pending %d bytes (limit %d), paused %v", pending, limit, paused)
	}
	unblock()
	waitFor(t, "every message", func() bool { return handled.Load() == n })
}

func TestServerMaxOutboundBytes(t *testing.T) {
	opened := make(chan *Conn, 1)
	addr := serve(t, &Server{
		Split:            lengthPrefixed,
		MaxOutboundBytes: 64 << 10,
		OnOpen:           func(c *Conn) { opened <- c },
		OnMessage:        func(*Conn, []byte) {},
	})
	_ = dial(t, addr) // never reads
	sc := recv(t, opened)
	// With nothing queued, one message larger than the cap goes out whole...
	big := make([]byte, 32<<20) // more than any kernel buffer takes
	if n, err := sc.Write(big); err != nil || n != len(big) {
		t.Fatalf("Write(big) = %d, %v", n, err)
	}
	// ...but while it waits for the peer, more output is refused.
	if _, err := sc.Write(make([]byte, 1024)); !errors.Is(err, ErrWriteBufferFull) {
		t.Fatalf("Write over the cap: %v, want ErrWriteBufferFull", err)
	}
}

// ---------------------------------------------------------------------------
// Timeouts
// ---------------------------------------------------------------------------

// ReadTimeout counts from a message's first byte: trickling cannot extend it.
func TestServerReadTimeoutStopsTrickle(t *testing.T) {
	closed := make(chan error, 1)
	addr := serve(t, &Server{
		Split:       lengthPrefixed,
		ReadTimeout: 300 * time.Millisecond,
		OnMessage:   func(*Conn, []byte) { t.Error("OnMessage ran") },
		OnClose:     func(_ *Conn, err error) { closed <- err },
	})
	c := dial(t, addr)
	msg := frame(make([]byte, 100))
	start := time.Now()
	go func() {
		for _, b := range msg {
			if _, err := c.Write([]byte{b}); err != nil {
				return
			}
			time.Sleep(50 * time.Millisecond)
		}
	}()
	if err := recv(t, closed); !errors.Is(err, os.ErrDeadlineExceeded) {
		t.Fatalf("OnClose err = %v", err)
	}
	if el := time.Since(start); el < 250*time.Millisecond || el > 3*time.Second {
		t.Fatalf("closed after %v, want about 300ms", el)
	}
}

// Time the server itself spends paused by backpressure is not the peer's.
func TestServerReadTimeoutExcludesBackpressurePause(t *testing.T) {
	closed := make(chan error, 1)
	release, unblock := releaser(t)
	got := make(chan byte, 8)
	addr := serve(t, &Server{
		Split:                  lengthPrefixed,
		ReadTimeout:            500 * time.Millisecond,
		MaxPendingMessageBytes: 2 << 10,
		OnMessage: func(_ *Conn, msg []byte) {
			if msg[0] == 'a' {
				<-release
			}
			got <- msg[0]
		},
		OnClose: func(_ *Conn, err error) { closed <- err },
	})
	c := dial(t, addr)
	body := func(b byte) []byte { return bytes.Repeat([]byte{b}, 1000) }
	last := frame(body('d'))
	burst := append(append(append(frame(body('a')), frame(body('b'))...), frame(body('c'))...), last[:500]...)
	_, _ = c.Write(burst) // three messages pause reading; the fourth is half there
	time.Sleep(1200 * time.Millisecond)
	unblock()
	time.Sleep(100 * time.Millisecond)
	_, _ = c.Write(last[500:])
	for _, want := range []byte("abcd") {
		if b := recv(t, got); b != want {
			t.Fatalf("got %c, want %c", b, want)
		}
	}
	select {
	case err := <-closed:
		t.Fatalf("closed while paused by backpressure: %v", err)
	default:
	}
}

func TestServerIdleTimeout(t *testing.T) {
	closed := make(chan error, 4)
	addr := serve(t, &Server{
		Split:       lengthPrefixed,
		IdleTimeout: 300 * time.Millisecond,
		OnMessage: func(c *Conn, msg []byte) {
			if string(msg) == "slow" {
				time.Sleep(700 * time.Millisecond) // the peer waits for us, not idle
			}
			echo(c, msg)
		},
		OnClose: func(_ *Conn, err error) { closed <- err },
	})
	start := time.Now()
	silent := dial(t, addr)
	if err := recv(t, closed); !errors.Is(err, os.ErrDeadlineExceeded) {
		t.Fatalf("OnClose err = %v", err)
	}
	if el := time.Since(start); el < 250*time.Millisecond {
		t.Fatalf("idle connection closed after %v", el)
	}
	expectClosed(t, silent)

	c := dial(t, addr)
	for i := 0; i < 8; i++ { // heartbeats every 150ms keep it open well past 300ms
		_, _ = c.Write(frame([]byte("ping")))
		if m := readFrame(t, c); string(m) != "ping" {
			t.Fatalf("got %q", m)
		}
		time.Sleep(150 * time.Millisecond)
	}
	_, _ = c.Write(frame([]byte("slow")))
	if m := readFrame(t, c); string(m) != "slow" {
		t.Fatalf("got %q", m)
	}
	select {
	case err := <-closed:
		t.Fatalf("an active connection was closed: %v", err)
	default:
	}
}

// ---------------------------------------------------------------------------
// Server lifecycle
// ---------------------------------------------------------------------------

func TestServerRequiresSplitAndOnMessage(t *testing.T) {
	if err := (&Server{OnMessage: echo}).ListenAndServe(); err == nil {
		t.Fatal("served without Split")
	}
	if err := (&Server{Split: lengthPrefixed}).ListenAndServe(); err == nil {
		t.Fatal("served without OnMessage")
	}
}

func TestServerCloseReportsErrServerClosed(t *testing.T) {
	opened := make(chan *Conn, 1)
	closed := make(chan error, 1)
	s := &Server{
		Split:     lengthPrefixed,
		OnOpen:    func(c *Conn) { opened <- c },
		OnMessage: echo,
		OnClose:   func(_ *Conn, err error) { closed <- err },
	}
	addr := serve(t, s)
	c := dial(t, addr)
	recv(t, opened)
	_ = s.Close()
	if err := recv(t, closed); !errors.Is(err, ErrServerClosed) {
		t.Fatalf("OnClose err = %v", err)
	}
	expectClosed(t, c)
}

// Shutdown lets received messages finish, flushes their replies, and returns
// only after every OnClose has.
func TestServerShutdown(t *testing.T) {
	opened := make(chan struct{}, 2)
	started := make(chan struct{}, 1)
	var (
		mu     sync.Mutex
		errs   []error
		closes atomic.Int32
	)
	s := &Server{
		Split:  lengthPrefixed,
		OnOpen: func(*Conn) { opened <- struct{}{} },
		OnMessage: func(c *Conn, msg []byte) {
			started <- struct{}{}
			time.Sleep(300 * time.Millisecond) // still busy when Shutdown starts
			echo(c, msg)
		},
		OnClose: func(_ *Conn, err error) {
			time.Sleep(100 * time.Millisecond) // e.g. saving the player
			mu.Lock()
			errs = append(errs, err)
			mu.Unlock()
			closes.Add(1)
		},
	}
	addr := serve(t, s)
	busy, idle := dial(t, addr), dial(t, addr)
	recv(t, opened)
	recv(t, opened)
	_, _ = busy.Write(frame([]byte("work")))
	recv(t, started)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := s.Shutdown(ctx); err != nil {
		t.Fatalf("Shutdown: %v", err)
	}
	if n := closes.Load(); n != 2 {
		t.Fatalf("Shutdown returned after %d of 2 OnClose calls", n)
	}
	if m := readFrame(t, busy); string(m) != "work" {
		t.Fatalf("reply %q", m)
	}
	expectClosed(t, busy)
	expectClosed(t, idle)
	mu.Lock()
	for _, err := range errs {
		if !errors.Is(err, ErrServerClosed) {
			t.Errorf("OnClose err = %v, want ErrServerClosed", err)
		}
	}
	mu.Unlock()
	if c, err := net.DialTimeout("tcp", addr, time.Second); err == nil {
		_ = c.Close()
		t.Fatal("still accepting after Shutdown")
	}
}

func TestServerShutdownGivesUpAtDeadline(t *testing.T) {
	release, _ := releaser(t)
	started := make(chan struct{}, 1)
	s := &Server{
		Split: lengthPrefixed,
		OnMessage: func(*Conn, []byte) {
			started <- struct{}{}
			<-release
		},
	}
	addr := serve(t, s)
	c := dial(t, addr)
	_, _ = c.Write(frame([]byte("stuck")))
	recv(t, started)
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	start := time.Now()
	if err := s.Shutdown(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Shutdown: %v", err)
	}
	if el := time.Since(start); el > 3*time.Second {
		t.Fatalf("Shutdown took %v", el)
	}
	expectClosed(t, c)
}

func TestServerShutdownBeforeServe(t *testing.T) {
	s := &Server{Split: lengthPrefixed, OnMessage: echo, Addr: "127.0.0.1:0"}
	if err := s.Shutdown(context.Background()); err != nil {
		t.Fatalf("Shutdown: %v", err)
	}
	if err := s.ListenAndServe(); err != ErrServerClosed {
		t.Fatalf("ListenAndServe after Shutdown: %v", err)
	}
}
