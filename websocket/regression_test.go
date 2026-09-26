package websocket_test

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"os"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gobwas/ws"
	"github.com/gobwas/ws/wsutil"

	"github.com/linfeip/fnet/websocket"
)

// A plain GET on a WebSocket route (a health check, a crawler) is refused with
// an ordinary HTTP error, and the connection is not left hijacked: once the
// client is done, the server closes its side too.
func TestFailedHandshakeLeavesNoConnection(t *testing.T) {
	u := &websocket.Upgrader{OnMessage: func(*websocket.Conn, websocket.OpCode, []byte) {}}
	addr := startWSServer(t, u)
	c, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	_, _ = io.WriteString(c, "GET /ws HTTP/1.1\r\nHost: x\r\n\r\n")
	_ = c.SetReadDeadline(time.Now().Add(3 * time.Second))
	br := bufio.NewReader(c)
	resp, err := http.ReadResponse(br, nil)
	if err != nil {
		t.Fatal(err)
	}
	_, _ = io.Copy(io.Discard, resp.Body)
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("status %d, want 400", resp.StatusCode)
	}
	_ = c.(*net.TCPConn).CloseWrite()
	if _, err := br.ReadByte(); err != io.EOF {
		t.Fatalf("after the client's FIN the server side returned %v, want EOF", err)
	}
}

// Replies are not held back behind a slow handler for a later message, and
// neither is a write from another goroutine (a room broadcast).
func TestRepliesAndBroadcastsAreNotDelayed(t *testing.T) {
	var conn atomic.Pointer[websocket.Conn]
	u := &websocket.Upgrader{
		OnOpen: func(c *websocket.Conn) { conn.Store(c) },
		OnMessage: func(c *websocket.Conn, op websocket.OpCode, p []byte) {
			_ = c.WriteMessage(op, p)
			if string(p) == "slow" {
				time.Sleep(time.Second)
			}
		},
	}
	addr := startWSServer(t, u)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	c, _, _, err := ws.DefaultDialer.Dial(ctx, "ws://"+addr+"/ws")
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	var burst bytes.Buffer
	for _, m := range []string{"fast", "slow"} {
		_ = ws.WriteFrame(&burst, ws.MaskFrameInPlace(ws.NewTextFrame([]byte(m))))
	}
	start := time.Now()
	_, _ = c.Write(burst.Bytes()) // one read on the server: one batch for the worker
	time.AfterFunc(100*time.Millisecond, func() {
		if wc := conn.Load(); wc != nil {
			_ = wc.WriteText("broadcast")
		}
	})
	_ = c.SetReadDeadline(time.Now().Add(4 * time.Second))
	for range 3 {
		msg, err := wsutil.ReadServerText(c)
		if err != nil {
			t.Fatal(err)
		}
		if d := time.Since(start); d > 600*time.Millisecond {
			t.Fatalf("%q arrived after %v, behind the slow handler", msg, d)
		}
	}
}

// On an event-driven connection the read deadline closes a peer that went
// quiet; OnPong sees the Pongs that push it forward.
func TestEventReadDeadlineAndPong(t *testing.T) {
	closed := make(chan error, 1)
	pongs := make(chan string, 1)
	u := &websocket.Upgrader{
		OnOpen: func(c *websocket.Conn) { _ = c.SetReadDeadline(time.Now().Add(300 * time.Millisecond)) },
		OnPong: func(c *websocket.Conn, data []byte) {
			_ = c.SetReadDeadline(time.Now().Add(300 * time.Millisecond))
			pongs <- string(data)
		},
		OnMessage: func(*websocket.Conn, websocket.OpCode, []byte) {},
		OnClose:   func(_ *websocket.Conn, err error) { closed <- err },
	}
	addr := startWSServer(t, u)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	c, _, _, err := ws.DefaultDialer.Dial(ctx, "ws://"+addr+"/ws")
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	start := time.Now()
	for range 3 { // alive: each Pong moves the deadline
		time.Sleep(150 * time.Millisecond)
		if err := wsutil.WriteClientMessage(c, ws.OpPong, []byte("hb")); err != nil {
			t.Fatal(err)
		}
		select {
		case p := <-pongs:
			if p != "hb" {
				t.Fatalf("OnPong got %q", p)
			}
		case err := <-closed:
			t.Fatalf("closed while the peer answered: %v", err)
		case <-time.After(2 * time.Second):
			t.Fatal("OnPong not called")
		}
	}
	select { // then silent: the deadline closes it
	case err := <-closed:
		if !errors.Is(err, os.ErrDeadlineExceeded) {
			t.Fatalf("OnClose err = %v, want deadline exceeded", err)
		}
		if d := time.Since(start); d < 600*time.Millisecond {
			t.Fatalf("closed after %v, before the peer went quiet", d)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("a silent peer outlived its read deadline")
	}
}

// A custom pool that refuses the connection still gets its OnClose run.
func TestPoolRejectionStillCallsOnClose(t *testing.T) {
	closed := make(chan error, 1)
	u := &websocket.Upgrader{
		WorkerPool: func(uint64, func()) error { return errors.New("full") },
		OnMessage:  func(*websocket.Conn, websocket.OpCode, []byte) {},
		OnClose:    func(_ *websocket.Conn, err error) { closed <- err },
	}
	addr := startWSServer(t, u)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	c, _, _, err := ws.DefaultDialer.Dial(ctx, "ws://"+addr+"/ws")
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	_ = wsutil.WriteClientText(c, []byte("hello"))
	select {
	case <-closed:
	case <-time.After(3 * time.Second):
		t.Fatal("OnClose never ran for a refused connection")
	}
}

// The messages that arrive whole in one read share a buffer: while one of
// them is still being handled, the next read must not be copied over the ones
// queued behind it.
func TestMessagesOfOneReadKeepTheirPayloads(t *testing.T) {
	release := make(chan struct{})
	got := make(chan string, 32)
	var first atomic.Bool
	addr := startWSServer(t, &websocket.Upgrader{OnMessage: func(_ *websocket.Conn, _ websocket.OpCode, msg []byte) {
		if first.CompareAndSwap(false, true) {
			<-release
		}
		got <- string(msg)
	}})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	c, _, _, err := ws.DefaultDialer.Dial(ctx, "ws://"+addr+"/ws")
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	var want []string
	burst := func(tag byte) {
		var b bytes.Buffer
		for i := range 8 {
			m := append([]byte{tag, byte('0' + i)}, bytes.Repeat([]byte{tag}, 100)...)
			want = append(want, string(m))
			_ = wsutil.WriteClientText(&b, m)
		}
		if _, err := c.Write(b.Bytes()); err != nil { // one read, one buffer
			t.Fatal(err)
		}
		time.Sleep(50 * time.Millisecond)
	}
	burst('a') // a0 is handled, a1.. wait behind it
	burst('b') // parsed while a1.. still wait
	close(release)
	for _, w := range want {
		select {
		case s := <-got:
			if s != w {
				t.Fatalf("got %.8q..., want %.8q...", s, w)
			}
		case <-time.After(5 * time.Second):
			t.Fatal("timed out")
		}
	}
}
