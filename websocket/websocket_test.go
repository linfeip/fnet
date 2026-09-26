package websocket_test

import (
	"bytes"
	"compress/flate"
	"context"
	"crypto/tls"
	"fmt"
	"io"
	"net"
	"net/http"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/linfeip/fnet/fhttp"
	"github.com/linfeip/fnet/internal/testcert"
	"github.com/linfeip/fnet/websocket"

	"github.com/gobwas/httphead"
	"github.com/gobwas/ws"
	"github.com/gobwas/ws/wsflate"
	"github.com/gobwas/ws/wsutil"
)

func getFreePort(t *testing.T) int {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("getFreePort: %v", err)
	}
	defer l.Close()
	return l.Addr().(*net.TCPAddr).Port
}

func TestWebSocketEchoHTTP(t *testing.T) {
	port := getFreePort(t)
	addr := fmt.Sprintf("127.0.0.1:%d", port)

	mux := http.NewServeMux()
	mux.HandleFunc("/ws", func(w http.ResponseWriter, r *http.Request) {
		conn, err := websocket.Upgrade(w, r)
		if err != nil {
			t.Errorf("Upgrade failed: %v", err)
			return
		}
		defer conn.Close()

		// Echo loop using conn.Handle
		_ = conn.Handle(func(op websocket.OpCode, msg []byte) error {
			return conn.WriteMessage(op, msg)
		})
	})

	srv := &fhttp.Server{
		Addr:    addr,
		Handler: mux,
	}

	go func() {
		_ = srv.ListenAndServe()
	}()
	defer srv.Close()

	// Wait for server to listen
	time.Sleep(50 * time.Millisecond)

	// Dial from client
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	conn, _, _, err := ws.Dial(ctx, "ws://"+addr+"/ws")
	if err != nil {
		t.Fatalf("client ws.Dial failed: %v", err)
	}
	defer conn.Close()

	// 1. Test Text Message Echo
	testMsg := "Hello fnet WebSocket!"
	if err := wsutil.WriteClientText(conn, []byte(testMsg)); err != nil {
		t.Fatalf("WriteClientText failed: %v", err)
	}

	reply, err := wsutil.ReadServerText(conn)
	if err != nil {
		t.Fatalf("ReadServerText failed: %v", err)
	}
	if string(reply) != testMsg {
		t.Fatalf("expected %q, got %q", testMsg, string(reply))
	}

	// 2. Test Binary Message Echo
	testBin := []byte{0x01, 0x02, 0x03, 0x04, 0x05}
	if err := wsutil.WriteClientBinary(conn, testBin); err != nil {
		t.Fatalf("WriteClientBinary failed: %v", err)
	}

	replyBin, err := wsutil.ReadServerBinary(conn)
	if err != nil {
		t.Fatalf("ReadServerBinary failed: %v", err)
	}
	if string(replyBin) != string(testBin) {
		t.Fatalf("expected binary %v, got %v", testBin, replyBin)
	}
}

func TestWebSocketEchoHTTPS(t *testing.T) {
	port := getFreePort(t)
	addr := fmt.Sprintf("127.0.0.1:%d", port)

	certPEM, keyPEM, err := testcert.Generate()
	if err != nil {
		t.Fatalf("testcert.Generate: %v", err)
	}
	cert, err := tls.X509KeyPair(certPEM, keyPEM)
	if err != nil {
		t.Fatalf("X509KeyPair: %v", err)
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/wss", func(w http.ResponseWriter, r *http.Request) {
		conn, err := websocket.Upgrade(w, r)
		if err != nil {
			t.Errorf("WSS Upgrade failed: %v", err)
			return
		}
		defer conn.Close()

		for {
			op, msg, err := conn.ReadMessage()
			if err != nil {
				break
			}
			if err := conn.WriteMessage(op, msg); err != nil {
				break
			}
		}
	})

	srv := &fhttp.Server{
		Addr:    addr,
		Handler: mux,
		TLSConfig: &tls.Config{
			Certificates: []tls.Certificate{cert},
		},
	}

	go func() {
		_ = srv.ListenAndServe()
	}()
	defer srv.Close()

	time.Sleep(50 * time.Millisecond)

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	dialer := ws.Dialer{
		TLSConfig: &tls.Config{
			InsecureSkipVerify: true,
		},
	}
	conn, _, _, err := dialer.Dial(ctx, "wss://"+addr+"/wss")
	if err != nil {
		t.Fatalf("client wss dial failed: %v", err)
	}
	defer conn.Close()

	testMsg := "Secure WSS on fnet"
	if err := wsutil.WriteClientText(conn, []byte(testMsg)); err != nil {
		t.Fatalf("WriteClientText failed: %v", err)
	}

	reply, err := wsutil.ReadServerText(conn)
	if err != nil {
		t.Fatalf("ReadServerText failed: %v", err)
	}
	if string(reply) != testMsg {
		t.Fatalf("expected %q, got %q", testMsg, string(reply))
	}
}

func TestWebSocketOriginCheck(t *testing.T) {
	port := getFreePort(t)
	addr := fmt.Sprintf("127.0.0.1:%d", port)

	upgrader := &websocket.Upgrader{
		CheckOrigin: func(r *http.Request) bool {
			return r.Header.Get("Origin") == "https://trusted.example.com"
		},
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/ws", func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r)
		if err != nil {
			return
		}
		defer conn.Close()
	})

	srv := &fhttp.Server{
		Addr:    addr,
		Handler: mux,
	}

	go func() {
		_ = srv.ListenAndServe()
	}()
	defer srv.Close()

	time.Sleep(50 * time.Millisecond)

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	// Dial with bad origin -> should fail
	dialerBad := ws.Dialer{
		Header: ws.HandshakeHeaderHTTP(http.Header{
			"Origin": []string{"https://evil.example.com"},
		}),
	}
	_, _, _, err := dialerBad.Dial(ctx, "ws://"+addr+"/ws")
	if err == nil {
		t.Fatal("expected handshake rejection on untrusted origin, got nil")
	}

	// Dial with good origin -> should succeed
	dialerGood := ws.Dialer{
		Header: ws.HandshakeHeaderHTTP(http.Header{
			"Origin": []string{"https://trusted.example.com"},
		}),
	}
	conn, _, _, err := dialerGood.Dial(ctx, "ws://"+addr+"/ws")
	if err != nil {
		t.Fatalf("expected handshake success on trusted origin, got err: %v", err)
	}
	conn.Close()
}

func TestWebSocketSubprotocol(t *testing.T) {
	port := getFreePort(t)
	addr := fmt.Sprintf("127.0.0.1:%d", port)

	upgrader := &websocket.Upgrader{
		Subprotocols: []string{"chat.v2", "chat.v1"},
	}

	var negotiatedProto atomic.Pointer[string]
	mux := http.NewServeMux()
	mux.HandleFunc("/ws", func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r)
		if err != nil {
			t.Errorf("Upgrade failed: %v", err)
			return
		}
		defer conn.Close()
		p := conn.Subprotocol()
		negotiatedProto.Store(&p)
	})

	srv := &fhttp.Server{
		Addr:    addr,
		Handler: mux,
	}

	go func() {
		_ = srv.ListenAndServe()
	}()
	defer srv.Close()

	time.Sleep(50 * time.Millisecond)

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	dialer := ws.Dialer{
		Protocols: []string{"chat.v2"},
	}
	conn, _, hs, err := dialer.Dial(ctx, "ws://"+addr+"/ws")
	if err != nil {
		t.Fatalf("Dial failed: %v", err)
	}
	defer conn.Close()

	if hs.Protocol != "chat.v2" {
		t.Fatalf("client expected protocol chat.v2, got %q", hs.Protocol)
	}
	time.Sleep(20 * time.Millisecond)
	protoPtr := negotiatedProto.Load()
	if protoPtr == nil || *protoPtr != "chat.v2" {
		t.Fatalf("server expected negotiated protocol chat.v2, got %v", protoPtr)
	}
}

func TestWebSocketZeroGoroutinesOnIdleConnections(t *testing.T) {
	skipOnPumpEmulation(t)
	port := getFreePort(t)
	addr := fmt.Sprintf("127.0.0.1:%d", port)

	upgrader := &websocket.Upgrader{
		CheckOrigin: func(r *http.Request) bool { return true },
		OnMessage: func(c *websocket.Conn, op websocket.OpCode, msg []byte) {
			_ = c.WriteMessage(op, msg)
		},
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/ws", func(w http.ResponseWriter, r *http.Request) {
		_, err := upgrader.Upgrade(w, r)
		if err != nil {
			t.Errorf("Upgrade failed: %v", err)
			return
		}
		// Return immediately! Worker goroutine exits!
	})

	srv := &fhttp.Server{
		Addr:    addr,
		Handler: mux,
	}

	go func() { _ = srv.ListenAndServe() }()
	defer srv.Close()

	time.Sleep(50 * time.Millisecond)

	initialGoroutines := runtime.NumGoroutine()

	const clientCount = 50
	conns := make([]net.Conn, clientCount)
	for i := 0; i < clientCount; i++ {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		c, _, _, err := ws.DefaultDialer.Dial(ctx, "ws://"+addr+"/ws")
		cancel()
		if err != nil {
			t.Fatalf("client %d dial failed: %v", i, err)
		}
		conns[i] = c
		defer c.Close()
	}

	time.Sleep(100 * time.Millisecond)

	// In the event-driven WebSocket architecture, 50 idle WebSocket connections
	// MUST NOT spawn 50 permanent worker goroutines.
	currentGoroutines := runtime.NumGoroutine()
	diff := currentGoroutines - initialGoroutines
	if diff > 10 {
		t.Fatalf("WebSocket idle goroutine leak: initial=%d, current=%d, diff=%d (expected <= 10)",
			initialGoroutines, currentGoroutines, diff)
	}
}

func TestWebSocketEventDrivenEcho(t *testing.T) {
	port := getFreePort(t)
	addr := fmt.Sprintf("127.0.0.1:%d", port)

	var opened, closed atomic.Bool
	upgrader := &websocket.Upgrader{
		CheckOrigin: func(r *http.Request) bool { return true },
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/ws", func(w http.ResponseWriter, r *http.Request) {
		_, err := upgrader.UpgradeEvent(w, r, websocket.EventHandler{
			OnOpen: func(c *websocket.Conn) {
				opened.Store(true)
			},
			OnMessage: func(c *websocket.Conn, op websocket.OpCode, msg []byte) {
				_ = c.WriteMessage(op, msg)
			},
			OnClose: func(c *websocket.Conn, err error) {
				closed.Store(true)
			},
		})
		if err != nil {
			t.Errorf("UpgradeEvent failed: %v", err)
			return
		}
	})

	srv := &fhttp.Server{
		Addr:    addr,
		Handler: mux,
	}

	go func() { _ = srv.ListenAndServe() }()
	defer srv.Close()

	time.Sleep(50 * time.Millisecond)

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	clientConn, _, _, err := ws.DefaultDialer.Dial(ctx, "ws://"+addr+"/ws")
	if err != nil {
		t.Fatalf("dial failed: %v", err)
	}
	defer clientConn.Close()

	// Wait for OnOpen
	time.Sleep(30 * time.Millisecond)
	if !opened.Load() {
		t.Fatalf("expected OnOpen to be called")
	}

	// Send message 1
	msg1 := "hello event-driven ws"
	if err := wsutil.WriteClientText(clientConn, []byte(msg1)); err != nil {
		t.Fatalf("write 1 failed: %v", err)
	}

	res1, err := wsutil.ReadServerText(clientConn)
	if err != nil {
		t.Fatalf("read 1 failed: %v", err)
	}
	if string(res1) != msg1 {
		t.Fatalf("expected %q, got %q", msg1, string(res1))
	}

	// Send message 2
	msg2 := "second message on same connection"
	if err := wsutil.WriteClientText(clientConn, []byte(msg2)); err != nil {
		t.Fatalf("write 2 failed: %v", err)
	}

	res2, err := wsutil.ReadServerText(clientConn)
	if err != nil {
		t.Fatalf("read 2 failed: %v", err)
	}
	if string(res2) != msg2 {
		t.Fatalf("expected %q, got %q", msg2, string(res2))
	}
}

func TestWebSocketEventDrivenPingPong(t *testing.T) {
	port := getFreePort(t)
	addr := fmt.Sprintf("127.0.0.1:%d", port)

	mux := http.NewServeMux()
	mux.HandleFunc("/ws", func(w http.ResponseWriter, r *http.Request) {
		_, err := websocket.UpgradeEvent(w, r, websocket.EventHandler{})
		if err != nil {
			t.Errorf("UpgradeEvent failed: %v", err)
			return
		}
	})

	srv := &fhttp.Server{
		Addr:    addr,
		Handler: mux,
	}

	go func() { _ = srv.ListenAndServe() }()
	defer srv.Close()

	time.Sleep(50 * time.Millisecond)

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	clientConn, _, _, err := ws.DefaultDialer.Dial(ctx, "ws://"+addr+"/ws")
	if err != nil {
		t.Fatalf("dial failed: %v", err)
	}
	defer clientConn.Close()

	// Client sends Ping frame (RFC 6455 requires client frames to be masked)
	pingPayload := []byte("ping-data-12345")
	if err := ws.WriteFrame(clientConn, ws.MaskFrame(ws.NewPingFrame(pingPayload))); err != nil {
		t.Fatalf("write ping failed: %v", err)
	}

	// Server should automatically respond with Pong frame carrying identical payload
	_ = clientConn.SetReadDeadline(time.Now().Add(2 * time.Second))
	frame, err := ws.ReadFrame(clientConn)
	if err != nil {
		t.Fatalf("read frame failed: %v", err)
	}
	if frame.Header.OpCode != ws.OpPong {
		t.Fatalf("expected OpPong, got opcode %v", frame.Header.OpCode)
	}
	if string(frame.Payload) != string(pingPayload) {
		t.Fatalf("expected pong payload %q, got %q", pingPayload, frame.Payload)
	}
}

func BenchmarkWebSocketEcho(b *testing.B) {
	port := getFreePort(&testing.T{})
	addr := fmt.Sprintf("127.0.0.1:%d", port)

	mux := http.NewServeMux()
	mux.HandleFunc("/ws", func(w http.ResponseWriter, r *http.Request) {
		conn, err := websocket.Upgrade(w, r)
		if err != nil {
			return
		}
		defer conn.Close()
		_ = conn.Handle(func(op websocket.OpCode, msg []byte) error {
			return conn.WriteMessage(op, msg)
		})
	})

	srv := &fhttp.Server{Addr: addr, Handler: mux}
	go func() { _ = srv.ListenAndServe() }()
	defer srv.Close()

	time.Sleep(50 * time.Millisecond)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	clientConn, _, _, err := ws.DefaultDialer.Dial(ctx, "ws://"+addr+"/ws")
	if err != nil {
		b.Fatalf("dial: %v", err)
	}
	defer clientConn.Close()

	payload := make([]byte, 1024)
	for i := range payload {
		payload[i] = byte(i)
	}

	b.ResetTimer()
	b.ReportAllocs()
	b.SetBytes(1024)

	for i := 0; i < b.N; i++ {
		if err := wsutil.WriteClientBinary(clientConn, payload); err != nil {
			b.Fatalf("write: %v", err)
		}
		resp, err := wsutil.ReadServerBinary(clientConn)
		if err != nil {
			b.Fatalf("read: %v", err)
		}
		if len(resp) != 1024 {
			b.Fatalf("bad resp len: %d", len(resp))
		}
	}
}

func BenchmarkWebSocketEventDrivenEcho(b *testing.B) {
	port := getFreePort(&testing.T{})
	addr := fmt.Sprintf("127.0.0.1:%d", port)

	upgrader := &websocket.Upgrader{
		OnMessage: func(c *websocket.Conn, op websocket.OpCode, msg []byte) {
			_ = c.WriteMessage(op, msg)
		},
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/ws", func(w http.ResponseWriter, r *http.Request) {
		_, _ = upgrader.Upgrade(w, r)
	})

	srv := &fhttp.Server{Addr: addr, Handler: mux}
	go func() { _ = srv.ListenAndServe() }()
	defer srv.Close()

	time.Sleep(50 * time.Millisecond)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	clientConn, _, _, err := ws.DefaultDialer.Dial(ctx, "ws://"+addr+"/ws")
	if err != nil {
		b.Fatalf("dial: %v", err)
	}
	defer clientConn.Close()

	payload := make([]byte, 1024)
	for i := range payload {
		payload[i] = byte(i)
	}

	b.ResetTimer()
	b.ReportAllocs()
	b.SetBytes(1024)

	for i := 0; i < b.N; i++ {
		if err := wsutil.WriteClientBinary(clientConn, payload); err != nil {
			b.Fatalf("write: %v", err)
		}
		resp, err := wsutil.ReadServerBinary(clientConn)
		if err != nil {
			b.Fatalf("read: %v", err)
		}
		if len(resp) != 1024 {
			b.Fatalf("bad resp len: %d", len(resp))
		}
	}
}

func TestWebSocketIdleMemoryFootprint(t *testing.T) {
	skipOnPumpEmulation(t)
	port := getFreePort(t)
	addr := fmt.Sprintf("127.0.0.1:%d", port)

	upgrader := &websocket.Upgrader{
		OnMessage: func(c *websocket.Conn, op websocket.OpCode, msg []byte) {
			_ = c.WriteMessage(op, msg)
		},
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/ws", func(w http.ResponseWriter, r *http.Request) {
		_, _ = upgrader.Upgrade(w, r)
	})

	srv := &fhttp.Server{Addr: addr, Handler: mux}
	go func() { _ = srv.ListenAndServe() }()
	defer srv.Close()

	time.Sleep(50 * time.Millisecond)

	runtime.GC()
	var msBefore runtime.MemStats
	runtime.ReadMemStats(&msBefore)

	const count = 200
	conns := make([]net.Conn, count)
	for i := 0; i < count; i++ {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		c, _, _, err := ws.DefaultDialer.Dial(ctx, "ws://"+addr+"/ws")
		cancel()
		if err != nil {
			t.Fatalf("dial %d failed: %v", i, err)
		}
		conns[i] = c
		defer c.Close()
	}

	time.Sleep(50 * time.Millisecond)
	runtime.GC()
	var msAfter runtime.MemStats
	runtime.ReadMemStats(&msAfter)

	heapGrowth := int64(msAfter.HeapAlloc) - int64(msBefore.HeapAlloc)
	bytesPerConn := heapGrowth / count
	t.Logf("Heap growth for %d conns: %d bytes (~%d bytes/conn)", count, heapGrowth, bytesPerConn)

	// Note that in this test both client dialer and server live in the same process.
	// Previously, each connection held a 4096-byte bufio.Writer + responseWriter + headers,
	// leading to >5KB per connection. Now server + client combined should be well below 3500 bytes/conn.
	if bytesPerConn > 3500 {
		t.Errorf("expected < 3500 bytes/conn, got %d bytes/conn", bytesPerConn)
	}
}

func createDeflateBomb(t *testing.T, uncompressedSize int) []byte {
	t.Helper()
	var buf bytes.Buffer
	w, err := flate.NewWriter(&buf, flate.BestCompression)
	if err != nil {
		t.Fatalf("flate.NewWriter: %v", err)
	}
	chunk := make([]byte, 32*1024)
	remaining := uncompressedSize
	for remaining > 0 {
		toWrite := len(chunk)
		if toWrite > remaining {
			toWrite = remaining
		}
		if _, err := w.Write(chunk[:toWrite]); err != nil {
			t.Fatalf("flate write: %v", err)
		}
		remaining -= toWrite
	}
	if err := w.Close(); err != nil {
		t.Fatalf("flate close: %v", err)
	}
	return buf.Bytes()
}

func TestWebSocketCompressionBomb(t *testing.T) {
	// 1. Create a compression bomb: 10MB of zeros compressed into a tiny payload (~10KB)
	const uncompressedBytes = 10 * 1024 * 1024 // 10 MB
	bombPayload := createDeflateBomb(t, uncompressedBytes)
	t.Logf("Compression bomb: uncompressed %d bytes -> compressed %d bytes (ratio: ~%.1fx)",
		uncompressedBytes, len(bombPayload), float64(uncompressedBytes)/float64(len(bombPayload)))

	// Subtest 1: Verify handshake does not negotiate permessage-deflate extension
	t.Run("HandshakeRejectsCompressionExtension", func(t *testing.T) {
		port := getFreePort(t)
		addr := fmt.Sprintf("127.0.0.1:%d", port)

		mux := http.NewServeMux()
		mux.HandleFunc("/ws", func(w http.ResponseWriter, r *http.Request) {
			conn, err := websocket.Upgrade(w, r)
			if err != nil {
				t.Errorf("Upgrade failed: %v", err)
				return
			}
			defer conn.Close()
		})

		srv := &fhttp.Server{Addr: addr, Handler: mux}
		go func() { _ = srv.ListenAndServe() }()
		defer srv.Close()

		time.Sleep(50 * time.Millisecond)

		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()

		dialer := ws.Dialer{
			Header: ws.HandshakeHeaderHTTP(http.Header{
				"Sec-WebSocket-Extensions": []string{"permessage-deflate; client_max_window_bits"},
			}),
		}
		clientConn, _, hs, err := dialer.Dial(ctx, "ws://"+addr+"/ws")
		if err != nil {
			t.Fatalf("Dial failed: %v", err)
		}
		defer clientConn.Close()

		// Server must not return Sec-WebSocket-Extensions header
		if len(hs.Extensions) > 0 {
			t.Fatalf("server unexpectedly negotiated extensions: %v", hs.Extensions)
		}
	})

	// Subtest 2: Event-driven mode receives raw wire bytes without decompressing or OOMing
	t.Run("EventDrivenModeSafeFromDecompressionBomb", func(t *testing.T) {
		port := getFreePort(t)
		addr := fmt.Sprintf("127.0.0.1:%d", port)

		var receivedLen atomic.Int64
		doneCh := make(chan struct{})

		upgrader := &websocket.Upgrader{
			OnMessage: func(c *websocket.Conn, op websocket.OpCode, msg []byte) {
				receivedLen.Store(int64(len(msg)))
				close(doneCh)
			},
		}

		mux := http.NewServeMux()
		mux.HandleFunc("/ws", func(w http.ResponseWriter, r *http.Request) {
			_, err := upgrader.Upgrade(w, r)
			if err != nil {
				t.Errorf("Upgrade failed: %v", err)
			}
		})

		srv := &fhttp.Server{Addr: addr, Handler: mux}
		go func() { _ = srv.ListenAndServe() }()
		defer srv.Close()

		time.Sleep(50 * time.Millisecond)

		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()

		clientConn, _, _, err := ws.DefaultDialer.Dial(ctx, "ws://"+addr+"/ws")
		if err != nil {
			t.Fatalf("Dial failed: %v", err)
		}
		defer clientConn.Close()

		// Send raw wire bytes without compression
		frame := ws.MaskFrame(ws.Frame{
			Header: ws.Header{
				Fin:    true,
				OpCode: ws.OpBinary,
				Masked: true,
				Length: int64(len(bombPayload)),
			},
			Payload: bombPayload,
		})

		if err := ws.WriteFrame(clientConn, frame); err != nil {
			t.Fatalf("WriteFrame failed: %v", err)
		}

		select {
		case <-doneCh:
			rec := receivedLen.Load()
			// Must receive raw wire payload length (~10KB), NOT 10MB
			if rec != int64(len(bombPayload)) {
				t.Fatalf("expected received size %d, got %d", len(bombPayload), rec)
			}
			t.Logf("Event-driven OnMessage received wire size %d bytes (server did not decompress)", rec)
		case <-time.After(3 * time.Second):
			t.Fatal("timed out waiting for OnMessage")
		}
	})

	// Subtest 3: Traditional goroutine mode receives raw wire bytes without decompressing
	t.Run("GoroutineModeSafeFromDecompressionBomb", func(t *testing.T) {
		port := getFreePort(t)
		addr := fmt.Sprintf("127.0.0.1:%d", port)

		var receivedLen atomic.Int64
		doneCh := make(chan struct{})

		mux := http.NewServeMux()
		mux.HandleFunc("/ws", func(w http.ResponseWriter, r *http.Request) {
			conn, err := websocket.Upgrade(w, r)
			if err != nil {
				t.Errorf("Upgrade failed: %v", err)
				return
			}
			defer conn.Close()

			op, msg, err := conn.ReadMessage()
			if err != nil {
				t.Errorf("ReadMessage error: %v", err)
				return
			}
			if op != websocket.OpBinary {
				t.Errorf("expected OpBinary, got %v", op)
			}
			receivedLen.Store(int64(len(msg)))
			close(doneCh)
		})

		srv := &fhttp.Server{Addr: addr, Handler: mux}
		go func() { _ = srv.ListenAndServe() }()
		defer srv.Close()

		time.Sleep(50 * time.Millisecond)

		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()

		clientConn, _, _, err := ws.DefaultDialer.Dial(ctx, "ws://"+addr+"/ws")
		if err != nil {
			t.Fatalf("Dial failed: %v", err)
		}
		defer clientConn.Close()

		frame := ws.MaskFrame(ws.Frame{
			Header: ws.Header{
				Fin:    true,
				OpCode: ws.OpBinary,
				Masked: true,
				Length: int64(len(bombPayload)),
			},
			Payload: bombPayload,
		})

		if err := ws.WriteFrame(clientConn, frame); err != nil {
			t.Fatalf("WriteFrame failed: %v", err)
		}

		select {
		case <-doneCh:
			rec := receivedLen.Load()
			if rec != int64(len(bombPayload)) {
				t.Fatalf("expected received size %d, got %d", len(bombPayload), rec)
			}
			t.Logf("Goroutine ReadMessage received wire size %d bytes", rec)
		case <-time.After(3 * time.Second):
			t.Fatal("timed out waiting for ReadMessage")
		}
	})

	// Subtest 4: Safe application-level decompression pattern with io.LimitReader
	t.Run("ApplicationLevelDecompressionDefense", func(t *testing.T) {
		// If application business logic specifically decodes deflate messages,
		// it must protect against decompression bombs using io.LimitReader.
		const maxAllowedDecompressed = 100 * 1024 // 100 KB limit

		flateReader := flate.NewReader(bytes.NewReader(bombPayload))
		defer flateReader.Close()

		// Read with limit + 1 to detect if payload exceeds limit
		limited := io.LimitReader(flateReader, maxAllowedDecompressed+1)
		var decompressedBuf bytes.Buffer
		n, err := io.Copy(&decompressedBuf, limited)

		if n > maxAllowedDecompressed {
			t.Logf("Successfully caught compression bomb at application layer: exceeded %d bytes limit", maxAllowedDecompressed)
		} else if err != nil {
			t.Logf("Decompression encountered error: %v", err)
		} else {
			t.Fatalf("expected decompression bomb to be caught by limit reader, but got %d bytes", n)
		}
	})
}

func compressClientFrame(f ws.Frame) (ws.Frame, error) {
	var buf bytes.Buffer
	w, err := flate.NewWriter(&buf, flate.BestCompression)
	if err != nil {
		return f, err
	}
	if _, err := w.Write(f.Payload); err != nil {
		return f, err
	}
	if err := w.Flush(); err != nil {
		return f, err
	}
	b := buf.Bytes()
	if len(b) >= 4 && bytes.Equal(b[len(b)-4:], []byte{0x00, 0x00, 0xff, 0xff}) {
		b = b[:len(b)-4]
	}
	f.Payload = b
	f.Header.Length = int64(len(b))
	f.Header.Rsv = ws.Rsv(true, false, false) // set RSV1
	return f, nil
}

func decompressServerFrame(f ws.Frame) (ws.Frame, error) {
	r := flate.NewReader(io.MultiReader(bytes.NewReader(f.Payload), bytes.NewReader([]byte{0x00, 0x00, 0xff, 0xff, 0x01, 0x00, 0x00, 0xff, 0xff})))
	defer r.Close()
	decomp, err := io.ReadAll(r)
	if err != nil {
		return f, err
	}
	f.Payload = decomp
	f.Header.Length = int64(len(decomp))
	return f, nil
}

func TestFlateRoundtrip(t *testing.T) {
	original := []byte("Hello, compressed fnet websocket! " + strings.Repeat("ABCDEFG ", 50))
	f := ws.NewTextFrame(original)
	comp, err := compressClientFrame(f)
	if err != nil {
		t.Fatalf("compress: %v", err)
	}
	t.Logf("original len=%d, compressed len=%d", len(original), len(comp.Payload))

	decomp, err := decompressServerFrame(comp)
	if err != nil {
		t.Fatalf("decompress: %v", err)
	}
	if !bytes.Equal(decomp.Payload, original) {
		t.Fatalf("mismatch")
	}
}

func TestWebSocketCompressionEcho(t *testing.T) {
	// Test normal compression echo for both goroutine mode and event-driven mode
	t.Run("EventDrivenMode", func(t *testing.T) {
		port := getFreePort(t)
		addr := fmt.Sprintf("127.0.0.1:%d", port)

		upgrader := &websocket.Upgrader{
			EnableCompression: true,
			OnMessage: func(c *websocket.Conn, op websocket.OpCode, msg []byte) {
				_ = c.WriteMessage(op, msg)
			},
		}

		mux := http.NewServeMux()
		mux.HandleFunc("/ws", func(w http.ResponseWriter, r *http.Request) {
			_, err := upgrader.Upgrade(w, r)
			if err != nil {
				t.Errorf("Upgrade failed: %v", err)
			}
		})

		srv := &fhttp.Server{Addr: addr, Handler: mux}
		go func() { _ = srv.ListenAndServe() }()
		defer srv.Close()

		time.Sleep(50 * time.Millisecond)

		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()

		dialer := ws.Dialer{
			Extensions: []httphead.Option{
				wsflate.DefaultParameters.Option(),
			},
		}
		clientConn, _, hs, err := dialer.Dial(ctx, "ws://"+addr+"/ws")
		if err != nil {
			t.Fatalf("Dial failed: %v", err)
		}
		defer clientConn.Close()

		if len(hs.Extensions) == 0 {
			t.Fatalf("expected compression extension negotiated, got %v", hs.Extensions)
		}

		// Prepare a compressible message > 128 bytes
		originalMsg := []byte("Hello, compressed fnet websocket! " + strings.Repeat("ABCDEFG ", 50))
		f := ws.NewTextFrame(originalMsg)
		compFrame, err := compressClientFrame(f)
		if err != nil {
			t.Fatalf("compressClientFrame: %v", err)
		}
		compFrame = ws.MaskFrameInPlace(compFrame)
		if err := ws.WriteFrame(clientConn, compFrame); err != nil {
			t.Fatalf("WriteFrame: %v", err)
		}

		// Read echo from server
		respFrame, err := ws.ReadFrame(clientConn)
		if err != nil {
			t.Fatalf("ReadFrame: %v", err)
		}

		if !respFrame.Header.Rsv1() {
			t.Fatalf("expected server response to have RSV1 compression bit set")
		}

		decompFrame, err := decompressServerFrame(respFrame)
		if err != nil {
			t.Fatalf("decompressServerFrame: %v", err)
		}

		if !bytes.Equal(decompFrame.Payload, originalMsg) {
			t.Fatalf("echoed payload mismatch: expected %q, got %q", string(originalMsg), string(decompFrame.Payload))
		}
		t.Logf("Successfully verified event-driven compression echo (orig: %d bytes, compressed: %d bytes)",
			len(originalMsg), len(respFrame.Payload))
	})

	t.Run("GoroutineMode", func(t *testing.T) {
		port := getFreePort(t)
		addr := fmt.Sprintf("127.0.0.1:%d", port)

		upgrader := &websocket.Upgrader{
			EnableCompression: true,
		}

		mux := http.NewServeMux()
		mux.HandleFunc("/ws", func(w http.ResponseWriter, r *http.Request) {
			conn, err := upgrader.Upgrade(w, r)
			if err != nil {
				t.Errorf("Upgrade failed: %v", err)
				return
			}
			defer conn.Close()

			if !conn.IsCompressed() {
				t.Errorf("expected conn.IsCompressed() == true")
			}

			op, msg, err := conn.ReadMessage()
			if err != nil {
				t.Errorf("ReadMessage: %v", err)
				return
			}
			_ = conn.WriteMessage(op, msg)
		})

		srv := &fhttp.Server{Addr: addr, Handler: mux}
		go func() { _ = srv.ListenAndServe() }()
		defer srv.Close()

		time.Sleep(50 * time.Millisecond)

		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()

		dialer := ws.Dialer{
			Extensions: []httphead.Option{
				wsflate.DefaultParameters.Option(),
			},
		}
		clientConn, _, hs, err := dialer.Dial(ctx, "ws://"+addr+"/ws")
		if err != nil {
			t.Fatalf("Dial failed: %v", err)
		}
		defer clientConn.Close()

		if len(hs.Extensions) == 0 {
			t.Fatalf("expected compression extension negotiated, got %v", hs.Extensions)
		}

		originalMsg := []byte("Goroutine mode compressed test: " + strings.Repeat("XYZ123 ", 40))
		f := ws.NewTextFrame(originalMsg)
		compFrame, err := compressClientFrame(f)
		if err != nil {
			t.Fatalf("compressClientFrame: %v", err)
		}
		compFrame = ws.MaskFrameInPlace(compFrame)
		if err := ws.WriteFrame(clientConn, compFrame); err != nil {
			t.Fatalf("WriteFrame: %v", err)
		}

		respFrame, err := ws.ReadFrame(clientConn)
		if err != nil {
			t.Fatalf("ReadFrame: %v", err)
		}

		if !respFrame.Header.Rsv1() {
			t.Fatalf("expected server response to have RSV1 compression bit set")
		}

		decompFrame, err := decompressServerFrame(respFrame)
		if err != nil {
			t.Fatalf("decompressServerFrame: %v", err)
		}

		if !bytes.Equal(decompFrame.Payload, originalMsg) {
			t.Fatalf("echoed payload mismatch")
		}
		t.Logf("Successfully verified goroutine mode compression echo")
	})
}

func TestWebSocketCompressionBombDefenseWhenCompressionEnabled(t *testing.T) {
	// Create a 10MB uncompressed compression bomb packed into ~10KB
	const bombUncompressedSize = 10 * 1024 * 1024 // 10 MB
	bombPayload := createDeflateBomb(t, bombUncompressedSize)
	t.Logf("Created compression bomb: uncompressed %d bytes -> wire %d bytes", bombUncompressedSize, len(bombPayload))

	const maxAllowed = 128 * 1024 // 128 KB max limit

	t.Run("EventDrivenModeBlocksBomb", func(t *testing.T) {
		port := getFreePort(t)
		addr := fmt.Sprintf("127.0.0.1:%d", port)

		var (
			messageReceived atomic.Bool
			closeErr        atomic.Pointer[error]
			closeCh         = make(chan struct{})
			closeOnce       sync.Once
		)

		upgrader := &websocket.Upgrader{
			EnableCompression:          true,
			MaxDecompressedMessageSize: maxAllowed,
			OnMessage: func(c *websocket.Conn, op websocket.OpCode, msg []byte) {
				// Must NEVER be called with the bomb!
				messageReceived.Store(true)
			},
			OnClose: func(c *websocket.Conn, err error) {
				closeErr.Store(&err)
				closeOnce.Do(func() { close(closeCh) })
			},
		}

		mux := http.NewServeMux()
		mux.HandleFunc("/ws", func(w http.ResponseWriter, r *http.Request) {
			_, err := upgrader.Upgrade(w, r)
			if err != nil {
				t.Errorf("Upgrade: %v", err)
			}
		})

		srv := &fhttp.Server{Addr: addr, Handler: mux}
		go func() { _ = srv.ListenAndServe() }()
		defer srv.Close()

		time.Sleep(50 * time.Millisecond)

		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()

		dialer := ws.Dialer{
			Extensions: []httphead.Option{
				wsflate.DefaultParameters.Option(),
			},
		}
		clientConn, _, hs, err := dialer.Dial(ctx, "ws://"+addr+"/ws")
		if err != nil {
			t.Fatalf("Dial: %v", err)
		}
		defer clientConn.Close()

		if len(hs.Extensions) == 0 {
			t.Fatalf("expected compression negotiated")
		}

		// Send compression bomb with RSV1 set
		frame := ws.MaskFrame(ws.Frame{
			Header: ws.Header{
				Fin:    true,
				Rsv:    ws.Rsv(true, false, false), // RSV1 = 1
				OpCode: ws.OpBinary,
				Masked: true,
				Length: int64(len(bombPayload)),
			},
			Payload: bombPayload,
		})

		if err := ws.WriteFrame(clientConn, frame); err != nil {
			t.Fatalf("WriteFrame failed: %v", err)
		}

		// Read frame from server - should be a Close frame with status 1009 (StatusMessageTooBig)
		replyFrame, err := ws.ReadFrame(clientConn)
		if err == nil {
			if replyFrame.Header.OpCode == ws.OpClose {
				code, reason := ws.ParseCloseFrameData(replyFrame.Payload)
				t.Logf("Client received expected Close frame from server: code=%v (%d), reason=%q", code, code, reason)
				if code != ws.StatusMessageTooBig {
					t.Errorf("expected close code %d (StatusMessageTooBig), got %d", ws.StatusMessageTooBig, code)
				}
			}
		}

		select {
		case <-closeCh:
			if messageReceived.Load() {
				t.Fatal("SECURITY ERROR: OnMessage was invoked with a decompression bomb!")
			}
			errPtr := closeErr.Load()
			t.Logf("Connection successfully closed on server with error: %v", *errPtr)
		case <-time.After(3 * time.Second):
			t.Fatal("timed out waiting for connection close")
		}
	})

	t.Run("GoroutineModeBlocksBomb", func(t *testing.T) {
		port := getFreePort(t)
		addr := fmt.Sprintf("127.0.0.1:%d", port)

		upgrader := &websocket.Upgrader{
			EnableCompression:          true,
			MaxDecompressedMessageSize: maxAllowed,
		}

		serverErrCh := make(chan error, 1)

		mux := http.NewServeMux()
		mux.HandleFunc("/ws", func(w http.ResponseWriter, r *http.Request) {
			conn, err := upgrader.Upgrade(w, r)
			if err != nil {
				t.Errorf("Upgrade: %v", err)
				return
			}
			defer conn.Close()

			_, _, readErr := conn.ReadMessage()
			serverErrCh <- readErr
		})

		srv := &fhttp.Server{Addr: addr, Handler: mux}
		go func() { _ = srv.ListenAndServe() }()
		defer srv.Close()

		time.Sleep(50 * time.Millisecond)

		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()

		dialer := ws.Dialer{
			Extensions: []httphead.Option{
				wsflate.DefaultParameters.Option(),
			},
		}
		clientConn, _, _, err := dialer.Dial(ctx, "ws://"+addr+"/ws")
		if err != nil {
			t.Fatalf("Dial: %v", err)
		}
		defer clientConn.Close()

		frame := ws.MaskFrame(ws.Frame{
			Header: ws.Header{
				Fin:    true,
				Rsv:    ws.Rsv(true, false, false),
				OpCode: ws.OpBinary,
				Masked: true,
				Length: int64(len(bombPayload)),
			},
			Payload: bombPayload,
		})

		if err := ws.WriteFrame(clientConn, frame); err != nil {
			t.Fatalf("WriteFrame failed: %v", err)
		}

		select {
		case sErr := <-serverErrCh:
			if sErr == nil {
				t.Fatal("SECURITY ERROR: ReadMessage did not report error on compression bomb!")
			}
			t.Logf("Goroutine mode successfully caught bomb: %v", sErr)
		case <-time.After(3 * time.Second):
			t.Fatal("timed out waiting for server error")
		}

		replyFrame, err := ws.ReadFrame(clientConn)
		if err == nil && replyFrame.Header.OpCode == ws.OpClose {
			code, reason := ws.ParseCloseFrameData(replyFrame.Payload)
			t.Logf("Client received Close frame: code=%v, reason=%q", code, reason)
			if code != ws.StatusMessageTooBig {
				t.Errorf("expected close code %d, got %d", ws.StatusMessageTooBig, code)
			}
		}
	})

	t.Run("FragmentedBombBlocked", func(t *testing.T) {
		port := getFreePort(t)
		addr := fmt.Sprintf("127.0.0.1:%d", port)

		var messageReceived atomic.Bool
		var closeErr atomic.Pointer[error]
		closeCh := make(chan struct{})
		var closeOnce sync.Once

		upgrader := &websocket.Upgrader{
			EnableCompression:          true,
			MaxDecompressedMessageSize: maxAllowed,
			OnMessage: func(c *websocket.Conn, op websocket.OpCode, msg []byte) {
				messageReceived.Store(true)
			},
			OnClose: func(c *websocket.Conn, err error) {
				closeErr.Store(&err)
				closeOnce.Do(func() { close(closeCh) })
			},
		}

		mux := http.NewServeMux()
		mux.HandleFunc("/ws", func(w http.ResponseWriter, r *http.Request) {
			_, err := upgrader.Upgrade(w, r)
			if err != nil {
				t.Errorf("Upgrade: %v", err)
			}
		})

		srv := &fhttp.Server{Addr: addr, Handler: mux}
		go func() { _ = srv.ListenAndServe() }()
		defer srv.Close()

		time.Sleep(50 * time.Millisecond)

		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()

		dialer := ws.Dialer{
			Extensions: []httphead.Option{
				wsflate.DefaultParameters.Option(),
			},
		}
		clientConn, _, _, err := dialer.Dial(ctx, "ws://"+addr+"/ws")
		if err != nil {
			t.Fatalf("Dial: %v", err)
		}
		defer clientConn.Close()

		// Split bombPayload into 2 fragments:
		// Frame 1: OpBinary, Fin=false, RSV1=1
		// Frame 2: OpContinuation, Fin=true, RSV1=0
		mid := len(bombPayload) / 2
		f1 := ws.MaskFrame(ws.Frame{
			Header: ws.Header{
				Fin:    false,
				Rsv:    ws.Rsv(true, false, false), // RSV1 = 1 on first fragment
				OpCode: ws.OpBinary,
				Masked: true,
				Length: int64(mid),
			},
			Payload: bombPayload[:mid],
		})
		if err := ws.WriteFrame(clientConn, f1); err != nil {
			t.Fatalf("WriteFrame f1: %v", err)
		}

		f2 := ws.MaskFrame(ws.Frame{
			Header: ws.Header{
				Fin:    true,
				Rsv:    0,
				OpCode: ws.OpContinuation,
				Masked: true,
				Length: int64(len(bombPayload) - mid),
			},
			Payload: bombPayload[mid:],
		})
		if err := ws.WriteFrame(clientConn, f2); err != nil {
			t.Fatalf("WriteFrame f2: %v", err)
		}

		select {
		case <-closeCh:
			if messageReceived.Load() {
				t.Fatal("SECURITY ERROR: OnMessage was invoked with a fragmented decompression bomb!")
			}
			t.Logf("Fragmented compression bomb was successfully blocked")
		case <-time.After(3 * time.Second):
			t.Fatal("timed out waiting for connection close")
		}

		replyFrame, err := ws.ReadFrame(clientConn)
		if err == nil && replyFrame.Header.OpCode == ws.OpClose {
			code, _ := ws.ParseCloseFrameData(replyFrame.Payload)
			if code != ws.StatusMessageTooBig {
				t.Errorf("expected 1009, got %v", code)
			}
		}
	})
}

func TestWebSocketAsyncDecompressionWorkerPool(t *testing.T) {
	port := getFreePort(t)
	addr := fmt.Sprintf("127.0.0.1:%d", port)

	var workerPoolTaskCount atomic.Int64
	customPool := func(connID uint64, task func()) error {
		workerPoolTaskCount.Add(1)
		go task()
		return nil
	}

	var receivedCount atomic.Int64
	doneCh := make(chan struct{})

	upgrader := &websocket.Upgrader{
		EnableCompression: true,
		WorkerPool:        customPool,
		OnMessage: func(c *websocket.Conn, op websocket.OpCode, msg []byte) {
			if receivedCount.Add(1) == 5 {
				close(doneCh)
			}
		},
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/ws", func(w http.ResponseWriter, r *http.Request) {
		_, err := upgrader.Upgrade(w, r)
		if err != nil {
			t.Errorf("Upgrade: %v", err)
		}
	})

	srv := &fhttp.Server{Addr: addr, Handler: mux}
	go func() { _ = srv.ListenAndServe() }()
	defer srv.Close()

	time.Sleep(50 * time.Millisecond)

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	dialer := ws.Dialer{
		Extensions: []httphead.Option{
			wsflate.DefaultParameters.Option(),
		},
	}
	clientConn, _, _, err := dialer.Dial(ctx, "ws://"+addr+"/ws")
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	defer clientConn.Close()

	for i := 0; i < 5; i++ {
		text := fmt.Sprintf("Message %d with repeated payload %s", i, strings.Repeat("DATA ", 30))
		f := ws.NewTextFrame([]byte(text))
		comp, err := compressClientFrame(f)
		if err != nil {
			t.Fatalf("compressClientFrame: %v", err)
		}
		comp = ws.MaskFrameInPlace(comp)
		if err := ws.WriteFrame(clientConn, comp); err != nil {
			t.Fatalf("WriteFrame: %v", err)
		}
	}

	select {
	case <-doneCh:
		t.Logf("Received all 5 messages. WorkerPool tasks dispatched: %d", workerPoolTaskCount.Load())
		if workerPoolTaskCount.Load() == 0 {
			t.Fatal("WorkerPool was never invoked!")
		}
	case <-time.After(3 * time.Second):
		t.Fatalf("timed out waiting for 5 messages, got %d", receivedCount.Load())
	}
}

func TestWebSocketAsyncMessageOrderingFIFO(t *testing.T) {
	// Verify that when AsyncDecompress is enabled, messages on the same connection
	// are delivered to OnMessage in strict FIFO order, even when compressed (slow)
	// and uncompressed (fast) messages are interleaved.
	port := getFreePort(t)
	addr := fmt.Sprintf("127.0.0.1:%d", port)

	const totalMsgs = 30
	receivedIDs := make([]int, 0, totalMsgs)
	var mu sync.Mutex
	doneCh := make(chan struct{})

	upgrader := &websocket.Upgrader{
		EnableCompression: true,
		OnMessage: func(c *websocket.Conn, op websocket.OpCode, msg []byte) {
			var id int
			_, err := fmt.Sscanf(string(msg), "MSG_%d_", &id)
			if err != nil {
				t.Errorf("failed to parse message ID from %q: %v", string(msg), err)
				return
			}
			mu.Lock()
			receivedIDs = append(receivedIDs, id)
			if len(receivedIDs) == totalMsgs {
				close(doneCh)
			}
			mu.Unlock()
		},
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/ws", func(w http.ResponseWriter, r *http.Request) {
		_, err := upgrader.Upgrade(w, r)
		if err != nil {
			t.Errorf("Upgrade: %v", err)
		}
	})

	srv := &fhttp.Server{Addr: addr, Handler: mux}
	go func() { _ = srv.ListenAndServe() }()
	defer srv.Close()

	time.Sleep(50 * time.Millisecond)

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	dialer := ws.Dialer{
		Extensions: []httphead.Option{
			wsflate.DefaultParameters.Option(),
		},
	}
	clientConn, _, _, err := dialer.Dial(ctx, "ws://"+addr+"/ws")
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	defer clientConn.Close()

	for i := 0; i < totalMsgs; i++ {
		text := fmt.Sprintf("MSG_%d_ payload %s", i, strings.Repeat("X", 200))
		f := ws.NewTextFrame([]byte(text))
		// Alternating: even is compressed, odd is uncompressed
		if i%2 == 0 {
			var err error
			f, err = compressClientFrame(f)
			if err != nil {
				t.Fatalf("compress: %v", err)
			}
		}
		f = ws.MaskFrameInPlace(f)
		if err := ws.WriteFrame(clientConn, f); err != nil {
			t.Fatalf("WriteFrame msg %d: %v", i, err)
		}
	}

	select {
	case <-doneCh:
		mu.Lock()
		defer mu.Unlock()
		for i, id := range receivedIDs {
			if id != i {
				t.Fatalf("FIFO violation at index %d: expected message ID %d, but got %d (full: %v)",
					i, i, id, receivedIDs)
			}
		}
		t.Logf("Successfully verified strict FIFO order for %d interleaved compressed/uncompressed messages", totalMsgs)
	case <-time.After(3 * time.Second):
		mu.Lock()
		got := len(receivedIDs)
		mu.Unlock()
		t.Fatalf("timed out waiting for messages, got %d/%d", got, totalMsgs)
	}
}

func TestWebSocketAsyncDecompressionBombDoesNotBlockReactor(t *testing.T) {
	// Verify that a malicious client sending a 10MB compression bomb does NOT
	// block or stall other connections handled by the server.
	port := getFreePort(t)
	addr := fmt.Sprintf("127.0.0.1:%d", port)

	upgrader := &websocket.Upgrader{
		EnableCompression:          true,
		MaxDecompressedMessageSize: 64 * 1024, // 64KB limit
		OnMessage: func(c *websocket.Conn, op websocket.OpCode, msg []byte) {
			_ = c.WriteMessage(op, msg)
		},
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/ws", func(w http.ResponseWriter, r *http.Request) {
		_, _ = upgrader.Upgrade(w, r)
	})

	srv := &fhttp.Server{Addr: addr, Handler: mux}
	go func() { _ = srv.ListenAndServe() }()
	defer srv.Close()

	time.Sleep(50 * time.Millisecond)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	dialer := ws.Dialer{
		Extensions: []httphead.Option{
			wsflate.DefaultParameters.Option(),
		},
	}

	// 1. Establish benign client connection
	benignConn, _, _, err := dialer.Dial(ctx, "ws://"+addr+"/ws")
	if err != nil {
		t.Fatalf("benign dial: %v", err)
	}
	defer benignConn.Close()

	// 2. Establish attacker client connection
	attackerConn, _, _, err := dialer.Dial(ctx, "ws://"+addr+"/ws")
	if err != nil {
		t.Fatalf("attacker dial: %v", err)
	}
	defer attackerConn.Close()

	// Attacker sends a 10MB compression bomb
	bomb := createDeflateBomb(t, 10*1024*1024)
	fBomb := ws.MaskFrame(ws.Frame{
		Header: ws.Header{
			Fin:    true,
			Rsv:    ws.Rsv(true, false, false),
			OpCode: ws.OpBinary,
			Masked: true,
			Length: int64(len(bomb)),
		},
		Payload: bomb,
	})
	if err := ws.WriteFrame(attackerConn, fBomb); err != nil {
		t.Fatalf("attacker write bomb: %v", err)
	}

	// Meanwhile, benign client immediately sends echo requests.
	// Because decompression is offloaded asynchronously, the benign client should
	// get instantaneous echo responses without being blocked by attacker's bomb.
	for i := 0; i < 5; i++ {
		pingText := fmt.Sprintf("instant echo %d", i)
		f := ws.MaskFrame(ws.NewTextFrame([]byte(pingText)))
		start := time.Now()
		if err := ws.WriteFrame(benignConn, f); err != nil {
			t.Fatalf("benign write: %v", err)
		}
		resp, err := ws.ReadFrame(benignConn)
		elapsed := time.Since(start)
		if err != nil {
			t.Fatalf("benign read: %v", err)
		}
		if string(resp.Payload) != pingText {
			t.Fatalf("benign mismatch: %q vs %q", string(resp.Payload), pingText)
		}
		t.Logf("Benign client roundtrip while bomb is processed: %v", elapsed)
		if elapsed > 100*time.Millisecond {
			t.Errorf("Reactor was stalled! Benign client took %v to echo", elapsed)
		}
	}

	// Verify attacker was closed with 1009
	replyFrame, err := ws.ReadFrame(attackerConn)
	if err == nil && replyFrame.Header.OpCode == ws.OpClose {
		code, _ := ws.ParseCloseFrameData(replyFrame.Payload)
		if code != ws.StatusMessageTooBig {
			t.Errorf("expected attacker close 1009, got %v", code)
		}
	}
}

// ---------------------------------------------------------------------------
// Tests for Large Frames, Streaming Assembly, Backpressure, and Cleanup
// ---------------------------------------------------------------------------

func TestWebSocketLargeFrames_StreamingAssemblyAndEcho(t *testing.T) {
	port := getFreePort(t)
	addr := fmt.Sprintf("127.0.0.1:%d", port)

	upgrader := &websocket.Upgrader{
		CheckOrigin: func(r *http.Request) bool { return true },
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/ws", func(w http.ResponseWriter, r *http.Request) {
		_, err := upgrader.UpgradeEvent(w, r, websocket.EventHandler{
			OnMessage: func(c *websocket.Conn, op websocket.OpCode, msg []byte) {
				_ = c.WriteMessage(op, msg)
			},
		})
		if err != nil {
			t.Errorf("UpgradeEvent error: %v", err)
		}
	})

	srv := &fhttp.Server{Addr: addr, Handler: mux}
	go func() { _ = srv.ListenAndServe() }()
	defer srv.Close()

	time.Sleep(50 * time.Millisecond)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	conn, _, _, err := ws.DefaultDialer.Dial(ctx, "ws://"+addr+"/ws")
	if err != nil {
		t.Fatalf("dial failed: %v", err)
	}
	defer conn.Close()

	testSizes := []int{
		128 * 1024,  // 128 KB
		512 * 1024,  // 512 KB
		1024 * 1024, // 1 MB
		2048 * 1024, // 2 MB
	}

	for _, size := range testSizes {
		t.Run(fmt.Sprintf("%d_Bytes", size), func(t *testing.T) {
			payload := make([]byte, size)
			for i := range payload {
				payload[i] = byte((i * 37) & 0xff)
			}

			// Send binary frame
			f := ws.MaskFrame(ws.NewBinaryFrame(payload))
			if err := ws.WriteFrame(conn, f); err != nil {
				t.Fatalf("write %d bytes failed: %v", size, err)
			}

			// Read echo
			resp, err := ws.ReadFrame(conn)
			if err != nil {
				t.Fatalf("read %d bytes failed: %v", size, err)
			}
			if len(resp.Payload) != size {
				t.Fatalf("expected len %d, got %d", size, len(resp.Payload))
			}
			if !bytes.Equal(resp.Payload, payload) {
				t.Fatalf("payload content mismatch for %d bytes", size)
			}
		})
	}
}

func TestWebSocketLargeFrame_PipeliningWithSmallFrame(t *testing.T) {
	port := getFreePort(t)
	addr := fmt.Sprintf("127.0.0.1:%d", port)

	upgrader := &websocket.Upgrader{
		CheckOrigin: func(r *http.Request) bool { return true },
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/ws", func(w http.ResponseWriter, r *http.Request) {
		_, err := upgrader.UpgradeEvent(w, r, websocket.EventHandler{
			OnMessage: func(c *websocket.Conn, op websocket.OpCode, msg []byte) {
				_ = c.WriteMessage(op, msg)
			},
		})
		if err != nil {
			t.Errorf("UpgradeEvent error: %v", err)
		}
	})

	srv := &fhttp.Server{Addr: addr, Handler: mux}
	go func() { _ = srv.ListenAndServe() }()
	defer srv.Close()

	time.Sleep(50 * time.Millisecond)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	conn, _, _, err := ws.DefaultDialer.Dial(ctx, "ws://"+addr+"/ws")
	if err != nil {
		t.Fatalf("dial failed: %v", err)
	}
	defer conn.Close()

	bigSize := 256 * 1024
	bigPayload := make([]byte, bigSize)
	for i := range bigPayload {
		bigPayload[i] = byte(i % 251)
	}
	smallPayload := []byte("immediately following small frame!")

	f1 := ws.MaskFrame(ws.NewBinaryFrame(bigPayload))
	f2 := ws.MaskFrame(ws.NewTextFrame(smallPayload))

	// Write both frames back-to-back in a single combined buffer
	var combined bytes.Buffer
	if err := ws.WriteFrame(&combined, f1); err != nil {
		t.Fatalf("write f1: %v", err)
	}
	if err := ws.WriteFrame(&combined, f2); err != nil {
		t.Fatalf("write f2: %v", err)
	}

	if _, err := conn.Write(combined.Bytes()); err != nil {
		t.Fatalf("conn.Write combined: %v", err)
	}

	// First echo must be the big frame
	resp1, err := ws.ReadFrame(conn)
	if err != nil {
		t.Fatalf("read resp1: %v", err)
	}
	if !bytes.Equal(resp1.Payload, bigPayload) {
		t.Fatalf("resp1 mismatch: len %d vs %d", len(resp1.Payload), len(bigPayload))
	}

	// Second echo must be the small frame
	resp2, err := ws.ReadFrame(conn)
	if err != nil {
		t.Fatalf("read resp2: %v", err)
	}
	if !bytes.Equal(resp2.Payload, smallPayload) {
		t.Fatalf("resp2 mismatch: %q vs %q", string(resp2.Payload), string(smallPayload))
	}
}

func TestWebSocketSilentBackpressure(t *testing.T) {
	port := getFreePort(t)
	addr := fmt.Sprintf("127.0.0.1:%d", port)

	var receivedCount atomic.Int32
	upgrader := &websocket.Upgrader{
		CheckOrigin: func(r *http.Request) bool { return true },
		// Set low threshold to easily trigger backpressure
		MaxPendingMessageBytes: 256 * 1024, // 256KB
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/ws", func(w http.ResponseWriter, r *http.Request) {
		_, err := upgrader.UpgradeEvent(w, r, websocket.EventHandler{
			OnMessage: func(c *websocket.Conn, op websocket.OpCode, msg []byte) {
				receivedCount.Add(1)
				// Simulate slow business processing
				time.Sleep(10 * time.Millisecond)
				_ = c.WriteMessage(op, msg[:10]) // reply with small ack
			},
		})
		if err != nil {
			t.Errorf("UpgradeEvent error: %v", err)
		}
	})

	srv := &fhttp.Server{Addr: addr, Handler: mux}
	go func() { _ = srv.ListenAndServe() }()
	defer srv.Close()

	time.Sleep(50 * time.Millisecond)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	conn, _, _, err := ws.DefaultDialer.Dial(ctx, "ws://"+addr+"/ws")
	if err != nil {
		t.Fatalf("dial failed: %v", err)
	}
	defer conn.Close()

	// Send 8 messages of 64KB each (8 * 64KB = 512KB > 256KB threshold)
	const numMessages = 8
	const msgSize = 64 * 1024
	payload := make([]byte, msgSize)

	for i := 0; i < numMessages; i++ {
		payload[0] = byte(i)
		f := ws.MaskFrame(ws.NewBinaryFrame(payload))
		if err := ws.WriteFrame(conn, f); err != nil {
			t.Fatalf("write frame %d: %v", i, err)
		}
	}

	// Read all 8 replies
	for i := 0; i < numMessages; i++ {
		resp, err := ws.ReadFrame(conn)
		if err != nil {
			t.Fatalf("read reply %d: %v", i, err)
		}
		if len(resp.Payload) < 1 || resp.Payload[0] != byte(i) {
			t.Fatalf("reply %d out of order or invalid: %v", i, resp.Payload)
		}
	}

	if receivedCount.Load() != numMessages {
		t.Fatalf("expected %d messages, got %d", numMessages, receivedCount.Load())
	}
}

func TestWebSocketMaxMessageSizeProtection(t *testing.T) {
	port := getFreePort(t)
	addr := fmt.Sprintf("127.0.0.1:%d", port)

	upgrader := &websocket.Upgrader{
		CheckOrigin:    func(r *http.Request) bool { return true },
		MaxMessageSize: 64 * 1024, // 64KB limit
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/ws", func(w http.ResponseWriter, r *http.Request) {
		_, err := upgrader.UpgradeEvent(w, r, websocket.EventHandler{
			OnMessage: func(c *websocket.Conn, op websocket.OpCode, msg []byte) {
				_ = c.WriteMessage(op, msg)
			},
		})
		if err != nil {
			t.Errorf("UpgradeEvent error: %v", err)
		}
	})

	srv := &fhttp.Server{Addr: addr, Handler: mux}
	go func() { _ = srv.ListenAndServe() }()
	defer srv.Close()

	time.Sleep(50 * time.Millisecond)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	conn, _, _, err := ws.DefaultDialer.Dial(ctx, "ws://"+addr+"/ws")
	if err != nil {
		t.Fatalf("dial failed: %v", err)
	}
	defer conn.Close()

	// 1. Send frame within limit: 32KB -> should succeed
	smallPayload := make([]byte, 32*1024)
	if err := ws.WriteFrame(conn, ws.MaskFrame(ws.NewBinaryFrame(smallPayload))); err != nil {
		t.Fatalf("write small: %v", err)
	}
	resp, err := ws.ReadFrame(conn)
	if err != nil || len(resp.Payload) != len(smallPayload) {
		t.Fatalf("read small echo failed: %v", err)
	}

	// 2. Send frame exceeding limit: 128KB -> should be closed with 1009
	largePayload := make([]byte, 128*1024)
	_ = ws.WriteFrame(conn, ws.MaskFrame(ws.NewBinaryFrame(largePayload)))

	reply, err := ws.ReadFrame(conn)
	if err == nil && reply.Header.OpCode == ws.OpClose {
		code, _ := ws.ParseCloseFrameData(reply.Payload)
		if code != ws.StatusMessageTooBig {
			t.Errorf("expected close code 1009 (StatusMessageTooBig), got %v", code)
		}
	}
}

func TestWebSocketLargeFrameAbortedAssemblyCleanup(t *testing.T) {
	port := getFreePort(t)
	addr := fmt.Sprintf("127.0.0.1:%d", port)

	upgrader := &websocket.Upgrader{
		CheckOrigin: func(r *http.Request) bool { return true },
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/ws", func(w http.ResponseWriter, r *http.Request) {
		_, _ = upgrader.UpgradeEvent(w, r, websocket.EventHandler{
			OnMessage: func(c *websocket.Conn, op websocket.OpCode, msg []byte) {
				_ = c.WriteMessage(op, msg)
			},
		})
	})

	srv := &fhttp.Server{Addr: addr, Handler: mux}
	go func() { _ = srv.ListenAndServe() }()
	defer srv.Close()

	time.Sleep(50 * time.Millisecond)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	conn, _, _, err := ws.DefaultDialer.Dial(ctx, "ws://"+addr+"/ws")
	if err != nil {
		t.Fatalf("dial failed: %v", err)
	}

	// Send frame header indicating 1MB payload, but only send 10KB of payload then abrupt close
	f := ws.MaskFrame(ws.Frame{
		Header: ws.Header{
			Fin:    true,
			OpCode: ws.OpBinary,
			Masked: true,
			Length: 1024 * 1024,
		},
		Payload: make([]byte, 10*1024),
	})
	_ = ws.WriteFrame(conn, f)

	// Abruptly close socket without finishing the remaining 1014KB
	_ = conn.Close()

	// Give server time to clean up
	time.Sleep(50 * time.Millisecond)

	// Now connect a new client and ensure server is healthy and pool is functional
	conn2, _, _, err := ws.DefaultDialer.Dial(ctx, "ws://"+addr+"/ws")
	if err != nil {
		t.Fatalf("dial 2 failed: %v", err)
	}
	defer conn2.Close()

	hello := []byte("server is healthy after aborted assembly")
	if err := wsutil.WriteClientText(conn2, hello); err != nil {
		t.Fatalf("write to conn2 failed: %v", err)
	}
	echo, err := wsutil.ReadServerText(conn2)
	if err != nil {
		t.Fatalf("read from conn2 failed: %v", err)
	}
	if string(echo) != string(hello) {
		t.Fatalf("echo mismatch: %q vs %q", string(echo), string(hello))
	}
}

func TestWebSocket_BatchCoalescing(t *testing.T) {
	port := getFreePort(t)
	addr := fmt.Sprintf("127.0.0.1:%d", port)

	upgrader := &websocket.Upgrader{}
	mux := http.NewServeMux()
	mux.HandleFunc("/ws", func(w http.ResponseWriter, r *http.Request) {
		_, _ = upgrader.UpgradeEvent(w, r, websocket.EventHandler{
			OnMessage: func(c *websocket.Conn, op websocket.OpCode, msg []byte) {
				if string(msg) == "trigger_manual_batch" {
					c.BeginBatch()
					_ = c.WriteText("batch_1")
					_ = c.WriteText("batch_2")
					_ = c.WriteText("batch_3")
					_ = c.EndBatch()
					return
				}
				_ = c.WriteMessage(op, msg)
			},
		})
	})

	srv := &fhttp.Server{Addr: addr, Handler: mux}
	go func() { _ = srv.ListenAndServe() }()
	defer srv.Close()

	time.Sleep(50 * time.Millisecond)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	conn, _, _, err := ws.DefaultDialer.Dial(ctx, "ws://"+addr+"/ws")
	if err != nil {
		t.Fatalf("dial failed: %v", err)
	}
	defer conn.Close()

	// 1. Test manual BeginBatch / EndBatch
	if err := wsutil.WriteClientText(conn, []byte("trigger_manual_batch")); err != nil {
		t.Fatalf("write trigger failed: %v", err)
	}
	for _, expected := range []string{"batch_1", "batch_2", "batch_3"} {
		msg, err := wsutil.ReadServerText(conn)
		if err != nil {
			t.Fatalf("read %s failed: %v", expected, err)
		}
		if string(msg) != expected {
			t.Fatalf("got %q want %q", string(msg), expected)
		}
	}

	// 2. Test burst messages to trigger automatic batching in processQueue
	const burstCount = 10
	for i := 0; i < burstCount; i++ {
		text := fmt.Sprintf("burst_msg_%d", i)
		if err := wsutil.WriteClientText(conn, []byte(text)); err != nil {
			t.Fatalf("write burst %d failed: %v", i, err)
		}
	}

	for i := 0; i < burstCount; i++ {
		msg, err := wsutil.ReadServerText(conn)
		if err != nil {
			t.Fatalf("read burst %d failed: %v", i, err)
		}
		expected := fmt.Sprintf("burst_msg_%d", i)
		if string(msg) != expected {
			t.Fatalf("burst %d: got %q want %q", i, string(msg), expected)
		}
	}

	// 3. Test multi-frame packet (benchcli-uwscpp style batchFrame in single TCP write)
	var multiBuf bytes.Buffer
	for i := 0; i < burstCount; i++ {
		payload := []byte(fmt.Sprintf("multi_msg_%d", i))
		f := ws.MaskFrame(ws.NewTextFrame(payload))
		_ = ws.WriteFrame(&multiBuf, f)
	}
	if _, err := conn.Write(multiBuf.Bytes()); err != nil {
		t.Fatalf("write multiBuf failed: %v", err)
	}
	for i := 0; i < burstCount; i++ {
		msg, err := wsutil.ReadServerText(conn)
		if err != nil {
			t.Fatalf("read multi %d failed: %v", i, err)
		}
		expected := fmt.Sprintf("multi_msg_%d", i)
		if string(msg) != expected {
			t.Fatalf("multi %d: got %q want %q", i, string(msg), expected)
		}
	}
}

// skipOnPumpEmulation skips tests that count goroutines or bytes per idle
// connection: the Windows emulation in internal/netpoll parks a read-pump
// goroutine on every socket, which the native epoll/kqueue pollers do not.
func skipOnPumpEmulation(t *testing.T) {
	t.Helper()
	if runtime.GOOS == "windows" {
		t.Skip("the Windows socket emulation holds a pump goroutine per connection")
	}
}

// Messages that arrive before the client's Close frame are all delivered, in
// order, and OnClose runs after them.
func TestWebSocketMessagesBeforeCloseAreDelivered(t *testing.T) {
	var (
		mu       sync.Mutex
		got      []string
		atClose  = -1
		closedCh = make(chan struct{})
	)
	upgrader := &websocket.Upgrader{
		OnMessage: func(c *websocket.Conn, op websocket.OpCode, msg []byte) {
			time.Sleep(20 * time.Millisecond) // slow business logic
			mu.Lock()
			got = append(got, string(msg))
			mu.Unlock()
		},
		OnClose: func(c *websocket.Conn, err error) {
			mu.Lock()
			atClose = len(got)
			mu.Unlock()
			close(closedCh)
		},
	}
	addr := startWSServer(t, upgrader)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	conn, _, _, err := ws.DefaultDialer.Dial(ctx, "ws://"+addr+"/ws")
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	var burst bytes.Buffer
	for i := 0; i < 5; i++ {
		_ = ws.WriteFrame(&burst, ws.MaskFrame(ws.NewTextFrame([]byte(fmt.Sprintf("m%d", i)))))
	}
	_ = ws.WriteFrame(&burst, ws.MaskFrame(ws.NewCloseFrame(ws.NewCloseFrameBody(ws.StatusNormalClosure, ""))))
	if _, err := conn.Write(burst.Bytes()); err != nil {
		t.Fatal(err)
	}

	select {
	case <-closedCh:
	case <-time.After(5 * time.Second):
		t.Fatal("OnClose not called")
	}
	mu.Lock()
	defer mu.Unlock()
	if strings.Join(got, ",") != "m0,m1,m2,m3,m4" || atClose != 5 {
		t.Fatalf("delivered %v, OnClose after %d messages", got, atClose)
	}
}

// A panicking OnMessage closes its connection with 1011; the server lives on.
func TestWebSocketOnMessagePanicClosesWith1011(t *testing.T) {
	upgrader := &websocket.Upgrader{
		OnMessage: func(c *websocket.Conn, op websocket.OpCode, msg []byte) {
			if string(msg) == "boom" {
				panic("handler bug")
			}
			_ = c.WriteMessage(op, msg)
		},
	}
	addr := startWSServer(t, upgrader)

	dial := func() net.Conn {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		conn, _, _, err := ws.DefaultDialer.Dial(ctx, "ws://"+addr+"/ws")
		if err != nil {
			t.Fatal(err)
		}
		_ = conn.SetDeadline(time.Now().Add(5 * time.Second))
		return conn
	}

	bad := dial()
	defer bad.Close()
	_ = wsutil.WriteClientText(bad, []byte("boom"))
	f, err := ws.ReadFrame(bad)
	if err != nil || f.Header.OpCode != ws.OpClose {
		t.Fatalf("want a Close frame, got %+v %v", f.Header, err)
	}
	if code, _ := ws.ParseCloseFrameData(f.Payload); code != ws.StatusInternalServerError {
		t.Fatalf("close status %d, want 1011", code)
	}

	good := dial()
	defer good.Close()
	_ = wsutil.WriteClientText(good, []byte("hi"))
	if msg, err := wsutil.ReadServerText(good); err != nil || string(msg) != "hi" {
		t.Fatalf("echo after panic: %q %v", msg, err)
	}
}

func startWSServer(t *testing.T, u *websocket.Upgrader) string {
	t.Helper()
	addr := fmt.Sprintf("127.0.0.1:%d", getFreePort(t))
	mux := http.NewServeMux()
	mux.HandleFunc("/ws", func(w http.ResponseWriter, r *http.Request) { _, _ = u.Upgrade(w, r) })
	srv := &fhttp.Server{Addr: addr, Handler: mux}
	go func() { _ = srv.ListenAndServe() }()
	t.Cleanup(func() { _ = srv.Close() })
	time.Sleep(50 * time.Millisecond)
	return addr
}
