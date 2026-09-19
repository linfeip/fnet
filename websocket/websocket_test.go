package websocket_test

import (
	"context"
	"crypto/tls"
	"fmt"
	"net"
	"net/http"
	"testing"
	"time"

	"fnet"
	"fnet/websocket"

	"github.com/gobwas/ws"
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

	srv := &fnet.Server{
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

	certPEM, keyPEM, err := fnet.GenerateSelfSignedCertPEM()
	if err != nil {
		t.Fatalf("GenerateSelfSignedCertPEM: %v", err)
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

	srv := &fnet.Server{
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

	srv := &fnet.Server{
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

	var negotiatedProto string
	mux := http.NewServeMux()
	mux.HandleFunc("/ws", func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r)
		if err != nil {
			t.Errorf("Upgrade failed: %v", err)
			return
		}
		defer conn.Close()
		negotiatedProto = conn.Subprotocol()
	})

	srv := &fnet.Server{
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
	if negotiatedProto != "chat.v2" {
		t.Fatalf("server expected negotiated protocol chat.v2, got %q", negotiatedProto)
	}
}
