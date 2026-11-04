// udp-kcp is an example KCP reliable-UDP echo server built on fkcp.
//
// Run a standalone echo server:
//
//	cd examples && go run ./udp-kcp -addr :9000
//
// Or run a self-contained interoperability check with a github.com/xtaci/kcp-go/v5 client:
//
//	cd examples && go run ./udp-kcp -demo
package main

import (
	"bytes"
	"errors"
	"flag"
	"io"
	"log/slog"
	"time"

	"github.com/linfeip/fnet"
	"github.com/linfeip/fnet/fkcp"
	"github.com/xtaci/kcp-go/v5"
)

type echo struct{}

func (echo) OnOpen(c *fkcp.Conn) {}

func (echo) OnData(c *fkcp.Conn, data []byte) {
	_, _ = c.Write(data)
}

func (echo) OnClose(c *fkcp.Conn, err error) {}

func main() {
	addr := flag.String("addr", ":9000", "listen address")
	maxSessions := flag.Int("max-sessions", 65536, "maximum concurrent KCP sessions")
	idleTimeout := flag.Duration("idle", 30*time.Second, "idle timeout before evicting a session")
	demo := flag.Bool("demo", false, "run a self-contained interop and idle-eviction check and exit")
	flag.Parse()

	if *demo {
		runDemo()
		return
	}

	srv, err := fkcp.NewServer(*addr, echo{}, fkcp.Options{
		UDP:         fnet.UDPOptions{ReusePort: true},
		NoDelay:     true,
		StreamMode:  true,
		MaxSessions: *maxSessions,
		IdleTimeout: *idleTimeout,
	})
	if err != nil {
		panic(err)
	}
	slog.Info("kcp echo server listening", "addr", srv.Addr().String(), "maxSessions", *maxSessions, "idle", *idleTimeout)
	if err := srv.Serve(); err != nil && !errors.Is(err, fnet.ErrServerClosed) {
		panic(err)
	}
}

func runDemo() {
	srv, err := fkcp.NewServer("127.0.0.1:0", echo{}, fkcp.Options{
		UDP:         fnet.UDPOptions{ReusePort: true},
		NoDelay:     true,
		StreamMode:  true,
		MaxSessions: 16,
		IdleTimeout: 150 * time.Millisecond,
	})
	if err != nil {
		panic(err)
	}
	served := make(chan error, 1)
	go func() { served <- srv.Serve() }()

	// Connect an unmodified kcp-go client (pure ARQ, no encryption/FEC) to verify wire compatibility.
	client, err := kcp.DialWithOptions(srv.Addr().String(), nil, 0, 0)
	if err != nil {
		panic(err)
	}
	client.SetStreamMode(true)
	client.SetWriteDelay(false)
	client.SetNoDelay(1, 10, 2, 1)
	client.SetWindowSize(128, 128)
	_ = client.SetDeadline(time.Now().Add(5 * time.Second))

	payload := make([]byte, 64<<10)
	for i := range payload {
		payload[i] = byte(i*17 + 3)
	}
	if _, err := client.Write(payload); err != nil {
		panic(err)
	}
	reply := make([]byte, len(payload))
	if _, err := io.ReadFull(client, reply); err != nil {
		panic(err)
	}
	if !bytes.Equal(reply, payload) {
		panic("kcp echo payload mismatch")
	}
	_ = client.Close()
	slog.Info("fkcp <-> kcp-go wire interop verified", "bytes", len(payload), "activeSessions", srv.SessionCount())

	time.Sleep(250 * time.Millisecond)
	if remaining := srv.SessionCount(); remaining != 0 {
		panic("expected idle session to be evicted")
	}
	slog.Info("fkcp idle session eviction verified", "activeSessions", srv.SessionCount())

	_ = srv.Close()
	if err := <-served; !errors.Is(err, fnet.ErrServerClosed) {
		panic(err)
	}
}
