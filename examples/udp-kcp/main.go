// udp-kcp demonstrates running KCP reliable streams on top of fnet's stateless UDP datagram API.
//
// fnet manages only the non-blocking UDP sockets and serial OnPacket callbacks; per-peer KCP state, session limits,
// idle eviction, and the periodic Update ticker live in the application layer.
//
// Run a standalone echo server:
//
//	cd examples && go run ./udp-kcp -addr :9000
//
// Or run a self-contained client/server verification (including multi-segment payloads and idle eviction):
//
//	cd examples && go run ./udp-kcp -demo
package main

import (
	"bytes"
	"encoding/binary"
	"errors"
	"flag"
	"io"
	"log/slog"
	"net/netip"
	"sync"
	"sync/atomic"
	"time"

	"github.com/linfeip/fnet"
	"github.com/xtaci/kcp-go/v5"
)

const kcpTickInterval = 10 * time.Millisecond

type kcpSession struct {
	mu           sync.Mutex
	stateMachine *kcp.KCP
	recvBuf      []byte
	lastActive   atomic.Int64
}

func newKCPSession(conn fnet.UDPConn, peerAddr netip.AddrPort, conversationID uint32, now int64) *kcpSession {
	sm := kcp.NewKCP(conversationID, func(buf []byte, size int) {
		if size > 0 {
			_, _ = conn.WriteToUDPAddrPort(buf[:size], peerAddr)
		}
	})
	sm.NoDelay(1, 10, 2, 1)
	sm.WndSize(128, 128)
	s := &kcpSession{
		stateMachine: sm,
		recvBuf:      make([]byte, 64<<10),
	}
	s.lastActive.Store(now)
	return s
}

func (s *kcpSession) onPacket(data []byte, now int64) {
	s.lastActive.Store(now)
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.stateMachine.Input(data, kcp.IKCP_PACKET_REGULAR, true) < 0 {
		return
	}
	for {
		size := s.stateMachine.PeekSize()
		if size <= 0 {
			break
		}
		if size > len(s.recvBuf) {
			s.recvBuf = make([]byte, size)
		}
		n := s.stateMachine.Recv(s.recvBuf[:size])
		if n > 0 {
			_ = s.stateMachine.Send(s.recvBuf[:n])
		}
	}
	s.stateMachine.Update()
}

func (s *kcpSession) update() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.stateMachine.Update()
}

type sessionTable struct {
	mu          sync.Mutex
	sessions    map[netip.AddrPort]*kcpSession
	batch       []*kcpSession
	maxSessions int
	idleTimeout time.Duration
}

func newSessionTable(maxSessions int, idleTimeout time.Duration) *sessionTable {
	return &sessionTable{
		sessions:    make(map[netip.AddrPort]*kcpSession),
		maxSessions: maxSessions,
		idleTimeout: idleTimeout,
	}
}

func (t *sessionTable) OnPacket(c fnet.UDPConn, data []byte, from netip.AddrPort) {
	if len(data) < kcp.IKCP_OVERHEAD {
		return
	}
	now := time.Now().UnixNano()
	session := t.getOrCreate(c, from, binary.LittleEndian.Uint32(data), now)
	if session == nil {
		return // session cap reached: drop packets from new peers so spoofed source addresses cannot exhaust memory
	}
	session.onPacket(data, now)
}

func (t *sessionTable) getOrCreate(c fnet.UDPConn, from netip.AddrPort, conversationID uint32, now int64) *kcpSession {
	t.mu.Lock()
	defer t.mu.Unlock()
	if s, ok := t.sessions[from]; ok {
		return s
	}
	if len(t.sessions) >= t.maxSessions {
		return nil
	}
	s := newKCPSession(c, from, conversationID, now)
	t.sessions[from] = s
	return s
}

// tick evicts idle sessions and drives Update on active ones using a single shared ticker rather than one timer per peer.
func (t *sessionTable) tick(now time.Time) {
	cutoff := now.Add(-t.idleTimeout).UnixNano()
	t.mu.Lock()
	t.batch = t.batch[:0]
	for addr, s := range t.sessions {
		if s.lastActive.Load() < cutoff {
			delete(t.sessions, addr)
			continue
		}
		t.batch = append(t.batch, s)
	}
	active := t.batch
	t.mu.Unlock()

	for _, s := range active {
		s.update()
	}
}

func (t *sessionTable) count() int {
	t.mu.Lock()
	defer t.mu.Unlock()
	return len(t.sessions)
}

func main() {
	addr := flag.String("addr", ":9000", "listen address")
	maxSessions := flag.Int("max-sessions", 4096, "maximum concurrent peer sessions")
	idleTimeout := flag.Duration("idle", 30*time.Second, "idle timeout before evicting a peer session")
	demo := flag.Bool("demo", false, "run a self-contained KCP echo and idle-eviction check and exit")
	flag.Parse()

	if *demo {
		runDemo()
		return
	}

	table := newSessionTable(*maxSessions, *idleTimeout)
	srv, err := fnet.NewUDPServer(*addr, table, fnet.UDPOptions{ReusePort: true})
	if err != nil {
		panic(err)
	}
	go func() {
		ticker := time.NewTicker(kcpTickInterval)
		defer ticker.Stop()
		for now := range ticker.C {
			table.tick(now)
		}
	}()
	slog.Info("udp kcp server listening", "addr", srv.Addr().String(), "maxSessions", *maxSessions, "idle", *idleTimeout)
	if err := srv.Serve(); err != nil && !errors.Is(err, fnet.ErrServerClosed) {
		panic(err)
	}
}

func runDemo() {
	table := newSessionTable(16, 150*time.Millisecond)
	srv, err := fnet.NewUDPServer("127.0.0.1:0", table, fnet.UDPOptions{ReusePort: true})
	if err != nil {
		panic(err)
	}
	stopTicker := make(chan struct{})
	go func() {
		ticker := time.NewTicker(kcpTickInterval)
		defer ticker.Stop()
		for {
			select {
			case now := <-ticker.C:
				table.tick(now)
			case <-stopTicker:
				return
			}
		}
	}()
	served := make(chan error, 1)
	go func() { served <- srv.Serve() }()

	// Connect a standard kcp-go client (pure ARQ, no encryption/FEC) and exchange a multi-segment 32KB payload.
	client, err := kcp.DialWithOptions(srv.Addr().String(), nil, 0, 0)
	if err != nil {
		panic(err)
	}
	client.SetStreamMode(true)
	client.SetNoDelay(1, 10, 2, 1)
	client.SetWindowSize(128, 128)
	_ = client.SetDeadline(time.Now().Add(5 * time.Second))

	payload := make([]byte, 32<<10)
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
	slog.Info("kcp reliable stream verified", "bytes", len(payload), "activeSessions", table.count())

	// Wait past the 150ms idle timeout so the unified ticker evicts the session.
	time.Sleep(250 * time.Millisecond)
	if remaining := table.count(); remaining != 0 {
		panic("expected idle session to be evicted")
	}
	slog.Info("kcp idle session eviction verified", "activeSessions", table.count())

	close(stopTicker)
	_ = srv.Close()
	if err := <-served; !errors.Is(err, fnet.ErrServerClosed) {
		panic(err)
	}
}
