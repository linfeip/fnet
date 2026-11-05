// Package kcp is a zero-dependency KCP reliable-UDP server built on fnet.UDPServer.
//
// It implements the wire-compatible KCP ARQ protocol directly over fnet's non-blocking UDP sockets without spawning a
// goroutine per session:
//   - Sessions are partitioned across 64 shards keyed by value-type netip.AddrPort (zero heap allocation per lookup).
//   - Segment payloads and temporary packet encoding buffers borrow from fnet's internal bytepool, so an idle session
//     holds no packet buffers and steady-state echo allocates nothing on the heap.
//   - A single background ticker drives periodic retransmissions and idle eviction across all shards.
package kcp

import (
	"encoding/binary"
	"io"
	"log/slog"
	"net"
	"net/netip"
	"os"
	"runtime/debug"
	"sync"
	"sync/atomic"
	"time"

	"github.com/linfeip/fnet"
)

const (
	numSessionShards        = 64
	defaultSocketBufferSize = 4 << 20 // 4MB SO_RCVBUF / SO_SNDBUF to absorb KCP sliding-window bursts
	defaultInterval         = 10 * time.Millisecond
	defaultIdleTimeout      = 30 * time.Second
	defaultMaxSessions      = 65536
)

// Handler receives the lifecycle and message callbacks of KCP sessions.
//
// Callbacks of a single Conn always run serially (OnOpen first, OnClose last and only once), and run on fnet's
// executor goroutines (or the caller of Close/tick for an idle session), so they should return quickly and hand
// blocking work to other goroutines, which may call Conn.Write and Conn.Close concurrently.
type Handler interface {
	// OnOpen is called when the first valid KCP packet of a new session arrives.
	OnOpen(c *Conn)
	// OnData is called for each complete message (or contiguous chunk when Options.StreamMode is true). data is valid
	// only within this callback; copy it if it must be retained afterwards.
	OnData(c *Conn, data []byte)
	// OnClose is called once when the session closes; err is nil for Conn.Close, os.ErrDeadlineExceeded on idle
	// timeout, ErrDeadLink after unacknowledged retransmissions, io.EOF when the peer resets with a new conversation ID
	// from the same address, or fnet.ErrServerClosed on Server.Close.
	OnClose(c *Conn, err error)
}

// Options configures a KCP Server; the zero value provides production-ready defaults.
type Options struct {
	// UDP configures the underlying fnet.UDPServer (ReusePort, ReadBufferSize, WriteBufferSize, NumLoops, Executor).
	// When ReadBufferSize or WriteBufferSize is 0, NewServer defaults it to 4MB so multi-segment KCP bursts do not
	// overflow small OS default socket buffers.
	UDP fnet.UDPOptions
	// MTU is the maximum UDP payload size emitted by KCP; 1400 when <=0 (clamped to [128, 65500]).
	MTU int
	// SendWindow and ReceiveWindow set the KCP sliding window sizes in segments; 128 when <=0.
	SendWindow    int
	ReceiveWindow int
	// Interval is the periodic ARQ timer interval; 10ms when <=0 (clamped to [10ms, 5s]).
	Interval time.Duration
	// IdleTimeout closes sessions that receive no packets for this duration; 0 means 30s, <0 disables idle eviction.
	IdleTimeout time.Duration
	// MaxSessions caps the total concurrent sessions across all shards to bound memory against spoofed source IPs;
	// 65536 when <=0.
	MaxSessions int
	// NoDelay enables low-latency ARQ: 30ms minimum RTO, fast retransmit after 2 out-of-order ACKs, and congestion
	// window bypass.
	NoDelay bool
	// StreamMode treats the session as a byte stream (coalescing small writes and delivering contiguous chunks as soon
	// as they arrive) rather than preserving per-Write message boundaries.
	StreamMode bool
}

func (o Options) withDefaults() Options {
	if o.UDP.ReadBufferSize == 0 {
		o.UDP.ReadBufferSize = defaultSocketBufferSize
	}
	if o.UDP.WriteBufferSize == 0 {
		o.UDP.WriteBufferSize = defaultSocketBufferSize
	}
	if o.MTU <= 0 {
		o.MTU = defaultMTU
	}
	o.MTU = min(max(o.MTU, minMTU), maxMTU)
	if o.SendWindow <= 0 {
		o.SendWindow = defaultWindowSize
	}
	if o.ReceiveWindow <= 0 {
		o.ReceiveWindow = defaultWindowSize
	}
	if o.Interval <= 0 {
		o.Interval = defaultInterval
	}
	o.Interval = min(max(o.Interval, 10*time.Millisecond), 5*time.Second)
	if o.IdleTimeout == 0 {
		o.IdleTimeout = defaultIdleTimeout
	}
	if o.MaxSessions <= 0 {
		o.MaxSessions = defaultMaxSessions
	}
	return o
}

type sessionShard struct {
	srv      *Server
	mu       sync.Mutex
	sessions map[netip.AddrPort]*Conn
	batch    []*Conn
}

func (sh *sessionShard) remove(addr netip.AddrPort, target *Conn) {
	sh.mu.Lock()
	if sh.sessions[addr] == target {
		delete(sh.sessions, addr)
		sh.srv.sessionCount.Add(-1)
	}
	sh.mu.Unlock()
}

// Server is a KCP server running on top of an fnet.UDPServer.
type Server struct {
	handler      Handler
	opts         Options
	udpServer    *fnet.UDPServer
	startClock   time.Time
	shards       [numSessionShards]sessionShard
	sessionCount atomic.Int64
	callbacksWg  sync.WaitGroup

	mu        sync.Mutex
	serving   bool
	closed    atomic.Bool
	stopping  chan struct{}
	stopped   chan struct{}
	done      chan struct{}
	closeOnce sync.Once
}

// NewServer creates a KCP server bound to addr; sessions start being accepted once Serve is called.
func NewServer(addr string, handler Handler, opts Options) (*Server, error) {
	opts = opts.withDefaults()
	s := &Server{
		handler:    handler,
		opts:       opts,
		startClock: time.Now(),
		stopping:   make(chan struct{}),
		stopped:    make(chan struct{}),
		done:       make(chan struct{}),
	}
	for i := range s.shards {
		s.shards[i].srv = s
		s.shards[i].sessions = make(map[netip.AddrPort]*Conn)
	}
	udpSrv, err := fnet.NewUDPServer(addr, s, opts.UDP)
	if err != nil {
		return nil, err
	}
	s.udpServer = udpSrv
	return s, nil
}

// Addr returns the UDP address actually bound by the server.
func (s *Server) Addr() net.Addr { return s.udpServer.Addr() }

// UDPConn returns one of the underlying UDP sockets, allowing outbound hole-punching packets before any KCP session is
// established.
func (s *Server) UDPConn() fnet.UDPConn { return s.udpServer.Conn() }

// SessionCount returns the number of currently active KCP sessions.
func (s *Server) SessionCount() int { return int(s.sessionCount.Load()) }

// Serve runs the periodic KCP timer loop and the underlying fnet.UDPServer until Close is called (returning
// fnet.ErrServerClosed).
func (s *Server) Serve() error {
	s.mu.Lock()
	if s.closed.Load() {
		s.mu.Unlock()
		return fnet.ErrServerClosed
	}
	if s.serving {
		s.mu.Unlock()
		return s.udpServer.Serve()
	}
	s.serving = true
	s.mu.Unlock()

	go s.tickLoop()
	err := s.udpServer.Serve()
	s.stopTicker()
	<-s.stopped
	s.closeAllSessions()
	s.callbacksWg.Wait()
	close(s.done)
	return err
}

// Close stops the server, closes all active KCP sessions (invoking OnClose with fnet.ErrServerClosed), and waits for
// all callbacks to finish. Like fnet.Server.Close, it must not be called directly from inside a callback.
func (s *Server) Close() error {
	s.mu.Lock()
	s.closed.Store(true)
	serving := s.serving
	s.mu.Unlock()

	s.stopTicker()
	err := s.udpServer.Close()
	if serving {
		<-s.done
	}
	return err
}

func (s *Server) stopTicker() {
	s.closeOnce.Do(func() {
		close(s.stopping)
	})
}

func (s *Server) nowNs() int64  { return int64(time.Since(s.startClock)) }
func (s *Server) nowMs() uint32 { return uint32(time.Since(s.startClock).Milliseconds()) }

func (s *Server) tickLoop() {
	defer close(s.stopped)
	ticker := time.NewTicker(s.opts.Interval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			s.tick()
		case <-s.stopping:
			return
		}
	}
}

func (s *Server) tick() {
	nowNs := s.nowNs()
	nowMs := s.nowMs()
	var idleCutoff int64 = -1
	if s.opts.IdleTimeout > 0 {
		idleCutoff = nowNs - int64(s.opts.IdleTimeout)
	}

	var expired []*Conn
	for i := range s.shards {
		sh := &s.shards[i]
		sh.mu.Lock()
		sh.batch = sh.batch[:0]
		for _, c := range sh.sessions {
			if idleCutoff >= 0 && c.lastActive.Load() < idleCutoff {
				expired = append(expired, c)
				continue
			}
			sh.batch = append(sh.batch, c)
		}
		active := sh.batch
		sh.mu.Unlock()

		for _, c := range active {
			c.update(nowMs)
		}
	}

	for _, c := range expired {
		func() {
			defer recoverCallbackPanic()
			c.closeWith(os.ErrDeadlineExceeded)
		}()
	}
}

func (s *Server) closeAllSessions() {
	var remaining []*Conn
	for i := range s.shards {
		sh := &s.shards[i]
		sh.mu.Lock()
		for _, c := range sh.sessions {
			remaining = append(remaining, c)
		}
		sh.mu.Unlock()
	}
	for _, c := range remaining {
		func() {
			defer recoverCallbackPanic()
			c.closeWith(fnet.ErrServerClosed)
		}()
	}
}

func recoverCallbackPanic() {
	if r := recover(); r != nil {
		slog.Error("kcp: panic in callback", "panic", r, "stack", string(debug.Stack()))
	}
}

func shardIndex(addr netip.AddrPort) uint64 {
	raw := addr.Addr().As16()
	v := binary.LittleEndian.Uint64(raw[8:]) ^ (uint64(addr.Port()) * 0x9e3779b97f4a7c15)
	v ^= v >> 33
	return v & (numSessionShards - 1)
}

func (s *Server) OnPacket(udpConn fnet.UDPConn, data []byte, from netip.AddrPort) {
	if len(data) < headerSize || s.closed.Load() {
		return
	}
	conv := binary.LittleEndian.Uint32(data[0:4])
	cmd := data[4]
	if cmd != cmdPush && cmd != cmdAck && cmd != cmdWindowProbeAsk && cmd != cmdWindowProbeTell {
		return
	}

	nowNs := s.nowNs()
	nowMs := s.nowMs()
	sh := &s.shards[shardIndex(from)]

	sh.mu.Lock()
	conn, exists := sh.sessions[from]
	if exists && conn.conversationID != conv {
		// Allow a new session from the same peer port only on an initial segment (sn == 0); ignore stray packets.
		if cmd != cmdPush || binary.LittleEndian.Uint32(data[sequenceOffset:sequenceOffset+4]) != 0 {
			sh.mu.Unlock()
			return
		}
		delete(sh.sessions, from)
		s.sessionCount.Add(-1)
		sh.mu.Unlock()
		conn.closeWith(io.EOF)
		sh.mu.Lock()
		exists = false
	}
	if !exists {
		if !s.reserveSessionSlot() {
			sh.mu.Unlock()
			return
		}
		conn = newConn(s, sh, udpConn, from, conv, nowNs)
		sh.sessions[from] = conn
	}
	sh.mu.Unlock()

	conn.handleInbound(data, nowNs, nowMs)
}

func (s *Server) reserveSessionSlot() bool {
	limit := int64(s.opts.MaxSessions)
	for {
		cur := s.sessionCount.Load()
		if cur >= limit {
			return false
		}
		if s.sessionCount.CompareAndSwap(cur, cur+1) {
			return true
		}
	}
}
