package kcp

import (
	"errors"
	"net"
	"net/netip"
	"sync"
	"sync/atomic"

	"github.com/linfeip/fnet"
)

// ErrDeadLink is the OnClose reason when a peer stops acknowledging retransmissions after 20 attempts.
var ErrDeadLink = errors.New("kcp: dead link")

const (
	cbRead      uint32 = 1 << 0
	cbClose     uint32 = 1 << 1
	cbScheduled uint32 = 1 << 31
)

// Conn represents an active KCP session over a Server's UDP socket.
//
// Except for SetContext (which should be called in OnOpen or OnData), all methods may be called concurrently from any
// goroutine. The callbacks of one Conn always run serially: OnOpen first, OnData for each received message/chunk, and
// OnClose last and only once.
type Conn struct {
	srv            *Server
	shard          *sessionShard
	udpConn        fnet.UDPConn
	remoteAddr     netip.AddrPort
	conversationID uint32

	state      atomic.Uint32
	lastActive atomic.Int64
	closed     atomic.Bool
	opened     bool // accessed only inside runCallbacks
	ctx        any

	mu       sync.Mutex
	arq      arqState
	closeErr error
}

func newConn(srv *Server, shard *sessionShard, udpConn fnet.UDPConn, remoteAddr netip.AddrPort, conversationID uint32, nowNs int64) *Conn {
	c := &Conn{
		srv:            srv,
		shard:          shard,
		udpConn:        udpConn,
		remoteAddr:     remoteAddr,
		conversationID: conversationID,
	}
	c.lastActive.Store(nowNs)
	c.arq.init(arqConfig{
		conversationID: conversationID,
		mtu:            srv.opts.MTU,
		sendWindow:     uint32(srv.opts.SendWindow),
		receiveWindow:  uint32(srv.opts.ReceiveWindow),
		interval:       uint32(srv.opts.Interval.Milliseconds()),
		noDelay:        srv.opts.NoDelay,
		streamMode:     srv.opts.StreamMode,
	}, func(pkt []byte) {
		_, _ = c.udpConn.WriteToUDPAddrPort(pkt, c.remoteAddr)
	})
	return c
}

// ConversationID returns the 32-bit KCP conversation identifier of the session.
func (c *Conn) ConversationID() uint32 { return c.conversationID }

// LocalAddr returns the server UDP address this session communicates through.
func (c *Conn) LocalAddr() net.Addr { return c.udpConn.LocalAddr() }

// RemoteAddr returns the peer's UDP address as a *net.UDPAddr. For a zero-allocation value address, use RemoteAddrPort.
func (c *Conn) RemoteAddr() net.Addr { return net.UDPAddrFromAddrPort(c.remoteAddr) }

// RemoteAddrPort returns the peer's UDP address and port without allocating.
func (c *Conn) RemoteAddrPort() netip.AddrPort { return c.remoteAddr }

// Context returns the user value bound to the session.
func (c *Conn) Context() any { return c.ctx }

// SetContext binds user data to the session; call it in OnOpen or OnData.
func (c *Conn) SetContext(ctx any) { c.ctx = ctx }

// Write queues b into the KCP send window and flushes immediately over UDP without blocking.
// In message mode (Options.StreamMode == false) b must fit within 255 segments (ErrMessageTooLarge otherwise); in
// stream mode arbitrarily large slices are split across segments. The caller may reuse b once Write returns.
func (c *Conn) Write(b []byte) (int, error) {
	c.mu.Lock()
	if c.closed.Load() {
		c.mu.Unlock()
		return 0, net.ErrClosed
	}
	if len(b) == 0 && c.arq.streamMode {
		c.mu.Unlock()
		return 0, nil
	}
	if err := c.arq.send(b); err != nil {
		c.mu.Unlock()
		return 0, err
	}
	c.arq.flush(c.srv.nowMs(), false)
	dead := c.arq.dead
	c.mu.Unlock()
	if dead {
		c.closeWith(ErrDeadLink)
	}
	return len(b), nil
}

// Close flushes any segments already queued once, removes the session from the server, and schedules OnClose(c, nil).
// Subsequent writes return net.ErrClosed.
func (c *Conn) Close() error {
	c.closeWith(nil)
	return nil
}

func (c *Conn) closeWith(err error) {
	if !c.closed.CompareAndSwap(false, true) {
		return
	}
	c.shard.remove(c.remoteAddr, c)

	c.mu.Lock()
	c.closeErr = err
	if err == nil {
		c.arq.flush(c.srv.nowMs(), false)
	}
	c.arq.release()
	c.mu.Unlock()

	c.notify(cbClose)
}

func (c *Conn) handleInbound(data []byte, nowNs int64, nowMs uint32) {
	c.lastActive.Store(nowNs)
	c.mu.Lock()
	if c.closed.Load() {
		c.mu.Unlock()
		return
	}
	if c.arq.input(data, nowMs) {
		c.arq.flush(nowMs, false)
	}
	dead := c.arq.dead
	c.mu.Unlock()

	if dead {
		c.closeWith(ErrDeadLink)
		return
	}
	c.notify(cbRead)
}

func (c *Conn) update(nowMs uint32) {
	c.mu.Lock()
	if c.closed.Load() {
		c.mu.Unlock()
		return
	}
	c.arq.update(nowMs)
	dead := c.arq.dead
	c.mu.Unlock()
	if dead {
		c.closeWith(ErrDeadLink)
	}
}

func (c *Conn) notify(ev uint32) {
	if c.state.Or(ev|cbScheduled)&cbScheduled == 0 {
		c.srv.callbacksWg.Add(1)
		c.runCallbacks()
	}
}

func (c *Conn) runCallbacks() {
	defer c.srv.callbacksWg.Done()
	for {
		ev := c.state.Swap(cbScheduled) &^ cbScheduled
		if !c.opened && ev&cbClose == 0 {
			c.opened = true
			c.srv.handler.OnOpen(c)
		}
		if ev&cbRead != 0 && !c.closed.Load() {
			c.drainMessages()
		}
		if ev&cbClose != 0 || c.closed.Load() {
			if c.opened {
				c.mu.Lock()
				err := c.closeErr
				c.mu.Unlock()
				c.srv.handler.OnClose(c, err)
			}
			return // keep cbScheduled set permanently so no further callback runs after OnClose
		}
		if c.state.CompareAndSwap(cbScheduled, 0) {
			return
		}
	}
}

func (c *Conn) drainMessages() {
	delivered := false
	for !c.closed.Load() {
		c.mu.Lock()
		msg, ok := c.arq.popMessage()
		c.mu.Unlock()
		if !ok {
			break
		}
		delivered = true
		c.srv.handler.OnData(c, msg.Bytes())
		msg.Release()
	}

	// Flush pending ACKs (if OnData did not already piggyback them onto a Write) so the peer's send window advances
	// immediately instead of waiting for the next periodic tick.
	c.mu.Lock()
	if !c.closed.Load() && len(c.arq.pendingAcks) > 0 {
		c.arq.flush(c.srv.nowMs(), !delivered && len(c.arq.sendQueue) == 0)
	}
	c.mu.Unlock()
}
