//go:build linux || darwin

package fnet

import (
	"errors"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/linfeip/fnet/internal/bytepool"

	"golang.org/x/sys/unix"
)

// TestSocketOptions verifies TCP_NODELAY (with Options.NoDelay) and TCP keepalive are set on the connections the
// server accepts by the time OnOpen runs, before anything can be written to them.
func TestSocketOptions(t *testing.T) {
	type result struct {
		noDelay, keepAlive, interval int
		err                          error
	}
	for _, noDelay := range []bool{false, true} {
		t.Run(fmt.Sprintf("NoDelay=%v", noDelay), func(t *testing.T) {
			got := make(chan result, 1)
			srv := startServerWith(t, &funcHandler{open: func(c Conn) {
				fd := c.(*conn).fd
				var r result
				if r.noDelay, r.err = unix.GetsockoptInt(fd, unix.IPPROTO_TCP, unix.TCP_NODELAY); r.err == nil {
					if r.keepAlive, r.err = unix.GetsockoptInt(fd, unix.SOL_SOCKET, unix.SO_KEEPALIVE); r.err == nil {
						r.interval, r.err = unix.GetsockoptInt(fd, unix.IPPROTO_TCP, unix.TCP_KEEPINTVL)
					}
				}
				got <- r
			}}, Options{NoDelay: noDelay})
			dial(t, srv)
			if r := <-got; r.err != nil || (r.noDelay != 0) != noDelay || r.keepAlive == 0 || r.interval != 15 {
				t.Fatalf("TCP_NODELAY=%d SO_KEEPALIVE=%d TCP_KEEPINTVL=%d err=%v", r.noDelay, r.keepAlive, r.interval, r.err)
			}
		})
	}
}

// TestInet6AddrPort checks that a scope id becomes the zone of the address by its number, which dials back like a name.
func TestInet6AddrPort(t *testing.T) {
	addr := [16]byte{0: 0xfe, 1: 0x80, 15: 1}
	for _, tt := range []struct {
		scopeID uint32
		want    string
	}{{0, "[fe80::1]:80"}, {7, "[fe80::1%7]:80"}} {
		if got := inet6AddrPort(addr, 80, tt.scopeID).String(); got != tt.want {
			t.Errorf("scope id %d: got %s, want %s", tt.scopeID, got, tt.want)
		}
	}
}

// TestCompactOut checks when the send buffer drops the part already sent to make room: only when the room after the data
// is short and the part is at least as large as what is left to send, so that it never moves more than it frees.
func TestCompactOut(t *testing.T) {
	for _, tt := range []struct {
		name          string
		sent, live    int // the part already sent, and what is left to send, of a full 4KB buffer
		size          int // what is to be appended
		wantCompacted bool
	}{
		{"room left", 0, 1 << 10, 512, false},
		{"sent part as large as the rest", 2 << 10, 2 << 10, 512, true},
		{"sent part smaller than the rest", 512, 3584, 512, false},
	} {
		var c conn
		c.out = bytepool.Get(4 << 10)
		c.out.Write(make([]byte, tt.sent+tt.live))
		c.outPos = tt.sent
		c.compactOut(tt.size)
		if compacted := c.outPos == 0 && tt.sent > 0; compacted != tt.wantCompacted || c.out.Len()-c.outPos != tt.live {
			t.Errorf("%s: compacted=%v with %d bytes left to send, want compacted=%v and %d", tt.name, compacted,
				c.out.Len()-c.outPos, tt.wantCompacted, tt.live)
		}
		c.out.Release()
	}
}

// TestBacklogStopsReading checks that a connection is not read while more than maxBacklog waits in its send buffer: the
// replies to a peer that does not read stay bounded, and the peer's writes block instead.
func TestBacklogStopsReading(t *testing.T) {
	opened := make(chan *conn, 1)
	srv := startServer(t, &funcHandler{
		open: func(c Conn) { opened <- c.(*conn) },
		data: func(c Conn, b []byte) int {
			for range 4 { // replies with four times what it reads
				c.Write(b)
			}
			return len(b)
		},
	})
	c := dial(t, srv)
	sc := <-opened
	c.SetWriteDeadline(time.Now().Add(time.Second))
	if n, err := c.Write(make([]byte, 64<<20)); !errors.Is(err, os.ErrDeadlineExceeded) {
		t.Fatalf("peer write should block, wrote %d bytes, err=%v", n, err)
	}
	if backlog, limit := sc.backlog(), maxBacklog+4*sc.loop.srv.opts.ReadBufferSize; backlog > limit {
		t.Fatalf("send buffer holds %d bytes, want no more than %d", backlog, limit)
	}
}
