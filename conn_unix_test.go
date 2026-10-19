//go:build linux || darwin

package fnet

import (
	"errors"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/linfeip/fnet/internal/units"

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
	if n, err := c.Write(make([]byte, 64*units.MB)); !errors.Is(err, os.ErrDeadlineExceeded) {
		t.Fatalf("peer write should block, wrote %d bytes, err=%v", n, err)
	}
	if backlog, limit := sc.backlog(), maxBacklog+4*sc.loop.srv.opts.ReadBufferSize; backlog > limit {
		t.Fatalf("send buffer holds %d bytes, want no more than %d", backlog, limit)
	}
}
