//go:build linux || darwin

package fnet

import (
	"fmt"
	"testing"
	"unsafe"

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

// TestConnSize checks that conn stays within the 176B size class, which matters at large connection counts:
// if adding, removing or reordering fields pushes it over, the fields have to be rearranged.
func TestConnSize(t *testing.T) {
	if size := unsafe.Sizeof(conn{}); unsafe.Sizeof(uintptr(0)) == 8 && size > 176 {
		t.Fatalf("conn is %dB, over the 176B size class", size)
	}
}
