//go:build linux || darwin

package fnet

import (
	"testing"
	"unsafe"

	"golang.org/x/sys/unix"
)

// TestKeepAlive verifies TCP keepalive is enabled on the connections the server accepts.
func TestKeepAlive(t *testing.T) {
	type result struct {
		keepAlive, interval int
		err                 error
	}
	got := make(chan result, 1)
	srv := startServer(t, &funcHandler{open: func(c Conn) {
		fd := c.(*conn).fd
		var r result
		if r.keepAlive, r.err = unix.GetsockoptInt(fd, unix.SOL_SOCKET, unix.SO_KEEPALIVE); r.err == nil {
			r.interval, r.err = unix.GetsockoptInt(fd, unix.IPPROTO_TCP, unix.TCP_KEEPINTVL)
		}
		got <- r
	}})
	dial(t, srv)
	if r := <-got; r.err != nil || r.keepAlive == 0 || r.interval != 15 {
		t.Fatalf("SO_KEEPALIVE=%d TCP_KEEPINTVL=%d err=%v", r.keepAlive, r.interval, r.err)
	}
}

// TestConnSize checks that conn stays within the 176B size class, which matters at large connection counts:
// if adding, removing or reordering fields pushes it over, the fields have to be rearranged.
func TestConnSize(t *testing.T) {
	if size := unsafe.Sizeof(conn{}); unsafe.Sizeof(uintptr(0)) == 8 && size > 176 {
		t.Fatalf("conn 为 %dB，超出 176B 的内存分级", size)
	}
}
