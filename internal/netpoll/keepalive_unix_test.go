//go:build linux || darwin

package netpoll

import (
	"net"
	"testing"
	"time"

	"golang.org/x/sys/unix"
)

// acceptOne dials lfd and returns the accepted socket.
func acceptOne(t *testing.T, lfd int, laddr net.Addr) int {
	t.Helper()
	client, err := net.Dial("tcp", laddr.String())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = client.Close() })
	deadline := time.Now().Add(5 * time.Second)
	for {
		fd, _, err := Accept(lfd)
		if err == nil {
			t.Cleanup(func() { _ = Close(fd) })
			return fd
		}
		if !IsAgain(err) || time.Now().After(deadline) {
			t.Fatalf("Accept: %v", err)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// Keep-alive reaches accepted sockets: on Linux by inheritance from the
// listener (no system call per accept), elsewhere by setting it on each.
func TestKeepAliveOnAcceptedSockets(t *testing.T) {
	lfd, laddr, err := Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer Close(lfd)
	ka := KeepAlive{Idle: 42 * time.Second, Interval: 7 * time.Second, Count: 3}
	if KeepAliveInherited {
		if err := SetKeepAlive(lfd, ka); err != nil {
			t.Fatal(err)
		}
	}
	fd := acceptOne(t, lfd, laddr)
	if !KeepAliveInherited {
		if err := SetKeepAlive(fd, ka); err != nil {
			t.Fatal(err)
		}
	}
	check := func(name string, level, opt int, want func(int) bool) {
		t.Helper()
		v, err := unix.GetsockoptInt(fd, level, opt)
		if err != nil || !want(v) {
			t.Errorf("%s = %d (%v)", name, v, err)
		}
	}
	is := func(n int) func(int) bool { return func(v int) bool { return v == n } }
	check("SO_KEEPALIVE", unix.SOL_SOCKET, unix.SO_KEEPALIVE, func(v int) bool { return v != 0 })
	check("idle", unix.IPPROTO_TCP, tcpKeepIdle, is(42))
	check("TCP_KEEPINTVL", unix.IPPROTO_TCP, unix.TCP_KEEPINTVL, is(7))
	check("TCP_KEEPCNT", unix.IPPROTO_TCP, unix.TCP_KEEPCNT, is(3))

	if err := SetKeepAlive(fd, KeepAlive{Idle: -1}); err != nil {
		t.Fatal(err)
	}
	check("SO_KEEPALIVE after off", unix.SOL_SOCKET, unix.SO_KEEPALIVE, is(0))
}

// Accepted sockets have TCP_NODELAY: on Linux by inheritance from the listener
// (no system call per accept), elsewhere by setting it on each.
func TestNoDelayOnAcceptedSockets(t *testing.T) {
	lfd, laddr, err := Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer Close(lfd)
	fd := acceptOne(t, lfd, laddr)
	if v, err := unix.GetsockoptInt(fd, unix.IPPROTO_TCP, unix.TCP_NODELAY); err != nil || v == 0 {
		t.Fatalf("TCP_NODELAY = %d (%v)", v, err)
	}
}
