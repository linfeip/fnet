//go:build linux

package fnet

import (
	"net/netip"
	"syscall"
	"unsafe"

	"golang.org/x/sys/unix"
)

// accept accepts a new connection and returns its peer address; the returned fd is already set to non-blocking and
// close-on-exec.
//
// It calls accept4 itself rather than through unix.Accept4, which decodes an IP peer address only after asking for
// the socket's protocol with a getsockopt: one more syscall per connection on the accept path. The
// listener is non-blocking, so the call never blocks and is a raw one, like the connection reads and writes (see rawIO).
func accept(fd int) (int, netip.AddrPort, error) {
	var rsa unix.RawSockaddrAny
	size := uint32(unix.SizeofSockaddrAny)
	nfd, _, errno := syscall.RawSyscall6(unix.SYS_ACCEPT4, uintptr(fd), uintptr(unsafe.Pointer(&rsa)),
		uintptr(unsafe.Pointer(&size)), unix.SOCK_NONBLOCK|unix.SOCK_CLOEXEC, 0, 0)
	if errno != 0 {
		return -1, netip.AddrPort{}, errno
	}
	switch rsa.Addr.Family {
	case unix.AF_INET:
		sa := (*unix.RawSockaddrInet4)(unsafe.Pointer(&rsa))
		return int(nfd), netip.AddrPortFrom(netip.AddrFrom4(sa.Addr), networkPort(sa.Port)), nil
	case unix.AF_INET6:
		sa := (*unix.RawSockaddrInet6)(unsafe.Pointer(&rsa))
		return int(nfd), inet6AddrPort(sa.Addr, networkPort(sa.Port), sa.Scope_id), nil
	}
	return int(nfd), netip.AddrPort{}, nil
}

// networkPort reads a port kept in network byte order.
func networkPort(port uint16) uint16 {
	b := (*[2]byte)(unsafe.Pointer(&port))
	return uint16(b[0])<<8 | uint16(b[1])
}

// listenerPerLoop: every loop listens on each address with a socket of its own, sharing the address through
// SO_REUSEPORT, and serves the connections it accepts. The kernel spreads the incoming connections over the sockets, so
// the loops accept in parallel instead of contending for one listener, and a connection stays with the worker that
// accepted it.
const listenerPerLoop = true

// setListenerOptions sets TCP_NODELAY and keepalive on a listening socket before it is bound: Linux copies them to the
// connections accepted from it, which need no setsockopt of their own (see setConnOptions).
func setListenerOptions(fd int) {
	unix.SetsockoptInt(fd, unix.IPPROTO_TCP, unix.TCP_NODELAY, 1)
	setKeepAlive(fd)
}

// setConnOptions sets the options of an accepted connection: none, it inherits them from its listener.
func setConnOptions(int) {}

// setKeepAlive enables TCP keepalive with the same parameters as the standard library net defaults: probing starts
// after 15s of idle time, at a 15s interval, and the connection is considered down after 9 unanswered probes.
func setKeepAlive(fd int) {
	unix.SetsockoptInt(fd, unix.SOL_SOCKET, unix.SO_KEEPALIVE, 1)
	unix.SetsockoptInt(fd, unix.IPPROTO_TCP, unix.TCP_KEEPIDLE, 15)
	unix.SetsockoptInt(fd, unix.IPPROTO_TCP, unix.TCP_KEEPINTVL, 15)
	unix.SetsockoptInt(fd, unix.IPPROTO_TCP, unix.TCP_KEEPCNT, 9)
}
