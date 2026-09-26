//go:build linux

package netpoll

import (
	"net/netip"
	"unsafe"

	"golang.org/x/sys/unix"
)

// sysAccept calls accept4 itself rather than through unix.Accept4, whose
// Sockaddr costs an allocation per accept and, for IPv4, a getsockopt on the
// listener (to tell L2TP sockets apart): a second system call on the path
// that bounds how fast a burst of clients is let in. The listener is
// non-blocking, so it is a raw call (see io_linux.go).
func sysAccept(lnFD int) (int, netip.AddrPort, error) {
	var rsa unix.RawSockaddrAny
	n := uint32(unix.SizeofSockaddrAny)
	fd, _, errno := unix.RawSyscall6(unix.SYS_ACCEPT4, uintptr(lnFD), uintptr(unsafe.Pointer(&rsa)),
		uintptr(unsafe.Pointer(&n)), unix.SOCK_NONBLOCK|unix.SOCK_CLOEXEC, 0, 0)
	if errno != 0 {
		return -1, netip.AddrPort{}, errno
	}
	return int(fd), rawAddrPort(&rsa), nil
}

func rawAddrPort(rsa *unix.RawSockaddrAny) netip.AddrPort {
	switch rsa.Addr.Family {
	case unix.AF_INET:
		sa := (*unix.RawSockaddrInet4)(unsafe.Pointer(rsa))
		return netip.AddrPortFrom(netip.AddrFrom4(sa.Addr), ntohs(sa.Port))
	case unix.AF_INET6:
		sa := (*unix.RawSockaddrInet6)(unsafe.Pointer(rsa))
		ip := netip.AddrFrom16(sa.Addr).Unmap()
		if sa.Scope_id != 0 {
			ip = ip.WithZone(zoneName(sa.Scope_id))
		}
		return netip.AddrPortFrom(ip, ntohs(sa.Port))
	}
	return netip.AddrPort{}
}

// ntohs reads a port stored in network byte order.
func ntohs(port uint16) uint16 {
	b := (*[2]byte)(unsafe.Pointer(&port))
	return uint16(b[0])<<8 | uint16(b[1])
}
