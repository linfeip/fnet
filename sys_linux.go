//go:build linux

package fnet

import (
	"net"
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

// reusePortSpreads: the kernel spreads the incoming connections over the sockets sharing an address through
// SO_REUSEPORT, so every loop can listen with a socket of its own (see Server.listenerPerLoop).
const reusePortSpreads = true

// setListenerOptions sets TCP_NODELAY (with noDelay) and keepalive on a listening socket before it is bound: Linux
// copies them to the connections accepted from it, which need no setsockopt of their own (see setConnOptions).
func setListenerOptions(fd int, noDelay bool) {
	if noDelay {
		unix.SetsockoptInt(fd, unix.IPPROTO_TCP, unix.TCP_NODELAY, 1)
	}
	setKeepAlive(fd)
}

// setConnOptions sets the options of an accepted connection: none, it inherits them from its listener.
func setConnOptions(int, bool) {}

// setKeepAlive enables TCP keepalive with the same parameters as the standard library net defaults: probing starts
// after 15s of idle time, at a 15s interval, and the connection is considered down after 9 unanswered probes.
func setKeepAlive(fd int) {
	unix.SetsockoptInt(fd, unix.SOL_SOCKET, unix.SO_KEEPALIVE, 1)
	unix.SetsockoptInt(fd, unix.IPPROTO_TCP, unix.TCP_KEEPIDLE, 15)
	unix.SetsockoptInt(fd, unix.IPPROTO_TCP, unix.TCP_KEEPINTVL, 15)
	unix.SetsockoptInt(fd, unix.IPPROTO_TCP, unix.TCP_KEEPCNT, 9)
}

// The sockets of connections are all non-blocking, so reads and writes never block and RawSyscall is used
// directly: it saves the entersyscall/exitsyscall that Syscall performs before and after every call to hand the
// P over to the scheduler.
//
// They go through the socket calls (recvfrom, sendto, sendmsg) rather than read, write and writev: those take the
// file-level path first - the position lock, rw_verify_area and the file permission hooks of the security modules -
// which a socket has no use for, and it is measurable at one read and one write per message.
// MSG_NOSIGNAL turns a write to a connection the peer has reset into EPIPE without raising SIGPIPE.
// The return values are the same as unix.Read, unix.Write and unix.Writev (n is -1 on error).

func sysRead(fd int, b []byte) (int, error) {
	return rawIO(unix.SYS_RECVFROM, fd, unsafe.Pointer(unsafe.SliceData(b)), len(b), 0)
}

func sysWrite(fd int, b []byte) (int, error) {
	return rawIO(unix.SYS_SENDTO, fd, unsafe.Pointer(unsafe.SliceData(b)), len(b), unix.MSG_NOSIGNAL)
}

// sysWritev, like unix.Writev, allocates the iovec array on the stack when there are no more than 8 segments.
func sysWritev(fd int, bs [][]byte) (int, error) {
	iovecs := make([]unix.Iovec, 0, 8)
	for _, b := range bs {
		if len(b) == 0 {
			continue
		}
		v := unix.Iovec{Base: unsafe.SliceData(b)}
		v.SetLen(len(b))
		iovecs = append(iovecs, v)
	}
	msg := unix.Msghdr{Iov: unsafe.SliceData(iovecs)}
	msg.SetIovlen(len(iovecs))
	return rawIO(unix.SYS_SENDMSG, fd, unsafe.Pointer(&msg), unix.MSG_NOSIGNAL, 0)
}

// rawIO issues a socket call whose remaining arguments (recvfrom's and sendto's address) are all zero.
func rawIO(trap uintptr, fd int, p unsafe.Pointer, n, flags int) (int, error) {
	r, _, errno := syscall.RawSyscall6(trap, uintptr(fd), uintptr(p), uintptr(n), uintptr(flags), 0, 0)
	if errno != 0 {
		return int(r), errno
	}
	return int(r), nil
}

// recvDatagram receives one datagram into b and decodes the sender without allocating.
func recvDatagram(fd int, b []byte) (int, netip.AddrPort, error) {
	var rsa unix.RawSockaddrAny
	for {
		size := uint32(unix.SizeofSockaddrAny)
		r, _, errno := syscall.RawSyscall6(unix.SYS_RECVFROM,
			uintptr(fd), uintptr(unsafe.Pointer(unsafe.SliceData(b))), uintptr(len(b)), 0,
			uintptr(unsafe.Pointer(&rsa)), uintptr(unsafe.Pointer(&size)))
		if errno == unix.EINTR {
			continue
		}
		if errno != 0 {
			return int(r), netip.AddrPort{}, errno
		}
		switch rsa.Addr.Family {
		case unix.AF_INET:
			sa := (*unix.RawSockaddrInet4)(unsafe.Pointer(&rsa))
			return int(r), netip.AddrPortFrom(netip.AddrFrom4(sa.Addr), networkPort(sa.Port)), nil
		case unix.AF_INET6:
			sa := (*unix.RawSockaddrInet6)(unsafe.Pointer(&rsa))
			return int(r), inet6AddrPort(sa.Addr, networkPort(sa.Port), sa.Scope_id), nil
		}
		return int(r), netip.AddrPort{}, nil
	}
}

// sendDatagram sends b as one datagram to addr; ipv4 says the socket family is AF_INET.
func sendDatagram(fd int, b []byte, addr netip.AddrPort, ipv4 bool) error {
	if !addr.IsValid() {
		return &net.AddrError{Err: "invalid address", Addr: addr.String()}
	}
	var (
		ptr  unsafe.Pointer
		size uintptr
	)
	if ipv4 {
		ip := addr.Addr().Unmap()
		if !ip.Is4() {
			return &net.AddrError{Err: "non-IPv4 address", Addr: ip.String()}
		}
		var sa unix.RawSockaddrInet4
		sa.Family = unix.AF_INET
		sa.Port = networkPort(addr.Port())
		sa.Addr = ip.As4()
		ptr, size = unsafe.Pointer(&sa), unix.SizeofSockaddrInet4
	} else {
		var sa unix.RawSockaddrInet6
		sa.Family = unix.AF_INET6
		sa.Port = networkPort(addr.Port())
		sa.Addr = addr.Addr().As16() // IPv4 becomes v4-mapped, as the standard library does
		sa.Scope_id = zoneToUint32(addr.Addr().Zone())
		ptr, size = unsafe.Pointer(&sa), unix.SizeofSockaddrInet6
	}
	for {
		_, _, errno := syscall.RawSyscall6(unix.SYS_SENDTO,
			uintptr(fd), uintptr(unsafe.Pointer(unsafe.SliceData(b))), uintptr(len(b)),
			unix.MSG_NOSIGNAL, uintptr(ptr), size)
		if errno == unix.EINTR {
			continue
		}
		if errno != 0 {
			return errno
		}
		return nil
	}
}
