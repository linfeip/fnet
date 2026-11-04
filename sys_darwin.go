//go:build darwin

package fnet

import (
	"net"
	"net/netip"
	"syscall"

	"golang.org/x/sys/unix"
)

// accept accepts a new connection and returns its peer address; the returned fd is already set to non-blocking and
// close-on-exec.
// darwin has no accept4: as in the standard library, CLOEXEC is set under the protection of ForkLock to keep the
// fd from leaking into a child process of a concurrent exec.
func accept(fd int) (int, netip.AddrPort, error) {
	syscall.ForkLock.RLock()
	nfd, sa, err := unix.Accept(fd)
	if err == nil {
		unix.CloseOnExec(nfd)
	}
	syscall.ForkLock.RUnlock()
	if err != nil {
		return -1, netip.AddrPort{}, err
	}
	if err := unix.SetNonblock(nfd, true); err != nil {
		unix.Close(nfd)
		return -1, netip.AddrPort{}, err
	}
	return nfd, sockaddrToAddrPort(sa), nil
}

// reusePortSpreads: macOS's SO_REUSEPORT does not spread connections over the sockets sharing an address, so the loops
// always share one listener per address (see Server.listenerPerLoop).
const reusePortSpreads = false

// setListenerOptions does nothing: the connections set their options themselves (see setConnOptions).
func setListenerOptions(int, bool) {}

// setConnOptions sets TCP_NODELAY (with noDelay) and keepalive on an accepted connection.
func setConnOptions(fd int, noDelay bool) {
	if noDelay {
		unix.SetsockoptInt(fd, unix.IPPROTO_TCP, unix.TCP_NODELAY, 1)
	}
	setKeepAlive(fd)
}

// setKeepAlive enables TCP keepalive with the same parameters as the standard library net defaults: probing starts
// after 15s of idle time, at a 15s interval, and the connection is considered down after 9 unanswered probes.
// darwin's idle-time option is TCP_KEEPALIVE (TCP_KEEPIDLE on Linux).
func setKeepAlive(fd int) {
	unix.SetsockoptInt(fd, unix.SOL_SOCKET, unix.SO_KEEPALIVE, 1)
	unix.SetsockoptInt(fd, unix.IPPROTO_TCP, unix.TCP_KEEPALIVE, 15)
	unix.SetsockoptInt(fd, unix.IPPROTO_TCP, unix.TCP_KEEPINTVL, 15)
	unix.SetsockoptInt(fd, unix.IPPROTO_TCP, unix.TCP_KEEPCNT, 9)
}

// darwin's system calls go through libc, so the unix wrappers are kept.

func sysRead(fd int, b []byte) (int, error)      { return unix.Read(fd, b) }
func sysWrite(fd int, b []byte) (int, error)     { return unix.Write(fd, b) }
func sysWritev(fd int, bs [][]byte) (int, error) { return unix.Writev(fd, bs) }

func recvDatagram(fd int, b []byte) (int, netip.AddrPort, error) {
	for {
		n, sa, err := unix.Recvfrom(fd, b, 0)
		if err == unix.EINTR {
			continue
		}
		if err != nil {
			return n, netip.AddrPort{}, err
		}
		return n, sockaddrToAddrPort(sa), nil
	}
}

func sendDatagram(fd int, b []byte, addr netip.AddrPort, ipv4 bool) error {
	sa, err := addrPortToSockaddr(addr, ipv4)
	if err != nil {
		return err
	}
	for {
		err := unix.Sendto(fd, b, unix.MSG_NOSIGNAL, sa)
		if err != unix.EINTR {
			return err
		}
	}
}

func addrPortToSockaddr(addr netip.AddrPort, ipv4 bool) (unix.Sockaddr, error) {
	if !addr.IsValid() {
		return nil, &net.AddrError{Err: "invalid address", Addr: addr.String()}
	}
	if ipv4 {
		ip := addr.Addr().Unmap()
		if !ip.Is4() {
			return nil, &net.AddrError{Err: "non-IPv4 address", Addr: ip.String()}
		}
		return &unix.SockaddrInet4{Port: int(addr.Port()), Addr: ip.As4()}, nil
	}
	return &unix.SockaddrInet6{
		Port:   int(addr.Port()),
		Addr:   addr.Addr().As16(),
		ZoneId: zoneToUint32(addr.Addr().Zone()),
	}, nil
}
