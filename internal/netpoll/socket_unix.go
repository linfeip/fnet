//go:build unix

package netpoll

import (
	"fmt"
	"net"
	"net/netip"
	"os"
	"strconv"
	"time"

	"golang.org/x/sys/unix"
)

// Listen opens a non-blocking TCP listener and returns its fd. Like
// net.Listen it sets SO_REUSEADDR, but not SO_REUSEPORT: a second server on
// the same port fails instead of silently taking half the connections.
func Listen(network, address string) (int, net.Addr, error) {
	ln, err := net.Listen(network, address)
	if err != nil {
		return -1, nil, err
	}
	return FromListener(ln)
}

// FromListener takes over ln: it returns a non-blocking duplicate of the
// listening socket and closes ln.
func FromListener(ln net.Listener) (int, net.Addr, error) {
	defer ln.Close()
	fl, ok := ln.(interface{ File() (*os.File, error) })
	if !ok {
		return -1, nil, fmt.Errorf("fnet: listener %T does not expose its file descriptor", ln)
	}
	file, err := fl.File()
	if err != nil {
		return -1, nil, err
	}
	fd, err := unix.Dup(int(file.Fd()))
	_ = file.Close()
	if err != nil {
		return -1, nil, err
	}
	unix.CloseOnExec(fd)
	if err := unix.SetNonblock(fd, true); err != nil {
		_ = unix.Close(fd)
		return -1, nil, err
	}
	if noDelayInherited {
		_ = unix.SetsockoptInt(fd, unix.IPPROTO_TCP, unix.TCP_NODELAY, 1)
	}
	return fd, ln.Addr(), nil
}

// Accept accepts one pending connection as a non-blocking socket with
// TCP_NODELAY set. It returns an IsAgain error once the backlog is empty.
func Accept(lnFD int) (int, netip.AddrPort, error) {
	for {
		fd, addr, err := sysAccept(lnFD)
		if err == nil {
			if !noDelayInherited {
				_ = unix.SetsockoptInt(fd, unix.IPPROTO_TCP, unix.TCP_NODELAY, 1)
			}
			return fd, addr, nil
		}
		if err == unix.EINTR || err == unix.ECONNABORTED {
			continue // interrupted, or the peer gave up while queued
		}
		return -1, netip.AddrPort{}, err
	}
}

func addrPort(sa unix.Sockaddr) netip.AddrPort {
	switch a := sa.(type) {
	case *unix.SockaddrInet4:
		return netip.AddrPortFrom(netip.AddrFrom4(a.Addr), uint16(a.Port))
	case *unix.SockaddrInet6:
		ip := netip.AddrFrom16(a.Addr).Unmap()
		if a.ZoneId != 0 {
			ip = ip.WithZone(zoneName(a.ZoneId))
		}
		return netip.AddrPortFrom(ip, uint16(a.Port))
	}
	return netip.AddrPort{}
}

func zoneName(id uint32) string {
	if ifi, err := net.InterfaceByIndex(int(id)); err == nil {
		return ifi.Name
	}
	return strconv.FormatUint(uint64(id), 10)
}

// Read reads from a non-blocking fd. n == 0 with a nil error means EOF.
func Read(fd int, b []byte) (int, error) {
	for {
		n, err := unix.Read(fd, b)
		if err != unix.EINTR {
			if n < 0 {
				n = 0
			}
			return n, err
		}
	}
}

// Write writes to a non-blocking fd and may return a short count.
func Write(fd int, b []byte) (int, error) {
	for {
		n, err := unix.Write(fd, b)
		if err != unix.EINTR {
			if n < 0 {
				n = 0
			}
			return n, err
		}
	}
}

// Close closes fd. Closing also removes it from any poller.
func Close(fd int) error { return unix.Close(fd) }

// CloseWrite shuts down the sending side of fd: the peer reads EOF once the
// data already written is delivered.
func CloseWrite(fd int) error { return unix.Shutdown(fd, unix.SHUT_WR) }

// LocalAddr returns the local address of the connected socket fd.
func LocalAddr(fd int) (netip.AddrPort, error) {
	sa, err := unix.Getsockname(fd)
	if err != nil {
		return netip.AddrPort{}, err
	}
	return addrPort(sa), nil
}

// SetKeepAlive applies ka to the TCP socket fd.
func SetKeepAlive(fd int, ka KeepAlive) error {
	if ka.Idle < 0 {
		return unix.SetsockoptInt(fd, unix.SOL_SOCKET, unix.SO_KEEPALIVE, 0)
	}
	if err := unix.SetsockoptInt(fd, unix.SOL_SOCKET, unix.SO_KEEPALIVE, 1); err != nil {
		return err
	}
	if ka.Idle > 0 {
		if err := unix.SetsockoptInt(fd, unix.IPPROTO_TCP, tcpKeepIdle, seconds(ka.Idle)); err != nil {
			return err
		}
	}
	if ka.Interval > 0 {
		if err := unix.SetsockoptInt(fd, unix.IPPROTO_TCP, unix.TCP_KEEPINTVL, seconds(ka.Interval)); err != nil {
			return err
		}
	}
	if ka.Count > 0 {
		return unix.SetsockoptInt(fd, unix.IPPROTO_TCP, unix.TCP_KEEPCNT, ka.Count)
	}
	return nil
}

// seconds rounds d up to whole seconds, the unit of the keep-alive options.
func seconds(d time.Duration) int { return max(int((d+time.Second-1)/time.Second), 1) }
