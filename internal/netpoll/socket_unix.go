//go:build unix

package netpoll

import (
	"context"
	"fmt"
	"net"
	"net/netip"
	"os"
	"strconv"
	"syscall"

	"golang.org/x/sys/unix"
)

// Listen opens a non-blocking TCP listener with SO_REUSEADDR and SO_REUSEPORT
// set before bind and returns its fd.
func Listen(network, address string) (int, net.Addr, error) {
	lc := net.ListenConfig{
		Control: func(_, _ string, c syscall.RawConn) error {
			return c.Control(func(fd uintptr) {
				_ = unix.SetsockoptInt(int(fd), unix.SOL_SOCKET, unix.SO_REUSEADDR, 1)
				_ = unix.SetsockoptInt(int(fd), unix.SOL_SOCKET, unix.SO_REUSEPORT, 1)
			})
		},
	}
	ln, err := lc.Listen(context.Background(), network, address)
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
	return fd, ln.Addr(), nil
}

// Accept accepts one pending connection as a non-blocking socket with
// TCP_NODELAY set. It returns an IsAgain error once the backlog is empty.
func Accept(lnFD int) (int, netip.AddrPort, error) {
	for {
		fd, sa, err := sysAccept(lnFD)
		if err == nil {
			_ = unix.SetsockoptInt(fd, unix.IPPROTO_TCP, unix.TCP_NODELAY, 1)
			return fd, addrPort(sa), nil
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
