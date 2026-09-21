//go:build unix

package fnet

import (
	"context"
	"fmt"
	"net"
	"os"
	"syscall"

	"golang.org/x/sys/unix"
)

func setNonblock(fd int) error {
	return unix.SetNonblock(fd, true)
}

func setReuseAddr(fd int) error {
	return unix.SetsockoptInt(fd, unix.SOL_SOCKET, unix.SO_REUSEADDR, 1)
}

func closeFD(fd int) error {
	return unix.Close(fd)
}

func readFD(fd int, buf []byte) (int, error) {
	n, err := unix.Read(fd, buf)
	if err != nil {
		return n, err
	}
	return n, nil
}

func writeFD(fd int, buf []byte) (int, error) {
	return unix.Write(fd, buf)
}

func writevFD(fd int, iovs [][]byte) (int, error) {
	return unix.Writev(fd, iovs)
}

func isIPv4Mapped(addr [16]byte) bool {
	return addr[0] == 0 && addr[1] == 0 && addr[2] == 0 && addr[3] == 0 &&
		addr[4] == 0 && addr[5] == 0 && addr[6] == 0 && addr[7] == 0 &&
		addr[8] == 0 && addr[9] == 0 && addr[10] == 0xff && addr[11] == 0xff
}

func acceptConn(lnFD int, laddr net.Addr) (int, *VirtualConn, error) {
	nfd, sa, err := sysAccept(lnFD)
	if err != nil {
		return -1, nil, err
	}
	_ = unix.SetsockoptInt(nfd, unix.IPPROTO_TCP, unix.TCP_NODELAY, 1)
	vc := NewVirtualConn(laddr, nil)
	if sa4, ok := sa.(*unix.SockaddrInet4); ok {
		copy(vc.raddrIP[:4], sa4.Addr[:])
		vc.raddrPort = uint16(sa4.Port)
		vc.raddrLen = 4
	} else if sa6, ok := sa.(*unix.SockaddrInet6); ok {
		if isIPv4Mapped(sa6.Addr) {
			copy(vc.raddrIP[:4], sa6.Addr[12:16])
			vc.raddrPort = uint16(sa6.Port)
			vc.raddrLen = 4
		} else if sa6.ZoneId == 0 {
			vc.raddrIP = sa6.Addr
			vc.raddrPort = uint16(sa6.Port)
			vc.raddrLen = 16
		} else {
			vc.remote = sockaddrToAddr(sa)
		}
	} else if sa != nil {
		vc.remote = sockaddrToAddr(sa)
	}
	return nfd, vc, nil
}

func acceptFD(fd int) (int, net.Addr, error) {
	nfd, sa, err := sysAccept(fd)
	if err != nil {
		return -1, nil, err
	}
	_ = unix.SetsockoptInt(nfd, unix.IPPROTO_TCP, unix.TCP_NODELAY, 1)
	return nfd, sockaddrToAddr(sa), nil
}

func sockaddrToAddr(sa unix.Sockaddr) net.Addr {
	switch a := sa.(type) {
	case *unix.SockaddrInet4:
		return &net.TCPAddr{IP: net.IP(a.Addr[:]).To4(), Port: a.Port}
	case *unix.SockaddrInet6:
		return &net.TCPAddr{IP: net.IP(a.Addr[:]), Port: a.Port, Zone: zoneToString(a.ZoneId)}
	default:
		return &net.TCPAddr{}
	}
}

func zoneToString(id uint32) string {
	if id == 0 {
		return ""
	}
	ifi, err := net.InterfaceByIndex(int(id))
	if err != nil {
		return fmt.Sprintf("%d", id)
	}
	return ifi.Name
}

type fileListener interface {
	File() (*os.File, error)
}

// dupListenerFD duplicates the OS file descriptor from a TCP listener and
// puts it into non-blocking mode for use with the poller.
func dupListenerFD(ln net.Listener) (int, error) {
	fl, ok := ln.(fileListener)
	if !ok {
		return -1, fmt.Errorf("listener does not implement File(): %T", ln)
	}
	file, err := fl.File()
	if err != nil {
		return -1, err
	}
	fd := int(file.Fd())
	dup, err := unix.Dup(fd)
	_ = file.Close()
	if err != nil {
		return -1, err
	}
	if err := setNonblock(dup); err != nil {
		_ = unix.Close(dup)
		return -1, err
	}
	_ = setReuseAddr(dup)
	return dup, nil
}

// listenNonblock creates a non-blocking TCP listener with SO_REUSEADDR and
// SO_REUSEPORT configured before bind, and returns its duplicated non-blocking FD.
func listenNonblock(network, address string) (int, net.Addr, error) {
	lc := net.ListenConfig{
		Control: func(network, address string, c syscall.RawConn) error {
			var ctrlErr error
			err := c.Control(func(fd uintptr) {
				_ = unix.SetsockoptInt(int(fd), unix.SOL_SOCKET, unix.SO_REUSEADDR, 1)
				_ = unix.SetsockoptInt(int(fd), unix.SOL_SOCKET, unix.SO_REUSEPORT, 1)
			})
			if err != nil {
				return err
			}
			return ctrlErr
		},
	}
	ln, err := lc.Listen(context.Background(), network, address)
	if err != nil {
		return -1, nil, err
	}
	addr := ln.Addr()
	fd, err := dupListenerFD(ln)
	_ = ln.Close()
	if err != nil {
		return -1, nil, err
	}
	return fd, addr, nil
}
