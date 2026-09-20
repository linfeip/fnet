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

func acceptFD(fd int) (int, net.Addr, error) {
	nfd, sa, err := unix.Accept(fd)
	if err != nil {
		return -1, nil, err
	}
	if err := setNonblock(nfd); err != nil {
		_ = unix.Close(nfd)
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
