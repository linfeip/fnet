//go:build unix

package fnet

import (
	"fmt"
	"net"

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

// dupListenerFD duplicates the OS file descriptor from a TCP listener and
// puts it into non-blocking mode for use with the poller.
func dupListenerFD(ln net.Listener) (int, error) {
	tcpln, ok := ln.(*net.TCPListener)
	if !ok {
		return -1, fmt.Errorf("listener is not *net.TCPListener")
	}
	file, err := tcpln.File()
	if err != nil {
		return -1, err
	}
	fd := int(file.Fd())
	// File() duplicates; keep our own FD and close the *os.File wrapper's
	// reference carefully: we Dup again so closing file doesn't close ours.
	dup, err := unix.Dup(fd)
	if err != nil {
		_ = file.Close()
		return -1, err
	}
	_ = file.Close()
	if err := setNonblock(dup); err != nil {
		_ = unix.Close(dup)
		return -1, err
	}
	_ = setReuseAddr(dup)
	return dup, nil
}

// listenNonblock creates a non-blocking TCP listener and returns its FD.
func listenNonblock(network, address string) (int, net.Addr, error) {
	ln, err := net.Listen(network, address)
	if err != nil {
		return -1, nil, err
	}
	addr := ln.Addr()
	fd, err := dupListenerFD(ln)
	if err != nil {
		_ = ln.Close()
		return -1, nil, err
	}
	// Close the Go listener; we own the duplicated FD.
	_ = ln.Close()
	return fd, addr, nil
}
