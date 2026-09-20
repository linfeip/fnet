//go:build unix && !linux

package fnet

import (
	"golang.org/x/sys/unix"
)

func sysAccept(fd int) (int, unix.Sockaddr, error) {
	nfd, sa, err := unix.Accept(fd)
	if err != nil {
		return -1, nil, err
	}
	if err := setNonblock(nfd); err != nil {
		_ = unix.Close(nfd)
		return -1, nil, err
	}
	return nfd, sa, nil
}
