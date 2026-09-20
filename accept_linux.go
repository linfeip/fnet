//go:build linux

package fnet

import (
	"golang.org/x/sys/unix"
)

func sysAccept(fd int) (int, unix.Sockaddr, error) {
	return unix.Accept4(fd, unix.SOCK_NONBLOCK|unix.SOCK_CLOEXEC)
}
