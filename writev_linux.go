//go:build linux

package fnet

import "golang.org/x/sys/unix"

func writevFD(fd int, iovs [][]byte) (int, error) {
	return unix.Writev(fd, iovs)
}
