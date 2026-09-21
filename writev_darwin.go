//go:build darwin

package fnet

import (
	"syscall"
	"unsafe"
)

func writevFD(fd int, iovs [][]byte) (int, error) {
	if len(iovs) == 0 {
		return 0, nil
	}
	var stackVecs [4]syscall.Iovec
	var vecs []syscall.Iovec
	if len(iovs) <= len(stackVecs) {
		vecs = stackVecs[:len(iovs)]
	} else {
		vecs = make([]syscall.Iovec, len(iovs))
	}
	for i, b := range iovs {
		vecs[i].SetLen(len(b))
		if len(b) > 0 {
			vecs[i].Base = &b[0]
		}
	}
	r1, _, errno := syscall.Syscall(syscall.SYS_WRITEV, uintptr(fd), uintptr(unsafe.Pointer(&vecs[0])), uintptr(len(vecs)))
	if errno != 0 {
		return int(r1), errno
	}
	return int(r1), nil
}
