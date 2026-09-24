//go:build darwin

package netpoll

import (
	"syscall"
	"unsafe"
)

// Writev writes iovs with a single writev(2) and may return a short count.
func Writev(fd int, iovs [][]byte) (int, error) {
	if len(iovs) == 0 {
		return 0, nil
	}
	var stack [4]syscall.Iovec
	vecs := stack[:0]
	if len(iovs) > len(stack) {
		vecs = make([]syscall.Iovec, 0, len(iovs))
	}
	for _, b := range iovs {
		var v syscall.Iovec
		v.SetLen(len(b))
		if len(b) > 0 {
			v.Base = &b[0]
		}
		vecs = append(vecs, v)
	}
	for {
		r1, _, errno := syscall.Syscall(syscall.SYS_WRITEV, uintptr(fd), uintptr(unsafe.Pointer(&vecs[0])), uintptr(len(vecs)))
		switch errno {
		case 0:
			return int(r1), nil
		case syscall.EINTR:
			continue
		default:
			return 0, errno
		}
	}
}
