//go:build linux || darwin

package netpoll

import (
	"syscall"
	"unsafe"
)

// Writev writes iovs with writev(2), iovBatch buffers per call, and may return
// a short count. The buffers are described in an array on the stack, so they
// do not escape to the heap.
func Writev(fd int, iovs [][]byte) (int, error) {
	total := 0
	for len(iovs) > 0 {
		var vecs [iovBatch]syscall.Iovec
		n, want := 0, 0
		for ; n < len(vecs) && len(iovs) > 0; iovs = iovs[1:] {
			if b := iovs[0]; len(b) > 0 {
				vecs[n].Base = &b[0]
				vecs[n].SetLen(len(b))
				want += len(b)
				n++
			}
		}
		if n == 0 {
			break
		}
		w, err := writev(fd, &vecs[0], n)
		total += w
		if err != nil || w < want {
			return total, err
		}
	}
	return total, nil
}

func writev(fd int, vecs *syscall.Iovec, n int) (int, error) {
	for {
		r, _, errno := syscall.Syscall(syscall.SYS_WRITEV, uintptr(fd), uintptr(unsafe.Pointer(vecs)), uintptr(n))
		switch errno {
		case 0:
			return int(r), nil
		case syscall.EINTR:
		default:
			return 0, errno
		}
	}
}
