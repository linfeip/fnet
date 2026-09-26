//go:build unix && !linux

package netpoll

import (
	"syscall"
	"unsafe"

	"golang.org/x/sys/unix"
)

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
