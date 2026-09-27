//go:build linux

package netpoll

import (
	"syscall"
	"unsafe"
)

// I/O on a non-blocking socket never blocks, so on Linux it uses raw system
// calls and skips entersyscall/exitsyscall. That handshake costs time, and a
// call that outlasts a sysmon tick (~20µs, as a loopback writev can) loses its
// P; with the CPU saturated the caller then waits to get one back.

// Read reads from a non-blocking fd. n == 0 with a nil error means EOF.
func Read(fd int, b []byte) (int, error) {
	return rawIO(syscall.SYS_READ, fd, unsafe.Pointer(unsafe.SliceData(b)), len(b))
}

// Write writes to a non-blocking fd and may return a short count.
func Write(fd int, b []byte) (int, error) {
	return rawIO(syscall.SYS_WRITE, fd, unsafe.Pointer(unsafe.SliceData(b)), len(b))
}

func writev(fd int, vecs *syscall.Iovec, n int) (int, error) {
	return rawIO(syscall.SYS_WRITEV, fd, unsafe.Pointer(vecs), n)
}

func rawIO(trap uintptr, fd int, p unsafe.Pointer, n int) (int, error) {
	for {
		r, _, errno := syscall.RawSyscall(trap, uintptr(fd), uintptr(p), uintptr(n))
		switch errno {
		case 0:
			return int(r), nil
		case syscall.EINTR:
		default:
			return 0, errno
		}
	}
}
