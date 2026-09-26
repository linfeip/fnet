//go:build linux

package netpoll

import (
	"syscall"
	"unsafe"
)

// Reads, writes and accepts on a non-blocking socket never block, so on Linux
// they are raw system calls, without the scheduler handshake of a blocking one
// (entersyscall and exitsyscall). Besides its cost, that handshake lets the
// runtime hand the caller's P to another thread once a call has lasted a
// sysmon tick, 20µs or so, which a loopback writev can take; with the CPU
// saturated the caller then queues for a P again before it can go on.

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
