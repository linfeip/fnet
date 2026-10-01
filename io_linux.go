//go:build linux

package fnet

import (
	"syscall"
	"unsafe"

	"golang.org/x/sys/unix"
)

// The sockets of connections are all non-blocking, so reads and writes never block and RawSyscall is used
// directly: it saves the entersyscall/exitsyscall that Syscall performs before and after every call to hand the
// P over to the scheduler.
// The return values are the same as unix.Read, unix.Write and unix.Writev (n is -1 on error).

func sysRead(fd int, b []byte) (int, error) {
	return rawIO(unix.SYS_READ, fd, unsafe.Pointer(unsafe.SliceData(b)), len(b))
}

func sysWrite(fd int, b []byte) (int, error) {
	return rawIO(unix.SYS_WRITE, fd, unsafe.Pointer(unsafe.SliceData(b)), len(b))
}

// sysWritev, like unix.Writev, allocates the iovec array on the stack when there are no more than 8 segments.
func sysWritev(fd int, bs [][]byte) (int, error) {
	iovecs := make([]unix.Iovec, 0, 8)
	for _, b := range bs {
		v := unix.Iovec{Base: unsafe.SliceData(b)}
		v.SetLen(len(b))
		iovecs = append(iovecs, v)
	}
	return rawIO(unix.SYS_WRITEV, fd, unsafe.Pointer(unsafe.SliceData(iovecs)), len(iovecs))
}

func rawIO(trap uintptr, fd int, p unsafe.Pointer, n int) (int, error) {
	r, _, errno := syscall.RawSyscall(trap, uintptr(fd), uintptr(p), uintptr(n))
	if errno != 0 {
		return int(r), errno
	}
	return int(r), nil
}
