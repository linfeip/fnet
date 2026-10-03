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
//
// They go through the socket calls (recvfrom, sendto, sendmsg) rather than read, write and writev: those take the
// file-level path first - the position lock, rw_verify_area and the file permission hooks of the security modules -
// which a socket has no use for, and it is measurable at one read and one write per message.
// MSG_NOSIGNAL turns a write to a connection the peer has reset into EPIPE without raising SIGPIPE.
// The return values are the same as unix.Read, unix.Write and unix.Writev (n is -1 on error).

func sysRead(fd int, b []byte) (int, error) {
	return rawIO(unix.SYS_RECVFROM, fd, unsafe.Pointer(unsafe.SliceData(b)), len(b), 0)
}

func sysWrite(fd int, b []byte) (int, error) {
	return rawIO(unix.SYS_SENDTO, fd, unsafe.Pointer(unsafe.SliceData(b)), len(b), unix.MSG_NOSIGNAL)
}

// sysWritev, like unix.Writev, allocates the iovec array on the stack when there are no more than 8 segments.
func sysWritev(fd int, bs [][]byte) (int, error) {
	iovecs := make([]unix.Iovec, 0, 8)
	for _, b := range bs {
		v := unix.Iovec{Base: unsafe.SliceData(b)}
		v.SetLen(len(b))
		iovecs = append(iovecs, v)
	}
	msg := unix.Msghdr{Iov: unsafe.SliceData(iovecs)}
	msg.SetIovlen(len(iovecs))
	return rawIO(unix.SYS_SENDMSG, fd, unsafe.Pointer(&msg), unix.MSG_NOSIGNAL, 0)
}

// rawIO issues a socket call whose remaining arguments (recvfrom's and sendto's address) are all zero.
func rawIO(trap uintptr, fd int, p unsafe.Pointer, n, flags int) (int, error) {
	r, _, errno := syscall.RawSyscall6(trap, uintptr(fd), uintptr(p), uintptr(n), uintptr(flags), 0, 0)
	if errno != 0 {
		return int(r), errno
	}
	return int(r), nil
}
