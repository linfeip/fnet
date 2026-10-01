//go:build darwin

package fnet

import (
	"syscall"

	"golang.org/x/sys/unix"
)

// accept accepts a new connection; the returned fd is already set to non-blocking and close-on-exec.
// darwin has no accept4: as in the standard library, CLOEXEC is set under the protection of ForkLock to keep the
// fd from leaking into a child process of a concurrent exec.
func accept(fd int) (int, unix.Sockaddr, error) {
	syscall.ForkLock.RLock()
	nfd, sa, err := unix.Accept(fd)
	if err == nil {
		unix.CloseOnExec(nfd)
	}
	syscall.ForkLock.RUnlock()
	if err != nil {
		return -1, nil, err
	}
	if err := unix.SetNonblock(nfd, true); err != nil {
		unix.Close(nfd)
		return -1, nil, err
	}
	return nfd, sa, nil
}

// setKeepAlive enables TCP keepalive with the same parameters as the standard library net defaults: probing starts
// after 15s of idle time, at a 15s interval, and the connection is considered down after 9 unanswered probes.
// darwin's idle-time option is TCP_KEEPALIVE (TCP_KEEPIDLE on Linux).
func setKeepAlive(fd int) {
	unix.SetsockoptInt(fd, unix.SOL_SOCKET, unix.SO_KEEPALIVE, 1)
	unix.SetsockoptInt(fd, unix.IPPROTO_TCP, unix.TCP_KEEPALIVE, 15)
	unix.SetsockoptInt(fd, unix.IPPROTO_TCP, unix.TCP_KEEPINTVL, 15)
	unix.SetsockoptInt(fd, unix.IPPROTO_TCP, unix.TCP_KEEPCNT, 9)
}
