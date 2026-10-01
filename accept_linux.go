//go:build linux

package fnet

import "golang.org/x/sys/unix"

// accept accepts a new connection; the returned fd is already set to non-blocking and close-on-exec.
func accept(fd int) (int, unix.Sockaddr, error) {
	return unix.Accept4(fd, unix.SOCK_NONBLOCK|unix.SOCK_CLOEXEC)
}

// setKeepAlive enables TCP keepalive with the same parameters as the standard library net defaults: probing starts
// after 15s of idle time, at a 15s interval, and the connection is considered down after 9 unanswered probes.
func setKeepAlive(fd int) {
	unix.SetsockoptInt(fd, unix.SOL_SOCKET, unix.SO_KEEPALIVE, 1)
	unix.SetsockoptInt(fd, unix.IPPROTO_TCP, unix.TCP_KEEPIDLE, 15)
	unix.SetsockoptInt(fd, unix.IPPROTO_TCP, unix.TCP_KEEPINTVL, 15)
	unix.SetsockoptInt(fd, unix.IPPROTO_TCP, unix.TCP_KEEPCNT, 9)
}
