//go:build linux

package netpoll

import "golang.org/x/sys/unix"

// KeepAliveInherited reports whether accepted sockets inherit their listener's
// keep-alive settings, so they can be set once on the listener instead of with
// several system calls per accept. Linux copies them into every child socket.
const KeepAliveInherited = true

// noDelayInherited: accepted sockets inherit TCP_NODELAY from their listener
// too, so it is set once on the listener rather than per accept.
const noDelayInherited = true

const tcpKeepIdle = unix.TCP_KEEPIDLE

// acceptRetryable reports whether accept failed for the queued connection
// rather than the listener, so the next one may succeed: the peer gave up, a
// firewall refused it (EPERM), or it had a pending network error, which
// accept(2) on Linux returns and says to retry like EAGAIN.
func acceptRetryable(err error) bool {
	switch err {
	case unix.ECONNABORTED, unix.EPROTO, unix.ENETDOWN, unix.ENOPROTOOPT, unix.EHOSTDOWN,
		unix.ENONET, unix.EHOSTUNREACH, unix.EOPNOTSUPP, unix.ENETUNREACH, unix.EPERM:
		return true
	}
	return false
}
