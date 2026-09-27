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

// acceptRetryable reports whether accept failed for the connection it took
// off the queue rather than for the listener, so the next one may be fine:
// the peer gave up, or, since Linux reports a new socket's pending network
// errors this way, one of those accept(2) says to retry like EAGAIN (and
// EPERM, a firewall refusing the connection).
func acceptRetryable(err error) bool {
	switch err {
	case unix.ECONNABORTED, unix.EPROTO, unix.ENETDOWN, unix.ENOPROTOOPT, unix.EHOSTDOWN,
		unix.ENONET, unix.EHOSTUNREACH, unix.EOPNOTSUPP, unix.ENETUNREACH, unix.EPERM:
		return true
	}
	return false
}
