//go:build darwin

package netpoll

import "golang.org/x/sys/unix"

// KeepAliveInherited reports whether accepted sockets inherit their listener's
// keep-alive settings. Darwin is not relied on to copy the timings, so they are
// set on every accepted socket.
const KeepAliveInherited = false

// noDelayInherited: TCP_NODELAY is set on every accepted socket, since Darwin is
// not relied on to copy it from the listener.
const noDelayInherited = false

const tcpKeepIdle = unix.TCP_KEEPALIVE

// acceptRetryable reports whether accept failed for the connection it took
// off the queue rather than for the listener: the peer gave up.
func acceptRetryable(err error) bool { return err == unix.ECONNABORTED }
