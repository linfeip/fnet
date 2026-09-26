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
