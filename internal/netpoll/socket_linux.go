//go:build linux

package netpoll

import "golang.org/x/sys/unix"

// KeepAliveInherited reports whether accepted sockets inherit their listener's
// keep-alive settings, so they can be set once on the listener instead of with
// several system calls per accept. Linux copies them into every child socket.
const KeepAliveInherited = true

const tcpKeepIdle = unix.TCP_KEEPIDLE
