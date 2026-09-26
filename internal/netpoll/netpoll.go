// Package netpoll is the platform layer: readiness polling (epoll, kqueue, or
// the Windows emulation) and the raw non-blocking socket calls around it.
// Everything above this package is platform independent.
package netpoll

import (
	"errors"
	"syscall"
	"time"
)

// Event is one readiness notification. Errors and hang-ups are reported as
// Readable: the next Read surfaces them as an error or EOF.
type Event struct {
	Fd       int
	Readable bool
	Writable bool
	// Hup reports that the peer finished sending or the socket failed: once
	// the buffered bytes are read, Read returns EOF or the error. It rides on
	// the same edge as the last data, so a reader must not stop at a short
	// read or it never sees the end.
	Hup bool
}

// Poller is an edge-triggered readiness multiplexer. Registration methods are
// safe to call from any goroutine and take effect at once, even while another
// goroutine is blocked in Wait.
type Poller interface {
	// Add registers fd for read readiness.
	Add(fd int) error
	// EnableWrite adds write readiness to an fd registered with Add.
	EnableWrite(fd int) error
	// DisableWrite drops write readiness, keeping read readiness.
	DisableWrite(fd int) error
	// Wait blocks until events arrive, Wake is called, or timeout elapses
	// (timeout < 0 waits forever). The returned slice is reused by the next Wait.
	Wait(timeout time.Duration) ([]Event, error)
	// Wake interrupts a blocked Wait.
	Wake() error
	// Close releases the poller.
	Close() error
}

// KeepAlive configures TCP keep-alive probes on a socket: Idle is the quiet
// time before the first probe, Interval the time between unanswered probes, and
// Count how many unanswered probes drop the peer. Zero fields keep the platform
// defaults; a negative Idle turns keep-alive off.
type KeepAlive struct {
	Idle     time.Duration
	Interval time.Duration
	Count    int
}

// iovBatch is how many buffers Writev passes to one writev(2), well below
// IOV_MAX (1024 on Linux and Darwin), beyond which writev fails with EINVAL.
const iovBatch = 64

// NewPoller creates the platform-native poller.
func NewPoller() (Poller, error) { return newPoller() }

// IsAgain reports whether err means the operation would block.
func IsAgain(err error) bool {
	return errors.Is(err, syscall.EAGAIN) || errors.Is(err, syscall.EWOULDBLOCK)
}
