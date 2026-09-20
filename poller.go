package fnet

import "time"

// Event describes a readiness event returned by a Poller.
type Event struct {
	Fd       int
	Readable bool
	Writable bool
	Error    bool
	Hangup   bool
}

// Poller is the cross-platform I/O multiplexing abstraction.
type Poller interface {
	// AddRead registers fd for read readiness notifications.
	AddRead(fd int) error
	// AddWrite registers fd for write readiness notifications (keeps read).
	AddWrite(fd int) error
	// ModRead switches fd to read-only interest.
	ModRead(fd int) error
	// ModReadWrite switches fd to read+write interest.
	ModReadWrite(fd int) error
	// Delete removes fd from the poller.
	Delete(fd int) error
	// Wait blocks until events are ready or timeout elapses.
	// timeout < 0 waits forever; timeout == 0 is non-blocking.
	Wait(timeout time.Duration) ([]Event, error)
	// Wake interrupts a blocked Wait call immediately.
	Wake() error
	// Close releases poller resources.
	Close() error
}

// NewPoller creates a platform-native poller.
func NewPoller() (Poller, error) {
	return newPoller()
}
