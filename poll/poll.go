// Package poll wraps the operating system's IO multiplexing mechanism (epoll on Linux, kqueue on macOS) and
// provides the Reactor event loops with a uniform minimal interface: registering/removing an fd,
// waiting for ready events, and waking up a waiting event loop from another goroutine.
//
// Conventions:
//   - an fd registered with AddRead is level-triggered (LT), used for the listening socket; an fd registered with
//     AddEdge is edge-triggered (ET), used for connections.
//   - AddRead/AddEdge/Delete/Wake may be called concurrently from any goroutine;
//     Wait may only be called by the single event loop goroutine.
package poll

// Event is a ready event.
type Event uint8

const (
	// EventRead means readable. A close by the peer or a connection error is also reported as a readable event,
	// and the subsequent read returns EOF or the specific error.
	EventRead Event = 1 << iota
	// EventWrite means writable.
	EventWrite
	// EventHup means the peer has closed its write direction (a FIN was received) or the connection failed; it is
	// always reported together with EventRead. Under edge-triggered mode the FIN may be merged with data that
	// arrived earlier into the same notification: there is no further notification once the data has been read, so
	// data that does not fill the buffer must not be taken as having drained it — keep reading until EOF or an error.
	EventHup
)
