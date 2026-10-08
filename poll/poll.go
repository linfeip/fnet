// Package poll wraps the operating system's IO multiplexing mechanism (epoll on Linux, kqueue on macOS) and
// provides the Reactor event loops with a uniform minimal interface: registering/removing an fd,
// waiting for ready events, and waking up a waiting event loop from another goroutine.
//
// Conventions:
//   - an fd registered with AddListener (a listening socket) or AddEdge (a connection) is edge-triggered (ET).
//   - AddListener/AddEdge/Delete/Wake may be called concurrently from any goroutine;
//     Poll and Block may be called by any number of goroutines at the same time, each with its own Batch.
package poll

// BatchSize is the number of events one Poll or Block call retrieves at most. The goroutine that retrieves events only
// hands their tasks over, so a large batch takes fewer system calls under load: with 8 events a call, 4096 echoing
// connections on 24 CPUs ran about 2% slower.
const BatchSize = 1024

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
