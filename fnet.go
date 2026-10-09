// Package fnet is an event-driven TCP network library based on the Reactor model.
//
// On Linux (epoll) / macOS (kqueue) it uses a main/sub-reactor structure:
//   - listeners (a server may listen on several addresses): on Linux every sub-reactor listens on each address
//     with a socket of its own, sharing it through SO_REUSEPORT, so that the kernel spreads the connections over
//     them and each accepts and keeps its own; on macOS the first sub-reactor's Poller watches the one listener of
//     each address, and the connections are distributed to the sub-reactors in round-robin order. The worker that
//     finds a listener ready accepts;
//   - sub-reactor (event loop): a Poller for the events of the connections it owns (edge-triggered), polled by
//     the loop's worker, one per loop and by default one loop per 8 Ps, which hands the tasks of the connections it
//     finds ready to the executor (see Options.Executor), helps the other loops when its own has nothing to do,
//     and only then waits in the kernel;
//   - connection task: run by the executor, it reads the data, invokes the Handler callbacks, keeps draining the
//     send buffer and closes the connection; a connection has at most one task at a time, and the data read
//     borrows a buffer from a pool that is returned as soon as the callback returns;
//   - a connection does not occupy a goroutine of its own and an idle connection holds no read or write
//     buffer, so a small number of goroutines can carry a million connections.
//
// Other platforms (such as Windows) implement the same API on top of the standard library net: one goroutine
// per connection.
package fnet

import (
	"errors"
	"io"
	"net"
	"runtime"
	"time"

	"github.com/linfeip/fnet/internal/units"
	"github.com/linfeip/fnet/taskpool"
)

var (
	// ErrServerClosed is returned by Serve after Server.Close, and is also the OnClose reason when the
	// server is shut down.
	ErrServerClosed = errors.New("fnet: server closed")
	// ErrHandlerPanic is the OnClose reason when a callback panics: the connection is closed and the panic
	// keeps propagating upwards, to be recovered by the executor. Linux/macOS only.
	ErrHandlerPanic = errors.New("fnet: handler panic")
)

// Handler is the connection event callback interface.
//
// The callbacks of a single connection always run serially: OnOpen first, OnClose last and only once. On
// Linux/macOS the callbacks run in a goroutine of the executor (see Options.Executor), the connection is not
// read again before the callback returns, and the callbacks of other connections share the executor's
// goroutines as well, so a callback must return quickly and should not block: the default executor starts more
// goroutines while callbacks block, up to its limit (see taskpool.DefaultTaskPool), and each blocked callback
// holds one of them. Time-consuming logic should be handed to other goroutines, which may safely call Conn.Write
// and Conn.Close concurrently.
type Handler interface {
	// OnOpen is called after a new connection has been established.
	OnOpen(c Conn)
	// OnData is called after data has been received. data is all of the connection's currently unconsumed
	// inbound data and is only valid within this callback (on Linux/macOS it may be a read buffer borrowed by
	// the engine that is reused once the callback returns, and it may be modified in place);
	// return the number of bytes consumed this time, the unconsumed part is kept by the engine and passed to
	// the callback again joined with the data that arrives later.
	OnData(c Conn, data []byte) (consumed int)
	// OnClose is called after the connection has been closed; err is the close reason: nil for an explicit
	// Close, io.EOF when the peer closed.
	//
	// Half-close is not supported: after EOF is read (the peer closed its write direction) the engine first
	// sends out the data in the send buffer and then closes the connection, and writes after that return
	// net.ErrClosed. A reply written synchronously in OnData is unaffected, but a reply written by another
	// goroutine only after the EOF is discarded.
	OnClose(c Conn, err error)
}

// Conn represents a TCP connection. Except for SetContext, all methods may be called concurrently from any
// goroutine.
type Conn interface {
	// LocalAddr returns the local address. On Linux/macOS the local address has to be queried from the kernel
	// and is not kept resident per connection, so it returns nil once the connection has been closed
	// (including inside OnClose); RemoteAddr is unaffected.
	LocalAddr() net.Addr
	// RemoteAddr returns the peer address.
	RemoteAddr() net.Addr
	// Context returns the user data bound to the connection.
	Context() any
	// SetContext binds user data; it should be called in OnOpen.
	SetContext(ctx any)
	// Write sends data. On Linux/macOS it never blocks: the data is written directly to the socket as far as
	// possible, and the part that does not fit is copied into the connection's send buffer, from where the
	// connection's task keeps sending once the socket becomes writable.
	// The caller may reuse b once it returns; it returns net.ErrClosed when the connection is already closed.
	Write(b []byte) (n int, err error)
	// Writev sends the several segments in bs in order, with the same semantics as Write, and no other write
	// is interleaved between the segments. The data is handed to the kernel in a single writev, which saves
	// the caller the copy of joining the segments into one block. bs is not modified.
	Writev(bs [][]byte) (n int, err error)
	// Close closes the connection. Data in the send buffer that has not been sent yet is sent out in full
	// before closing, even if an EOF from the peer is read in the meantime; on an error or an expired deadline
	// it closes immediately. Writes after the call return net.ErrClosed.
	Close() error
	// PauseRead pauses reading (read backpressure): no more data is read from the socket and OnData is no
	// longer called, until ResumeRead.
	// The unread data stays in the kernel receive buffer, and once that buffer is full TCP flow control makes
	// the peer stop sending.
	// When called in OnData it takes effect once this callback returns; when called from another goroutine, a
	// read that has already started still results in one more OnData callback.
	// Writes are unaffected while paused; since nothing is read, a close by the peer is usually only noticed
	// after reading is resumed. Deadlines apply as usual
	// (other platforms do not check deadlines while paused, they only take effect after reading is resumed).
	// Repeated calls have no additional effect, and a call after the connection is closed has no effect.
	PauseRead()
	// ResumeRead resumes reading. Data that was handed to OnData before the pause but left unconsumed is only
	// passed to the callback again, joined with new data, once new data arrives.
	ResumeRead()
	// SetDeadline sets the connection's close deadline: when it expires the connection is closed immediately
	// (without waiting for the send buffer to drain) and the err of OnClose is os.ErrDeadlineExceeded;
	// the zero value cancels the deadline. Receiving data does not extend it automatically; whether to extend
	// it is up to the caller.
	// On Linux/macOS the deadline is checked once per second, so the actual close time may be up to about one
	// second later than t.
	SetDeadline(t time.Time)
	// Detach takes the connection out of the engine and returns it as a standard library net.Conn, so that the
	// rest of it can be served with blocking reads and writes (fhttp hands such connections to net/http). Read on
	// the net.Conn first returns the data OnData left unconsumed, and the data still in the send buffer is
	// written out before Detach returns. From then on the engine is done with the connection: no callback is
	// invoked again (OnClose included), this Conn behaves as a closed one, and closing the net.Conn is up to the
	// caller.
	//
	// Reading must be paused first (PauseRead from OnData), so that OnData does not consume what is meant for
	// the net.Conn. Detach waits for a running callback of the connection to return, so it must not be called
	// from a callback. It returns net.ErrClosed when the connection is closed or a close has been requested.
	Detach() (net.Conn, error)
}

// detachedConn is a connection taken out of the engine by Conn.Detach. Read first returns the data OnData left
// unconsumed; the *net.TCPConn is embedded so that the rest of its methods (CloseWrite, ReadFrom, ...) stay
// available, which net/http uses.
type detachedConn struct {
	*net.TCPConn
	in []byte
}

func (c *detachedConn) Read(b []byte) (int, error) {
	if len(c.in) == 0 {
		return c.TCPConn.Read(b)
	}
	n := copy(b, c.in)
	if c.in = c.in[n:]; len(c.in) == 0 {
		c.in = nil
	}
	return n, nil
}

// WriteTo hides (*net.TCPConn).WriteTo, which would read from the socket past the unconsumed data.
func (c *detachedConn) WriteTo(w io.Writer) (int64, error) {
	return io.Copy(w, struct{ io.Reader }{c})
}

// Options holds the engine parameters; the zero value is the default configuration.
type Options struct {
	// NumLoops is the number of sub-reactors (event loops); max(2, runtime.GOMAXPROCS(0)/8) when <=0. Each loop has
	// one worker polling it, and a few loops leave the Ps to the executor's goroutines running the tasks, while
	// every poll returns a larger batch. Linux/macOS only.
	NumLoops int
	// ReadBufferSize is the buffer size of a single read; 16KB when <=0. A connection's task borrows from the
	// buffer pool only while reading and returns the buffer once the callback returns, so idle connections
	// hold none, and the number of buffers held at the same time never exceeds the number of tasks currently
	// in a callback. Linux/macOS only.
	ReadBufferSize int
	// Executor runs a connection's tasks (reading, invoking the Handler callbacks, draining the send buffer,
	// closing): it is called once when a connection has events, a connection has at most one task at a time,
	// and it is called again when more events arrive while one is being processed. key is a connection key,
	// the same for all of its tasks, so an executor may use it to keep a connection's tasks on one queue.
	// Executor may be called from any goroutine (the event loops' workers, the executor's own goroutines, the
	// application goroutines calling Write and Close) and must not block; the task must run asynchronously
	// (it must not run directly on the caller's stack) and must not be dropped.
	// When a callback panics the connection is closed with ErrHandlerPanic and the task panics, so Executor
	// must recover, otherwise the process exits.
	// When nil it is the SubmitTo of taskpool.DefaultTaskPool (lock-free submission, worker reuse, recovers and
	// logs the panic), which puts the tasks of a connection always in the shard key selects rather than in a
	// random one each time. Linux/macOS only.
	Executor func(key int, task func())
}

func (o Options) withDefaults() Options {
	if o.NumLoops <= 0 {
		o.NumLoops = max(2, runtime.GOMAXPROCS(0)/8)
	}
	if o.ReadBufferSize <= 0 {
		o.ReadBufferSize = 16 * units.KB
	}
	if o.Executor == nil {
		o.Executor = taskpool.DefaultTaskPool.SubmitTo
	}
	return o
}
