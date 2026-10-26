//go:build linux || darwin

package fnet

import (
	"io"
	"os"
	"time"

	"github.com/linfeip/fnet/internal/bytepool"
	"github.com/linfeip/fnet/internal/units"
	"github.com/linfeip/fnet/poll"

	"golang.org/x/sys/unix"
)

// Values of the connection state word conn.state: the low bits are the pending events, and scheduledBit
// means a task is already queued or running.
const (
	evRead     = uint32(poll.EventRead)  // readable (a closed peer and errors count as well)
	evWrite    = uint32(poll.EventWrite) // writable
	evHup      = uint32(poll.EventHup)   // the peer has closed or an error occurred, see conn.peerClosed
	evOpen     = evHup << 1              // new connection, the OnOpen callback is pending
	evClose    = evOpen << 1             // a close has been requested, see requestClose
	evExpired  = evClose << 1            // the deadline has expired, see loop.checkDeadlines
	evShutdown = evExpired << 1          // the server is closing the connection, see Server.Serve
	evResume   = evShutdown << 1         // reading has resumed, see conn.ResumeRead

	scheduledBit = 1 << 31
)

// notify tells the connection that event ev is pending; it may be called from any goroutine.
//
// state records both the pending events and the status of the task: setting an event also sets scheduledBit, and if
// that bit was clear there is no task queued or running, so this call submits one; otherwise a task already exists
// and it will notice the new events at the end of its round and run another one (see run).
// A connection therefore has at most one task at a time, with reading, callbacks, flushing and closing all done
// serially inside it, and no events are lost. It reports whether this call submitted the task.
func (c *conn) notify(ev uint32) bool {
	if c.markEvents(ev) {
		c.schedule()
		return true
	}
	return false
}

// schedule submits the connection's task to the Executor (a private taskpool.NewDefault().SubmitTo by default), keyed by the
// connection's loop-affine key. The caller must hold the task, see markEvents.
func (c *conn) schedule() { c.loop.srv.opts.Executor(c.loop.taskKey(c.fd), c.task) }

// markEvents merges events and takes the right to submit the connection's single task; a caller it returns true for
// must submit it, never drop it.
func (c *conn) markEvents(ev uint32) bool {
	return c.state.Or(ev|scheduledBit)&scheduledBit == 0
}

// run is the connection's task: it takes the pending events and processes one round. When more events arrive while
// it is processing, it does not continue on the spot but submits itself again, going to the end of the executor's
// queue so that other connections run first, keeping connections fair with each other.
//
// If a callback panics the connection is closed and the panic keeps propagating up, to be recovered by the executor.
func (c *conn) run() {
	// clear the scheduledBit marker, clear the event markers
	ev := c.state.Swap(scheduledBit) &^ scheduledBit
	done := false
	defer func() {
		if !done {
			c.close(ErrHandlerPanic) // scheduledBit stays set, so there will be no further task
			if c.lingering() {       // OnClose itself panicked, the linger having begun
				c.endLinger()
			}
		}
	}()
	c.handle(ev)
	done = true
	// A closed connection keeps scheduledBit set, so there is no further task, unless it lingers.
	if (!c.closed || c.lingering()) && !c.state.CompareAndSwap(scheduledBit, 0) {
		c.schedule()
	}
}

// handle processes one round of events: setting up a new connection (its socket options, then the OnOpen callback),
// flushing the send buffer (and OnWritable once it is back down), OnResume once reading resumes, reading, requesting the
// close of a connection whose deadline expired or whose server is closing (see onClosing), and finally a check for
// whether the connection has to be closed.
// A close request (including one made from a callback in this round) is handled in the next round: data the peer has
// already sent but that has not been read yet is read away first, so that the socket holds no unread data at close
// time, which would make the peer receive an RST instead of a FIN.
func (c *conn) handle(ev uint32) {
	if c.closed { // events left over after the connection was closed, or for its linger
		if c.lingering() {
			c.linger(ev)
		}
		return
	}
	if ev&evOpen != 0 {
		// The connection is watched here rather than where it is accepted: accepts on a listener are serialized (the
		// kernel locks it), and an epoll_ctl per connection on that path would cap the rate at which connections are
		// accepted. Nothing is written to the socket before OnOpen, so its options are in place before the first byte
		// goes out.
		if !c.loop.watch(c) {
			return
		}
		setConnOptions(c.fd, c.loop.srv.opts.NoDelay)
		c.loop.srv.handler.OnOpen(c)
	}
	// The poller reports a writable socket along with every event, so evWrite is nearly always set: only a backlog
	// needs the lock and a write.
	drained, writable := false, false
	if ev&evWrite != 0 && c.backlogged.Load() {
		drained, writable = c.flush()
	}
	if writable { // the backlog is back down to maxBacklog: reading resumes, and a WritableHandler hears of it
		ev |= evRead
		if h, ok := c.loop.srv.handler.(WritableHandler); ok {
			h.OnWritable(c)
		}
	}
	if ev&evHup != 0 {
		c.peerClosed = true
	}
	if ev&evResume != 0 && !c.readPaused.Load() {
		if h, ok := c.loop.srv.handler.(ResumeHandler); ok {
			h.OnResume(c) // what it kept goes before what the socket holds
		}
	}
	// While the backlog is over maxBacklog the read is skipped; the flush that brings it back down reads again.
	if ev&evRead != 0 && !c.readClosed && !c.readPaused.Load() && c.Writable() {
		c.read()
	}
	var err error
	switch {
	case ev&evShutdown != 0:
		err = ErrServerClosed
	case ev&evExpired != 0 && c.expiredBy(time.Now().UnixNano()):
		err = os.ErrDeadlineExceeded
	}
	if err != nil {
		c.onClosing(err)
		c.requestClose(err)
	}
	if drained || ev&evClose != 0 { // the send buffer drained, or a close has been requested
		c.closeIfRequested()
	}
}

// read reads from the socket once and invokes OnData; the read buffer is borrowed from the pool and returned after the
// callback returns.
//
// Edge-triggered mode does not notify again for data that has already arrived: filling the buffer means the socket may
// still hold data, so the connection notifies itself to run another round; not filling it means the socket is drained.
// The exception is a closed peer: the FIN may have been merged into the same notification as the data, or have arrived
// while reading was paused, so once the data is read there will be no further notification and we must read until EOF.
func (c *conn) read() {
	srv := c.loop.srv
	size := srv.opts.ReadBufferSize
	buf := bytepool.Get(size)
	defer buf.Release()
	b := buf.Bytes()[:size]
	// Half a frame is left over on the connection: when the leftover data is small, the data read is appended after it
	// and both are handed to the callback together (the leftover data is copied to the start of the borrowed buffer, so
	// neither idle connections nor backed-up connections need a separate large buffer for it); when the leftover data
	// is too large (a big frame is being reassembled), the data read is appended to c.in.
	pending := 0
	if c.in.Len() <= size/2 {
		pending = c.in.Len()
	}
	n, err := sysRead(c.fd, b[pending:])
	if n <= 0 {
		switch err {
		case unix.EAGAIN: // already drained, wait for the next edge-triggered event
			// The peer has not closed after all: the evHup was left over from a previous connection with this fd (see
			// loop.handleEvent), and would otherwise cost every read from now on another round.
			c.peerClosed = false
		case unix.EINTR: // interrupted by a signal: this edge-triggered event was used up without reading data, read again
			c.notify(evRead)
		case nil: // EOF: the peer closed its write direction
			if !c.halfClose() {
				c.onClosing(io.EOF)
				c.requestCloseOnEOF()
			}
		default:
			c.requestClose(err)
		}
		return
	}
	if pending+n == size || c.peerClosed {
		c.notify(evRead)
	}
	data := b[:pending+n]
	switch {
	case pending > 0:
		copy(b, c.in.Bytes())
		c.in.Release()
	case c.in.Len() > 0:
		c.in.Append(data)
		data = c.in.Bytes()
	}
	consumed := min(max(srv.handler.OnData(c, data), 0), len(data))
	switch {
	case consumed == len(data):
		c.in.Release()
	case c.in.Len() > 0: // data is c.in itself, move the unconsumed part to the front
		if consumed > 0 {
			c.in.Discard(consumed)
		}
	default: // data lives in the borrowed buffer, copy the unconsumed part out
		c.in.Append(data[consumed:])
	}
}

// maxBacklog is how much data may wait in a connection's send buffer before the connection stops being read: what it
// sends in reply to what it reads then waits for the peer to take what it was sent, so a peer that does not read cannot
// make the server hold more than about this much plus the replies to one read (the same default high-water mark as
// Netty's). Reading resumes once the backlog is back down to maxBacklog, which only flush brings about.
const maxBacklog = 64 * units.KB

// backlog returns the number of bytes waiting in the send buffer.
func (c *conn) backlog() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.out.Len() - c.outPos
}

// flush sends the backed-up data on a writable event, writing only once per event: when it cannot write everything the
// kernel send buffer is full, and another writable event will come. It reports whether the send buffer drained, and
// whether the backlog came back down to maxBacklog from over it (see Writable).
func (c *conn) flush() (drained, writable bool) {
	c.mu.Lock()
	if c.out.Len() == 0 {
		c.mu.Unlock()
		return false, false
	}
	backlog := c.out.Len() - c.outPos
	n, err := sysWrite(c.fd, c.out.Bytes()[c.outPos:])
	switch err {
	case nil, unix.EAGAIN:
	case unix.EINTR: // interrupted by a signal: this edge-triggered event was used up without writing data, write again
		c.mu.Unlock()
		c.notify(evWrite)
		return false, false
	default:
		c.mu.Unlock()
		c.requestClose(err)
		return false, false
	}
	if n > 0 {
		c.outPos += n
		writable = backlog > maxBacklog && backlog-n <= maxBacklog
	}
	drained = c.outPos == c.out.Len()
	if drained { // the send buffer has drained: return the buffer to the pool
		c.out.Release()
		c.outPos = 0
		c.backlogged.Store(false)
	}
	c.mu.Unlock()
	return drained, writable
}
