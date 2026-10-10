//go:build linux || darwin

package fnet

import (
	"io"

	"github.com/linfeip/fnet/internal/bytepool"
	"github.com/linfeip/fnet/poll"

	"golang.org/x/sys/unix"
)

// Values of the connection state word conn.state: the low bits are the pending events, and scheduledBit
// means a task is already queued or running.
const (
	evRead  = uint32(poll.EventRead)  // readable (a closed peer and errors count as well)
	evWrite = uint32(poll.EventWrite) // writable
	evHup   = uint32(poll.EventHup)   // the peer has closed or an error occurred, see conn.peerClosed
	evOpen  = evHup << 1              // new connection, the OnOpen callback is pending
	evClose = evOpen << 1             // a close has been requested, see requestClose

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

// schedule submits the connection's task to the Executor (taskpool.DefaultTaskPool.SubmitTo by default), keyed by the
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
		}
	}()
	c.handle(ev)
	done = true
	// A closed connection keeps scheduledBit set, so there is no further task.
	if !c.closed && !c.state.CompareAndSwap(scheduledBit, 0) {
		c.schedule()
	}
}

// handle processes one round of events: setting up a new connection (its socket options, then the OnOpen callback),
// flushing the send buffer, reading, and finally a check for whether the connection has to be closed. A close request
// (including one made from a callback in this round) is handled in the next round: data the peer has already sent but
// that has not been read yet is read away first, so that the socket holds no unread data at close time, which would make
// the peer receive an RST instead of a FIN.
func (c *conn) handle(ev uint32) {
	if c.closed { // events left over after the connection was closed
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
	drained := false
	if ev&evWrite != 0 && c.backlogged.Load() {
		drained = c.flush()
	}
	if ev&evHup != 0 {
		c.peerClosed = true
	}
	if ev&evRead != 0 && !c.readPaused.Load() {
		c.read()
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
		case unix.EINTR: // interrupted by a signal: this edge-triggered event was used up without reading data, read again
			c.notify(evRead)
		case nil: // EOF: the peer closed its write direction
			c.requestCloseOnEOF()
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

// flush sends the backed-up data on a writable event, writing only once per event: when it cannot write everything the
// kernel send buffer is full, and another writable event will come. It reports whether the send buffer drained.
func (c *conn) flush() bool {
	c.mu.Lock()
	if c.out.Len() == 0 {
		c.mu.Unlock()
		return false
	}
	n, err := sysWrite(c.fd, c.out.Bytes()[c.outPos:])
	switch err {
	case nil, unix.EAGAIN:
	case unix.EINTR: // interrupted by a signal: this edge-triggered event was used up without writing data, write again
		c.mu.Unlock()
		c.notify(evWrite)
		return false
	default:
		c.mu.Unlock()
		c.requestClose(err)
		return false
	}
	if n > 0 {
		c.outPos += n
	}
	drained := c.outPos == c.out.Len()
	if drained { // the send buffer has drained: return the buffer to the pool
		c.out.Release()
		c.outPos = 0
		c.backlogged.Store(false)
	}
	c.mu.Unlock()
	return drained
}

// requestClose requests that the connection be closed; it may be called from any goroutine, and the close itself is
// done by the connection's task (see closeIfRequested).
// err is the reason reported to OnClose: nil means a local close, which waits until the send buffer has been sent before
// closing; a non-nil err closes immediately, and so it does for a connection that has already requested a local close or
// read EOF, where the first non-nil err becomes the close reason. Writes after the request return net.ErrClosed.
func (c *conn) requestClose(err error) {
	c.mu.Lock()
	c.closing = true
	if err != nil && !c.closeNow {
		c.closeNow, c.closeErr = true, err
	}
	c.mu.Unlock()
	c.notify(evClose)
}

// requestCloseOnEOF requests a close when EOF is read (the peer closed its write direction): data already written still
// has to reach the peer, so just like a local close it waits until the send buffer has been sent before closing, and the
// err reported to OnClose is io.EOF; a connection that has already requested a close keeps its original close reason
// (nil for a local close).
func (c *conn) requestCloseOnEOF() {
	c.mu.Lock()
	if !c.closing {
		c.closing, c.closeErr = true, io.EOF
	}
	c.mu.Unlock()
	c.notify(evClose)
}

// closeIfRequested closes the connection when a close has been requested and either an immediate close was asked for or
// the send buffer has already been sent.
func (c *conn) closeIfRequested() {
	c.mu.Lock()
	closing, now, err, backlog := c.closing, c.closeNow, c.closeErr, c.out.Len() > 0
	c.mu.Unlock()
	if closing && (now || !backlog) {
		c.close(err)
	}
}

// close closes the connection and invokes OnClose with err as the close reason; it may only be called from the
// connection's task.
func (c *conn) close(err error) {
	if c.closed {
		return
	}
	defer c.loop.srv.openConnsWg.Done()
	connsByFd.remove(c) // before the fd is closed: a closed fd may immediately be reused by a new connection
	c.discardInbound()
	c.mu.Lock()
	c.closed = true
	unix.Close(c.fd)
	c.out.Release()
	c.backlogged.Store(false)
	c.mu.Unlock()
	c.in.Release()
	c.loop.srv.handler.OnClose(c, err)
}

// abandon closes a connection that could not be watched, before OnOpen; it may only be called from the connection's
// first task.
func (c *conn) abandon() {
	c.mu.Lock()
	c.closed = true
	unix.Close(c.fd)
	c.mu.Unlock()
	c.loop.srv.openConnsWg.Done()
}

// discardInbound reads away data that has arrived but has not been read yet: closing while the socket holds unread data
// sends an RST, which may make the peer discard the data it has already sent (such as a close frame).
// It reads only once, since it must not keep reading while the peer keeps sending.
func (c *conn) discardInbound() {
	size := c.loop.srv.opts.ReadBufferSize
	buf := bytepool.Get(size)
	defer buf.Release()
	sysRead(c.fd, buf.Bytes()[:size])
}
