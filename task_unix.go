//go:build linux || darwin

package fnet

import (
	"io"
	"os"

	"github.com/linfeip/fnet/internal/bytepool"
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
	evExpired  = evClose << 1            // the deadline has expired, see Server.checkDeadlines
	evShutdown = evExpired << 1          // the server is closing the connection, see Server.Serve
	evResume   = evShutdown << 1         // reading has resumed, see conn.ResumeRead

	scheduledBit = 1 << 31
)

// notify tells the connection that event ev is pending; it may be called from any goroutine.
//
// state records both the pending events and the status of the task: setting an event also sets scheduledBit, and if
// that bit was clear there is no task queued or running, so this call submits one; otherwise a task already exists
// and it will notice the new events at the end of its round and run another one (see release).
// A connection therefore has at most one task at a time, with reading, callbacks, flushing and closing all done
// serially inside it, and no events are lost. The state word is used only here, in run, release and takeOver.
func (c *conn) notify(ev uint32) {
	if c.state.Or(ev|scheduledBit)&scheduledBit == 0 {
		c.schedule()
	}
}

// schedule submits the connection's task to the Executor (a private taskpool.NewDefault().SubmitTo by default), keyed by
// the connection's fd.
func (c *conn) schedule() { c.loop.srv.opts.Executor(c.fd, c.task) }

// release gives up the task held by the caller: when events arrived meanwhile, it submits the task again for them.
func (c *conn) release() {
	if !c.state.CompareAndSwap(scheduledBit, 0) {
		c.schedule()
	}
}

// takeOver takes the task while no task is queued or running, and reports whether it did: from then on notify never
// submits it, so the caller owns everything the task owns, until it calls release.
func (c *conn) takeOver() bool { return c.state.CompareAndSwap(0, scheduledBit) }

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
			c.close(ErrHandlerPanic) // the task is not released, so there will be no further one
		}
	}()
	c.handle(ev)
	done = true
	// A closed connection keeps its task for good, so there is no further one.
	if !c.closed {
		c.release()
	}
}

// handle processes one round of events: setting up a new connection (its socket options, then the OnOpen callback),
// flushing the send buffer (and OnWritable once it is back down), OnResume once reading resumes, reading, requesting the
// close of a connection whose deadline expired or whose server is closing (see onClosing), and finally a check for
// whether the connection has to be closed.
// A close request (including one made from a callback in this round) is handled in the next round: data the peer has
// already sent but that has not been read yet is read away first, without being passed on, so that the socket holds no
// unread data at close time, which would make the peer receive an RST instead of a FIN.
func (c *conn) handle(ev uint32) {
	if ev&evHup != 0 {
		c.peerClosed = true
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
	// needs the lock and a write. A peer close or reset on a socket whose send buffer is full may report only evHup
	// (such as EVFILT_READ with EV_EOF on kqueue), and flushing then learns of the error at once.
	drained, writable := false, false
	if ev&(evWrite|evHup) != 0 && c.backlogged.Load() {
		drained, writable = c.flush()
	}
	if writable { // the backlog is back down to maxBacklog: reading resumes, and a WritableHandler hears of it
		ev |= evRead
		if h, ok := c.loop.srv.handler.(WritableHandler); ok {
			h.OnWritable(c)
		}
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
	case ev&evExpired != 0 && c.expiredBy(clockNow()):
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
		case unix.EINTR: // interrupted by a signal: this edge-triggered event was used up without reading, read again
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
	if c.closing.Load() { // read away after a close request (see handle)
		return
	}
	data := c.in.Join(b[:pending+n], pending)
	c.in.Keep(data, srv.handler.OnData(c, data))
}

// maxBacklog is how much data may wait in a connection's send buffer before the connection stops being read: what it
// sends in reply to what it reads then waits for the peer to take what it was sent, so a peer that does not read cannot
// make the server hold more than about this much plus the replies to one read (the same default high-water mark as
// Netty's). Reading resumes once the backlog is back down to maxBacklog, which only flush brings about.
const maxBacklog = 64 << 10

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

// requestClose requests that the connection be closed; it may be called from any goroutine, and the close itself is
// done by the connection's task (see closeIfRequested).
// err is the reason reported to OnClose: nil means a local close, which waits until the send buffer has been sent before
// closing; a non-nil err closes immediately, and so it does for a connection that has already requested a local close or
// read EOF, where the first non-nil err becomes the close reason. Writes after the request return net.ErrClosed.
func (c *conn) requestClose(err error) {
	c.mu.Lock()
	c.closing.Store(true)
	if err != nil && !closesNow(c.closeErr) {
		c.closeErr = err
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
	if c.closing.CompareAndSwap(false, true) {
		c.closeErr = io.EOF
	}
	c.mu.Unlock()
	c.notify(evClose)
}

// closesNow reports whether a close for reason err is carried out without sending the send buffer first: a local close
// (nil) and the peer's EOF still let the data already written reach the peer, any other reason does not.
func closesNow(err error) bool { return err != nil && err != io.EOF }

// onClosing invokes the OnClosing callback of a ClosingHandler before the engine requests the close of the connection on
// its own, err being the reason, unless a close has been requested already.
func (c *conn) onClosing(err error) {
	if h, ok := c.loop.srv.handler.(ClosingHandler); ok && !c.closing.Load() {
		h.OnClosing(c, err)
	}
}

// halfClose asks a HalfCloseHandler whether to keep the connection open now that EOF is read, and reports whether it does:
// the connection is then not read again. A connection a close has been requested for is not asked.
func (c *conn) halfClose() bool {
	h, ok := c.loop.srv.handler.(HalfCloseHandler)
	if !ok || c.closing.Load() || !h.OnHalfClose(c) {
		return false
	}
	c.readClosed = true
	return true
}

// closeIfRequested closes the connection when a close has been requested and either its reason closes it at once (see
// closesNow) or the send buffer has already been sent.
func (c *conn) closeIfRequested() {
	c.mu.Lock()
	closing, err, backlog := c.closing.Load(), c.closeErr, c.out.Len() > 0
	c.mu.Unlock()
	if closing && (closesNow(err) || !backlog) {
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
	c.mu.Lock()
	c.closed = true // before the fd is closed: whoever sees closed=false while holding mu may use the fd
	c.out.Release()
	c.backlogged.Store(false)
	c.mu.Unlock()
	c.in.Release()
	if err != io.EOF { // once EOF is read nothing is left to read away
		c.discardInbound()
	}
	connsByFd.remove(c) // before the fd is closed: a closed fd may immediately be reused by a new connection
	unix.Close(c.fd)
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
	sysRead(c.fd, buf.Bytes()[:size])
	buf.Release()
}
