package fnet

import (
	"bufio"
	"errors"
	"io"
	"net"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/linfeip/fnet/internal/bufpool"
	"github.com/linfeip/fnet/internal/reactor"
	"github.com/linfeip/fnet/pool"
)

// Conn is one connection of a Server. Its methods are safe to call from any
// goroutine; OnOpen, OnMessage and OnClose for one Conn never run concurrently.
//
// On the event loop, Split cuts complete messages out of the input and each is
// copied into a pooled buffer and queued; a partial message stays with the
// reactor until the rest arrives. On a worker, one drain at a time per
// connection runs the callbacks in order. An idle Conn keeps no queue.
type Conn struct {
	raw  *reactor.Conn
	h    *handler
	ctx  atomic.Pointer[any]
	quit atomic.Bool // closed from this side: skip the messages not handled yet
	dead bool        // event loop only: no more input is taken

	mu       sync.Mutex
	queue    []message // cut, waiting for OnMessage
	pending  int       // message bytes queued or being handled
	since    int64     // unix nanos: when the partial message began, or idleness did
	state    uint32
	closeErr error
}

// Conn state bits, guarded by mu.
const (
	csOpen      uint32 = 1 << iota // OnOpen is still due
	csRunning                      // a drain is scheduled or running
	csPaused                       // reading is paused: too many pending bytes
	csPartial                      // the reactor holds part of a message
	csInputDone                    // no more input (EOF, final token, Shutdown): close once handled
	csClosing                      // closed from this side: drop what is not handled yet
	csClosed                       // the connection is gone: OnClose is due
	csErrSet                       // closeErr holds the reason
	csArmed                        // a close deadline is set
)

// message is one complete message waiting for OnMessage.
type message struct {
	data []byte
	buf  *bufpool.Buffer // holds data; nil for an empty message
}

var emptyMessage = []byte{}

func (m *message) bytes() []byte {
	if m.data == nil {
		return emptyMessage
	}
	return m.data
}

func release(ms []message) {
	for i := range ms {
		bufpool.Put(ms[i].buf)
		ms[i] = message{}
	}
}

const (
	// batchSize messages cut from one read fit on the loop's stack.
	batchSize = 16
	// maxEmptyTokens without advancing close the connection, like bufio.Scanner.
	maxEmptyTokens = 100
)

var (
	errBadAdvance   = errors.New("fnet: Split advanced beyond its input")
	errNoProgress   = errors.New("fnet: Split returned too many messages without advancing")
	errPoolRejected = errors.New("fnet: the worker pool rejected the connection")
)

func newConn(rc *reactor.Conn, h *handler) *Conn {
	c := &Conn{raw: rc, h: h, since: time.Now().UnixNano()}
	if h.onOpen != nil {
		c.state = csOpen
	}
	return c
}

// ---------------------------------------------------------------------------
// Public API
// ---------------------------------------------------------------------------

// Write sends b and never blocks: what the kernel does not take at once is
// copied into the connection's queue and flushed by the event loop, so b may
// be reused as soon as Write returns, and one b may be written to many
// connections. The bytes of one Write or Writev are never interleaved with
// another's. With more than MaxOutboundBytes queued because the peer is not
// reading, Write fails with ErrWriteBufferFull; a write into an empty queue is
// always taken whole, so a message is never cut short on the wire. After
// Close it fails with net.ErrClosed. While the connection handles a burst of
// messages its output is held, for a millisecond at most, so that the replies
// leave in one system call.
func (c *Conn) Write(b []byte) (int, error) { return c.raw.Write(b) }

// Writev sends bufs as one unit, e.g. a header and a payload, with a single
// writev(2) when nothing is queued instead of copying them together first.
// Otherwise it behaves like Write.
func (c *Conn) Writev(bufs [][]byte) (int, error) { return c.raw.Writev(bufs) }

// Close closes the connection from this side: messages not handled yet are
// dropped, output already written is flushed, and OnClose follows with a nil
// error once the connection is gone. Close is idempotent.
func (c *Conn) Close() error {
	c.closeWith(nil)
	return nil
}

// LocalAddr returns the local network address.
func (c *Conn) LocalAddr() net.Addr { return c.raw.LocalAddr() }

// RemoteAddr returns the peer's network address.
func (c *Conn) RemoteAddr() net.Addr { return c.raw.RemoteAddr() }

// Context returns the value stored with SetContext, or nil.
func (c *Conn) Context() any {
	if p := c.ctx.Load(); p != nil {
		return *p
	}
	return nil
}

// SetContext stores a value with the connection, typically its session.
func (c *Conn) SetContext(v any) { c.ctx.Store(&v) }

// ---------------------------------------------------------------------------
// Event loop side (reactor.Handler, reactor.EOFHandler)
// ---------------------------------------------------------------------------

// OnData cuts the complete messages out of the input and queues them for the
// worker. It returns how much it consumed; the reactor keeps the rest, a
// partial message, and offers it again with the next bytes.
func (c *Conn) OnData(_ *reactor.Conn, data []byte) int {
	if c.dead {
		return len(data)
	}
	var buf [batchSize]message
	var a bufpool.Arena
	batch, off, final, err := c.cut(data, false, buf[:0], &a)
	a.Release()
	if err == nil && len(data)-off > c.h.maxMessage {
		err = ErrMessageTooLarge // a partial message already beyond the limit
	}
	if err != nil {
		release(batch)
		c.dead = true
		c.closeWith(err)
		return len(data)
	}
	if final {
		off = len(data) // bufio.ErrFinalToken: the rest is dropped
	}
	if !c.push(batch, off > 0, off < len(data), final, nil) || final {
		c.dead = true
		return len(data)
	}
	return off
}

// OnEOF runs when the peer finishes sending: what it left is cut once more
// with atEOF set, and the connection closes after the messages are handled.
func (c *Conn) OnEOF(_ *reactor.Conn, rest []byte) {
	if c.dead {
		return
	}
	c.dead = true
	var buf [batchSize]message
	var a bufpool.Arena
	batch, off, _, err := c.cut(rest, true, buf[:0], &a)
	a.Release()
	if err != nil {
		release(batch)
		c.closeWith(err)
		return
	}
	reason := io.EOF
	if off < len(rest) {
		reason = io.ErrUnexpectedEOF // the peer stopped in the middle of a message
	}
	c.push(batch, off > 0, false, true, reason)
}

// OnClose runs once the reactor connection is gone; the user's OnClose follows
// on a worker, after the messages still queued.
func (c *Conn) OnClose(_ *reactor.Conn, err error) {
	c.dead = true
	if errors.Is(err, net.ErrClosed) && c.h.stopping.Load() {
		err = ErrServerClosed
	}
	// A timeout or the server's Close is a close from this side: the messages
	// not handled yet are dropped. After the peer's close they still run.
	local := err == ErrServerClosed || errors.Is(err, net.ErrClosed) || errors.Is(err, os.ErrDeadlineExceeded)
	c.mu.Lock()
	c.recordLocked(err)
	c.state |= csClosed
	var dropped []message
	if local && c.state&csClosing == 0 {
		c.state |= csClosing
		c.quit.Store(true)
		dropped = c.dropLocked()
	}
	idle := c.state&csRunning == 0
	if idle {
		c.state |= csRunning // stays set: nothing is scheduled after OnClose
	}
	c.mu.Unlock()
	release(dropped)
	switch {
	case !idle: // the running drain gets to OnClose after the queue
	case c.h.onClose == nil:
		c.h.live.Done() // no business code to run: no worker hop
	default:
		c.schedule()
	}
}

// cut runs Split over data and appends a copy of each message it yields to
// batch, the copies sharing the arena's buffers. It returns the batch, how much
// was consumed, whether Split ended the input with bufio.ErrFinalToken, and why
// the connection must close, if it must. A panic in Split closes only this
// connection.
func (c *Conn) cut(data []byte, atEOF bool, into []message, a *bufpool.Arena) (batch []message, off int, final bool, err error) {
	batch = into
	defer func() {
		if r := recover(); r != nil {
			err = newPanicError(r)
		}
	}()
	empties := 0
	for off < len(data) || atEOF {
		adv, tok, serr := c.h.split(data[off:], atEOF)
		if serr != nil && serr != bufio.ErrFinalToken {
			return batch, off, false, serr
		}
		if adv < 0 || adv > len(data)-off {
			return batch, off, false, errBadAdvance
		}
		off += adv
		if tok != nil {
			if len(tok) > c.h.maxMessage {
				return batch, off, false, ErrMessageTooLarge
			}
			var m message
			if len(tok) > 0 {
				m.data, m.buf = a.Copy(tok, len(tok)+len(data)-off)
			}
			batch = append(batch, m)
		}
		switch {
		case serr != nil:
			return batch, off, true, nil
		case adv > 0:
			empties = 0
		case tok == nil:
			return batch, off, false, nil // Split wants more input
		default:
			if empties++; empties > maxEmptyTokens {
				return batch, off, false, errNoProgress
			}
		}
	}
	return batch, off, false, nil
}

// push hands messages cut on the loop to the worker, and updates backpressure
// and the close deadline. partial reports that the reactor keeps part of a
// message; done that no input follows, for reason end. It reports false once
// the connection takes no more input.
func (c *Conn) push(batch []message, progressed, partial, done bool, end error) bool {
	size := 0
	for i := range batch {
		size += len(batch[i].data)
	}
	now := time.Now().UnixNano()
	c.mu.Lock()
	if c.state&(csInputDone|csClosing|csClosed) != 0 {
		c.mu.Unlock()
		release(batch)
		return false
	}
	c.queue = append(c.queue, batch...)
	c.pending += size
	if partial {
		if c.state&csPartial == 0 || progressed {
			c.since = now // a new message began in this read
		}
		c.state |= csPartial
	} else {
		c.state &^= csPartial
		c.since = now
	}
	if done {
		c.state |= csInputDone
		c.recordLocked(end)
	}
	if c.h.maxPending > 0 && c.pending > c.h.maxPending && c.state&csPaused == 0 {
		// Under the lock, so a worker that drains everything at once cannot
		// resume before the pause lands and leave the connection unread.
		c.state |= csPaused
		c.raw.PauseRead()
	}
	idle := c.state&csRunning == 0 && len(c.queue) == 0
	closeNow := done && idle // nothing left to handle: no worker hop just to close
	if closeNow {
		c.state |= csClosing
		c.quit.Store(true)
	}
	start := !idle && c.state&csRunning == 0
	if start {
		c.state |= csRunning
	}
	c.rearmLocked()
	c.mu.Unlock()
	switch {
	case closeNow:
		_ = c.raw.Close()
	case start:
		c.schedule()
	}
	return true
}

// ---------------------------------------------------------------------------
// Worker side
// ---------------------------------------------------------------------------

// start runs OnOpen on a worker, or arms the idle timeout. Accept loop.
func (c *Conn) start() {
	c.mu.Lock()
	if c.state&csOpen == 0 {
		c.rearmLocked()
		c.mu.Unlock()
		return
	}
	c.state |= csRunning
	c.mu.Unlock()
	c.schedule()
}

func (c *Conn) schedule() {
	if !pool.DispatchTask(c.h.submit, uint64(c.raw.Fd()), (*drainTask)(c)) {
		// A custom pool refused the task: shed the connection, but still run
		// the callbacks it is owed, on a goroutine of its own.
		c.closeWith(errPoolRejected)
		go c.drain()
	}
}

// drainTask is a Conn as its own pool.Task, which runs drain: dispatching it
// allocates nothing, and Conn itself gets no Run method.
type drainTask Conn

func (t *drainTask) Run() { (*Conn)(t).drain() }

// drain runs the connection's callbacks on a worker, in order. At most one
// drain per connection is scheduled or running (csRunning). A handled batch is
// settled under the same lock that takes the next one, and its emptied array
// takes the messages that arrive while the next batch is handled. An idle
// Conn keeps no array.
func (c *Conn) drain() {
	var (
		spare   []message // the array of the batch just handled, emptied
		handled int       // that batch's message bytes
	)
	for {
		c.mu.Lock()
		c.settleLocked(handled)
		handled = 0
		switch {
		case c.state&csOpen != 0:
			c.state &^= csOpen
			c.mu.Unlock()
			c.callOpen()

		case len(c.queue) > 0 && c.state&csClosing == 0:
			batch := c.queue
			c.queue = spare
			c.mu.Unlock()
			handled = c.handle(batch)
			spare = batch[:0]

		case c.state&csClosed != 0:
			// Gone, and everything it is owed has run: OnClose comes last.
			// csRunning stays set, so no drain is scheduled again.
			dropped := c.dropLocked()
			err := c.closeErr
			c.mu.Unlock()
			release(dropped)
			c.callClose(err)
			c.h.live.Done()
			return

		default:
			dropped := c.dropLocked()
			// The input ended and everything received is handled: close from
			// here, after the replies written so far are flushed.
			closeNow := c.state&(csInputDone|csClosing) == csInputDone
			if closeNow {
				c.state |= csClosing
				c.quit.Store(true)
			}
			c.state &^= csRunning
			if c.state&csPartial == 0 {
				c.since = time.Now().UnixNano() // idle from now
			}
			c.rearmLocked()
			c.mu.Unlock()
			release(dropped)
			if closeNow {
				_ = c.raw.Close()
			}
			return
		}
	}
}

// handle runs OnMessage for a batch, returns its buffers to the pool, and
// reports its message bytes. The replies to a batch of several messages leave
// in one write; a slow OnMessage holds them back for a millisecond at most
// (see reactor.Conn.Corked).
func (c *Conn) handle(batch []message) (size int) {
	run := func() {
		for i := range batch {
			size += len(batch[i].data)
			if !c.quit.Load() {
				c.deliver(&batch[i])
			}
			bufpool.Put(batch[i].buf)
			batch[i] = message{}
		}
	}
	if len(batch) > 1 {
		c.raw.Corked(run)
	} else {
		run()
	}
	return size
}

// settleLocked takes a handled batch's bytes out of pending, which may resume
// reading.
func (c *Conn) settleLocked(size int) {
	c.pending -= size
	if c.state&csPaused != 0 && c.pending <= c.h.lowPending {
		c.state &^= csPaused
		if c.state&csPartial != 0 {
			c.since = time.Now().UnixNano() // time spent paused is not the peer's
		}
		c.raw.ResumeRead()
		c.rearmLocked()
	}
}

func (c *Conn) deliver(m *message) {
	defer func() {
		if r := recover(); r != nil {
			c.closeWith(newPanicError(r))
		}
	}()
	c.h.onMessage(c, m.bytes())
}

func (c *Conn) callOpen() {
	defer func() {
		if r := recover(); r != nil {
			c.closeWith(newPanicError(r))
		}
	}()
	c.h.onOpen(c)
}

func (c *Conn) callClose(err error) {
	if c.h.onClose == nil {
		return
	}
	defer func() { _ = recover() }() // the connection is gone already
	c.h.onClose(c, err)
}

// ---------------------------------------------------------------------------
// Closing and timers
// ---------------------------------------------------------------------------

// closeWith closes the connection from this side for reason err: the messages
// not handled yet are dropped and the output is flushed.
func (c *Conn) closeWith(err error) {
	c.mu.Lock()
	if c.state&(csClosing|csClosed) != 0 {
		c.mu.Unlock()
		return
	}
	c.state |= csClosing
	c.quit.Store(true)
	c.recordLocked(err)
	dropped := c.dropLocked()
	c.rearmLocked()
	c.mu.Unlock()
	release(dropped)
	_ = c.raw.Close()
}

// shutdown ends the connection for Server.Shutdown: no more input is taken,
// the messages already received are handled and their replies flushed, and
// then it closes.
func (c *Conn) shutdown() {
	c.mu.Lock()
	if c.state&(csInputDone|csClosing|csClosed) != 0 {
		c.mu.Unlock()
		return
	}
	c.state |= csInputDone
	c.recordLocked(ErrServerClosed)
	if c.state&csRunning != 0 {
		c.rearmLocked()
		c.mu.Unlock()
		return // the drain closes it when done
	}
	c.state |= csClosing
	c.quit.Store(true)
	c.rearmLocked()
	c.mu.Unlock()
	_ = c.raw.Close()
}

// dropLocked takes the queued messages out, for the caller to release.
func (c *Conn) dropLocked() []message {
	q := c.queue
	c.queue = nil
	for i := range q {
		c.pending -= len(q[i].data)
	}
	return q
}

func (c *Conn) recordLocked(err error) {
	if c.state&csErrSet == 0 {
		c.state |= csErrSet
		c.closeErr = err
	}
}

// rearmLocked points the close deadline at what the connection waits for.
// Only the peer's slowness is timed: a partial message from its first byte
// (ReadTimeout), and silence once everything received is handled
// (IdleTimeout). Time spent in the callbacks, or paused by backpressure, is
// never counted against the peer.
func (c *Conn) rearmLocked() {
	var d int64
	switch st := c.state; {
	case st&(csInputDone|csClosing|csClosed) != 0:
	case st&csPartial != 0:
		if st&csPaused == 0 && c.h.readTimeout > 0 {
			d = c.since + c.h.readTimeout
		}
	case st&csRunning != 0:
	case c.h.idleTimeout > 0:
		d = c.since + c.h.idleTimeout
	}
	switch {
	case d != 0:
		c.state |= csArmed
		c.raw.SetCloseDeadline(time.Unix(0, d))
	case c.state&csArmed != 0:
		c.state &^= csArmed
		c.raw.SetCloseDeadline(time.Time{})
	}
}
