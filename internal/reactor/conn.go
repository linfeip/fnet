package reactor

import (
	"errors"
	"io"
	"net"
	"net/netip"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/linfeip/fnet/internal/bufpool"
	"github.com/linfeip/fnet/internal/netpoll"
)

// ErrWriteBufferFull is returned by Write when the connection's outbound queue
// would exceed its cap (Config.MaxOutbound) because the peer is not reading.
var ErrWriteBufferFull = errors.New("fnet: outbound write buffer full")

// errHandlerOwned is returned by Read while the input is delivered to a Handler.
var errHandlerOwned = errors.New("fnet: connection input is owned by its event handler")

const (
	// DefaultMaxOutbound is the per-connection cap on queued output when
	// Config.MaxOutbound is 0.
	DefaultMaxOutbound = 16 << 20
	// A blocking writer (see Detach) waits while highWatermark bytes are
	// queued, until they drain to lowWatermark: the queue plays the part of a
	// socket send buffer.
	highWatermark = 64 << 10
	lowWatermark  = 16 << 10
	// A blocking reader that falls maxBlockingInput bytes behind stops the
	// loop from reading the socket, so TCP flow control pushes back on the
	// peer; reading resumes once it catches up to a quarter of that.
	maxBlockingInput = 256 << 10
	compactAfter     = 4 << 10
	// Corked holds at most maxCorked bytes, for at most maxCorkDelay.
	maxCorked    = 64 << 10
	maxCorkDelay = time.Millisecond
)

// Conn state bits.
const (
	stClosed     uint32 = 1 << iota // torn down; the fd is (being) closed
	stDraining                      // Close was called: no more writes, close once output is flushed
	stLinger                        // drained and half-closed: waiting for the peer to finish
	stPeerDone                      // the peer finished sending
	stWriteArmed                    // write readiness is registered with the poller
	stPausedIn                      // PauseRead
	stPausedBuf                     // a blocking reader is maxBlockingInput behind
	stCorked                        // writes are held for Corked to send together
	stPaused     = stPausedIn | stPausedBuf
)

// Handler is the protocol bound to a Conn. Both methods run on the Conn's
// event-loop goroutine and must not block.
type Handler interface {
	// OnData receives inbound bytes while no blocking reader owns the input.
	// data is only valid during the call. OnData returns how many bytes it
	// consumed; the rest is kept and offered again, followed by newer bytes.
	// A handler that calls Detach must return 0 and not touch data afterwards.
	OnData(c *Conn, data []byte) int
	// OnClose is called exactly once when the connection is torn down.
	OnClose(c *Conn, err error)
}

// EOFHandler is implemented by a Handler that serves half-closed connections.
// When the peer finishes sending, OnEOF receives the input OnData left
// unconsumed, instead of the connection being closed; output keeps flowing
// until the handler closes the connection. OnEOF runs once, on the event loop,
// and must not block.
type EOFHandler interface {
	OnEOF(c *Conn, rest []byte)
}

// Conn is one TCP connection owned by an event loop. Its input is in one of two
// modes: delivered to a Handler on the loop (the idle, zero-goroutine mode), or
// Detached to a blocking reader on a worker goroutine, with net.Conn semantics.
// Output written from any goroutine goes straight to the socket and is only
// queued, then flushed by the loop, when the kernel buffer is full. Conn
// implements net.Conn.
type Conn struct {
	fd    int
	loop  *Loop
	ln    *Listener
	raddr netip.AddrPort
	state atomic.Uint32

	// Close deadline, guarded by loop.wheel.mu.
	tslot        int32 // wheel slot + 1; 0 when no deadline is set
	tprev, tnext *Conn
	tdeadline    int64

	// blocking is set while a blocking reader owns the input.
	blocking atomic.Pointer[blocking]

	// Inbound side.
	mu       sync.Mutex
	handler  Handler
	peerEOF  bool // the peer finished sending
	eofDone  bool // an EOFHandler was told about peerEOF
	in       []byte
	inOff    int
	inSince  int64 // unix nanos when the retained input started to accumulate
	closeErr error // why the connection closed: passed to OnClose, returned by Read

	// Outbound side. wmu also serialises direct socket writes.
	wmu        sync.Mutex
	out        *bufpool.Buffer
	outR, outW int
	werr       error
	wdeadline  int64
}

// blocking is the state of a connection whose input a blocking reader owns,
// from Detach to Attach. Read waits for input and Write waits while the
// output queue is full, as on a blocking socket. It lives only while a worker
// serves the connection, so idle connections do not pay for it.
type blocking struct {
	readable  sync.Cond // Read waits here; L is the Conn's mu
	writable  sync.Cond // Write waits here; L is the Conn's wmu
	rdeadline int64     // guarded by mu
	onEnd     func()    // guarded by mu; see NotifyInputEnd
	writing   bool      // guarded by wmu: a write is in progress
}

func newConn(fd int, l *Loop, ln *Listener, raddr netip.AddrPort, h Handler) *Conn {
	return &Conn{fd: fd, loop: l, ln: ln, raddr: raddr, handler: h}
}

// Fd returns the connection's file descriptor; it is stable for the life of
// the Conn and suitable as a worker-affinity key.
func (c *Conn) Fd() int { return c.fd }

func (c *Conn) setState(bit uint32) {
	for {
		s := c.state.Load()
		if s&bit != 0 || c.state.CompareAndSwap(s, s|bit) {
			return
		}
	}
}

// clearState clears bit and returns the state it left behind, or ok=false if
// bit was already clear.
func (c *Conn) clearState(bit uint32) (uint32, bool) {
	for {
		s := c.state.Load()
		if s&bit == 0 {
			return s, false
		}
		if c.state.CompareAndSwap(s, s&^bit) {
			return s &^ bit, true
		}
	}
}

// ---------------------------------------------------------------------------
// Input
// ---------------------------------------------------------------------------

// Detach hands the input to blocking readers: OnData is no longer called,
// inbound bytes are buffered for Read, and writes block while the output
// queue is full. It cancels the close deadline, since the reader's own
// deadlines apply from now on. Call it from OnData.
func (c *Conn) Detach() {
	b := &blocking{}
	b.readable.L = &c.mu
	b.writable.L = &c.wmu
	c.blocking.Store(b)
	c.SetCloseDeadline(time.Time{})
}

// Attach makes h the connection's handler and returns the input to it. Bytes
// already buffered are offered to h on the event loop. If the connection is
// already closed, h.OnClose is called instead.
func (c *Conn) Attach(h Handler) {
	c.mu.Lock()
	if c.state.Load()&stClosed != 0 {
		err := c.closeErr
		c.mu.Unlock()
		c.loop.post(task{kind: taskNotify, c: c, h: h, err: err})
		return
	}
	c.handler = h
	b := c.blocking.Swap(nil)
	if b != nil {
		b.onEnd = nil
	}
	if c.inOff == len(c.in) {
		c.in, c.inOff = nil, 0 // idle connections keep no input buffer
	} else {
		c.inSince = time.Now().UnixNano() // the handler's clock starts now
	}
	resume := len(c.in) > 0 || c.peerEOF
	c.mu.Unlock()
	if b != nil {
		c.wmu.Lock()
		b.writable.Broadcast() // a writer still waiting carries on unblocked
		c.wmu.Unlock()
	}
	if _, ok := c.clearState(stPausedBuf); ok || resume {
		c.loop.post(task{kind: taskResume, c: c})
	}
}

// NotifyInputEnd arranges for f to run once the input of a detached
// connection ends: the peer finished sending, or the connection closed. If it
// has ended already, f runs at once. f runs on whichever goroutine notices and
// must not block (canceling a context is typical). Attach, and a nil f, drop
// it.
func (c *Conn) NotifyInputEnd(f func()) {
	c.mu.Lock()
	b := c.blocking.Load()
	if b == nil || f == nil {
		if b != nil {
			b.onEnd = nil
		}
		c.mu.Unlock()
		return
	}
	ended := c.peerEOF || c.state.Load()&(stClosed|stDraining) != 0
	if !ended {
		b.onEnd = f
	}
	c.mu.Unlock()
	if ended {
		f()
	}
}

// takeEndLocked removes the NotifyInputEnd callback, for the caller to run
// after unlocking.
func (c *Conn) takeEndLocked() func() {
	b := c.blocking.Load()
	if b == nil {
		return nil
	}
	f := b.onEnd
	b.onEnd = nil
	return f
}

// NewBytes reports, inside OnData, how many bytes at the end of data arrived
// since the previous OnData call; the rest was offered before and left
// unconsumed. An incremental parser need only look at those.
func (c *Conn) NewBytes() int { return c.loop.fresh }

// Detached reports whether a blocking reader owns the input.
func (c *Conn) Detached() bool { return c.blocking.Load() != nil }

// Handler returns the connection's handler; nil once it is closed.
func (c *Conn) Handler() Handler {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.handler
}

// Unread pushes b back in front of the buffered input.
func (c *Conn) Unread(b []byte) {
	if len(b) == 0 {
		return
	}
	c.mu.Lock()
	if c.inOff >= len(b) {
		c.inOff -= len(b)
		copy(c.in[c.inOff:], b)
	} else {
		in := make([]byte, 0, len(b)+len(c.in)-c.inOff)
		in = append(in, b...)
		c.in = append(in, c.in[c.inOff:]...)
		c.inOff = 0
	}
	c.mu.Unlock()
}

// SetCloseDeadline makes the event loop close the connection gracefully at t,
// unless it is moved first; a zero t cancels it. It guards input owned by the
// handler: Detach cancels it, and it is ignored while a blocking reader owns
// the connection.
func (c *Conn) SetCloseDeadline(t time.Time) { c.setDeadline(unixNano(t)) }

// setDeadline arms the close deadline. A loop whose wheel was empty may be
// asleep for up to pollTimeout, so it is only woken for a deadline sooner than
// that: a later one is on the wheel when the loop next wakes. Most deadlines
// (a request header's, a keep-alive's) are seconds away, and a wake costs a
// system call.
func (c *Conn) setDeadline(d int64) {
	if c.loop != nil && c.loop.wheel.set(c, d) && d-time.Now().UnixNano() < int64(pollTimeout) {
		c.loop.wake()
	}
}

// Buffered reports how many input bytes are waiting to be read or handled.
func (c *Conn) Buffered() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.in) - c.inOff
}

// InputSince reports when the oldest input byte not yet consumed by the
// handler arrived. Inside OnData with nothing retained, that is now.
func (c *Conn) InputSince() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.inOff < len(c.in) {
		return time.Unix(0, c.inSince)
	}
	return time.Now()
}

// expire runs on the loop when the close deadline passes.
func (c *Conn) expire() {
	switch st := c.state.Load(); {
	case st&stClosed != 0:
	case st&stLinger != 0:
		c.abort(nil) // the output is delivered; the peer just never finished
	case st&stDraining != 0:
		c.abort(os.ErrDeadlineExceeded) // the peer stopped taking our output
	case c.blocking.Load() == nil:
		c.shutdown(os.ErrDeadlineExceeded)
	}
}

// PauseRead stops reading from the socket until ResumeRead, letting TCP flow
// control push back on the peer.
func (c *Conn) PauseRead() { c.setState(stPausedIn) }

// ResumeRead undoes PauseRead.
func (c *Conn) ResumeRead() { c.unpause(stPausedIn) }

func (c *Conn) unpause(bit uint32) {
	if s, ok := c.clearState(bit); ok && s&stPaused == 0 {
		c.loop.post(task{kind: taskResume, c: c})
	}
}

// signalLocked wakes blocking readers to look at the input again.
func (c *Conn) signalLocked() {
	if b := c.blocking.Load(); b != nil {
		b.readable.Broadcast()
	}
}

func (c *Conn) appendLocked(b []byte) {
	if c.inOff > 0 {
		unread := len(c.in) - c.inOff
		if unread == 0 {
			c.in, c.inOff = c.in[:0], 0
		} else if c.inOff > compactAfter || c.inOff >= unread {
			copy(c.in, c.in[c.inOff:])
			c.in, c.inOff = c.in[:unread], 0
		}
	}
	c.in = append(c.in, b...)
}

// retainLocked keeps input the handler has not consumed yet.
func (c *Conn) retainLocked(b []byte) {
	if c.inOff == len(c.in) {
		c.inSince = time.Now().UnixNano()
	}
	c.appendLocked(b)
}

func (c *Conn) consumeLocked(n int) {
	c.inOff += n
	unread := len(c.in) - c.inOff
	if unread <= 0 {
		if c.blocking.Load() != nil {
			c.in, c.inOff = c.in[:0], 0 // an active reader will want the space again
		} else {
			c.in, c.inOff = nil, 0
		}
		return
	}
	if c.inOff > compactAfter && c.inOff >= unread {
		copy(c.in, c.in[c.inOff:])
		c.in, c.inOff = c.in[:unread], 0
	}
}

// deliver hands bytes just read from the socket to the handler or the blocking
// reader. Loop goroutine only.
func (c *Conn) deliver(data []byte) {
	c.mu.Lock()
	if c.state.Load()&(stClosed|stDraining) != 0 {
		c.mu.Unlock()
		return
	}
	h := c.handler
	if b := c.blocking.Load(); b != nil || h == nil {
		c.appendLocked(data)
		if b != nil {
			b.readable.Broadcast()
			if len(c.in)-c.inOff >= maxBlockingInput {
				c.setState(stPausedBuf) // the reader fell behind: let TCP push back
			}
		}
		c.mu.Unlock()
		return
	}
	if c.inOff < len(c.in) {
		c.retainLocked(data)
		c.mu.Unlock()
		c.offer(len(data))
		return
	}
	// Nothing retained: the handler reads straight out of the loop's buffer and
	// only an unconsumed tail is copied.
	c.mu.Unlock()
	c.loop.fresh = len(data)
	n := h.OnData(c, data)
	if n < len(data) {
		c.mu.Lock()
		if c.state.Load()&stClosed == 0 {
			c.retainLocked(data[n:])
			c.signalLocked()
		}
		c.mu.Unlock()
	}
}

// offer passes the retained input to the handler, of which the last fresh
// bytes are new (-1: all of it). Loop goroutine only.
func (c *Conn) offer(fresh int) {
	c.mu.Lock()
	h := c.handler
	if c.blocking.Load() != nil || h == nil || c.inOff == len(c.in) || c.state.Load()&(stClosed|stDraining) != 0 {
		c.mu.Unlock()
		return
	}
	view := c.in[c.inOff:]
	c.mu.Unlock()
	if fresh < 0 {
		fresh = len(view)
	}
	c.loop.fresh = fresh
	if n := h.OnData(c, view); n > 0 {
		c.mu.Lock()
		if c.blocking.Load() == nil && c.state.Load()&stClosed == 0 {
			c.consumeLocked(n)
		}
		c.mu.Unlock()
	}
}

// onPeerEOF records that the peer finished sending. A detached reader drains
// the input and decides when to close (half-close); otherwise the handler is
// told.
func (c *Conn) onPeerEOF() {
	c.mu.Lock()
	c.peerEOF = true
	c.signalLocked()
	detached := c.blocking.Load() != nil
	end := c.takeEndLocked()
	c.mu.Unlock()
	if end != nil {
		end()
	}
	if !detached {
		c.handlerEOF()
	}
}

// handlerEOF ends the input of a handler-owned connection. An EOFHandler gets
// the unconsumed input, once, and decides when to close; for any other handler
// the connection is done. Loop goroutine only.
func (c *Conn) handlerEOF() {
	c.mu.Lock()
	eh, ok := c.handler.(EOFHandler)
	if !ok {
		c.mu.Unlock()
		c.abort(io.EOF)
		return
	}
	if c.eofDone || c.state.Load()&stClosed != 0 {
		c.mu.Unlock()
		return
	}
	c.eofDone = true
	rest := c.in[c.inOff:]
	c.in, c.inOff = nil, 0 // handed over: no more input will follow it
	c.mu.Unlock()
	eh.OnEOF(c, rest)
}

// handlerSawEOF reports whether the handler owns an input that has ended.
func (c *Conn) handlerSawEOF() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.peerEOF && c.blocking.Load() == nil
}

// Read implements net.Conn for a detached connection. It blocks until input
// arrives, the peer finishes, the connection closes, or the deadline passes.
// After Close it fails with net.ErrClosed, or with why the connection closed.
func (c *Conn) Read(b []byte) (int, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for {
		if c.state.Load()&(stClosed|stDraining) != 0 {
			if c.closeErr != nil {
				return 0, c.closeErr
			}
			return 0, net.ErrClosed
		}
		bl := c.blocking.Load()
		if bl == nil {
			return 0, errHandlerOwned
		}
		if c.inOff < len(c.in) {
			n := copy(b, c.in[c.inOff:])
			c.consumeLocked(n)
			if c.state.Load()&stPausedBuf != 0 && len(c.in)-c.inOff <= maxBlockingInput/4 {
				c.unpause(stPausedBuf)
			}
			return n, nil
		}
		if c.peerEOF {
			return 0, io.EOF
		}
		if bl.rdeadline == 0 {
			bl.readable.Wait()
			continue
		}
		remain := time.Duration(bl.rdeadline - time.Now().UnixNano())
		if remain <= 0 {
			return 0, os.ErrDeadlineExceeded
		}
		t := time.AfterFunc(remain, func() {
			c.mu.Lock()
			bl.readable.Broadcast()
			c.mu.Unlock()
		})
		bl.readable.Wait()
		t.Stop()
	}
}

// ---------------------------------------------------------------------------
// Output
// ---------------------------------------------------------------------------

func (c *Conn) writableLocked() error {
	if c.state.Load()&(stClosed|stDraining) != 0 {
		return net.ErrClosed
	}
	if c.werr != nil {
		return c.werr
	}
	if c.wdeadline > 0 && time.Now().UnixNano() > c.wdeadline {
		return os.ErrDeadlineExceeded
	}
	return nil
}

// reserveLocked makes room for n more queued bytes and returns the slice to
// fill, or ErrWriteBufferFull. An empty queue takes any n: a write that the
// kernel accepted in part always queues the rest, so a message is never cut
// short on the wire, and one message larger than the cap still gets through.
func (c *Conn) reserveLocked(n int) ([]byte, error) {
	if pending := c.outW - c.outR; pending > 0 && pending+n > c.maxOutbound() {
		return nil, ErrWriteBufferFull
	}
	return c.growLocked(n), nil
}

// growLocked appends room for n bytes to the queue and returns it.
func (c *Conn) growLocked(n int) []byte {
	pending := c.outW - c.outR
	if c.out == nil {
		c.out = bufpool.Get(max(n, 4<<10))
		c.outR, c.outW = 0, 0
	}
	buf := c.out.B[:cap(c.out.B)]
	if len(buf)-c.outW < n && c.outR > 0 { // compact before growing
		copy(buf, buf[c.outR:c.outW])
		c.outR, c.outW = 0, pending
	}
	if len(buf)-c.outW < n {
		nb := bufpool.Get(pending + n)
		copy(nb.B[:cap(nb.B)], buf[c.outR:c.outW])
		bufpool.Put(c.out)
		c.out = nb
		c.outR, c.outW = 0, pending
		buf = nb.B[:cap(nb.B)]
	}
	c.outW += n
	return buf[c.outW-n : c.outW]
}

func (c *Conn) maxOutbound() int {
	if c.loop == nil {
		return DefaultMaxOutbound
	}
	return c.loop.eng.maxOutbound
}

func (c *Conn) releaseOutLocked() {
	bufpool.Put(c.out)
	c.out = nil
	c.outR, c.outW = 0, 0
}

// Write implements net.Conn; b is not referenced after it returns. While a
// Handler owns the connection, Write never blocks: bytes the kernel does not
// take right away are queued, up to the engine's MaxOutbound, and flushed by
// the event loop. While a blocking reader owns it (see Detach), Write blocks
// like one on a blocking socket.
func (c *Conn) Write(b []byte) (int, error) { return c.Writev([][]byte{b}) }

// Writev writes several buffers as one unit, with one writev(2) when nothing
// is queued. It blocks or not like Write.
func (c *Conn) Writev(iovs [][]byte) (int, error) {
	total := 0
	for _, b := range iovs {
		total += len(b)
	}
	if total == 0 {
		return 0, nil
	}
	c.wmu.Lock()
	var (
		n   int
		arm bool
		err error
	)
	if bl := c.blocking.Load(); bl != nil {
		n, err = c.writeBlockingLocked(bl, iovs)
	} else {
		n, arm, err = c.writeLocked(iovs, total)
	}
	c.wmu.Unlock()
	if arm {
		c.armWrite()
	}
	return n, err
}

// writeLocked writes for a handler-owned connection, without blocking: what
// the kernel does not take at once is queued for the loop to flush, which arm
// asks for. While corked, the bytes are held instead, up to corkLimit.
func (c *Conn) writeLocked(iovs [][]byte, total int) (n int, arm bool, err error) {
	if err := c.writableLocked(); err != nil {
		return 0, false, err
	}
	if c.state.Load()&stCorked != 0 {
		if c.outW-c.outR+total <= c.corkLimit() {
			copyIovs(c.growLocked(total), iovs, 0)
			return total, false, nil
		}
		// Too much to hold: what is held goes first, then this write as usual.
		if arm, err = c.flushLocked(); err != nil {
			return 0, false, err
		}
	}
	if c.outW == c.outR && c.fd >= 0 {
		if n, err = netpoll.Writev(c.fd, iovs); n == total {
			return n, false, nil
		}
		if err != nil && !netpoll.IsAgain(err) {
			c.werr = err
			return n, false, err
		}
	}
	dst, err := c.reserveLocked(total - n)
	if err != nil {
		return n, arm, err
	}
	copyIovs(dst, iovs, n)
	return total, true, nil
}

// copyIovs copies iovs, but for their first skip bytes, into dst.
func copyIovs(dst []byte, iovs [][]byte, skip int) {
	for _, b := range iovs {
		if skip >= len(b) {
			skip -= len(b)
			continue
		}
		dst = dst[copy(dst, b[skip:]):]
		skip = 0
	}
}

// writeBlockingLocked writes iovs for a blocking writer. What the kernel does
// not take at once is queued, but never beyond highWatermark: a writer to a
// slow peer waits for the loop to drain the queue, like one on a blocking
// socket. Writers take turns, so the bytes of one call are never interleaved
// with another's.
func (c *Conn) writeBlockingLocked(bl *blocking, iovs [][]byte) (int, error) {
	for bl.writing {
		bl.writable.Wait()
	}
	bl.writing = true
	defer func() {
		bl.writing = false
		bl.writable.Broadcast()
	}()
	if err := c.writableLocked(); err != nil {
		return 0, err
	}
	written := 0
	if c.outW == c.outR && c.fd >= 0 {
		var err error
		if written, err = netpoll.Writev(c.fd, iovs); err != nil && !netpoll.IsAgain(err) {
			c.werr = err
			return written, err
		}
	}
	skip := written
	for _, p := range iovs {
		if skip >= len(p) {
			skip -= len(p)
			continue
		}
		p, skip = p[skip:], 0
		for len(p) > 0 {
			if err := c.waitRoomLocked(bl); err != nil {
				return written, err
			}
			if c.outW == c.outR && c.fd >= 0 {
				n, err := netpoll.Write(c.fd, p)
				written += n
				p = p[n:]
				if err != nil && !netpoll.IsAgain(err) {
					c.werr = err
					return written, err
				}
				if len(p) == 0 {
					break
				}
			}
			n := min(len(p), max(highWatermark-(c.outW-c.outR), lowWatermark))
			copy(c.growLocked(n), p[:n])
			written += n
			p = p[n:]
			c.armWrite()
		}
	}
	return written, nil
}

// waitRoomLocked waits until a blocking writer may queue more output: the
// queue is below highWatermark, or has drained to lowWatermark. It fails when
// the connection closes, the write deadline passes, or the peer takes nothing
// for drainStall.
func (c *Conn) waitRoomLocked(bl *blocking) error {
	if err := c.writableLocked(); err != nil || c.outW-c.outR < highWatermark {
		return err
	}
	last, since := c.outW-c.outR, time.Now()
	for {
		if err := c.writableLocked(); err != nil {
			return err
		}
		pending := c.outW - c.outR
		if pending <= lowWatermark || c.blocking.Load() != bl {
			return nil // drained, or attached to a handler meanwhile
		}
		now := time.Now()
		if pending < last {
			last, since = pending, now
		} else if now.Sub(since) >= drainStall {
			return os.ErrDeadlineExceeded // the peer stopped reading
		}
		wait := drainStall - now.Sub(since)
		if c.wdeadline > 0 {
			wait = min(wait, time.Duration(c.wdeadline-now.UnixNano()))
		}
		t := time.AfterFunc(wait, func() {
			c.wmu.Lock()
			bl.writable.Broadcast()
			c.wmu.Unlock()
		})
		bl.writable.Wait()
		t.Stop()
	}
}

// wakeWritersLocked wakes blocking writers to look at the queue again.
func (c *Conn) wakeWritersLocked() {
	if b := c.blocking.Load(); b != nil {
		b.writable.Broadcast()
	}
}

func (c *Conn) armWrite() {
	if c.loop == nil {
		return
	}
	for {
		s := c.state.Load()
		if s&(stClosed|stWriteArmed) != 0 {
			return
		}
		if c.state.CompareAndSwap(s, s|stWriteArmed) {
			_ = c.loop.poller.EnableWrite(c.fd)
			return
		}
	}
}

func (c *Conn) hasPendingOutput() bool {
	c.wmu.Lock()
	defer c.wmu.Unlock()
	return c.outW > c.outR
}

// flush writes queued output to the socket for the loop. It reports whether
// output is still queued.
func (c *Conn) flush() (bool, error) {
	c.wmu.Lock()
	defer c.wmu.Unlock()
	return c.flushLocked()
}

// flushLocked writes queued output until the kernel takes no more, and reports
// whether some is left.
func (c *Conn) flushLocked() (bool, error) {
	for c.outR < c.outW && c.fd >= 0 {
		n, err := netpoll.Write(c.fd, c.out.B[c.outR:c.outW])
		c.outR += n
		if err != nil {
			if netpoll.IsAgain(err) {
				break
			}
			c.werr = err
			c.wakeWritersLocked()
			return false, err
		}
		if n == 0 {
			break
		}
	}
	pending := c.outW - c.outR
	if pending == 0 {
		c.releaseOutLocked()
	}
	if pending <= lowWatermark {
		c.wakeWritersLocked()
	}
	return pending > 0, nil
}

// Corked runs fn with the connection's output corked: writes from any
// goroutine are held and leave together, in one write, when fn returns, so
// the replies to a burst of messages share one system call. A cork holds at
// most maxCorked bytes (and never more than MaxOutbound), for at most
// maxCorkDelay: if fn runs longer, a slow handler say, what is held goes out
// then and later writes go out as they come, so neither the replies written
// before it nor another goroutine's writes (a broadcast) wait for fn. Only a
// handler-owned connection is corked; a blocking writer (see Detach) is not.
func (c *Conn) Corked(fn func()) {
	k := corkTimers.Get().(*corkTimer)
	k.c.Store(c)
	c.setState(stCorked)
	k.t.Reset(maxCorkDelay)
	defer func() {
		k.t.Stop()
		k.c.Store(nil)
		corkTimers.Put(k)
		c.uncork()
	}()
	fn()
}

func (c *Conn) corkLimit() int { return min(maxCorked, c.maxOutbound()) }

// uncork lifts the cork and sends what it held, unless the loop owns the
// queue already (the kernel buffer was full, or a Close is draining it).
func (c *Conn) uncork() {
	c.wmu.Lock()
	arm := false
	if _, ok := c.clearState(stCorked); ok && c.state.Load()&(stClosed|stWriteArmed) == 0 {
		arm, _ = c.flushLocked() // a failure stays in werr for the next write
	}
	c.wmu.Unlock()
	if arm {
		c.armWrite()
	}
}

// corkTimer lifts a cork that outlives maxCorkDelay. They are pooled: a timer
// that fires late, for a cork already lifted, lifts at most another one early,
// which costs that one some coalescing and nothing else.
type corkTimer struct {
	t *time.Timer
	c atomic.Pointer[Conn]
}

var corkTimers = sync.Pool{New: func() any {
	k := new(corkTimer)
	k.t = time.AfterFunc(time.Hour, func() {
		if c := k.c.Load(); c != nil {
			c.uncork()
		}
	})
	k.t.Stop()
	return k
}}

// ---------------------------------------------------------------------------
// Close
// ---------------------------------------------------------------------------

// Close implements net.Conn. Further writes fail and reads fail with
// net.ErrClosed at once; output already queued is flushed before the socket
// is closed.
func (c *Conn) Close() error {
	c.shutdown(nil)
	return nil
}

// shutdown closes the connection once its queued output is flushed and records
// err for OnClose. A peer that stops reading cannot hold the flush open: it
// ends after drainStall without progress. Like a kernel send buffer after
// close(2), the flush ignores the write deadline, which only governs Write
// calls (tls.Conn.Close moves it to "now" before closing the connection).
func (c *Conn) shutdown(err error) {
	c.wmu.Lock()
	if c.state.Load()&(stClosed|stDraining) != 0 {
		c.wmu.Unlock()
		return
	}
	c.setState(stDraining)
	pending := c.outW > c.outR
	c.wakeWritersLocked() // blocked writers fail from now on
	c.wmu.Unlock()

	c.mu.Lock()
	c.closeErr = err
	c.signalLocked()
	end := c.takeEndLocked()
	c.mu.Unlock()
	if end != nil {
		end()
	}
	switch {
	case c.loop == nil:
		c.abort(err)
	case pending:
		c.setDeadline(c.drainDeadline())
		c.armWrite() // the loop finishes the close once the queue drains
	default:
		c.loop.post(task{kind: taskDrained, c: c})
	}
}

// drainDeadline is when a draining connection that makes no further progress
// is dropped.
func (c *Conn) drainDeadline() int64 {
	return time.Now().UnixNano() + int64(drainStall)
}

// drained finishes closing a connection whose output is flushed. If the peer
// has finished sending too, the socket closes at once. Otherwise the sending
// side is shut down, so the peer reads EOF right after the last byte, and the
// peer gets lingerTimeout to finish while whatever it still sends is read and
// dropped: closing a socket with unread input sends a reset, which can destroy
// output the peer has not read yet. Loop goroutine only.
func (c *Conn) drained() {
	switch st := c.state.Load(); {
	case st&(stClosed|stLinger) != 0:
	case st&stPeerDone != 0:
		c.abort(nil)
	default:
		_ = netpoll.CloseWrite(c.fd)
		c.setState(stLinger)
		c.loop.wheel.set(c, time.Now().Add(lingerTimeout).UnixNano())
		c.loop.onReadable(c, true) // the peer's EOF may be waiting already
	}
}

// abort tears the connection down immediately, dropping queued output. The fd
// is closed and the handler notified on the event loop, so an fd number is
// never reused while an event batch that references it is in flight.
func (c *Conn) abort(err error) {
	c.mu.Lock()
	if c.state.Load()&stClosed != 0 {
		c.mu.Unlock()
		return
	}
	c.setState(stClosed)
	if err == nil {
		err = c.closeErr // the reason shutdown recorded
	}
	c.closeErr = err
	h := c.handler
	c.handler = nil
	c.in, c.inOff = nil, 0
	c.signalLocked()
	end := c.takeEndLocked()
	c.mu.Unlock()

	// Wait out any writer inside a syscall on c.fd, then drop the queue.
	c.wmu.Lock()
	c.releaseOutLocked()
	c.wakeWritersLocked()
	c.wmu.Unlock()
	if end != nil {
		end()
	}

	if c.loop == nil {
		return
	}
	c.loop.wheel.set(c, 0)
	c.loop.eng.table.delete(c.fd, c)
	c.loop.post(task{kind: taskClose, c: c, h: h, err: err})
}

// ---------------------------------------------------------------------------
// net.Conn plumbing
// ---------------------------------------------------------------------------

// LocalAddr implements net.Conn: the address the peer connected to.
func (c *Conn) LocalAddr() net.Addr {
	var ln *net.TCPAddr
	if c.ln != nil {
		ln, _ = c.ln.Addr.(*net.TCPAddr)
	}
	if ln != nil && !ln.IP.IsUnspecified() {
		return ln // a listener bound to one address accepts connections to it alone
	}
	c.wmu.Lock() // keeps the fd from being closed underneath getsockname
	defer c.wmu.Unlock()
	if c.state.Load()&stClosed == 0 {
		if ap, err := netpoll.LocalAddr(c.fd); err == nil {
			return net.TCPAddrFromAddrPort(ap)
		}
	}
	if ln != nil {
		return ln
	}
	return &net.TCPAddr{}
}

// RemoteAddr implements net.Conn.
func (c *Conn) RemoteAddr() net.Addr { return net.TCPAddrFromAddrPort(c.raddr) }

// RemoteAddrString returns the peer address as text.
func (c *Conn) RemoteAddrString() string { return c.raddr.String() }

// SetDeadline implements net.Conn.
func (c *Conn) SetDeadline(t time.Time) error {
	_ = c.SetReadDeadline(t)
	return c.SetWriteDeadline(t)
}

// SetReadDeadline implements net.Conn for a detached connection. It has no
// effect while a Handler owns the input; SetCloseDeadline bounds that.
func (c *Conn) SetReadDeadline(t time.Time) error {
	c.mu.Lock()
	if b := c.blocking.Load(); b != nil {
		b.rdeadline = unixNano(t)
		b.readable.Broadcast()
	}
	c.mu.Unlock()
	return nil
}

// SetWriteDeadline implements net.Conn. Writes issued after the deadline
// fail, and so does a blocking write still waiting when it passes.
func (c *Conn) SetWriteDeadline(t time.Time) error {
	c.wmu.Lock()
	c.wdeadline = unixNano(t)
	c.wakeWritersLocked()
	c.wmu.Unlock()
	return nil
}

func unixNano(t time.Time) int64 {
	if t.IsZero() {
		return 0
	}
	return t.UnixNano()
}
