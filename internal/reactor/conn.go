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
// would exceed maxOutbound because the peer is not reading.
var ErrWriteBufferFull = errors.New("fnet: outbound write buffer full")

// errHandlerOwned is returned by Read while the input is delivered to a Handler.
var errHandlerOwned = errors.New("fnet: connection input is owned by its event handler")

const (
	maxOutbound   = 16 << 20 // per-connection cap on queued output
	highWatermark = 64 << 10 // queued output above this pauses reading
	lowWatermark  = 16 << 10 // ...and reading resumes once it drains below this
	compactAfter  = 4 << 10
)

// Conn state bits.
const (
	stClosed     uint32 = 1 << iota // torn down; the fd is (being) closed
	stDraining                      // Close was called: no more writes, close once output is flushed
	stWriteArmed                    // write readiness is registered with the poller
	stPausedIn                      // PauseRead
	stPausedOut                     // outbound queue above highWatermark
	stPaused     = stPausedIn | stPausedOut
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

// Conn is one TCP connection owned by an event loop. Its input is in one of two
// modes: delivered to a Handler on the loop (the idle, zero-goroutine mode), or
// Detached and buffered for blocking Read calls from a worker goroutine. Output
// written from any goroutine goes straight to the socket and is only queued,
// then flushed by the loop, when the kernel buffer is full. Conn implements
// net.Conn.
type Conn struct {
	fd    int
	loop  *Loop
	ln    *Listener
	raddr netip.AddrPort
	rstr  atomic.Pointer[string] // cached raddr.String()
	state atomic.Uint32

	// Close deadline, guarded by loop.wheel.mu.
	tslot        int32 // wheel slot + 1; 0 when no deadline is set
	tprev, tnext *Conn
	tdeadline    int64

	// Inbound side.
	mu        sync.Mutex
	cond      *sync.Cond // created on the first blocking Read
	handler   Handler
	detached  bool // a blocking reader owns the input
	peerEOF   bool // the peer finished sending
	in        []byte
	inOff     int
	inSince   int64 // unix nanos when the retained input started to accumulate
	closeErr  error // why the connection closed: passed to OnClose, returned by Read
	rdeadline int64

	// Outbound side. wmu also serialises direct socket writes.
	wmu        sync.Mutex
	out        *bufpool.Buffer
	outR, outW int
	werr       error
	wdeadline  int64
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

// Detach hands the input to blocking readers: OnData is no longer called and
// inbound bytes are buffered for Read. It cancels the close deadline, since the
// reader's own deadlines apply from now on. Call it from OnData.
func (c *Conn) Detach() {
	c.mu.Lock()
	c.detached = true
	c.mu.Unlock()
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
	c.detached = false
	if c.inOff == len(c.in) {
		c.in, c.inOff = nil, 0 // idle connections keep no input buffer
	} else {
		c.inSince = time.Now().UnixNano() // the handler's clock starts now
	}
	resume := len(c.in) > 0 || c.peerEOF
	c.mu.Unlock()
	if resume {
		c.loop.post(task{kind: taskResume, c: c})
	}
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

func (c *Conn) setDeadline(d int64) {
	if c.loop != nil && c.loop.wheel.set(c, d) {
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
	st := c.state.Load()
	switch {
	case st&stClosed != 0:
	case st&stDraining != 0:
		c.abort(os.ErrDeadlineExceeded) // the peer stopped taking our output
	default:
		c.mu.Lock()
		detached := c.detached
		c.mu.Unlock()
		if !detached {
			c.shutdown(os.ErrDeadlineExceeded)
		}
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

func (c *Conn) signalLocked() {
	if c.cond != nil {
		c.cond.Broadcast()
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
		if c.detached {
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
	if c.detached || h == nil {
		c.appendLocked(data)
		c.signalLocked()
		c.mu.Unlock()
		return
	}
	if c.inOff < len(c.in) {
		c.retainLocked(data)
		c.mu.Unlock()
		c.offer()
		return
	}
	// Nothing retained: the handler reads straight out of the loop's buffer and
	// only an unconsumed tail is copied.
	c.mu.Unlock()
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

// offer passes the retained input to the handler. Loop goroutine only.
func (c *Conn) offer() {
	c.mu.Lock()
	h := c.handler
	if c.detached || h == nil || c.inOff == len(c.in) || c.state.Load()&stClosed != 0 {
		c.mu.Unlock()
		return
	}
	view := c.in[c.inOff:]
	c.mu.Unlock()
	if n := h.OnData(c, view); n > 0 {
		c.mu.Lock()
		if !c.detached && c.state.Load()&stClosed == 0 {
			c.consumeLocked(n)
		}
		c.mu.Unlock()
	}
}

// onPeerEOF records that the peer finished sending. A detached reader drains
// the input and decides when to close (half-close); otherwise the handler is
// done with the connection.
func (c *Conn) onPeerEOF() {
	c.mu.Lock()
	c.peerEOF = true
	c.signalLocked()
	detached := c.detached
	c.mu.Unlock()
	if !detached {
		c.abort(io.EOF)
	}
}

// handlerSawEOF reports whether the handler owns an input that has ended.
func (c *Conn) handlerSawEOF() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.peerEOF && !c.detached
}

// Read implements net.Conn for a detached connection. It blocks until input
// arrives, the peer finishes, the connection closes, or the deadline passes.
func (c *Conn) Read(b []byte) (int, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for {
		if c.state.Load()&(stClosed|stDraining) != 0 {
			if c.closeErr != nil && c.closeErr != io.EOF {
				return 0, c.closeErr
			}
			return 0, io.EOF
		}
		if !c.detached {
			return 0, errHandlerOwned
		}
		if c.inOff < len(c.in) {
			n := copy(b, c.in[c.inOff:])
			c.consumeLocked(n)
			return n, nil
		}
		if c.peerEOF {
			return 0, io.EOF
		}
		if c.cond == nil {
			c.cond = sync.NewCond(&c.mu)
		}
		if c.rdeadline == 0 {
			c.cond.Wait()
			continue
		}
		remain := time.Duration(c.rdeadline - time.Now().UnixNano())
		if remain <= 0 {
			return 0, os.ErrDeadlineExceeded
		}
		t := time.AfterFunc(remain, func() {
			c.mu.Lock()
			c.cond.Broadcast()
			c.mu.Unlock()
		})
		c.cond.Wait()
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
// fill, or ErrWriteBufferFull.
func (c *Conn) reserveLocked(n int) ([]byte, error) {
	pending := c.outW - c.outR
	if pending+n > maxOutbound {
		return nil, ErrWriteBufferFull
	}
	if pending+n > highWatermark {
		c.setState(stPausedOut)
	}
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
	return buf[c.outW-n : c.outW], nil
}

func (c *Conn) releaseOutLocked() {
	bufpool.Put(c.out)
	c.out = nil
	c.outR, c.outW = 0, 0
}

// Write implements net.Conn. It never blocks: bytes the kernel does not take
// right away are queued (up to maxOutbound) and flushed by the event loop.
func (c *Conn) Write(b []byte) (int, error) {
	if len(b) == 0 {
		return 0, nil
	}
	c.wmu.Lock()
	if err := c.writableLocked(); err != nil {
		c.wmu.Unlock()
		return 0, err
	}
	n := 0
	if c.outW == c.outR && c.fd >= 0 {
		var err error
		if n, err = netpoll.Write(c.fd, b); n == len(b) {
			c.wmu.Unlock()
			return n, nil
		}
		if err != nil && !netpoll.IsAgain(err) {
			c.werr = err
			c.wmu.Unlock()
			return n, err
		}
	}
	dst, err := c.reserveLocked(len(b) - n)
	if err != nil {
		c.wmu.Unlock()
		return n, err
	}
	copy(dst, b[n:])
	c.wmu.Unlock()
	c.armWrite()
	return len(b), nil
}

// Writev writes several buffers with one writev(2) when nothing is queued.
func (c *Conn) Writev(iovs [][]byte) (int, error) {
	total := 0
	for _, b := range iovs {
		total += len(b)
	}
	if total == 0 {
		return 0, nil
	}
	c.wmu.Lock()
	if err := c.writableLocked(); err != nil {
		c.wmu.Unlock()
		return 0, err
	}
	n := 0
	if c.outW == c.outR && c.fd >= 0 {
		var err error
		if n, err = netpoll.Writev(c.fd, iovs); n == total {
			c.wmu.Unlock()
			return n, nil
		}
		if err != nil && !netpoll.IsAgain(err) {
			c.werr = err
			c.wmu.Unlock()
			return n, err
		}
	}
	dst, err := c.reserveLocked(total - n)
	if err != nil {
		c.wmu.Unlock()
		return n, err
	}
	skip := n
	for _, b := range iovs {
		if skip >= len(b) {
			skip -= len(b)
			continue
		}
		dst = dst[copy(dst, b[skip:]):]
		skip = 0
	}
	c.wmu.Unlock()
	c.armWrite()
	return total, nil
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

// flush writes queued output to the socket. Loop goroutine only. It reports
// whether output is still queued.
func (c *Conn) flush() (bool, error) {
	c.wmu.Lock()
	defer c.wmu.Unlock()
	for c.outR < c.outW {
		n, err := netpoll.Write(c.fd, c.out.B[c.outR:c.outW])
		c.outR += n
		if err != nil {
			if netpoll.IsAgain(err) {
				break
			}
			c.werr = err
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
		c.unpause(stPausedOut)
	}
	return pending > 0, nil
}

// ---------------------------------------------------------------------------
// Close
// ---------------------------------------------------------------------------

// Close implements net.Conn. Further writes fail and reads report EOF at once;
// output already queued is flushed before the socket is closed.
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
	c.wmu.Unlock()

	c.mu.Lock()
	c.closeErr = err
	c.signalLocked()
	c.mu.Unlock()
	if !pending {
		c.abort(err)
		return
	}
	c.setDeadline(c.drainDeadline())
	c.armWrite() // the loop closes the socket once the queue drains
}

// drainDeadline is when a draining connection that makes no further progress
// is dropped.
func (c *Conn) drainDeadline() int64 {
	return time.Now().UnixNano() + int64(drainStall)
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
	c.mu.Unlock()

	// Wait out any writer inside a syscall on c.fd, then drop the queue.
	c.wmu.Lock()
	c.releaseOutLocked()
	c.wmu.Unlock()

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

// LocalAddr implements net.Conn.
func (c *Conn) LocalAddr() net.Addr {
	if c.ln == nil || c.ln.Addr == nil {
		return &net.TCPAddr{}
	}
	return c.ln.Addr
}

// RemoteAddr implements net.Conn.
func (c *Conn) RemoteAddr() net.Addr { return net.TCPAddrFromAddrPort(c.raddr) }

// RemoteAddrString returns the peer address as text, formatted once per
// connection.
func (c *Conn) RemoteAddrString() string {
	if s := c.rstr.Load(); s != nil {
		return *s
	}
	s := c.raddr.String()
	c.rstr.Store(&s)
	return s
}

// SetDeadline implements net.Conn.
func (c *Conn) SetDeadline(t time.Time) error {
	_ = c.SetReadDeadline(t)
	return c.SetWriteDeadline(t)
}

// SetReadDeadline implements net.Conn.
func (c *Conn) SetReadDeadline(t time.Time) error {
	c.mu.Lock()
	c.rdeadline = unixNano(t)
	c.signalLocked()
	c.mu.Unlock()
	return nil
}

// SetWriteDeadline implements net.Conn. Writes never block, so the deadline
// only makes writes issued after it fail.
func (c *Conn) SetWriteDeadline(t time.Time) error {
	c.wmu.Lock()
	c.wdeadline = unixNano(t)
	c.wmu.Unlock()
	return nil
}

func unixNano(t time.Time) int64 {
	if t.IsZero() {
		return 0
	}
	return t.UnixNano()
}
