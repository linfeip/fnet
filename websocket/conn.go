package websocket

import (
	"encoding/binary"
	"errors"
	"net"
	"sync"
	"sync/atomic"
	"time"
	"unicode/utf8"

	"github.com/linfeip/fnet"
	"github.com/linfeip/fnet/internal/bytepool"
	"github.com/linfeip/fnet/internal/units"

	"github.com/gobwas/ws"
	"github.com/gobwas/ws/wsutil"
)

// maxCorkBytes is the upper bound of the cork buffer (see Conn.cork).
const maxCorkBytes = 64 * units.KB

// maxFrameHeaderSize is the longest header of a frame the server sends: 2 bytes and a 64-bit length, never a mask.
const maxFrameHeaderSize = 2 + 8

// maxCopiedPayload is the largest payload writeFrame copies next to its header to write the frame in one piece.
const maxCopiedPayload = 4 * units.KB

// The values of Conn.phase: OnOpen runs on the goroutine of the upgrade request, while OnData and OnClose are
// invoked from fnet's tasks; they must not be invoked before OnOpen returns, and they must not block a goroutine of
// the executor. The zero value means already opened.
const (
	phaseOpened      int32 = iota // OnOpen has returned
	phaseOpening                  // OnOpen has not returned yet; set by Upgrade during the handshake
	phaseClosedEarly              // the connection closed before OnOpen returned; Upgrade invokes Handler.OnClose after OnOpen returns
)

var (
	errMessageTooBig = errors.New("websocket: message too big")
	errHandlerPanic  = errors.New("websocket: handler panic")
)

// Conn is a WebSocket connection. Except for SetContext, all methods can be called concurrently from any goroutine.
//
// The field order also accounts for alignment: small fields fill the gaps left by fields such as sync.Mutex (4-byte
// alignment), so that Conn stays in the smallest possible size class (see TestConnSize).
type Conn struct {
	phase   atomic.Int32 // the progress of OnOpen (phaseOpening and so on), see Upgrade
	closing atomic.Bool  // close frame sent or connection closed: set under writeMu, read lock-free by OnData; later data is all dropped

	// The following fields are accessed only in fnet's callbacks (OnData, OnClose): callbacks of one connection run
	// serially
	messageOpCode   ws.OpCode // the type of the message field; 0 (ws.OpContinuation) means there is no unfinished message
	receiving       bool      // the current deadline belongs to a message (or frame) that has not been received in full
	messageFinished bool      // a data message was received in full during this OnData
	corking         bool      // this OnData has taken the cork buffer and has not written it out yet, see cork

	frameHeader [maxFrameHeaderSize]byte // the frame-header encoding buffer, used while holding writeMu
	writeMu     sync.Mutex               // guarantees that no frame is sent after the close frame
	connection  fnet.Conn
	handler     Handler
	// The cork buffer; nil means no corking is in progress. Used while holding writeMu. A pointer rather than a
	// slice is stored so that Conn stays in a smaller size class.
	corkBuffer     *bytepool.Buffer
	message        bytepool.Buffer // the fragmented message being reassembled (already unmasked); callbacks only
	maxMessageSize int
	idleTimeout    time.Duration
	messageTimeout time.Duration

	segments [2][]byte // frame header and payload, Writev's argument (no per-frame allocation), used while holding writeMu
	ctx      any
	closeErr error // the reason for closing; determined when closing is set and never changed afterwards
}

// LocalAddr returns the local address; after the connection is closed (including inside OnClose) it returns nil,
// see fnet.Conn.LocalAddr.
func (c *Conn) LocalAddr() net.Addr { return c.connection.LocalAddr() }

// RemoteAddr returns the peer address.
func (c *Conn) RemoteAddr() net.Addr { return c.connection.RemoteAddr() }

// Context returns the user data bound to the connection.
func (c *Conn) Context() any { return c.ctx }

// SetContext binds user data; it should be called from a callback (callbacks of one connection run serially).
func (c *Conn) SetContext(ctx any) { c.ctx = ctx }

// WriteMessage sends a message as a single frame; op is usually ws.OpText or ws.OpBinary. Like fnet.Conn.Write it
// does not block, and the caller may reuse data once it returns; it returns net.ErrClosed when a close frame has
// been sent or the connection has closed.
// When several frames arrive in one OnData, the frames written while they are being processed (including those
// written by other goroutines) are corked first and written out all at once afterwards.
func (c *Conn) WriteMessage(op ws.OpCode, data []byte) error {
	c.writeMu.Lock()
	defer c.writeMu.Unlock()
	if c.closing.Load() {
		return net.ErrClosed
	}
	if c.corkBuffer != nil {
		return c.writeCorked(op, data)
	}
	return c.writeFrame(op, data)
}

// Close sends a close frame (1000) and then closes the connection. After Close returns, OnMessage is no longer
// called: messages not yet processed in the same batch and data arriving afterwards are all discarded, while a
// callback that is already running is unaffected.
func (c *Conn) Close() error {
	c.closeWith(ws.StatusNormalClosure, nil)
	return nil
}

// closeWith sends a close frame (without a status code when code is 0) and then closes the connection; err is the
// recorded reason for closing. Only the first call takes effect.
// After sending it does not wait for the peer's close frame in reply; fnet first finishes sending the data already
// written, then closes the socket.
func (c *Conn) closeWith(code ws.StatusCode, err error) {
	c.writeMu.Lock()
	if c.closing.Load() {
		c.writeMu.Unlock()
		return
	}
	c.closing.Store(true)
	c.closeErr = err
	var body []byte
	if !code.Empty() {
		body = ws.NewCloseFrameBody(code, "")
	}
	c.uncorkLocked() // the corked frames go out before the close frame
	c.writeFrame(ws.OpClose, body)
	c.writeMu.Unlock()
	c.connection.Close()
}

// writeFrame sends one unfragmented, unmasked frame and requires the caller to hold writeMu.
//
// A small frame is copied together with its header into one buffer and written with a single write: for a few KB the
// copy costs less than the kernel taking in the iovec array of a writev. A larger frame is written with a single
// writev of the header and the payload, so the payload is not copied.
func (c *Conn) writeFrame(op ws.OpCode, payload []byte) error {
	header := appendFrameHeader(c.frameHeader[:0], op, len(payload))
	if len(payload) <= maxCopiedPayload {
		frame := bytepool.Get(len(header) + len(payload))
		frame.Append(header)
		frame.Append(payload)
		_, err := c.connection.Write(frame.Bytes())
		frame.Release()
		return err
	}
	c.segments = [2][]byte{header, payload}
	_, err := c.connection.Writev(c.segments[:])
	c.segments[1] = nil // stop referencing the caller's payload
	return err
}

// writeCorked sends a message as one frame: it is appended to the cork buffer; when it does not fit, it is written
// out together with the already corked frames in a single writev, and this frame is not copied.
// The caller must hold writeMu and corking must be in progress.
//
// Not inlined: WriteMessage is on the call chain of every Echo message, and the initial stack is very small when the
// Executor starts a new goroutine per task; if the local variables here were merged into the caller's stack frame,
// every message would trigger one stack growth (measured at about 17% more CPU per message).
//
//go:noinline
func (c *Conn) writeCorked(op ws.OpCode, payload []byte) error {
	b := c.corkBuffer
	header := appendFrameHeader(c.frameHeader[:0], op, len(payload))
	if b.Len()+len(header)+len(payload) <= maxCorkBytes {
		b.Append(header)
		b.Append(payload)
		return nil
	}
	_, err := c.connection.Writev([][]byte{b.Bytes(), header, payload})
	b.Reset()
	return err
}

// cork starts corking: frames written afterwards (including those written by other goroutines) go into the cork
// buffer first and are written out all at once by uncork. The cork buffer is borrowed large enough in one go,
// according to the expected size bytes.
// cork and uncork are called only by the connection's task and record their state in the task-only field corking, so
// the other frames of the same OnData do not take writeMu to cork again, and an OnData that never corked does not take
// it to uncork. closeWith, which may run on any goroutine, writes the cork buffer out through uncorkLocked and leaves
// corking alone.
func (c *Conn) cork(size int) {
	c.corking = true
	c.writeMu.Lock()
	if c.corkBuffer == nil {
		b := bytepool.Get(min(size, maxCorkBytes))
		c.corkBuffer = &b
	}
	c.writeMu.Unlock()
}

// uncork writes out the corked frames and ends corking.
func (c *Conn) uncork() {
	c.corking = false
	c.writeMu.Lock()
	c.uncorkLocked()
	c.writeMu.Unlock()
}

// uncorkLocked is the same as uncork but requires the caller to hold writeMu; it does nothing when no corking is in
// progress.
func (c *Conn) uncorkLocked() {
	b := c.corkBuffer
	if b == nil {
		return
	}
	if b.Len() > 0 {
		c.connection.Write(b.Bytes())
	}
	b.Release()
	c.corkBuffer = nil
}

// appendFrameHeader appends a frame header with FIN=1, RSV of 0 and no mask (RFC 6455 5.2); the payload length is
// encoded in 7, 16 or 64 bits.
func appendFrameHeader(b []byte, op ws.OpCode, length int) []byte {
	b = append(b, 0x80|byte(op))
	switch {
	case length <= 125:
		return append(b, byte(length))
	case length <= 0xffff:
		return binary.BigEndian.AppendUint16(append(b, 126), uint16(length))
	default:
		return binary.BigEndian.AppendUint64(append(b, 127), uint64(length))
	}
}

// parseHeader parses the frame header at the start of data and returns it together with its length in bytes; the
// length is 0 when data does not hold the whole header yet. It is ws.ReadHeader on a byte slice: that one reads
// through an io.Reader and allocates a scratch buffer for every frame, which, run once per received frame, was the
// biggest source of garbage under pipelined load.
// Like ws.ReadHeader it checks nothing but the encoding: a 64-bit length with its most significant bit set is
// rejected, and only once the whole header is there. The rest of the validation is ws.CheckHeader's.
func parseHeader(data []byte) (h ws.Header, n int, err error) {
	if len(data) < 2 {
		return h, 0, nil
	}
	h.Fin = data[0]&0x80 != 0
	h.Rsv = (data[0] & 0x70) >> 4
	h.OpCode = ws.OpCode(data[0] & 0x0f)
	h.Masked = data[1]&0x80 != 0
	size := 2 // header length: the two fixed bytes, the extended length and the mask
	length := data[1] & 0x7f
	switch length {
	case 126:
		size += 2
	case 127:
		size += 8
	}
	if h.Masked {
		size += 4
	}
	if len(data) < size {
		return h, 0, nil
	}
	switch length {
	case 126:
		h.Length = int64(binary.BigEndian.Uint16(data[2:]))
	case 127:
		if data[2]&0x80 != 0 {
			return h, 0, ws.ErrHeaderLengthMSB
		}
		h.Length = int64(binary.BigEndian.Uint64(data[2:]))
	default:
		h.Length = int64(length)
	}
	if h.Masked {
		copy(h.Mask[:], data[size-4:size])
	}
	return h, size, nil
}

// OnData implements fhttp.Protocol: it processes inbound data frame by frame and returns the number of consumed
// bytes. An unfragmented message goes straight to the OnMessage callback, with the payload being a slice of data,
// neither queued nor copied. When data still holds further frames, the frames written while processing them are
// corked first and written out all at once afterwards (see cork).
//
// When a callback panics, the corked frames are written out and the connection is closed with 1011; the panic
// propagates upwards, so fnet closes the connection and the executor recovers it.
func (c *Conn) OnData(data []byte) int {
	// The connection is not read before OnOpen returns (see Upgrade), and a legitimate peer sends data only after
	// receiving the 101; data that arrives early stays in the engine and is processed the next time data arrives
	// after OnOpen has returned.
	if c.phase.Load() != phaseOpened {
		return 0
	}
	done := false
	defer func() {
		if !done {
			c.uncork()
			c.closeWith(ws.StatusInternalServerError, errHandlerPanic)
		}
	}()
	consumed := 0
	for !c.closing.Load() { // after Close in a callback or a protocol error, the rest of the batch is not processed
		n := c.readFrame(data[consumed:])
		if n == 0 {
			break
		}
		consumed += n
	}
	if c.corking {
		c.uncork()
	}
	done = true
	if c.closing.Load() { // already closing (close frame received, protocol error or local Close): discard all later data
		return len(data)
	}
	c.updateDeadline(c.messageOpCode != 0 || consumed < len(data))
	return consumed
}

// updateDeadline sets the connection's deadline according to the receive state. When there is an incomplete message
// or frame (pending), messageTimeout applies from when it started being received; fragments and control frames
// arriving in the meantime do not extend it, and the timer is only restarted for the next message once a data
// message has been received in full. Otherwise the connection is idle and idleTimeout applies.
func (c *Conn) updateDeadline(pending bool) {
	finished := c.messageFinished
	c.messageFinished = false
	switch {
	case !pending: // set every time: this also overrides a deadline fhttp may have set before the upgrade (see fhttp.Upgrade)
		c.receiving = false
		c.connection.SetDeadline(deadlineAfter(c.idleTimeout))
	case !c.receiving || finished:
		c.receiving = true
		c.connection.SetDeadline(deadlineAfter(c.messageTimeout))
	}
}

// deadlineAfter returns the instant timeout from now; timeout<=0 means no limit and the zero value is returned.
func deadlineAfter(timeout time.Duration) time.Time {
	if timeout <= 0 {
		return time.Time{}
	}
	return time.Now().Add(timeout)
}

// readFrame processes the one frame at the start of data and returns the frame's length; it returns 0 when the frame
// is incomplete, leaving the data in the engine to be processed once it has all arrived.
func (c *Conn) readFrame(data []byte) int {
	h, start, err := parseHeader(data)
	if err == nil && start == 0 { // the header is incomplete
		return 0
	}
	state := ws.StateServerSide
	if c.messageOpCode != 0 {
		state = state.Set(ws.StateFragmented)
	}
	if err == nil {
		err = ws.CheckHeader(h, state)
	}
	if err != nil {
		c.closeWith(ws.StatusProtocolError, err)
		return 0
	}
	// Check the length before the payload has all arrived, so the engine does not buffer data for an oversized
	// frame. Written as a subtraction to avoid overflow when h.Length is very large.
	if !h.OpCode.IsControl() && h.Length > int64(c.maxMessageSize-c.message.Len()) {
		c.closeWith(ws.StatusMessageTooBig, errMessageTooBig)
		return 0
	}
	if int64(len(data)-start) < h.Length {
		return 0
	}
	end := start + int(h.Length)
	payload := data[start:end]
	ws.Cipher(payload, h.Mask, 0) // unmask in place: this data is consumed right now
	if end < len(data) && !c.corking {
		c.cork(len(data) - start) // more data follows: cork frames from callbacks; OnData writes them all at once when done
	}

	switch h.OpCode {
	case ws.OpPing:
		c.WriteMessage(ws.OpPong, payload)
	case ws.OpPong:
	case ws.OpClose:
		c.onCloseFrame(payload)
	default: // data frames
		if h.Fin && h.OpCode != ws.OpContinuation { // an unfragmented message: the payload needs no copy, deliver it directly
			c.deliver(h.OpCode, payload)
			break
		}
		if h.OpCode != ws.OpContinuation {
			c.messageOpCode = h.OpCode
		}
		c.message.Append(payload)
		if h.Fin { // the last fragment has arrived, deliver the reassembled message
			c.deliver(c.messageOpCode, c.message.Bytes())
			c.message.Release()
			c.messageOpCode = 0
		}
	}
	return end
}

// onCloseFrame handles the peer's close frame: it replies with the same status code and then closes the connection
// (RFC 6455 5.5.1).
func (c *Conn) onCloseFrame(payload []byte) {
	if len(payload) == 0 { // no status code counts as 1005 (RFC 6455 7.1.5); reply with a close frame without a status code too
		c.closeWith(0, wsutil.ClosedError{Code: ws.StatusNoStatusRcvd})
		return
	}
	code, reason := ws.ParseCloseFrameData(payload)
	if err := ws.CheckCloseFrameData(code, reason); err != nil {
		c.closeWith(ws.StatusProtocolError, err)
		return
	}
	c.closeWith(code, wsutil.ClosedError{Code: code, Reason: reason})
}

// deliver validates one complete message and calls OnMessage; data is valid only within the callback.
func (c *Conn) deliver(op ws.OpCode, data []byte) {
	c.messageFinished = true
	if op == ws.OpText && !utf8.Valid(data) {
		c.closeWith(ws.StatusInvalidFramePayloadData, wsutil.ErrInvalidUTF8)
		return
	}
	c.handler.OnMessage(c, op, data)
}

// OnClose implements fhttp.Protocol: it calls Handler.OnClose after the connection is closed. fnet guarantees that
// it is the connection's last callback and that all messages received earlier have already been delivered.
func (c *Conn) OnClose(err error) {
	c.message.Release()
	c.writeMu.Lock()
	if !c.closing.Load() { // the close was not initiated locally: the reason given by the engine wins
		c.closing.Store(true)
		c.closeErr = err
	}
	c.writeMu.Unlock()
	// Engine-initiated closes, such as server shutdown or deadline expiry, can happen before OnOpen returns: leave
	// the callback to Upgrade after OnOpen returns, so that OnClose really is the last callback.
	if !c.phase.CompareAndSwap(phaseOpening, phaseClosedEarly) {
		c.handler.OnClose(c, c.closeErr)
	}
}
