package websocket

import (
	"errors"
	"fmt"
	"io"
	"net"
	"slices"
	"sync"
	"sync/atomic"
	"time"
	"unicode/utf8"

	"github.com/gobwas/ws"

	"github.com/linfeip/fnet/internal/bufpool"
	"github.com/linfeip/fnet/internal/reactor"
)

const (
	// maxBatch is the largest frame coalesced into a write batch; bigger
	// frames flush the batch and go out on their own.
	maxBatch = 64 << 10
	// maxHeaderSize is the largest header of a server frame, which is unmasked.
	maxHeaderSize = 10
)

var (
	errEventDriven   = errors.New("fnet/websocket: messages of an event-driven connection are delivered to OnMessage")
	errControlTooBig = errors.New("fnet/websocket: control frame payload exceeds 125 bytes")
)

// Conn is a server-side WebSocket connection. Writes are safe from any
// goroutine. ReadMessage and Handle are for connections upgraded without an
// OnMessage callback.
type Conn struct {
	nc       net.Conn      // hijacked connection, or the reactor conn in event mode
	raw      *reactor.Conn // set in event mode
	br       io.Reader     // blocking reads; nil in event mode
	protocol string
	closed   atomic.Bool

	compressed bool
	limits     *limits
	onPong     func(c *Conn, data []byte)

	wmu        sync.Mutex
	batch      *bufpool.Buffer
	batchDepth int32
	batchLen   int32 // at most maxBatch
}

// Subprotocol returns the negotiated subprotocol, or "".
func (c *Conn) Subprotocol() string { return c.protocol }

// IsCompressed reports whether permessage-deflate is active.
func (c *Conn) IsCompressed() bool { return c.compressed }

// SetMaxDecompressedMessageSize changes the decompressed size limit. Call it
// before messages flow (e.g. in OnOpen).
func (c *Conn) SetMaxDecompressedMessageSize(limit int64) {
	l := *c.limits
	l.maxDecompressSize = limit
	c.limits = share(l)
}

// SetMaxMessageSize changes the received message size limit. Call it before
// messages flow (e.g. in OnOpen).
func (c *Conn) SetMaxMessageSize(limit int64) {
	l := *c.limits
	l.maxMessageSize = limit
	c.limits = share(l)
}

// NetConn returns the underlying connection.
func (c *Conn) NetConn() net.Conn { return c.nc }

// LocalAddr returns the local network address.
func (c *Conn) LocalAddr() net.Addr { return c.nc.LocalAddr() }

// RemoteAddr returns the remote network address.
func (c *Conn) RemoteAddr() net.Addr { return c.nc.RemoteAddr() }

// SetDeadline sets the read and write deadlines.
func (c *Conn) SetDeadline(t time.Time) error {
	_ = c.SetReadDeadline(t)
	return c.SetWriteDeadline(t)
}

// SetReadDeadline sets the read deadline: a blocking ReadMessage fails once it
// passes. On an event-driven connection, which nobody reads, the connection
// closes at t unless the deadline is moved first (OnClose gets
// os.ErrDeadlineExceeded): pushing it forward on each message or Pong makes a
// liveness timeout.
func (c *Conn) SetReadDeadline(t time.Time) error {
	if c.raw != nil && c.br == nil {
		c.raw.SetCloseDeadline(t)
		return nil
	}
	return c.nc.SetReadDeadline(t)
}

// SetWriteDeadline sets the write deadline.
func (c *Conn) SetWriteDeadline(t time.Time) error { return c.nc.SetWriteDeadline(t) }

// ---------------------------------------------------------------------------
// Writing
// ---------------------------------------------------------------------------

// WriteMessage sends one data message. Compression, when negotiated, happens
// before the connection's write lock is taken.
func (c *Conn) WriteMessage(op OpCode, payload []byte) error {
	data, rsv1 := payload, false
	if c.compressed && (op == OpText || op == OpBinary) && len(payload) >= c.limits.compressThreshold {
		if z, err := compress(payload, c.limits.compressLevel); err == nil && len(z) < len(payload) {
			data, rsv1 = z, true
		}
	}
	c.wmu.Lock()
	defer c.wmu.Unlock()
	return c.writeFrameLocked(op, data, rsv1)
}

// WriteText sends a text message.
func (c *Conn) WriteText(text string) error { return c.WriteMessage(OpText, []byte(text)) }

// WriteBinary sends a binary message.
func (c *Conn) WriteBinary(payload []byte) error { return c.WriteMessage(OpBinary, payload) }

// WritePing sends a Ping control frame.
func (c *Conn) WritePing(data []byte) error { return c.writeControl(OpPing, data) }

// WritePong sends a Pong control frame.
func (c *Conn) WritePong(data []byte) error { return c.writeControl(OpPong, data) }

// writeControl sends a Ping or Pong. A control frame carries at most 125
// bytes (RFC 6455 5.5); a longer one would make the peer fail the connection.
func (c *Conn) writeControl(op OpCode, payload []byte) error {
	if len(payload) > 125 {
		return errControlTooBig
	}
	c.wmu.Lock()
	defer c.wmu.Unlock()
	return c.writeFrameLocked(op, payload, false)
}

// BeginBatch starts coalescing written frames; EndBatch sends them in one
// write. Batches nest. Control frames are never delayed by a batch.
func (c *Conn) BeginBatch() {
	c.wmu.Lock()
	c.batchDepth++
	c.wmu.Unlock()
}

// EndBatch closes the innermost batch and flushes when it was the outermost.
func (c *Conn) EndBatch() error {
	c.wmu.Lock()
	defer c.wmu.Unlock()
	if c.batchDepth == 0 {
		return nil
	}
	if c.batchDepth--; c.batchDepth > 0 {
		return nil
	}
	return c.flushBatchLocked()
}

func (c *Conn) flushBatchLocked() error {
	b, n := c.batch, int(c.batchLen)
	if b == nil {
		return nil
	}
	c.batch, c.batchLen = nil, 0
	var err error
	if n > 0 {
		_, err = c.nc.Write(b.B[:n]) // the reactor conn copies what it cannot send now
	}
	bufpool.Put(b)
	return err
}

func (c *Conn) writeFrameLocked(op OpCode, payload []byte, rsv1 bool) error {
	var hdr [maxHeaderSize]byte
	hn := putHeader(hdr[:], op, len(payload), rsv1)
	if size := hn + len(payload); c.batchDepth > 0 && !op.IsControl() && size <= maxBatch {
		if c.batch != nil && int(c.batchLen)+size > len(c.batch.B) {
			if err := c.flushBatchLocked(); err != nil {
				return err
			}
		}
		if c.batch == nil {
			c.batch = bufpool.Get(maxBatch)
		}
		c.batchLen += int32(copy(c.batch.B[c.batchLen:], hdr[:hn]))
		c.batchLen += int32(copy(c.batch.B[c.batchLen:], payload))
		return nil
	}
	if err := c.flushBatchLocked(); err != nil { // keep frames in order
		return err
	}
	return c.writev(hdr[:hn], payload)
}

// writev sends a frame header and payload as one unit. hdr does not escape:
// the event-driven path keeps it on the caller's stack, and the others, which
// hand their buffers to an interface, send a copy.
func (c *Conn) writev(hdr, payload []byte) error {
	if c.raw != nil {
		_, err := c.raw.Writev([][]byte{hdr, payload})
		return err
	}
	var h [maxHeaderSize]byte
	parts := [][]byte{h[:copy(h[:], hdr)], payload}
	if vw, ok := c.nc.(interface{ Writev([][]byte) (int, error) }); ok {
		_, err := vw.Writev(parts)
		return err
	}
	buf := bufpool.Get(len(parts[0]) + len(payload))
	copy(buf.B[copy(buf.B, parts[0]):], payload)
	_, err := c.nc.Write(buf.B)
	bufpool.Put(buf)
	return err
}

// ---------------------------------------------------------------------------
// Closing
// ---------------------------------------------------------------------------

// Close sends a normal Close frame and closes the connection once queued
// output is flushed.
func (c *Conn) Close() error { return c.closeWith(closeNormal) }

// CloseWithStatus sends a Close frame with the given status and reason, then
// closes the connection.
func (c *Conn) CloseWithStatus(status ws.StatusCode, reason string) error {
	return c.closeWith(ws.NewCloseFrameBody(status, reason))
}

func (c *Conn) closeWith(body []byte) error {
	if !c.closed.CompareAndSwap(false, true) {
		return nil
	}
	c.wmu.Lock()
	c.batchDepth = 0
	_ = c.writeFrameLocked(OpClose, body, false)
	c.wmu.Unlock()
	return c.nc.Close()
}

func (c *Conn) fail(e *protocolError) error {
	_ = c.CloseWithStatus(e.code, e.reason)
	return e
}

// ---------------------------------------------------------------------------
// Blocking reads
// ---------------------------------------------------------------------------

// ReadMessage reads the next data message, answering Ping and Close frames
// itself. The returned payload belongs to the caller.
func (c *Conn) ReadMessage() (OpCode, []byte, error) {
	if c.br == nil {
		return 0, nil, errEventDriven
	}
	var (
		op         OpCode
		deflated   bool
		msg        []byte
		fragmented bool
	)
	for {
		h, err := ws.ReadHeader(c.br)
		if err != nil {
			return 0, nil, err
		}
		if perr := checkHeader(h, c.compressed, fragmented); perr != nil {
			return 0, nil, c.fail(perr)
		}
		if h.OpCode.IsControl() {
			payload := make([]byte, h.Length)
			if _, err := io.ReadFull(c.br, payload); err != nil {
				return 0, nil, err
			}
			ws.Cipher(payload, h.Mask, 0)
			switch h.OpCode {
			case OpPing:
				_ = c.writeControl(OpPong, payload)
			case OpPong:
				if c.onPong != nil {
					c.onPong(c, payload)
				}
			case OpClose:
				echo, perr := checkClosePayload(payload)
				if perr != nil {
					return 0, nil, c.fail(perr)
				}
				_ = c.closeWith(echo)
				return 0, nil, io.EOF
			}
			continue
		}
		if h.Length > maxFrameLength {
			return 0, nil, c.fail(headerError(errFrameTooLarge))
		}
		if perr := checkSize(h, len(msg), c.limits.maxMessageSize); perr != nil {
			return 0, nil, c.fail(perr)
		}
		if h.OpCode != OpContinuation {
			op, deflated = h.OpCode, c.compressed && h.Rsv1()
		}
		n := len(msg)
		if msg, err = readPayload(c.br, msg, int(h.Length)); err != nil {
			return 0, nil, err
		}
		ws.Cipher(msg[n:], h.Mask, 0)
		if fragmented = !h.Fin; fragmented {
			continue
		}
		if msg, err = c.decode(op, deflated, msg); err != nil {
			return 0, nil, err
		}
		return op, msg, nil
	}
}

// readPayload appends length bytes from r to msg. msg grows with what
// arrives, not with what the frame header claims, so a peer cannot make the
// server allocate a whole message it never sends.
func readPayload(r io.Reader, msg []byte, length int) ([]byte, error) {
	for length > 0 {
		chunk := min(length, 64<<10)
		n := len(msg)
		msg = slices.Grow(msg, chunk)[:n+chunk]
		if _, err := io.ReadFull(r, msg[n:]); err != nil {
			return msg[:n], err
		}
		length -= chunk
	}
	return msg, nil
}

// decode inflates a message and validates text as UTF-8, closing the
// connection with the matching status on failure.
func (c *Conn) decode(op OpCode, deflated bool, msg []byte) ([]byte, error) {
	if deflated {
		out, err := decompress(msg, c.limits.maxDecompressSize)
		if err != nil {
			status := ws.StatusProtocolError
			if errors.Is(err, ErrMessageTooBig) {
				status = ws.StatusMessageTooBig
			}
			_ = c.CloseWithStatus(status, "")
			return nil, err
		}
		msg = out
	}
	if op == OpText && !utf8.Valid(msg) {
		return nil, c.fail(&protocolError{ws.StatusInvalidFramePayloadData, "invalid UTF-8 in text message"})
	}
	return msg, nil
}

// ReadText reads the next message, which must be text.
func (c *Conn) ReadText() (string, error) {
	op, data, err := c.ReadMessage()
	if err != nil {
		return "", err
	}
	if op != OpText {
		return "", fmt.Errorf("fnet/websocket: expected text message, got op=%v", op)
	}
	return string(data), nil
}

// ReadBinary reads the next message, which must be binary.
func (c *Conn) ReadBinary() ([]byte, error) {
	op, data, err := c.ReadMessage()
	if err != nil {
		return nil, err
	}
	if op != OpBinary {
		return nil, fmt.Errorf("fnet/websocket: expected binary message, got op=%v", op)
	}
	return data, nil
}

// Handle calls onMessage for each message until the connection closes or
// onMessage returns an error.
func (c *Conn) Handle(onMessage func(op OpCode, payload []byte) error) error {
	for {
		op, msg, err := c.ReadMessage()
		if err != nil {
			return err
		}
		if err := onMessage(op, msg); err != nil {
			return err
		}
	}
}
