package websocket

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"github.com/linfeip/fnet"

	"github.com/gobwas/ws"
	"github.com/gobwas/ws/wsutil"
)

// Re-export common WebSocket opcodes from gobwas/ws for convenience.
const (
	OpContinuation = ws.OpContinuation
	OpText         = ws.OpText
	OpBinary       = ws.OpBinary
	OpClose        = ws.OpClose
	OpPing         = ws.OpPing
	OpPong         = ws.OpPong
)

// OpCode represents WebSocket frame opcode.
type OpCode = ws.OpCode

// Upgrader handles WebSocket handshake upgrades.
// It leverages github.com/gobwas/ws for RFC 6455 compliant, zero-alloc negotiation.
type Upgrader struct {
	// Subprotocols is a list of supported subprotocols.
	Subprotocols []string

	// CheckOrigin returns true if the request Origin is acceptable.
	// Defaults to nil (accept all origins).
	CheckOrigin func(r *http.Request) bool

	// Header contains optional headers to include in the 101 response.
	Header http.Header

	// Optional event-driven callbacks. When OnMessage is set, Upgrade will
	// automatically operate in event-driven mode (zero goroutines while idle).
	// See EventHandler for the callback contract.
	OnOpen    func(c *Conn)
	OnMessage func(c *Conn, op OpCode, payload []byte)
	OnClose   func(c *Conn, err error)
}

// EventHandler defines the callbacks for event-driven WebSocket connections.
//
// In event-driven mode fnet parses frames on its reactor goroutine and invokes
// OnMessage inline, so idle connections hold no goroutines. Consequently:
//
//   - OnMessage must not block; blocking stalls every connection on that reactor.
//     Offload long work to your own goroutine/worker pool.
//   - payload is a view into the reactor's read buffer and is only valid for the
//     duration of the callback. Copy it if it must outlive the call.
//   - Writes from inside OnMessage (e.g. echo) are cheap: they go directly to the
//     socket and only queue when the kernel send buffer is full.
type EventHandler struct {
	OnOpen    func(c *Conn)
	OnMessage func(c *Conn, op OpCode, payload []byte)
	OnClose   func(c *Conn, err error)
}

type wsHandlerBridge struct {
	conn    *Conn
	handler EventHandler
}

func (b *wsHandlerBridge) OnOpen() {
	if b.handler.OnOpen != nil {
		b.handler.OnOpen(b.conn)
	}
}

func (b *wsHandlerBridge) OnMessage(opcode byte, payload []byte) {
	if b.handler.OnMessage != nil {
		b.handler.OnMessage(b.conn, OpCode(opcode), payload)
	}
}

func (b *wsHandlerBridge) OnClose(err error) {
	if b.handler.OnClose != nil {
		b.handler.OnClose(b.conn, err)
	}
}

// DefaultUpgrader is a ready-to-use Upgrader with sensible defaults.
var DefaultUpgrader = &Upgrader{}

// Upgrade upgrades the HTTP connection using DefaultUpgrader.
func Upgrade(w http.ResponseWriter, r *http.Request) (*Conn, error) {
	return DefaultUpgrader.Upgrade(w, r)
}

// UpgradeEvent upgrades the HTTP connection using DefaultUpgrader in event-driven mode.
func UpgradeEvent(w http.ResponseWriter, r *http.Request, handler EventHandler) (*Conn, error) {
	return DefaultUpgrader.UpgradeEvent(w, r, handler)
}

// Upgrade upgrades an incoming HTTP request to a WebSocket connection.
// If u.OnMessage is configured, it operates in event-driven mode (zero goroutines while idle).
// Otherwise, it returns a classic blocking *Conn for manual ReadMessage calls.
func (u *Upgrader) Upgrade(w http.ResponseWriter, r *http.Request) (*Conn, error) {
	if u.OnMessage != nil {
		return u.UpgradeEvent(w, r, EventHandler{
			OnOpen:    u.OnOpen,
			OnMessage: u.OnMessage,
			OnClose:   u.OnClose,
		})
	}
	return u.upgradeInternal(w, r)
}

// UpgradeEvent upgrades an incoming HTTP request to an event-driven WebSocket connection.
// It executes the WebSocket handshake, registers the event callbacks, and instructs
// fnet to manage the connection in its poller. The HTTP ServeHTTP handler can (and should)
// return immediately, releasing the request goroutine.
// While idle, 0 goroutines are held by this connection.
func (u *Upgrader) UpgradeEvent(w http.ResponseWriter, r *http.Request, h EventHandler) (*Conn, error) {
	conn, err := u.upgradeInternal(w, r)
	if err != nil {
		return nil, err
	}

	var attacher fnet.WSAttacher
	if a, ok := w.(fnet.WSAttacher); ok {
		attacher = a
	} else if a, ok := conn.conn.(fnet.WSAttacher); ok {
		attacher = a
	}

	bridge := &wsHandlerBridge{conn: conn, handler: h}
	if attacher != nil {
		_, err := attacher.AttachWS(bridge)
		if err == nil {
			bridge.OnOpen()
			return conn, nil
		}
		if !errors.Is(err, fnet.ErrWSAttachUnsupported) {
			_ = conn.Close()
			return nil, err
		}
	}

	// The reactor cannot drive this connection (TLS, or a non-fnet ResponseWriter):
	// deliver the same callbacks from a dedicated goroutine instead.
	go func() {
		bridge.OnOpen()
		err := conn.Handle(func(op OpCode, msg []byte) error {
			bridge.OnMessage(byte(op), msg)
			return nil
		})
		_ = conn.Close()
		bridge.OnClose(err)
	}()
	return conn, nil
}

func (u *Upgrader) upgradeInternal(w http.ResponseWriter, r *http.Request) (*Conn, error) {
	if u.CheckOrigin != nil && !u.CheckOrigin(r) {
		http.Error(w, "origin not allowed", http.StatusForbidden)
		return nil, errors.New("fnet/websocket: origin not allowed")
	}

	httpUpgrader := ws.HTTPUpgrader{
		Header: u.Header,
	}
	if len(u.Subprotocols) > 0 {
		httpUpgrader.Protocol = func(proto string) bool {
			for _, sp := range u.Subprotocols {
				if sp == proto {
					return true
				}
			}
			return false
		}
	}

	netConn, brw, hs, err := httpUpgrader.Upgrade(r, w)
	if err != nil {
		return nil, err
	}

	// Determine read source: prefer brw.Reader (which has any read-ahead bytes),
	// falling back to netConn.
	var reader io.Reader = netConn
	if brw != nil && brw.Reader != nil {
		reader = brw.Reader
	}

	return &Conn{
		conn:     netConn,
		reader:   reader,
		rw:       readWriter{Reader: reader, Writer: netConn},
		protocol: hs.Protocol,
	}, nil
}

type readWriter struct {
	io.Reader
	io.Writer
}

// VectorWriter is an optional interface implemented by connections that support
// zero-copy scatter-gather vector writes (e.g. *fnet.VirtualConn via writev).
type VectorWriter interface {
	WriteVector(iovs [][]byte) (int, error)
}

// formatServerHeader encodes an unmasked server WebSocket frame header into bts.
// It returns the number of bytes written (2, 4, or 10).
func formatServerHeader(bts []byte, op OpCode, length int) int {
	bts[0] = 0x80 | byte(op)
	switch {
	case length <= 125:
		bts[1] = byte(length)
		return 2
	case length <= 65535:
		bts[1] = 126
		binary.BigEndian.PutUint16(bts[2:4], uint16(length))
		return 4
	default:
		bts[1] = 127
		binary.BigEndian.PutUint64(bts[2:10], uint64(length))
		return 10
	}
}

// Conn represents an active WebSocket connection.
// It uses github.com/gobwas/ws and wsutil under the hood for high performance,
// RFC-compliant framing, and fast SIMD/SWAR unmasking.
type Conn struct {
	conn     net.Conn
	reader   io.Reader
	rw       readWriter
	protocol string
	closed   atomic.Bool
	writeMu  sync.Mutex
}

var writeBufPool = sync.Pool{
	New: func() any {
		b := make([]byte, 64*1024)
		return &b
	},
}

func (c *Conn) writeFrameLocked(op OpCode, payload []byte) error {
	total := 10 + len(payload)
	if total <= 64*1024 {
		bp := writeBufPool.Get().(*[]byte)
		buf := *bp
		hLen := formatServerHeader(buf[:10], op, len(payload))
		copy(buf[hLen:], payload)
		_, err := c.conn.Write(buf[:hLen+len(payload)])
		writeBufPool.Put(bp)
		return err
	}

	var hBuf [10]byte
	hLen := formatServerHeader(hBuf[:], op, len(payload))
	iovs := [][]byte{hBuf[:hLen], payload}

	if vw, ok := c.conn.(VectorWriter); ok {
		_, err := vw.WriteVector(iovs)
		return err
	}

	return wsutil.WriteServerMessage(c.conn, op, payload)
}

// ReadMessage reads the next data message from the peer.
// Control frames (Ping, Pong, Close) are automatically handled and replied to.
// Payload is allocated directly matching header length to avoid io.ReadAll reallocation.
func (c *Conn) ReadMessage() (OpCode, []byte, error) {
	for {
		header, err := ws.ReadHeader(c.reader)
		if err != nil {
			return 0, nil, err
		}

		if header.OpCode.IsControl() {
			var ctrlPayload []byte
			if header.Length > 0 {
				ctrlPayload = make([]byte, header.Length)
				if _, err := io.ReadFull(c.reader, ctrlPayload); err != nil {
					return 0, nil, err
				}
				if header.Masked {
					ws.Cipher(ctrlPayload, header.Mask, 0)
				}
			}

			if header.OpCode == OpClose {
				c.writeMu.Lock()
				_ = c.writeFrameLocked(OpClose, ws.NewCloseFrameBody(ws.StatusNormalClosure, ""))
				c.writeMu.Unlock()
				return 0, nil, io.EOF
			}
			if header.OpCode == OpPing {
				c.writeMu.Lock()
				_ = c.writeFrameLocked(OpPong, ctrlPayload)
				c.writeMu.Unlock()
				continue
			}
			if header.OpCode == OpPong {
				continue
			}
			continue
		}

		// Data frame (Text, Binary, Continuation)
		payload := make([]byte, header.Length)
		if header.Length > 0 {
			if _, err := io.ReadFull(c.reader, payload); err != nil {
				return 0, nil, err
			}
			if header.Masked {
				ws.Cipher(payload, header.Mask, 0)
			}
		}

		if header.Fin {
			return header.OpCode, payload, nil
		}

		// Handle fragmented messages: read subsequent continuation frames
		msgOp := header.OpCode
		for {
			nextHdr, err := ws.ReadHeader(c.reader)
			if err != nil {
				return 0, nil, err
			}
			if nextHdr.OpCode.IsControl() {
				var ctrlPayload []byte
				if nextHdr.Length > 0 {
					ctrlPayload = make([]byte, nextHdr.Length)
					if _, err := io.ReadFull(c.reader, ctrlPayload); err != nil {
						return 0, nil, err
					}
					if nextHdr.Masked {
						ws.Cipher(ctrlPayload, nextHdr.Mask, 0)
					}
				}
				if nextHdr.OpCode == OpPing {
					c.writeMu.Lock()
					_ = c.writeFrameLocked(OpPong, ctrlPayload)
					c.writeMu.Unlock()
				}
				continue
			}
			if nextHdr.Length > 0 {
				part := make([]byte, nextHdr.Length)
				if _, err := io.ReadFull(c.reader, part); err != nil {
					return 0, nil, err
				}
				if nextHdr.Masked {
					ws.Cipher(part, nextHdr.Mask, 0)
				}
				payload = append(payload, part...)
			}
			if nextHdr.Fin {
				break
			}
		}

		return msgOp, payload, nil
	}
}

// ReadText reads the next text message as string.
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

// ReadBinary reads the next binary message as a byte slice.
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

// WriteMessage sends a WebSocket data message with the specified opcode.
func (c *Conn) WriteMessage(op OpCode, payload []byte) error {
	c.writeMu.Lock()
	defer c.writeMu.Unlock()
	return c.writeFrameLocked(op, payload)
}

// WriteText sends a text message.
func (c *Conn) WriteText(text string) error {
	return c.WriteMessage(OpText, []byte(text))
}

// WriteBinary sends a binary message.
func (c *Conn) WriteBinary(payload []byte) error {
	return c.WriteMessage(OpBinary, payload)
}

// WritePing sends a Ping control frame.
func (c *Conn) WritePing(data []byte) error {
	c.writeMu.Lock()
	defer c.writeMu.Unlock()
	return c.writeFrameLocked(OpPing, data)
}

// WritePong sends a Pong control frame.
func (c *Conn) WritePong(data []byte) error {
	c.writeMu.Lock()
	defer c.writeMu.Unlock()
	return c.writeFrameLocked(OpPong, data)
}

// Close gracefully closes the WebSocket connection by sending a Close control frame.
func (c *Conn) Close() error {
	if c.closed.CompareAndSwap(false, true) {
		c.writeMu.Lock()
		_ = c.writeFrameLocked(OpClose, ws.NewCloseFrameBody(ws.StatusNormalClosure, ""))
		c.writeMu.Unlock()
		return c.conn.Close()
	}
	return nil
}

// Handle runs a read loop calling onMessage for each received message.
// It returns when the connection closes or when onMessage returns an error.
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

// NetConn returns the underlying net.Conn (e.g. *fnet.VirtualConn or *tls.Conn).
func (c *Conn) NetConn() net.Conn {
	return c.conn
}

// Subprotocol returns the negotiated subprotocol, or empty string if none.
func (c *Conn) Subprotocol() string {
	return c.protocol
}

// SetDeadline sets the read and write deadlines associated with the connection.
func (c *Conn) SetDeadline(t time.Time) error {
	return c.conn.SetDeadline(t)
}

// SetReadDeadline sets the read deadline.
func (c *Conn) SetReadDeadline(t time.Time) error {
	return c.conn.SetReadDeadline(t)
}

// SetWriteDeadline sets the write deadline.
func (c *Conn) SetWriteDeadline(t time.Time) error {
	return c.conn.SetWriteDeadline(t)
}

// LocalAddr returns the local network address.
func (c *Conn) LocalAddr() net.Addr {
	return c.conn.LocalAddr()
}

// RemoteAddr returns the remote network address.
func (c *Conn) RemoteAddr() net.Addr {
	return c.conn.RemoteAddr()
}
