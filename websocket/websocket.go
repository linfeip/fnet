package websocket

import (
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

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
}

// DefaultUpgrader is a ready-to-use Upgrader with sensible defaults.
var DefaultUpgrader = &Upgrader{}

// Upgrade upgrades the HTTP connection using DefaultUpgrader.
func Upgrade(w http.ResponseWriter, r *http.Request) (*Conn, error) {
	return DefaultUpgrader.Upgrade(w, r)
}

// Upgrade upgrades an incoming HTTP request to a WebSocket connection.
func (u *Upgrader) Upgrade(w http.ResponseWriter, r *http.Request) (*Conn, error) {
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

// ReadMessage reads the next data message from the peer.
// Control frames (Ping, Pong, Close) are automatically handled and replied to.
func (c *Conn) ReadMessage() (OpCode, []byte, error) {
	payload, op, err := wsutil.ReadClientData(&c.rw)
	return op, payload, err
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
	return wsutil.WriteServerMessage(c.conn, op, payload)
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
	return ws.WriteFrame(c.conn, ws.NewPingFrame(data))
}

// WritePong sends a Pong control frame.
func (c *Conn) WritePong(data []byte) error {
	c.writeMu.Lock()
	defer c.writeMu.Unlock()
	return ws.WriteFrame(c.conn, ws.NewPongFrame(data))
}

// Close gracefully closes the WebSocket connection by sending a Close control frame.
func (c *Conn) Close() error {
	if c.closed.CompareAndSwap(false, true) {
		c.writeMu.Lock()
		_ = ws.WriteFrame(c.conn, ws.NewCloseFrame(ws.NewCloseFrameBody(ws.StatusNormalClosure, "")))
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
