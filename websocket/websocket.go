// Package websocket implements RFC 6455 WebSocket (with RFC 7692
// permessage-deflate) on fnet connections.
//
// With an OnMessage callback a connection is event-driven: after the upgrade
// it lives on fnet's event loop, holds no goroutine while idle, and each
// message runs on the worker pool. Without one, Upgrade returns a Conn for
// blocking ReadMessage loops.
package websocket

import (
	"bytes"
	"compress/flate"
	"errors"
	"io"
	"net/http"
	"strings"

	"github.com/gobwas/httphead"
	"github.com/gobwas/ws"
	"github.com/gobwas/ws/wsflate"

	"github.com/linfeip/fnet/internal/reactor"
)

// Opcodes, re-exported from gobwas/ws.
const (
	OpContinuation = ws.OpContinuation
	OpText         = ws.OpText
	OpBinary       = ws.OpBinary
	OpClose        = ws.OpClose
	OpPing         = ws.OpPing
	OpPong         = ws.OpPong
)

// OpCode is a frame opcode.
type OpCode = ws.OpCode

// StatusCode is a close status (RFC 6455 7.4).
type StatusCode = ws.StatusCode

// Close statuses, re-exported from gobwas/ws.
const (
	StatusNormalClosure           = ws.StatusNormalClosure
	StatusGoingAway               = ws.StatusGoingAway
	StatusProtocolError           = ws.StatusProtocolError
	StatusUnsupportedData         = ws.StatusUnsupportedData
	StatusInvalidFramePayloadData = ws.StatusInvalidFramePayloadData
	StatusPolicyViolation         = ws.StatusPolicyViolation
	StatusMessageTooBig           = ws.StatusMessageTooBig
	StatusInternalServerError     = ws.StatusInternalServerError
)

const (
	// DefaultMaxDecompressedMessageSize (16 MiB) guards against deflate bombs.
	DefaultMaxDecompressedMessageSize int64 = 16 << 20
	// DefaultMaxMessageSize (32 MiB) bounds a received message.
	DefaultMaxMessageSize int64 = 32 << 20
	// DefaultMaxPendingMessageBytes (64 KiB) of messages waiting for a worker
	// pause reading from the connection.
	DefaultMaxPendingMessageBytes int64 = 64 << 10
	// DefaultLowPendingMessageBytes (16 KiB): reading resumes below this.
	DefaultLowPendingMessageBytes int64 = 16 << 10
	// DefaultCompressionThreshold is the smallest message worth compressing.
	DefaultCompressionThreshold = 128
)

// ErrMessageTooBig is returned when a message inflates past the limit.
var ErrMessageTooBig = errors.New("fnet/websocket: decompressed message exceeds maximum allowed size (possible compression bomb)")

// Upgrader performs the WebSocket handshake.
type Upgrader struct {
	// Subprotocols lists the supported subprotocols.
	Subprotocols []string
	// CheckOrigin rejects the request when it returns false. Nil accepts all.
	CheckOrigin func(r *http.Request) bool
	// Header is added to the 101 response.
	Header http.Header

	// EnableCompression negotiates permessage-deflate with clients that ask.
	EnableCompression bool
	// CompressionLevel is the flate level (-2..9); 0 means flate.DefaultCompression.
	CompressionLevel int
	// CompressionThreshold is the smallest message compressed; 0 means 128.
	CompressionThreshold int

	// MaxDecompressedMessageSize bounds an inflated message: 0 means 16 MiB,
	// negative disables the limit.
	MaxDecompressedMessageSize int64
	// MaxMessageSize bounds a received message: 0 means 32 MiB, negative
	// disables the limit.
	MaxMessageSize int64
	// MaxPendingMessageBytes of messages waiting for a worker pause reading
	// until they drain to a quarter of it: 0 means 64 KiB, negative disables.
	MaxPendingMessageBytes int64

	// WorkerPool runs OnMessage (e.g. p.SubmitConn for a *pool.Pool p, or
	// pool.Adapt(ants.Submit)). It is called on an event loop and must not
	// block; an error refuses the task and closes that connection. Defaults
	// to pool.Default().
	WorkerPool func(connID uint64, task func()) error

	// Setting OnMessage makes Upgrade event-driven; see EventHandler.
	OnOpen    func(c *Conn)
	OnMessage func(c *Conn, op OpCode, payload []byte)
	OnClose   func(c *Conn, err error)
	// OnPong, if set, receives the Pong frames, in both modes; see
	// EventHandler.
	OnPong func(c *Conn, data []byte)
}

// EventHandler holds the callbacks of an event-driven connection.
//
//   - OnOpen runs once, in the upgrading HTTP handler, before any OnMessage.
//   - OnMessage runs on the worker pool, one call at a time per connection and
//     in arrival order, so it may block on databases or downstream calls.
//     payload is only valid during the call; copy what must outlive it.
//   - OnClose runs once, on the worker pool, after the messages that arrived
//     before the close.
//   - OnPong, if set, runs on the worker pool for each Pong frame, in order
//     with the messages: with a server-side Ping and SetReadDeadline it tells
//     live peers from vanished ones.
//
// A panic in OnMessage closes the connection with status 1011.
type EventHandler struct {
	OnOpen    func(c *Conn)
	OnMessage func(c *Conn, op OpCode, payload []byte)
	OnClose   func(c *Conn, err error)
	OnPong    func(c *Conn, data []byte)
}

// DefaultUpgrader is an Upgrader with default settings.
var DefaultUpgrader = &Upgrader{}

// Upgrade upgrades with DefaultUpgrader.
func Upgrade(w http.ResponseWriter, r *http.Request) (*Conn, error) {
	return DefaultUpgrader.Upgrade(w, r)
}

// UpgradeEvent upgrades with DefaultUpgrader in event-driven mode.
func UpgradeEvent(w http.ResponseWriter, r *http.Request, h EventHandler) (*Conn, error) {
	return DefaultUpgrader.UpgradeEvent(w, r, h)
}

// Upgrade upgrades the request. With OnMessage set the connection is
// event-driven; otherwise the returned Conn is read with ReadMessage.
func (u *Upgrader) Upgrade(w http.ResponseWriter, r *http.Request) (*Conn, error) {
	if u.OnMessage != nil {
		return u.UpgradeEvent(w, r, EventHandler{OnOpen: u.OnOpen, OnMessage: u.OnMessage, OnClose: u.OnClose, OnPong: u.OnPong})
	}
	return u.handshake(w, r)
}

// UpgradeEvent upgrades the request and drives the connection with h. On an
// fhttp server the connection moves onto the event loop and the HTTP handler
// should return; it then holds no goroutine while idle. Where the loop cannot
// read the stream (TLS, or another HTTP server) a goroutine delivers the same
// callbacks.
func (u *Upgrader) UpgradeEvent(w http.ResponseWriter, r *http.Request, h EventHandler) (*Conn, error) {
	c, err := u.handshake(w, r)
	if err != nil {
		return nil, err
	}
	c.onPong = h.OnPong
	maxPending := u.MaxPendingMessageBytes
	if maxPending == 0 {
		maxPending = DefaultMaxPendingMessageBytes
	}
	e := &eventConn{
		c:          c,
		handler:    h,
		submit:     u.WorkerPool,
		maxPending: maxPending,
		lowPending: maxPending / 4,
	}

	if hc, ok := c.nc.(interface{ Unwrap() *reactor.Conn }); ok {
		c.raw = hc.Unwrap()
	}
	if c.raw == nil {
		if h.OnOpen != nil {
			h.OnOpen(c)
		}
		go e.serveBlocking()
		return c, nil
	}
	c.nc, c.br = c.raw, nil
	if h.OnOpen != nil {
		h.OnOpen(c)
	}
	c.raw.Attach(e) // frames flow from here on, strictly after OnOpen
	return c, nil
}

func (u *Upgrader) handshake(w http.ResponseWriter, r *http.Request) (*Conn, error) {
	if u.CheckOrigin != nil && !u.CheckOrigin(r) {
		http.Error(w, "origin not allowed", http.StatusForbidden)
		return nil, errors.New("fnet/websocket: origin not allowed")
	}
	if err := checkUpgrade(w, r); err != nil {
		return nil, err
	}
	up := ws.HTTPUpgrader{Header: u.Header}
	if len(u.Subprotocols) > 0 {
		up.Protocol = func(proto string) bool {
			for _, sp := range u.Subprotocols {
				if sp == proto {
					return true
				}
			}
			return false
		}
	}
	var ext wsflate.Extension
	if u.EnableCompression {
		ext.Parameters = wsflate.DefaultParameters
		up.Negotiate = func(opt httphead.Option) (httphead.Option, error) {
			if bytes.Equal(opt.Name, wsflate.ExtensionNameBytes) {
				return ext.Negotiate(opt)
			}
			return httphead.Option{}, nil
		}
	}
	nc, brw, hs, err := up.Upgrade(r, w)
	if err != nil {
		if nc != nil {
			// The upgrader hijacked the connection before failing (a bad
			// subprotocol or extension offer) and answered: the connection is
			// ours to close.
			_ = nc.Close()
		}
		return nil, err
	}

	var br io.Reader = nc
	if brw != nil && brw.Reader != nil {
		br = brw.Reader // it may hold bytes read ahead of the handshake
	}
	c := &Conn{
		nc:                nc,
		br:                br,
		protocol:          hs.Protocol,
		compressLevel:     orDefault(u.CompressionLevel, flate.DefaultCompression),
		compressThreshold: orDefault(u.CompressionThreshold, DefaultCompressionThreshold),
		maxDecompressSize: orDefault(u.MaxDecompressedMessageSize, DefaultMaxDecompressedMessageSize),
		maxMessageSize:    orDefault(u.MaxMessageSize, DefaultMaxMessageSize),
		onPong:            u.OnPong,
	}
	if u.EnableCompression {
		_, c.compressed = ext.Accepted()
	}
	return c, nil
}

func orDefault[T int | int64](v, def T) T {
	if v == 0 {
		return def
	}
	return v
}

// checkUpgrade refuses, with a plain HTTP error, a request that is not a
// WebSocket handshake (RFC 6455 4.2.1): the checks the upgrader makes only
// after hijacking the connection, which would leave a refused connection
// hijacked and open.
func checkUpgrade(w http.ResponseWriter, r *http.Request) error {
	var err error
	switch v := r.Header.Get("Sec-WebSocket-Version"); {
	case r.Method != http.MethodGet:
		err = ws.ErrHandshakeBadMethod
	case !r.ProtoAtLeast(1, 1):
		err = ws.ErrHandshakeBadProtocol
	case r.Host == "":
		err = ws.ErrHandshakeBadHost
	case !strings.EqualFold(r.Header.Get("Upgrade"), "websocket"):
		err = ws.ErrHandshakeBadUpgrade
	case !hasToken(r.Header.Get("Connection"), "upgrade"):
		err = ws.ErrHandshakeBadConnection
	case len(r.Header.Get("Sec-WebSocket-Key")) != 24:
		err = ws.ErrHandshakeBadSecKey
	case v == "":
		err = ws.ErrHandshakeBadSecVersion
	case v != "13":
		w.Header().Set("Sec-WebSocket-Version", "13")
		err = ws.ErrHandshakeUpgradeRequired
	default:
		return nil
	}
	code := http.StatusBadRequest
	if rej, ok := err.(*ws.ConnectionRejectedError); ok {
		code = rej.StatusCode()
	}
	http.Error(w, err.Error(), code)
	return err
}

// hasToken reports whether the comma-separated list v holds token, in any case.
func hasToken(v, token string) bool {
	for t := range strings.SplitSeq(v, ",") {
		if strings.EqualFold(strings.TrimSpace(t), token) {
			return true
		}
	}
	return false
}
