package websocket

import (
	"bytes"
	"compress/flate"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"sync"
	"sync/atomic"
	"time"
	"unicode/utf8"

	"github.com/linfeip/fnet"

	"github.com/gobwas/httphead"
	"github.com/gobwas/ws"
	"github.com/gobwas/ws/wsflate"
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

// DefaultMaxDecompressedMessageSize is the default limit (16MB) on decompressed payload size
// to protect against decompression bombs (zip bombs).
const DefaultMaxDecompressedMessageSize int64 = 16 * 1024 * 1024

// DefaultCompressionThreshold is the minimum payload size in bytes to trigger compression.
const DefaultCompressionThreshold = 128

// ErrMessageTooBig is returned when a decompressed message exceeds the configured limit.
var ErrMessageTooBig = errors.New("fnet/websocket: decompressed message exceeds maximum allowed size (possible compression bomb)")

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

	// EnableCompression enables RFC 7692 permessage-deflate compression extension.
	// When true, the server negotiates compression with clients requesting it.
	EnableCompression bool

	// MaxDecompressedMessageSize limits the maximum allowable decompressed message size
	// in bytes to prevent decompression bombs (zip bombs).
	// If 0, DefaultMaxDecompressedMessageSize (16MB) is used.
	// If negative, size limit is disabled (not recommended in production).
	MaxDecompressedMessageSize int64

	// CompressionLevel specifies the flate compression level (-1 to 9).
	// If 0, flate.DefaultCompression (-1) is used.
	CompressionLevel int

	// CompressionThreshold specifies the minimum message size in bytes to trigger compression.
	// Defaults to 128 bytes if 0.
	CompressionThreshold int

	// WorkerPool is an optional worker pool function (e.g. pool.Submit, ants.Submit, or custom scheduler)
	// used to execute async tasks. If nil, the built-in, highly scalable DefaultWorkerPool is automatically used.
	WorkerPool func(task func())

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

type wsTask struct {
	op           OpCode
	payload      []byte
	pooled       *pooledBuffer
	isCompressed bool
}

var nextBridgeID atomic.Uint64

type wsHandlerBridge struct {
	id         uint64
	conn       *Conn
	handler    EventHandler
	fragOp     OpCode
	fragBuf    []byte
	fragComp   bool

	workerPool func(task func())
	queueMu    sync.Mutex
	queue      []wsTask
	batch      []wsTask
	running    bool
	closed     bool
	closeOnce  sync.Once
}

var _ fnet.WSFrameHandler = (*wsHandlerBridge)(nil)

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

func (b *wsHandlerBridge) enqueueTask(op OpCode, payload []byte, isCompressed bool) {
	if len(payload) == 0 {
		b.enqueueTaskOwned(op, nil, nil, isCompressed)
		return
	}
	buf, pb := getPayloadBuffer(len(payload))
	copy(buf, payload)
	b.enqueueTaskOwned(op, buf, pb, isCompressed)
}

func (b *wsHandlerBridge) enqueueTaskOwned(op OpCode, payload []byte, pooled *pooledBuffer, isCompressed bool) {
	b.queueMu.Lock()
	if b.closed {
		b.queueMu.Unlock()
		if pooled != nil {
			putPayloadBuffer(pooled)
		}
		return
	}
	b.queue = append(b.queue, wsTask{
		op:           op,
		payload:      payload,
		pooled:       pooled,
		isCompressed: isCompressed,
	})
	if !b.running {
		b.running = true
		b.queueMu.Unlock()
		b.schedule(b.processQueue)
		return
	}
	b.queueMu.Unlock()
}

func (b *wsHandlerBridge) schedule(task func()) {
	if b.workerPool != nil {
		b.workerPool(task)
	} else {
		DefaultWorkerPool.SubmitConn(b.id, task)
	}
}

func (b *wsHandlerBridge) processQueue() {
	for {
		b.queueMu.Lock()
		if len(b.queue) == 0 || b.closed {
			b.running = false
			b.queue = b.queue[:0]
			b.queueMu.Unlock()
			return
		}
		// Batch drain: swap active queue with local batch slice to minimize lock duration
		b.batch, b.queue = b.queue, b.batch[:0]
		b.queueMu.Unlock()

		for i := range b.batch {
			task := b.batch[i]
			b.batch[i] = wsTask{} // Clear pointer for GC

			if b.closed {
				if task.pooled != nil {
					putPayloadBuffer(task.pooled)
				}
				continue
			}

			b.executeTask(task)
			if task.pooled != nil {
				putPayloadBuffer(task.pooled)
			}
		}
		b.batch = b.batch[:0]
	}
}

func (b *wsHandlerBridge) executeTask(task wsTask) {
	msgPayload := task.payload
	if task.isCompressed {
		decompressed, err := decompressMessage(task.payload, b.conn.maxDecompressSize)
		if err != nil {
			if errors.Is(err, ErrMessageTooBig) {
				_ = b.conn.CloseWithStatus(ws.StatusMessageTooBig, "decompressed message too large")
			} else {
				_ = b.conn.CloseWithStatus(ws.StatusProtocolError, "decompression error")
			}
			b.markClosed()
			b.OnClose(err)
			return
		}
		msgPayload = decompressed
	}

	if task.op == OpText && !utf8.Valid(msgPayload) {
		_ = b.conn.CloseWithStatus(ws.StatusInvalidFramePayloadData, "invalid UTF-8 in text message")
		b.markClosed()
		return
	}

	if b.handler.OnMessage != nil {
		b.handler.OnMessage(b.conn, task.op, msgPayload)
	}
}

func (b *wsHandlerBridge) markClosed() {
	b.queueMu.Lock()
	b.closed = true
	for i := range b.queue {
		if b.queue[i].pooled != nil {
			putPayloadBuffer(b.queue[i].pooled)
		}
		b.queue[i] = wsTask{}
	}
	b.queue = b.queue[:0]
	b.queueMu.Unlock()
}

func checkClosePayload(payload []byte) (ws.StatusCode, bool) {
	if len(payload) == 1 {
		return ws.StatusProtocolError, false
	}
	if len(payload) >= 2 {
		code := ws.StatusCode(binary.BigEndian.Uint16(payload[:2]))
		reason := string(payload[2:])
		if code >= 5000 {
			return ws.StatusProtocolError, false
		}
		if err := ws.CheckCloseFrameData(code, reason); err != nil {
			if errors.Is(err, ws.ErrProtocolInvalidUTF8) {
				return ws.StatusInvalidFramePayloadData, false
			}
			return ws.StatusProtocolError, false
		}
		return code, true
	}
	return ws.StatusNormalClosure, true
}

func (b *wsHandlerBridge) OnFrame(h ws.Header, payload []byte) {
	// 1. RSV validation (RFC 6455 5.2 & RFC 7692 5.1)
	if !b.conn.compressed {
		if h.Rsv != 0 {
			_ = b.conn.CloseWithStatus(ws.StatusProtocolError, "non-zero RSV without compression")
			return
		}
	} else {
		if h.Rsv2() || h.Rsv3() {
			_ = b.conn.CloseWithStatus(ws.StatusProtocolError, "non-zero RSV2/RSV3")
			return
		}
		if h.OpCode == ws.OpContinuation && h.Rsv1() {
			_ = b.conn.CloseWithStatus(ws.StatusProtocolError, "RSV1 set on continuation frame")
			return
		}
	}

	// 2. Unfragmented frame
	if b.fragOp == 0 {
		if h.OpCode == ws.OpContinuation {
			_ = b.conn.CloseWithStatus(ws.StatusProtocolError, "unexpected continuation frame")
			return
		}

		if !h.Fin {
			// First fragment of fragmented message
			b.fragOp = OpCode(h.OpCode)
			b.fragComp = b.conn.compressed && h.Rsv1()
			b.fragBuf = append(b.fragBuf[:0], payload...)
			return
		}

		// Single complete frame
		isComp := b.conn.compressed && h.Rsv1()
		b.enqueueTask(OpCode(h.OpCode), payload, isComp)
		return
	}

	// 3. In the middle of a fragmented message (b.fragOp != 0)
	if h.OpCode != ws.OpContinuation {
		_ = b.conn.CloseWithStatus(ws.StatusProtocolError, "expected continuation frame")
		return
	}

	b.fragBuf = append(b.fragBuf, payload...)
	if h.Fin {
		op := b.fragOp
		fullPayload := append([]byte(nil), b.fragBuf...)
		isComp := b.fragComp
		b.fragOp = 0
		b.fragBuf = b.fragBuf[:0]
		b.fragComp = false

		b.enqueueTaskOwned(op, fullPayload, nil, isComp)
		return
	}
}

func (b *wsHandlerBridge) OnClose(err error) {
	b.queueMu.Lock()
	b.closed = true
	for i := range b.queue {
		if b.queue[i].pooled != nil {
			putPayloadBuffer(b.queue[i].pooled)
		}
		b.queue[i] = wsTask{}
	}
	b.queue = b.queue[:0]
	b.queueMu.Unlock()

	b.closeOnce.Do(func() {
		if b.handler.OnClose != nil {
			b.handler.OnClose(b.conn, err)
		}
	})
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

	bridge := &wsHandlerBridge{
		id:         nextBridgeID.Add(1),
		conn:       conn,
		handler:    h,
		workerPool: u.WorkerPool,
	}
	if attacher != nil {
		vc, err := attacher.AttachWS(bridge)
		if err == nil {
			if vc != nil {
				conn.conn = vc
				conn.reader = nil
				conn.rw = readWriter{}
			}
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

	var (
		ext              wsflate.Extension
		compressAccepted bool
	)
	if u.EnableCompression {
		ext = wsflate.Extension{
			Parameters: wsflate.DefaultParameters,
		}
		httpUpgrader.Negotiate = func(opt httphead.Option) (httphead.Option, error) {
			if bytes.Equal(opt.Name, wsflate.ExtensionNameBytes) {
				return ext.Negotiate(opt)
			}
			return httphead.Option{}, nil
		}
	}

	netConn, brw, hs, err := httpUpgrader.Upgrade(r, w)
	if err != nil {
		return nil, err
	}

	if u.EnableCompression {
		_, compressAccepted = ext.Accepted()
	}

	// Determine read source: prefer brw.Reader (which has any read-ahead bytes),
	// falling back to netConn.
	var reader io.Reader = netConn
	if brw != nil && brw.Reader != nil {
		reader = brw.Reader
	}

	maxDecompress := u.MaxDecompressedMessageSize
	if maxDecompress == 0 {
		maxDecompress = DefaultMaxDecompressedMessageSize
	}
	compLevel := u.CompressionLevel
	if compLevel == 0 {
		compLevel = flate.DefaultCompression
	}
	compThreshold := u.CompressionThreshold
	if compThreshold == 0 {
		compThreshold = DefaultCompressionThreshold
	}

	return &Conn{
		conn:              netConn,
		reader:            reader,
		rw:                readWriter{Reader: reader, Writer: netConn},
		protocol:          hs.Protocol,
		compressed:        compressAccepted,
		maxDecompressSize: maxDecompress,
		compressLevel:     compLevel,
		compressThreshold: compThreshold,
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

var (
	flateTail     = []byte{0x00, 0x00, 0xff, 0xff}
	flateReadTail = []byte{0x00, 0x00, 0xff, 0xff, 0x01, 0x00, 0x00, 0xff, 0xff}
)

// decompressMessage decompresses a permessage-deflate payload while enforcing
// maxLimit bytes to prevent decompression bombs.
func decompressMessage(payload []byte, maxLimit int64) ([]byte, error) {
	if len(payload) == 0 {
		return nil, nil
	}
	// Append RFC 7692 tail + empty final block so Go's flate.Reader finishes cleanly
	r := flate.NewReader(io.MultiReader(bytes.NewReader(payload), bytes.NewReader(flateReadTail)))
	defer r.Close()

	var limit int64
	if maxLimit > 0 {
		limit = maxLimit
	} else if maxLimit == 0 {
		limit = DefaultMaxDecompressedMessageSize
	}

	var buf bytes.Buffer
	if limit > 0 {
		limited := io.LimitReader(r, limit+1)
		n, err := buf.ReadFrom(limited)
		if err != nil {
			return nil, err
		}
		if n > limit {
			return nil, ErrMessageTooBig
		}
	} else {
		if _, err := buf.ReadFrom(r); err != nil {
			return nil, err
		}
	}
	return buf.Bytes(), nil
}

func compressMessage(payload []byte, level int) ([]byte, error) {
	if len(payload) == 0 {
		return nil, nil
	}
	var buf bytes.Buffer
	w, err := flate.NewWriter(&buf, level)
	if err != nil {
		return nil, err
	}
	if _, err := w.Write(payload); err != nil {
		return nil, err
	}
	if err := w.Flush(); err != nil {
		return nil, err
	}
	b := buf.Bytes()
	if len(b) >= 4 && bytes.Equal(b[len(b)-4:], flateTail) {
		b = b[:len(b)-4]
	}
	return b, nil
}

// formatServerHeader encodes an unmasked server WebSocket frame header into bts.
// It returns the number of bytes written (2, 4, or 10).
func formatServerHeader(bts []byte, op OpCode, length int, compressed bool) int {
	b0 := 0x80 | byte(op)
	if compressed {
		b0 |= 0x40 // RSV1: permessage-deflate
	}
	bts[0] = b0
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
	conn              net.Conn
	reader            io.Reader
	rw                readWriter
	protocol          string
	closed            atomic.Bool
	writeMu           sync.Mutex
	compressed        bool
	maxDecompressSize int64
	compressLevel     int
	compressThreshold int
}

// IsCompressed reports whether permessage-deflate compression is active on this connection.
func (c *Conn) IsCompressed() bool {
	return c.compressed
}

// SetMaxDecompressedMessageSize updates the limit on decompressed message size for this connection.
func (c *Conn) SetMaxDecompressedMessageSize(limit int64) {
	c.maxDecompressSize = limit
}

// CloseWithStatus closes the connection with a specific WebSocket close status and reason.
func (c *Conn) CloseWithStatus(status ws.StatusCode, reason string) error {
	if c.closed.CompareAndSwap(false, true) {
		c.writeMu.Lock()
		_ = c.writeFrameLocked(OpClose, ws.NewCloseFrameBody(status, reason))
		c.writeMu.Unlock()
		return c.conn.Close()
	}
	return nil
}

func (c *Conn) writeFrameLocked(op OpCode, payload []byte) error {
	compressed := false
	toWrite := payload

	if c.compressed && (op == OpText || op == OpBinary) && len(payload) >= c.compressThreshold {
		if comp, err := compressMessage(payload, c.compressLevel); err == nil && len(comp) < len(payload) {
			toWrite = comp
			compressed = true
		}
	}

	var hBuf [10]byte
	hLen := formatServerHeader(hBuf[:], op, len(toWrite), compressed)

	if vw, ok := c.conn.(VectorWriter); ok {
		var iovs [2][]byte
		iovs[0] = hBuf[:hLen]
		if len(toWrite) > 0 {
			iovs[1] = toWrite
			_, err := vw.WriteVector(iovs[:2])
			return err
		}
		_, err := vw.WriteVector(iovs[:1])
		return err
	}

	if len(toWrite) == 0 {
		_, err := c.conn.Write(hBuf[:hLen])
		return err
	}

	if _, err := c.conn.Write(hBuf[:hLen]); err != nil {
		return err
	}
	_, err := c.conn.Write(toWrite)
	return err
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

		if !header.Masked {
			_ = c.CloseWithStatus(ws.StatusProtocolError, "unmasked client frame")
			return 0, nil, ws.ErrProtocolMaskRequired
		}

		if header.OpCode.IsReserved() {
			_ = c.CloseWithStatus(ws.StatusProtocolError, "reserved opcode")
			return 0, nil, ws.ErrProtocolOpCodeReserved
		}

		if header.OpCode.IsControl() {
			if !header.Fin || header.Length > 125 || header.Rsv != 0 {
				_ = c.CloseWithStatus(ws.StatusProtocolError, "invalid control frame")
				return 0, nil, ws.ErrProtocolControlPayloadOverflow
			}

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
				code, ok := checkClosePayload(ctrlPayload)
				if !ok {
					_ = c.CloseWithStatus(code, "")
					return 0, nil, io.EOF
				}
				_ = c.CloseWithStatus(code, "")
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

		// Non-control frame
		if header.OpCode == OpContinuation {
			_ = c.CloseWithStatus(ws.StatusProtocolError, "unexpected continuation frame")
			return 0, nil, ws.ErrProtocolContinuationUnexpected
		}

		if !c.compressed {
			if header.Rsv != 0 {
				_ = c.CloseWithStatus(ws.StatusProtocolError, "non-zero RSV")
				return 0, nil, ws.ErrProtocolNonZeroRsv
			}
		} else {
			if header.Rsv2() || header.Rsv3() {
				_ = c.CloseWithStatus(ws.StatusProtocolError, "non-zero RSV2/RSV3")
				return 0, nil, ws.ErrProtocolNonZeroRsv
			}
		}

		// Data frame (Text, Binary)
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
			if c.compressed && header.Rsv1() {
				decompressed, err := decompressMessage(payload, c.maxDecompressSize)
				if err != nil {
					if errors.Is(err, ErrMessageTooBig) {
						_ = c.CloseWithStatus(ws.StatusMessageTooBig, "decompressed message too large")
					} else {
						_ = c.CloseWithStatus(ws.StatusProtocolError, "decompression error")
					}
					return 0, nil, err
				}
				if header.OpCode == OpText && !utf8.Valid(decompressed) {
					_ = c.CloseWithStatus(ws.StatusInvalidFramePayloadData, "invalid UTF-8")
					return 0, nil, ws.ErrProtocolInvalidUTF8
				}
				return header.OpCode, decompressed, nil
			}
			if header.OpCode == OpText && !utf8.Valid(payload) {
				_ = c.CloseWithStatus(ws.StatusInvalidFramePayloadData, "invalid UTF-8")
				return 0, nil, ws.ErrProtocolInvalidUTF8
			}
			return header.OpCode, payload, nil
		}

		// Handle fragmented messages: read subsequent continuation frames
		msgOp := header.OpCode
		isCompressed := c.compressed && header.Rsv1()
		for {
			nextHdr, err := ws.ReadHeader(c.reader)
			if err != nil {
				return 0, nil, err
			}
			if !nextHdr.Masked {
				_ = c.CloseWithStatus(ws.StatusProtocolError, "unmasked client frame")
				return 0, nil, ws.ErrProtocolMaskRequired
			}
			if nextHdr.OpCode.IsControl() {
				if !nextHdr.Fin || nextHdr.Length > 125 || nextHdr.Rsv != 0 {
					_ = c.CloseWithStatus(ws.StatusProtocolError, "invalid control frame")
					return 0, nil, ws.ErrProtocolControlPayloadOverflow
				}
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
				if nextHdr.OpCode == OpClose {
					code, ok := checkClosePayload(ctrlPayload)
					if !ok {
						_ = c.CloseWithStatus(code, "")
						return 0, nil, io.EOF
					}
					_ = c.CloseWithStatus(code, "")
					return 0, nil, io.EOF
				}
				if nextHdr.OpCode == OpPing {
					c.writeMu.Lock()
					_ = c.writeFrameLocked(OpPong, ctrlPayload)
					c.writeMu.Unlock()
				}
				continue
			}

			if nextHdr.OpCode != OpContinuation {
				_ = c.CloseWithStatus(ws.StatusProtocolError, "expected continuation frame")
				return 0, nil, ws.ErrProtocolContinuationExpected
			}
			if nextHdr.Rsv != 0 {
				_ = c.CloseWithStatus(ws.StatusProtocolError, "non-zero RSV on continuation frame")
				return 0, nil, ws.ErrProtocolNonZeroRsv
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

		if isCompressed {
			decompressed, err := decompressMessage(payload, c.maxDecompressSize)
			if err != nil {
				if errors.Is(err, ErrMessageTooBig) {
					_ = c.CloseWithStatus(ws.StatusMessageTooBig, "decompressed message too large")
				} else {
					_ = c.CloseWithStatus(ws.StatusProtocolError, "decompression error")
				}
				return 0, nil, err
			}
			if msgOp == OpText && !utf8.Valid(decompressed) {
				_ = c.CloseWithStatus(ws.StatusInvalidFramePayloadData, "invalid UTF-8")
				return 0, nil, ws.ErrProtocolInvalidUTF8
			}
			return msgOp, decompressed, nil
		}

		if msgOp == OpText && !utf8.Valid(payload) {
			_ = c.CloseWithStatus(ws.StatusInvalidFramePayloadData, "invalid UTF-8")
			return 0, nil, ws.ErrProtocolInvalidUTF8
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
