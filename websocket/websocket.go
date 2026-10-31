// Package websocket is a WebSocket (RFC 6455) server built on fhttp.
//
// A connection's data is read and processed by fnet's executor (see fnet.Options.Executor, a private taskpool.NewDefault()
// by default) in the connection's task: splitting frames, unmasking, replying to ping and close frames, and
// invoking the Handler in order. For an unfragmented message the payload is taken straight from the read buffer,
// with no copy and no queueing; a fragmented message is delivered once it has been reassembled in full. An idle
// connection occupies no goroutine.
//
// When the application cannot keep up, the connection is not read again until the callback returns; the data stays
// in the kernel buffer and TCP flow control makes the peer slow down (backpressure).
//
// The handshake is completed by calling Upgrade from fhttp's http.Handler, after which websocket takes over the
// connection. Every message must be received in full within MessageTimeout; the idle timeout is off by default (see
// Options). Extensions (such as permessage-deflate) are not supported; outgoing messages are always a single frame.
package websocket

import (
	"crypto/sha1"
	"encoding/base64"
	"errors"
	"net/http"
	"strconv"
	"time"

	"github.com/linfeip/fnet"
	"github.com/linfeip/fnet/fhttp"
	"github.com/linfeip/fnet/internal/httpx"
)

// OpCode is the operation code of a WebSocket frame (RFC 6455 5.2).
type OpCode byte

const (
	OpContinuation OpCode = 0x0
	OpText         OpCode = 0x1
	OpBinary       OpCode = 0x2
	OpClose        OpCode = 0x8
	OpPing         OpCode = 0x9
	OpPong         OpCode = 0xa
)

func (op OpCode) isControl() bool { return op&0x08 != 0 }

// StatusCode is a WebSocket close status code (RFC 6455 7.4).
type StatusCode uint16

const (
	StatusNormalClosure           StatusCode = 1000
	StatusGoingAway               StatusCode = 1001
	StatusProtocolError           StatusCode = 1002
	StatusUnsupportedData         StatusCode = 1003
	StatusNoStatusRcvd            StatusCode = 1005
	StatusAbnormalClosure         StatusCode = 1006
	StatusInvalidFramePayloadData StatusCode = 1007
	StatusPolicyViolation         StatusCode = 1008
	StatusMessageTooBig           StatusCode = 1009
	StatusMandatoryExtension      StatusCode = 1010
	StatusInternalServerError     StatusCode = 1011
	StatusServiceRestart          StatusCode = 1012
	StatusTryAgainLater           StatusCode = 1013
	StatusBadGateway              StatusCode = 1014
	StatusTLSHandshake            StatusCode = 1015
)

// ClosedError is passed to Handler.OnClose when the peer closed the connection with a close frame.
type ClosedError struct {
	Code   StatusCode
	Reason string
}

func (e ClosedError) Error() string {
	return "websocket: peer closed (" + strconv.Itoa(int(e.Code)) + "): " + e.Reason
}

var (
	errHandshakeBadMethod       = errors.New("websocket: handshake request method is not GET")
	errHandshakeBadProtocol     = errors.New("websocket: handshake HTTP version is not at least 1.1")
	errHandshakeBadUpgrade      = errors.New("websocket: missing or invalid Upgrade header")
	errHandshakeBadConnection   = errors.New("websocket: missing or invalid Connection header")
	errHandshakeBadSecVersion   = errors.New("websocket: missing or multiple Sec-WebSocket-Version headers")
	errHandshakeUpgradeRequired = errors.New("websocket: unsupported Sec-WebSocket-Version")
	errHandshakeBadSecKey       = errors.New("websocket: missing or invalid Sec-WebSocket-Key")
)

// Handler receives the event callbacks of a WebSocket connection. OnMessage and OnClose run on a goroutine of
// fnet's executor and must return quickly: when several frames arrive at once, the frames written while they are
// being processed are merged into a single write (see Conn.WriteMessage), so one blocking callback delays replies
// already written earlier in the same batch; the connection is not read again until the callback returns, and the
// callback also occupies a goroutine of the executor (the default taskpool.NewDefault() starts more workers as
// callbacks block, up to 512 × GOMAXPROCS, so blocking callbacks are absorbed up to that limit but each one holds a
// goroutine and its stack), slowing down the callbacks of other connections once the limit is reached.
// Time-consuming logic should be handed off to another goroutine.
// Callbacks of one connection always run serially: OnOpen first, OnClose last and only once.
// After a local Close, a close frame from the peer or a protocol error, OnMessage is no longer called: messages not
// yet processed in the same batch and data arriving afterwards are all discarded.
// When a callback panics the connection is closed with 1011 and only OnClose is called afterwards; the panic
// propagates upwards and is recovered by the executor (a panic in OnOpen is recovered by fhttp).
type Handler interface {
	// OnOpen is called after a successful handshake and runs on the goroutine that called Upgrade.
	OnOpen(c *Conn)
	// OnMessage is called after a complete message has been received (fragments already reassembled); op is
	// OpText or OpBinary. data is valid only within this callback (it may be the engine's read buffer), so
	// copy it if you need to keep it.
	OnMessage(c *Conn, op OpCode, data []byte)
	// OnClose is called after the connection has closed and all previously received messages have been delivered.
	// err is the reason for closing: nil for a local Close; ClosedError when the peer sent a close frame;
	// the corresponding error on a protocol error; io.EOF when the peer disconnected outright;
	// os.ErrDeadlineExceeded on timeout (see Options), in which case no close frame is sent.
	OnClose(c *Conn, err error)
}

// Options are the WebSocket parameters; the zero value is the default configuration.
type Options struct {
	// MaxMessageSize is the maximum number of bytes of a single message (after fragments are reassembled); when
	// <=0 it is 1MB. When exceeded, the connection is closed with 1009.
	MaxMessageSize int
	// MessageTimeout is the maximum time from the first byte of a message (or of a frame) until it has been
	// received in full (including all fragments); fragments and control frames arriving in the meantime do not
	// extend it. When 0 it is 30s, when <0 it is unlimited.
	MessageTimeout time.Duration
	// IdleTimeout is the maximum time without receiving any frame while there is no incomplete message; when <=0
	// it is unlimited (the default).
	// To detect idle connections, the server can send a ping periodically (WriteMessage(OpPing, nil)); the pong
	// the client replies with refreshes the timer.
	IdleTimeout time.Duration
}

func (o Options) withDefaults() Options {
	if o.MaxMessageSize <= 0 {
		o.MaxMessageSize = 1 << 20
	}
	if o.MessageTimeout == 0 {
		o.MessageTimeout = 30 * time.Second
	}
	return o
}

// Upgrade completes the WebSocket handshake inside fhttp's http.Handler and hands the connection to h; by the time
// it returns, h.OnOpen has finished running on the current goroutine.
// Headers already set in w.Header() (such as Sec-WebSocket-Protocol and Set-Cookie) are written into the 101
// response as well.
// w must not be used after this call; when the handshake fails, the corresponding HTTP error has already been
// replied and the error is returned.
func Upgrade(w http.ResponseWriter, r *http.Request, h Handler, opts Options) error {
	key, status, err := checkHandshake(r)
	if err != nil {
		if status == http.StatusUpgradeRequired {
			w.Header().Set("Sec-WebSocket-Version", "13")
		}
		http.Error(w, err.Error(), status)
		return err
	}
	header := w.Header()
	header.Set("Upgrade", "websocket")
	header.Set("Connection", "Upgrade")
	header.Set("Sec-WebSocket-Accept", acceptKey(key))

	opts = opts.withDefaults()
	c := &Conn{
		handler:        h,
		maxMessageSize: opts.MaxMessageSize,
		messageTimeout: opts.MessageTimeout,
		idleTimeout:    opts.IdleTimeout,
	}
	c.phase.Store(phaseOpening)
	err = fhttp.Upgrade(w, func(nc fnet.Conn) fhttp.Protocol {
		c.connection = nc
		nc.PauseRead()                                     // no reads until OnOpen returns: peer data sent after the 101 waits in the kernel
		nc.SetDeadline(httpx.DeadlineAfter(c.idleTimeout)) // set before the protocol switch; cannot race with OnData in a callback
		return c
	})
	if err != nil {
		if err == http.ErrNotSupported { // not an fhttp connection; other errors are already handled by fhttp
			http.Error(w, err.Error(), http.StatusInternalServerError)
		}
		return err
	}
	opened := false
	defer func() {
		if !opened { // OnOpen panicked: close the connection with 1011; the panic propagates upwards and is recovered by fhttp
			c.closeWith(StatusInternalServerError, errHandlerPanic)
		}
		// After OnOpen returns (panic included): resume reading; if the engine closed the connection before OnOpen
		// returned (see Conn.OnClose), make up for the missing OnClose
		if c.phase.CompareAndSwap(phaseOpening, phaseOpened) {
			c.connection.ResumeRead()
		} else {
			h.OnClose(c, c.closeErr)
		}
	}()
	h.OnOpen(c)
	opened = true
	return nil
}

// checkHandshake validates the handshake request per RFC 6455 4.2.1 (Host has already been validated by fhttp) and
// returns the Sec-WebSocket-Key or the HTTP status code and error for rejecting the handshake.
func checkHandshake(r *http.Request) (key string, status int, err error) {
	switch {
	case r.Method != http.MethodGet:
		return "", http.StatusMethodNotAllowed, errHandshakeBadMethod
	case !r.ProtoAtLeast(1, 1):
		return "", http.StatusHTTPVersionNotSupported, errHandshakeBadProtocol
	case !httpx.HasToken(r.Header["Upgrade"], "websocket"):
		return "", http.StatusBadRequest, errHandshakeBadUpgrade
	case !httpx.HasToken(r.Header["Connection"], "upgrade"):
		return "", http.StatusBadRequest, errHandshakeBadConnection
	}
	// Neither of these may be repeated (RFC 6455 11.3.1, 11.3.5): the parties on the way could take different ones.
	versions, keys := r.Header["Sec-Websocket-Version"], r.Header["Sec-Websocket-Key"]
	switch {
	case len(versions) != 1:
		return "", http.StatusBadRequest, errHandshakeBadSecVersion
	case versions[0] != "13":
		return "", http.StatusUpgradeRequired, errHandshakeUpgradeRequired
	case len(keys) != 1 || !validNonce(keys[0]):
		return "", http.StatusBadRequest, errHandshakeBadSecKey
	}
	return keys[0], 0, nil
}

// validNonce reports whether key is a Sec-WebSocket-Key: the base64 encoding of 16 bytes (RFC 6455 4.1), 24 characters.
func validNonce(key string) bool {
	var nonce [18]byte
	if len(key) != 24 {
		return false
	}
	n, err := base64.StdEncoding.Decode(nonce[:], []byte(key))
	return err == nil && n == 16
}

// acceptGUID is the GUID RFC 6455 appends to the key to compute Sec-WebSocket-Accept.
const acceptGUID = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11"

// acceptKey computes Sec-WebSocket-Accept (RFC 6455 4.2.2). The key and the GUID are joined in a buffer on the stack:
// checkHandshake lets only 24-byte keys through.
func acceptKey(key string) string {
	var buf [24 + len(acceptGUID)]byte
	sum := sha1.Sum(append(append(buf[:0], key...), acceptGUID...))
	return base64.StdEncoding.EncodeToString(sum[:])
}
