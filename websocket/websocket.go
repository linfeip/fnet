// Package websocket is a WebSocket (RFC 6455) server built on fhttp; the frame format and protocol validation use
// github.com/gobwas/ws.
//
// A connection's data is read and processed by fnet's executor (see fnet.Options.Executor, taskpool.DefaultTaskPool
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
	"net/http"
	"strings"
	"time"

	"github.com/linfeip/fnet"
	"github.com/linfeip/fnet/fhttp"
	"github.com/linfeip/fnet/internal/units"

	"github.com/gobwas/ws"
)

// Handler receives the event callbacks of a WebSocket connection. OnMessage and OnClose run on a goroutine of
// fnet's executor and must return quickly: when several frames arrive at once, the frames written while they are
// being processed are merged into a single write (see Conn.WriteMessage), so one blocking callback delays replies
// already written earlier in the same batch; the connection is not read again until the callback returns, and the
// callback also occupies a goroutine of the executor (the default taskpool.DefaultTaskPool has about as many
// workers as there are CPUs), slowing down the callbacks of other connections. Time-consuming logic should be
// handed off to another goroutine.
// Callbacks of one connection always run serially: OnOpen first, OnClose last and only once.
// After a local Close, a close frame from the peer or a protocol error, OnMessage is no longer called: messages not
// yet processed in the same batch and data arriving afterwards are all discarded.
// When a callback panics the connection is closed with 1011 and only OnClose is called afterwards; the panic
// propagates upwards and is recovered by the executor (a panic in OnOpen is recovered by fhttp).
type Handler interface {
	// OnOpen is called after a successful handshake and runs on the goroutine that called Upgrade.
	OnOpen(c *Conn)
	// OnMessage is called after a complete message has been received (fragments already reassembled); op is
	// ws.OpText or ws.OpBinary. data is valid only within this callback (it may be the engine's read buffer), so
	// copy it if you need to keep it.
	OnMessage(c *Conn, op ws.OpCode, data []byte)
	// OnClose is called after the connection has closed and all previously received messages have been delivered.
	// err is the reason for closing: nil for a local Close; wsutil.ClosedError when the peer sent a close frame;
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
	// To detect idle connections, the server can send a ping periodically (WriteMessage(ws.OpPing, nil)); the pong
	// the client replies with refreshes the timer.
	IdleTimeout time.Duration
}

func (o Options) withDefaults() Options {
	if o.MaxMessageSize <= 0 {
		o.MaxMessageSize = units.MB
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
	key, err := checkHandshake(r)
	if err != nil {
		if err == ws.ErrHandshakeUpgradeRequired {
			w.Header().Set("Sec-WebSocket-Version", "13")
		}
		http.Error(w, err.Error(), err.(*ws.ConnectionRejectedError).StatusCode())
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
		nc.PauseRead()                               // no reads until OnOpen returns: peer data sent after the 101 waits in the kernel
		nc.SetDeadline(deadlineAfter(c.idleTimeout)) // set before the protocol switch; cannot race with OnData in a callback
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
			c.closeWith(ws.StatusInternalServerError, errHandlerPanic)
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
// returns the Sec-WebSocket-Key.
// Every error it returns is a *ws.ConnectionRejectedError carrying a response status code.
func checkHandshake(r *http.Request) (key string, err error) {
	switch {
	case r.Method != http.MethodGet:
		return "", ws.ErrHandshakeBadMethod
	case !r.ProtoAtLeast(1, 1):
		return "", ws.ErrHandshakeBadProtocol
	case !strings.EqualFold(r.Header.Get("Upgrade"), "websocket"):
		return "", ws.ErrHandshakeBadUpgrade
	case !hasToken(r.Header["Connection"], "upgrade"):
		return "", ws.ErrHandshakeBadConnection
	}
	switch r.Header.Get("Sec-WebSocket-Version") {
	case "13":
	case "":
		return "", ws.ErrHandshakeBadSecVersion
	default:
		return "", ws.ErrHandshakeUpgradeRequired
	}
	if key = r.Header.Get("Sec-WebSocket-Key"); len(key) != 24 { // base64 of a 16-byte random value
		return "", ws.ErrHandshakeBadSecKey
	}
	return key, nil
}

// acceptKey computes Sec-WebSocket-Accept (RFC 6455 4.2.2).
func acceptKey(key string) string {
	sum := sha1.Sum([]byte(key + "258EAFA5-E914-47DA-95CA-C5AB0DC85B11"))
	return base64.StdEncoding.EncodeToString(sum[:])
}

// hasToken reports whether the comma-separated header values contain token (case-insensitive).
func hasToken(values []string, token string) bool {
	for _, v := range values {
		for v != "" {
			var t string
			t, v, _ = strings.Cut(v, ",")
			if strings.EqualFold(strings.TrimSpace(t), token) {
				return true
			}
		}
	}
	return false
}
