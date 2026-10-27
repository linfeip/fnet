// Package ftls adds TLS to fnet: Handler wraps an fnet.Handler, whose connections then write and receive plaintext over
// TLS.
//
// All of it runs in the engine's callbacks, and a connection costs no goroutine. crypto/tls drives the handshake with
// blocking reads, so the handshake runs as a coroutine (iter.Pull) that OnData starts when the first data arrives, resumes
// with the data that follows and that ends with the handshake, so a connection that has sent nothing costs none either;
// the executor's goroutines passing it on to each other must not be locked to OS threads (runtime.LockOSThread). From then
// on OnData feeds the ciphertext to crypto/tls and passes the plaintext on: once the ciphertext is used up the read
// returns a temporary error, which crypto/tls hands back without failing the connection, keeping a record cut short until
// more data arrives.
package ftls

import (
	"crypto/tls"
	"io"
	"iter"
	"net"
	"sync/atomic"
	"time"

	"github.com/linfeip/fnet"
	"github.com/linfeip/fnet/internal/bytepool"
	"github.com/linfeip/fnet/internal/units"
)

// defaultHandshakeTimeout limits the handshake when NewHandler is given 0.
const defaultHandshakeTimeout = 10 * time.Second

// recordSize is the largest plaintext a TLS record carries, the size of the buffer drain decrypts into.
const recordSize = 16 * units.KB

// errWouldBlock is what a read returns once the handshake is done and the ciphertext at hand is used up: crypto/tls
// hands a temporary net.Error back without failing the connection, keeping a record cut short for the next read.
var errWouldBlock net.Error = wouldBlockError{}

type wouldBlockError struct{}

func (wouldBlockError) Error() string   { return "ftls: no more input" }
func (wouldBlockError) Timeout() bool   { return false }
func (wouldBlockError) Temporary() bool { return true }

// Handler wraps an fnet.Handler with TLS. The wrapped handler's callbacks get a *Conn and keep the guarantees of
// fnet.Handler; OnOpen comes once the handshake has succeeded, and a connection whose handshake fails never reaches the
// handler. Every close of an open connection sends close_notify first, as RFC 9112 9.8 requires of a server: Conn.Close
// sends it, and so does OnClosing when the engine closes the connection on its own.
type Handler struct {
	handler          fnet.Handler
	config           *tls.Config
	handshakeTimeout time.Duration
}

// NewHandler wraps handler with TLS using config. A connection whose handshake is not done within handshakeTimeout is
// closed; 0 means 10s, <0 means no limit.
func NewHandler(handler fnet.Handler, config *tls.Config, handshakeTimeout time.Duration) *Handler {
	if handshakeTimeout == 0 {
		handshakeTimeout = defaultHandshakeTimeout
	}
	return &Handler{handler: handler, config: config, handshakeTimeout: handshakeTimeout}
}

// OnOpen sets up the handshake, which OnData runs.
func (h *Handler) OnOpen(c fnet.Conn) {
	tc := &Conn{Conn: c, handler: h.handler}
	tc.tlsConn = tls.Server((*rawConn)(tc), h.config)
	tc.handshaking = true
	c.SetContext(tc)
	if h.handshakeTimeout > 0 {
		c.SetDeadline(time.Now().Add(h.handshakeTimeout))
	}
}

// OnData hands data to the handshake until it has succeeded, and from then on decrypts data and passes the plaintext on
// to the handler. What arrives after a close_notify the handler kept the connection open for is dropped: crypto/tls
// reads nothing past it.
func (h *Handler) OnData(c fnet.Conn, data []byte) int {
	tc := c.Context().(*Conn)
	if tc.readClosed {
		return len(data)
	}
	tc.ciphertext = data
	defer func() { tc.ciphertext = nil }()
	if !tc.handshaking || tc.open() {
		tc.drain()
	}
	return len(data)
}

// OnClosing sends close_notify before the engine closes the connection on its own, once the handler has had its
// OnClosing, if it is an fnet.ClosingHandler. As with crypto/tls, no alert goes out before the handshake has succeeded.
func (h *Handler) OnClosing(c fnet.Conn, err error) {
	tc := c.Context().(*Conn)
	if tc.handshaking {
		return
	}
	if ch, ok := tc.handler.(fnet.ClosingHandler); ok {
		ch.OnClosing(tc, err)
	}
	tc.closing.Store(true)
	tc.tlsConn.CloseWrite() // sends close_notify, once: a no-op if the handler has called Close
}

// OnHalfClose lets the handler keep the connection open for what the peer awaits after it closed its write direction, if
// it is an fnet.HalfCloseHandler; the engine closes the connection otherwise, close_notify included (see OnClosing), as it
// does before the handshake has succeeded. A peer whose close_notify the handler already kept the connection open for has
// been asked about once.
func (h *Handler) OnHalfClose(c fnet.Conn) bool {
	tc := c.Context().(*Conn)
	return tc.readClosed || !tc.handshaking && tc.halfClose()
}

// OnWritable passes the engine's OnWritable on to the handler, if it is an fnet.WritableHandler: what waits in the send
// buffer is ciphertext, which Conn.Writable reports on as it is.
func (h *Handler) OnWritable(c fnet.Conn) {
	tc := c.Context().(*Conn)
	if tc.handshaking {
		return
	}
	if wh, ok := tc.handler.(fnet.WritableHandler); ok {
		wh.OnWritable(tc)
	}
}

// OnResume passes on the plaintext held (see Conn.PauseRead), then calls the handler's OnResume, if it is an
// fnet.ResumeHandler.
func (h *Handler) OnResume(c fnet.Conn) {
	tc := c.Context().(*Conn)
	if tc.handshaking {
		return
	}
	if tc.plaintext.Len() > 0 && !tc.readPaused.Load() {
		tc.deliver(nil, 0)
	}
	if rh, ok := tc.handler.(fnet.ResumeHandler); ok {
		rh.OnResume(tc)
	}
}

// OnClose reports the close to the handler, or ends the handshake of a connection the handler never saw.
func (h *Handler) OnClose(c fnet.Conn, err error) {
	tc := c.Context().(*Conn)
	if tc.handshaking {
		if tc.stop != nil {
			tc.stop() // a handshake waiting for data fails
		}
		return
	}
	if tc.closeErr != nil {
		err = tc.closeErr
	}
	tc.plaintext.Release()
	tc.handler.OnClose(tc, err)
}

// Conn is the connection the wrapped handler sees: Write and Writev encrypt, OnData receives the plaintext, and the
// other methods keep the semantics of fnet.Conn.
type Conn struct {
	fnet.Conn
	tlsConn    *tls.Conn
	handler    fnet.Handler
	userCtx    any
	resume     func() (struct{}, bool) // resumes the handshake, see open; nil until the first data arrives, and once it has succeeded
	stop       func()                  // ends the handshake
	yield      func(struct{}) bool     // suspends the handshake until more data arrives, see rawConn.Read
	ciphertext []byte                  // what OnData received, for rawConn.Read
	plaintext  bytepool.Buffer         // what the handler left unconsumed, or what arrived while reading was paused
	closeErr   error                   // the TLS error that closed the connection, see drain
	netConn    net.Conn                // set by Detach: TLS reads and writes it from then on
	readPaused atomic.Bool
	closing    atomic.Bool // Close or OnClosing was called: the engine's close reason stands, and writes fail with net.ErrClosed
	detached   atomic.Bool

	handshaking bool // the handshake has not succeeded yet, so the handler has not seen the connection
	readClosed  bool // the peer's close_notify was read and the handler kept the connection open, so nothing more is read
}

// Context returns the user data bound to the connection.
func (c *Conn) Context() any { return c.userCtx }

// SetContext binds user data to the connection.
func (c *Conn) SetContext(ctx any) { c.userCtx = ctx }

// ConnectionState returns the TLS details of the connection.
func (c *Conn) ConnectionState() tls.ConnectionState { return c.tlsConn.ConnectionState() }

// Write encrypts b and sends it like fnet.Conn.Write.
func (c *Conn) Write(b []byte) (int, error) {
	if c.detached.Load() {
		return 0, net.ErrClosed
	}
	n, err := c.tlsConn.Write(b)
	if err != nil && c.closing.Load() {
		err = net.ErrClosed // in place of crypto/tls's own error once close_notify has gone out, see Handler.OnClosing
	}
	return n, err
}

// Writev joins bs and encrypts them in a single write: no other write comes in between, and small segments do not take a
// record each.
func (c *Conn) Writev(bs [][]byte) (int, error) {
	if len(bs) == 1 {
		return c.Write(bs[0])
	}
	size := 0
	for _, b := range bs {
		size += len(b)
	}
	buf := bytepool.Get(size)
	for _, b := range bs {
		buf.Append(b)
	}
	n, err := c.Write(buf.Bytes())
	buf.Release()
	return n, err
}

// Close sends close_notify and closes the connection like fnet.Conn.Close.
func (c *Conn) Close() error {
	if !c.detached.Load() {
		c.closing.Store(true)
		c.tlsConn.Close() // closes the engine's connection as well, see rawConn.Close
	}
	return nil
}

// PauseRead pauses reading like fnet.Conn.PauseRead. The plaintext held, decrypted meanwhile or left unconsumed by the
// handler, is passed on again as soon as ResumeRead resumes reading (see Handler.OnResume).
//
// The engine's reading is paused first: plaintext is held only once it is, so the engine calls OnResume after ResumeRead.
// The other way round, a pause from another goroutine could leave plaintext held while the engine goes on reading, and
// on other platforms than Linux/macOS, whose read loop calls OnResume only between reads, a peer awaiting an answer to it
// would never get one.
func (c *Conn) PauseRead() {
	c.Conn.PauseRead()
	c.readPaused.Store(true)
}

// ResumeRead resumes reading, see PauseRead.
func (c *Conn) ResumeRead() {
	c.readPaused.Store(false)
	c.Conn.ResumeRead()
}

// Detach takes the connection out of the engine like fnet.Conn.Detach, returning a net.Conn that carries on with the TLS
// session: its Read first returns the plaintext OnData left unconsumed. No write may be in progress meanwhile.
func (c *Conn) Detach() (net.Conn, error) {
	nc, err := c.Conn.Detach()
	if err != nil {
		return nil, err
	}
	c.netConn = nc
	c.detached.Store(true)
	in := c.plaintext.Bytes() // leaves the pool with the detached connection
	c.plaintext = bytepool.Buffer{}
	return &detachedConn{Conn: c.tlsConn, in: in}, nil
}

// handshake is the coroutine the handshake runs in, see rawConn.Read.
func (c *Conn) handshake(yield func(struct{}) bool) {
	c.yield = yield
	c.tlsConn.Handshake()
}

// open resumes the handshake with the data at hand and, once it has succeeded, opens the connection for the handler; it
// reports whether it did.
func (c *Conn) open() bool {
	if c.resume == nil {
		c.resume, c.stop = iter.Pull(c.handshake) // the coroutine, and its goroutine, exist from the first data on
	}
	if _, waiting := c.resume(); waiting {
		return false
	}
	if c.tlsConn.Handshake() != nil { // the handshake is over: this returns its error at once
		c.Conn.Close() // the handler never sees the connection
		return false
	}
	c.handshaking = false
	c.resume, c.stop, c.yield = nil, nil, nil
	c.Conn.SetDeadline(time.Time{}) // the handshake's; the handler sets its own
	c.handler.OnOpen(c)
	return true
}

// drain feeds the ciphertext to TLS and passes the plaintext on to the handler, until the ciphertext is used up; a
// record cut short stays in TLS for the next call. A TLS error, or the peer's close_notify (io.EOF), closes the
// connection and becomes the close reason, unless the handler keeps the connection open for the peer that has closed its
// write direction (see halfClose).
//
// As on the engine's reads, the plaintext goes into a buffer borrowed for the call, after what the handler left
// unconsumed when that is small (a large leftover has the new plaintext appended to it instead), and only what the
// handler leaves unconsumed is kept.
func (c *Conn) drain() {
	for {
		err := c.readPlaintext()
		if err == errWouldBlock {
			return
		}
		if err == io.EOF && c.halfClose() {
			c.readClosed = true
			return
		}
		if err != nil {
			if !c.closing.Load() {
				c.closeErr = err
			}
			c.tlsConn.Close()
			return
		}
	}
}

// readPlaintext delivers the records at hand together. The borrowed buffer stays owned by this call,
// including when the handler panics or exits its goroutine.
func (c *Conn) readPlaintext() (err error) {
	buf := bytepool.Get(recordSize)
	defer buf.Release()
	b := buf.Bytes()[:recordSize]
	pending := 0
	if c.plaintext.Len() <= recordSize/2 {
		pending = c.plaintext.Len()
	}
	n := pending
	for n < len(b) && err == nil {
		var m int
		m, err = c.tlsConn.Read(b[n:])
		n += m
	}
	if n > pending {
		c.deliver(b[:n], pending)
	}
	return err
}

// halfClose asks the handler, if it is an fnet.HalfCloseHandler, whether to keep the connection open now that the peer has
// closed its write direction: with its close_notify, or at the TCP level (see Handler.OnHalfClose).
func (c *Conn) halfClose() bool {
	h, ok := c.handler.(fnet.HalfCloseHandler)
	return ok && h.OnHalfClose(c)
}

// deliver passes the plaintext on to the handler, unless reading is paused; data holds the new plaintext after room for
// the pending bytes the handler left unconsumed (see drain), and is nil when only those are passed on again.
func (c *Conn) deliver(data []byte, pending int) {
	switch {
	case pending > 0:
		copy(data, c.plaintext.Bytes())
		c.plaintext.Release()
	case c.plaintext.Len() > 0:
		c.plaintext.Append(data)
		data = c.plaintext.Bytes()
	}
	consumed := 0
	if !c.readPaused.Load() {
		consumed = min(max(c.handler.OnData(c, data), 0), len(data))
	}
	switch {
	case consumed == len(data):
		c.plaintext.Release()
	case c.plaintext.Len() > 0: // data is plaintext itself
		if consumed > 0 {
			c.plaintext.Discard(consumed)
		}
	default:
		c.plaintext.Append(data[consumed:])
	}
}
