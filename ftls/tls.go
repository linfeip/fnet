// Package ftls adds TLS to fnet: Handler wraps an fnet.Handler, whose connections then write and receive plaintext over
// TLS.
//
// All of it runs in the engine's callbacks, and a connection costs no goroutine. crypto/tls drives the handshake with
// blocking reads, so the handshake runs as a coroutine (iter.Pull) that OnData resumes with the data that arrives, and
// that ends with the handshake; the executor's goroutines passing it on to each other must not be locked to OS threads
// (runtime.LockOSThread). From then on OnData feeds the ciphertext to crypto/tls and passes the plaintext on: once
// the ciphertext is used up the read returns a temporary error, which crypto/tls hands back without failing the
// connection, keeping a record cut short until more data arrives.
package ftls

import (
	"crypto/tls"
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
// handler.
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
	tc.resume, tc.stop = iter.Pull(tc.handshake)
	c.SetContext(tc)
	if h.handshakeTimeout > 0 {
		c.SetDeadline(time.Now().Add(h.handshakeTimeout))
	}
}

// OnData hands data to the handshake until it has succeeded, and from then on decrypts data and passes the plaintext on
// to the handler.
func (h *Handler) OnData(c fnet.Conn, data []byte) int {
	tc := c.Context().(*Conn)
	tc.ciphertext = data
	if tc.resume == nil || tc.open() {
		tc.drain()
	}
	tc.ciphertext = nil
	return len(data)
}

// OnClose reports the close to the handler, or ends the handshake of a connection the handler never saw.
func (h *Handler) OnClose(c fnet.Conn, err error) {
	tc := c.Context().(*Conn)
	if tc.resume != nil {
		tc.stop() // a handshake waiting for data fails
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
	resume     func() (struct{}, bool) // resumes the handshake, see open; nil once it has succeeded
	stop       func()                  // ends the handshake
	yield      func(struct{}) bool     // suspends the handshake until more data arrives, see rawConn.Read
	ciphertext []byte                  // what OnData received, for rawConn.Read
	plaintext  bytepool.Buffer         // what the handler left unconsumed, or what arrived while reading was paused
	closeErr   error                   // the TLS error that closed the connection, see drain
	netConn    net.Conn                // set by Detach: TLS reads and writes it from then on
	readPaused atomic.Bool
	closing    atomic.Bool // Close was called: the close reason stays nil, as on the engine
	detached   atomic.Bool
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
	return c.tlsConn.Write(b)
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

// PauseRead pauses reading like fnet.Conn.PauseRead. Plaintext decrypted meanwhile is kept, and passed on together with
// the data that arrives after ResumeRead.
func (c *Conn) PauseRead() {
	c.readPaused.Store(true)
	c.Conn.PauseRead()
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
	if _, waiting := c.resume(); waiting {
		return false
	}
	if c.tlsConn.Handshake() != nil { // the handshake is over: this returns its error at once
		c.Conn.Close() // the handler never sees the connection
		return false
	}
	c.resume, c.stop, c.yield = nil, nil, nil
	c.Conn.SetDeadline(time.Time{}) // the handshake's; the handler sets its own
	c.handler.OnOpen(c)
	return true
}

// drain feeds the ciphertext to TLS and passes the plaintext on to the handler, until the ciphertext is used up; a
// record cut short stays in TLS for the next call. A TLS error, or the peer's close_notify (io.EOF), closes the
// connection and becomes the close reason.
//
// As on the engine's reads, the plaintext goes into a buffer borrowed for the call, after what the handler left
// unconsumed when that is small (a large leftover has the new plaintext appended to it instead), and only what the
// handler leaves unconsumed is kept.
func (c *Conn) drain() {
	for {
		buf := bytepool.Get(recordSize)
		b := buf.Bytes()[:recordSize]
		pending := 0
		if c.plaintext.Len() <= recordSize/2 {
			pending = c.plaintext.Len()
		}
		n := pending
		var err error
		for n < len(b) && err == nil { // the records at hand go to the handler together
			var m int
			m, err = c.tlsConn.Read(b[n:])
			n += m
		}
		if n > pending {
			c.deliver(b[:n], pending)
		}
		buf.Release()
		if err == errWouldBlock {
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

// deliver passes the plaintext on to the handler, unless reading is paused; data holds the new plaintext after room for
// the pending bytes the handler left unconsumed (see drain).
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

// rawConn is the connection crypto/tls runs on: it reads the ciphertext the engine received and writes to the engine.
// It is the Conn itself under another method set, so it takes no allocation of its own.
type rawConn Conn

// Read hands TLS the ciphertext OnData received. Once that is used up, the handshake waits for OnData to resume it with
// more, and afterwards Read returns errWouldBlock, or reads the detached connection.
func (r *rawConn) Read(b []byte) (int, error) {
	for len(r.ciphertext) == 0 {
		switch {
		case r.yield != nil:
			if !r.yield(struct{}{}) {
				return 0, net.ErrClosed // stopped by OnClose
			}
		case r.netConn != nil:
			return r.netConn.Read(b)
		default:
			return 0, errWouldBlock
		}
	}
	n := copy(b, r.ciphertext)
	r.ciphertext = r.ciphertext[n:]
	return n, nil
}

func (r *rawConn) Write(b []byte) (int, error) {
	if r.netConn != nil {
		return r.netConn.Write(b)
	}
	return r.Conn.Write(b)
}

func (r *rawConn) Close() error {
	if r.netConn != nil {
		return r.netConn.Close()
	}
	return r.Conn.Close()
}

func (r *rawConn) LocalAddr() net.Addr {
	if r.netConn != nil {
		return r.netConn.LocalAddr()
	}
	return r.Conn.LocalAddr()
}

// The engine's writes never block and its reads are fed by OnData, so deadlines only apply to the detached connection.
func (r *rawConn) SetDeadline(t time.Time) error {
	return r.setDeadline(t, net.Conn.SetDeadline)
}

func (r *rawConn) SetReadDeadline(t time.Time) error {
	return r.setDeadline(t, net.Conn.SetReadDeadline)
}

func (r *rawConn) SetWriteDeadline(t time.Time) error {
	return r.setDeadline(t, net.Conn.SetWriteDeadline)
}

func (r *rawConn) setDeadline(t time.Time, set func(net.Conn, time.Time) error) error {
	if r.netConn == nil {
		return nil
	}
	return set(r.netConn, t)
}

// detachedConn is the connection Detach returns: Read first returns the plaintext OnData left unconsumed.
type detachedConn struct {
	*tls.Conn
	in []byte
}

func (c *detachedConn) Read(b []byte) (int, error) {
	if len(c.in) == 0 {
		return c.Conn.Read(b)
	}
	n := copy(b, c.in)
	c.in = c.in[n:]
	return n, nil
}
