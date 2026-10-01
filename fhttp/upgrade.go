package fhttp

import (
	"errors"
	"net"
	"net/http"

	"github.com/linfeip/fnet"
	"github.com/linfeip/fnet/internal/bytepool"
	"github.com/linfeip/fnet/internal/units"
)

// errEarlyRequest means the client sent a follow-up request without waiting for the upgrade response.
var errEarlyRequest = errors.New("fhttp: request received before upgrade response")

// Protocol is the protocol that takes over the connection after a protocol upgrade; see Upgrade. Its callbacks run
// on a goroutine of fnet's executor, so they must return quickly and must not block.
type Protocol interface {
	// OnData has the same semantics as fnet.Handler.OnData: data is valid only within this callback, and the
	// number of consumed bytes is returned.
	OnData(data []byte) (consumed int)
	// OnClose is called after the connection is closed; err is the same as in fnet.Handler.OnClose, and OnData is
	// not called afterwards.
	OnClose(err error)
}

// Upgrade switches the protocol of the connection that w belongs to (for example to WebSocket) from inside a
// Handler: the protocol created by newProtocol(underlying connection) takes over the inbound data, then w.Header()
// is written out with 101 Switching Protocols. The protocol is switched before the response, so all data the peer
// sends after receiving the response goes to the new protocol.
//
// After the switch, the connection's deadline (fnet.Conn.SetDeadline) is managed by the new protocol and fhttp no
// longer sets it; newProtocol is called before the switch, so the initial deadline can be set there. The exception:
// when the client sent data without waiting for the upgrade response, fhttp sets the deadline according to
// ReadHeaderTimeout, and the new protocol should override it in OnData.
//
// The Handler must not use w after this call. http.ErrNotSupported is returned when w is not an fhttp
// ResponseWriter (and cannot be obtained through Unwrap); an error is returned when the connection is already
// closed, or when the client sent a follow-up request without waiting for the upgrade response, and the connection
// is then closed (in the latter case a 400 is replied first).
func Upgrade(w http.ResponseWriter, newProtocol func(fnet.Conn) Protocol) error {
	for {
		if rw, ok := w.(*response); ok {
			return rw.conn.upgrade(rw, newProtocol)
		}
		u, ok := w.(interface{ Unwrap() http.ResponseWriter })
		if !ok {
			return http.ErrNotSupported
		}
		w = u.Unwrap()
	}
}

func (c *conn) upgrade(w *response, newProtocol func(fnet.Conn) Protocol) error {
	w.upgraded = true
	p := newProtocol(c.connection)
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return net.ErrClosed
	}
	// There are already requests queued after the upgrade request: the client did not wait for the upgrade
	// response, so these requests must no longer be executed as HTTP.
	if len(c.queue) > 0 {
		c.mu.Unlock()
		c.writeError(http.StatusBadRequest)
		return errEarlyRequest
	}
	c.protocol.Store(&p)
	// No HTTP requests are handled from now on: release the request queue and the cached peer address (remoteAddr
	// is accessed only by the worker, and we are the worker right now).
	c.queue, c.remoteAddr = nil, ""
	if c.partial {
		// Data the client sent without waiting for the upgrade response is handed to the new protocol only when
		// data next arrives; until then the request-header timeout applies.
		c.connection.SetDeadline(deadlineAfter(c.srv.opts.ReadHeaderTimeout))
	}
	c.mu.Unlock()

	b := &w.buf
	b.buffer = bytepool.Get(units.KB / 2)
	b.WriteString("HTTP/1.1 101 Switching Protocols\r\n")
	w.header.Write(b)
	b.WriteString("\r\n")
	c.connection.Write(b.buffer.Bytes())
	b.buffer.Release()
	return nil
}
