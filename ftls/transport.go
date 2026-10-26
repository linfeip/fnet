package ftls

import (
	"crypto/tls"
	"net"
	"time"
)

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
