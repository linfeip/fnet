//go:build linux || darwin

package fnet

import (
	"io"
	"time"

	"github.com/linfeip/fnet/internal/bytepool"

	"golang.org/x/sys/unix"
)

// requestClose requests that the connection be closed; it may be called from any goroutine, and the close itself is
// done by the connection's task (see closeIfRequested).
// err is the reason reported to OnClose: nil means a local close, which waits until the send buffer has been sent before
// closing; a non-nil err closes immediately, and so it does for a connection that has already requested a local close or
// read EOF, where the first non-nil err becomes the close reason. Writes after the request return net.ErrClosed.
func (c *conn) requestClose(err error) {
	c.mu.Lock()
	c.closing = true
	if err != nil && !c.closeNow {
		c.closeNow, c.closeErr = true, err
	}
	c.mu.Unlock()
	c.notify(evClose)
}

// requestCloseOnEOF requests a close when EOF is read (the peer closed its write direction): data already written still
// has to reach the peer, so just like a local close it waits until the send buffer has been sent before closing, and the
// err reported to OnClose is io.EOF; a connection that has already requested a close keeps its original close reason
// (nil for a local close).
func (c *conn) requestCloseOnEOF() {
	c.mu.Lock()
	if !c.closing {
		c.closing, c.closeErr = true, io.EOF
	}
	c.mu.Unlock()
	c.notify(evClose)
}

// expiredBy reports whether the deadline, or the end of the linger (see beginLinger), has passed by now. The task checks
// it again on evExpired: loop.checkDeadlines found it so, but it may have been put off since, even by the callback of
// this very round.
func (c *conn) expiredBy(now int64) bool {
	d := c.deadline.Load()
	return d != 0 && max(d, -d) <= now
}

// onClosing invokes the OnClosing callback of a ClosingHandler before the engine requests the close of the connection on
// its own, err being the reason, unless a close has been requested already.
func (c *conn) onClosing(err error) {
	if h, ok := c.loop.srv.handler.(ClosingHandler); ok && !c.closeRequested() {
		h.OnClosing(c, err)
	}
}

// halfClose asks a HalfCloseHandler whether to keep the connection open now that EOF is read, and reports whether it does:
// the connection is then not read again. A connection a close has been requested for is not asked.
func (c *conn) halfClose() bool {
	h, ok := c.loop.srv.handler.(HalfCloseHandler)
	if !ok || c.closeRequested() || !h.OnHalfClose(c) {
		return false
	}
	c.readClosed = true
	return true
}

// closeRequested reports whether a close has been requested, see requestClose.
func (c *conn) closeRequested() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.closing
}

// closeIfRequested closes the connection when a close has been requested and either an immediate close was asked for or
// the send buffer has already been sent.
func (c *conn) closeIfRequested() {
	c.mu.Lock()
	closing, now, err, backlog := c.closing, c.closeNow, c.closeErr, c.out.Len() > 0
	c.mu.Unlock()
	if closing && (now || !backlog) {
		c.close(err)
	}
}

// close closes the connection and invokes OnClose with err as the close reason; it may only be called from the
// connection's task. The fd of a local close (err nil) stays open for a while, see beginLinger.
func (c *conn) close(err error) {
	if c.closed {
		return
	}
	c.mu.Lock()
	c.closed = true // before the fd is closed: whoever sees closed=false while holding mu may use the fd
	c.out.Release()
	c.backlogged.Store(false)
	c.mu.Unlock()
	c.in.Release()
	if err == nil {
		c.beginLinger()
	} else {
		defer c.loop.srv.openConnsWg.Done()
		if err != io.EOF { // once EOF is read nothing is left to read away
			c.discardInbound()
		}
		c.closeFd()
	}
	c.loop.srv.handler.OnClose(c, err)
}

// lingerTimeout is how long a connection closed locally keeps reading what the peer still sends, see beginLinger. The
// deadlines are checked once per second, so its fd is closed within two seconds.
const lingerTimeout = time.Second

// beginLinger closes the connection in stages (RFC 9112 9.6): while the peer may still be sending, closing the socket
// makes the kernel answer what arrives with an RST, which can make the peer drop what it has received but not read yet,
// such as the last response. So only the write direction is shut down, a FIN following the data, and the task goes on
// reading away what arrives (see linger), until the peer closes too, a read fails, the server closes or lingerTimeout
// has passed; the fd is closed then, and the server counts the connection as open until that happens.
//
// Meanwhile the deadline holds the end of the linger, negated: that marks the connection as lingering, which SetDeadline
// then leaves alone, and loop.checkDeadlines still finds it.
func (c *conn) beginLinger() {
	unix.Shutdown(c.fd, unix.SHUT_WR)
	c.deadline.Store(-time.Now().Add(lingerTimeout).UnixNano())
	// What has arrived already raises no new event: it is read here once, and linger goes on in another round only when
	// there was something, the EOF included. Usually there is nothing yet, and the peer's FIN brings the next event.
	if _, err := c.discardInbound(); err != unix.EAGAIN {
		c.notify(evRead)
	}
}

// lingering reports whether the connection is closed but its fd is still open, see beginLinger.
func (c *conn) lingering() bool { return c.deadline.Load() < 0 }

// linger handles the events of a lingering connection: it reads away what arrives, a read per round as in read, and
// closes the fd once the peer has closed, a read has failed, the linger has timed out or the server is closing.
func (c *conn) linger(ev uint32) {
	if ev&evShutdown != 0 || ev&evExpired != 0 && c.expiredBy(time.Now().UnixNano()) {
		c.endLinger()
		return
	}
	if ev&evHup != 0 {
		c.peerClosed = true
	}
	if ev&evRead == 0 {
		return
	}
	n, err := c.discardInbound()
	if n > 0 {
		if n == c.loop.srv.opts.ReadBufferSize || c.peerClosed {
			c.notify(evRead) // there may be more, or the EOF after the data, see read
		}
		return
	}
	switch err {
	case unix.EAGAIN:
		c.peerClosed = false // a stale evHup, as in read
	case unix.EINTR:
		c.notify(evRead)
	default: // EOF, or the read failed
		c.endLinger()
	}
}

// endLinger closes the fd of a lingering connection.
func (c *conn) endLinger() {
	c.deadline.Store(0) // no longer lingering: no task runs after this one (see run)
	c.closeFd()
	c.loop.srv.openConnsWg.Done()
}

// closeFd takes the connection out of the table and closes its fd.
func (c *conn) closeFd() {
	connsByFd.remove(c) // before the fd is closed: a closed fd may immediately be reused by a new connection
	unix.Close(c.fd)
}

// abandon closes a connection that could not be watched, before OnOpen; it may only be called from the connection's
// first task.
func (c *conn) abandon() {
	c.mu.Lock()
	c.closed = true
	unix.Close(c.fd)
	c.mu.Unlock()
	c.loop.srv.openConnsWg.Done()
}

// discardInbound reads away data that has arrived but has not been read yet, and returns the result of the read: closing
// while the socket holds unread data sends an RST, which may make the peer discard the data it has already sent (such as
// a close frame). It reads only once, since it must not keep reading while the peer keeps sending.
func (c *conn) discardInbound() (int, error) {
	size := c.loop.srv.opts.ReadBufferSize
	buf := bytepool.Get(size)
	defer buf.Release()
	return sysRead(c.fd, buf.Bytes()[:size])
}
