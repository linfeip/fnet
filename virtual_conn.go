package fnet

import (
	"io"
	"net"
	"sync"
	"syscall"
	"time"
)

// VirtualConn is a concurrency-safe net.Conn adapter. The reactor feeds
// inbound socket bytes via FeedInput; a worker goroutine may block in Read
// (e.g. inside tls.Conn / http.ReadRequest) without blocking the event loop.
// Writes are queued and drained by the reactor via DrainWrite.
type VirtualConn struct {
	local  net.Addr
	remote net.Addr

	mu     sync.Mutex
	inCond *sync.Cond

	inBuf  []byte
	outBuf []byte

	closed   bool
	readEOF  bool
	writeEOF bool
	readErr  error
	writeErr error

	readDeadline  time.Time
	writeDeadline time.Time
	deadlineTimer *time.Timer

	onWritable func() // optional: notify reactor that outbound data is pending
}

// NewVirtualConn creates a VirtualConn with the given addresses.
func NewVirtualConn(local, remote net.Addr) *VirtualConn {
	vc := &VirtualConn{
		local:  local,
		remote: remote,
	}
	vc.inCond = sync.NewCond(&vc.mu)
	return vc
}

// SetWritableCallback registers a callback invoked (unlocked) when Write
// queues data. Used by the server to arm EPOLLOUT / EVFILT_WRITE.
func (vc *VirtualConn) SetWritableCallback(fn func()) {
	vc.mu.Lock()
	vc.onWritable = fn
	vc.mu.Unlock()
}

// FeedInput appends network data for subsequent Read calls.
func (vc *VirtualConn) FeedInput(b []byte) {
	if len(b) == 0 {
		return
	}
	vc.mu.Lock()
	if vc.closed || vc.readEOF {
		vc.mu.Unlock()
		return
	}
	vc.inBuf = append(vc.inBuf, b...)
	vc.inCond.Signal()
	vc.mu.Unlock()
}

// FeedEOF signals that the peer closed the read side.
func (vc *VirtualConn) FeedEOF() {
	vc.mu.Lock()
	vc.readEOF = true
	vc.inCond.Broadcast()
	vc.mu.Unlock()
}

// FeedError injects a permanent read/write error and wakes waiters.
func (vc *VirtualConn) FeedError(err error) {
	vc.mu.Lock()
	if vc.readErr == nil {
		vc.readErr = err
	}
	if vc.writeErr == nil {
		vc.writeErr = err
	}
	vc.inCond.Broadcast()
	vc.mu.Unlock()
}

// DrainWrite copies queued outbound bytes into dst and removes them from
// the write buffer. Returns the number of bytes copied and whether more
// data remains.
func (vc *VirtualConn) DrainWrite(dst []byte) (n int, remaining bool) {
	vc.mu.Lock()
	defer vc.mu.Unlock()
	if len(vc.outBuf) == 0 {
		return 0, false
	}
	n = copy(dst, vc.outBuf)
	vc.outBuf = vc.outBuf[n:]
	if len(vc.outBuf) == 0 {
		vc.outBuf = vc.outBuf[:0]
	}
	return n, len(vc.outBuf) > 0
}

// PendingWrite reports whether outbound data is queued.
func (vc *VirtualConn) PendingWrite() bool {
	vc.mu.Lock()
	defer vc.mu.Unlock()
	return len(vc.outBuf) > 0
}

// UnshiftWrite prepends bytes to the front of the outbound buffer
// (used when a partial socket write needs retry).
func (vc *VirtualConn) UnshiftWrite(b []byte) {
	if len(b) == 0 {
		return
	}
	vc.mu.Lock()
	vc.outBuf = append(b, vc.outBuf...)
	vc.mu.Unlock()
}

// Read implements net.Conn. Blocks until data is available, EOF, or error.
func (vc *VirtualConn) Read(b []byte) (int, error) {
	vc.mu.Lock()
	defer vc.mu.Unlock()

	for {
		if vc.readErr != nil && len(vc.inBuf) == 0 {
			return 0, vc.readErr
		}
		if len(vc.inBuf) > 0 {
			n := copy(b, vc.inBuf)
			vc.inBuf = vc.inBuf[n:]
			if len(vc.inBuf) == 0 {
				vc.inBuf = vc.inBuf[:0]
			}
			return n, nil
		}
		if vc.readEOF || vc.closed {
			return 0, io.EOF
		}
		if !vc.readDeadline.IsZero() {
			remain := time.Until(vc.readDeadline)
			if remain <= 0 {
				return 0, syscall.ETIMEDOUT
			}
			timer := time.AfterFunc(remain, func() {
				vc.mu.Lock()
				vc.inCond.Broadcast()
				vc.mu.Unlock()
			})
			vc.inCond.Wait()
			timer.Stop()
			if !vc.readDeadline.IsZero() && time.Now().After(vc.readDeadline) && len(vc.inBuf) == 0 {
				return 0, syscall.ETIMEDOUT
			}
			continue
		}
		vc.inCond.Wait()
	}
}

// Write implements net.Conn. Queues data for the reactor to send.
func (vc *VirtualConn) Write(b []byte) (int, error) {
	vc.mu.Lock()
	if vc.closed || vc.writeEOF {
		vc.mu.Unlock()
		return 0, net.ErrClosed
	}
	if vc.writeErr != nil {
		err := vc.writeErr
		vc.mu.Unlock()
		return 0, err
	}
	if !vc.writeDeadline.IsZero() && time.Now().After(vc.writeDeadline) {
		vc.mu.Unlock()
		return 0, syscall.ETIMEDOUT
	}
	vc.outBuf = append(vc.outBuf, b...)
	cb := vc.onWritable
	vc.mu.Unlock()
	if cb != nil {
		cb()
	}
	return len(b), nil
}

// Close implements net.Conn.
func (vc *VirtualConn) Close() error {
	vc.mu.Lock()
	if vc.closed {
		vc.mu.Unlock()
		return nil
	}
	vc.closed = true
	vc.readEOF = true
	vc.writeEOF = true
	vc.inCond.Broadcast()
	cb := vc.onWritable
	vc.mu.Unlock()
	if cb != nil {
		cb()
	}
	return nil
}

// LocalAddr implements net.Conn.
func (vc *VirtualConn) LocalAddr() net.Addr {
	if vc.local != nil {
		return vc.local
	}
	return &net.TCPAddr{}
}

// RemoteAddr implements net.Conn.
func (vc *VirtualConn) RemoteAddr() net.Addr {
	if vc.remote != nil {
		return vc.remote
	}
	return &net.TCPAddr{}
}

// SetDeadline implements net.Conn.
func (vc *VirtualConn) SetDeadline(t time.Time) error {
	if err := vc.SetReadDeadline(t); err != nil {
		return err
	}
	return vc.SetWriteDeadline(t)
}

// SetReadDeadline implements net.Conn.
func (vc *VirtualConn) SetReadDeadline(t time.Time) error {
	vc.mu.Lock()
	vc.readDeadline = t
	vc.inCond.Broadcast()
	vc.mu.Unlock()
	return nil
}

// SetWriteDeadline implements net.Conn.
func (vc *VirtualConn) SetWriteDeadline(t time.Time) error {
	vc.mu.Lock()
	vc.writeDeadline = t
	vc.mu.Unlock()
	return nil
}

// Closed reports whether Close has been called.
func (vc *VirtualConn) Closed() bool {
	vc.mu.Lock()
	defer vc.mu.Unlock()
	return vc.closed
}
