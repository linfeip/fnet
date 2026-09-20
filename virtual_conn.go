package fnet

import (
	"bytes"
	"errors"
	"io"
	"net"
	"sync"
	"syscall"
	"time"

	"github.com/gobwas/ws"
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

	inBuf     []byte
	inReadOff int
	outBuf    []byte

	closed   bool
	readEOF  bool
	writeEOF bool
	readErr  error
	writeErr error

	readDeadline  time.Time
	writeDeadline time.Time
	deadlineTimer *time.Timer

	onWritable func() // optional: notify reactor that outbound data is pending

	directWrite  func([]byte) (int, error)   // optional: fast-path direct socket write
	directWritev func([][]byte) (int, error) // optional: fast-path direct vector socket write
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

// SetDirectWrite registers a callback invoked to attempt a direct non-blocking
// write to the underlying socket before buffering.
func (vc *VirtualConn) SetDirectWrite(fn func([]byte) (int, error)) {
	vc.mu.Lock()
	vc.directWrite = fn
	vc.mu.Unlock()
}

// SetDirectWritev registers a callback invoked to attempt a direct non-blocking
// vector write (writev) to the underlying socket before buffering.
func (vc *VirtualConn) SetDirectWritev(fn func([][]byte) (int, error)) {
	vc.mu.Lock()
	vc.directWritev = fn
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
	if vc.inReadOff > 0 {
		if vc.inReadOff == len(vc.inBuf) {
			vc.inBuf = vc.inBuf[:0]
			vc.inReadOff = 0
		} else if vc.inReadOff > 4096 {
			copy(vc.inBuf, vc.inBuf[vc.inReadOff:])
			vc.inBuf = vc.inBuf[:len(vc.inBuf)-vc.inReadOff]
			vc.inReadOff = 0
		}
	}
	vc.inBuf = append(vc.inBuf, b...)
	vc.inCond.Signal()
	vc.mu.Unlock()
}

// HasCompleteHeader reports whether the buffered input contains a complete HTTP header
// marked by \r\n\r\n or \n\n.
func (vc *VirtualConn) HasCompleteHeader() bool {
	vc.mu.Lock()
	defer vc.mu.Unlock()
	buf := vc.inBuf[vc.inReadOff:]
	return bytes.Contains(buf, []byte("\r\n\r\n")) || bytes.Contains(buf, []byte("\n\n"))
}

// HasCompleteWSFrame reports whether vc.inBuf contains at least one complete WebSocket frame.
func (vc *VirtualConn) HasCompleteWSFrame() bool {
	vc.mu.Lock()
	defer vc.mu.Unlock()

	buf := vc.inBuf[vc.inReadOff:]
	if len(buf) < 2 {
		return false
	}
	r := bytes.NewReader(buf)
	h, err := ws.ReadHeader(r)
	if err != nil {
		return false
	}
	headerSize := len(buf) - r.Len()
	return len(buf) >= headerSize+int(h.Length)
}

// PopWSFrame extracts the next complete WebSocket frame from the input buffer.
// If complete, it returns header, payload (unmasked if masked), found=true, nil.
// If the buffer does not have a complete frame yet, it returns found=false, nil without advancing inBuf.
// If the frame header is corrupted or violates protocol, it returns found=false, err.
func (vc *VirtualConn) PopWSFrame() (ws.Header, []byte, bool, error) {
	vc.mu.Lock()
	defer vc.mu.Unlock()

	buf := vc.inBuf[vc.inReadOff:]
	if len(buf) < 2 {
		return ws.Header{}, nil, false, nil
	}

	r := bytes.NewReader(buf)
	h, err := ws.ReadHeader(r)
	if err != nil {
		if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
			return ws.Header{}, nil, false, nil
		}
		return ws.Header{}, nil, false, err
	}

	headerSize := len(buf) - r.Len()
	totalSize := headerSize + int(h.Length)
	if len(buf) < totalSize {
		return ws.Header{}, nil, false, nil
	}

	var payload []byte
	if h.Length > 0 {
		payload = make([]byte, h.Length)
		copy(payload, buf[headerSize:totalSize])
		if h.Masked {
			ws.Cipher(payload, h.Mask, 0)
		}
	}

	vc.inReadOff += totalSize
	if vc.inReadOff == len(vc.inBuf) {
		vc.inBuf = vc.inBuf[:0]
		vc.inReadOff = 0
	}

	return h, payload, true, nil
}

// HasBufferedInput reports whether there is any pending input data in the read buffer.
func (vc *VirtualConn) HasBufferedInput() bool {
	vc.mu.Lock()
	defer vc.mu.Unlock()
	return len(vc.inBuf) > vc.inReadOff
}

// InputLen returns the length of unconsumed input buffer.
func (vc *VirtualConn) InputLen() int {
	vc.mu.Lock()
	defer vc.mu.Unlock()
	return len(vc.inBuf) - vc.inReadOff
}

// UnshiftInput prepends unconsumed bytes back to the front of the inbound buffer.
func (vc *VirtualConn) UnshiftInput(b []byte) {
	if len(b) == 0 {
		return
	}
	vc.mu.Lock()
	rem := vc.inBuf[vc.inReadOff:]
	newBuf := make([]byte, 0, len(b)+len(rem))
	newBuf = append(newBuf, b...)
	newBuf = append(newBuf, rem...)
	vc.inBuf = newBuf
	vc.inReadOff = 0
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
		if cap(vc.outBuf) > 64*1024 {
			vc.outBuf = nil
		} else {
			vc.outBuf = vc.outBuf[:0]
		}
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
		if vc.readErr != nil && len(vc.inBuf) == vc.inReadOff {
			return 0, vc.readErr
		}
		if len(vc.inBuf) > vc.inReadOff {
			n := copy(b, vc.inBuf[vc.inReadOff:])
			vc.inReadOff += n
			if vc.inReadOff == len(vc.inBuf) {
				if cap(vc.inBuf) > 64*1024 {
					vc.inBuf = nil
				} else {
					vc.inBuf = vc.inBuf[:0]
				}
				vc.inReadOff = 0
			} else if vc.inReadOff > 4096 && vc.inReadOff > len(vc.inBuf)/2 {
				copy(vc.inBuf, vc.inBuf[vc.inReadOff:])
				vc.inBuf = vc.inBuf[:len(vc.inBuf)-vc.inReadOff]
				vc.inReadOff = 0
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
			if !vc.readDeadline.IsZero() && time.Now().After(vc.readDeadline) && len(vc.inBuf) == vc.inReadOff {
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

	origLen := len(b)
	if len(vc.outBuf) == 0 && vc.directWrite != nil {
		n, err := vc.directWrite(b)
		if n == len(b) {
			vc.mu.Unlock()
			return n, nil
		}
		if n > 0 {
			b = b[n:]
		}
		if err != nil && !errors.Is(err, syscall.EAGAIN) && !errors.Is(err, syscall.EWOULDBLOCK) {
			vc.writeErr = err
			vc.mu.Unlock()
			return n, err
		}
	}

	vc.outBuf = append(vc.outBuf, b...)
	cb := vc.onWritable
	vc.mu.Unlock()
	if cb != nil {
		cb()
	}
	return origLen, nil
}

// WriteVector writes multiple byte slices in a single logical write operation,
// using direct vector write (writev) when possible to avoid multiple syscalls
// and memory copies.
func (vc *VirtualConn) WriteVector(iovs [][]byte) (int, error) {
	totalLen := 0
	for _, b := range iovs {
		totalLen += len(b)
	}
	if totalLen == 0 {
		return 0, nil
	}

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

	written := 0
	if len(vc.outBuf) == 0 && vc.directWritev != nil {
		n, err := vc.directWritev(iovs)
		if n > 0 {
			written = n
		}
		if written == totalLen {
			vc.mu.Unlock()
			return totalLen, nil
		}
		if err != nil && !errors.Is(err, syscall.EAGAIN) && !errors.Is(err, syscall.EWOULDBLOCK) {
			vc.writeErr = err
			vc.mu.Unlock()
			return written, err
		}
	}

	remToSkip := written
	for _, b := range iovs {
		if remToSkip >= len(b) {
			remToSkip -= len(b)
			continue
		}
		chunk := b[remToSkip:]
		remToSkip = 0
		vc.outBuf = append(vc.outBuf, chunk...)
	}

	cb := vc.onWritable
	vc.mu.Unlock()
	if cb != nil {
		cb()
	}
	return totalLen, nil
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
