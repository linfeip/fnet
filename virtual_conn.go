package fnet

import (
	"bytes"
	"errors"
	"io"
	"net"
	"os"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/gobwas/ws"
)

// ErrWriteBufferFull is returned when outbound queue exceeds maxOutboundBufferSize.
var ErrWriteBufferFull = errors.New("fnet: outbound write buffer full")

const maxOutboundBufferSize = 16 * 1024 * 1024 // 16MB per connection safety cap against slowloris

// VirtualConn is a concurrency-safe net.Conn adapter. The reactor feeds
// inbound socket bytes via FeedInput; a worker goroutine may block in Read
// (e.g. inside tls.Conn / http.ReadRequest) without blocking the event loop.
// Writes go straight to the socket when possible and are otherwise queued and
// drained by the reactor on write readiness.
//
// Locking: mu guards the inbound side (inBuf and read state), wmu guards the
// outbound side (outBuf, write state) and serialises direct socket writes.
// The reactor's FeedInput therefore never waits behind a writer that is inside
// a write syscall.
type vcCallbacks struct {
	onWritable   func()
	onClose      func()
	directWrite  func([]byte) (int, error)
	directWritev func([][]byte) (int, error)
	pauseRead    func()
	resumeRead   func()
}

type VirtualConn struct {
	local  net.Addr
	remote net.Addr

	raddrIP   [16]byte
	raddrPort uint16
	raddrLen  uint8 // 4 for IPv4, 16 for IPv6, 0 for unset

	// raddrStr caches the textual peer address. It never changes for the life
	// of the connection, so re-formatting it per request is wasted work.
	raddrStr atomic.Value // string

	fd int
	c  *conn

	mu               sync.Mutex
	inCond           *sync.Cond
	inBuf            []byte
	inReadOff        int
	inLen            atomic.Int32
	readEOF          bool
	readErr          error
	readDeadlineNano int64

	wmu               sync.Mutex
	outBuf            []byte
	outReadOff        int
	outBlock          *outBlock
	writeErr          error
	writeDeadlineNano int64
	cb                *vcCallbacks

	closed atomic.Bool
}

const vcReleaseThreshold = 64 * 1024

// NewVirtualConn creates a VirtualConn with the given addresses.
func NewVirtualConn(local, remote net.Addr) *VirtualConn {
	return &VirtualConn{
		local:  local,
		remote: remote,
	}
}

func (vc *VirtualConn) attachConn(fd int, c *conn) {
	vc.fd = fd
	vc.c = c
}

func (vc *VirtualConn) condLocked() *sync.Cond {
	if vc.inCond == nil {
		vc.inCond = sync.NewCond(&vc.mu)
	}
	return vc.inCond
}

func (vc *VirtualConn) callbacksLocked() *vcCallbacks {
	if vc.cb == nil {
		vc.cb = new(vcCallbacks)
	}
	return vc.cb
}

func (vc *VirtualConn) doDirectWrite(b []byte) (int, error) {
	if vc.fd > 0 {
		return writeFD(vc.fd, b)
	}
	if vc.cb != nil && vc.cb.directWrite != nil {
		return vc.cb.directWrite(b)
	}
	return 0, nil
}

func (vc *VirtualConn) canDirectWrite() bool {
	return vc.fd > 0 || (vc.cb != nil && vc.cb.directWrite != nil)
}

func (vc *VirtualConn) doDirectWritev(iovs [][]byte) (int, error) {
	if vc.fd > 0 {
		return writevFD(vc.fd, iovs)
	}
	if vc.cb != nil && vc.cb.directWritev != nil {
		return vc.cb.directWritev(iovs)
	}
	return 0, nil
}

func (vc *VirtualConn) canDirectWritev() bool {
	return vc.fd > 0 || (vc.cb != nil && vc.cb.directWritev != nil)
}

func (vc *VirtualConn) notifyWritable() {
	if vc.c != nil && vc.c.reactor != nil {
		vc.c.reactor.armWrite(vc.c)
		return
	}
	if vc.cb != nil && vc.cb.onWritable != nil {
		vc.cb.onWritable()
	}
}

func (vc *VirtualConn) notifyClose() {
	if vc.c != nil && vc.c.server != nil {
		vc.c.server.closeConnGraceful(vc.c, nil)
		return
	}
	if vc.cb != nil && vc.cb.onClose != nil {
		vc.cb.onClose()
	}
}

// SetWritableCallback registers a callback invoked (unlocked) when Write
// queues data that could not be written directly. Used by the server to arm
// EPOLLOUT / EVFILT_WRITE.
func (vc *VirtualConn) SetWritableCallback(fn func()) {
	vc.wmu.Lock()
	vc.callbacksLocked().onWritable = fn
	vc.wmu.Unlock()
}

// SetCloseCallback registers a callback invoked (unlocked) once when Close is
// called. The server uses it to release the socket owned by the reactor.
func (vc *VirtualConn) SetCloseCallback(fn func()) {
	vc.wmu.Lock()
	vc.callbacksLocked().onClose = fn
	vc.wmu.Unlock()
}

// SetDirectWrite registers a callback invoked to attempt a direct non-blocking
// write to the underlying socket before buffering.
func (vc *VirtualConn) SetDirectWrite(fn func([]byte) (int, error)) {
	vc.wmu.Lock()
	vc.callbacksLocked().directWrite = fn
	vc.wmu.Unlock()
}

// SetDirectWritev registers a callback invoked to attempt a direct non-blocking
// vector write (writev) to the underlying socket before buffering.
func (vc *VirtualConn) SetDirectWritev(fn func([][]byte) (int, error)) {
	vc.wmu.Lock()
	vc.callbacksLocked().directWritev = fn
	vc.wmu.Unlock()
}

// PauseRead pauses event-driven reading on the connection for backpressure.
func (vc *VirtualConn) PauseRead() {
	if vc.c != nil {
		vc.c.readPaused.Store(true)
	}
	if vc.cb != nil && vc.cb.pauseRead != nil {
		vc.cb.pauseRead()
	}
}

// ResumeRead resumes event-driven reading on the connection and wakes the reactor.
func (vc *VirtualConn) ResumeRead() {
	if vc.c != nil {
		if vc.c.readPaused.CompareAndSwap(true, false) {
			if vc.c.reactor != nil {
				vc.c.reactor.enqueue(func() {
					vc.c.reactor.handleRead(vc.c)
				})
			}
		}
	}
	if vc.cb != nil && vc.cb.resumeRead != nil {
		vc.cb.resumeRead()
	}
}

// SetPauseReadCallback registers a callback invoked when PauseRead is called.
func (vc *VirtualConn) SetPauseReadCallback(fn func()) {
	vc.mu.Lock()
	vc.callbacksLocked().pauseRead = fn
	vc.mu.Unlock()
}

// SetResumeReadCallback registers a callback invoked when ResumeRead is called.
func (vc *VirtualConn) SetResumeReadCallback(fn func()) {
	vc.mu.Lock()
	vc.callbacksLocked().resumeRead = fn
	vc.mu.Unlock()
}

// ---------------------------------------------------------------------------
// Inbound side
// ---------------------------------------------------------------------------

// appendInputLocked appends b to inBuf, compacting consumed bytes first when
// that is cheap (amortised O(1)).
func (vc *VirtualConn) appendInputLocked(b []byte) {
	if vc.inReadOff > 0 {
		unread := len(vc.inBuf) - vc.inReadOff
		if unread == 0 {
			vc.inBuf = vc.inBuf[:0]
			vc.inReadOff = 0
		} else if vc.inReadOff > 4096 || vc.inReadOff >= unread {
			copy(vc.inBuf, vc.inBuf[vc.inReadOff:])
			vc.inBuf = vc.inBuf[:unread]
			vc.inReadOff = 0
		}
	}
	vc.inBuf = append(vc.inBuf, b...)
	vc.inLen.Store(int32(len(vc.inBuf) - vc.inReadOff))
}

// consumeLocked advances the read offset by n and releases/compacts the buffer.
func (vc *VirtualConn) consumeLocked(n int) {
	vc.inReadOff += n
	unread := len(vc.inBuf) - vc.inReadOff
	if unread <= 0 {
		vc.inBuf = nil
		vc.inReadOff = 0
		vc.inLen.Store(0)
		return
	}
	if vc.inReadOff > 4096 && vc.inReadOff >= unread {
		copy(vc.inBuf, vc.inBuf[vc.inReadOff:])
		vc.inBuf = vc.inBuf[:unread]
		vc.inReadOff = 0
	}
	vc.inLen.Store(int32(unread))
}

// CompactOrRelease releases the input buffer if fully consumed or compacts it.
func (vc *VirtualConn) CompactOrRelease() {
	vc.mu.Lock()
	unread := len(vc.inBuf) - vc.inReadOff
	if unread <= 0 {
		vc.inBuf = nil
		vc.inReadOff = 0
		vc.inLen.Store(0)
	} else if vc.inReadOff > 0 {
		copy(vc.inBuf, vc.inBuf[vc.inReadOff:])
		vc.inBuf = vc.inBuf[:unread]
		vc.inReadOff = 0
		vc.inLen.Store(int32(unread))
	}
	vc.mu.Unlock()
}

// FeedInput appends network data for subsequent Read calls.
func (vc *VirtualConn) FeedInput(b []byte) {
	if len(b) == 0 {
		return
	}
	vc.mu.Lock()
	if vc.closed.Load() || vc.readEOF {
		vc.mu.Unlock()
		return
	}
	vc.appendInputLocked(b)
	if vc.inCond != nil {
		vc.inCond.Signal()
	}
	vc.mu.Unlock()
}

// inputView returns the unconsumed input bytes. The reactor uses it in
// event-driven WebSocket mode where it is the sole consumer; the slice is only
// valid until the next mutation of the input buffer.
func (vc *VirtualConn) inputView() []byte {
	vc.mu.Lock()
	b := vc.inBuf[vc.inReadOff:]
	vc.mu.Unlock()
	return b
}

// consumeInput marks n bytes returned by inputView as processed.
func (vc *VirtualConn) consumeInput(n int) {
	if n <= 0 {
		return
	}
	vc.mu.Lock()
	vc.consumeLocked(n)
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

// HasCompleteWSFrame reports whether the input buffer contains at least one complete WebSocket frame.
func (vc *VirtualConn) HasCompleteWSFrame() bool {
	vc.mu.Lock()
	defer vc.mu.Unlock()
	buf := vc.inBuf[vc.inReadOff:]
	h, size, ok, err := parseWSHeader(buf)
	if err != nil || !ok {
		return false
	}
	return len(buf) >= size+int(h.Length)
}

// PopWSFrame extracts the next complete WebSocket frame from the input buffer.
// If complete, it returns header, payload (unmasked if masked), found=true, nil.
// If the buffer does not have a complete frame yet, it returns found=false, nil without advancing.
// If the frame header is corrupted or violates protocol, it returns found=false, err.
func (vc *VirtualConn) PopWSFrame() (ws.Header, []byte, bool, error) {
	vc.mu.Lock()
	defer vc.mu.Unlock()

	buf := vc.inBuf[vc.inReadOff:]
	h, size, ok, err := parseWSHeader(buf)
	if err != nil {
		return ws.Header{}, nil, false, err
	}
	if !ok {
		return ws.Header{}, nil, false, nil
	}
	total := size + int(h.Length)
	if len(buf) < total {
		return ws.Header{}, nil, false, nil
	}

	var payload []byte
	if h.Length > 0 {
		payload = make([]byte, h.Length)
		copy(payload, buf[size:total])
		if h.Masked {
			ws.Cipher(payload, h.Mask, 0)
		}
	}
	vc.consumeLocked(total)
	return h, payload, true, nil
}

// HasBufferedInput reports whether there is any pending input data in the read buffer.
func (vc *VirtualConn) HasBufferedInput() bool {
	return vc.inLen.Load() > 0
}

// InputLen returns the length of unconsumed input buffer.
func (vc *VirtualConn) InputLen() int {
	return int(vc.inLen.Load())
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
	vc.inLen.Store(int32(len(newBuf)))
	vc.mu.Unlock()
}

// FeedEOF signals that the peer closed the read side.
func (vc *VirtualConn) FeedEOF() {
	vc.mu.Lock()
	vc.readEOF = true
	if vc.inCond != nil {
		vc.inCond.Broadcast()
	}
	vc.mu.Unlock()
}

// FeedError injects a permanent read/write error and wakes waiters.
func (vc *VirtualConn) FeedError(err error) {
	vc.mu.Lock()
	if vc.readErr == nil {
		vc.readErr = err
	}
	if vc.inCond != nil {
		vc.inCond.Broadcast()
	}
	vc.mu.Unlock()

	vc.wmu.Lock()
	if vc.writeErr == nil {
		vc.writeErr = err
	}
	vc.wmu.Unlock()
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
			vc.consumeLocked(n)
			return n, nil
		}
		if vc.readEOF || vc.closed.Load() {
			return 0, io.EOF
		}
		if vc.readDeadlineNano > 0 {
			now := time.Now().UnixNano()
			remain := time.Duration(vc.readDeadlineNano - now)
			if remain <= 0 {
				return 0, os.ErrDeadlineExceeded
			}
			timer := time.AfterFunc(remain, func() {
				vc.mu.Lock()
				if vc.inCond != nil {
					vc.inCond.Broadcast()
				}
				vc.mu.Unlock()
			})
			vc.condLocked().Wait()
			timer.Stop()
			if vc.readDeadlineNano > 0 && time.Now().UnixNano() > vc.readDeadlineNano && len(vc.inBuf) == vc.inReadOff {
				return 0, os.ErrDeadlineExceeded
			}
			continue
		}
		vc.condLocked().Wait()
	}
}

// ---------------------------------------------------------------------------
// Outbound side
// ---------------------------------------------------------------------------

// checkWritableLocked validates that the connection can accept writes.
func (vc *VirtualConn) checkWritableLocked() error {
	if vc.closed.Load() {
		return net.ErrClosed
	}
	if vc.writeErr != nil {
		return vc.writeErr
	}
	if vc.writeDeadlineNano > 0 && time.Now().UnixNano() > vc.writeDeadlineNano {
		return os.ErrDeadlineExceeded
	}
	return nil
}

func isWouldBlock(err error) bool {
	return errors.Is(err, syscall.EAGAIN) || errors.Is(err, syscall.EWOULDBLOCK)
}

type outBlock struct {
	buf  []byte
	pool *sync.Pool
}

var (
	outPool4k = sync.Pool{
		New: func() any { return &outBlock{buf: make([]byte, 4096)} },
	}
	outPool16k = sync.Pool{
		New: func() any { return &outBlock{buf: make([]byte, 16384)} },
	}
	outPool64k = sync.Pool{
		New: func() any { return &outBlock{buf: make([]byte, 65536)} },
	}
	outPool256k = sync.Pool{
		New: func() any { return &outBlock{buf: make([]byte, 262144)} },
	}
	outPool1m = sync.Pool{
		New: func() any { return &outBlock{buf: make([]byte, 1048576)} },
	}
)

func getOutBlock(size int) *outBlock {
	var ob *outBlock
	switch {
	case size <= 4096:
		ob = outPool4k.Get().(*outBlock)
		ob.pool = &outPool4k
	case size <= 16384:
		ob = outPool16k.Get().(*outBlock)
		ob.pool = &outPool16k
	case size <= 65536:
		ob = outPool64k.Get().(*outBlock)
		ob.pool = &outPool64k
	case size <= 262144:
		ob = outPool256k.Get().(*outBlock)
		ob.pool = &outPool256k
	case size <= 1048576:
		ob = outPool1m.Get().(*outBlock)
		ob.pool = &outPool1m
	default:
		return &outBlock{buf: make([]byte, size)}
	}
	return ob
}

func putOutBlock(ob *outBlock) {
	if ob != nil && ob.pool != nil {
		ob.pool.Put(ob)
	}
}

func (vc *VirtualConn) releaseOutLocked() {
	if vc.outBlock != nil {
		putOutBlock(vc.outBlock)
		vc.outBlock = nil
	}
	vc.outBuf = nil
	vc.outReadOff = 0
}

func (vc *VirtualConn) ensureOutCapLocked(needed int) {
	if vc.outBlock == nil {
		vc.outBlock = getOutBlock(needed)
		vc.outBuf = vc.outBlock.buf[:0]
		vc.outReadOff = 0
		return
	}
	unread := len(vc.outBuf) - vc.outReadOff
	if unread == 0 {
		vc.outBuf = vc.outBlock.buf[:0]
		vc.outReadOff = 0
		if cap(vc.outBlock.buf) >= needed {
			return
		}
	}
	if cap(vc.outBlock.buf) >= unread+needed {
		if vc.outReadOff > 0 {
			copy(vc.outBlock.buf, vc.outBuf[vc.outReadOff:])
			vc.outBuf = vc.outBlock.buf[:unread]
			vc.outReadOff = 0
		}
		return
	}
	newBlock := getOutBlock(unread + needed)
	newBuf := newBlock.buf[:0]
	if unread > 0 {
		newBuf = append(newBuf, vc.outBuf[vc.outReadOff:]...)
	}
	putOutBlock(vc.outBlock)
	vc.outBlock = newBlock
	vc.outBuf = newBuf
	vc.outReadOff = 0
}

// Write implements net.Conn. Data is written directly to the socket when no
// backlog exists; any remainder is queued for the reactor.
func (vc *VirtualConn) Write(b []byte) (int, error) {
	if len(b) == 0 {
		return 0, nil
	}
	vc.wmu.Lock()
	if err := vc.checkWritableLocked(); err != nil {
		vc.wmu.Unlock()
		return 0, err
	}

	origLen := len(b)
	unread := len(vc.outBuf) - vc.outReadOff
	if unread == 0 && vc.canDirectWrite() {
		n, err := vc.doDirectWrite(b)
		if n == len(b) {
			vc.wmu.Unlock()
			return n, nil
		}
		if n > 0 {
			b = b[n:]
		}
		if err != nil && !isWouldBlock(err) {
			vc.writeErr = err
			vc.wmu.Unlock()
			return n, err
		}
	}

	unread = len(vc.outBuf) - vc.outReadOff
	if unread+len(b) > maxOutboundBufferSize {
		vc.wmu.Unlock()
		return 0, ErrWriteBufferFull
	}
	vc.ensureOutCapLocked(len(b))
	vc.outBuf = append(vc.outBuf, b...)
	vc.wmu.Unlock()
	vc.notifyWritable()
	return origLen, nil
}

// WriteVector writes multiple byte slices in a single logical write operation,
// using writev when possible to avoid multiple syscalls and memory copies.
func (vc *VirtualConn) WriteVector(iovs [][]byte) (int, error) {
	totalLen := 0
	for _, b := range iovs {
		totalLen += len(b)
	}
	if totalLen == 0 {
		return 0, nil
	}

	vc.wmu.Lock()
	if err := vc.checkWritableLocked(); err != nil {
		vc.wmu.Unlock()
		return 0, err
	}

	written := 0
	unread := len(vc.outBuf) - vc.outReadOff
	if unread == 0 && vc.canDirectWritev() {
		n, err := vc.doDirectWritev(iovs)
		if n > 0 {
			written = n
		}
		if written == totalLen {
			vc.wmu.Unlock()
			return totalLen, nil
		}
		if err != nil && !isWouldBlock(err) {
			vc.writeErr = err
			vc.wmu.Unlock()
			return written, err
		}
	}

	remLen := totalLen - written
	unread = len(vc.outBuf) - vc.outReadOff
	if unread+remLen > maxOutboundBufferSize {
		vc.wmu.Unlock()
		return written, ErrWriteBufferFull
	}

	vc.ensureOutCapLocked(remLen)
	skip := written
	for _, b := range iovs {
		if skip >= len(b) {
			skip -= len(b)
			continue
		}
		vc.outBuf = append(vc.outBuf, b[skip:]...)
		skip = 0
	}
	vc.wmu.Unlock()
	vc.notifyWritable()
	return totalLen, nil
}

// flushOut writes queued outbound bytes directly to the socket. It is the
// reactor's write-readiness path. pending reports whether data is still queued.
func (vc *VirtualConn) flushOut() (pending bool, err error) {
	vc.wmu.Lock()
	defer vc.wmu.Unlock()
	for vc.outReadOff < len(vc.outBuf) {
		if !vc.canDirectWrite() {
			return true, nil
		}
		n, werr := vc.doDirectWrite(vc.outBuf[vc.outReadOff:])
		if n > 0 {
			vc.outReadOff += n
		}
		if werr != nil {
			if isWouldBlock(werr) {
				return vc.outReadOff < len(vc.outBuf), nil
			}
			vc.writeErr = werr
			return false, werr
		}
		if n == 0 {
			return true, nil
		}
	}
	vc.releaseOutLocked()
	return false, nil
}

// DrainWrite copies queued outbound bytes into dst and removes them from
// the write buffer. Returns the number of bytes copied and whether more
// data remains.
func (vc *VirtualConn) DrainWrite(dst []byte) (n int, remaining bool) {
	vc.wmu.Lock()
	defer vc.wmu.Unlock()
	unread := len(vc.outBuf) - vc.outReadOff
	if unread <= 0 {
		return 0, false
	}
	n = copy(dst, vc.outBuf[vc.outReadOff:])
	vc.outReadOff += n
	if vc.outReadOff >= len(vc.outBuf) {
		vc.releaseOutLocked()
		return n, false
	}
	return n, true
}

// PendingWrite reports whether outbound data is queued.
func (vc *VirtualConn) PendingWrite() bool {
	vc.wmu.Lock()
	defer vc.wmu.Unlock()
	return len(vc.outBuf)-vc.outReadOff > 0
}

// UnshiftWrite prepends bytes to the front of the outbound buffer
// (used when a partial socket write needs retry).
func (vc *VirtualConn) UnshiftWrite(b []byte) {
	if len(b) == 0 {
		return
	}
	vc.wmu.Lock()
	defer vc.wmu.Unlock()
	if vc.outBlock != nil && vc.outReadOff >= len(b) {
		copy(vc.outBlock.buf[vc.outReadOff-len(b):], b)
		vc.outReadOff -= len(b)
		return
	}
	unread := len(vc.outBuf) - vc.outReadOff
	needed := unread + len(b)
	newBlock := getOutBlock(needed)
	newBuf := newBlock.buf[:0]
	newBuf = append(newBuf, b...)
	if unread > 0 {
		newBuf = append(newBuf, vc.outBuf[vc.outReadOff:]...)
	}
	putOutBlock(vc.outBlock)
	vc.outBlock = newBlock
	vc.outBuf = newBuf
	vc.outReadOff = 0
}

// ---------------------------------------------------------------------------
// net.Conn plumbing
// ---------------------------------------------------------------------------

// Close implements net.Conn. Readers are unblocked with io.EOF, further writes
// fail with net.ErrClosed, and the close callback (if any) is invoked once.
func (vc *VirtualConn) Close() error {
	if !vc.closed.CompareAndSwap(false, true) {
		return nil
	}
	vc.mu.Lock()
	vc.readEOF = true
	if vc.inCond != nil {
		vc.inCond.Broadcast()
	}
	vc.mu.Unlock()

	vc.notifyClose()
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
	if vc.raddrLen == 4 {
		return &net.TCPAddr{
			IP:   net.IPv4(vc.raddrIP[0], vc.raddrIP[1], vc.raddrIP[2], vc.raddrIP[3]),
			Port: int(vc.raddrPort),
		}
	}
	if vc.raddrLen == 16 {
		ip := make(net.IP, 16)
		copy(ip, vc.raddrIP[:])
		return &net.TCPAddr{
			IP:   ip,
			Port: int(vc.raddrPort),
		}
	}
	return &net.TCPAddr{}
}

// RemoteAddrString returns the textual peer address, formatting it once per
// connection rather than once per request.
func (vc *VirtualConn) RemoteAddrString() string {
	if s, ok := vc.raddrStr.Load().(string); ok {
		return s
	}
	s := vc.RemoteAddr().String()
	vc.raddrStr.Store(s)
	return s
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
	var nano int64
	if !t.IsZero() {
		nano = t.UnixNano()
	}
	vc.mu.Lock()
	vc.readDeadlineNano = nano
	if vc.inCond != nil {
		vc.inCond.Broadcast()
	}
	vc.mu.Unlock()
	return nil
}

// SetWriteDeadline implements net.Conn.
func (vc *VirtualConn) SetWriteDeadline(t time.Time) error {
	var nano int64
	if !t.IsZero() {
		nano = t.UnixNano()
	}
	vc.wmu.Lock()
	vc.writeDeadlineNano = nano
	vc.wmu.Unlock()
	return nil
}

// Closed reports whether Close has been called.
func (vc *VirtualConn) Closed() bool {
	return vc.closed.Load()
}
