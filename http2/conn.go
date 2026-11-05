package http2

import (
	"context"
	"crypto/tls"
	"encoding/binary"
	"errors"
	"log/slog"
	"math"
	"net"
	"net/http"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"github.com/linfeip/fnet"
	"github.com/linfeip/fnet/internal/bytepool"
	"github.com/linfeip/fnet/internal/httpx"
	"golang.org/x/net/http2/hpack"
)

const (
	maxContinuationFrames = 64
	maxResetBurst         = 1000
	maxControlBurst       = 2000
	maxCopiedBody         = 4 << 10
)

var serverOptionsHandler = http.HandlerFunc(func(http.ResponseWriter, *http.Request) {})

type tlsConnection interface {
	ConnectionState() tls.ConnectionState
}

func isAllowedTLS12Cipher(suite uint16) bool {
	switch suite {
	case tls.TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256,
		tls.TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256,
		tls.TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384,
		tls.TLS_ECDHE_ECDSA_WITH_AES_256_GCM_SHA384,
		tls.TLS_ECDHE_RSA_WITH_CHACHA20_POLY1305_SHA256,
		tls.TLS_ECDHE_ECDSA_WITH_CHACHA20_POLY1305_SHA256:
		return true
	default:
		return false
	}
}

type bufferWriter struct {
	buf bytepool.Buffer
}

func (w *bufferWriter) Write(p []byte) (int, error) {
	w.buf.Write(p)
	return len(p), nil
}

// connState holds connection-level metadata constructed on the first stream of a connection.
type connState struct {
	connection *conn
	remoteAddr string
	tlsState   *tls.ConnectionState
	baseCtx    atomic.Pointer[context.Context]
}

// conn manages a single HTTP/2 connection without spawning a dedicated read or control goroutine.
//
// Lock hierarchy: when acquiring both locks, writeMu MUST always be acquired before mu. Neither mu nor writeMu is ever
// held while waiting on channels/timers, and mu is never held across socket writes or context cancellation callbacks.
type conn struct {
	srv        *Server
	connection fnet.Conn
	state      *connState

	// Inbound framing & HPACK decoding state: accessed exclusively by fnet's serial callbacks.
	hpackDecoder         *hpack.Decoder
	headerBuilder        headerBuilder
	continuationStreamID uint32
	continuationFrames   int
	maxClientStreamID    uint32
	controlTokens        int
	prefaceRead          uint8
	broken               bool

	// Outbound frame & HPACK serialization: guarded by writeMu.
	writeMu      sync.Mutex
	hpackEncoder *hpack.Encoder
	headerWriter bufferWriter

	// Shared connection & stream state: guarded by mu.
	mu                     sync.Mutex
	streams                map[uint32]*stream
	waitingStreams         []*stream
	sendWindow             int64
	receiveWindow          int32
	unackedReceiveBytes    int32
	peerInitialWindowSize  int32
	peerMaxFrameSize       uint32
	peerHeaderTableSize    uint32
	minHeaderTableSize     uint32
	headerTableSizeUpdated bool
	activeStreams          uint32
	resetTokens            int
	recentResets           [4]uint32
	recentResetPos         uint8
	sawFirstSettings       bool
	inHeaderBlock          bool
	partialFrame           bool
	goingAway              bool
	closeWhenIdle          bool
	closed                 atomic.Bool
}

func newConn(s *Server, c fnet.Conn) *conn {
	return &conn{
		srv:                   s,
		connection:            c,
		controlTokens:         maxControlBurst,
		sendWindow:            defaultInitialWindowSize,
		receiveWindow:         int32(s.opts.ConnectionWindowSize),
		peerInitialWindowSize: defaultInitialWindowSize,
		peerMaxFrameSize:      minMaxFrameSize,
		peerHeaderTableSize:   defaultHeaderTableSize,
		minHeaderTableSize:    math.MaxUint32,
		resetTokens:           maxResetBurst,
	}
}

func (c *conn) onOpen() {
	if tc, ok := c.connection.(tlsConnection); ok {
		st := tc.ConnectionState()
		if st.Version < tls.VersionTLS12 || (st.Version == tls.VersionTLS12 && !isAllowedTLS12Cipher(st.CipherSuite)) {
			c.failConnImmediate(errCodeInadequateSecurity)
			return
		}
		if st.NegotiatedProtocol != "h2" {
			c.failConnImmediate(errCodeProtocol)
			return
		}
	}

	c.connection.SetDeadline(httpx.DeadlineAfter(c.srv.opts.ReadHeaderTimeout))

	out := bytepool.Get(64)
	appendSettings(&out,
		setting{id: settingMaxConcurrentStreams, value: c.srv.opts.MaxConcurrentStreams},
		setting{id: settingInitialWindowSize, value: uint32(c.srv.opts.InitialWindowSize)},
		setting{id: settingMaxFrameSize, value: uint32(c.srv.opts.MaxReadFrameSize)},
		setting{id: settingMaxHeaderListSize, value: uint32(c.srv.opts.MaxHeaderBytes)},
		setting{id: settingNoRFC7540Priorities, value: 1},
	)
	if extra := c.srv.opts.ConnectionWindowSize - defaultInitialWindowSize; extra > 0 {
		appendWindowUpdate(&out, 0, uint32(extra))
	}
	c.writeMu.Lock()
	_, err := c.connection.Write(out.Bytes())
	c.writeMu.Unlock()
	out.Release()
	if err != nil {
		c.broken = true
		c.connection.Close()
	}
}

func (c *conn) failConnImmediate(code errCode) {
	c.broken = true
	var out bytepool.Buffer
	appendGoAway(&out, 0, code)
	c.writeMu.Lock()
	_, _ = c.connection.Write(out.Bytes())
	c.writeMu.Unlock()
	out.Release()
	c.connection.Close()
}

// setDeadlineLocked updates the connection deadline based on its current state; caller holds mu.
func (c *conn) setDeadlineLocked() {
	if c.closed.Load() {
		return
	}
	var timeout time.Duration
	switch {
	case c.goingAway && c.activeStreams == 0:
		timeout = c.srv.opts.ReadHeaderTimeout
	case !c.sawFirstSettings || c.inHeaderBlock || (c.activeStreams == 0 && c.partialFrame):
		timeout = c.srv.opts.ReadHeaderTimeout
	case c.activeStreams > 0:
		timeout = 0
	default:
		timeout = c.srv.opts.IdleTimeout
	}
	c.connection.SetDeadline(httpx.DeadlineAfter(timeout))
}

func (c *conn) onData(data []byte) int {
	if c.broken {
		return len(data)
	}
	consumed := 0

	if int(c.prefaceRead) < len(clientPreface) {
		need := len(clientPreface) - int(c.prefaceRead)
		n := min(len(data), need)
		if string(data[:n]) != clientPreface[c.prefaceRead:int(c.prefaceRead)+n] {
			c.broken = true
			c.connection.Close()
			return len(data)
		}
		c.prefaceRead += uint8(n)
		consumed += n
		if int(c.prefaceRead) < len(clientPreface) {
			return consumed
		}
	}

	var outControl bytepool.Buffer
	defer outControl.Release()

	var cancelsBuf [4]context.CancelFunc
	var newStreamsBuf [8]*stream
	cancels := cancelsBuf[:0]
	newStreams := newStreamsBuf[:0]
	refreshIdle := false

	maxReadSize := uint32(c.srv.opts.MaxReadFrameSize)
	for len(data)-consumed >= frameHeaderLen {
		fh := parseFrameHeader(data[consumed:])
		if fh.length > maxReadSize {
			c.abortInOnData(&outControl, errCodeFrameSize)
			consumed = len(data)
			break
		}
		frameLen := frameHeaderLen + int(fh.length)
		if len(data)-consumed < frameLen {
			break
		}
		payload := data[consumed+frameHeaderLen : consumed+frameLen]
		consumed += frameLen

		if !c.sawFirstSettings && (fh.frameType != frameSettings || fh.flags&flagAck != 0 || fh.streamID != 0) {
			c.abortInOnData(&outControl, errCodeProtocol)
			consumed = len(data)
			break
		}

		var s *stream
		var cancel context.CancelFunc
		var code errCode

		if c.continuationStreamID != 0 {
			if fh.frameType != frameContinuation || fh.streamID != c.continuationStreamID {
				code = errCodeProtocol
			} else {
				s, cancel, code = c.handleContinuation(fh, payload, &outControl)
			}
		} else {
			switch fh.frameType {
			case frameData:
				cancel, code = c.handleData(fh, payload, &outControl)
			case frameHeaders:
				s, cancel, code = c.handleHeaders(fh, payload, &outControl)
			case framePriority:
				code = c.handlePriority(fh, payload, &outControl)
			case frameRSTStream:
				cancel, code = c.handleRSTStream(fh, &outControl)
			case frameSettings:
				code = c.handleSettings(fh, payload, &outControl)
			case framePushPromise, frameContinuation:
				code = errCodeProtocol
			case framePing:
				code = c.handlePing(fh, payload, &outControl)
				refreshIdle = true
			case frameGoAway:
				code = c.handleGoAway(fh)
			case frameWindowUpdate:
				cancel, code = c.handleWindowUpdate(fh, payload, &outControl)
			}
		}

		if cancel != nil {
			cancels = append(cancels, cancel)
		}
		if s != nil {
			newStreams = append(newStreams, s)
		}
		if code != errCodeNo {
			c.abortInOnData(&outControl, code)
			consumed = len(data)
			break
		}
	}

	if outControl.Len() > 0 {
		c.writeMu.Lock()
		_, err := c.connection.Write(outControl.Bytes())
		c.writeMu.Unlock()
		if err != nil {
			c.broken = true
		}
	}

	if c.broken {
		c.connection.Close()
	} else {
		partial := consumed < len(data)
		inHeader := c.continuationStreamID != 0
		c.mu.Lock()
		changed := partial != c.partialFrame || inHeader != c.inHeaderBlock || refreshIdle
		c.partialFrame = partial
		c.inHeaderBlock = inHeader
		if changed {
			c.setDeadlineLocked()
		}
		shouldClose := c.goingAway && c.activeStreams == 0
		c.mu.Unlock()
		if shouldClose {
			c.connection.Close()
		}
	}

	for _, cancel := range cancels {
		cancel()
	}
	fd := c.connection.Fd()
	for _, s := range newStreams {
		streamToServe := s
		c.srv.opts.Executor(fd+int(streamToServe.id>>1), func() {
			c.serveStream(streamToServe)
		})
	}
	return consumed
}

func (c *conn) abortInOnData(outControl *bytepool.Buffer, code errCode) {
	c.broken = true
	c.mu.Lock()
	c.goingAway = true
	lastID := c.maxClientStreamID
	c.mu.Unlock()
	appendGoAway(outControl, lastID, code)
}

func (c *conn) takeControlToken() bool {
	if c.controlTokens <= 0 {
		return false
	}
	c.controlTokens--
	return true
}

func (c *conn) replenishControlTokens(n int) {
	c.controlTokens = min(c.controlTokens+n, maxControlBurst)
}

// failStreamLocked resets s from OnData, queues RST_STREAM and any pending connection WINDOW_UPDATE into outControl,
// and returns s's context CancelFunc; caller holds mu.
func (c *conn) failStreamLocked(s *stream, readErr error, code errCode, outControl *bytepool.Buffer) context.CancelFunc {
	alreadyReset := s.sentReset
	s.sentReset = true
	c.noteServerResetLocked(s.id)
	cancel := c.removeStreamLocked(s, readErr, errStreamReset)
	if inc := c.takeConnWindowUpdateLocked(); inc > 0 {
		appendWindowUpdate(outControl, 0, inc)
	}
	if !alreadyReset {
		appendRSTStream(outControl, s.id, code)
	}
	return cancel
}

func (c *conn) handleSettings(fh frameHeader, payload []byte, outControl *bytepool.Buffer) errCode {
	if fh.streamID != 0 {
		return errCodeProtocol
	}
	if fh.flags&flagAck != 0 {
		if fh.length != 0 {
			return errCodeFrameSize
		}
		return errCodeNo
	}
	if fh.length%6 != 0 {
		return errCodeFrameSize
	}
	if !c.takeControlToken() {
		return errCodeEnhanceYourCalm
	}

	c.mu.Lock()
	firstSettings := !c.sawFirstSettings
	c.sawFirstSettings = true
	for i := 0; i < len(payload); i += 6 {
		id := binary.BigEndian.Uint16(payload[i : i+2])
		val := binary.BigEndian.Uint32(payload[i+2 : i+6])
		switch id {
		case settingHeaderTableSize:
			capped := min(val, defaultHeaderTableSize)
			if capped < c.minHeaderTableSize {
				c.minHeaderTableSize = capped
			}
			c.peerHeaderTableSize = capped
			c.headerTableSizeUpdated = true
		case settingEnablePush:
			if val > 1 {
				c.mu.Unlock()
				return errCodeProtocol
			}
		case settingInitialWindowSize:
			if val > maxWindowSize {
				c.mu.Unlock()
				return errCodeFlowControl
			}
			delta := int64(val) - int64(c.peerInitialWindowSize)
			c.peerInitialWindowSize = int32(val)
			if delta != 0 {
				for _, s := range c.streams {
					if s.sendWindow+delta > maxWindowSize {
						c.mu.Unlock()
						return errCodeFlowControl
					}
					wasNonPositive := s.sendWindow <= 0
					s.sendWindow += delta
					if wasNonPositive && s.sendWindow > 0 {
						s.wakeWriterLocked()
					}
				}
			}
		case settingMaxFrameSize:
			if val < minMaxFrameSize || val > maxFrameSizeLimit {
				c.mu.Unlock()
				return errCodeProtocol
			}
			c.peerMaxFrameSize = val
		}
	}
	if firstSettings {
		c.setDeadlineLocked()
	}
	c.mu.Unlock()

	appendSettingsAck(outControl)
	return errCodeNo
}

func (c *conn) handlePing(fh frameHeader, payload []byte, outControl *bytepool.Buffer) errCode {
	if fh.streamID != 0 {
		return errCodeProtocol
	}
	if fh.length != 8 {
		return errCodeFrameSize
	}
	if fh.flags&flagAck != 0 {
		return errCodeNo
	}
	if !c.takeControlToken() {
		return errCodeEnhanceYourCalm
	}
	appendPing(outControl, true, payload)
	return errCodeNo
}

func (c *conn) handleGoAway(fh frameHeader) errCode {
	if fh.streamID != 0 {
		return errCodeProtocol
	}
	if fh.length < 8 {
		return errCodeFrameSize
	}
	return errCodeNo
}

func (c *conn) handlePriority(fh frameHeader, payload []byte, outControl *bytepool.Buffer) errCode {
	if fh.streamID == 0 {
		return errCodeProtocol
	}
	if fh.length != 5 {
		return errCodeFrameSize
	}
	if binary.BigEndian.Uint32(payload[:4])&0x7fffffff == fh.streamID {
		if !c.takeControlToken() {
			return errCodeEnhanceYourCalm
		}
		appendRSTStream(outControl, fh.streamID, errCodeProtocol)
	}
	return errCodeNo
}

func (c *conn) handleWindowUpdate(fh frameHeader, payload []byte, outControl *bytepool.Buffer) (context.CancelFunc, errCode) {
	if fh.length != 4 {
		return nil, errCodeFrameSize
	}
	inc := binary.BigEndian.Uint32(payload) & 0x7fffffff
	if inc == 0 {
		if fh.streamID == 0 {
			return nil, errCodeProtocol
		}
		if !c.takeControlToken() {
			return nil, errCodeEnhanceYourCalm
		}
		appendRSTStream(outControl, fh.streamID, errCodeProtocol)
		return nil, errCodeNo
	}

	if fh.streamID == 0 {
		c.mu.Lock()
		if c.sendWindow+int64(inc) > maxWindowSize {
			c.mu.Unlock()
			return nil, errCodeFlowControl
		}
		c.sendWindow += int64(inc)
		c.wakeWaitingStreamsLocked()
		c.mu.Unlock()
		return nil, errCodeNo
	}

	if fh.streamID%2 == 0 || fh.streamID > c.maxClientStreamID {
		return nil, errCodeProtocol
	}
	c.mu.Lock()
	s := c.streams[fh.streamID]
	if s == nil {
		c.mu.Unlock()
		return nil, errCodeNo
	}
	if s.sendWindow+int64(inc) > maxWindowSize {
		cancel := c.failStreamLocked(s, errStreamReset, errCodeFlowControl, outControl)
		c.mu.Unlock()
		return cancel, errCodeNo
	}
	wasNonPositive := s.sendWindow <= 0
	s.sendWindow += int64(inc)
	if wasNonPositive && s.sendWindow > 0 {
		s.wakeWriterLocked()
	}
	c.mu.Unlock()
	return nil, errCodeNo
}

func (c *conn) handleRSTStream(fh frameHeader, outControl *bytepool.Buffer) (context.CancelFunc, errCode) {
	if fh.streamID == 0 {
		return nil, errCodeProtocol
	}
	if fh.length != 4 {
		return nil, errCodeFrameSize
	}
	if fh.streamID%2 == 0 || fh.streamID > c.maxClientStreamID {
		return nil, errCodeProtocol
	}

	c.mu.Lock()
	if c.resetTokens <= 0 {
		c.mu.Unlock()
		return nil, errCodeEnhanceYourCalm
	}
	c.resetTokens--
	var cancel context.CancelFunc
	if s := c.streams[fh.streamID]; s != nil {
		s.sentReset = true
		cancel = c.removeStreamLocked(s, errStreamReset, errStreamReset)
		if inc := c.takeConnWindowUpdateLocked(); inc > 0 {
			appendWindowUpdate(outControl, 0, inc)
		}
	}
	c.mu.Unlock()
	return cancel, errCodeNo
}

func (c *conn) handleData(fh frameHeader, payload []byte, outControl *bytepool.Buffer) (context.CancelFunc, errCode) {
	if fh.streamID == 0 || fh.streamID%2 == 0 || fh.streamID > c.maxClientStreamID {
		return nil, errCodeProtocol
	}
	data, padBytes, ok := stripPadding(payload, fh.flags)
	if !ok {
		return nil, errCodeProtocol
	}
	frameBytes := int32(fh.length)

	c.mu.Lock()
	if frameBytes > c.receiveWindow {
		c.mu.Unlock()
		return nil, errCodeFlowControl
	}
	c.receiveWindow -= frameBytes

	s := c.streams[fh.streamID]
	if s == nil {
		c.unackedReceiveBytes += frameBytes
		if inc := c.takeConnWindowUpdateLocked(); inc > 0 {
			appendWindowUpdate(outControl, 0, inc)
		}
		c.mu.Unlock()
		if !c.takeControlToken() {
			return nil, errCodeEnhanceYourCalm
		}
		appendRSTStream(outControl, fh.streamID, errCodeStreamClosed)
		return nil, errCodeNo
	}
	if s.state == streamHalfClosedRemote || s.state == streamClosed || s.gotTrailers {
		c.unackedReceiveBytes += frameBytes
		cancel := c.failStreamLocked(s, errStreamReset, errCodeStreamClosed, outControl)
		c.mu.Unlock()
		return cancel, errCodeNo
	}
	if frameBytes > s.receiveWindow {
		c.unackedReceiveBytes += frameBytes
		cancel := c.failStreamLocked(s, errStreamReset, errCodeFlowControl, outControl)
		c.mu.Unlock()
		return cancel, errCodeNo
	}
	s.receiveWindow -= frameBytes

	endStream := fh.flags&flagEndStream != 0
	newTotal := s.receivedBytes + int64(len(data))
	if s.declaredLength >= 0 && (newTotal > s.declaredLength || (endStream && newTotal != s.declaredLength)) {
		c.unackedReceiveBytes += frameBytes
		cancel := c.failStreamLocked(s, errContentLengthMismatch, errCodeProtocol, outControl)
		c.mu.Unlock()
		return cancel, errCodeNo
	}
	s.receivedBytes = newTotal

	if s.bodyClosed {
		c.unackedReceiveBytes += frameBytes
	} else {
		if padBytes > 0 {
			c.unackedReceiveBytes += int32(padBytes)
			s.unackedBytes += int32(padBytes)
		}
		if len(data) > 0 {
			s.appendDataLocked(data)
		}
	}

	var cancel context.CancelFunc
	if endStream {
		s.eof = true
		if s.state == streamHalfClosedLocal {
			cancel = c.removeStreamLocked(s, nil, nil)
		} else {
			s.state = streamHalfClosedRemote
		}
	}
	if inc := c.takeConnWindowUpdateLocked(); inc > 0 {
		appendWindowUpdate(outControl, 0, inc)
	}
	if !s.eof && !s.bodyClosed && s.unackedBytes >= int32(c.srv.opts.InitialWindowSize)/2 {
		inc := uint32(s.unackedBytes)
		s.receiveWindow += int32(inc)
		s.unackedBytes = 0
		appendWindowUpdate(outControl, s.id, inc)
	}
	s.wakeReaderLocked()
	c.mu.Unlock()
	return cancel, errCodeNo
}

func (c *conn) handleHeaders(fh frameHeader, payload []byte, outControl *bytepool.Buffer) (*stream, context.CancelFunc, errCode) {
	if fh.streamID == 0 || fh.streamID%2 == 0 {
		return nil, nil, errCodeProtocol
	}
	frag, _, ok := stripPadding(payload, fh.flags)
	if !ok {
		return nil, nil, errCodeProtocol
	}
	selfDep := false
	if fh.flags&flagPriority != 0 {
		if len(frag) < 5 {
			return nil, nil, errCodeFrameSize
		}
		selfDep = binary.BigEndian.Uint32(frag[:4])&0x7fffffff == fh.streamID
		frag = frag[5:]
	}

	c.mu.Lock()
	existing := c.streams[fh.streamID]
	wasReset := existing == nil && c.wasRecentlyResetLocked(fh.streamID)
	c.mu.Unlock()
	isTrailer := existing != nil || wasReset
	if !isTrailer && fh.streamID <= c.maxClientStreamID {
		return nil, nil, errCodeProtocol
	}

	if c.hpackDecoder == nil {
		dec := hpack.NewDecoder(defaultHeaderTableSize, nil)
		dec.SetMaxStringLength(max(2*c.srv.opts.MaxHeaderBytes, minMaxFrameSize))
		c.hpackDecoder = dec
	}
	c.headerBuilder.reset(fh.streamID, uint32(c.srv.opts.MaxHeaderBytes), isTrailer, fh.flags&flagEndStream != 0, selfDep)
	c.hpackDecoder.SetEmitEnabled(true)
	c.hpackDecoder.SetEmitFunc(func(hf hpack.HeaderField) {
		if !c.headerBuilder.emit(hf) {
			c.hpackDecoder.SetEmitEnabled(false)
		}
	})

	if _, err := c.hpackDecoder.Write(frag); err != nil {
		c.hpackDecoder.SetEmitFunc(func(hpack.HeaderField) {})
		return nil, nil, errCodeCompression
	}

	if fh.flags&flagEndHeaders == 0 {
		c.continuationStreamID = fh.streamID
		c.continuationFrames = 0
		return nil, nil, errCodeNo
	}
	return c.finishHeaderBlock(outControl)
}

func (c *conn) handleContinuation(fh frameHeader, payload []byte, outControl *bytepool.Buffer) (*stream, context.CancelFunc, errCode) {
	c.continuationFrames++
	if c.continuationFrames > maxContinuationFrames || c.headerBuilder.invalid || int64(len(payload)) > 2*int64(c.headerBuilder.remainingBytes) {
		c.hpackDecoder.SetEmitFunc(func(hpack.HeaderField) {})
		return nil, nil, errCodeEnhanceYourCalm
	}
	if len(payload) == 0 && fh.flags&flagEndHeaders == 0 {
		c.hpackDecoder.SetEmitFunc(func(hpack.HeaderField) {})
		return nil, nil, errCodeEnhanceYourCalm
	}
	if _, err := c.hpackDecoder.Write(payload); err != nil {
		c.hpackDecoder.SetEmitFunc(func(hpack.HeaderField) {})
		return nil, nil, errCodeCompression
	}
	if fh.flags&flagEndHeaders == 0 {
		return nil, nil, errCodeNo
	}
	c.continuationStreamID = 0
	c.continuationFrames = 0
	return c.finishHeaderBlock(outControl)
}

func (c *conn) finishHeaderBlock(outControl *bytepool.Buffer) (*stream, context.CancelFunc, errCode) {
	c.hpackDecoder.SetEmitFunc(func(hpack.HeaderField) {})
	if err := c.hpackDecoder.Close(); err != nil {
		return nil, nil, errCodeCompression
	}

	streamID := c.headerBuilder.streamID
	if c.headerBuilder.isTrailer {
		cancel, code := c.finishTrailerBlock(streamID, outControl)
		return nil, cancel, code
	}

	c.maxClientStreamID = streamID
	c.replenishControlTokens(2)

	c.mu.Lock()
	if c.goingAway {
		c.mu.Unlock()
		return nil, nil, errCodeNo
	}
	if c.activeStreams >= c.srv.opts.MaxConcurrentStreams {
		c.mu.Unlock()
		if !c.takeControlToken() {
			return nil, nil, errCodeEnhanceYourCalm
		}
		appendRSTStream(outControl, streamID, errCodeRefusedStream)
		return nil, nil, errCodeNo
	}
	c.mu.Unlock()

	parsed, ok := c.headerBuilder.buildRequest()
	if !ok {
		if !c.takeControlToken() {
			return nil, nil, errCodeEnhanceYourCalm
		}
		appendRSTStream(outControl, streamID, errCodeProtocol)
		return nil, nil, errCodeNo
	}
	if parsed.status != 0 {
		c.replyErrorStatus(streamID, parsed.status, c.headerBuilder.streamEnded)
		return nil, nil, errCodeNo
	}

	state := c.ensureConnState()
	initialState := streamOpen
	if c.headerBuilder.streamEnded {
		initialState = streamHalfClosedRemote
	}

	c.mu.Lock()
	if c.goingAway || c.closed.Load() {
		c.mu.Unlock()
		return nil, nil, errCodeNo
	}
	s := &stream{
		conn:           c,
		req:            parsed.req,
		id:             streamID,
		sendWindow:     int64(c.peerInitialWindowSize),
		receiveWindow:  int32(c.srv.opts.InitialWindowSize),
		declaredLength: parsed.declaredLength,
		state:          initialState,
		eof:            c.headerBuilder.streamEnded,
		expectContinue: parsed.expectContinue,
	}
	s.body.stream = s
	s.req.RemoteAddr = state.remoteAddr
	if s.req.URL.Scheme == "https" || state.tlsState != nil {
		s.req.TLS = state.tlsState
	}
	if c.headerBuilder.streamEnded {
		s.req.Body = http.NoBody
	} else {
		s.req.Body = &s.body
	}
	s.req = *s.req.WithContext(s)

	if c.streams == nil {
		c.streams = make(map[uint32]*stream)
	}
	c.streams[streamID] = s
	wasIdle := c.activeStreams == 0
	c.activeStreams++
	if wasIdle {
		c.setDeadlineLocked()
	}
	c.mu.Unlock()
	return s, nil, errCodeNo
}

func (c *conn) finishTrailerBlock(streamID uint32, outControl *bytepool.Buffer) (context.CancelFunc, errCode) {
	c.mu.Lock()
	s := c.streams[streamID]
	if s == nil {
		c.mu.Unlock()
		return nil, errCodeNo
	}
	if s.gotTrailers {
		c.mu.Unlock()
		return nil, errCodeProtocol
	}
	if s.state == streamHalfClosedRemote {
		cancel := c.failStreamLocked(s, errStreamReset, errCodeStreamClosed, outControl)
		c.mu.Unlock()
		return cancel, errCodeNo
	}
	if !c.headerBuilder.buildTrailers(s.req.Trailer) {
		cancel := c.failStreamLocked(s, errStreamReset, errCodeProtocol, outControl)
		c.mu.Unlock()
		return cancel, errCodeNo
	}
	if s.declaredLength >= 0 && s.receivedBytes != s.declaredLength {
		cancel := c.failStreamLocked(s, errContentLengthMismatch, errCodeProtocol, outControl)
		c.mu.Unlock()
		return cancel, errCodeNo
	}
	s.gotTrailers = true
	s.eof = true
	var cancel context.CancelFunc
	if s.state == streamHalfClosedLocal {
		cancel = c.removeStreamLocked(s, nil, nil)
	} else {
		s.state = streamHalfClosedRemote
	}
	s.wakeReaderLocked()
	c.mu.Unlock()
	return cancel, errCodeNo
}

func (c *conn) noteServerResetLocked(streamID uint32) {
	c.recentResets[c.recentResetPos&3] = streamID
	c.recentResetPos++
}

func (c *conn) wasRecentlyResetLocked(streamID uint32) bool {
	for _, id := range c.recentResets {
		if id == streamID {
			return true
		}
	}
	return false
}

// serveStream runs http.Handler for stream s on an Executor worker.
func (c *conn) serveStream(s *stream) {
	w := acquireResponse(s)
	defer w.release()

	returned := false
	defer func() {
		if !returned {
			c.resetStream(s, errCodeInternal)
		}
	}()

	if !c.runHandler(w, &s.req) {
		c.resetStream(s, errCodeInternal)
		returned = true
		return
	}
	w.finish()
	c.cleanupStreamAfterHandler(s)
	returned = true
}

func (c *conn) runHandler(w *response, req *http.Request) (ok bool) {
	reqBody := req.Body
	defer func() {
		_ = reqBody.Close()
		if req.MultipartForm != nil {
			_ = req.MultipartForm.RemoveAll()
		}
		if err := recover(); err != nil {
			if err != http.ErrAbortHandler {
				buf := make([]byte, 64<<10)
				slog.Error("http2: panic serving stream", "remoteAddr", c.state.remoteAddr, "streamID", w.stream.id,
					"panic", err, "stack", string(buf[:runtime.Stack(buf, false)]))
			}
			ok = false
		}
	}()
	handler := c.srv.handler
	if req.RequestURI == "*" && req.Method == http.MethodOptions {
		handler = serverOptionsHandler
	}
	handler.ServeHTTP(w, req)
	return true
}

func (c *conn) cleanupStreamAfterHandler(s *stream) {
	c.mu.Lock()
	if s.state == streamClosed {
		shouldCloseConn := c.closeWhenIdle && c.activeStreams == 0
		c.mu.Unlock()
		if shouldCloseConn {
			c.connection.SetDeadline(httpx.DeadlineAfter(c.srv.opts.IdleTimeout))
			c.connection.Close()
		}
		return
	}
	needReset := !s.sentReset && (!s.sentEndStream || s.state == streamOpen || s.state == streamHalfClosedLocal)
	if needReset {
		s.sentReset = true
		c.noteServerResetLocked(s.id)
	}
	cancel := c.removeStreamLocked(s, errBodyClosedByHandler, errStreamReset)
	connInc := c.takeConnWindowUpdateLocked()
	shouldCloseConn := c.closeWhenIdle && c.activeStreams == 0
	c.mu.Unlock()

	if needReset || connInc > 0 {
		var out bytepool.Buffer
		if connInc > 0 {
			appendWindowUpdate(&out, 0, connInc)
		}
		if needReset {
			code := errCodeNo
			if !s.sentEndStream {
				code = errCodeInternal
			}
			appendRSTStream(&out, s.id, code)
		}
		c.writeMu.Lock()
		if !c.closed.Load() {
			_, _ = c.connection.Write(out.Bytes())
		}
		c.writeMu.Unlock()
		out.Release()
	}
	if shouldCloseConn {
		c.connection.SetDeadline(httpx.DeadlineAfter(c.srv.opts.IdleTimeout))
		c.connection.Close()
	}
	if cancel != nil {
		cancel()
	}
}

// removeStreamLocked transitions s to streamClosed, removes it from c.streams, releases any unread body bytes, wakes
// blocked readers/writers and returns its context CancelFunc (if any); caller holds mu.
func (c *conn) removeStreamLocked(s *stream, readErr, writeErr error) context.CancelFunc {
	if s.state == streamClosed {
		return nil
	}
	s.state = streamClosed
	if readErr != nil && s.readErr == nil {
		s.readErr = readErr
	}
	if writeErr != nil && s.writeErr == nil {
		s.writeErr = writeErr
	}
	s.discardUnreadBodyLocked()
	c.unregisterWaiterLocked(s)
	if c.streams != nil {
		if _, exists := c.streams[s.id]; exists {
			delete(c.streams, s.id)
			if c.activeStreams > 0 {
				c.activeStreams--
			}
			if c.activeStreams == 0 {
				c.setDeadlineLocked()
			}
		}
	}
	s.wakeReaderLocked()
	s.wakeWriterLocked()
	return s.cancelFunc()
}

func (c *conn) unregisterWaiterLocked(s *stream) {
	if !s.waitingConn {
		return
	}
	s.waitingConn = false
	for i, w := range c.waitingStreams {
		if w == s {
			c.waitingStreams = append(c.waitingStreams[:i], c.waitingStreams[i+1:]...)
			return
		}
	}
}

func (c *conn) wakeWaitingStreamsLocked() {
	if len(c.waitingStreams) == 0 {
		return
	}
	for i, s := range c.waitingStreams {
		c.waitingStreams[i] = nil
		s.waitingConn = false
		s.wakeWriterLocked()
	}
	c.waitingStreams = c.waitingStreams[:0]
}

func (c *conn) consumeReceiveBytesLocked(s *stream, n int32) (connInc, streamInc uint32) {
	c.unackedReceiveBytes += n
	connInc = c.takeConnWindowUpdateLocked()
	if !s.eof && !s.bodyClosed && s.state != streamClosed {
		s.unackedBytes += n
		if s.unackedBytes >= int32(c.srv.opts.InitialWindowSize)/2 {
			streamInc = uint32(s.unackedBytes)
			s.receiveWindow += int32(streamInc)
			s.unackedBytes = 0
		}
	}
	return connInc, streamInc
}

func (c *conn) takeConnWindowUpdateLocked() uint32 {
	if c.unackedReceiveBytes >= int32(c.srv.opts.ConnectionWindowSize)/2 {
		inc := uint32(c.unackedReceiveBytes)
		c.receiveWindow += int32(inc)
		c.unackedReceiveBytes = 0
		return inc
	}
	return 0
}

func (c *conn) onWritable() {
	c.mu.Lock()
	c.wakeWaitingStreamsLocked()
	c.mu.Unlock()
}

func (c *conn) onClosing(err error) {
	if c.broken || errors.Is(err, fnet.ErrHandlerPanic) {
		return
	}
	c.mu.Lock()
	ready := c.sawFirstSettings
	c.mu.Unlock()
	if ready {
		c.sendGoAway(false)
	}
}

func (c *conn) onClose(_ error) {
	c.writeMu.Lock()
	c.headerWriter.buf.Release()
	c.writeMu.Unlock()

	c.mu.Lock()
	c.closed.Store(true)
	var cancels []context.CancelFunc
	for _, s := range c.streams {
		if cancel := c.removeStreamLocked(s, errClientDisconnected, net.ErrClosed); cancel != nil {
			cancels = append(cancels, cancel)
		}
	}
	c.streams = nil
	c.waitingStreams = nil
	c.mu.Unlock()

	for _, cancel := range cancels {
		cancel()
	}
}

func (c *conn) ensureConnState() *connState {
	if c.state != nil {
		return c.state
	}
	st := &connState{
		connection: c,
		remoteAddr: c.connection.RemoteAddr().String(),
	}
	if tc, ok := c.connection.(tlsConnection); ok {
		tlsSt := tc.ConnectionState()
		st.tlsState = &tlsSt
	}
	c.state = st
	return st
}

func (c *conn) baseContext() context.Context {
	st := c.state
	if ctx := st.baseCtx.Load(); ctx != nil {
		return *ctx
	}
	ctx := context.WithValue(context.Background(), http.ServerContextKey, c.srv.httpServer)
	ctx = context.WithValue(ctx, http.LocalAddrContextKey, c.connection.LocalAddr())
	st.baseCtx.CompareAndSwap(nil, &ctx)
	return *st.baseCtx.Load()
}
