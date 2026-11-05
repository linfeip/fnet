package http2

import (
	"context"
	"math"
	"net"
	"net/http"
	"os"
	"strconv"
	"time"

	"github.com/linfeip/fnet/internal/bytepool"
	"golang.org/x/net/http2/hpack"
)

// encoderLocked returns the connection's HPACK encoder with any pending dynamic table size updates applied; caller
// holds both writeMu and mu.
func (c *conn) encoderLocked() *hpack.Encoder {
	if c.hpackEncoder == nil {
		c.hpackEncoder = hpack.NewEncoder(&c.headerWriter)
	}
	if c.headerTableSizeUpdated {
		c.headerTableSizeUpdated = false
		if c.minHeaderTableSize < c.peerHeaderTableSize {
			c.hpackEncoder.SetMaxDynamicTableSize(c.minHeaderTableSize)
		}
		c.hpackEncoder.SetMaxDynamicTableSize(c.peerHeaderTableSize)
		c.minHeaderTableSize = math.MaxUint32
	}
	return c.hpackEncoder
}

func (c *conn) releaseHeaderWriterLocked() {
	if cap(c.headerWriter.buf.Bytes()) <= maxBufferedBody {
		c.headerWriter.buf.Reset()
	} else {
		c.headerWriter.buf.Release()
	}
}

func (c *conn) replyErrorStatus(streamID uint32, code int, clientEnded bool) {
	body := strconv.Itoa(code) + " " + http.StatusText(code)
	c.writeMu.Lock()
	c.mu.Lock()
	if c.closed.Load() {
		c.mu.Unlock()
		c.writeMu.Unlock()
		return
	}
	if !clientEnded {
		c.noteServerResetLocked(streamID)
	}
	enc := c.encoderLocked()
	maxFrame := int(c.peerMaxFrameSize)
	canSendBody := int64(len(body)) <= c.sendWindow && int64(len(body)) <= int64(c.peerInitialWindowSize) && len(body) <= maxFrame
	if canSendBody {
		c.sendWindow -= int64(len(body))
	}
	c.mu.Unlock()

	contentLength := ""
	if canSendBody {
		contentLength = strconv.Itoa(len(body))
	}
	c.encodeResponseHeadersLocked(enc, code, nil, "text/plain; charset=utf-8", contentLength, currentDate())
	out := bytepool.Get(c.headerWriter.buf.Len() + len(body) + 32)
	appendHeaders(&out, streamID, c.headerWriter.buf.Bytes(), !canSendBody, maxFrame)
	if canSendBody {
		appendFrameHeader(&out, len(body), frameData, flagEndStream, streamID)
		out.WriteString(body)
	}
	if !clientEnded {
		appendRSTStream(&out, streamID, errCodeNo)
	}
	_, _ = c.connection.Write(out.Bytes())
	out.Release()
	c.releaseHeaderWriterLocked()
	c.writeMu.Unlock()
}

// writeHeaders encodes and sends an informational (1xx), response (status > 0) or trailer (status == 0) HEADERS block.
func (c *conn) writeHeaders(s *stream, status int, fields []headerPair, contentType, contentLength, date string, endStream, informational bool) error {
	c.writeMu.Lock()
	c.mu.Lock()
	if err := c.writeErrLocked(s); err != nil {
		c.mu.Unlock()
		c.writeMu.Unlock()
		return err
	}
	if informational && s.wroteHeaders {
		c.mu.Unlock()
		c.writeMu.Unlock()
		return nil
	}
	if !informational && status > 0 {
		s.wroteHeaders = true
		s.expectContinue = false
	}
	enc := c.encoderLocked()
	maxFrame := int(c.peerMaxFrameSize)
	needEarlyReset := false
	var cancel context.CancelFunc
	if endStream {
		needEarlyReset, cancel = c.endStreamLocalLocked(s)
	}
	c.mu.Unlock()

	c.encodeResponseHeadersLocked(enc, status, fields, contentType, contentLength, date)
	out := bytepool.Get(c.headerWriter.buf.Len() + 32)
	appendHeaders(&out, s.id, c.headerWriter.buf.Bytes(), endStream, maxFrame)
	if needEarlyReset {
		appendRSTStream(&out, s.id, errCodeNo)
	}
	_, err := c.connection.Write(out.Bytes())
	out.Release()
	c.releaseHeaderWriterLocked()
	c.writeMu.Unlock()

	if cancel != nil {
		cancel()
	}
	return err
}

// writeHeadersAndBody sends response headers and a buffered body together in one Write / Writev when the connection
// and stream send windows allow, falling back to writeHeaders + writeData when backpressured or larger than a frame.
func (c *conn) writeHeadersAndBody(s *stream, status int, fields []headerPair, contentType, contentLength, date string, body []byte) error {
	c.writeMu.Lock()
	c.mu.Lock()
	if err := c.writeErrLocked(s); err != nil {
		c.mu.Unlock()
		c.writeMu.Unlock()
		return err
	}
	s.wroteHeaders = true
	s.expectContinue = false
	enc := c.encoderLocked()
	maxFrame := int(c.peerMaxFrameSize)
	canCombine := len(body) <= maxFrame && c.connection.Writable() && int64(len(body)) <= c.sendWindow && int64(len(body)) <= s.sendWindow
	needEarlyReset := false
	var cancel context.CancelFunc
	if canCombine {
		c.sendWindow -= int64(len(body))
		s.sendWindow -= int64(len(body))
		needEarlyReset, cancel = c.endStreamLocalLocked(s)
	}
	c.mu.Unlock()

	c.encodeResponseHeadersLocked(enc, status, fields, contentType, contentLength, date)
	if !canCombine {
		out := bytepool.Get(c.headerWriter.buf.Len() + 16)
		appendHeaders(&out, s.id, c.headerWriter.buf.Bytes(), false, maxFrame)
		_, err := c.connection.Write(out.Bytes())
		out.Release()
		c.releaseHeaderWriterLocked()
		c.writeMu.Unlock()
		if err != nil {
			return err
		}
		return c.writeData(s, body, true)
	}

	var err error
	if len(body) <= maxCopiedBody || needEarlyReset {
		out := bytepool.Get(c.headerWriter.buf.Len() + len(body) + 32)
		appendHeaders(&out, s.id, c.headerWriter.buf.Bytes(), false, maxFrame)
		appendFrameHeader(&out, len(body), frameData, flagEndStream, s.id)
		out.Write(body)
		if needEarlyReset {
			appendRSTStream(&out, s.id, errCodeNo)
		}
		_, err = c.connection.Write(out.Bytes())
		out.Release()
	} else {
		out := bytepool.Get(c.headerWriter.buf.Len() + 32)
		appendHeaders(&out, s.id, c.headerWriter.buf.Bytes(), false, maxFrame)
		appendFrameHeader(&out, len(body), frameData, flagEndStream, s.id)
		segments := [2][]byte{out.Bytes(), body}
		_, err = c.connection.Writev(segments[:])
		out.Release()
	}
	c.releaseHeaderWriterLocked()
	c.writeMu.Unlock()

	if cancel != nil {
		cancel()
	}
	return err
}

func (c *conn) encodeResponseHeadersLocked(enc *hpack.Encoder, status int, fields []headerPair, contentType, contentLength, date string) {
	c.headerWriter.buf.Reset()
	if status > 0 {
		_ = enc.WriteField(hpack.HeaderField{Name: ":status", Value: statusCodeString(status)})
	}
	for _, f := range fields {
		_ = enc.WriteField(hpack.HeaderField{Name: f.name, Value: f.value})
	}
	if contentType != "" {
		_ = enc.WriteField(hpack.HeaderField{Name: "content-type", Value: contentType})
	}
	if contentLength != "" {
		_ = enc.WriteField(hpack.HeaderField{Name: "content-length", Value: contentLength})
	}
	if date != "" {
		_ = enc.WriteField(hpack.HeaderField{Name: "date", Value: date})
	}
}

func (c *conn) writeData(s *stream, data []byte, endStream bool) error {
	if len(data) == 0 {
		if !endStream {
			return nil
		}
		c.writeMu.Lock()
		c.mu.Lock()
		if err := c.writeErrLocked(s); err != nil {
			c.mu.Unlock()
			c.writeMu.Unlock()
			return err
		}
		needEarlyReset, cancel := c.endStreamLocalLocked(s)
		c.mu.Unlock()

		var out bytepool.Buffer
		appendFrameHeader(&out, 0, frameData, flagEndStream, s.id)
		if needEarlyReset {
			appendRSTStream(&out, s.id, errCodeNo)
		}
		_, err := c.connection.Write(out.Bytes())
		out.Release()
		c.writeMu.Unlock()
		if cancel != nil {
			cancel()
		}
		return err
	}

	var timer *time.Timer
	defer func() {
		if timer != nil {
			timer.Stop()
		}
	}()

	for len(data) > 0 {
		c.writeMu.Lock()
		c.mu.Lock()
		if err := c.writeErrLocked(s); err != nil {
			c.unregisterWaiterLocked(s)
			c.mu.Unlock()
			c.writeMu.Unlock()
			return err
		}
		availWindow := min(c.sendWindow, s.sendWindow)
		if !c.connection.Writable() || availWindow <= 0 {
			if s.writeReady == nil {
				s.writeReady = make(chan struct{}, 1)
			}
			select {
			case <-s.writeReady:
			default:
			}
			if (!c.connection.Writable() || c.sendWindow <= 0) && !s.waitingConn {
				s.waitingConn = true
				c.waitingStreams = append(c.waitingStreams, s)
			}
			ready := s.writeReady
			// Check Writable once more after registering: OnWritable may have raced just before mu was acquired.
			if c.connection.Writable() && availWindow > 0 {
				c.unregisterWaiterLocked(s)
				c.mu.Unlock()
				c.writeMu.Unlock()
				continue
			}
			c.mu.Unlock()
			c.writeMu.Unlock()

			var expired <-chan time.Time
			if timeout := c.srv.opts.IdleTimeout; timeout > 0 {
				if timer == nil {
					timer = time.NewTimer(timeout)
				} else {
					timer.Reset(timeout)
				}
				expired = timer.C
			}
			select {
			case <-ready:
			case <-expired:
				return os.ErrDeadlineExceeded
			}
			continue
		}

		c.unregisterWaiterLocked(s)
		take := int(min(int64(len(data)), int64(c.peerMaxFrameSize), availWindow))
		c.sendWindow -= int64(take)
		s.sendWindow -= int64(take)
		lastChunk := endStream && take == len(data)
		needEarlyReset := false
		var cancel context.CancelFunc
		if lastChunk {
			needEarlyReset, cancel = c.endStreamLocalLocked(s)
		}
		c.mu.Unlock()

		flags := uint8(0)
		if lastChunk {
			flags = flagEndStream
		}
		var err error
		if needEarlyReset {
			out := bytepool.Get(frameHeaderLen + take + frameHeaderLen + 4)
			appendFrameHeader(&out, take, frameData, flags, s.id)
			out.Write(data[:take])
			appendRSTStream(&out, s.id, errCodeNo)
			_, err = c.connection.Write(out.Bytes())
			out.Release()
		} else {
			var hdr [frameHeaderLen]byte
			putFrameHeader(hdr[:], take, frameData, flags, s.id)
			segments := [2][]byte{hdr[:], data[:take]}
			_, err = c.connection.Writev(segments[:])
		}
		c.writeMu.Unlock()

		if cancel != nil {
			cancel()
		}
		if err != nil {
			return err
		}
		data = data[take:]
	}
	return nil
}

// endStreamLocalLocked records that the server is sending END_STREAM for s. When the client has not finished sending
// its request yet (streamOpen), RFC 9113 8.1 lets the server follow END_STREAM with RST_STREAM(NO_ERROR) and close the
// stream immediately; caller holds mu.
func (c *conn) endStreamLocalLocked(s *stream) (needEarlyReset bool, cancel context.CancelFunc) {
	s.sentEndStream = true
	if s.state == streamOpen {
		s.sentReset = true
		c.noteServerResetLocked(s.id)
		return true, c.removeStreamLocked(s, errBodyClosedByHandler, nil)
	}
	c.resetTokens = min(c.resetTokens+1, maxResetBurst)
	return false, c.removeStreamLocked(s, nil, nil)
}

func (c *conn) resetStream(s *stream, code errCode) {
	c.writeMu.Lock()
	c.mu.Lock()
	alreadyDone := c.closed.Load() || s.sentReset
	s.sentReset = true
	c.noteServerResetLocked(s.id)
	cancel := c.removeStreamLocked(s, errStreamReset, errStreamReset)
	connInc := c.takeConnWindowUpdateLocked()
	c.mu.Unlock()

	if !alreadyDone || connInc > 0 {
		var out bytepool.Buffer
		if connInc > 0 {
			appendWindowUpdate(&out, 0, connInc)
		}
		if !alreadyDone {
			appendRSTStream(&out, s.id, code)
		}
		_, _ = c.connection.Write(out.Bytes())
		out.Release()
	}
	c.writeMu.Unlock()

	if cancel != nil {
		cancel()
	}
}

func (c *conn) sendGoAway(closeWhenIdle bool) {
	c.writeMu.Lock()
	c.mu.Lock()
	if closeWhenIdle {
		c.closeWhenIdle = true
	}
	if c.goingAway || c.closed.Load() {
		c.mu.Unlock()
		c.writeMu.Unlock()
		return
	}
	c.goingAway = true
	lastID := c.maxClientStreamID
	c.setDeadlineLocked()
	c.mu.Unlock()

	var out bytepool.Buffer
	appendGoAway(&out, lastID, errCodeNo)
	_, _ = c.connection.Write(out.Bytes())
	out.Release()
	c.writeMu.Unlock()
}

func (c *conn) sendWindowUpdates(streamID, connInc, streamInc uint32) {
	var out bytepool.Buffer
	if connInc > 0 {
		appendWindowUpdate(&out, 0, connInc)
	}
	if streamInc > 0 {
		appendWindowUpdate(&out, streamID, streamInc)
	}
	c.writeMu.Lock()
	if !c.closed.Load() {
		_, _ = c.connection.Write(out.Bytes())
	}
	c.writeMu.Unlock()
	out.Release()
}

func (c *conn) streamWriteError(s *stream) error {
	c.mu.Lock()
	err := c.writeErrLocked(s)
	c.mu.Unlock()
	return err
}

func (c *conn) writeErrLocked(s *stream) error {
	if c.closed.Load() {
		return net.ErrClosed
	}
	if s.writeErr != nil {
		return s.writeErr
	}
	if s.state == streamClosed || s.sentEndStream || s.sentReset {
		return errStreamReset
	}
	return nil
}
