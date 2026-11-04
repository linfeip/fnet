package fh2

import (
	"context"
	"errors"
	"io"
	"net/http"
	"sync/atomic"
	"time"

	"github.com/linfeip/fnet/internal/bytepool"
)

var (
	errStreamReset           = errors.New("fh2: stream reset")
	errClientDisconnected    = errors.New("fh2: client disconnected")
	errBodyClosedByHandler   = errors.New("fh2: request body closed by handler")
	errContentLengthMismatch = errors.New("fh2: request body size does not match Content-Length")
)

type streamState uint8

const (
	streamOpen streamState = iota
	streamHalfClosedRemote
	streamHalfClosedLocal
	streamClosed
)

type streamContext struct {
	context.Context
	cancel context.CancelFunc
}

// stream holds the per-stream state of one HTTP/2 request/response exchange. Most fields are guarded by conn.mu so
// connection-level and stream-level flow control update atomically under one lock.
type stream struct {
	conn           *conn
	req            http.Request
	body           requestBody
	id             uint32
	sendWindow     int64
	receiveWindow  int32
	unackedBytes   int32
	receivedBytes  int64
	declaredLength int64
	bodyBuf        bytepool.Buffer
	bodyPos        int
	readReady      chan struct{}
	writeReady     chan struct{}
	readErr        error
	writeErr       error
	ctx            atomic.Pointer[streamContext]
	state          streamState
	eof            bool
	gotTrailers    bool
	bodyClosed     bool
	expectContinue bool
	wroteHeaders   bool
	sentEndStream  bool
	sentReset      bool
	waitingConn    bool
}

func (s *stream) Deadline() (time.Time, bool) { return time.Time{}, false }

func (s *stream) Done() <-chan struct{} { return s.context().Done() }

func (s *stream) Err() error {
	if sc := s.ctx.Load(); sc != nil {
		return sc.Err()
	}
	if s.conn.closed.Load() {
		return context.Canceled
	}
	s.conn.mu.Lock()
	closed := s.state == streamClosed
	s.conn.mu.Unlock()
	if closed {
		return context.Canceled
	}
	return nil
}

func (s *stream) Value(key any) any {
	switch key {
	case http.ServerContextKey:
		return s.conn.srv.httpServer
	case http.LocalAddrContextKey:
		return s.conn.connection.LocalAddr()
	}
	return s.context().Value(key)
}

func (s *stream) context() context.Context {
	if sc := s.ctx.Load(); sc != nil {
		return sc.Context
	}
	base := s.conn.baseContext()
	ctx, cancel := context.WithCancel(base)
	c := s.conn
	c.mu.Lock()
	var toCancel context.CancelFunc
	if s.ctx.Load() == nil {
		s.ctx.Store(&streamContext{Context: ctx, cancel: cancel})
		if s.state == streamClosed || c.closed.Load() {
			toCancel = cancel
		}
	} else {
		toCancel = cancel
	}
	c.mu.Unlock()
	if toCancel != nil {
		toCancel()
	}
	return s.ctx.Load().Context
}

// cancelFunc returns the stream's cancel function if a context was initialized; caller holds conn.mu.
func (s *stream) cancelFunc() context.CancelFunc {
	if sc := s.ctx.Load(); sc != nil {
		return sc.cancel
	}
	return nil
}

// appendDataLocked appends inbound DATA payload to bodyBuf, compacting consumed prefix when beneficial; caller holds
// conn.mu.
func (s *stream) appendDataLocked(data []byte) {
	if s.bodyPos > 0 && cap(s.bodyBuf.Bytes())-s.bodyBuf.Len() < len(data) && s.bodyPos >= s.bodyBuf.Len()-s.bodyPos {
		s.bodyBuf.Discard(s.bodyPos)
		s.bodyPos = 0
	}
	s.bodyBuf.Write(data)
}

// discardUnreadBodyLocked releases bodyBuf and credits any unread bytes back to the connection's unackedBytes so the
// connection-level receive window never leaks when a stream closes or resets early; caller holds conn.mu.
func (s *stream) discardUnreadBodyLocked() {
	if unread := s.bodyBuf.Len() - s.bodyPos; unread > 0 {
		s.conn.unackedReceiveBytes += int32(unread)
	}
	s.bodyBuf.Release()
	s.bodyPos = 0
}

// wakeReaderLocked signals a Handler blocked in requestBody.Read; caller holds conn.mu.
func (s *stream) wakeReaderLocked() {
	select {
	case s.readReady <- struct{}{}:
	default:
	}
}

// wakeWriterLocked signals a Handler blocked waiting for send window or Conn.Writable; caller holds conn.mu.
func (s *stream) wakeWriterLocked() {
	select {
	case s.writeReady <- struct{}{}:
	default:
	}
}

// requestBody implements io.ReadCloser for an HTTP/2 stream's inbound DATA frames.
type requestBody struct {
	stream *stream
}

func (b *requestBody) Read(p []byte) (int, error) {
	s := b.stream
	c := s.conn

	c.mu.Lock()
	sendContinue := s.expectContinue && !s.wroteHeaders && !s.eof && !s.bodyClosed && s.state != streamClosed
	if sendContinue {
		s.expectContinue = false
	}
	c.mu.Unlock()

	if sendContinue {
		_ = c.writeHeaders(s, http.StatusContinue, nil, "", "", "", false, true)
	}

	if len(p) == 0 {
		return 0, nil
	}

	for {
		c.mu.Lock()
		if s.bodyClosed {
			c.mu.Unlock()
			return 0, errBodyClosedByHandler
		}
		if avail := s.bodyBuf.Bytes()[s.bodyPos:]; len(avail) > 0 {
			n := copy(p, avail)
			s.bodyPos += n
			if s.bodyPos == s.bodyBuf.Len() {
				s.bodyBuf.Release()
				s.bodyPos = 0
			}
			connInc, streamInc := c.consumeReceiveBytesLocked(s, int32(n))
			done := s.bodyPos == s.bodyBuf.Len() && s.eof && s.readErr == nil
			c.mu.Unlock()
			if connInc > 0 || streamInc > 0 {
				c.sendWindowUpdates(s.id, connInc, streamInc)
			}
			if done {
				return n, io.EOF
			}
			return n, nil
		}
		if s.readErr != nil {
			err := s.readErr
			c.mu.Unlock()
			return 0, err
		}
		if s.eof {
			c.mu.Unlock()
			return 0, io.EOF
		}
		if s.readReady == nil {
			s.readReady = make(chan struct{}, 1)
		}
		ready := s.readReady
		c.mu.Unlock()

		<-ready
	}
}

func (b *requestBody) Close() error {
	s := b.stream
	c := s.conn
	c.mu.Lock()
	if s.bodyClosed {
		c.mu.Unlock()
		return nil
	}
	s.bodyClosed = true
	s.expectContinue = false
	s.discardUnreadBodyLocked()
	connInc := c.takeConnWindowUpdateLocked()
	s.wakeReaderLocked()
	c.mu.Unlock()
	if connInc > 0 {
		c.sendWindowUpdates(0, connInc, 0)
	}
	return nil
}
