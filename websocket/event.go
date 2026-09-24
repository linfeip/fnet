package websocket

import (
	"errors"
	"sync"
	"time"

	"github.com/gobwas/ws"

	"github.com/linfeip/fnet"
	"github.com/linfeip/fnet/internal/bufpool"
	"github.com/linfeip/fnet/internal/reactor"
)

// message is one complete, unmasked data message waiting for a worker.
type message struct {
	op       OpCode
	deflated bool
	buf      *bufpool.Buffer // owns the payload bytes, nil when empty
	n        int
}

func (m *message) payload() []byte {
	if m.buf == nil {
		return nil
	}
	return m.buf.B[:m.n]
}

// eventConn drives an event-driven connection. OnData runs on the event loop:
// it parses frames, enforces RFC 6455, answers control frames, and copies each
// complete message into a pooled buffer. Messages run on the worker pool, one
// task at a time per connection, so OnMessage calls stay ordered and never
// stall the loop. A slow consumer pauses reading (backpressure) instead of
// buffering without bound.
type eventConn struct {
	c       *Conn
	handler EventHandler
	submit  func(connID uint64, task func())
	id      uint64

	maxPending int64 // queued bytes that pause reading; <= 0 disables
	lowPending int64 // ...and the level at which reading resumes

	// Event-loop state.
	dead       bool            // closing: ignore further input
	msg        *bufpool.Buffer // message being assembled
	msgLen     int
	msgOp      OpCode
	deflated   bool
	fragmented bool      // a fragmented message is open
	part       ws.Header // frame whose payload is still arriving
	partGot    int
	streaming  bool
	batch      []message // messages parsed from the current read

	// Shared with the worker.
	mu       sync.Mutex
	queue    []message
	spare    []message
	running  bool
	failed   bool // a message broke the protocol: drop the rest
	closed   bool // the connection is gone: OnClose is due
	notified bool
	closeErr error
	pending  int64
	paused   bool
}

var errHandlerPanic = errors.New("fnet/websocket: OnMessage panicked")

const (
	// idleQueueCap is the largest queue array an idle connection keeps.
	idleQueueCap = 8
	// maxBatchDelay bounds how long replies to a burst are held for coalescing.
	maxBatchDelay = time.Millisecond
)

// ---------------------------------------------------------------------------
// Event loop side (reactor.Handler)
// ---------------------------------------------------------------------------

func (e *eventConn) OnData(_ *reactor.Conn, data []byte) int {
	if e.dead {
		return len(data)
	}
	n := e.parse(data)
	e.flush()
	if e.dead {
		return len(data)
	}
	return n
}

// parse consumes complete frames, and the available part of a data frame whose
// header is complete. Only an incomplete header is left for the next read.
func (e *eventConn) parse(data []byte) int {
	off := 0
	if e.streaming {
		if off = e.fill(data); e.streaming {
			return off
		}
	}
	for off < len(data) {
		h, hn, ok, err := parseHeader(data[off:])
		if err != nil {
			e.fail(headerError(err))
			return len(data)
		}
		if !ok {
			break
		}
		if perr := e.check(h); perr != nil {
			e.fail(perr)
			return len(data)
		}
		start := off + hn
		end := start + int(h.Length)
		if h.OpCode.IsControl() {
			if end > len(data) {
				break // control payloads are tiny: wait for the rest
			}
			off = end
			if !e.control(h, data[start:end]) {
				return len(data)
			}
			continue
		}
		if h.OpCode != OpContinuation {
			e.msgOp, e.deflated, e.msgLen = h.OpCode, e.c.compressed && h.Rsv1(), 0
		}
		e.part, e.partGot, e.streaming = h, 0, true
		off = start + e.fill(data[start:])
		if e.streaming {
			return off
		}
	}
	return off
}

func (e *eventConn) check(h ws.Header) *protocolError {
	if perr := checkHeader(h, e.c.compressed, e.fragmented); perr != nil || h.OpCode.IsControl() {
		return perr
	}
	have := 0
	if h.OpCode == OpContinuation {
		have = e.msgLen
	}
	return checkSize(h, have, e.c.maxMessageSize)
}

// fill copies the next bytes of the current frame's payload into the message
// buffer, unmasking as it goes. The buffer grows with what actually arrived,
// never with what a header merely claims.
func (e *eventConn) fill(data []byte) int {
	n := min(len(data), int(e.part.Length)-e.partGot)
	if n > 0 {
		if need := e.msgLen + n; e.msg == nil || need > cap(e.msg.B) {
			nb := bufpool.Get(need)
			if e.msg != nil {
				copy(nb.B, e.msg.B[:e.msgLen])
				bufpool.Put(e.msg)
			}
			e.msg = nb
		}
		dst := e.msg.B[e.msgLen : e.msgLen+n : cap(e.msg.B)]
		copy(dst, data[:n])
		ws.Cipher(dst, e.part.Mask, e.partGot)
		e.msgLen += n
		e.partGot += n
	}
	if e.partGot == int(e.part.Length) {
		e.streaming = false
		if e.fragmented = !e.part.Fin; !e.fragmented {
			e.batch = append(e.batch, message{op: e.msgOp, deflated: e.deflated, buf: e.msg, n: e.msgLen})
			e.msg, e.msgLen = nil, 0
		}
	}
	return n
}

// control answers a Ping or Close frame on the loop. It reports whether the
// connection is still open.
func (e *eventConn) control(h ws.Header, payload []byte) bool {
	ws.Cipher(payload, h.Mask, 0)
	switch h.OpCode {
	case OpPing:
		_ = e.c.writeControl(OpPong, payload)
	case OpClose:
		echo, perr := checkClosePayload(payload)
		if perr != nil {
			e.fail(perr)
			return false
		}
		e.dead = true
		_ = e.c.closeWith(echo)
		return false
	}
	return true
}

func (e *eventConn) fail(perr *protocolError) {
	e.dead = true
	_ = e.c.fail(perr)
}

// flush hands the messages parsed from one read to the worker in one step.
func (e *eventConn) flush() {
	if len(e.batch) == 0 {
		return
	}
	var size int64
	for i := range e.batch {
		size += int64(e.batch[i].n)
	}
	e.mu.Lock()
	if e.closed || e.failed {
		e.mu.Unlock()
		releaseAll(e.batch)
		e.batch = e.batch[:0]
		return
	}
	e.queue = append(e.queue, e.batch...)
	e.pending += size
	pause := e.maxPending > 0 && e.pending > e.maxPending && !e.paused
	if pause {
		e.paused = true
	}
	start := !e.running
	e.running = true
	e.mu.Unlock()
	clear(e.batch)
	e.batch = e.batch[:0]
	if cap(e.batch) > idleQueueCap {
		e.batch = nil
	}
	if pause {
		e.c.raw.PauseRead()
	}
	if start {
		e.schedule()
	}
}

// OnClose runs on the event loop once the connection is torn down. The user's
// OnClose follows on the worker, after messages already queued.
func (e *eventConn) OnClose(_ *reactor.Conn, err error) {
	e.dead = true
	e.c.closed.Store(true)
	bufpool.Put(e.msg)
	e.msg, e.streaming = nil, false
	e.mu.Lock()
	e.closed = true
	if e.closeErr == nil {
		e.closeErr = err
	}
	start := !e.running
	e.running = true
	e.mu.Unlock()
	if start {
		e.schedule()
	}
}

// ---------------------------------------------------------------------------
// Worker side
// ---------------------------------------------------------------------------

func (e *eventConn) schedule() {
	if e.submit == nil {
		fnet.DefaultWorkerPool.SubmitConn(e.id, e.run)
		return
	}
	defer func() {
		if recover() != nil { // the custom pool rejected the task
			_ = e.c.nc.Close()
			e.mu.Lock()
			e.running = false
			e.mu.Unlock()
		}
	}()
	e.submit(e.id, e.run)
}

// run drains the queue on a worker; at most one run is active per connection.
func (e *eventConn) run() {
	for {
		e.mu.Lock()
		if len(e.queue) == 0 {
			e.running = false
			e.spare = nil // an idle connection keeps no burst-sized arrays
			if cap(e.queue) > idleQueueCap {
				e.queue = nil
			}
			notify := e.closed && !e.notified
			e.notified = e.notified || notify
			err := e.closeErr
			e.mu.Unlock()
			if notify && e.handler.OnClose != nil {
				e.handler.OnClose(e.c, err)
			}
			return
		}
		batch := e.queue
		e.queue, e.spare = e.spare[:0], batch
		failed := e.failed
		e.mu.Unlock()

		// Replies to a burst leave in one write, but a slow handler must not
		// hold earlier replies back for more than maxBatchDelay.
		coalesce := len(batch) > 1
		var since time.Time
		if coalesce {
			e.c.BeginBatch()
			since = time.Now()
		}
		var size int64
		for i := range batch {
			size += int64(batch[i].n)
			if !failed {
				if err := e.deliver(&batch[i]); err != nil {
					failed = true
					e.mu.Lock()
					e.failed, e.closeErr = true, err
					e.mu.Unlock()
				}
			}
			bufpool.Put(batch[i].buf)
			batch[i] = message{}
			if coalesce && time.Since(since) > maxBatchDelay {
				_ = e.c.EndBatch()
				e.c.BeginBatch()
				since = time.Now()
			}
		}
		if coalesce {
			_ = e.c.EndBatch()
		}

		e.mu.Lock()
		e.pending -= size
		resume := e.paused && e.pending <= e.lowPending
		if resume {
			e.paused = false
		}
		e.mu.Unlock()
		if resume {
			e.c.raw.ResumeRead()
		}
	}
}

// deliver decodes one message and calls OnMessage. A decode failure or a
// handler panic closes the connection and is returned.
func (e *eventConn) deliver(m *message) (err error) {
	payload, err := e.c.decode(m.op, m.deflated, m.payload())
	if err != nil {
		return err
	}
	if e.handler.OnMessage == nil {
		return nil
	}
	defer func() {
		if recover() != nil {
			_ = e.c.CloseWithStatus(ws.StatusInternalServerError, "")
			err = errHandlerPanic
		}
	}()
	e.handler.OnMessage(e.c, m.op, payload)
	return nil
}

func releaseAll(ms []message) {
	for i := range ms {
		bufpool.Put(ms[i].buf)
		ms[i] = message{}
	}
}

// serveBlocking delivers the same callbacks from a dedicated goroutine for a
// connection the event loop cannot drive (TLS, or a foreign http.Server).
func (e *eventConn) serveBlocking() {
	err := e.c.Handle(func(op OpCode, msg []byte) (err error) {
		defer func() {
			if recover() != nil {
				_ = e.c.CloseWithStatus(ws.StatusInternalServerError, "")
				err = errHandlerPanic
			}
		}()
		if e.handler.OnMessage != nil {
			e.handler.OnMessage(e.c, op, msg)
		}
		return nil
	})
	_ = e.c.Close()
	if e.handler.OnClose != nil {
		e.handler.OnClose(e.c, err)
	}
}
