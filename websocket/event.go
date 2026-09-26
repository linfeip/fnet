package websocket

import (
	"errors"
	"log"
	"runtime/debug"
	"sync"

	"github.com/gobwas/ws"

	"github.com/linfeip/fnet/internal/bufpool"
	"github.com/linfeip/fnet/internal/reactor"
	"github.com/linfeip/fnet/pool"
)

// message is one complete, unmasked data message waiting for a worker.
type message struct {
	op       OpCode
	deflated bool
	data     []byte          // the payload
	buf      *bufpool.Buffer // holds data; nil when empty
}

// batchSize messages parsed from one read fit on the loop's stack: a whole read
// (64 KiB) of 1 KiB messages. A longer run of smaller ones spills to the heap.
const batchSize = 64

// eventConn drives an event-driven connection. OnData runs on the event loop:
// it parses frames, enforces RFC 6455, answers control frames, and copies each
// complete message into a pooled buffer, which the messages that arrived whole
// in the same read share. Messages run on the worker pool, one
// task at a time per connection, so OnMessage calls stay ordered and never
// stall the loop. A slow consumer pauses reading (backpressure) instead of
// buffering without bound.
type eventConn struct {
	c       *Conn
	handler EventHandler
	submit  func(connID uint64, task func()) error

	maxPending int64 // queued bytes that pause reading; <= 0 disables
	lowPending int64 // ...and the level at which reading resumes

	// Event-loop state.
	msg        *bufpool.Buffer // message being assembled across reads or frames
	msgLen     int
	part       ws.Header // frame whose payload is still arriving
	partGot    int
	dead       bool // closing: ignore further input
	msgOp      OpCode
	deflated   bool
	fragmented bool // a fragmented message is open
	streaming  bool

	// Shared with the worker.
	mu       sync.Mutex
	queue    []message
	spare    []message
	closeErr error
	pending  int64
	running  bool
	failed   bool // a message broke the protocol: drop the rest
	closed   bool // the connection is gone: OnClose is due
	notified bool
	paused   bool
}

var (
	errHandlerPanic = errors.New("fnet/websocket: OnMessage panicked")
	errPoolRejected = errors.New("fnet/websocket: the worker pool rejected the connection")
)

// idleQueueCap is the largest queue array an idle connection keeps.
const idleQueueCap = 8

// ---------------------------------------------------------------------------
// Event loop side (reactor.Handler)
// ---------------------------------------------------------------------------

func (e *eventConn) OnData(_ *reactor.Conn, data []byte) int {
	if e.dead {
		return len(data)
	}
	// The messages completed in this read collect on the stack; the payloads
	// of those whole in it share the arena's buffers.
	var buf [batchSize]message
	var a bufpool.Arena
	n, msgs := e.parse(data, buf[:0], &a)
	a.Release()
	e.flush(msgs)
	if e.dead {
		return len(data)
	}
	return n
}

// parse consumes complete frames, and the available part of a data frame whose
// header is complete, appending the messages it completes to msgs. Only an
// incomplete header is left for the next read.
func (e *eventConn) parse(data []byte, msgs []message, a *bufpool.Arena) (int, []message) {
	off := 0
	if e.streaming {
		if off, msgs = e.fill(data, msgs); e.streaming {
			return off, msgs
		}
	}
	for off < len(data) {
		h, hn, ok, err := parseHeader(data[off:])
		if err != nil {
			e.fail(headerError(err))
			return len(data), msgs
		}
		if !ok {
			break
		}
		if perr := e.check(h); perr != nil {
			e.fail(perr)
			return len(data), msgs
		}
		start := off + hn
		end := start + int(h.Length)
		if h.OpCode.IsControl() {
			if end > len(data) {
				break // control payloads are tiny: wait for the rest
			}
			off = end
			var open bool
			if open, msgs = e.control(h, data[start:end], msgs, a); !open {
				return len(data), msgs
			}
			continue
		}
		if h.Fin && h.OpCode != OpContinuation && end <= len(data) {
			// A whole message in this read: its payload joins the arena.
			m := message{op: h.OpCode, deflated: e.c.compressed && h.Rsv1()}
			if h.Length > 0 {
				m.data, m.buf = a.Copy(data[start:end], len(data)-start)
				ws.Cipher(m.data, h.Mask, 0)
			}
			msgs = append(msgs, m)
			off = end
			continue
		}
		if h.OpCode != OpContinuation {
			e.msgOp, e.deflated, e.msgLen = h.OpCode, e.c.compressed && h.Rsv1(), 0
		}
		e.part, e.partGot, e.streaming = h, 0, true
		var n int
		n, msgs = e.fill(data[start:], msgs)
		if off = start + n; e.streaming {
			return off, msgs
		}
	}
	return off, msgs
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
// buffer, unmasking as it goes, and appends the message to msgs once it is
// complete. The buffer grows with what actually arrived, never with what a
// header merely claims.
func (e *eventConn) fill(data []byte, msgs []message) (int, []message) {
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
			m := message{op: e.msgOp, deflated: e.deflated, buf: e.msg}
			if e.msg != nil {
				m.data = e.msg.B[:e.msgLen]
			}
			msgs = append(msgs, m)
			e.msg, e.msgLen = nil, 0
		}
	}
	return n, msgs
}

// control answers a Ping or Close frame on the loop, and queues a Pong for
// OnPong. It reports whether the connection is still open.
func (e *eventConn) control(h ws.Header, payload []byte, msgs []message, a *bufpool.Arena) (bool, []message) {
	ws.Cipher(payload, h.Mask, 0)
	switch h.OpCode {
	case OpPing:
		_ = e.c.writeControl(OpPong, payload)
	case OpPong:
		if e.handler.OnPong != nil {
			// Delivered on the worker, in order with the messages.
			m := message{op: OpPong}
			if len(payload) > 0 {
				m.data, m.buf = a.Copy(payload, len(payload))
			}
			msgs = append(msgs, m)
		}
	case OpClose:
		echo, perr := checkClosePayload(payload)
		if perr != nil {
			e.fail(perr)
			return false, msgs
		}
		e.dead = true
		_ = e.c.closeWith(echo)
		return false, msgs
	}
	return true, msgs
}

func (e *eventConn) fail(perr *protocolError) {
	e.dead = true
	_ = e.c.fail(perr)
}

// flush hands the messages parsed from one read to the worker in one step.
func (e *eventConn) flush(msgs []message) {
	if len(msgs) == 0 {
		return
	}
	var size int64
	for i := range msgs {
		size += int64(len(msgs[i].data))
	}
	e.mu.Lock()
	if e.closed || e.failed {
		e.mu.Unlock()
		releaseAll(msgs)
		return
	}
	e.queue = append(e.queue, msgs...)
	e.pending += size
	if e.maxPending > 0 && e.pending > e.maxPending && !e.paused {
		// Under the lock, so a worker that drains the queue at once cannot
		// resume before the pause lands and leave the connection unread.
		e.paused = true
		e.c.raw.PauseRead()
	}
	start := !e.running
	e.running = true
	e.mu.Unlock()
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
	if pool.Dispatch(e.submit, uint64(e.c.raw.Fd()), e.run) {
		return
	}
	// A custom pool refused the task: shed the connection and the messages it
	// sent, but still deliver the OnClose it is owed, on a goroutine of its own.
	e.mu.Lock()
	e.failed = true
	if e.closeErr == nil {
		e.closeErr = errPoolRejected
	}
	e.mu.Unlock()
	_ = e.c.nc.Close()
	go e.run()
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

		var size int64
		handle := func() {
			for i := range batch {
				size += int64(len(batch[i].data))
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
			}
		}
		if len(batch) > 1 {
			// The replies to a burst leave in one write; a slow handler
			// holds them back for a millisecond at most (see Corked).
			e.c.raw.Corked(handle)
		} else {
			handle()
		}

		e.mu.Lock()
		e.pending -= size
		if e.paused && e.pending <= e.lowPending {
			e.paused = false
			e.c.raw.ResumeRead()
		}
		e.mu.Unlock()
	}
}

// deliver decodes one message and calls OnMessage. A decode failure or a
// handler panic closes the connection and is returned.
func (e *eventConn) deliver(m *message) (err error) {
	defer func() {
		if r := recover(); r != nil {
			log.Printf("fnet/websocket: panic in a callback for %s: %v\n%s", e.c.RemoteAddr(), r, debug.Stack())
			_ = e.c.CloseWithStatus(ws.StatusInternalServerError, "")
			err = errHandlerPanic
		}
	}()
	if m.op == OpPong {
		e.handler.OnPong(e.c, m.data)
		return nil
	}
	payload, err := e.c.decode(m.op, m.deflated, m.data)
	if err != nil {
		return err
	}
	if e.handler.OnMessage != nil {
		e.handler.OnMessage(e.c, m.op, payload)
	}
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
			if r := recover(); r != nil {
				log.Printf("fnet/websocket: panic in OnMessage for %s: %v\n%s", e.c.RemoteAddr(), r, debug.Stack())
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
