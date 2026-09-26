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

	// Event-loop state.
	asm  *assembly // a message arriving in pieces; nil between messages
	dead bool      // closing: ignore further input

	// Shared with the worker.
	mu       sync.Mutex
	queue    []message // parsed, waiting for the worker; nil while idle
	closeErr error
	pending  int64
	running  bool
	failed   bool // a message broke the protocol: drop the rest
	closed   bool // the connection is gone: OnClose is due
	notified bool
	paused   bool
}

// assembly is a data message the loop is still putting together: a frame whose
// payload spans reads, or a fragmented message. A connection has one only
// meanwhile, so between messages it pays a pointer for it.
type assembly struct {
	msg        *bufpool.Buffer // the payload so far; nil while empty
	msgLen     int
	part       ws.Header // the frame being read
	partGot    int       // how much of part's payload has arrived
	op         OpCode
	deflated   bool
	fragmented bool // the message goes on in further frames
	streaming  bool // part's payload is still arriving
}

var (
	errHandlerPanic = errors.New("fnet/websocket: OnMessage panicked")
	errPoolRejected = errors.New("fnet/websocket: the worker pool rejected the connection")
)

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
	if e.streaming() {
		if off, msgs = e.fill(data, msgs); e.streaming() {
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
			e.asm = &assembly{op: h.OpCode, deflated: e.c.compressed && h.Rsv1()}
		} // else check saw the fragmented message the frame continues
		e.asm.part, e.asm.partGot, e.asm.streaming = h, 0, true
		var n int
		n, msgs = e.fill(data[start:], msgs)
		if off = start + n; e.streaming() {
			return off, msgs
		}
	}
	return off, msgs
}

// streaming reports whether the payload of a frame is still arriving.
func (e *eventConn) streaming() bool { return e.asm != nil && e.asm.streaming }

func (e *eventConn) check(h ws.Header) *protocolError {
	fragmented := e.asm != nil && e.asm.fragmented
	if perr := checkHeader(h, e.c.compressed, fragmented); perr != nil || h.OpCode.IsControl() {
		return perr
	}
	have := 0
	if h.OpCode == OpContinuation {
		have = e.asm.msgLen
	}
	return checkSize(h, have, e.c.limits.maxMessageSize)
}

// fill copies the next bytes of the current frame's payload into the message
// buffer, unmasking as it goes, and appends the message to msgs once it is
// complete. The buffer grows with what actually arrived, never with what a
// header merely claims.
func (e *eventConn) fill(data []byte, msgs []message) (int, []message) {
	asm := e.asm
	n := min(len(data), int(asm.part.Length)-asm.partGot)
	if n > 0 {
		if need := asm.msgLen + n; asm.msg == nil || need > cap(asm.msg.B) {
			nb := bufpool.Get(need)
			if asm.msg != nil {
				copy(nb.B, asm.msg.B[:asm.msgLen])
				bufpool.Put(asm.msg)
			}
			asm.msg = nb
		}
		dst := asm.msg.B[asm.msgLen : asm.msgLen+n : cap(asm.msg.B)]
		copy(dst, data[:n])
		ws.Cipher(dst, asm.part.Mask, asm.partGot)
		asm.msgLen += n
		asm.partGot += n
	}
	if asm.partGot == int(asm.part.Length) {
		asm.streaming = false
		if asm.fragmented = !asm.part.Fin; !asm.fragmented {
			m := message{op: asm.op, deflated: asm.deflated, buf: asm.msg}
			if asm.msg != nil {
				m.data = asm.msg.B[:asm.msgLen]
			}
			msgs = append(msgs, m)
			e.asm = nil
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
	if limit := e.c.limits.maxPending; limit > 0 && e.pending > limit && !e.paused {
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
	if e.asm != nil {
		bufpool.Put(e.asm.msg)
		e.asm = nil
	}
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
	if pool.DispatchTask(e.submit, uint64(e.c.raw.Fd()), e) {
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
	go e.Run()
}

// Run drains the queue on a worker (eventConn is its own pool.Task); at most
// one Run is active per connection.
// A handled batch is settled under the same lock that takes the next one: its
// bytes leave pending, and its emptied array takes the messages that arrive
// while the next batch is handled. An idle connection keeps no array.
func (e *eventConn) Run() {
	var (
		spare   []message // the array of the batch just handled, emptied
		handled int64     // that batch's payload bytes
	)
	for {
		e.mu.Lock()
		e.pending -= handled
		if e.paused && e.pending <= e.c.limits.lowPending {
			e.paused = false
			e.c.raw.ResumeRead()
		}
		if len(e.queue) == 0 {
			e.running = false
			e.queue = nil
			notify := e.closed && !e.notified
			e.notified = e.notified || notify
			err := e.closeErr
			e.mu.Unlock()
			if notify {
				e.callClose(err)
			}
			return
		}
		batch := e.queue
		e.queue = spare
		failed := e.failed
		e.mu.Unlock()
		handled = e.handle(batch, failed)
		spare = batch[:0]
	}
}

// handle delivers a batch in order, returns its buffers to the pool, and
// reports its payload bytes. The replies to a burst leave in one write; a slow
// handler holds them back for a millisecond at most (see Corked).
func (e *eventConn) handle(batch []message, failed bool) (size int64) {
	run := func() {
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
		e.c.raw.Corked(run)
	} else {
		run()
	}
	return size
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
	e.callClose(err)
}

// callClose runs OnClose. A panic in it is logged, like one in OnMessage, and
// goes no further: the connection is gone already, and the goroutine may be
// one of its own (serveBlocking), where nothing else would recover it.
func (e *eventConn) callClose(err error) {
	if e.handler.OnClose == nil {
		return
	}
	defer func() {
		if r := recover(); r != nil {
			log.Printf("fnet/websocket: panic in OnClose for %s: %v\n%s", e.c.RemoteAddr(), r, debug.Stack())
		}
	}()
	e.handler.OnClose(e.c, err)
}
