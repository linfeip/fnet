package fhttp

import (
	"bytes"
	"net/http"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/linfeip/fnet"
	"github.com/linfeip/fnet/internal/bytepool"
	"github.com/linfeip/fnet/internal/units"
	"github.com/linfeip/fnet/taskpool"
)

// maxPipeline and maxQueuedBytes bound the requests queued for processing on a connection, by number and by size: once
// either is reached the connection is not read until the worker has answered them all (see conn.serve), so a client
// that pipelines deeply, or sends requests faster than the Handlers answer them, is slowed down by TCP flow control
// instead of being held in memory. The requests of the data at hand are queued in full first, so the queue may go past
// them by what one read brings (see conn.onData).
const (
	maxPipeline    = 128
	maxQueuedBytes = 64 * units.KB
)

// maxRetainedQueueCapacity is the maximum capacity retained once the request queue has been drained: a connection
// pipelining a few requests at a time (16 is a common depth) needs no reallocation every time, a connection that sends
// them one by one keeps only one item, and the large capacity left behind by a pipelining burst is released, so a
// large number of connections do not hold on to memory for long.
const maxRetainedQueueCapacity = 16

// request is one item in the request queue: either a complete request message, or a status code to reply with
// (100 Continue, or an error that closes the connection), or statusHandoff or statusClose.
type request struct {
	msg     bytepool.Buffer
	header  int32 // the length of the message's request line and headers (see maxLimit)
	status  int16 // 0 for a message, otherwise the status code to reply with, or statusHandoff or statusClose
	chunked bool  // the message's body is chunked
}

const (
	// statusHandoff is the queue item that hands the connection to net/http, see conn.handoff.
	statusHandoff = -1
	// statusClose is the queue item that closes the connection once the requests before it are answered, see
	// conn.halfClose.
	statusClose = -2
)

// conn is the state of one HTTP connection.
type conn struct {
	srv        *Server
	connection fnet.Conn
	remoteAddr string       // produced on the first request, accessed only by the worker (workers of one connection run serially)
	last       *lastRequest // what parseRequest shares with the next request, made on the first one; accessed only by the worker, except for its ctx (see context)
	serveTask  func()       // serve, bound once, so that handing it to a worker does not allocate each time

	framer      framer // accessed only by fnet's callbacks (callbacks of one connection run serially)
	queuedBytes int32  // the total size of the messages in the queue, below maxLimit; guarded by mu, placed here to fill the room framer leaves

	protocol atomic.Pointer[Protocol] // the protocol that takes over the connection after Upgrade switches; written only while holding mu

	mu         sync.Mutex
	queue      []request
	writable   chan struct{} // signaled by onWritable and onClose for a Handler waiting in waitWritable, made the first time one waits
	worker     workerState   // what the connection's worker is doing
	partial    bool          // the connection buffer holds an incomplete request; written only by fnet's callbacks
	readPaused bool          // too much is queued, so reading is paused until the worker has emptied the queue (see maxPipeline)
	broken     bool          // a protocol error has occurred, all subsequent data is discarded; accessed only by fnet's callbacks
	closed     atomic.Bool   // set by onClose while holding mu; read without it by the response's writes
}

// workerState is what the worker of a connection is doing (see serve); it changes while holding mu. At most one worker
// processes a connection's queue at a time.
type workerState uint8

const (
	workerIdle    workerState = iota // no worker: the next complete request submits one, see onData
	workerServing                    // a worker processes the queue until it is empty
	workerWaiting                    // the worker stopped, as the client was not taking the responses, until fnet reports them taken (see onWritable)
	workerClosing                    // the worker is closing the connection, so none is submitted again
)

// setDeadline sets the deadline according to the connection's state and requires the caller to hold mu: while a
// request is being processed there is no limit; when there is an incomplete request, ReadHeaderTimeout applies
// starting now; otherwise the connection is idle and IdleTimeout applies. It is called only when the state changes, so
// arriving data does not extend the deadline. After a protocol switch the deadline is managed by the new protocol.
func (c *conn) setDeadline() {
	if c.worker == workerClosing || c.protocol.Load() != nil { // the deadline is the closing worker's (see serve), or the new protocol's
		return
	}
	var timeout time.Duration
	switch {
	case c.worker == workerWaiting:
		timeout = c.srv.opts.IdleTimeout // the client has as long to take the responses, see serve
	case c.worker == workerServing:
	case c.partial:
		timeout = c.srv.opts.ReadHeaderTimeout
	default:
		timeout = c.srv.opts.IdleTimeout
	}
	c.connection.SetDeadline(deadlineAfter(timeout))
}

// deadlineAfter returns the instant timeout from now; timeout<=0 means no limit and the zero value is returned.
func deadlineAfter(timeout time.Duration) time.Time {
	if timeout <= 0 {
		return time.Time{}
	}
	return time.Now().Add(timeout)
}

// onData splits out the complete request messages and enqueues them all at once, returning the number of consumed
// bytes.
func (c *conn) onData(data []byte) int {
	if p := c.protocol.Load(); p != nil {
		// The protocol takes over the context from here on, which a callback may set. Nothing of the HTTP state is
		// needed any more: Upgrade released the queue, and closed only stops an upgrade that has already happened.
		c.connection.SetContext(*p)
		return (*p).OnData(data)
	}
	if c.broken {
		return len(data)
	}
	c.mu.Lock()
	if c.protocol.Load() != nil {
		// The protocol was switched (it is stored while holding mu) after the check above: this data was sent by a client
		// that did not wait for the upgrade response, so it must not be executed as HTTP.
		c.broken = true
		c.mu.Unlock()
		c.connection.Close()
		return len(data)
	}
	consumed := 0
	queued := len(c.queue)
	for consumed < len(data) {
		if c.framer == (framer{}) {
			// An empty line before a request, CRLF or a bare LF, is ignored, as some clients send one after a body (RFC
			// 9112 2.2); a bare CR is not one, and is left to make the request line invalid.
			if rest := data[consumed:]; rest[0] == '\n' {
				consumed++
				continue
			} else if rest[0] == '\r' && len(rest) == 1 {
				break // its LF may be on its way
			} else if bytes.HasPrefix(rest, []byte("\r\n")) {
				consumed += 2
				continue
			}
		}
		m, expectContinue, err := c.framer.next(data[consumed:], &c.srv.opts)
		if err == errHandoff {
			// The rest of the connection goes to net/http once the earlier requests are answered. Reading stops, so the
			// request stays unconsumed for it.
			c.connection.PauseRead()
			c.push(nil, message{}, statusHandoff)
			break
		}
		if err != nil {
			// The error response also goes out through the queue, which guarantees it comes after the responses of
			// earlier requests.
			status := int16(http.StatusBadRequest)
			switch err {
			case errHeaderTooLarge:
				status = http.StatusRequestHeaderFieldsTooLarge
			case errURITooLong:
				status = http.StatusRequestURITooLong
			}
			c.broken = true
			c.push(nil, message{}, status)
			consumed = len(data)
			break
		}
		if m.size == 0 {
			// The header is complete and the client waits for 100 Continue before sending the body. It goes through
			// the queue as well, so it is not written in the middle of earlier responses.
			if expectContinue {
				c.push(nil, message{}, http.StatusContinue)
			}
			break
		}
		c.push(data[consumed:consumed+m.size], m, 0)
		consumed += m.size
	}
	if len(c.queue) >= maxPipeline || c.queuedBytes >= maxQueuedBytes {
		// Paused only once every complete request at hand is queued: the engine does not pass the unconsumed data on again
		// when reading resumes, so that must be no more than a request whose next bytes are still to come.
		c.readPaused = true // resumed by the worker, see serve
		c.connection.PauseRead()
	}
	submit := len(c.queue) > queued && c.worker == workerIdle
	if submit {
		c.worker = workerServing
	}
	partial := consumed < len(data)
	if submit || partial != c.partial {
		c.partial = partial
		c.setDeadline()
	}
	c.mu.Unlock()
	if submit {
		c.srv.opts.Executor(taskpool.RandomKey(), c.serveTask)
	}
	return consumed
}

// push puts a copy of the request message data framed as m (or, when data is nil, the status code status) into the
// queue; the caller holds mu.
func (c *conn) push(data []byte, m message, status int16) {
	r := request{header: int32(m.header), status: status, chunked: m.chunked}
	r.msg.Append(data) // handle owns a dequeued buffer; onClose releases those still queued
	c.queue = append(c.queue, r)
	c.queuedBytes += int32(len(data))
}

// halfClose is called when the client has closed its write direction, which the engine finds out only after it has read
// all that the client sent, so the requests sent in full are queued. Those not answered yet still are, as on net/http,
// and the connection closes behind the last of them (see statusClose); the requests' context ends, as it does on
// net/http, since the client is leaving. It reports whether the connection stays open: it does not when there is no
// request left to answer, or a protocol that took the connection over is in charge of it.
func (c *conn) halfClose() bool {
	c.mu.Lock()
	if c.protocol.Load() != nil || c.worker != workerServing && c.worker != workerWaiting {
		c.mu.Unlock()
		return false
	}
	c.push(nil, message{}, statusClose)
	cancel := c.contextCancel()
	c.mu.Unlock()
	if cancel != nil {
		cancel()
	}
	return true
}

// halfClosed reports whether the client has closed its write direction while requests were still to be answered, which is
// when the queue ends with statusClose; the caller holds mu.
func (c *conn) halfClosed() bool {
	n := len(c.queue)
	return n > 0 && c.queue[n-1].status == statusClose
}

func (c *conn) onClose(err error) {
	c.mu.Lock()
	c.closed.Store(true)
	cancel := c.contextCancel()
	for i := range c.queue {
		c.queue[i].msg.Release()
	}
	c.queue, c.queuedBytes = nil, 0
	c.wakeWriter() // a Handler waiting in Write gives up, see waitWritable
	c.mu.Unlock()
	if cancel != nil {
		cancel()
	}
	// Once closed is set, Upgrade no longer switches the protocol, so what is read here is the final result.
	if p := c.protocol.Load(); p != nil {
		(*p).OnClose(err)
	}
}

// serve processes the queued requests in order on a worker, until the queue is empty or the connection needs to be
// closed. The responses are collected and written out together whenever the queue is found empty, so pipelined
// requests are answered in one write.
//
// A Handler that exits the goroutine (runtime.Goexit, which is what a test's FailNow does), or a panic that serveHTTP does
// not stop, ends the worker without it having stopped: no other is submitted while it is serving, and the client would
// wait for answers for good. The connection is closed instead, after the responses collected so far, as on net/http.
func (c *conn) serve() {
	w := responsePool.Get().(*response)
	w.conn = c
	defer w.release()
	returned := false
	defer func() {
		if !returned {
			w.flush()
			c.closeFromWorker()
		}
	}()
	c.serveQueue(w)
	returned = true
}

// serveQueue is the loop of serve.
func (c *conn) serveQueue(w *response) {
	for {
		c.mu.Lock()
		// The client is not taking the responses: answering more would only pile them up in fnet's send buffer.
		stalled := len(c.queue) > 0 && !c.connection.Writable()
		if !c.closed.Load() && w.out.Len() > 0 && (len(c.queue) == 0 || stalled) {
			// Write out what was collected while still serving, so no other worker writes in between; then look again, as
			// requests may have been queued meanwhile.
			c.mu.Unlock()
			if w.flush() != nil {
				c.closeFromWorker()
				return
			}
			continue
		}
		switch {
		case c.closed.Load() || len(c.queue) == 0:
			c.worker = workerIdle
			resume := c.readPaused // everything queued has been answered, see maxPipeline
			c.readPaused = false
			c.setDeadline()
			c.mu.Unlock()
			if resume {
				c.connection.ResumeRead()
			}
			return
		case stalled:
			// What was collected has gone out: the worker stops until fnet reports the buffer back down (see onWritable);
			// meanwhile the client has IdleTimeout to take the responses, as on a connection kept alive.
			c.worker = workerWaiting
			c.setDeadline()
			c.mu.Unlock()
			return
		}
		r := c.queue[0]
		n := copy(c.queue, c.queue[1:])
		c.queue[n] = request{}
		c.queue = c.queue[:n]
		c.queuedBytes -= int32(r.msg.Len())
		if n == 0 && cap(c.queue) > maxRetainedQueueCapacity {
			c.queue = nil
		}
		c.mu.Unlock()

		if !c.handle(w, r) {
			w.flush()
			c.closeFromWorker()
			return
		}
	}
}

// closeFromWorker closes the connection from its worker, which then stops. fnet sends what is left before closing, which
// a client that does not read would put off for good: IdleTimeout bounds it, as it bounds reading the responses on a
// connection kept alive.
func (c *conn) closeFromWorker() {
	c.mu.Lock()
	c.worker = workerClosing // what the client sends meanwhile neither submits a worker nor changes the deadline
	c.mu.Unlock()
	c.connection.SetDeadline(deadlineAfter(c.srv.opts.IdleTimeout))
	c.connection.Close()
}

// onWritable resumes the worker that stopped while the client was not taking the responses, see serve, or the Handler
// waiting in Write for it to take a response, see waitWritable.
func (c *conn) onWritable() {
	c.mu.Lock()
	c.wakeWriter()
	resume := c.worker == workerWaiting && !c.closed.Load()
	if resume {
		c.worker = workerServing
		c.setDeadline()
	}
	c.mu.Unlock()
	if resume {
		c.srv.opts.Executor(taskpool.RandomKey(), c.serveTask)
	}
}

// waitWritable has the Handler's Write wait while the client is not taking the response (fnet's send buffer holds more
// than fnet.Conn.Writable allows), until onWritable reports the buffer back down or onClose the connection closed. The
// client has IdleTimeout to take it, as between requests (see serve). The wait is timed here rather than by the
// connection's deadline: only a task of the executor carries a deadline out, and the Handlers waiting may hold every
// goroutine of the executor.
func (c *conn) waitWritable() error {
	if c.connection.Writable() {
		return nil
	}
	c.mu.Lock()
	if c.writable == nil {
		c.writable = make(chan struct{}, 1)
	}
	c.mu.Unlock()
	var expired <-chan time.Time
	if timeout := c.srv.opts.IdleTimeout; timeout > 0 {
		timer := time.NewTimer(timeout)
		defer timer.Stop()
		expired = timer.C
	}
	for !c.connection.Writable() && !c.closed.Load() { // looked at again: onWritable may have come before writable was made
		select {
		case <-c.writable:
		case <-expired:
			return os.ErrDeadlineExceeded
		}
	}
	return nil // once closed, the write that follows fails with net.ErrClosed
}

// wakeWriter wakes the Handler waiting in waitWritable, if any, and requires the caller to hold mu. A signal left when no
// Handler waits only has the next wait look at Writable once more.
func (c *conn) wakeWriter() {
	select {
	case c.writable <- struct{}{}: // never ready while writable is nil
	default:
	}
}
