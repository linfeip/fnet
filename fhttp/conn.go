package fhttp

import (
	"bytes"
	"context"
	"log/slog"
	"net/http"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"github.com/linfeip/fnet"
	"github.com/linfeip/fnet/ftls"
	"github.com/linfeip/fnet/internal/bytepool"
	"github.com/linfeip/fnet/internal/units"
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
// (100 Continue, or an error that closes the connection), or statusHandoff.
type request struct {
	msg     bytepool.Buffer
	header  int32 // the length of the message's request line and headers (see maxLimit)
	status  int16 // 0 for a message, otherwise the status code to reply with, or statusHandoff
	chunked bool  // the message's body is chunked
}

// statusHandoff is the queue item that hands the connection to net/http, see conn.handoff.
const statusHandoff = -1

// conn is the state of one HTTP connection.
type conn struct {
	srv        *Server
	connection fnet.Conn
	remoteAddr string       // produced on the first request, accessed only by the worker (workers of one connection run serially)
	last       *lastRequest // what parseRequest shares with the next request, made on the first one; accessed only by the worker, except for its ctx (see context)
	serveTask  func()       // serve, bound once, so that handing it to a worker does not allocate each time

	framer framer // accessed only by fnet's callbacks (callbacks of one connection run serially)
	broken bool   // a protocol error has occurred, all subsequent data is discarded; accessed only by fnet's callbacks

	protocol atomic.Pointer[Protocol] // the protocol that takes over the connection after Upgrade switches; written only while holding mu

	mu          sync.Mutex
	queue       []request
	worker      workerState // what the connection's worker is doing
	partial     bool        // the connection buffer holds an incomplete request; written only by fnet's callbacks
	readPaused  bool        // too much is queued, so reading is paused until the worker has emptied the queue (see maxPipeline)
	closed      atomic.Bool // set by onClose while holding mu; read without it by the response's writes
	queuedBytes int32       // the total size of the messages in the queue, below maxLimit
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
		c.srv.opts.Executor(c.connection.TaskKey(), c.serveTask)
	}
	return consumed
}

// push puts a copy of the request message data framed as m (or, when data is nil, the status code status) into the
// queue; the caller holds mu.
func (c *conn) push(data []byte, m message, status int16) {
	r := request{header: int32(m.header), status: status, chunked: m.chunked}
	r.msg.Append(data) // serve returns the buffer on dequeue, onClose when the queue is dropped
	c.queue = append(c.queue, r)
	c.queuedBytes += int32(len(data))
}

func (c *conn) onClose(err error) {
	c.mu.Lock()
	c.closed.Store(true)
	var cancel context.CancelFunc // the requests' context, see conn.context
	if c.last != nil {
		if ctx := c.last.ctx.Load(); ctx != nil {
			cancel = ctx.cancel
		}
	}
	for i := range c.queue {
		c.queue[i].msg.Release()
	}
	c.queue, c.queuedBytes = nil, 0
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
func (c *conn) serve() {
	w := responsePool.Get().(*response)
	w.conn = c
	defer w.release()
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

		keepAlive := c.handle(w, r)
		r.msg.Release() // the dequeued message is owned by serve, handle only borrows it
		if !keepAlive {
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

// onWritable resumes the worker that stopped while the client was not taking the responses, see serve.
func (c *conn) onWritable() {
	c.mu.Lock()
	resume := c.worker == workerWaiting && !c.closed.Load()
	if resume {
		c.worker = workerServing
		c.setDeadline()
	}
	c.mu.Unlock()
	if resume {
		c.srv.opts.Executor(c.connection.TaskKey(), c.serveTask)
	}
}

// continueResponse is the interim response that asks the client to send the request body.
var continueResponse = []byte("HTTP/1.1 100 Continue\r\n\r\n")

// handle parses and processes one request, its response going into w's output, and reports whether to keep the
// connection alive.
func (c *conn) handle(w *response, r request) bool {
	switch r.status {
	case 0:
	case http.StatusContinue:
		w.out.Append(continueResponse)
		return true
	case statusHandoff:
		if w.flush() != nil {
			c.connection.Close()
			return false
		}
		c.handoff()
		return false // closing the detached connection does nothing
	default:
		w.writeError(int(r.status))
		return false
	}

	if c.last == nil {
		last := new(lastRequest)
		c.mu.Lock() // onClose reads it while holding mu
		c.last = last
		c.mu.Unlock()
	}
	req, parsed, status := c.parse(r)
	if status != 0 {
		w.writeError(status)
		return false
	}
	parsed.bind(c)
	if c.remoteAddr == "" {
		c.remoteAddr = c.connection.RemoteAddr().String()
	}
	req.RemoteAddr = c.remoteAddr
	if tc, ok := c.connection.(*ftls.Conn); ok {
		if c.last.tls == nil { // the requests of a connection share it, as on net/http
			state := tc.ConnectionState()
			c.last.tls = &state
		}
		req.TLS = c.last.tls
	}

	w.start(req)
	reqBody := req.Body
	ok := c.serveHTTP(w, req)
	// Same as the net/http server: close the body, so a Handler that kept it cannot read the message buffer or the
	// pooled reader once they are reused, and remove the files ParseMultipartForm stored on disk.
	reqBody.Close()
	if req.MultipartForm != nil {
		req.MultipartForm.RemoveAll()
	}
	keepAlive := false
	switch {
	case !ok:
	case w.upgraded: // Upgrade already wrote the response: on success the connection goes to the new protocol, on failure it closes
		keepAlive = c.protocol.Load() != nil && w.writeErr == nil
		c.endContext()
	default:
		keepAlive = w.finish()
	}
	parsed.release() // the Handler has returned, see the package documentation
	return keepAlive
}

// serveHTTP runs the Handler; when the Handler panics it logs the panic (except for http.ErrAbortHandler) and
// returns false.
func (c *conn) serveHTTP(w *response, req *http.Request) (ok bool) {
	defer func() {
		if err := recover(); err != nil {
			if err != http.ErrAbortHandler {
				buf := make([]byte, 64*units.KB)
				slog.Error("fhttp: panic serving request", "remoteAddr", c.remoteAddr, "panic", err,
					"stack", string(buf[:runtime.Stack(buf, false)]))
			}
			ok = false
		}
	}()
	handler := c.srv.handler
	if req.RequestURI == "*" && req.Method == http.MethodOptions {
		handler = serverOptions
	}
	handler.ServeHTTP(w, req)
	return true
}

// serverOptions answers "OPTIONS *", which asks about the server as a whole rather than a resource (RFC 9110 9.3.7), the
// way the net/http server does: 200 with no content, where http.ServeMux would answer 400.
var serverOptions = http.HandlerFunc(func(http.ResponseWriter, *http.Request) {})
