package fhttp

import (
	"context"
	"crypto/tls"
	"log/slog"
	"net/http"
	"os"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"github.com/linfeip/fnet"
	"github.com/linfeip/fnet/internal/bytepool"
	"github.com/linfeip/fnet/internal/httpx"
)

// maxPipeline and maxQueuedBytes bound the requests queued for processing on a connection, by number and by size: once
// either is reached the connection is not read until the worker has answered them all (see conn.serve), so a client
// that pipelines deeply, or sends requests faster than the Handlers answer them, is slowed down by TCP flow control
// instead of being held in memory. The requests of the data at hand are queued in full first, so the queue may go past
// them by what one read brings (see conn.onData).
const (
	maxPipeline    = 128
	maxQueuedBytes = 64 << 10
)

// maxRetainedQueueCapacity is the maximum capacity retained once the request queue has been drained: a connection
// pipelining a few requests at a time (16 is a common depth) needs no reallocation every time, a connection that sends
// them one by one keeps only one item, and the large capacity left behind by a pipelining burst is released, so a
// large number of connections do not hold on to memory for long.
const maxRetainedQueueCapacity = 16

// continueResponse is the interim response that asks the client to send the request body.
var continueResponse = []byte("HTTP/1.1 100 Continue\r\n\r\n")

// serverOptions answers "OPTIONS *", which asks about the server as a whole rather than a resource (RFC 9110 9.3.7), the
// way the net/http server does: 200 with no content, where http.ServeMux would answer 400.
var serverOptions = http.HandlerFunc(func(http.ResponseWriter, *http.Request) {})

// tlsConnection is implemented by connections that carry a TLS session (such as *ftls.Conn).
type tlsConnection interface {
	ConnectionState() tls.ConnectionState
}

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
	state      *connState // what a connection's requests share, made on the first one; accessed only by the worker, except for its ctx
	serveTask  func()     // serve, bound once, so that handing it to a worker does not allocate each time

	framer      framer // accessed only by fnet's callbacks (callbacks of one connection run serially)
	queuedBytes int32  // the total size of the messages in the queue, below maxLimit; guarded by mu, placed here to fill the room framer leaves

	protocol atomic.Pointer[Protocol] // the protocol that takes over the connection after Upgrade switches; written only while holding mu

	mu         sync.Mutex
	queue      []request
	writable   chan struct{} // signaled by onWritable and onClose for a Handler waiting in waitWritable, made the first time one waits
	worker     workerState   // what the connection's worker is doing
	partial    bool          // the connection buffer holds an incomplete request; written only by fnet's callbacks
	readPaused bool          // too much is queued, so reading is paused until the worker has emptied the queue (see maxPipeline)
	halfClosed bool          // the client has closed its write direction while requests were still to be answered; guarded by mu
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
	c.connection.SetDeadline(httpx.DeadlineAfter(timeout))
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
	queued := len(c.queue)
	consumed, end := c.framer.split(data, &c.srv.opts, c.push)
	switch {
	case end == statusHandoff:
		// The rest of the connection goes to net/http once the earlier requests are answered. Reading stops, so the
		// request stays unconsumed for it.
		c.connection.PauseRead()
	case end != 0: // the error response goes out through the queue, after the responses of earlier requests
		c.broken = true
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
		c.srv.opts.Executor(c.connection.Fd(), c.serveTask)
	}
	return consumed
}

// maxRetainedMessageBytes is the largest request message buffer retained in the connection's queue slot across requests.
const maxRetainedMessageBytes = 4 << 10

// push puts a copy of the request message data framed as m (or, when data is nil, the status code status) into the
// queue; the caller holds mu.
func (c *conn) push(data []byte, m message, status int16) {
	var msg bytepool.Buffer
	if i := len(c.queue); i < cap(c.queue) {
		slot := &c.queue[:i+1][i]
		msg = slot.msg
		slot.msg = bytepool.Buffer{}
	}
	msg.Write(data) // handle owns a dequeued buffer; releaseQueue releases those still in the queue
	c.queue = append(c.queue, request{msg: msg, header: int32(m.header), status: status, chunked: m.chunked})
	c.queuedBytes += int32(len(data))
}

// releaseQueue releases every buffer held in c.queue, including any retained in its spare capacity; the caller holds mu.
func (c *conn) releaseQueue() {
	full := c.queue[:cap(c.queue)]
	for i := range full {
		full[i].msg.Release()
	}
	c.queue, c.queuedBytes = nil, 0
}

// halfClose is called when the client has closed its write direction, which the engine finds out only after it has read
// all that the client sent, so the requests sent in full are queued. Those not answered yet still are, as on net/http,
// and the connection closes once the queue is empty (see serveQueue); the requests' context ends, as it does on
// net/http, since the client is leaving. It reports whether the connection stays open: it does not when there is no
// request left to answer, or a protocol that took the connection over is in charge of it. It is called once per
// connection, as fnet and package ftls ask once.
func (c *conn) halfClose() bool {
	c.mu.Lock()
	if c.protocol.Load() != nil || c.worker != workerServing && c.worker != workerWaiting {
		c.mu.Unlock()
		return false
	}
	c.halfClosed = true
	cancel := c.contextCancel()
	c.mu.Unlock()
	if cancel != nil {
		cancel()
	}
	return true
}

func (c *conn) onClose(err error) {
	c.mu.Lock()
	c.closed.Store(true)
	cancel := c.contextCancel()
	c.releaseQueue()
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
	var retained bytepool.Buffer
	defer func() { retained.Release() }()
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
			if c.halfClosed && !c.closed.Load() {
				c.mu.Unlock()
				c.closeFromWorker()
				return
			}
			if !c.closed.Load() && cap(c.queue) > 0 && cap(retained.Bytes()) > 0 {
				slot := &c.queue[:1][0]
				if cap(slot.msg.Bytes()) == 0 {
					slot.msg, retained = retained, bytepool.Buffer{}
				}
			}
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
		if n == 0 && cap(c.queue) > maxRetainedQueueCapacity {
			c.releaseQueue()
		} else {
			c.queue = c.queue[:n]
			c.queuedBytes -= int32(r.msg.Len())
		}
		c.mu.Unlock()

		keepAlive := c.handle(w, r)
		if keepAlive && cap(retained.Bytes()) == 0 && cap(r.msg.Bytes()) <= maxRetainedMessageBytes {
			r.msg.Reset()
			retained = r.msg
		} else {
			r.msg.Release()
		}
		if !keepAlive {
			w.flush()
			c.closeFromWorker()
			return
		}
	}
}

// handle parses and processes one request, its response going into w's output, and reports whether to keep the
// connection alive.
func (c *conn) handle(w *response, r request) bool {
	switch r.status {
	case 0:
	case http.StatusContinue:
		w.out.Write(continueResponse)
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

	if c.state == nil {
		state := &connState{connection: c}
		c.mu.Lock() // onClose reads it while holding mu
		c.state = state
		c.mu.Unlock()
	}
	if status := c.readRequest(w, r); status != 0 {
		w.writeError(status)
		return false
	}
	req := &w.req
	if c.state.remoteAddr == "" {
		c.state.remoteAddr = c.connection.RemoteAddr().String()
	}
	req.RemoteAddr = c.state.remoteAddr
	if tc, ok := c.connection.(tlsConnection); ok {
		if c.state.tls == nil { // the requests of a connection share it, as on net/http
			state := tc.ConnectionState()
			c.state.tls = &state
		}
		req.TLS = c.state.tls
	}

	w.start()
	ok := c.serveHTTP(w, req)
	keepAlive := false
	switch {
	case !ok:
	case w.upgraded: // Upgrade already wrote the response: on success the connection goes to the new protocol, on failure it closes
		keepAlive = c.protocol.Load() != nil && w.writeErr == nil
		c.endContext()
	default:
		keepAlive = w.finish()
	}
	return keepAlive
}

// serveHTTP runs the Handler; when the Handler panics it logs the panic (except for http.ErrAbortHandler) and
// returns false.
func (c *conn) serveHTTP(w *response, req *http.Request) (ok bool) {
	reqBody := req.Body
	defer func() {
		reqBody.Close()
		w.bytesReader.Reset(nil)
		if req.MultipartForm != nil {
			req.MultipartForm.RemoveAll()
		}
		if !ok && !w.streamed {
			w.out.Truncate(w.outStart)
		}
		if err := recover(); err != nil {
			if err != http.ErrAbortHandler {
				buf := make([]byte, 64<<10)
				slog.Error("fhttp: panic serving request", "remoteAddr", c.state.remoteAddr, "panic", err,
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

// closeFromWorker closes the connection from its worker, which then stops. fnet sends what is left before closing, which
// a client that does not read would put off for good: IdleTimeout bounds it, as it bounds reading the responses on a
// connection kept alive.
func (c *conn) closeFromWorker() {
	c.mu.Lock()
	c.worker = workerClosing // what the client sends meanwhile neither submits a worker nor changes the deadline
	c.mu.Unlock()
	c.connection.SetDeadline(httpx.DeadlineAfter(c.srv.opts.IdleTimeout))
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
		c.srv.opts.Executor(c.connection.Fd(), c.serveTask)
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

// connState holds what a connection's requests share, made on its first request: the peer address, the TLS state and
// the lazy connection context. It implements context.Context by forwarding to that context, so a Handler that does not
// use Request.Context pays neither for context.WithCancel nor for the LocalAddr syscall.
type connState struct {
	connection *conn
	remoteAddr string                      // Request.RemoteAddr, formatted on the connection's first request
	tls        *tls.ConnectionState        // Request.TLS, made on the first request of a TLS connection
	ctx        atomic.Pointer[connContext] // made when a Handler first asks for the request's context
}

// connContext is the cancelable context of a connection's requests.
type connContext struct {
	context.Context
	cancel context.CancelFunc
}

func (s *connState) Deadline() (time.Time, bool) { return time.Time{}, false }
func (s *connState) Done() <-chan struct{}       { return s.context().Done() }
func (s *connState) Err() error                  { return s.context().Err() }

// Value passes on to the connection's context: that is how context.WithCancel, WithTimeout and AfterFunc find the
// cancelable context in it and register with it, instead of starting a goroutine for each derived context.
func (s *connState) Value(key any) any { return s.context().Value(key) }

// context returns the context of the connection's requests, making it when a Handler first asks for it. It holds the
// values the net/http server puts in a request's context: the local address and an *http.Server.
func (s *connState) context() context.Context {
	if ctx := s.ctx.Load(); ctx != nil {
		return ctx.Context
	}
	c := s.connection
	values := context.WithValue(context.Background(), http.ServerContextKey, c.srv.streams)
	values = context.WithValue(values, http.LocalAddrContextKey, c.connection.LocalAddr()) // outside mu: a syscall
	c.mu.Lock()
	defer c.mu.Unlock()
	if s.ctx.Load() == nil {
		ctx, cancel := context.WithCancel(values)
		s.ctx.Store(&connContext{Context: ctx, cancel: cancel})
		if c.closed.Load() || c.halfClosed || c.protocol.Load() != nil {
			cancel()
		}
	}
	return s.ctx.Load().Context
}

// contextCancel returns the function that cancels the context of the connection's requests, nil while no Handler has asked
// for it yet; the caller holds mu.
func (c *conn) contextCancel() context.CancelFunc {
	if c.state != nil {
		if ctx := c.state.ctx.Load(); ctx != nil {
			return ctx.cancel
		}
	}
	return nil
}

// endContext cancels the context of the connection's requests and releases the shared request state once the
// connection has gone to a new protocol or to net/http, as on net/http, where a request's context ends with its
// Handler. It is called by the worker.
func (c *conn) endContext() {
	c.mu.Lock()
	cancel := c.contextCancel()
	c.state = nil
	c.writable = nil
	c.mu.Unlock()
	if cancel != nil {
		cancel()
	}
}
