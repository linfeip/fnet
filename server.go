package fnet

import (
	"bufio"
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/gobwas/ws"
)

// Server is a high-performance HTTP/HTTPS server driven by native multi-reactor pollers
// (epoll / kqueue / WSAPoll). TLS and HTTP parsing reuse the Go standard library.
//
// One Server owns a single accept loop and NumPollers sub-reactors regardless of how
// many addresses it listens on, so listening on many ports does not multiply pollers.
type Server struct {
	// Addr is the TCP address to listen on (e.g. ":8080").
	Addr string
	// Addrs is an optional list of additional TCP addresses to listen on.
	// All listeners share the same accept loop and reactor pool.
	Addrs []string
	// Handler is invoked for each HTTP request. Defaults to http.DefaultServeMux.
	Handler http.Handler
	// TLSConfig enables HTTPS when non-nil (or when ListenAndServeTLS is used).
	TLSConfig *tls.Config
	// ReadTimeout is an optional per-connection read deadline for the worker.
	ReadTimeout time.Duration
	// WriteTimeout is an optional per-connection write deadline for the worker.
	WriteTimeout time.Duration
	// IdleTimeout closes keep-alive connections after this idle period (0 = disable keep-alive loop beyond one request).
	IdleTimeout time.Duration
	// NumPollers specifies the number of I/O sub-reactors. Defaults to runtime.GOMAXPROCS(0).
	NumPollers int

	// Listen is an optional listener constructor (e.g. frameworks.Listen / reuseport.Listen).
	// When nil, net.ListenConfig with SO_REUSEADDR and SO_REUSEPORT is used.
	Listen func(network, addr string) (net.Listener, error)

	lnMu        sync.Mutex
	listeners   map[int]net.Addr // listener fd -> local address
	mainPoller  Poller
	reactors    []*subReactor
	nextReactor atomic.Uint64
	closing     atomic.Bool
	wg          sync.WaitGroup
}

type subReactor struct {
	id     int
	server *Server
	poller Poller
	conns  connTable
	rbuf   []byte // per-reactor read buffer

	// Work handed to the reactor goroutine from other goroutines.
	pendMu    sync.Mutex
	pendWS    []*conn // connections that just switched to event-driven WebSocket
	pendClose []int   // fds whose close must happen on the reactor
	pendCount atomic.Int32
	waiting   atomic.Bool // reactor is (about to be) blocked in Wait
}

const (
	connChunkShift = 11 // 2048 entries per chunk
	connChunkSize  = 1 << connChunkShift
	connChunkMask  = connChunkSize - 1
)

type connChunk [connChunkSize]atomic.Pointer[conn]

type connTable struct {
	mu     sync.Mutex
	chunks atomic.Pointer[[]*connChunk]
}

func (t *connTable) get(fd int) *conn {
	if fd < 0 {
		return nil
	}
	cp := t.chunks.Load()
	if cp == nil {
		return nil
	}
	chunks := *cp
	cIdx := fd >> connChunkShift
	if cIdx >= len(chunks) {
		return nil
	}
	ch := chunks[cIdx]
	if ch == nil {
		return nil
	}
	return ch[fd&connChunkMask].Load()
}

func (t *connTable) store(fd int, c *conn) {
	if fd < 0 {
		return
	}
	cIdx := fd >> connChunkShift
	sIdx := fd & connChunkMask

	cp := t.chunks.Load()
	if cp != nil {
		chunks := *cp
		if cIdx < len(chunks) && chunks[cIdx] != nil {
			chunks[cIdx][sIdx].Store(c)
			return
		}
	}

	t.mu.Lock()
	defer t.mu.Unlock()

	var oldChunks []*connChunk
	if p := t.chunks.Load(); p != nil {
		oldChunks = *p
	}
	newLen := len(oldChunks)
	if cIdx >= newLen {
		newLen = cIdx + 1
	}
	newChunks := make([]*connChunk, newLen)
	copy(newChunks, oldChunks)
	if newChunks[cIdx] == nil {
		newChunks[cIdx] = new(connChunk)
	}
	newChunks[cIdx][sIdx].Store(c)
	t.chunks.Store(&newChunks)
}

func (t *connTable) delete(fd int) {
	if fd < 0 {
		return
	}
	cp := t.chunks.Load()
	if cp == nil {
		return
	}
	chunks := *cp
	cIdx := fd >> connChunkShift
	if cIdx >= len(chunks) {
		return
	}
	ch := chunks[cIdx]
	if ch != nil {
		ch[fd&connChunkMask].Store(nil)
	}
}

func (t *connTable) forEach(fn func(*conn) bool) {
	cp := t.chunks.Load()
	if cp == nil {
		return
	}
	for _, ch := range *cp {
		if ch == nil {
			continue
		}
		for i := 0; i < connChunkSize; i++ {
			c := ch[i].Load()
			if c != nil {
				if !fn(c) {
					return
				}
			}
		}
	}
}

const (
	connStateIdle = iota
	connStateWorking
	connStateHijacked
	connStateWSAttached    // AttachWS called; reactor buffers frames until the HTTP handler returns
	connStateWSEventDriven // reactor parses frames and invokes the WSHandler inline
	connStateClosed
)

const (
	maxHeaderBuffer = 64 * 1024 // max buffered header bytes before a slow-loris connection is dropped
	reactorBufSize  = 64 * 1024
	pollTimeout     = time.Second
)

type conn struct {
	fd      int
	vc      *VirtualConn
	server  *Server
	reactor *subReactor

	state           atomic.Int32
	closed          atomic.Bool
	writeArmed      atomic.Bool // write interest registered with the poller
	closeAfterFlush atomic.Bool // close once the outbound queue drains

	mu        sync.Mutex
	wsHandler WSHandler
	closeErr  error
}

// ErrWSAttachUnsupported is returned by WSAttacher.AttachWS when the connection
// cannot be driven by the reactor (currently: TLS connections, whose records are
// decrypted by a worker goroutine).
var ErrWSAttachUnsupported = errors.New("fnet: event-driven websocket attach not supported on this connection")

var readerPool = sync.Pool{
	New: func() any { return bufio.NewReaderSize(nil, 4096) },
}

// ListenAndServe starts a plain HTTP server.
func (s *Server) ListenAndServe() error {
	return s.serve(nil)
}

// ListenAndServeTLS starts an HTTPS server with the given certificate files.
func (s *Server) ListenAndServeTLS(certFile, keyFile string) error {
	cert, err := tls.LoadX509KeyPair(certFile, keyFile)
	if err != nil {
		return err
	}
	cfg := s.TLSConfig
	if cfg == nil {
		cfg = &tls.Config{}
	} else {
		cfg = cfg.Clone()
	}
	cfg.Certificates = []tls.Certificate{cert}
	return s.serve(cfg)
}

func (s *Server) listenAddrs() []string {
	if s.Addr == "" && len(s.Addrs) > 0 {
		return s.Addrs
	}
	addrs := make([]string, 0, 1+len(s.Addrs))
	addrs = append(addrs, s.Addr)
	return append(addrs, s.Addrs...)
}

func (s *Server) serve(tlsCfg *tls.Config) error {
	if s.Handler == nil {
		s.Handler = http.DefaultServeMux
	}
	if tlsCfg != nil {
		s.TLSConfig = tlsCfg
	}

	listeners := make(map[int]net.Addr)
	closeListeners := func() {
		for fd := range listeners {
			_ = closeFD(fd)
		}
	}
	for _, addr := range s.listenAddrs() {
		var (
			fd    int
			laddr net.Addr
			err   error
		)
		if s.Listen != nil {
			var ln net.Listener
			ln, err = s.Listen("tcp", addr)
			if err == nil {
				laddr = ln.Addr()
				fd, err = dupListenerFD(ln)
				_ = ln.Close()
			}
		} else {
			fd, laddr, err = listenNonblock("tcp", addr)
		}
		if err != nil {
			closeListeners()
			return err
		}
		listeners[fd] = laddr
	}

	num := s.NumPollers
	if num <= 0 {
		num = runtime.GOMAXPROCS(0)
	}
	if num < 1 {
		num = 1
	}
	if num > 64 {
		num = 64
	}

	mainP, err := NewPoller()
	if err != nil {
		closeListeners()
		return err
	}

	reactors := make([]*subReactor, num)
	for i := 0; i < num; i++ {
		p, err := NewPoller()
		if err != nil {
			_ = mainP.Close()
			for j := 0; j < i; j++ {
				_ = reactors[j].poller.Close()
			}
			closeListeners()
			return err
		}
		reactors[i] = &subReactor{id: i, server: s, poller: p}
	}

	s.lnMu.Lock()
	s.listeners = listeners
	s.mainPoller = mainP
	s.reactors = reactors
	s.lnMu.Unlock()

	for _, r := range reactors {
		s.wg.Add(1)
		go r.loop()
	}

	for fd := range listeners {
		if err := mainP.AddRead(fd); err != nil {
			_ = s.Close()
			return err
		}
	}

	return s.acceptLoop()
}

// Close stops the server, closes the listening sockets and all connections.
func (s *Server) Close() error {
	if !s.closing.CompareAndSwap(false, true) {
		return nil
	}
	s.lnMu.Lock()
	mp := s.mainPoller
	listeners := s.listeners
	reactors := s.reactors
	s.lnMu.Unlock()

	for fd := range listeners {
		if mp != nil {
			_ = mp.Delete(fd)
		}
		_ = closeFD(fd)
	}
	if mp != nil {
		_ = mp.Wake()
	}

	for _, r := range reactors {
		r.conns.forEach(func(c *conn) bool {
			s.closeConn(c)
			return true
		})
		_ = r.poller.Wake()
	}

	s.wg.Wait()

	for _, r := range reactors {
		_ = r.poller.Close()
	}
	if mp != nil {
		_ = mp.Close()
	}
	return nil
}

func (s *Server) acceptLoop() error {
	for !s.closing.Load() {
		events, err := s.mainPoller.Wait(pollTimeout)
		if err != nil {
			if s.closing.Load() {
				return nil
			}
			return err
		}
		for _, ev := range events {
			if laddr, ok := s.listeners[ev.Fd]; ok {
				s.handleAccept(ev.Fd, laddr)
			}
		}
	}
	return nil
}

func (s *Server) handleAccept(lnFD int, laddr net.Addr) {
	for {
		nfd, raddr, err := acceptFD(lnFD)
		if err != nil {
			// EAGAIN / EWOULDBLOCK: backlog drained. Anything else: give up for now.
			return
		}

		idx := s.nextReactor.Add(1) % uint64(len(s.reactors))
		r := s.reactors[idx]

		vc := NewVirtualConn(laddr, raddr)
		c := &conn{
			fd:      nfd,
			vc:      vc,
			server:  s,
			reactor: r,
		}
		vc.attachConn(nfd, c)

		r.conns.store(nfd, c)
		if err := r.poller.AddRead(nfd); err != nil {
			s.closeConn(c)
			continue
		}
		// The connection stays idle under poller custody; a worker goroutine is
		// only dispatched once a complete HTTP header has arrived.
	}
}

// ---------------------------------------------------------------------------
// Reactor loop
// ---------------------------------------------------------------------------

func (r *subReactor) loop() {
	defer r.server.wg.Done()
	defer r.runPending(true)
	r.rbuf = make([]byte, reactorBufSize)

	for !r.server.closing.Load() {
		r.runPending(false)

		r.waiting.Store(true)
		if r.pendCount.Load() > 0 {
			// Work arrived between runPending and Wait; don't block.
			r.waiting.Store(false)
			continue
		}
		events, err := r.poller.Wait(pollTimeout)
		r.waiting.Store(false)
		if err != nil {
			if r.server.closing.Load() {
				return
			}
			continue
		}
		for _, ev := range events {
			c := r.conns.get(ev.Fd)
			if c == nil {
				continue
			}
			if ev.Error || ev.Hangup {
				if ev.Readable {
					r.handleRead(c)
				}
				r.server.closeConn(c)
				continue
			}
			if ev.Readable {
				r.handleRead(c)
			}
			if ev.Writable {
				r.handleWrite(c)
			}
		}
	}
}

// runPending executes work queued for this reactor by other goroutines:
// deferred fd closes and WebSocket hand-offs. In final mode (shutdown) only
// closes are processed.
func (r *subReactor) runPending(final bool) {
	if r.pendCount.Load() == 0 {
		return
	}
	r.pendMu.Lock()
	wsq := r.pendWS
	clq := r.pendClose
	r.pendWS = nil
	r.pendClose = nil
	r.pendCount.Store(0)
	r.pendMu.Unlock()

	for _, fd := range clq {
		_ = r.poller.Delete(fd)
		_ = closeFD(fd)
	}
	if final {
		return
	}
	for _, c := range wsq {
		if c.state.Load() == connStateWSEventDriven {
			r.server.drainWS(c)
		}
	}
}

func (r *subReactor) enqueue(fn func()) {
	r.pendMu.Lock()
	fn()
	r.pendCount.Add(1)
	r.pendMu.Unlock()
	if r.waiting.Load() {
		_ = r.poller.Wake()
	}
}

// scheduleWS asks the reactor to drain already-buffered frames of a connection
// that just became event-driven.
func (r *subReactor) scheduleWS(c *conn) {
	r.enqueue(func() { r.pendWS = append(r.pendWS, c) })
}

// scheduleClose defers the actual close(2) to the reactor goroutine so that an
// fd number is never reused while an event batch referencing it is in flight.
func (r *subReactor) scheduleClose(fd int) {
	r.enqueue(func() { r.pendClose = append(r.pendClose, fd) })
}

// armWrite registers write interest for c exactly once until the reactor
// drains the outbound queue.
func (r *subReactor) armWrite(c *conn) {
	if c.closed.Load() {
		return
	}
	if c.writeArmed.CompareAndSwap(false, true) {
		_ = r.poller.ModReadWrite(c.fd)
	}
}

func (r *subReactor) handleRead(c *conn) {
	if c.closeAfterFlush.Load() {
		r.discardRead(c)
		return
	}
	buf := r.rbuf
	for {
		n, err := readFD(c.fd, buf)
		if n > 0 {
			if c.state.Load() == connStateWSEventDriven {
				if !r.server.feedWS(c, buf[:n]) {
					return
				}
			} else {
				c.vc.FeedInput(buf[:n])
			}
		}
		if err != nil {
			if isWouldBlock(err) {
				break
			}
			if errors.Is(err, syscall.EINTR) {
				continue
			}
			c.vc.FeedError(err)
			r.server.closeConnWithErr(c, err)
			return
		}
		if n == 0 {
			c.vc.FeedEOF()
			r.server.closeConn(c)
			return
		}
		if n < len(buf) {
			// Edge-triggered: a short read means the socket buffer is empty; any
			// later data raises a new event, so skip the extra EAGAIN syscall.
			break
		}
	}
	if c.state.Load() == connStateWSEventDriven {
		if c.vc.InputLen() > 0 {
			r.server.drainWS(c)
		}
		return
	}
	r.server.checkAndDispatch(c)
}

// discardRead drains and drops inbound bytes on a connection that is closing
// after its outbound queue flushes.
func (r *subReactor) discardRead(c *conn) {
	for {
		n, err := readFD(c.fd, r.rbuf)
		if err != nil {
			if isWouldBlock(err) || errors.Is(err, syscall.EINTR) {
				return
			}
			r.server.finishClose(c)
			return
		}
		if n == 0 {
			r.server.finishClose(c)
			return
		}
		if n < len(r.rbuf) {
			return
		}
	}
}

func (r *subReactor) handleWrite(c *conn) {
	pending, err := c.vc.flushOut()
	if err != nil {
		r.server.closeConnWithErr(c, err)
		return
	}
	if pending {
		return // interest stays armed; kernel will notify when writable again
	}
	_ = r.poller.ModRead(c.fd)
	c.writeArmed.Store(false)
	if c.closeAfterFlush.Load() {
		r.server.finishClose(c)
		return
	}
	// A writer may have queued data between flushOut and the reset above.
	if c.vc.PendingWrite() {
		r.armWrite(c)
	}
}

// ---------------------------------------------------------------------------
// Dispatch
// ---------------------------------------------------------------------------

// checkAndDispatch is called on the reactor after inbound bytes were buffered.
func (s *Server) checkAndDispatch(c *conn) {
	switch c.state.Load() {
	case connStateWSEventDriven:
		s.drainWS(c)
		return
	case connStateIdle:
	default:
		return
	}

	c.mu.Lock()
	if c.state.Load() != connStateIdle {
		c.mu.Unlock()
		return
	}

	if s.TLSConfig != nil {
		if c.vc.HasBufferedInput() {
			c.state.Store(connStateWorking)
			s.wg.Add(1)
			go s.serveConn(c)
		}
		c.mu.Unlock()
		return
	}

	if c.vc.HasCompleteHeader() {
		c.state.Store(connStateWorking)
		s.wg.Add(1)
		c.mu.Unlock()
		go s.serveConn(c)
		return
	}
	// Defend against slow/malicious connections buffering large data without \r\n\r\n.
	tooLarge := c.vc.InputLen() > maxHeaderBuffer
	c.mu.Unlock()
	if tooLarge {
		s.closeConn(c)
	}
}

// feedWS handles bytes just read from the socket for an event-driven WebSocket
// connection. When no partial frame is pending, frames are parsed and dispatched
// straight out of the reactor read buffer (zero copy); only an incomplete tail is
// retained. Returns false if the connection was closed.
func (s *Server) feedWS(c *conn, data []byte) bool {
	vc := c.vc
	if vc.InputLen() == 0 {
		n, err := s.dispatchWSFrames(c, data)
		if err != nil {
			s.closeConnWithErr(c, err)
			return false
		}
		if c.closed.Load() {
			return false
		}
		if n < len(data) {
			vc.FeedInput(data[n:])
		}
		return true
	}
	vc.FeedInput(data)
	return s.drainWS(c)
}

// drainWS dispatches complete frames buffered in the VirtualConn. Only the
// reactor goroutine calls it. Returns false if the connection was closed.
func (s *Server) drainWS(c *conn) bool {
	if c.closeAfterFlush.Load() {
		return false
	}
	vc := c.vc
	buf := vc.inputView()
	if len(buf) == 0 {
		return true
	}
	n, err := s.dispatchWSFrames(c, buf)
	if err != nil {
		s.closeConnWithErr(c, err)
		return false
	}
	vc.consumeInput(n)
	return !c.closed.Load()
}

// dispatchWSFrames parses complete frames from data and invokes the handler for
// each. It returns the number of bytes consumed. Payloads are unmasked in place
// and passed as sub-slices of data: they are only valid during the callback.
func (s *Server) dispatchWSFrames(c *conn, data []byte) (int, error) {
	off := 0
	for off < len(data) {
		h, hlen, ok, err := parseWSHeader(data[off:])
		if err != nil {
			return off, err
		}
		if !ok {
			break
		}
		total := hlen + int(h.Length)
		if len(data)-off < total {
			break
		}
		start, end := off+hlen, off+total
		payload := data[start:end:end] // capped so a handler append cannot clobber the next frame
		if h.Masked {
			ws.Cipher(payload, h.Mask, 0)
		}
		off = end
		if !s.handleWSFrame(c, h, payload) {
			return off, nil
		}
	}
	return off, nil
}

// handleWSFrame processes one frame on the reactor. Returns false if the
// connection is closed or closing.
func (s *Server) handleWSFrame(c *conn, h ws.Header, payload []byte) bool {
	switch h.OpCode {
	case ws.OpClose:
		body := closeNormalBody[:]
		if len(payload) >= 2 {
			body = payload[:2] // echo the peer's status code
		}
		_ = writeWSFrame(c.vc, ws.OpClose, body)
		s.closeConnGraceful(c, nil)
		return false
	case ws.OpPing:
		_ = writeWSFrame(c.vc, ws.OpPong, payload)
		return !c.closed.Load()
	case ws.OpPong:
		return true
	}

	handler := c.wsHandler
	if handler != nil {
		handler.OnMessage(byte(h.OpCode), payload)
	}
	return !c.closed.Load() && !c.closeAfterFlush.Load()
}

// ---------------------------------------------------------------------------
// Close paths
// ---------------------------------------------------------------------------

// closeConnGraceful closes c after its outbound queue has been flushed by the
// reactor; if nothing is queued it closes immediately.
func (s *Server) closeConnGraceful(c *conn, err error) {
	if c.closed.Load() {
		return
	}
	if !c.vc.PendingWrite() {
		s.closeConnWithErr(c, err)
		return
	}
	c.mu.Lock()
	c.closeErr = err
	c.mu.Unlock()
	c.closeAfterFlush.Store(true)
	c.reactor.armWrite(c)
}

// finishClose completes a graceful close with the error recorded earlier.
func (s *Server) finishClose(c *conn) {
	c.mu.Lock()
	err := c.closeErr
	c.mu.Unlock()
	s.closeConnWithErr(c, err)
}

func (s *Server) closeConnWithErr(c *conn, err error) {
	if !c.closed.CompareAndSwap(false, true) {
		return
	}
	c.mu.Lock()
	c.state.Store(connStateClosed)
	handler := c.wsHandler
	c.wsHandler = nil
	c.mu.Unlock()

	c.reactor.conns.delete(c.fd)
	_ = c.vc.Close()

	if handler != nil {
		handler.OnClose(err)
	}

	if s.closing.Load() {
		// Reactor may already be gone; close inline.
		_ = c.reactor.poller.Delete(c.fd)
		_ = closeFD(c.fd)
		return
	}
	c.reactor.scheduleClose(c.fd)
}

func (s *Server) closeConn(c *conn) {
	s.closeConnWithErr(c, io.EOF)
}

// ---------------------------------------------------------------------------
// HTTP worker
// ---------------------------------------------------------------------------

func (s *Server) serveConn(c *conn) {
	defer s.wg.Done()

	var rw net.Conn = c.vc
	if s.TLSConfig != nil {
		tlsConn := tls.Server(c.vc, s.TLSConfig)
		if s.ReadTimeout > 0 {
			_ = tlsConn.SetReadDeadline(time.Now().Add(s.ReadTimeout))
		}
		if err := tlsConn.Handshake(); err != nil {
			s.closeConn(c)
			return
		}
		rw = tlsConn
	}

	reader := readerPool.Get().(*bufio.Reader)
	reader.Reset(rw)
	pooled := true
	defer func() {
		if pooled {
			reader.Reset(nil)
			readerPool.Put(reader)
		}
	}()

	for {
		if s.ReadTimeout > 0 {
			_ = rw.SetReadDeadline(time.Now().Add(s.ReadTimeout))
		}
		req, err := http.ReadRequest(reader)
		if err != nil {
			s.closeConnGraceful(c, io.EOF)
			return
		}
		req.RemoteAddr = c.vc.RemoteAddr().String()

		if s.WriteTimeout > 0 {
			_ = rw.SetWriteDeadline(time.Now().Add(s.WriteTimeout))
		}

		w := newResponseWriter(rw, reader)
		w.c = c
		if req.Close || strings.EqualFold(req.Header.Get("Connection"), "close") {
			w.SetClose(true)
		}
		s.Handler.ServeHTTP(w, req)
		if w.Hijacked() {
			// The hijacker (or event-driven WebSocket) now owns the reader.
			pooled = false
			c.mu.Lock()
			if c.state.Load() == connStateWSAttached {
				// Hand the connection to the reactor: from here on frames are
				// parsed and dispatched inline on the event loop.
				c.state.Store(connStateWSEventDriven)
				c.mu.Unlock()
				reader.Reset(nil)
				readerPool.Put(reader)
				pooled = false
				w.releaseBuffers()
				c.vc.CompactOrRelease()
				if c.vc.InputLen() > 0 {
					c.reactor.scheduleWS(c)
				}
				return
			}
			c.state.Store(connStateHijacked)
			c.mu.Unlock()
			return
		}
		_ = w.finish()
		_, _ = io.Copy(io.Discard, req.Body)
		_ = req.Body.Close()

		if req.Close || strings.EqualFold(w.header.Get("Connection"), "close") {
			s.closeConnGraceful(c, io.EOF)
			return
		}

		// Return any read-ahead bytes held by the bufio.Reader to the VirtualConn.
		if reader.Buffered() > 0 {
			rem := make([]byte, reader.Buffered())
			_, _ = io.ReadFull(reader, rem)
			c.vc.UnshiftInput(rem)
		}

		c.mu.Lock()
		if c.state.Load() == connStateClosed {
			c.mu.Unlock()
			return
		}

		if s.TLSConfig == nil && c.vc.HasCompleteHeader() {
			// Pipelined request already buffered: keep this worker.
			c.mu.Unlock()
			reader.Reset(rw)
			continue
		}

		if s.TLSConfig != nil {
			c.mu.Unlock()
			if s.IdleTimeout > 0 {
				_ = rw.SetReadDeadline(time.Now().Add(s.IdleTimeout))
				continue
			}
			s.closeConnGraceful(c, io.EOF)
			return
		}

		// Nothing pending: return the connection to poller custody and release
		// this goroutine. Any partially written response is flushed by the
		// reactor on write readiness.
		c.state.Store(connStateIdle)
		c.mu.Unlock()
		return
	}
}

// ListenAndServe is a convenience helper matching net/http style.
func ListenAndServe(addr string, handler http.Handler) error {
	s := &Server{Addr: addr, Handler: handler}
	return s.ListenAndServe()
}

// ListenAndServeTLS is a convenience helper matching net/http style.
func ListenAndServeTLS(addr, certFile, keyFile string, handler http.Handler) error {
	s := &Server{Addr: addr, Handler: handler}
	return s.ListenAndServeTLS(certFile, keyFile)
}

// ErrServerClosed is returned when the server has been closed.
var ErrServerClosed = fmt.Errorf("fnet: server closed")
