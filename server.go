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
type Server struct {
	// Addr is the TCP address to listen on (e.g. ":8080").
	Addr string
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

	lnMu        sync.Mutex
	lnFD        int
	lnAddr      net.Addr
	mainPoller  Poller
	reactors    []*subReactor
	nextReactor atomic.Uint64
	closing     atomic.Bool
	wg          sync.WaitGroup
}

type subReactor struct {
	id       int
	server   *Server
	poller   Poller
	conns    sync.Map // fd -> *conn
	notifyMu sync.Mutex
	wakeFDs  map[int]struct{} // fds needing write interest
}

const (
	connStateIdle = iota
	connStateWorking
	connStateHijacked
	connStateWSEventDriven
	connStateClosed
)

const maxHeaderBuffer = 64 * 1024 // 64KB max buffered header to prevent memory exhaustion

type conn struct {
	fd      int
	vc      *VirtualConn
	server  *Server
	reactor *subReactor
	once    sync.Once

	mu        sync.Mutex
	state     int
	wsWorking bool
	wsHandler WSHandler
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

func (s *Server) serve(tlsCfg *tls.Config) error {
	if s.Handler == nil {
		s.Handler = http.DefaultServeMux
	}
	if tlsCfg != nil {
		s.TLSConfig = tlsCfg
	}

	fd, addr, err := listenNonblock("tcp", s.Addr)
	if err != nil {
		return err
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
		_ = closeFD(fd)
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
			_ = closeFD(fd)
			return err
		}
		r := &subReactor{
			id:      i,
			server:  s,
			poller:  p,
			wakeFDs: make(map[int]struct{}),
		}
		reactors[i] = r
		s.wg.Add(1)
		go r.loop()
	}

	s.lnMu.Lock()
	s.lnFD = fd
	s.lnAddr = addr
	s.mainPoller = mainP
	s.reactors = reactors
	s.lnMu.Unlock()

	if err := mainP.AddRead(fd); err != nil {
		_ = s.Close()
		return err
	}

	return s.acceptLoop()
}

// Close stops the server and closes the listening socket.
func (s *Server) Close() error {
	if !s.closing.CompareAndSwap(false, true) {
		return nil
	}
	s.lnMu.Lock()
	mp := s.mainPoller
	lnFD := s.lnFD
	reactors := s.reactors
	s.lnMu.Unlock()

	if mp != nil && lnFD > 0 {
		_ = mp.Delete(lnFD)
	}
	if lnFD > 0 {
		_ = closeFD(lnFD)
	}
	if mp != nil {
		_ = mp.Wake()
		_ = mp.Close()
	}

	for _, r := range reactors {
		r.conns.Range(func(key, value any) bool {
			c := value.(*conn)
			s.closeConn(c)
			return true
		})
		_ = r.poller.Wake()
		_ = r.poller.Close()
	}

	s.wg.Wait()
	return nil
}

func (s *Server) acceptLoop() error {
	for !s.closing.Load() {
		events, err := s.mainPoller.Wait(100 * time.Millisecond)
		if err != nil {
			if s.closing.Load() {
				return nil
			}
			return err
		}
		for _, ev := range events {
			if ev.Fd == s.lnFD {
				s.handleAccept()
			}
		}
	}
	return nil
}

func (s *Server) handleAccept() {
	for {
		nfd, raddr, err := acceptFD(s.lnFD)
		if err != nil {
			if errors.Is(err, syscall.EAGAIN) || errors.Is(err, syscall.EWOULDBLOCK) {
				return
			}
			return
		}

		idx := s.nextReactor.Add(1) % uint64(len(s.reactors))
		r := s.reactors[idx]

		vc := NewVirtualConn(s.lnAddr, raddr)
		c := &conn{
			fd:      nfd,
			vc:      vc,
			server:  s,
			reactor: r,
			state:   connStateIdle,
		}
		fd := nfd
		vc.SetWritableCallback(func() {
			r.armWrite(fd)
		})
		vc.SetDirectWrite(func(b []byte) (int, error) {
			return writeFD(fd, b)
		})
		vc.SetDirectWritev(func(iovs [][]byte) (int, error) {
			return writevFD(fd, iovs)
		})

		r.conns.Store(nfd, c)
		if err := r.poller.AddRead(nfd); err != nil {
			s.closeConn(c)
			continue
		}
		// Newly accepted connection remains in connStateIdle.
		// Poller will dispatch a worker goroutine on-demand only when a complete HTTP header arrives.
	}
}

func (r *subReactor) loop() {
	defer r.server.wg.Done()
	buf := make([]byte, 64*1024)
	for !r.server.closing.Load() {
		r.flushWriteInterest()

		events, err := r.poller.Wait(50 * time.Millisecond)
		if err != nil {
			if r.server.closing.Load() {
				return
			}
			continue
		}
		for _, ev := range events {
			v, ok := r.conns.Load(ev.Fd)
			if !ok {
				continue
			}
			c := v.(*conn)
			if ev.Error || ev.Hangup {
				if ev.Readable {
					r.handleRead(c, buf)
				}
				r.server.closeConn(c)
				continue
			}
			if ev.Readable {
				r.handleRead(c, buf)
			}
			if ev.Writable {
				r.handleWrite(c, buf)
			}
		}
	}
}

func (r *subReactor) handleRead(c *conn, buf []byte) {
	for {
		n, err := readFD(c.fd, buf)
		if n > 0 {
			c.vc.FeedInput(buf[:n])
		}
		if err != nil {
			if errors.Is(err, syscall.EAGAIN) || errors.Is(err, syscall.EWOULDBLOCK) {
				r.server.checkAndDispatch(c)
				return
			}
			c.vc.FeedError(err)
			r.server.closeConn(c)
			return
		}
		if n == 0 {
			c.vc.FeedEOF()
			r.server.closeConn(c)
			return
		}
	}
}

func (r *subReactor) handleWrite(c *conn, buf []byte) {
	for {
		n, remaining := c.vc.DrainWrite(buf)
		if n == 0 {
			_ = r.poller.ModRead(c.fd)
			return
		}
		written := 0
		for written < n {
			wn, err := writeFD(c.fd, buf[written:n])
			if wn > 0 {
				written += wn
			}
			if err != nil {
				if errors.Is(err, syscall.EAGAIN) || errors.Is(err, syscall.EWOULDBLOCK) {
					if written < n {
						c.vc.UnshiftWrite(append([]byte(nil), buf[written:n]...))
					}
					_ = r.poller.ModReadWrite(c.fd)
					return
				}
				c.vc.FeedError(err)
				r.server.closeConn(c)
				return
			}
		}
		if !remaining && !c.vc.PendingWrite() {
			_ = r.poller.ModRead(c.fd)
			return
		}
	}
}

func (r *subReactor) armWrite(fd int) {
	r.notifyMu.Lock()
	r.wakeFDs[fd] = struct{}{}
	r.notifyMu.Unlock()
	_ = r.poller.Wake()
}

func (r *subReactor) flushWriteInterest() {
	r.notifyMu.Lock()
	if len(r.wakeFDs) == 0 {
		r.notifyMu.Unlock()
		return
	}
	fds := make([]int, 0, len(r.wakeFDs))
	for fd := range r.wakeFDs {
		fds = append(fds, fd)
	}
	r.wakeFDs = make(map[int]struct{})
	r.notifyMu.Unlock()

	for _, fd := range fds {
		if _, ok := r.conns.Load(fd); !ok {
			continue
		}
		_ = r.poller.ModReadWrite(fd)
	}
}

func (s *Server) checkAndDispatch(c *conn) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.state == connStateClosed {
		return
	}

	if c.state == connStateWSEventDriven {
		if !c.wsWorking && c.vc.HasCompleteWSFrame() {
			c.wsWorking = true
			s.wg.Add(1)
			go s.serveWSConn(c)
		}
		return
	}

	if c.state != connStateIdle {
		return
	}

	if s.TLSConfig != nil {
		if c.vc.HasBufferedInput() {
			c.state = connStateWorking
			s.wg.Add(1)
			go s.serveConn(c)
		}
		return
	}

	// Defend against slow/malicious connections buffering large data without \r\n\r\n
	if c.vc.InputLen() > maxHeaderBuffer && !c.vc.HasCompleteHeader() {
		go s.closeConn(c)
		return
	}

	if c.vc.HasCompleteHeader() {
		c.state = connStateWorking
		s.wg.Add(1)
		go s.serveConn(c)
	}
}

func (s *Server) serveWSConn(c *conn) {
	defer s.wg.Done()

	for {
		c.mu.Lock()
		if c.state != connStateWSEventDriven {
			c.wsWorking = false
			c.mu.Unlock()
			return
		}

		h, payload, ok, err := c.vc.PopWSFrame()
		if err != nil {
			c.wsWorking = false
			c.mu.Unlock()
			s.closeConnWithErr(c, err)
			return
		}
		if !ok {
			c.wsWorking = false
			c.mu.Unlock()
			return
		}
		handler := c.wsHandler
		c.mu.Unlock()

		if h.OpCode == ws.OpClose {
			_ = ws.WriteFrame(c.vc, ws.NewCloseFrame(ws.NewCloseFrameBody(ws.StatusNormalClosure, "")))
			s.flushConnSync(c)
			s.closeConnWithErr(c, nil)
			return
		}

		if h.OpCode == ws.OpPing {
			_ = ws.WriteFrame(c.vc, ws.NewPongFrame(payload))
			continue
		}

		if h.OpCode == ws.OpPong {
			continue
		}

		if handler != nil {
			handler.OnMessage(byte(h.OpCode), payload)
		}
	}
}

func (s *Server) closeConnWithErr(c *conn, err error) {
	c.once.Do(func() {
		c.mu.Lock()
		c.state = connStateClosed
		handler := c.wsHandler
		c.wsHandler = nil
		c.mu.Unlock()

		if handler != nil {
			handler.OnClose(err)
		}

		if c.reactor != nil {
			c.reactor.conns.Delete(c.fd)
			_ = c.reactor.poller.Delete(c.fd)
		}
		_ = c.vc.Close()
		_ = closeFD(c.fd)
	})
}

func (s *Server) closeConn(c *conn) {
	s.closeConnWithErr(c, io.EOF)
}

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

	reader := bufio.NewReader(rw)
	for {
		if s.ReadTimeout > 0 {
			_ = rw.SetReadDeadline(time.Now().Add(s.ReadTimeout))
		}
		req, err := http.ReadRequest(reader)
		if err != nil {
			s.flushConnSync(c)
			s.closeConn(c)
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
			// Connection was hijacked (e.g. WebSocket).
			c.mu.Lock()
			if c.state == connStateWSEventDriven {
				c.mu.Unlock()
				s.flushConnSync(c)
				s.checkAndDispatch(c)
				return
			}
			c.state = connStateHijacked
			c.mu.Unlock()
			// Caller took over connection lifecycle; do not close here.
			return
		}
		_ = w.finish()
		_, _ = io.Copy(io.Discard, req.Body)
		_ = req.Body.Close()

		if req.Close || strings.EqualFold(w.header.Get("Connection"), "close") {
			s.flushConnSync(c)
			s.closeConn(c)
			return
		}

		s.flushConnSync(c)

		// Unshift any excess buffered bytes read by bufio.Reader back to c.vc
		if reader.Buffered() > 0 {
			rem := make([]byte, reader.Buffered())
			_, _ = io.ReadFull(reader, rem)
			c.vc.UnshiftInput(rem)
		}

		// Check if there is an immediately pipelined complete HTTP request
		c.mu.Lock()
		if c.state == connStateClosed {
			c.mu.Unlock()
			return
		}

		if s.TLSConfig == nil && c.vc.HasCompleteHeader() {
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
			s.closeConn(c)
			return
		}

		// No complete request pending. Revert connection to Idle state under Poller custody,
		// and terminate this worker goroutine to avoid idle goroutine holding.
		c.state = connStateIdle
		c.mu.Unlock()
		return
	}
}

// flushConnSync writes any remaining VirtualConn outbound bytes directly to
// the socket so responses are not lost when the worker finishes.
func (s *Server) flushConnSync(c *conn) {
	buf := make([]byte, 32*1024)
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		n, remaining := c.vc.DrainWrite(buf)
		if n == 0 {
			return
		}
		off := 0
		for off < n {
			wn, err := writeFD(c.fd, buf[off:n])
			if wn > 0 {
				off += wn
			}
			if err != nil {
				if errors.Is(err, syscall.EAGAIN) || errors.Is(err, syscall.EWOULDBLOCK) {
					time.Sleep(time.Millisecond)
					continue
				}
				if off < n {
					c.vc.UnshiftWrite(append([]byte(nil), buf[off:n]...))
				}
				return
			}
		}
		if !remaining && !c.vc.PendingWrite() {
			return
		}
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
