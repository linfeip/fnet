package fnet

import (
	"bufio"
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/gobwas/ws"
)

// Server is a high-performance HTTP/HTTPS server driven by a native poller
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

	lnMu     sync.Mutex
	poller   Poller
	lnFD     int
	lnAddr   net.Addr
	conns    sync.Map // fd -> *conn
	closing  atomic.Bool
	wg       sync.WaitGroup
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
	fd     int
	vc     *VirtualConn
	server *Server
	once   sync.Once

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

	p, err := NewPoller()
	if err != nil {
		_ = closeFD(fd)
		return err
	}

	s.lnMu.Lock()
	s.lnFD = fd
	s.lnAddr = addr
	s.poller = p
	s.wakeFDs = make(map[int]struct{})
	s.lnMu.Unlock()

	if err := s.poller.AddRead(fd); err != nil {
		_ = s.poller.Close()
		_ = closeFD(fd)
		return err
	}

	return s.loop()
}

// Close stops the server and closes the listening socket.
func (s *Server) Close() error {
	if !s.closing.CompareAndSwap(false, true) {
		return nil
	}
	s.lnMu.Lock()
	p := s.poller
	lnFD := s.lnFD
	s.lnMu.Unlock()

	if p != nil && lnFD > 0 {
		_ = p.Delete(lnFD)
	}
	if lnFD > 0 {
		_ = closeFD(lnFD)
	}
	s.conns.Range(func(key, value any) bool {
		c := value.(*conn)
		s.closeConn(c)
		return true
	})
	if p != nil {
		_ = p.Close()
	}
	s.wg.Wait()
	return nil
}

func (s *Server) loop() error {
	buf := make([]byte, 32*1024)
	for !s.closing.Load() {
		s.flushWriteInterest()

		events, err := s.poller.Wait(50 * time.Millisecond)
		if err != nil {
			if s.closing.Load() {
				return nil
			}
			return err
		}
		for _, ev := range events {
			if ev.Fd == s.lnFD {
				s.handleAccept()
				continue
			}
			v, ok := s.conns.Load(ev.Fd)
			if !ok {
				continue
			}
			c := v.(*conn)
			if ev.Error || ev.Hangup {
				if ev.Readable {
					s.handleRead(c, buf)
				}
				s.closeConn(c)
				continue
			}
			if ev.Readable {
				s.handleRead(c, buf)
			}
			if ev.Writable {
				s.handleWrite(c, buf)
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
		vc := NewVirtualConn(s.lnAddr, raddr)
		c := &conn{
			fd:     nfd,
			vc:     vc,
			server: s,
			state:  connStateIdle,
		}
		fd := nfd
		vc.SetWritableCallback(func() {
			s.armWrite(fd)
		})
		vc.SetDirectWrite(func(b []byte) (int, error) {
			return writeFD(fd, b)
		})
		s.conns.Store(nfd, c)
		if err := s.poller.AddRead(nfd); err != nil {
			s.closeConn(c)
			continue
		}
		// Newly accepted connection remains in connStateIdle.
		// Poller will dispatch a worker goroutine on-demand only when a complete HTTP header arrives.
	}
}

func (s *Server) handleRead(c *conn, buf []byte) {
	for {
		n, err := readFD(c.fd, buf)
		if n > 0 {
			c.vc.FeedInput(buf[:n])
		}
		if err != nil {
			if errors.Is(err, syscall.EAGAIN) || errors.Is(err, syscall.EWOULDBLOCK) {
				s.checkAndDispatch(c)
				return
			}
			c.vc.FeedError(err)
			s.closeConn(c)
			return
		}
		if n == 0 {
			c.vc.FeedEOF()
			s.closeConn(c)
			return
		}
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
			s.flushConnSync(c)
			continue
		}

		if h.OpCode == ws.OpPong {
			continue
		}

		if handler != nil {
			handler.OnMessage(byte(h.OpCode), payload)
			s.flushConnSync(c)
		}
	}
}

func (s *Server) handleWrite(c *conn, buf []byte) {
	for {
		n, remaining := c.vc.DrainWrite(buf)
		if n == 0 {
			_ = s.poller.ModRead(c.fd)
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
					_ = s.poller.ModReadWrite(c.fd)
					return
				}
				c.vc.FeedError(err)
				s.closeConn(c)
				return
			}
		}
		if !remaining && !c.vc.PendingWrite() {
			_ = s.poller.ModRead(c.fd)
			return
		}
	}
}

func (s *Server) armWrite(fd int) {
	s.notifyMu.Lock()
	s.wakeFDs[fd] = struct{}{}
	s.notifyMu.Unlock()
}

func (s *Server) flushWriteInterest() {
	s.notifyMu.Lock()
	if len(s.wakeFDs) == 0 {
		s.notifyMu.Unlock()
		return
	}
	fds := make([]int, 0, len(s.wakeFDs))
	for fd := range s.wakeFDs {
		fds = append(fds, fd)
	}
	s.wakeFDs = make(map[int]struct{})
	s.notifyMu.Unlock()

	for _, fd := range fds {
		if _, ok := s.conns.Load(fd); !ok {
			continue
		}
		_ = s.poller.ModReadWrite(fd)
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

		s.conns.Delete(c.fd)
		_ = s.poller.Delete(c.fd)
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
			s.flushConnSync(c)
			s.closeConn(c)
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
