package fnet

import (
	"bufio"
	"context"
	"errors"
	"math"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/linfeip/fnet/internal/reactor"
)

// Defaults for zero Server fields.
const (
	// DefaultMaxMessageSize bounds a message, and the bytes buffered while one
	// is still incomplete.
	DefaultMaxMessageSize = 1 << 20
	// DefaultMaxPendingMessageBytes of received messages waiting for OnMessage
	// pause reading from the connection until they drain to a quarter of it.
	DefaultMaxPendingMessageBytes = 64 << 10
	// DefaultMaxOutboundBytes of output queued for a peer that is not reading
	// make further writes fail with ErrWriteBufferFull.
	DefaultMaxOutboundBytes = reactor.DefaultMaxOutbound
	// DefaultReadTimeout bounds the time from a message's first byte to its
	// last, so a peer cannot hold a partial message open by trickling it.
	DefaultReadTimeout = 30 * time.Second
	// DefaultKeepAlive is the TCP keep-alive idle time: a peer that vanished
	// without closing (power loss, NAT expiry) is dropped about two minutes
	// later.
	DefaultKeepAlive = reactor.DefaultKeepAliveIdle
)

var (
	// ErrServerClosed is returned by ListenAndServe after Close or Shutdown,
	// and passed to OnClose for the connections they close.
	ErrServerClosed = errors.New("fnet: server closed")
	// ErrMessageTooLarge closes a connection whose message, complete or not,
	// exceeds MaxMessageSize.
	ErrMessageTooLarge = errors.New("fnet: message exceeds MaxMessageSize")
)

// Server serves a message protocol over TCP from fnet's event loops.
//
// Split cuts each connection's byte stream into messages; it runs on the event
// loop and must not block. OnOpen, OnMessage and OnClose run on the worker
// pool, one call at a time per connection and in order, so they may block on
// databases or downstream calls without holding up other connections. An idle
// connection holds no goroutine and no buffer.
//
//   - OnOpen runs once, before any OnMessage.
//   - OnMessage gets exactly one message per call; msg is only valid during
//     the call, so copy what must outlive it (decoding it, e.g. with
//     proto.Unmarshal, already does).
//   - OnClose runs once, after the last OnMessage, and always follows OnOpen.
//
// When the peer closes, or half-closes, the messages it sent before are still
// handled and the replies written meanwhile are flushed before the connection
// closes. A close from this side (Conn.Close, a timeout, a limit, a panic,
// Close or Shutdown) drops the messages not handled yet.
type Server struct {
	// Addr is the TCP address to listen on (e.g. ":7001").
	Addr string
	// Addrs lists additional addresses; all share the accept loop and pollers.
	Addrs []string

	// Split frames messages, with bufio.SplitFunc semantics: it gets the input
	// not consumed yet and returns how much to consume and the message, if
	// any. (0, nil, nil) waits for more input, an error closes the connection,
	// and a non-nil empty token is an empty message. It runs on the event loop,
	// concurrently for different connections, so it must be fast, must not
	// block and must not keep data. LengthField covers length-prefixed
	// headers; bufio.ScanLines covers line protocols. Required.
	Split bufio.SplitFunc

	// OnOpen runs on a worker before the connection's first message.
	OnOpen func(c *Conn)
	// OnMessage handles one message on a worker. Required.
	OnMessage func(c *Conn, msg []byte)
	// OnClose runs on a worker when the connection is gone, with the reason:
	// nil after Conn.Close, io.EOF when the peer closed, ErrServerClosed,
	// os.ErrDeadlineExceeded, ErrMessageTooLarge, a *PanicError, or the error
	// Split or the network returned.
	OnClose func(c *Conn, err error)

	// MaxMessageSize bounds a message: 0 means DefaultMaxMessageSize,
	// negative disables the limit.
	MaxMessageSize int
	// MaxPendingMessageBytes of received messages waiting for OnMessage pause
	// reading until they drain to a quarter of it: 0 means
	// DefaultMaxPendingMessageBytes, negative disables backpressure.
	MaxPendingMessageBytes int
	// MaxOutboundBytes caps output queued for a peer that is not reading;
	// writes beyond it fail with ErrWriteBufferFull. 0 means
	// DefaultMaxOutboundBytes, negative disables the cap. Broadcast-heavy
	// servers (games, chat) want it far lower, e.g. 256 KiB, to find stalled
	// peers in seconds instead of minutes.
	MaxOutboundBytes int

	// ReadTimeout bounds the time from a message's first byte to its last:
	// 0 means DefaultReadTimeout, negative means no limit.
	ReadTimeout time.Duration
	// IdleTimeout closes a connection that sent nothing for this long after
	// its last message was handled (a heartbeat timeout). 0 means no limit.
	IdleTimeout time.Duration
	// KeepAlive is the idle time before TCP keep-alive probes start; 0 means
	// DefaultKeepAlive, negative turns probes off.
	KeepAlive time.Duration

	// NumPollers is the number of event loops. Defaults to runtime.GOMAXPROCS(0).
	NumPollers int
	// Listen optionally creates the listeners (e.g. for socket activation, or
	// SO_REUSEPORT to run several servers on one port). By default they are
	// opened like net.Listen.
	Listen func(network, addr string) (net.Listener, error)
	// WorkerPool runs the callbacks (e.g. p.SubmitConn for a *pool.Pool p, or
	// pool.Adapt(ants.Submit)). It is called on an event loop and must not
	// block; an error refuses the task and closes that connection. Defaults
	// to pool.Default().
	WorkerPool func(connID uint64, task func()) error

	run reactor.Runner
	mu  sync.Mutex
	h   *handler
}

// ListenAndServe listens on addr and serves a protocol framed by split with
// onMessage.
func ListenAndServe(addr string, split bufio.SplitFunc, onMessage func(c *Conn, msg []byte)) error {
	return (&Server{Addr: addr, Split: split, OnMessage: onMessage}).ListenAndServe()
}

// ListenAndServe serves until Close or Shutdown and then returns
// ErrServerClosed.
func (s *Server) ListenAndServe() error {
	switch {
	case s.Split == nil:
		return errors.New("fnet: Server.Split is nil")
	case s.OnMessage == nil:
		return errors.New("fnet: Server.OnMessage is nil")
	}
	s.mu.Lock()
	if s.h == nil {
		s.h = newHandler(s)
	}
	h := s.h
	s.mu.Unlock()

	cfg := reactor.Config{
		Loops:       s.NumPollers,
		KeepAlive:   reactor.KeepAliveFor(s.KeepAlive),
		MaxOutbound: s.MaxOutboundBytes,
	}
	if cfg.MaxOutbound < 0 {
		cfg.MaxOutbound = math.MaxInt
	}
	if err := s.run.Run(cfg, s.Addr, s.Addrs, s.Listen, h); err != nil {
		return err
	}
	return ErrServerClosed
}

// Close closes the listeners and every connection immediately: messages not
// handled yet are dropped and queued output is discarded. OnClose still runs
// for every connection, with ErrServerClosed, but Close does not wait for it.
// Close is idempotent.
func (s *Server) Close() error {
	if h := s.handler(); h != nil {
		h.stopping.Store(true)
	}
	if eng := s.run.Stop(); eng != nil {
		eng.Close()
	}
	return nil
}

// Shutdown stops the server gracefully. It stops accepting and stops reading
// from every connection; each connection finishes the messages it has already
// received, flushes the replies, and closes. Shutdown returns once every
// OnClose has returned, so state saved in OnClose is safe when the process
// exits. If ctx ends first, the remaining connections are closed like Close
// and Shutdown returns ctx.Err() without waiting for their OnClose.
//
// ListenAndServe returns ErrServerClosed as soon as Shutdown starts; wait for
// Shutdown to return before exiting.
func (s *Server) Shutdown(ctx context.Context) error {
	h := s.handler()
	if h != nil {
		h.stopping.Store(true)
	}
	eng := s.run.Stop()
	if eng == nil {
		return nil
	}
	eng.StopAccept()
	eng.ForEach(func(rc *reactor.Conn) {
		if c, ok := rc.Handler().(*Conn); ok {
			c.shutdown()
		}
	})
	done := make(chan struct{})
	go func() {
		h.live.Wait()
		close(done)
	}()
	select {
	case <-done:
		eng.Close()
		return nil
	case <-ctx.Done():
		eng.Close()
		return ctx.Err()
	}
}

func (s *Server) handler() *handler {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.h
}

// handler is a Server's settings, resolved when it starts, shared by its
// connections. It is the engine's handler only until OnOpen gives each new
// connection its own Conn.
type handler struct {
	split     bufio.SplitFunc
	onOpen    func(*Conn)
	onMessage func(*Conn, []byte)
	onClose   func(*Conn, error)
	submit    func(connID uint64, task func()) error

	maxMessage  int   // math.MaxInt when unlimited
	maxPending  int   // <= 0: no backpressure
	lowPending  int   // reading resumes at or below this
	readTimeout int64 // nanoseconds; 0: none
	idleTimeout int64 // nanoseconds; 0: none

	stopping atomic.Bool    // Close or Shutdown: closes report ErrServerClosed
	live     sync.WaitGroup // connections whose OnClose has not returned
}

func newHandler(s *Server) *handler {
	h := &handler{
		split:     s.Split,
		onOpen:    s.OnOpen,
		onMessage: s.OnMessage,
		onClose:   s.OnClose,
		submit:    s.WorkerPool,
	}
	switch {
	case s.MaxMessageSize > 0:
		h.maxMessage = s.MaxMessageSize
	case s.MaxMessageSize == 0:
		h.maxMessage = DefaultMaxMessageSize
	default:
		h.maxMessage = math.MaxInt
	}
	h.maxPending = s.MaxPendingMessageBytes
	if h.maxPending == 0 {
		h.maxPending = DefaultMaxPendingMessageBytes
	}
	h.lowPending = h.maxPending / 4
	switch {
	case s.ReadTimeout > 0:
		h.readTimeout = int64(s.ReadTimeout)
	case s.ReadTimeout == 0:
		h.readTimeout = int64(DefaultReadTimeout)
	}
	if s.IdleTimeout > 0 {
		h.idleTimeout = int64(s.IdleTimeout)
	}
	return h
}

// OnOpen runs on the accept loop for every new connection, before its first
// event, and hands it to its own Conn.
func (h *handler) OnOpen(rc *reactor.Conn) {
	c := newConn(rc, h)
	h.live.Add(1)
	rc.Attach(c)
	c.start()
}

// OnData and OnClose complete reactor.Handler; OnOpen replaces the handler of
// every connection before it can see an event, so they never run.
func (h *handler) OnData(_ *reactor.Conn, data []byte) int { return len(data) }

func (h *handler) OnClose(*reactor.Conn, error) {}
