// Package reactor is the event-driven I/O core: an acceptor, a set of event
// loops, and the connections they own. It moves bytes and nothing else;
// protocols plug in through Handler.
package reactor

import (
	"net"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"github.com/linfeip/fnet/internal/netpoll"
)

const (
	pollTimeout   = time.Second
	acceptBackoff = 50 * time.Millisecond // retry delay after EMFILE and friends
)

// Listener is a listening socket handed to the engine.
type Listener struct {
	FD   int
	Addr net.Addr
}

// Opener is implemented by a Handler that sets up each connection the engine
// accepts for it (e.g. arms a close deadline). OnOpen runs on the accept loop
// before the connection's first event.
type Opener interface {
	OnOpen(c *Conn)
}

// Config tunes an Engine. Zero fields take the defaults.
type Config struct {
	// Loops is the number of event loops; 0 means runtime.GOMAXPROCS(0).
	Loops int
	// KeepAlive is applied to every accepted connection; the zero value leaves
	// the platform default.
	KeepAlive netpoll.KeepAlive
	// MaxOutbound caps a connection's queued output; 0 means DefaultMaxOutbound.
	MaxOutbound int
}

// Engine accepts connections on its listeners and spreads them round-robin
// over its event loops. Every new connection starts with the engine's Handler.
type Engine struct {
	listeners   []Listener
	acceptor    netpoll.Poller
	loops       []*Loop
	handler     Handler
	opener      Opener // the handler, if it sets up new connections
	keepAlive   netpoll.KeepAlive
	maxOutbound int
	table       table
	next        atomic.Uint64
	noAccept    atomic.Bool    // StopAccept or Close: the accept loop is done
	closing     atomic.Bool    // Close: the event loops are done
	wg          sync.WaitGroup // loop goroutines

	mu            sync.Mutex
	serving       chan struct{} // closed when Serve returns; nil until Serve starts
	acceptStopped bool
	closed        bool
}

// New starts the event loops for the given listeners. The engine owns the
// listener fds from here on, including on error.
func New(cfg Config, listeners []Listener, h Handler) (*Engine, error) {
	e := &Engine{
		listeners:   listeners,
		handler:     h,
		keepAlive:   cfg.KeepAlive,
		maxOutbound: cfg.MaxOutbound,
	}
	e.opener, _ = h.(Opener)
	if e.maxOutbound <= 0 {
		e.maxOutbound = DefaultMaxOutbound
	}
	n := cfg.Loops
	if n <= 0 {
		n = runtime.GOMAXPROCS(0)
	}
	fail := func(err error) (*Engine, error) {
		for _, l := range e.loops {
			_ = l.poller.Close()
		}
		if e.acceptor != nil {
			_ = e.acceptor.Close()
		}
		for _, ln := range listeners {
			_ = netpoll.Close(ln.FD)
		}
		return nil, err
	}
	if netpoll.KeepAliveInherited && e.keepAlive != (netpoll.KeepAlive{}) {
		// Accepted sockets copy the listener's settings: no system call per accept.
		for _, ln := range listeners {
			if err := netpoll.SetKeepAlive(ln.FD, e.keepAlive); err != nil {
				return fail(err)
			}
		}
	}
	var err error
	if e.acceptor, err = netpoll.NewPoller(); err != nil {
		return fail(err)
	}
	for _, ln := range listeners {
		if err := e.acceptor.Add(ln.FD); err != nil {
			return fail(err)
		}
	}
	for i := 0; i < n; i++ {
		p, err := netpoll.NewPoller()
		if err != nil {
			return fail(err)
		}
		l := &Loop{eng: e, poller: p}
		l.wheel.cur = time.Now().UnixNano() / tickNanos
		e.loops = append(e.loops, l)
	}
	e.wg.Add(len(e.loops))
	for _, l := range e.loops {
		go l.run()
	}
	return e, nil
}

// Serve runs the accept loop until StopAccept or Close. It returns nil then.
func (e *Engine) Serve() error {
	e.mu.Lock()
	if e.acceptStopped {
		e.mu.Unlock()
		return nil
	}
	e.serving = make(chan struct{})
	done := e.serving
	e.mu.Unlock()
	defer close(done)

	retry := false
	for !e.noAccept.Load() {
		timeout := pollTimeout
		if retry {
			timeout = acceptBackoff
		}
		events, err := e.acceptor.Wait(timeout)
		if err != nil {
			if e.noAccept.Load() {
				return nil
			}
			return err
		}
		if retry {
			retry = false
			for i := range e.listeners {
				retry = e.accept(&e.listeners[i]) || retry
			}
		}
		for _, ev := range events {
			for i := range e.listeners {
				if e.listeners[i].FD == ev.Fd {
					retry = e.accept(&e.listeners[i]) || retry
				}
			}
		}
	}
	return nil
}

// accept drains ln's backlog. It reports whether it stopped on an error other
// than an empty backlog (e.g. out of fds), in which case the caller retries
// after a short delay instead of waiting for an edge that may never come.
func (e *Engine) accept(ln *Listener) bool {
	for {
		fd, raddr, err := netpoll.Accept(ln.FD)
		if err != nil {
			return !netpoll.IsAgain(err)
		}
		if e.noAccept.Load() {
			_ = netpoll.Close(fd)
			return false
		}
		if !netpoll.KeepAliveInherited && e.keepAlive != (netpoll.KeepAlive{}) {
			_ = netpoll.SetKeepAlive(fd, e.keepAlive)
		}
		l := e.loops[e.next.Add(1)%uint64(len(e.loops))]
		c := newConn(fd, l, ln, raddr, e.handler)
		e.table.store(fd, c)
		if e.opener != nil {
			e.opener.OnOpen(c)
		}
		if err := l.poller.Add(fd); err != nil {
			c.abort(err)
		}
	}
}

// StopAccept closes the listeners and stops accepting; established
// connections and the event loops keep running until Close. It waits for the
// accept loop to exit, so no connection is accepted after it returns.
// StopAccept is idempotent and safe before Serve.
func (e *Engine) StopAccept() {
	e.mu.Lock()
	if e.acceptStopped {
		e.mu.Unlock()
		return
	}
	e.acceptStopped = true
	serving := e.serving
	e.mu.Unlock()

	e.noAccept.Store(true)
	_ = e.acceptor.Wake()
	if serving != nil {
		<-serving
	}
	for _, ln := range e.listeners {
		_ = netpoll.Close(ln.FD)
	}
	_ = e.acceptor.Close()
}

// ForEach calls fn for every open connection. fn runs on the caller's
// goroutine and must not block.
func (e *Engine) ForEach(fn func(c *Conn)) { e.table.forEach(fn) }

// Handler returns the handler new connections start with.
func (e *Engine) Handler() Handler { return e.handler }

// Close stops accepting, closes every connection, and waits for the event
// loops to exit. Handlers still running on other goroutines see their
// connection closed. Close is idempotent and safe before Serve.
func (e *Engine) Close() {
	e.mu.Lock()
	if e.closed {
		e.mu.Unlock()
		return
	}
	e.closed = true
	e.mu.Unlock()

	e.StopAccept()
	e.closing.Store(true)
	e.table.forEach(func(c *Conn) { c.abort(net.ErrClosed) })
	for _, l := range e.loops {
		_ = l.poller.Wake()
	}
	e.wg.Wait()
	for _, l := range e.loops {
		_ = l.poller.Close()
	}
}
