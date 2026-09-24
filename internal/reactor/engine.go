// Package reactor is the event-driven I/O core: an acceptor, a set of event
// loops, and the connections they own. It moves bytes and nothing else;
// protocols plug in through Handler.
package reactor

import (
	"net"
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

// Engine accepts connections on its listeners and spreads them round-robin
// over its event loops. Every new connection starts with the engine's Handler.
type Engine struct {
	listeners []Listener
	acceptor  netpoll.Poller
	loops     []*Loop
	handler   Handler
	opener    Opener // the handler, if it sets up new connections
	table     table
	next      atomic.Uint64
	closing   atomic.Bool
	wg        sync.WaitGroup // loop goroutines

	mu      sync.Mutex
	serving chan struct{} // closed when Serve returns; nil until Serve starts
	closed  bool
}

// New starts n event loops for the given listeners. The engine owns the
// listener fds from here on, including on error.
func New(n int, listeners []Listener, h Handler) (*Engine, error) {
	e := &Engine{listeners: listeners, handler: h}
	e.opener, _ = h.(Opener)
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
	var err error
	if e.acceptor, err = netpoll.NewPoller(); err != nil {
		return fail(err)
	}
	for _, ln := range listeners {
		if err := e.acceptor.Add(ln.FD); err != nil {
			return fail(err)
		}
	}
	for i := 0; i < max(n, 1); i++ {
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

// Serve runs the accept loop until Close. It returns nil after Close.
func (e *Engine) Serve() error {
	e.mu.Lock()
	if e.closed {
		e.mu.Unlock()
		return nil
	}
	e.serving = make(chan struct{})
	done := e.serving
	e.mu.Unlock()
	defer close(done)

	retry := false
	for !e.closing.Load() {
		timeout := pollTimeout
		if retry {
			timeout = acceptBackoff
		}
		events, err := e.acceptor.Wait(timeout)
		if err != nil {
			if e.closing.Load() {
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
		if e.closing.Load() {
			_ = netpoll.Close(fd)
			return false
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

// Close stops accepting, closes the listeners and every connection, and waits
// for the event loops to exit. Handlers still running on other goroutines see
// their connection closed. Close is idempotent and safe before Serve.
func (e *Engine) Close() {
	e.mu.Lock()
	if e.closed {
		e.mu.Unlock()
		return
	}
	e.closed = true
	serving := e.serving
	e.mu.Unlock()

	e.closing.Store(true)
	_ = e.acceptor.Wake()
	if serving != nil {
		<-serving
	}
	for _, ln := range e.listeners {
		_ = netpoll.Close(ln.FD)
	}
	_ = e.acceptor.Close()

	e.table.forEach(func(c *Conn) { c.abort(net.ErrClosed) })
	for _, l := range e.loops {
		_ = l.poller.Wake()
	}
	e.wg.Wait()
	for _, l := range e.loops {
		_ = l.poller.Close()
	}
}
