package reactor

import (
	"errors"
	"net"
	"sync"
	"time"

	"github.com/linfeip/fnet/internal/netpoll"
)

// The public servers (fnet.Server, fhttp.Server) share their plumbing here:
// opening listeners, running one engine, and stopping it from any goroutine.

// ListenFunc creates a listener, like net.Listen.
type ListenFunc func(network, addr string) (net.Listener, error)

// Listen opens a TCP listener for addr, then for each of addrs; an empty addr
// is only used when addrs is empty (it means ":0"-style defaults, like
// net.Listen). listen, when non-nil, creates the listeners (e.g. for socket
// activation); by default they are opened like net.Listen. On error the
// listeners opened so far are closed.
func Listen(addr string, addrs []string, listen ListenFunc) ([]Listener, error) {
	if addr != "" || len(addrs) == 0 {
		addrs = append([]string{addr}, addrs...)
	}
	var lns []Listener
	for _, a := range addrs {
		var (
			fd    int
			laddr net.Addr
			err   error
		)
		if listen != nil {
			var ln net.Listener
			if ln, err = listen("tcp", a); err == nil {
				fd, laddr, err = netpoll.FromListener(ln)
			}
		} else {
			fd, laddr, err = netpoll.Listen("tcp", a)
		}
		if err != nil {
			for _, ln := range lns {
				_ = netpoll.Close(ln.FD)
			}
			return nil, err
		}
		lns = append(lns, Listener{FD: fd, Addr: laddr})
	}
	return lns, nil
}

// Keep-alive defaults of the public servers: a peer that vanished without a
// FIN or RST (power loss, NAT expiry, a dropped mobile link) is found within
// DefaultKeepAliveIdle + DefaultKeepAliveCount*DefaultKeepAliveInterval. With
// a million idle connections that is about 17k small probe packets a second.
const (
	DefaultKeepAliveIdle     = 60 * time.Second
	DefaultKeepAliveInterval = 15 * time.Second
	DefaultKeepAliveCount    = 4
)

// KeepAliveFor turns a public KeepAlive setting into probe timings: 0 means
// the defaults, a negative d turns keep-alive off, and a positive d is the idle
// time before probing (probes then follow every 15s, or every d if shorter).
func KeepAliveFor(d time.Duration) netpoll.KeepAlive {
	switch {
	case d < 0:
		return netpoll.KeepAlive{Idle: -1}
	case d == 0:
		d = DefaultKeepAliveIdle
	}
	return netpoll.KeepAlive{
		Idle:     d,
		Interval: min(d, DefaultKeepAliveInterval),
		Count:    DefaultKeepAliveCount,
	}
}

// errServing is returned by Runner.Run while the runner already serves.
var errServing = errors.New("fnet: server is already serving")

// Runner owns the engine of one public server, so Close and Shutdown can reach
// it from any goroutine, before, during or after Run.
type Runner struct {
	mu      sync.Mutex
	stopped bool
	engine  *Engine
}

// Run opens the listeners, starts an engine for h and runs its accept loop. It
// returns nil once the runner is stopped (before or while serving) and any
// other failure as is.
func (r *Runner) Run(cfg Config, addr string, addrs []string, listen ListenFunc, h Handler) error {
	r.mu.Lock()
	stopped, serving := r.stopped, r.engine != nil
	r.mu.Unlock()
	switch {
	case stopped:
		return nil
	case serving:
		return errServing
	}
	lns, err := Listen(addr, addrs, listen)
	if err != nil {
		return err
	}
	eng, err := New(cfg, lns, h)
	if err != nil {
		return err
	}
	// The sockets are already accepting, so Stop may have run meanwhile.
	r.mu.Lock()
	if r.stopped || r.engine != nil {
		stopped := r.stopped
		r.mu.Unlock()
		eng.Close()
		if stopped {
			return nil
		}
		return errServing
	}
	r.engine = eng
	r.mu.Unlock()
	return eng.Serve()
}

// Stop marks the runner stopped, so Run returns (or never starts serving), and
// returns the running engine for the caller to shut down; nil if none.
func (r *Runner) Stop() *Engine {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.stopped = true
	return r.engine
}
