//go:build linux || darwin

package fnet

import (
	"context"
	"errors"
	"net"
	"os"
	"runtime"
	"sync"
	"sync/atomic"
	"syscall"

	"github.com/linfeip/fnet/poll"
	"github.com/linfeip/fnet/taskpool"

	"golang.org/x/sys/unix"
)

// reactor is the shared event-loop foundation of Server (TCP) and UDPServer (UDP): the sub-reactors and their workers,
// the default taskpool when no Executor was supplied, and the Serve/Close lifecycle state machine.
type reactor struct {
	executor    func(key int, task func())
	pool        *taskpool.Pool // owned when the caller's Executor was nil; closed by releaseReactor
	subReactors []*subReactor
	openConnsWg sync.WaitGroup // TCP connections or UDP sockets not yet closed; Serve waits for them before stopping workers
	workersWg   sync.WaitGroup // the sub-reactors' workers
	stopping    atomic.Bool    // the workers are to exit

	mu      sync.Mutex
	serving bool
	closed  bool
	err     error         // the fatal error that made the server exit
	closing chan struct{} // closed by shutdown, waking Serve to drain and exit
	done    chan struct{} // closed once Serve has fully exited
}

func (r *reactor) initReactor(numLoops int, executor func(key int, task func())) error {
	if executor == nil {
		r.pool = taskpool.NewDefault()
		executor = r.pool.SubmitTo
	}
	r.executor = executor
	r.closing = make(chan struct{})
	r.done = make(chan struct{})
	for range numLoops {
		p, err := poll.New()
		if err != nil {
			r.releaseReactor()
			return err
		}
		r.subReactors = append(r.subReactors, &subReactor{reactor: r, poller: p})
	}
	return nil
}

func (r *reactor) beginServe() error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed {
		return ErrServerClosed
	}
	if r.serving {
		return errors.New("fnet: Serve called more than once")
	}
	r.serving = true
	return nil
}

func (r *reactor) startWorkers() {
	r.workersWg.Add(len(r.subReactors))
	for _, sub := range r.subReactors {
		w := &reactorWorker{home: sub}
		go w.run()
	}
}

func (r *reactor) stopWorkers() {
	r.stopping.Store(true)
	r.wakeAll()
	r.workersWg.Wait()
}

func (r *reactor) wakeAll() {
	for _, sub := range r.subReactors {
		sub.poller.Wake()
	}
}

func (r *reactor) finishServe() error {
	close(r.done)
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.err != nil {
		return r.err
	}
	return ErrServerClosed
}

// shutdown marks the server as closed and wakes Serve; err is the fatal error that caused the shutdown.
func (r *reactor) shutdown(err error) (first, serving bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed {
		return false, r.serving
	}
	r.closed, r.err = true, err
	close(r.closing)
	return true, r.serving
}

func (r *reactor) closeWith(release func()) error {
	first, serving := r.shutdown(nil)
	if !serving {
		if first {
			release()
		}
		return nil
	}
	<-r.done
	return nil
}

func (r *reactor) releaseReactor() {
	for _, sub := range r.subReactors {
		sub.poller.Close()
	}
	if r.pool != nil {
		r.pool.Close()
	}
}

// subReactor is a Poller and its readiness batch callback, served by one reactorWorker goroutine.
type subReactor struct {
	reactor *reactor
	poller  *poll.Poller
	onBatch func(*poll.Batch)
	active  atomic.Bool // the last poll retrieved events; other sub-reactors' workers help only while it is set
}

// reactorWorker is the goroutine serving a subReactor: it polls its Poller and dispatches ready batches via onBatch.
// When its own sub-reactor has nothing to do it helps active peers, and only when all of them are idle does it block in
// the kernel. It never runs user callbacks itself.
type reactorWorker struct {
	home  *subReactor
	batch poll.Batch
	next  int // where helping the other sub-reactors starts next time, so that it spreads over them
}

// run is the worker's loop until the server stops.
//
// After every round that had something to do the worker gives up its P: a goroutine woken for a task waits on the P of
// the goroutine that woke it, and yielding runs those executor workers at once instead of waiting for stealing or the
// 10ms preemption quantum, while events arriving meanwhile are retrieved together by the next poll.
func (w *reactorWorker) run() {
	home := w.home
	r := home.reactor
	defer r.workersWg.Done()
	for !r.stopping.Load() {
		if w.serve(home) || w.help() {
			runtime.Gosched()
			continue
		}
		// stopWorkers wakes every sub-reactor after setting stopping: a wakeup after the check below ends Block.
		if r.stopping.Load() {
			break
		}
		home.poller.Block(&w.batch)
		if w.batch.Len() > 0 {
			home.active.Store(true)
			home.onBatch(&w.batch)
		}
	}
	// The wakeups for stopping may have been taken by workers of other sub-reactors: pass them on.
	r.wakeAll()
}

func (w *reactorWorker) serve(sub *subReactor) bool {
	woken := sub.poller.Poll(&w.batch)
	if w.batch.Len() == 0 {
		sub.active.Store(false)
		return woken
	}
	sub.active.Store(true)
	sub.onBatch(&w.batch)
	return true
}

func (w *reactorWorker) help() bool {
	subs := w.home.reactor.subReactors
	w.next++
	for i := range subs {
		sub := subs[(w.next+i)%len(subs)]
		if sub == w.home || !sub.active.Load() {
			continue
		}
		if w.serve(sub) {
			return true
		}
	}
	return false
}

// listen creates n sockets on addr ("tcp" or "udp" network), sharing it through SO_REUSEPORT when n > 1, and returns
// the address actually bound; noDelay is passed on to setListenerOptions for TCP sockets.
//
// The first socket is bound like net.Listen's or net.ListenPacket's, without SO_REUSEPORT, so that an address already
// in use is reported as an error rather than shared with another process. It takes the option once bound, letting the
// remaining sockets join the exact port it received (including when port 0 was requested).
func listen(network, addr string, n int, noDelay bool) ([]int, net.Addr, error) {
	fd, laddr, err := listenSocket(network, addr, false, noDelay)
	if err != nil {
		return nil, nil, err
	}
	fds := []int{fd}
	if n > 1 {
		err = os.NewSyscallError("setsockopt", unix.SetsockoptInt(fd, unix.SOL_SOCKET, unix.SO_REUSEPORT, 1))
	}
	for len(fds) < n && err == nil {
		if fd, _, err = listenSocket(network, laddr.String(), true, noDelay); err == nil {
			fds = append(fds, fd)
		}
	}
	if err != nil {
		for _, fd := range fds {
			unix.Close(fd)
		}
		return nil, nil, err
	}
	return fds, laddr, nil
}

// listenSocket creates a TCP listening socket or a UDP socket ("udp" network) with the help of the standard library
// (address resolution, dual stack, SO_REUSEADDR, SO_BROADCAST and backlog are all handled by net), setting TCP listener
// options (see setListenerOptions) and, with reusePort, SO_REUSEPORT before bind; it then duplicates a separate
// non-blocking fd for fnet to manage and closes the standard library wrapper immediately.
func listenSocket(network, addr string, reusePort, noDelay bool) (int, net.Addr, error) {
	lc := net.ListenConfig{Control: func(_, _ string, rc syscall.RawConn) error {
		var err error
		if cerr := rc.Control(func(fd uintptr) {
			if network == "tcp" {
				setListenerOptions(int(fd), noDelay)
			}
			if reusePort {
				err = os.NewSyscallError("setsockopt", unix.SetsockoptInt(int(fd), unix.SOL_SOCKET, unix.SO_REUSEPORT, 1))
			}
		}); cerr != nil {
			return cerr
		}
		return err
	}}
	var (
		socket syscall.Conn
		laddr  net.Addr
	)
	if network == "udp" {
		pc, err := lc.ListenPacket(context.Background(), network, addr)
		if err != nil {
			return -1, nil, err
		}
		defer pc.Close()
		socket, laddr = pc.(*net.UDPConn), pc.LocalAddr()
	} else {
		ln, err := lc.Listen(context.Background(), network, addr)
		if err != nil {
			return -1, nil, err
		}
		defer ln.Close()
		socket, laddr = ln.(*net.TCPListener), ln.Addr()
	}
	rc, err := socket.SyscallConn()
	if err != nil {
		return -1, nil, err
	}
	fd, dupErr := -1, error(nil)
	if err := rc.Control(func(s uintptr) { fd, dupErr = unix.FcntlInt(s, unix.F_DUPFD_CLOEXEC, 0) }); err != nil {
		return -1, nil, err
	}
	if dupErr != nil {
		return -1, nil, os.NewSyscallError("fcntl", dupErr)
	}
	if err := unix.SetNonblock(fd, true); err != nil {
		unix.Close(fd)
		return -1, nil, os.NewSyscallError("setnonblock", err)
	}
	return fd, laddr, nil
}
