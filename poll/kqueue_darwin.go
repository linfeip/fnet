//go:build darwin

package poll

import (
	"os"
	"runtime"
	"sync/atomic"

	"golang.org/x/sys/unix"
)

// zeroTimeout is the timeout used for non-blocking polling.
var zeroTimeout unix.Timespec

// Poller is a multiplexer based on kqueue.
type Poller struct {
	kqueueFd int
	waking   atomic.Bool // coalesces concurrent Wake calls, avoiding triggering the user event repeatedly
	events   []unix.Kevent_t
	timeout  *unix.Timespec // &zeroTimeout when the previous round had events (poll non-blocking first), otherwise nil (blocking)
}

// New creates a Poller.
func New() (*Poller, error) {
	kq, err := unix.Kqueue()
	if err != nil {
		return nil, os.NewSyscallError("kqueue", err)
	}
	unix.CloseOnExec(kq)
	p := &Poller{kqueueFd: kq, events: make([]unix.Kevent_t, 1024)}
	// Register the user event (EV_CLEAR: reset automatically once retrieved), used to wake up from another goroutine.
	if err := p.ctl(0, unix.EVFILT_USER, unix.EV_ADD|unix.EV_CLEAR, 0); err != nil {
		unix.Close(kq)
		return nil, err
	}
	return p, nil
}

// Close releases the fd the Poller occupies.
func (p *Poller) Close() error { return os.NewSyscallError("close", unix.Close(p.kqueueFd)) }

// AddRead registers fd, watching for readable events.
func (p *Poller) AddRead(fd int) error { return p.ctl(fd, unix.EVFILT_READ, unix.EV_ADD, 0) }

// AddEdge registers a connection fd: edge-triggered (EV_CLEAR: the event is reset once retrieved), watching readable and
// writable at once, the same as the Go runtime's netpoller.
// Edge-triggered mode notifies only once per state change: after a readable event, the fd counts as drained only once
// EAGAIN is read (or the buffer was not filled; for a closed peer see EventHup); another writable event only comes after
// writing has hit EAGAIN. Registration is dropped automatically when the fd is closed.
func (p *Poller) AddEdge(fd int) error {
	changes := [2]unix.Kevent_t{
		{Ident: uint64(fd), Filter: unix.EVFILT_READ, Flags: unix.EV_ADD | unix.EV_CLEAR},
		{Ident: uint64(fd), Filter: unix.EVFILT_WRITE, Flags: unix.EV_ADD | unix.EV_CLEAR},
	}
	_, err := unix.Kevent(p.kqueueFd, changes[:], nil, nil)
	return os.NewSyscallError("kevent", err)
}

// Delete deregisters the read and write filters of fd.
func (p *Poller) Delete(fd int) error {
	// EV_RECEIPT: the result of every change is written into results instead of interrupting the batch;
	// a write filter that was never registered gets ENOENT, which can simply be ignored.
	changes := [2]unix.Kevent_t{
		{Ident: uint64(fd), Filter: unix.EVFILT_READ, Flags: unix.EV_DELETE | unix.EV_RECEIPT},
		{Ident: uint64(fd), Filter: unix.EVFILT_WRITE, Flags: unix.EV_DELETE | unix.EV_RECEIPT},
	}
	var results [2]unix.Kevent_t
	_, err := unix.Kevent(p.kqueueFd, changes[:], results[:], nil)
	return os.NewSyscallError("kevent", err)
}

func (p *Poller) ctl(fd int, filter int16, flags uint16, fflags uint32) error {
	changes := [1]unix.Kevent_t{{Ident: uint64(fd), Filter: filter, Flags: flags, Fflags: fflags}}
	_, err := unix.Kevent(p.kqueueFd, changes[:], nil, nil)
	return os.NewSyscallError("kevent", err)
}

// Wake wakes the event loop blocked in Wait; it may be called from any goroutine.
func (p *Poller) Wake() error {
	if !p.waking.CompareAndSwap(false, true) {
		return nil // there is already a wakeup signal that has not been consumed
	}
	err := p.ctl(0, unix.EVFILT_USER, 0, unix.NOTE_TRIGGER)
	if err != nil {
		p.waking.Store(false)
	}
	return err
}

// Wait waits for ready events and invokes fn for every ready fd; if a Wake signal arrives in the meantime it returns
// woken=true. When interrupted by a signal or when this round has no events it returns (false, nil), and the caller
// should call it in a loop.
//
// The strategy: when the previous round had events, this round polls without blocking first, and if there are no events
// it yields the CPU and then switches to blocking, reducing the thread-switching cost of blocking syscalls while busy.
func (p *Poller) Wait(fn func(fd int, ev Event)) (woken bool, err error) {
	n, err := unix.Kevent(p.kqueueFd, nil, p.events, p.timeout)
	if n <= 0 || err != nil {
		if err != nil && err != unix.EINTR {
			return false, os.NewSyscallError("kevent", err)
		}
		if p.timeout != nil {
			p.timeout = nil
			runtime.Gosched()
		}
		return false, nil
	}
	p.timeout = &zeroTimeout
	for i := range n {
		ev := &p.events[i]
		switch ev.Filter {
		case unix.EVFILT_USER:
			p.waking.Store(false) // reset first and let the caller handle the tasks after, so later Wake calls are not lost
			woken = true
		case unix.EVFILT_READ:
			e := EventRead
			if ev.Flags&unix.EV_EOF != 0 {
				e |= EventHup
			}
			fn(int(ev.Ident), e)
		case unix.EVFILT_WRITE:
			fn(int(ev.Ident), EventWrite)
		}
	}
	return woken, nil
}
