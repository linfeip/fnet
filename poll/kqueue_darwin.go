//go:build darwin

package poll

import (
	"os"
	"sync/atomic"

	"golang.org/x/sys/unix"
)

// zeroTimeout is the timeout used for non-blocking polling.
var zeroTimeout unix.Timespec

// Poller is a multiplexer based on kqueue.
type Poller struct {
	kqueueFd int
	waking   atomic.Bool // coalesces concurrent Wake calls, avoiding triggering the user event repeatedly
}

// New creates a Poller, whose waits block the calling thread in the kernel.
func New() (*Poller, error) {
	kq, err := unix.Kqueue()
	if err != nil {
		return nil, os.NewSyscallError("kqueue", err)
	}
	unix.CloseOnExec(kq)
	p := &Poller{kqueueFd: kq}
	// Register the user event (EV_CLEAR: reset automatically once retrieved), used to wake up from another goroutine.
	if err := p.ctl(0, unix.EVFILT_USER, unix.EV_ADD|unix.EV_CLEAR, 0); err != nil {
		unix.Close(kq)
		return nil, err
	}
	return p, nil
}

// Close releases the fd the Poller occupies.
func (p *Poller) Close() error { return os.NewSyscallError("close", unix.Close(p.kqueueFd)) }

// AddListener registers a listening socket, edge-triggered (EV_CLEAR): one notification when connections arrive,
// after which they are accepted until EAGAIN.
func (p *Poller) AddListener(fd int) error {
	return p.ctl(fd, unix.EVFILT_READ, unix.EV_ADD|unix.EV_CLEAR, 0)
}

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

// Wake wakes one goroutine blocked in Block; it may be called from any goroutine.
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

// Batch holds the events one Poll or Block call retrieved; goroutines polling at the same time each need their own.
type Batch struct {
	events [BatchSize]unix.Kevent_t
	count  int
}

// Len returns the number of events in the batch.
func (b *Batch) Len() int { return b.count }

// Event returns the i-th event: the ready fd and its events.
func (b *Batch) Event(i int) (fd int, ev Event) {
	fd, ev, _ = convert(&b.events[i])
	return fd, ev
}

// Poll retrieves up to BatchSize ready events into b without blocking. Any number of goroutines may call Poll and Block
// on the same Poller at the same time, each with its own Batch. woken reports that the call consumed the Wake signal,
// which is not counted as an event. The Poller must not be closed while Poll or Block runs.
func (p *Poller) Poll(b *Batch) (woken bool) { return p.retrieve(b, &zeroTimeout) }

// Block is Poll that blocks the calling thread in the kernel until an event is ready or Wake is called; when interrupted
// by a signal it returns with an empty batch.
func (p *Poller) Block(b *Batch) (woken bool) { return p.retrieve(b, nil) }

// retrieve calls kevent with timeout and keeps the connection events, consuming the wake signal.
func (p *Poller) retrieve(b *Batch, timeout *unix.Timespec) (woken bool) {
	n, err := unix.Kevent(p.kqueueFd, nil, b.events[:], timeout)
	if err != nil {
		n = 0
	}
	count := 0
	for i := range n {
		if b.events[i].Filter == unix.EVFILT_USER {
			p.waking.Store(false)
			woken = true
			continue
		}
		if _, _, ok := convert(&b.events[i]); ok {
			b.events[count] = b.events[i]
			count++
		}
	}
	b.count = count
	return woken
}

// convert turns a read or write filter event into the fd and its events; ok is false for any other filter.
func convert(ev *unix.Kevent_t) (fd int, e Event, ok bool) {
	switch ev.Filter {
	case unix.EVFILT_READ:
		e = EventRead
		if ev.Flags&unix.EV_EOF != 0 {
			e |= EventHup
		}
		return int(ev.Ident), e, true
	case unix.EVFILT_WRITE:
		return int(ev.Ident), EventWrite, true
	}
	return 0, 0, false
}
