//go:build linux

package poll

import (
	"encoding/binary"
	"os"
	"sync/atomic"
	"syscall"

	"golang.org/x/sys/unix"
)

// wakeValue is the counter value 1 written to the eventfd (in native byte order).
var wakeValue = binary.NativeEndian.AppendUint64(nil, 1)

// Poller is a multiplexer based on epoll.
type Poller struct {
	epollFd int
	// epollFile holds epollFd and registers it with the Go runtime's netpoller; Wait uses it to park the goroutine when
	// there are no events (see Wait), and it closes epollFd when the Poller is closed.
	epollFile *os.File
	epollConn syscall.RawConn
	wakeFd    int         // eventfd, used to wake Wait from another goroutine
	waking    atomic.Bool // coalesces concurrent Wake calls, avoiding repeated writes to the eventfd
	events    []unix.EpollEvent
	// the fields below are used by Wait only
	pollEventsFunc func(uintptr) bool // pollEvents bound in advance, avoiding a closure allocation per Wait round
	readyCount     int                // the number of events pollEvents retrieved
	pollErr        error              // the error from pollEvents
	wakeBuf        [8]byte            // buffer for reading the eventfd
}

// New creates a Poller.
func New() (*Poller, error) {
	epfd, err := unix.EpollCreate1(unix.EPOLL_CLOEXEC)
	if err != nil {
		return nil, os.NewSyscallError("epoll_create1", err)
	}
	// The runtime only takes over non-blocking fds; epoll_wait itself is not affected by O_NONBLOCK.
	if err := unix.SetNonblock(epfd, true); err != nil {
		unix.Close(epfd)
		return nil, os.NewSyscallError("fcntl", err)
	}
	wakeFd, err := unix.Eventfd(0, unix.EFD_NONBLOCK|unix.EFD_CLOEXEC)
	if err != nil {
		unix.Close(epfd)
		return nil, os.NewSyscallError("eventfd", err)
	}
	p := &Poller{
		epollFd:   epfd,
		epollFile: os.NewFile(uintptr(epfd), "epoll"),
		wakeFd:    wakeFd,
		events:    make([]unix.EpollEvent, 1024),
	}
	p.epollConn, _ = p.epollFile.SyscallConn() // only fails when File is nil
	p.pollEventsFunc = p.pollEvents
	if err := p.ctl(unix.EPOLL_CTL_ADD, wakeFd, unix.EPOLLIN); err != nil {
		p.Close()
		return nil, err
	}
	return p, nil
}

// Close releases the fds the Poller occupies.
func (p *Poller) Close() error {
	unix.Close(p.wakeFd)
	return p.epollFile.Close() // deregisters from the runtime's netpoller and closes epollFd
}

// AddRead registers fd, watching for readable events.
func (p *Poller) AddRead(fd int) error { return p.ctl(unix.EPOLL_CTL_ADD, fd, unix.EPOLLIN) }

// AddEdge registers a connection fd: edge-triggered, watching readable, writable and peer close at once, the same as the
// Go runtime's netpoller.
// Edge-triggered mode notifies only once per state change: after a readable event, the fd counts as drained only once
// EAGAIN is read (or the buffer was not filled; for a closed peer see EventHup); another writable event only comes after
// writing has hit EAGAIN. Registration is dropped automatically when the fd is closed.
func (p *Poller) AddEdge(fd int) error {
	return p.ctl(unix.EPOLL_CTL_ADD, fd, unix.EPOLLIN|unix.EPOLLOUT|unix.EPOLLRDHUP|unix.EPOLLET)
}

// Delete deregisters fd.
func (p *Poller) Delete(fd int) error { return p.ctl(unix.EPOLL_CTL_DEL, fd, 0) }

func (p *Poller) ctl(op, fd int, events uint32) error {
	ev := unix.EpollEvent{Events: events, Fd: int32(fd)}
	return os.NewSyscallError("epoll_ctl", unix.EpollCtl(p.epollFd, op, fd, &ev))
}

// Wake wakes the event loop blocked in Wait; it may be called from any goroutine.
func (p *Poller) Wake() error {
	if !p.waking.CompareAndSwap(false, true) {
		return nil // there is already a wakeup signal that has not been consumed
	}
	_, err := unix.Write(p.wakeFd, wakeValue)
	if err == unix.EAGAIN {
		err = nil // the counter is already full, so Wait is bound to be woken
	}
	if err != nil {
		p.waking.Store(false)
	}
	return os.NewSyscallError("write", err)
}

// Wait waits for ready events and invokes fn for every ready fd; if a Wake signal arrives in the meantime it returns
// woken=true. When interrupted by a signal it returns (false, nil), and the caller should call it in a loop.
//
// When there are no events it does not block inside epoll_wait; instead the Go runtime's netpoller parks the goroutine
// and wakes it when the epoll fd becomes readable (an event is ready). A goroutine blocked in a syscall keeps holding
// its P until the runtime (sysmon) takes it back, and during that time the goroutines in that P's local queue (such as
// application goroutines spawned by a callback) can only wait; parking gives up the P right away.
func (p *Poller) Wait(fn func(fd int, ev Event)) (woken bool, err error) {
	if err := p.epollConn.Read(p.pollEventsFunc); err != nil {
		return false, err
	}
	n, err := p.readyCount, p.pollErr
	if n <= 0 || err != nil {
		if err != nil && err != unix.EINTR {
			return false, os.NewSyscallError("epoll_wait", err)
		}
		return false, nil
	}
	for i := range n {
		ev := &p.events[i]
		fd := int(ev.Fd)
		if fd == p.wakeFd {
			unix.Read(p.wakeFd, p.wakeBuf[:])
			p.waking.Store(false) // reset first and let the caller handle the tasks after, so later Wake calls are not lost
			woken = true
			continue
		}
		var e Event
		if ev.Events&(unix.EPOLLIN|unix.EPOLLERR|unix.EPOLLHUP|unix.EPOLLRDHUP) != 0 {
			e |= EventRead
		}
		if ev.Events&unix.EPOLLOUT != 0 {
			e |= EventWrite
		}
		if ev.Events&(unix.EPOLLERR|unix.EPOLLHUP|unix.EPOLLRDHUP) != 0 {
			e |= EventHup
		}
		fn(fd, e)
	}
	return woken, nil
}

// pollEvents retrieves one round of ready events without blocking. When it returns false (no events), the runtime parks
// the goroutine running Wait and calls it again once the epoll fd becomes readable; on an error (including EINTR) it
// returns true and Wait handles it.
func (p *Poller) pollEvents(uintptr) bool {
	p.readyCount, p.pollErr = unix.EpollWait(p.epollFd, p.events, 0)
	return p.readyCount > 0 || p.pollErr != nil
}
