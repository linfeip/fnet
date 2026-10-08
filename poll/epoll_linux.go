//go:build linux

package poll

import (
	"encoding/binary"
	"os"
	"runtime"
	"sync/atomic"
	"syscall"
	"unsafe"

	"golang.org/x/sys/unix"
)

// wakeValue is the counter value 1 written to the eventfd (in native byte order).
var wakeValue = binary.NativeEndian.AppendUint64(nil, 1)

// Poller is a multiplexer based on epoll.
type Poller struct {
	epollFd int
	// epollFile holds epollFd and registers it with the Go runtime's netpoller; Wait uses it to park the goroutine when
	// there are no events (see Wait), and it closes epollFd when the Poller is closed. Nil for NewBlocking.
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

// NewBlocking creates a Poller for Poll and Block, whose waits block the calling thread in the kernel: its epoll fd is
// not watched by the runtime's netpoller. Every readiness notification of an fd in a watched epoll also goes through the
// netpoller's own epoll, one for the whole process, so under load every CPU delivering packets contends for that
// epoll's lock and wakes its waiter; a busy server's connections must not pay that.
func NewBlocking() (*Poller, error) {
	epfd, err := unix.EpollCreate1(unix.EPOLL_CLOEXEC)
	if err != nil {
		return nil, os.NewSyscallError("epoll_create1", err)
	}
	wakeFd, err := unix.Eventfd(0, unix.EFD_NONBLOCK|unix.EFD_CLOEXEC)
	if err != nil {
		unix.Close(epfd)
		return nil, os.NewSyscallError("eventfd", err)
	}
	p := &Poller{epollFd: epfd, wakeFd: wakeFd}
	if err := p.ctl(unix.EPOLL_CTL_ADD, wakeFd, unix.EPOLLIN); err != nil {
		p.Close()
		return nil, err
	}
	return p, nil
}

// Close releases the fds the Poller occupies.
func (p *Poller) Close() error {
	unix.Close(p.wakeFd)
	if p.epollFile == nil {
		return os.NewSyscallError("close", unix.Close(p.epollFd))
	}
	return p.epollFile.Close() // deregisters from the runtime's netpoller and closes epollFd
}

// AddRead registers fd, watching for readable events.
func (p *Poller) AddRead(fd int) error { return p.ctl(unix.EPOLL_CTL_ADD, fd, unix.EPOLLIN) }

// AddListener registers a listening socket, edge-triggered: one notification when connections arrive, after which
// they are accepted until EAGAIN.
func (p *Poller) AddListener(fd int) error {
	return p.ctl(unix.EPOLL_CTL_ADD, fd, unix.EPOLLIN|unix.EPOLLET)
}

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

// Wake wakes the event loop blocked in Wait, or one goroutine blocked in Block; it may be called from any goroutine.
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
//
// Before parking it yields the CPU once and polls again, as the kqueue Poller does: under load events arrive while the
// goroutine waits for a P, so it goes on without being parked and woken through the netpoller, which costs a round of
// the scheduler each way and leaves the events to be collected a few at a time.
func (p *Poller) Wait(fn func(fd int, ev Event)) (woken bool, err error) {
	if !p.pollEvents(0) {
		runtime.Gosched()
		if err := p.epollConn.Read(p.pollEventsFunc); err != nil {
			return false, err
		}
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
		fn(fd, convert(ev.Events))
	}
	return woken, nil
}

// Batch holds the events one Poll or Block call retrieved; goroutines polling at the same time each need their own.
type Batch struct {
	events [BatchSize]unix.EpollEvent
	count  int
}

// Len returns the number of events in the batch.
func (b *Batch) Len() int { return b.count }

// Event returns the i-th event: the ready fd and its events.
func (b *Batch) Event(i int) (fd int, ev Event) {
	return int(b.events[i].Fd), convert(b.events[i].Events)
}

// Poll retrieves up to BatchSize ready events into b without blocking. Any number of goroutines may call Poll and Block
// on the same Poller at the same time, each with its own Batch: edge-triggered readiness goes to whichever of them
// retrieves it. woken reports that the call consumed the Wake signal, which is not counted as an event. The Poller must
// not be closed while Poll or Block runs.
//
// A zero timeout never blocks, so the call is a raw one, without handing the P to the scheduler around it.
func (p *Poller) Poll(b *Batch) (woken bool) {
	r, _, errno := syscall.RawSyscall6(unix.SYS_EPOLL_PWAIT, uintptr(p.epollFd),
		uintptr(unsafe.Pointer(&b.events[0])), uintptr(len(b.events)), 0, 0, 0)
	if errno != 0 {
		r = 0
	}
	return p.collect(b, int(r))
}

// Block is Poll that blocks the calling thread in the kernel until an event is ready or Wake is called; when interrupted
// by a signal it returns with an empty batch.
func (p *Poller) Block(b *Batch) (woken bool) {
	n, err := unix.EpollWait(p.epollFd, b.events[:], -1)
	if err != nil {
		n = 0
	}
	return p.collect(b, n)
}

// collect drops the wake event from the n events retrieved into b, consuming the signal.
func (p *Poller) collect(b *Batch, n int) (woken bool) {
	for i := 0; i < n; i++ {
		if int(b.events[i].Fd) == p.wakeFd {
			var buf [8]byte
			unix.Read(p.wakeFd, buf[:])
			p.waking.Store(false)
			woken = true
			n--
			b.events[i] = b.events[n]
			i--
		}
	}
	b.count = n
	return woken
}

func convert(events uint32) Event {
	var e Event
	if events&(unix.EPOLLIN|unix.EPOLLERR|unix.EPOLLHUP|unix.EPOLLRDHUP) != 0 {
		e |= EventRead
	}
	if events&unix.EPOLLOUT != 0 {
		e |= EventWrite
	}
	if events&(unix.EPOLLERR|unix.EPOLLHUP|unix.EPOLLRDHUP) != 0 {
		e |= EventHup
	}
	return e
}

// pollEvents retrieves one round of ready events without blocking. When it returns false (no events), the runtime parks
// the goroutine running Wait and calls it again once the epoll fd becomes readable; on an error (including EINTR) it
// returns true and Wait handles it.
//
// A zero timeout never blocks, so the call is a raw one, without handing the P to the scheduler around it: under load
// the loop polls hundreds of times a second, and a P left in a syscall can be handed to another thread, with the loop
// then waiting for a P to come back from it. It is epoll_pwait with no signal mask, which is epoll_wait and the one of
// the two every architecture has.
func (p *Poller) pollEvents(uintptr) bool {
	r, _, errno := syscall.RawSyscall6(unix.SYS_EPOLL_PWAIT, uintptr(p.epollFd),
		uintptr(unsafe.Pointer(unsafe.SliceData(p.events))), uintptr(len(p.events)), 0, 0, 0)
	p.readyCount, p.pollErr = int(r), nil
	if errno != 0 {
		p.readyCount, p.pollErr = -1, errno
	}
	return p.readyCount > 0 || p.pollErr != nil
}
