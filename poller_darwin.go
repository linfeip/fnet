//go:build darwin

package fnet

import (
	"errors"
	"fmt"
	"time"

	"golang.org/x/sys/unix"
)

// kqueuePoller applies every registration change immediately with its own
// kevent(2) call. kqueue is thread-safe, so any goroutine may arm/disarm
// interest while the reactor is blocked in Wait and the change takes effect at
// once instead of at the next wake-up.
type kqueuePoller struct {
	fd     int
	wakeR  int
	wakeW  int
	events []unix.Kevent_t
	out    []Event
}

func newPoller() (Poller, error) {
	fd, err := unix.Kqueue()
	if err != nil {
		return nil, fmt.Errorf("kqueue: %w", err)
	}
	unix.CloseOnExec(fd)

	var pfd [2]int
	if err := unix.Pipe(pfd[:]); err != nil {
		_ = unix.Close(fd)
		return nil, fmt.Errorf("pipe: %w", err)
	}
	unix.CloseOnExec(pfd[0])
	unix.CloseOnExec(pfd[1])
	_ = unix.SetNonblock(pfd[0], true)
	_ = unix.SetNonblock(pfd[1], true)

	p := &kqueuePoller{
		fd:     fd,
		wakeR:  pfd[0],
		wakeW:  pfd[1],
		events: make([]unix.Kevent_t, 1024),
		out:    make([]Event, 0, 1024),
	}
	if err := p.change(p.wakeR, unix.EVFILT_READ, unix.EV_ADD|unix.EV_CLEAR); err != nil {
		_ = p.Close()
		return nil, fmt.Errorf("kqueue wake pipe: %w", err)
	}
	return p, nil
}

func (p *kqueuePoller) change(fd int, filter int16, flags uint16) error {
	ch := [1]unix.Kevent_t{{
		Ident:  uint64(fd),
		Filter: filter,
		Flags:  flags,
	}}
	_, err := unix.Kevent(p.fd, ch[:], nil, nil)
	return err
}

// ignoreMissing hides ENOENT/EBADF from delete operations: the filter was never
// registered or the fd is already closed (kqueue drops knotes on close).
func ignoreMissing(err error) error {
	if err == nil || errors.Is(err, unix.ENOENT) || errors.Is(err, unix.EBADF) {
		return nil
	}
	return err
}

func (p *kqueuePoller) AddRead(fd int) error {
	return p.change(fd, unix.EVFILT_READ, unix.EV_ADD|unix.EV_CLEAR)
}

func (p *kqueuePoller) AddWrite(fd int) error {
	return p.change(fd, unix.EVFILT_WRITE, unix.EV_ADD|unix.EV_CLEAR)
}

// ModRead drops write interest; read interest (registered by AddRead) is kept.
func (p *kqueuePoller) ModRead(fd int) error {
	return ignoreMissing(p.change(fd, unix.EVFILT_WRITE, unix.EV_DELETE))
}

// ModReadWrite adds write interest on top of the existing read interest.
func (p *kqueuePoller) ModReadWrite(fd int) error {
	return p.change(fd, unix.EVFILT_WRITE, unix.EV_ADD|unix.EV_CLEAR)
}

func (p *kqueuePoller) Delete(fd int) error {
	err1 := ignoreMissing(p.change(fd, unix.EVFILT_READ, unix.EV_DELETE))
	err2 := ignoreMissing(p.change(fd, unix.EVFILT_WRITE, unix.EV_DELETE))
	if err1 != nil {
		return err1
	}
	return err2
}

func (p *kqueuePoller) Wait(timeout time.Duration) ([]Event, error) {
	var tsp *unix.Timespec
	if timeout >= 0 {
		ts := unix.NsecToTimespec(timeout.Nanoseconds())
		tsp = &ts
	}

	n, err := unix.Kevent(p.fd, nil, p.events, tsp)
	if err != nil {
		if err == unix.EINTR {
			return nil, nil
		}
		return nil, err
	}

	out := p.out[:0]
	for i := 0; i < n; i++ {
		ev := p.events[i]
		if int(ev.Ident) == p.wakeR {
			var buf [128]byte
			for {
				n, err := unix.Read(p.wakeR, buf[:])
				if n <= 0 || err != nil {
					break
				}
			}
			continue
		}
		if ev.Flags&unix.EV_ERROR != 0 && ev.Data != 0 {
			errno := unix.Errno(ev.Data)
			if errno == unix.ENOENT || errno == unix.EBADF {
				continue
			}
		}
		e := Event{Fd: int(ev.Ident)}
		if ev.Flags&unix.EV_ERROR != 0 {
			e.Error = true
		}
		if ev.Flags&unix.EV_EOF != 0 {
			e.Hangup = true
		}
		switch ev.Filter {
		case unix.EVFILT_READ:
			e.Readable = true
		case unix.EVFILT_WRITE:
			e.Writable = true
		}
		out = append(out, e)
	}
	p.out = out
	return out, nil
}

func (p *kqueuePoller) Wake() error {
	var buf [1]byte
	buf[0] = 1
	_, err := unix.Write(p.wakeW, buf[:])
	if err != nil && err != unix.EAGAIN && err != unix.EWOULDBLOCK {
		return err
	}
	return nil
}

func (p *kqueuePoller) Close() error {
	_ = unix.Close(p.wakeR)
	_ = unix.Close(p.wakeW)
	return unix.Close(p.fd)
}
