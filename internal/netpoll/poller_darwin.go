//go:build darwin

package netpoll

import (
	"errors"
	"fmt"
	"time"

	"golang.org/x/sys/unix"
)

// kqueuePoller applies every registration change with its own kevent(2) call.
// kqueue is thread-safe, so interest may change while Wait is blocked.
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
	for _, f := range pfd {
		unix.CloseOnExec(f)
		_ = unix.SetNonblock(f, true)
	}
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
	ch := [1]unix.Kevent_t{{Ident: uint64(fd), Filter: filter, Flags: flags}}
	_, err := unix.Kevent(p.fd, ch[:], nil, nil)
	return err
}

func (p *kqueuePoller) Add(fd int) error {
	return p.change(fd, unix.EVFILT_READ, unix.EV_ADD|unix.EV_CLEAR)
}

func (p *kqueuePoller) EnableWrite(fd int) error {
	return p.change(fd, unix.EVFILT_WRITE, unix.EV_ADD|unix.EV_CLEAR)
}

func (p *kqueuePoller) DisableWrite(fd int) error {
	err := p.change(fd, unix.EVFILT_WRITE, unix.EV_DELETE)
	if err == nil || errors.Is(err, unix.ENOENT) || errors.Is(err, unix.EBADF) {
		return nil
	}
	return err
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
	for _, ev := range p.events[:n] {
		if int(ev.Ident) == p.wakeR {
			var buf [128]byte
			for {
				if n, err := unix.Read(p.wakeR, buf[:]); n <= 0 || err != nil {
					break
				}
			}
			continue
		}
		if ev.Flags&unix.EV_ERROR != 0 {
			if errno := unix.Errno(ev.Data); errno == unix.ENOENT || errno == unix.EBADF {
				continue // stale change on an fd that is already gone
			}
		}
		e := Event{Fd: int(ev.Ident)}
		switch ev.Filter {
		case unix.EVFILT_READ:
			e.Readable = true
		case unix.EVFILT_WRITE:
			e.Writable = true
		}
		if ev.Flags&unix.EV_ERROR != 0 {
			e.Readable = true
		}
		out = append(out, e)
	}
	p.out = out
	return out, nil
}

func (p *kqueuePoller) Wake() error {
	buf := [1]byte{1}
	if _, err := unix.Write(p.wakeW, buf[:]); err != nil && err != unix.EAGAIN {
		return err
	}
	return nil
}

func (p *kqueuePoller) Close() error {
	_ = unix.Close(p.wakeR)
	_ = unix.Close(p.wakeW)
	return unix.Close(p.fd)
}
