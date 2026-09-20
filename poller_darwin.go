//go:build darwin

package fnet

import (
	"fmt"
	"sync"
	"time"

	"golang.org/x/sys/unix"
)

type kqueuePoller struct {
	mu      sync.Mutex
	fd      int
	wakeR   int
	wakeW   int
	changes []unix.Kevent_t
	events  []unix.Kevent_t
	// Track which filters are registered to avoid EV_DELETE errors.
	readRegs  map[int]bool
	writeRegs map[int]bool
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
		fd:        fd,
		wakeR:     pfd[0],
		wakeW:     pfd[1],
		events:    make([]unix.Kevent_t, 1024),
		readRegs:  make(map[int]bool),
		writeRegs: make(map[int]bool),
	}
	p.control(p.wakeR, unix.EVFILT_READ, unix.EV_ADD|unix.EV_CLEAR)
	return p, nil
}

func (p *kqueuePoller) control(fd int, filter int16, flags uint16) {
	p.changes = append(p.changes, unix.Kevent_t{
		Ident:  uint64(fd),
		Filter: filter,
		Flags:  flags,
	})
}

func (p *kqueuePoller) AddRead(fd int) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.control(fd, unix.EVFILT_READ, unix.EV_ADD|unix.EV_CLEAR)
	p.readRegs[fd] = true
	return nil
}

func (p *kqueuePoller) AddWrite(fd int) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.control(fd, unix.EVFILT_WRITE, unix.EV_ADD|unix.EV_CLEAR)
	p.writeRegs[fd] = true
	return nil
}

func (p *kqueuePoller) ModRead(fd int) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.writeRegs[fd] {
		p.control(fd, unix.EVFILT_WRITE, unix.EV_DELETE)
		delete(p.writeRegs, fd)
	}
	if !p.readRegs[fd] {
		p.control(fd, unix.EVFILT_READ, unix.EV_ADD|unix.EV_CLEAR)
		p.readRegs[fd] = true
	}
	return nil
}

func (p *kqueuePoller) ModReadWrite(fd int) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if !p.readRegs[fd] {
		p.control(fd, unix.EVFILT_READ, unix.EV_ADD|unix.EV_CLEAR)
		p.readRegs[fd] = true
	}
	if !p.writeRegs[fd] {
		p.control(fd, unix.EVFILT_WRITE, unix.EV_ADD|unix.EV_CLEAR)
		p.writeRegs[fd] = true
	}
	return nil
}

func (p *kqueuePoller) Delete(fd int) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.readRegs[fd] {
		p.control(fd, unix.EVFILT_READ, unix.EV_DELETE)
		delete(p.readRegs, fd)
	}
	if p.writeRegs[fd] {
		p.control(fd, unix.EVFILT_WRITE, unix.EV_DELETE)
		delete(p.writeRegs, fd)
	}
	return nil
}

func (p *kqueuePoller) Wait(timeout time.Duration) ([]Event, error) {
	p.mu.Lock()
	changes := p.changes
	p.changes = nil
	p.mu.Unlock()

	var tsp *unix.Timespec
	if timeout >= 0 {
		ts := unix.NsecToTimespec(timeout.Nanoseconds())
		tsp = &ts
	}

	n, err := unix.Kevent(p.fd, changes, p.events, tsp)
	if err != nil {
		p.mu.Lock()
		p.changes = append(changes[:0], p.changes...)
		p.mu.Unlock()
		if err == unix.EINTR {
			return nil, nil
		}
		return nil, err
	}

	out := make([]Event, 0, n)
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
