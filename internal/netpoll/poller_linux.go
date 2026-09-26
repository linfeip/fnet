//go:build linux

package netpoll

import (
	"encoding/binary"
	"fmt"
	"time"

	"golang.org/x/sys/unix"
)

const epollRead = unix.EPOLLIN | unix.EPOLLRDHUP | unix.EPOLLET

// epollPoller: epoll_ctl is thread-safe and takes effect immediately, so no
// registration batching or locking is needed.
type epollPoller struct {
	fd     int
	wakeFD int
	events []unix.EpollEvent
	out    []Event
}

func newPoller() (Poller, error) {
	fd, err := unix.EpollCreate1(unix.EPOLL_CLOEXEC)
	if err != nil {
		return nil, fmt.Errorf("epoll_create1: %w", err)
	}
	wakeFD, err := unix.Eventfd(0, unix.EFD_NONBLOCK|unix.EFD_CLOEXEC)
	if err != nil {
		_ = unix.Close(fd)
		return nil, fmt.Errorf("eventfd: %w", err)
	}
	p := &epollPoller{
		fd:     fd,
		wakeFD: wakeFD,
		events: make([]unix.EpollEvent, 1024),
		out:    make([]Event, 0, 1024),
	}
	if err := p.ctl(unix.EPOLL_CTL_ADD, wakeFD, unix.EPOLLIN); err != nil {
		_ = p.Close()
		return nil, fmt.Errorf("epoll_ctl wakeFD: %w", err)
	}
	return p, nil
}

func (p *epollPoller) ctl(op, fd int, events uint32) error {
	return unix.EpollCtl(p.fd, op, fd, &unix.EpollEvent{Events: events, Fd: int32(fd)})
}

func (p *epollPoller) Add(fd int) error { return p.ctl(unix.EPOLL_CTL_ADD, fd, epollRead) }

func (p *epollPoller) EnableWrite(fd int) error {
	return p.ctl(unix.EPOLL_CTL_MOD, fd, epollRead|unix.EPOLLOUT)
}

func (p *epollPoller) DisableWrite(fd int) error { return p.ctl(unix.EPOLL_CTL_MOD, fd, epollRead) }

func (p *epollPoller) Wait(timeout time.Duration) ([]Event, error) {
	msec := -1
	if timeout >= 0 {
		// Round up: a timeout cut to 0ms would poll in a busy loop until it
		// really elapses.
		msec = int((timeout + time.Millisecond - 1) / time.Millisecond)
	}
	n, err := unix.EpollWait(p.fd, p.events, msec)
	if err != nil {
		if err == unix.EINTR {
			return nil, nil
		}
		return nil, err
	}
	out := p.out[:0]
	for _, ev := range p.events[:n] {
		if int(ev.Fd) == p.wakeFD {
			var buf [8]byte
			_, _ = unix.Read(p.wakeFD, buf[:])
			continue
		}
		out = append(out, Event{
			Fd:       int(ev.Fd),
			Readable: ev.Events&(unix.EPOLLIN|unix.EPOLLPRI|unix.EPOLLRDHUP|unix.EPOLLHUP|unix.EPOLLERR) != 0,
			Writable: ev.Events&unix.EPOLLOUT != 0,
			Hup:      ev.Events&(unix.EPOLLRDHUP|unix.EPOLLHUP|unix.EPOLLERR) != 0,
		})
	}
	p.out = out
	return out, nil
}

func (p *epollPoller) Wake() error {
	var buf [8]byte
	binary.LittleEndian.PutUint64(buf[:], 1)
	if _, err := unix.Write(p.wakeFD, buf[:]); err != nil && err != unix.EAGAIN {
		return err
	}
	return nil
}

func (p *epollPoller) Close() error {
	_ = unix.Close(p.wakeFD)
	return unix.Close(p.fd)
}
