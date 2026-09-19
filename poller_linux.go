//go:build linux

package fnet

import (
	"fmt"
	"sync"
	"time"

	"golang.org/x/sys/unix"
)

type epollPoller struct {
	mu     sync.Mutex
	fd     int
	events []unix.EpollEvent
}

func newPoller() (Poller, error) {
	fd, err := unix.EpollCreate1(unix.EPOLL_CLOEXEC)
	if err != nil {
		return nil, fmt.Errorf("epoll_create1: %w", err)
	}
	return &epollPoller{
		fd:     fd,
		events: make([]unix.EpollEvent, 128),
	}, nil
}

func (p *epollPoller) ctl(op int, fd int, events uint32) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	ev := &unix.EpollEvent{
		Events: events,
		Fd:     int32(fd),
	}
	return unix.EpollCtl(p.fd, op, fd, ev)
}

func (p *epollPoller) AddRead(fd int) error {
	return p.ctl(unix.EPOLL_CTL_ADD, fd, unix.EPOLLIN|unix.EPOLLET|unix.EPOLLRDHUP)
}

func (p *epollPoller) AddWrite(fd int) error {
	return p.ctl(unix.EPOLL_CTL_MOD, fd, unix.EPOLLIN|unix.EPOLLOUT|unix.EPOLLET|unix.EPOLLRDHUP)
}

func (p *epollPoller) ModRead(fd int) error {
	return p.ctl(unix.EPOLL_CTL_MOD, fd, unix.EPOLLIN|unix.EPOLLET|unix.EPOLLRDHUP)
}

func (p *epollPoller) ModReadWrite(fd int) error {
	return p.ctl(unix.EPOLL_CTL_MOD, fd, unix.EPOLLIN|unix.EPOLLOUT|unix.EPOLLET|unix.EPOLLRDHUP)
}

func (p *epollPoller) Delete(fd int) error {
	return p.ctl(unix.EPOLL_CTL_DEL, fd, 0)
}

func (p *epollPoller) Wait(timeout time.Duration) ([]Event, error) {
	msec := -1
	if timeout >= 0 {
		msec = int(timeout / time.Millisecond)
	}
	n, err := unix.EpollWait(p.fd, p.events, msec)
	if err != nil {
		if err == unix.EINTR {
			return nil, nil
		}
		return nil, err
	}
	out := make([]Event, 0, n)
	for i := 0; i < n; i++ {
		ev := p.events[i]
		e := Event{Fd: int(ev.Fd)}
		if ev.Events&(unix.EPOLLERR) != 0 {
			e.Error = true
		}
		if ev.Events&(unix.EPOLLHUP|unix.EPOLLRDHUP) != 0 {
			e.Hangup = true
		}
		if ev.Events&(unix.EPOLLIN|unix.EPOLLPRI) != 0 {
			e.Readable = true
		}
		if ev.Events&unix.EPOLLOUT != 0 {
			e.Writable = true
		}
		out = append(out, e)
	}
	return out, nil
}

func (p *epollPoller) Close() error {
	return unix.Close(p.fd)
}
