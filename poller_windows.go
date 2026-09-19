//go:build windows

package fnet

import (
	"fmt"
	"sync"
	"time"
)

// windowsPoller implements Poller via a stdlib net bridge. Socket readiness
// is collected from background accept/read pumps (see socket_windows.go) so
// the shared Server event-loop / VirtualConn / TLS / HTTP path stays identical
// to the epoll and kqueue builds.
type windowsPoller struct {
	mu       sync.Mutex
	interest map[int]int16 // fd -> poll flags
	closing  bool
	wake     chan struct{}
}

func newPoller() (Poller, error) {
	return &windowsPoller{
		interest: make(map[int]int16),
		wake:     make(chan struct{}, 1),
	}, nil
}

func (p *windowsPoller) signal() {
	select {
	case p.wake <- struct{}{}:
	default:
	}
}

func (p *windowsPoller) AddRead(fd int) error {
	p.mu.Lock()
	p.interest[fd] = pollRDNORM
	p.mu.Unlock()
	p.signal()
	return nil
}

func (p *windowsPoller) AddWrite(fd int) error {
	p.mu.Lock()
	p.interest[fd] = pollRDNORM | pollWRNORM
	p.mu.Unlock()
	p.signal()
	return nil
}

func (p *windowsPoller) ModRead(fd int) error {
	p.mu.Lock()
	p.interest[fd] = pollRDNORM
	p.mu.Unlock()
	p.signal()
	return nil
}

func (p *windowsPoller) ModReadWrite(fd int) error {
	p.mu.Lock()
	p.interest[fd] = pollRDNORM | pollWRNORM
	p.mu.Unlock()
	p.signal()
	return nil
}

func (p *windowsPoller) Delete(fd int) error {
	p.mu.Lock()
	delete(p.interest, fd)
	p.mu.Unlock()
	return nil
}

func (p *windowsPoller) Wait(timeout time.Duration) ([]Event, error) {
	p.mu.Lock()
	if p.closing {
		p.mu.Unlock()
		return nil, fmt.Errorf("poller closed")
	}
	interest := make(map[int]int16, len(p.interest))
	for fd, ev := range p.interest {
		interest[fd] = ev
	}
	p.mu.Unlock()

	if evs := winCollectReady(interest); len(evs) > 0 {
		return evs, nil
	}

	timer := time.NewTimer(timeout)
	if timeout < 0 {
		timer.Stop()
		timer = time.NewTimer(50 * time.Millisecond)
	} else if timeout == 0 {
		timer.Stop()
		return nil, nil
	}
	defer timer.Stop()

	select {
	case <-p.wake:
	case <-winPollCh:
	case <-timer.C:
	}

	p.mu.Lock()
	if p.closing {
		p.mu.Unlock()
		return nil, fmt.Errorf("poller closed")
	}
	interest = make(map[int]int16, len(p.interest))
	for fd, ev := range p.interest {
		interest[fd] = ev
	}
	p.mu.Unlock()
	return winCollectReady(interest), nil
}

func (p *windowsPoller) Close() error {
	p.mu.Lock()
	p.closing = true
	p.interest = nil
	p.mu.Unlock()
	p.signal()
	winWakePoller()
	return nil
}
