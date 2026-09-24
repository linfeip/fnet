//go:build windows

package netpoll

import (
	"net"
	"sync"
	"time"
)

// winPoller collects sockets marked ready by their pumps (see socket_windows.go).
// A socket only ever wakes the poller it is registered with.
type winPoller struct {
	mu     sync.Mutex
	ready  []*sock
	spare  []*sock
	closed bool
	woken  bool // Wake was called: the next Wait returns even without events
	wake   chan struct{}
	out    []Event
}

func newPoller() (Poller, error) {
	return &winPoller{wake: make(chan struct{}, 1)}, nil
}

func (p *winPoller) signal() {
	select {
	case p.wake <- struct{}{}:
	default:
	}
}

func (p *winPoller) push(s *sock) {
	p.mu.Lock()
	if !p.closed && !s.queued {
		s.queued = true
		p.ready = append(p.ready, s)
	}
	p.mu.Unlock()
	p.signal()
}

func (p *winPoller) Add(fd int) error {
	s := lookup(fd)
	if s == nil {
		return net.ErrClosed
	}
	s.mu.Lock()
	s.poller = p
	ready := s.readableLocked()
	s.mu.Unlock()
	if ready {
		p.push(s)
	}
	return nil
}

func (p *winPoller) EnableWrite(fd int) error {
	s := lookup(fd)
	if s == nil {
		return net.ErrClosed
	}
	s.mu.Lock()
	s.writeOn = true
	s.mu.Unlock()
	p.push(s)
	return nil
}

func (p *winPoller) DisableWrite(fd int) error {
	if s := lookup(fd); s != nil {
		s.mu.Lock()
		s.writeOn = false
		s.mu.Unlock()
	}
	return nil
}

func (p *winPoller) Wait(timeout time.Duration) ([]Event, error) {
	var timer *time.Timer
	defer func() {
		if timer != nil {
			timer.Stop()
		}
	}()
	for {
		p.mu.Lock()
		if p.closed {
			p.mu.Unlock()
			return nil, net.ErrClosed
		}
		batch := p.ready
		p.ready, p.spare = p.spare[:0], batch
		for _, s := range batch {
			s.queued = false
		}
		woken := p.woken
		p.woken = false
		p.mu.Unlock()

		out := p.out[:0]
		for _, s := range batch {
			s.mu.Lock()
			ev := Event{
				Fd:       s.id,
				Readable: !s.closed && s.readableLocked(),
				Writable: !s.closed && s.writeOn && s.writableLocked(),
			}
			s.mu.Unlock()
			if ev.Readable || ev.Writable {
				out = append(out, ev)
			}
		}
		p.out = out
		if len(out) > 0 || woken || timeout == 0 {
			return out, nil
		}
		if timeout < 0 {
			<-p.wake
			continue
		}
		if timer == nil {
			timer = time.NewTimer(timeout)
		}
		select {
		case <-p.wake:
		case <-timer.C:
			timer = nil
			return nil, nil
		}
	}
}

func (p *winPoller) Wake() error {
	p.mu.Lock()
	p.woken = true
	p.mu.Unlock()
	p.signal()
	return nil
}

func (p *winPoller) Close() error {
	p.mu.Lock()
	p.closed = true
	p.ready, p.spare = nil, nil
	p.mu.Unlock()
	p.signal()
	return nil
}
