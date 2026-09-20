//go:build windows

package fnet

import (
	"fmt"
	"net"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
)

// poll flag bits shared with poller_windows.go
const (
	pollRDNORM = 0x0100
	pollWRNORM = 0x0010
)

// Windows uses a stdlib net bridge for listen/accept/read/write while still
// driving the shared Server event-loop through Poller events.

type winNetFD struct {
	id   int
	ln   net.Listener
	conn net.Conn

	mu       sync.Mutex
	readable bool
	writable bool
	closed   bool
	acceptQ  []net.Conn
	readBuf  []byte
	readEOF  bool
	readErr  error
}

var (
	winFDs    sync.Map // int -> *winNetFD
	winFDNext atomic.Int64
	winPollCh = make(chan struct{}, 1)
)

func winAllocID() int {
	return int(winFDNext.Add(1))
}

func winWakePoller() {
	select {
	case winPollCh <- struct{}{}:
	default:
	}
}

func setNonblock(fd int) error { return nil }

func setReuseAddr(fd int) error { return nil }

func closeFD(fd int) error {
	v, ok := winFDs.LoadAndDelete(fd)
	if !ok {
		return nil
	}
	e := v.(*winNetFD)
	e.mu.Lock()
	e.closed = true
	ln, conn := e.ln, e.conn
	pending := e.acceptQ
	e.acceptQ = nil
	e.mu.Unlock()
	for _, c := range pending {
		_ = c.Close()
	}
	if ln != nil {
		return ln.Close()
	}
	if conn != nil {
		return conn.Close()
	}
	return nil
}

func readFD(fd int, buf []byte) (int, error) {
	v, ok := winFDs.Load(fd)
	if !ok {
		return 0, net.ErrClosed
	}
	e := v.(*winNetFD)
	e.mu.Lock()
	defer e.mu.Unlock()
	if len(e.readBuf) == 0 {
		if e.readEOF {
			return 0, nil
		}
		if e.readErr != nil {
			return 0, e.readErr
		}
		e.readable = false
		return 0, syscall.EAGAIN
	}
	n := copy(buf, e.readBuf)
	e.readBuf = e.readBuf[n:]
	if len(e.readBuf) == 0 {
		e.readBuf = nil
		e.readable = e.readEOF || e.readErr != nil
	}
	return n, nil
}

func writeFD(fd int, buf []byte) (int, error) {
	v, ok := winFDs.Load(fd)
	if !ok {
		return 0, net.ErrClosed
	}
	e := v.(*winNetFD)
	if e.conn == nil {
		return 0, fmt.Errorf("not a connection fd")
	}
	_ = e.conn.SetWriteDeadline(time.Now().Add(30 * time.Second))
	return e.conn.Write(buf)
}

func writevFD(fd int, iovs [][]byte) (int, error) {
	total := 0
	for _, b := range iovs {
		if len(b) == 0 {
			continue
		}
		n, err := writeFD(fd, b)
		if n > 0 {
			total += n
		}
		if err != nil {
			return total, err
		}
		if n < len(b) {
			return total, nil
		}
	}
	return total, nil
}

func acceptFD(fd int) (int, net.Addr, error) {
	v, ok := winFDs.Load(fd)
	if !ok {
		return -1, nil, net.ErrClosed
	}
	e := v.(*winNetFD)
	e.mu.Lock()
	if len(e.acceptQ) == 0 {
		e.readable = false
		e.mu.Unlock()
		return -1, nil, syscall.EAGAIN
	}
	c := e.acceptQ[0]
	e.acceptQ = e.acceptQ[1:]
	if len(e.acceptQ) == 0 {
		e.readable = false
	}
	e.mu.Unlock()

	id := winAllocID()
	entry := &winNetFD{id: id, conn: c, writable: true}
	winFDs.Store(id, entry)
	go winConnReader(entry)
	return id, c.RemoteAddr(), nil
}

func acceptConn(lnFD int, laddr net.Addr) (int, *VirtualConn, error) {
	nfd, raddr, err := acceptFD(lnFD)
	if err != nil {
		return -1, nil, err
	}
	vc := NewVirtualConn(laddr, raddr)
	return nfd, vc, nil
}

func listenNonblock(network, address string) (int, net.Addr, error) {
	ln, err := net.Listen(network, address)
	if err != nil {
		return -1, nil, err
	}
	id := winAllocID()
	entry := &winNetFD{id: id, ln: ln}
	winFDs.Store(id, entry)
	go winAcceptLoop(entry)
	return id, ln.Addr(), nil
}

func winAcceptLoop(e *winNetFD) {
	for {
		c, err := e.ln.Accept()
		if err != nil {
			e.mu.Lock()
			if !e.closed {
				e.readErr = err
				e.readable = true
			}
			e.mu.Unlock()
			winWakePoller()
			return
		}
		e.mu.Lock()
		if e.closed {
			e.mu.Unlock()
			_ = c.Close()
			return
		}
		e.acceptQ = append(e.acceptQ, c)
		e.readable = true
		e.mu.Unlock()
		winWakePoller()
	}
}

func winConnReader(e *winNetFD) {
	buf := make([]byte, 32*1024)
	for {
		n, err := e.conn.Read(buf)
		e.mu.Lock()
		if e.closed {
			e.mu.Unlock()
			return
		}
		if n > 0 {
			e.readBuf = append(e.readBuf, buf[:n]...)
			e.readable = true
		}
		if err != nil {
			e.readEOF = true
			e.readErr = err
			e.readable = true
			e.mu.Unlock()
			winWakePoller()
			return
		}
		e.mu.Unlock()
		winWakePoller()
	}
}

func winCollectReady(interest map[int]int16) []Event {
	out := make([]Event, 0, len(interest))
	for fd, ev := range interest {
		v, ok := winFDs.Load(fd)
		if !ok {
			continue
		}
		e := v.(*winNetFD)
		e.mu.Lock()
		evOut := Event{Fd: fd}
		if ev&pollRDNORM != 0 && e.readable {
			evOut.Readable = true
		}
		if ev&pollWRNORM != 0 && e.writable && e.conn != nil {
			evOut.Writable = true
		}
		if e.readErr != nil && e.conn != nil && len(e.readBuf) == 0 {
			evOut.Hangup = true
		}
		e.mu.Unlock()
		if evOut.Readable || evOut.Writable || evOut.Hangup || evOut.Error {
			out = append(out, evOut)
		}
	}
	return out
}
