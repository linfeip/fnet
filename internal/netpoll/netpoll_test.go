package netpoll

import (
	"net"
	"testing"
	"time"
)

// waitFor polls p until an event for fd matching want arrives.
func waitFor(t *testing.T, p Poller, fd int, want func(Event) bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		evs, err := p.Wait(100 * time.Millisecond)
		if err != nil {
			t.Fatalf("Wait: %v", err)
		}
		for _, ev := range evs {
			if ev.Fd == fd && want(ev) {
				return
			}
		}
	}
	t.Fatalf("no matching event for fd %d", fd)
}

func TestAcceptReadWrite(t *testing.T) {
	p, err := NewPoller()
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()

	lfd, laddr, err := Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer Close(lfd)
	if err := p.Add(lfd); err != nil {
		t.Fatal(err)
	}

	client, err := net.Dial("tcp", laddr.String())
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	waitFor(t, p, lfd, func(ev Event) bool { return ev.Readable })
	fd, raddr, err := Accept(lfd)
	if err != nil {
		t.Fatalf("Accept: %v", err)
	}
	defer Close(fd)
	if raddr.String() != client.LocalAddr().String() {
		t.Fatalf("peer address %s, want %s", raddr, client.LocalAddr())
	}
	if _, _, err := Accept(lfd); !IsAgain(err) {
		t.Fatalf("second Accept: want EAGAIN, got %v", err)
	}
	if err := p.Add(fd); err != nil {
		t.Fatal(err)
	}

	if _, err := client.Write([]byte("ping")); err != nil {
		t.Fatal(err)
	}
	waitFor(t, p, fd, func(ev Event) bool { return ev.Readable })
	buf := make([]byte, 16)
	n, err := Read(fd, buf)
	if err != nil || string(buf[:n]) != "ping" {
		t.Fatalf("Read: %q %v", buf[:n], err)
	}

	if err := p.EnableWrite(fd); err != nil {
		t.Fatal(err)
	}
	waitFor(t, p, fd, func(ev Event) bool { return ev.Writable })
	if err := p.DisableWrite(fd); err != nil {
		t.Fatal(err)
	}
	if n, err := Writev(fd, [][]byte{[]byte("po"), []byte("ng")}); n != 4 || err != nil {
		t.Fatalf("Writev: %d %v", n, err)
	}
	_ = client.SetReadDeadline(time.Now().Add(5 * time.Second))
	n, err = client.Read(buf)
	if err != nil || string(buf[:n]) != "pong" {
		t.Fatalf("client read: %q %v", buf[:n], err)
	}

	_ = client.Close()
	waitFor(t, p, fd, func(ev Event) bool { return ev.Readable })
	if n, err := Read(fd, buf); n != 0 || err != nil {
		t.Fatalf("Read after peer close: %d %v, want EOF", n, err)
	}
}

func TestWakeInterruptsWait(t *testing.T) {
	p, err := NewPoller()
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	go func() {
		time.Sleep(20 * time.Millisecond)
		_ = p.Wake()
	}()
	start := time.Now()
	if _, err := p.Wait(5 * time.Second); err != nil {
		t.Fatal(err)
	}
	if time.Since(start) > 2*time.Second {
		t.Fatal("Wake did not interrupt Wait")
	}
}
