//go:build windows

package netpoll

import (
	"net"
	"testing"
	"time"
)

func TestKeepAliveOnAcceptedSockets(t *testing.T) {
	lfd, laddr, err := Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer Close(lfd)
	if err := SetKeepAlive(lfd, KeepAlive{Idle: time.Minute}); err != nil {
		t.Fatalf("listener: %v", err)
	}
	client, err := net.Dial("tcp", laddr.String())
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	deadline := time.Now().Add(5 * time.Second)
	var fd int
	for {
		if fd, _, err = Accept(lfd); err == nil {
			break
		}
		if !IsAgain(err) || time.Now().After(deadline) {
			t.Fatalf("Accept: %v", err)
		}
		time.Sleep(5 * time.Millisecond)
	}
	defer Close(fd)
	for _, ka := range []KeepAlive{{Idle: 42 * time.Second, Interval: 7 * time.Second, Count: 3}, {Idle: -1}} {
		if err := SetKeepAlive(fd, ka); err != nil {
			t.Fatalf("SetKeepAlive(%+v): %v", ka, err)
		}
	}
}
