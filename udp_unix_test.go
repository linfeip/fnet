//go:build linux || darwin

package fnet

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"log"
	"log/slog"
	"net"
	"net/netip"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"golang.org/x/sys/unix"
)

type udpHandlerFunc func(c UDPConn, data []byte, from netip.AddrPort)

func (f udpHandlerFunc) OnPacket(c UDPConn, data []byte, from netip.AddrPort) {
	f(c, data, from)
}

func testDeadline(t *testing.T) time.Time {
	if d, ok := t.Deadline(); ok {
		return d
	}
	return time.Now().Add(10 * time.Second)
}

func startUDPServer(t *testing.T, h UDPHandler, opts UDPOptions) *UDPServer {
	t.Helper()
	if opts.NumLoops == 0 {
		opts.NumLoops = 2
	}
	srv, err := NewUDPServer("127.0.0.1:0", h, opts)
	if err != nil {
		t.Fatal(err)
	}
	served := make(chan error, 1)
	go func() { served <- srv.Serve() }()
	t.Cleanup(func() {
		srv.Close()
		if err := <-served; !errors.Is(err, ErrServerClosed) {
			t.Errorf("Serve returned %v, want ErrServerClosed", err)
		}
	})
	return srv
}

func udpClient(t *testing.T, srv *UDPServer) *net.UDPConn {
	t.Helper()
	c, err := net.DialUDP("udp", nil, srv.Addr().(*net.UDPAddr))
	if err != nil {
		t.Fatal(err)
	}
	if err := c.SetDeadline(testDeadline(t)); err != nil {
		c.Close()
		t.Fatal(err)
	}
	t.Cleanup(func() { c.Close() })
	return c
}

func udpAddrPort(t *testing.T, addr net.Addr) netip.AddrPort {
	t.Helper()
	ap, err := netip.ParseAddrPort(addr.String())
	if err != nil {
		t.Fatal(err)
	}
	return ap
}

func echoUDP(c UDPConn, data []byte, from netip.AddrPort) {
	c.WriteToUDPAddrPort(data, from)
}

// maxLoopbackDatagram returns the largest UDP payload the kernel accepts by default over 127.0.0.1: 65507 on Linux,
// and 9216 on macOS (capped by the net.inet.udp.maxdgram sysctl).
func maxLoopbackDatagram() int {
	if runtime.GOOS == "darwin" {
		return 9216
	}
	return 65507
}

func TestUDPEcho(t *testing.T) {
	srv := startUDPServer(t, udpHandlerFunc(echoUDP), UDPOptions{})
	c := udpClient(t, srv)
	sizes := []int{0, 1, 512, min(16000, maxLoopbackDatagram())}
	got := make([]byte, maxDatagramSize)
	for i := range 100 {
		msg := make([]byte, sizes[i%len(sizes)])
		for j := range msg {
			msg[j] = byte(i + j)
		}
		if _, err := c.Write(msg); err != nil {
			t.Fatalf("message %d (size %d) write: %v", i, len(msg), err)
		}
		n, err := c.Read(got)
		if err != nil {
			t.Fatalf("message %d (size %d) read: %v", i, len(msg), err)
		}
		if !bytes.Equal(got[:n], msg) {
			t.Fatalf("message %d: got %d bytes, want %d bytes", i, n, len(msg))
		}
	}
}

func TestUDPEchoConcurrent(t *testing.T) {
	srv := startUDPServer(t, udpHandlerFunc(echoUDP), UDPOptions{ReusePort: true})
	var wg sync.WaitGroup
	for i := range 50 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			c := udpClient(t, srv)
			got := make([]byte, 128)
			for j := range 100 {
				msg := []byte(fmt.Sprintf("client-%d-seq-%d", i, j))
				if _, err := c.Write(msg); err != nil {
					t.Errorf("client %d write %d: %v", i, j, err)
					return
				}
				n, err := c.Read(got)
				if err != nil || !bytes.Equal(got[:n], msg) {
					t.Errorf("client %d read %d: got %q (err=%v), want %q", i, j, got[:n], err, msg)
					return
				}
			}
		}()
	}
	wg.Wait()
}

func TestUDPLargeDatagram(t *testing.T) {
	srv := startUDPServer(t, udpHandlerFunc(echoUDP), UDPOptions{})
	c := udpClient(t, srv)
	size := maxLoopbackDatagram()
	msg := make([]byte, size)
	for i := range msg {
		msg[i] = byte(i*31 + 7)
	}
	if _, err := c.Write(msg); err != nil {
		t.Fatalf("write %d bytes: %v", size, err)
	}
	got := make([]byte, maxDatagramSize)
	n, err := c.Read(got)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if n != size || !bytes.Equal(got[:n], msg) {
		t.Fatalf("got %d bytes (equal=%v), want %d bytes", n, bytes.Equal(got[:n], msg), size)
	}
}

func TestUDPReusePortSockets(t *testing.T) {
	var (
		mu   sync.Mutex
		seen = make(map[UDPConn]struct{})
	)
	srv := startUDPServer(t, udpHandlerFunc(func(c UDPConn, data []byte, from netip.AddrPort) {
		mu.Lock()
		seen[c] = struct{}{}
		mu.Unlock()
		c.WriteToUDPAddrPort(data, from)
	}), UDPOptions{ReusePort: true})

	if runtime.GOOS == "darwin" {
		if got := len(srv.sockets); got != 1 {
			t.Fatalf("darwin sockets = %d, want 1", got)
		}
		return
	}
	want := runtime.GOMAXPROCS(0)
	if got := len(srv.sockets); got != want {
		t.Fatalf("linux sockets = %d, want %d", got, want)
	}
	if want == 1 {
		t.Skip("GOMAXPROCS == 1")
	}
	got := make([]byte, 16)
	for range 64 {
		c := udpClient(t, srv)
		if _, err := c.Write([]byte("ping")); err != nil {
			t.Fatal(err)
		}
		if _, err := c.Read(got); err != nil {
			t.Fatal(err)
		}
	}
	mu.Lock()
	distinct := len(seen)
	mu.Unlock()
	if distinct < 2 {
		t.Fatalf("observed %d distinct UDPConn handles across 64 clients, want >= 2", distinct)
	}
}

func TestUDPLocalAddr(t *testing.T) {
	srv := startUDPServer(t, udpHandlerFunc(echoUDP), UDPOptions{})
	if got, want := srv.Conn().LocalAddr().String(), srv.Addr().String(); got != want {
		t.Fatalf("LocalAddr = %q, want %q", got, want)
	}
}

func TestUDPSocketBufferSizes(t *testing.T) {
	const want = 512 << 10
	srv := startUDPServer(t, udpHandlerFunc(echoUDP), UDPOptions{
		ReadBufferSize:  want,
		WriteBufferSize: want,
	})
	fd := srv.sockets[0].fd
	rcv, err := unix.GetsockoptInt(fd, unix.SOL_SOCKET, unix.SO_RCVBUF)
	if err != nil {
		t.Fatal(err)
	}
	snd, err := unix.GetsockoptInt(fd, unix.SOL_SOCKET, unix.SO_SNDBUF)
	if err != nil {
		t.Fatal(err)
	}
	if rcv < want || snd < want {
		t.Fatalf("SO_RCVBUF=%d SO_SNDBUF=%d, want >= %d", rcv, snd, want)
	}
}

func TestUDPWriteAfterClose(t *testing.T) {
	connCh := make(chan UDPConn, 1)
	srv := startUDPServer(t, udpHandlerFunc(func(c UDPConn, data []byte, from netip.AddrPort) {
		select {
		case connCh <- c:
		default:
		}
		c.WriteToUDPAddrPort(data, from)
	}), UDPOptions{})

	client := udpClient(t, srv)
	if _, err := client.Write([]byte("hi")); err != nil {
		t.Fatal(err)
	}
	buf := make([]byte, 8)
	if _, err := client.Read(buf); err != nil {
		t.Fatal(err)
	}
	saved := <-connCh
	srv.Close()

	_, err := saved.WriteToUDPAddrPort([]byte("after"), udpAddrPort(t, client.LocalAddr()))
	if !errors.Is(err, net.ErrClosed) {
		t.Fatalf("WriteToUDPAddrPort after Close returned %v, want net.ErrClosed", err)
	}
}

func TestUDPConcurrentWriteAndClose(t *testing.T) {
	srv := startUDPServer(t, udpHandlerFunc(echoUDP), UDPOptions{})
	target, err := net.ListenPacket("udp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer target.Close()
	go func() {
		buf := make([]byte, 64)
		for {
			if _, _, err := target.ReadFrom(buf); err != nil {
				return
			}
		}
	}()

	targetAddr := udpAddrPort(t, target.LocalAddr())
	pc := srv.Conn()
	ready := make(chan struct{})
	var wg sync.WaitGroup
	for range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-ready
			for {
				_, err := pc.WriteToUDPAddrPort([]byte("ping"), targetAddr)
				if err == nil || errors.Is(err, unix.EAGAIN) || errors.Is(err, unix.ENOBUFS) {
					runtime.Gosched()
					continue
				}
				if !errors.Is(err, net.ErrClosed) {
					t.Errorf("WriteToUDPAddrPort returned %v, want nil or net.ErrClosed", err)
				}
				return
			}
		}()
	}
	close(ready)
	time.Sleep(5 * time.Millisecond)
	srv.Close()
	wg.Wait()
}

func TestUDPFamilyMismatch(t *testing.T) {
	srv := startUDPServer(t, udpHandlerFunc(echoUDP), UDPOptions{})
	_, err := srv.Conn().WriteToUDPAddrPort([]byte("v6"), netip.MustParseAddrPort("[::1]:1234"))
	var addrErr *net.AddrError
	if !errors.As(err, &addrErr) {
		t.Fatalf("IPv6 send on IPv4 socket returned %v, want *net.AddrError", err)
	}
	_, err = srv.Conn().WriteToUDPAddrPort([]byte("zero"), netip.AddrPort{})
	if !errors.As(err, &addrErr) {
		t.Fatalf("invalid AddrPort send returned %v, want *net.AddrError", err)
	}

	c := udpClient(t, srv)
	if _, err := c.Write([]byte("still-alive")); err != nil {
		t.Fatal(err)
	}
	got := make([]byte, 32)
	n, err := c.Read(got)
	if err != nil || string(got[:n]) != "still-alive" {
		t.Fatalf("echo after family mismatch: got %q, err %v", got[:n], err)
	}

	// A dual-stack ":0" socket accepts IPv4 peers as v4-mapped IPv6 and replies seamlessly.
	dualSrv, err := NewUDPServer(":0", udpHandlerFunc(echoUDP), UDPOptions{NumLoops: 2})
	if err != nil {
		t.Fatal(err)
	}
	served := make(chan error, 1)
	go func() { served <- dualSrv.Serve() }()
	t.Cleanup(func() {
		dualSrv.Close()
		<-served
	})
	v4Target := &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1), Port: dualSrv.Addr().(*net.UDPAddr).Port}
	v4Client, err := net.DialUDP("udp4", nil, v4Target)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { v4Client.Close() })
	v4Client.SetDeadline(testDeadline(t))
	if _, err := v4Client.Write([]byte("dual-v4")); err != nil {
		t.Fatal(err)
	}
	n, err = v4Client.Read(got)
	if err != nil || string(got[:n]) != "dual-v4" {
		t.Fatalf("dual-stack IPv4 echo: got %q, err %v", got[:n], err)
	}
}

func TestUDPPanic(t *testing.T) {
	defer func(w io.Writer, flags int) { log.SetOutput(w); log.SetFlags(flags) }(log.Writer(), log.Flags())
	defer slog.SetDefault(slog.Default())
	slog.SetDefault(slog.New(slog.NewTextHandler(io.Discard, nil)))

	var calls atomic.Int32
	panicked := make(chan struct{})
	srv := startUDPServer(t, udpHandlerFunc(func(c UDPConn, data []byte, from netip.AddrPort) {
		if calls.Add(1) == 1 {
			close(panicked)
			panic("boom")
		}
		c.WriteToUDPAddrPort(data, from)
	}), UDPOptions{})

	c := udpClient(t, srv)
	if _, err := c.Write([]byte("first")); err != nil {
		t.Fatal(err)
	}
	<-panicked
	if _, err := c.Write([]byte("second")); err != nil {
		t.Fatal(err)
	}
	got := make([]byte, 16)
	n, err := c.Read(got)
	if err != nil || string(got[:n]) != "second" {
		t.Fatalf("after panic: got %q, err %v, want %q", got[:n], err, "second")
	}
}

func TestUDPHolePunch(t *testing.T) {
	probeA, ackA := make(chan struct{}, 1), make(chan struct{}, 1)
	probeB, ackB := make(chan struct{}, 1), make(chan struct{}, 1)

	makeHandler := func(probeCh, ackCh chan struct{}) UDPHandler {
		return udpHandlerFunc(func(c UDPConn, data []byte, from netip.AddrPort) {
			switch string(data) {
			case "probe":
				select {
				case probeCh <- struct{}{}:
				default:
				}
				c.WriteToUDPAddrPort([]byte("ack"), from)
			case "ack":
				select {
				case ackCh <- struct{}{}:
				default:
				}
			}
		})
	}

	srvA := startUDPServer(t, makeHandler(probeA, ackA), UDPOptions{})
	srvB := startUDPServer(t, makeHandler(probeB, ackB), UDPOptions{})

	if _, err := srvA.Conn().WriteToUDPAddrPort([]byte("probe"), udpAddrPort(t, srvB.Addr())); err != nil {
		t.Fatalf("srvA initial punch write: %v", err)
	}
	if _, err := srvB.Conn().WriteToUDPAddrPort([]byte("probe"), udpAddrPort(t, srvA.Addr())); err != nil {
		t.Fatalf("srvB initial punch write: %v", err)
	}

	timer := time.NewTimer(time.Until(testDeadline(t)))
	defer timer.Stop()
	for _, ch := range []chan struct{}{probeA, ackA, probeB, ackB} {
		select {
		case <-ch:
		case <-timer.C:
			t.Fatal("timed out waiting for hole-punch probe/ack exchange")
		}
	}
}

func TestUDPShutdown(t *testing.T) {
	srv, err := NewUDPServer("127.0.0.1:0", udpHandlerFunc(echoUDP), UDPOptions{})
	if err != nil {
		t.Fatal(err)
	}
	served := make(chan error, 1)
	go func() { served <- srv.Serve() }()

	time.Sleep(5 * time.Millisecond)
	if err := srv.Close(); err != nil {
		t.Fatal(err)
	}
	if err := <-served; !errors.Is(err, ErrServerClosed) {
		t.Fatalf("Serve returned %v, want ErrServerClosed", err)
	}
	if err := srv.Serve(); err == nil {
		t.Fatal("second Serve succeeded, want error")
	}

	// Closing before Serve ever ran must also release sockets cleanly.
	unserved, err := NewUDPServer("127.0.0.1:0", udpHandlerFunc(echoUDP), UDPOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if err := unserved.Close(); err != nil {
		t.Fatal(err)
	}
	if err := unserved.Serve(); !errors.Is(err, ErrServerClosed) {
		t.Fatalf("Serve after Close returned %v, want ErrServerClosed", err)
	}
	if _, err := unserved.Conn().WriteToUDPAddrPort([]byte("x"), netip.MustParseAddrPort("127.0.0.1:9")); !errors.Is(err, net.ErrClosed) {
		t.Fatalf("WriteToUDPAddrPort on closed unserved server returned %v, want net.ErrClosed", err)
	}
}
