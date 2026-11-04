package fkcp

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/linfeip/fnet"
)

type funcHandler struct {
	open  func(c *Conn)
	data  func(c *Conn, data []byte)
	close func(c *Conn, err error)
}

func (h *funcHandler) OnOpen(c *Conn) {
	if h.open != nil {
		h.open(c)
	}
}

func (h *funcHandler) OnData(c *Conn, data []byte) {
	if h.data != nil {
		h.data(c, data)
	}
}

func (h *funcHandler) OnClose(c *Conn, err error) {
	if h.close != nil {
		h.close(c, err)
	}
}

func startTestServer(t *testing.T, h Handler, opts Options) *Server {
	t.Helper()
	if opts.UDP.NumLoops == 0 {
		opts.UDP.NumLoops = 2
	}
	srv, err := NewServer("127.0.0.1:0", h, opts)
	if err != nil {
		t.Fatal(err)
	}
	served := make(chan error, 1)
	go func() { served <- srv.Serve() }()
	t.Cleanup(func() {
		srv.Close()
		if err := <-served; !errors.Is(err, fnet.ErrServerClosed) {
			t.Errorf("Serve returned %v, want ErrServerClosed", err)
		}
	})
	return srv
}

// testClient wraps a UDP socket and an arqState so fkcp tests have zero external dependencies.
type testClient struct {
	udp   *net.UDPConn
	mu    sync.Mutex
	arq   arqState
	start time.Time
	done  chan struct{}
	inbox chan []byte
}

func dialTestClient(t *testing.T, addr string, conv uint32, streamMode bool) *testClient {
	t.Helper()
	raddr, err := net.ResolveUDPAddr("udp", addr)
	if err != nil {
		t.Fatal(err)
	}
	udp, err := net.DialUDP("udp", nil, raddr)
	if err != nil {
		t.Fatal(err)
	}
	tc := &testClient{
		udp:   udp,
		start: time.Now(),
		done:  make(chan struct{}),
		inbox: make(chan []byte, 256),
	}
	tc.arq.init(arqConfig{
		conversationID: conv,
		mtu:            defaultMTU,
		sendWindow:     128,
		receiveWindow:  128,
		interval:       10,
		noDelay:        true,
		streamMode:     streamMode,
	}, func(pkt []byte) {
		_, _ = udp.Write(pkt)
	})

	go tc.readLoop()
	go tc.tickLoop()
	t.Cleanup(func() { tc.close() })
	return tc
}

func (c *testClient) nowMs() uint32 {
	return uint32(time.Since(c.start).Milliseconds())
}

func (c *testClient) write(b []byte) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if err := c.arq.send(b); err != nil {
		return err
	}
	c.arq.flush(c.nowMs(), false)
	return nil
}

func (c *testClient) readMessage(t *testing.T) []byte {
	t.Helper()
	timeout := 5 * time.Second
	if d, ok := t.Deadline(); ok {
		timeout = time.Until(d)
	}
	select {
	case msg := <-c.inbox:
		return msg
	case <-time.After(timeout):
		t.Fatal("timed out waiting for KCP message")
		return nil
	}
}

func (c *testClient) readLoop() {
	buf := make([]byte, 64<<10)
	for {
		n, err := c.udp.Read(buf)
		if err != nil {
			return
		}
		var delivered [][]byte
		c.mu.Lock()
		now := c.nowMs()
		if c.arq.input(buf[:n], now) {
			c.arq.flush(now, false)
		}
		for {
			msg, ok := c.arq.popMessage()
			if !ok {
				break
			}
			cp := bytes.Clone(msg.Bytes())
			msg.Release()
			delivered = append(delivered, cp)
		}
		if len(c.arq.pendingAcks) > 0 {
			c.arq.flush(now, len(c.arq.sendQueue) == 0)
		}
		c.mu.Unlock()
		for _, m := range delivered {
			c.inbox <- m
		}
	}
}

func (c *testClient) tickLoop() {
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			c.mu.Lock()
			c.arq.update(c.nowMs())
			c.mu.Unlock()
		case <-c.done:
			return
		}
	}
}

func (c *testClient) close() {
	select {
	case <-c.done:
		return
	default:
		close(c.done)
		c.udp.Close()
		c.mu.Lock()
		c.arq.release()
		c.mu.Unlock()
	}
}

func TestARQLossAndReorder(t *testing.T) {
	for _, streamMode := range []bool{false, true} {
		t.Run(fmt.Sprintf("stream=%v", streamMode), func(t *testing.T) {
			var (
				aToB [][]byte
				bToA [][]byte
				seq  int
				a, b arqState
			)
			deliver := func(queue *[][]byte, pkt []byte) {
				seq++
				cp := bytes.Clone(pkt)
				switch seq % 5 {
				case 0:
					return // drop 20% of packets
				case 1:
					// duplicate packet
					*queue = append(*queue, cp, bytes.Clone(cp))
				default:
					*queue = append(*queue, cp)
					// swap last two packets to force out-of-order arrival
					if n := len(*queue); n >= 2 {
						(*queue)[n-1], (*queue)[n-2] = (*queue)[n-2], (*queue)[n-1]
					}
				}
			}

			cfg := arqConfig{
				conversationID: 42,
				mtu:            512,
				sendWindow:     128,
				receiveWindow:  128,
				interval:       10,
				noDelay:        true,
				streamMode:     streamMode,
			}
			a.init(cfg, func(pkt []byte) { deliver(&aToB, pkt) })
			b.init(cfg, func(pkt []byte) { deliver(&bToA, pkt) })
			defer a.release()
			defer b.release()

			want := make([]byte, 32<<10)
			for i := range want {
				want[i] = byte(i*29 + 11)
			}
			if err := a.send(want); err != nil {
				t.Fatal(err)
			}

			var got []byte
			for nowMs := uint32(0); nowMs < 20000 && len(got) < len(want); nowMs += 10 {
				a.update(nowMs)
				b.update(nowMs)
				for _, pkt := range aToB {
					b.input(pkt, nowMs)
				}
				aToB = aToB[:0]
				for {
					msg, ok := b.popMessage()
					if !ok {
						break
					}
					got = append(got, msg.Bytes()...)
					msg.Release()
				}
				for _, pkt := range bToA {
					a.input(pkt, nowMs)
				}
				bToA = bToA[:0]
			}

			if !bytes.Equal(got, want) {
				t.Fatalf("received %d bytes (equal=%v), want %d bytes", len(got), bytes.Equal(got, want), len(want))
			}
		})
	}
}

func TestServerEchoAndFragmentation(t *testing.T) {
	srv := startTestServer(t, &funcHandler{
		data: func(c *Conn, data []byte) {
			if _, err := c.Write(data); err != nil {
				t.Errorf("Conn.Write: %v", err)
			}
		},
	}, Options{NoDelay: true})

	client := dialTestClient(t, srv.Addr().String(), 1001, false)
	for _, size := range []int{0, 1, 512, 1400, 32 << 10} {
		want := make([]byte, size)
		for i := range want {
			want[i] = byte(i*13 + size)
		}
		if err := client.write(want); err != nil {
			t.Fatalf("size %d write: %v", size, err)
		}
		got := client.readMessage(t)
		if !bytes.Equal(got, want) {
			t.Fatalf("size %d: got %d bytes, want %d bytes", size, len(got), len(want))
		}
	}
}

func TestServerConcurrentClients(t *testing.T) {
	srv := startTestServer(t, &funcHandler{
		data: func(c *Conn, data []byte) {
			_, _ = c.Write(data)
		},
	}, Options{
		NoDelay: true,
		UDP:     fnet.UDPOptions{ReusePort: true},
	})

	var wg sync.WaitGroup
	for i := range 32 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			client := dialTestClient(t, srv.Addr().String(), uint32(2000+i), false)
			for j := range 50 {
				msg := []byte(fmt.Sprintf("peer-%d-msg-%d", i, j))
				if err := client.write(msg); err != nil {
					t.Errorf("peer %d write: %v", i, err)
					return
				}
				got := client.readMessage(t)
				if !bytes.Equal(got, msg) {
					t.Errorf("peer %d msg %d: got %q, want %q", i, j, got, msg)
					return
				}
			}
		}()
	}
	wg.Wait()
}

func TestSessionCapAndIdleEviction(t *testing.T) {
	closedReasons := make(chan error, 4)
	srv := startTestServer(t, &funcHandler{
		data:  func(c *Conn, data []byte) { _, _ = c.Write(data) },
		close: func(_ *Conn, err error) { closedReasons <- err },
	}, Options{
		NoDelay:     true,
		Interval:    10 * time.Millisecond,
		IdleTimeout: 80 * time.Millisecond,
		MaxSessions: 2,
	})

	c1 := dialTestClient(t, srv.Addr().String(), 1, false)
	c2 := dialTestClient(t, srv.Addr().String(), 2, false)
	for _, c := range []*testClient{c1, c2} {
		if err := c.write([]byte("hi")); err != nil {
			t.Fatal(err)
		}
		if got := string(c.readMessage(t)); got != "hi" {
			t.Fatalf("got %q, want hi", got)
		}
	}
	if got := srv.SessionCount(); got != 2 {
		t.Fatalf("SessionCount = %d, want 2", got)
	}

	// A third session exceeds MaxSessions=2 and must be ignored without growing SessionCount.
	raddr, _ := net.ResolveUDPAddr("udp", srv.Addr().String())
	extra, err := net.DialUDP("udp", nil, raddr)
	if err != nil {
		t.Fatal(err)
	}
	defer extra.Close()
	var fakeHeader [headerSize]byte
	fakeHeader[0] = 3
	fakeHeader[4] = cmdPush
	_, _ = extra.Write(fakeHeader[:])
	time.Sleep(20 * time.Millisecond)
	if got := srv.SessionCount(); got != 2 {
		t.Fatalf("SessionCount after cap = %d, want 2", got)
	}

	// Stop client traffic and verify both sessions are evicted with os.ErrDeadlineExceeded.
	for range 2 {
		select {
		case err := <-closedReasons:
			if !errors.Is(err, os.ErrDeadlineExceeded) {
				t.Fatalf("idle close reason = %v, want os.ErrDeadlineExceeded", err)
			}
		case <-time.After(2 * time.Second):
			t.Fatal("timed out waiting for idle session eviction")
		}
	}
	if got := srv.SessionCount(); got != 0 {
		t.Fatalf("SessionCount after eviction = %d, want 0", got)
	}
}

func TestCloseInsideCallbackAndReset(t *testing.T) {
	var (
		eventsMu sync.Mutex
		events   []string
		reasons  = make(chan error, 2)
		closedC  atomic.Pointer[Conn]
	)
	record := func(ev string) {
		eventsMu.Lock()
		events = append(events, ev)
		eventsMu.Unlock()
	}

	srv := startTestServer(t, &funcHandler{
		open: func(c *Conn) {
			c.SetContext("session-ctx")
			record("open")
		},
		data: func(c *Conn, data []byte) {
			record("data:" + string(data))
			if string(data) == "bye" {
				closedC.Store(c)
				_ = c.Close()
				record("after-close-call")
			} else {
				_, _ = c.Write(data)
			}
		},
		close: func(c *Conn, err error) {
			if c.Context() != "session-ctx" {
				t.Errorf("unexpected context %v", c.Context())
			}
			record("onclose")
			reasons <- err
		},
	}, Options{NoDelay: true})

	c1 := dialTestClient(t, srv.Addr().String(), 10, false)
	_ = c1.write([]byte("bye"))
	if err := <-reasons; err != nil {
		t.Fatalf("OnClose reason = %v, want nil", err)
	}
	if _, err := closedC.Load().Write([]byte("late")); !errors.Is(err, net.ErrClosed) {
		t.Fatalf("Write after Close returned %v, want net.ErrClosed", err)
	}

	eventsMu.Lock()
	wantOrder := []string{"open", "data:bye", "after-close-call", "onclose"}
	if len(events) != len(wantOrder) {
		t.Fatalf("callback sequence = %v, want %v", events, wantOrder)
	}
	for i := range wantOrder {
		if events[i] != wantOrder[i] {
			t.Fatalf("callback sequence = %v, want %v", events, wantOrder)
		}
	}
	eventsMu.Unlock()

	// Verify conversation reset from the exact same peer UDP address closes the old session with io.EOF.
	c2 := dialTestClient(t, srv.Addr().String(), 20, false)
	_ = c2.write([]byte("ping"))
	if got := string(c2.readMessage(t)); got != "ping" {
		t.Fatalf("got %q, want ping", got)
	}
	localAddr := c2.udp.LocalAddr().(*net.UDPAddr)
	c2.close()

	udpReuse, err := net.DialUDP("udp", localAddr, srv.Addr().(*net.UDPAddr))
	if err == nil {
		var resetSeg arqState
		resetSeg.init(arqConfig{conversationID: 21, mtu: 1400, sendWindow: 32, receiveWindow: 32, interval: 10, noDelay: true}, func(pkt []byte) {
			_, _ = udpReuse.Write(pkt)
		})
		_ = resetSeg.send([]byte("reconnected"))
		resetSeg.flush(10, false)
		resetSeg.release()
		udpReuse.Close()

		select {
		case err := <-reasons:
			if !errors.Is(err, io.EOF) {
				t.Fatalf("reset OnClose reason = %v, want io.EOF", err)
			}
		case <-time.After(2 * time.Second):
			t.Fatal("timed out waiting for io.EOF on conversation reset")
		}
	}
}
