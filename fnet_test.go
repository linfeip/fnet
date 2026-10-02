package fnet

import (
	"bytes"
	"encoding/binary"
	"errors"
	"io"
	"net"
	"os"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/linfeip/fnet/internal/units"
)

// funcHandler assembles a Handler from function fields; callbacks that are not set are no-ops.
type funcHandler struct {
	open  func(c Conn)
	data  func(c Conn, b []byte) int
	close func(c Conn, err error)
}

func (h *funcHandler) OnOpen(c Conn) {
	if h.open != nil {
		h.open(c)
	}
}

func (h *funcHandler) OnData(c Conn, b []byte) int {
	if h.data != nil {
		return h.data(c, b)
	}
	return len(b)
}

func (h *funcHandler) OnClose(c Conn, err error) {
	if h.close != nil {
		h.close(c, err)
	}
}

func startServer(t *testing.T, h Handler) *Server {
	t.Helper()
	return startServerWith(t, h, Options{NumLoops: 2})
}

func startServerWith(t *testing.T, h Handler, opts Options) *Server {
	t.Helper()
	srv, err := NewServer("127.0.0.1:0", h, opts)
	if err != nil {
		t.Fatal(err)
	}
	served := make(chan error, 1)
	go func() { served <- srv.Serve() }()
	t.Cleanup(func() {
		srv.Close()
		if err := <-served; !errors.Is(err, ErrServerClosed) {
			t.Errorf("Serve 返回 %v, 期望 ErrServerClosed", err)
		}
	})
	return srv
}

func dial(t *testing.T, srv *Server) net.Conn {
	t.Helper()
	c, err := net.Dial("tcp", srv.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { c.Close() })
	return c
}

// echo echoes back the data it receives.
func echo(c Conn, b []byte) int {
	c.Write(b)
	return len(b)
}

func TestEcho(t *testing.T) {
	srv := startServer(t, &funcHandler{data: func(c Conn, b []byte) int {
		c.Write(b)
		return len(b)
	}})
	var wg sync.WaitGroup
	for i := range 50 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			c := dial(t, srv)
			for j := range 100 {
				msg := []byte("hello-" + string(rune('a'+i%26)) + "-" + string(rune('a'+j%26)))
				if _, err := c.Write(msg); err != nil {
					t.Error(err)
					return
				}
				got := make([]byte, len(msg))
				if _, err := io.ReadFull(c, got); err != nil || !bytes.Equal(got, msg) {
					t.Errorf("echo 不一致: got %q err %v", got, err)
					return
				}
			}
		}()
	}
	wg.Wait()
}

// TestPartialConsume consumes only complete frames (4-byte length prefix) and verifies that unconsumed
// data is kept and joined with the data that follows.
func TestPartialConsume(t *testing.T) {
	srv := startServer(t, &funcHandler{data: func(c Conn, b []byte) int {
		consumed := 0
		for len(b)-consumed >= 4 {
			n := int(binary.BigEndian.Uint32(b[consumed:]))
			if len(b)-consumed-4 < n {
				break
			}
			c.Write(b[consumed+4 : consumed+4+n])
			consumed += 4 + n
		}
		return consumed
	}})
	c := dial(t, srv)
	var want, stream []byte
	for i := range 200 {
		payload := bytes.Repeat([]byte{byte(i)}, i*37%1000+1)
		want = append(want, payload...)
		stream = binary.BigEndian.AppendUint32(stream, uint32(len(payload)))
		stream = append(stream, payload...)
	}
	go func() {
		// Send in irregular small chunks so that frames end up split.
		for len(stream) > 0 {
			n := min(len(stream), 1+len(stream)%97)
			c.Write(stream[:n])
			stream = stream[n:]
		}
	}()
	got := make([]byte, len(want))
	if _, err := io.ReadFull(c, got); err != nil || !bytes.Equal(got, want) {
		t.Fatalf("帧数据不一致, err=%v", err)
	}
}

// TestLargeWriteThenClose writes far more data than the kernel buffer holds in OnOpen and then closes
// immediately: the data must be delivered in full before the connection is closed.
func TestLargeWriteThenClose(t *testing.T) {
	payload := make([]byte, 16*units.MB)
	for i := range payload {
		payload[i] = byte(i * 7)
	}
	srv := startServer(t, &funcHandler{open: func(c Conn) {
		c.Write(payload)
		c.Close()
	}})
	c := dial(t, srv)
	time.Sleep(100 * time.Millisecond) // don't read yet, let the server's send buffer back up
	got, err := io.ReadAll(c)
	if err != nil || !bytes.Equal(got, payload) {
		t.Fatalf("收到 %d 字节, 期望 %d, err=%v", len(got), len(payload), err)
	}
}

// TestWritev uses more segments than IOV_MAX with empty segments interspersed: the excess segments go
// into the send buffer and are sent in order by the connection's task.
func TestWritev(t *testing.T) {
	var bs [][]byte
	var want []byte
	for i := range 3000 {
		segment := []byte{byte(i)}
		if i%3 == 0 {
			segment = nil
		}
		bs = append(bs, segment)
		want = append(want, segment...)
	}
	srv := startServer(t, &funcHandler{open: func(c Conn) {
		if n, err := c.Writev(bs); n != len(want) || err != nil {
			t.Errorf("Writev = %d, %v, 期望 %d, nil", n, err, len(want))
		}
		if !bytes.Equal(bytes.Join(bs, nil), want) {
			t.Error("Writev 修改了 bs")
		}
		c.Close()
	}})
	c := dial(t, srv)
	got, err := io.ReadAll(c)
	if err != nil || !bytes.Equal(got, want) {
		t.Fatalf("收到 %d 字节, 期望 %d, err=%v", len(got), len(want), err)
	}
}

// TestLargeWritevThenClose uses Writev to write multiple segments far exceeding the kernel buffer: the
// part not written out must go into the send buffer in order, ahead of the Write that follows.
func TestLargeWritevThenClose(t *testing.T) {
	var segments [][]byte
	var want []byte
	for i := range 40 {
		segment := bytes.Repeat([]byte{byte(i)}, i%7*100*units.KB) // empty segment when i%7 == 0
		segments = append(segments, segment)
		want = append(want, segment...)
	}
	tail := []byte("tail")
	want = append(want, tail...)
	srv := startServer(t, &funcHandler{open: func(c Conn) {
		if n, err := c.Writev(segments); n != len(want)-len(tail) || err != nil {
			t.Errorf("Writev = %d, %v, 期望 %d, nil", n, err, len(want)-len(tail))
		}
		c.Write(tail)
		c.Close()
	}})
	c := dial(t, srv)
	time.Sleep(100 * time.Millisecond) // don't read yet, let the server's send buffer back up
	got, err := io.ReadAll(c)
	if err != nil || !bytes.Equal(got, want) {
		t.Fatalf("收到 %d 字节, 期望 %d, err=%v", len(got), len(want), err)
	}
}

// TestWriteFromOtherGoroutines simulates application goroutines: writing and closing concurrently from
// outside the connection's task.
func TestWriteFromOtherGoroutines(t *testing.T) {
	srv := startServer(t, &funcHandler{data: func(c Conn, b []byte) int {
		msg := bytes.Clone(b)
		go func() {
			var wg sync.WaitGroup
			for range 10 {
				wg.Add(1)
				go func() { defer wg.Done(); c.Write(msg) }()
			}
			wg.Wait()
			c.Close()
		}()
		return len(b)
	}})
	c := dial(t, srv)
	c.Write([]byte("0123456789"))
	got, err := io.ReadAll(c)
	if err != nil || len(got) != 100 {
		t.Fatalf("收到 %d 字节, err=%v", len(got), err)
	}
}

// TestPauseRead verifies that OnData is not called while reading is paused, that writes still work as
// usual, and that the peer blocks once it fills the kernel buffer; after resuming, the data arrives in full.
func TestPauseRead(t *testing.T) {
	out := bytes.Repeat([]byte("0123456789abcdef"), units.MB) // 16MB, far beyond the kernel buffer; written out via the send buffer while reading is paused
	in := make([]byte, 32*units.MB)
	var received atomic.Int64
	done := make(chan struct{})
	opened := make(chan Conn, 1)
	srv := startServer(t, &funcHandler{
		open: func(c Conn) {
			c.PauseRead()
			opened <- c
			c.Write(out)
		},
		data: func(c Conn, b []byte) int {
			if received.Add(int64(len(b))) == int64(len(in)) {
				close(done)
			}
			return len(b)
		},
	})
	c := dial(t, srv)
	sc := <-opened
	c.SetDeadline(time.Now().Add(5 * time.Second))
	got := make([]byte, len(out))
	if _, err := io.ReadFull(c, got); err != nil || !bytes.Equal(got, out) {
		t.Fatalf("暂停读取期间写出的数据不一致, err=%v", err)
	}

	c.SetWriteDeadline(time.Now().Add(500 * time.Millisecond))
	n, err := c.Write(in)
	if !errors.Is(err, os.ErrDeadlineExceeded) {
		t.Fatalf("对端写入应被阻塞, 写出 %d 字节, err=%v", n, err)
	}
	if got := received.Load(); got != 0 {
		t.Fatalf("暂停期间回调了 OnData, 收到 %d 字节", got)
	}

	sc.ResumeRead()
	c.SetWriteDeadline(time.Now().Add(5 * time.Second))
	if _, err := c.Write(in[n:]); err != nil {
		t.Fatal(err)
	}
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatalf("恢复读取后只收到 %d 字节, 期望 %d", received.Load(), len(in))
	}
}

func TestCloseReasons(t *testing.T) {
	reasons := make(chan error, 10)
	opened := make(chan Conn, 10)
	srv := startServer(t, &funcHandler{
		open:  func(c Conn) { opened <- c },
		close: func(c Conn, err error) { reasons <- err },
	})

	// peer closes -> io.EOF
	c := dial(t, srv)
	<-opened
	c.Close()
	if err := <-reasons; err != io.EOF {
		t.Fatalf("对端关闭: %v", err)
	}

	// local side closes -> nil, and writing after the close returns net.ErrClosed
	dial(t, srv)
	sc := <-opened
	sc.Close()
	if err := <-reasons; err != nil {
		t.Fatalf("主动关闭: %v", err)
	}
	if _, err := sc.Write([]byte("x")); !errors.Is(err, net.ErrClosed) {
		t.Fatalf("关闭后写入: %v", err)
	}
	if _, err := sc.Writev([][]byte{[]byte("x")}); !errors.Is(err, net.ErrClosed) {
		t.Fatalf("关闭后 Writev: %v", err)
	}
}

// TestAddrs verifies RemoteAddr returns the peer address both in OnOpen and in OnClose where the
// connection is already closed, and that LocalAddr returns the local side's address.
func TestAddrs(t *testing.T) {
	for _, addr := range []string{"127.0.0.1:0", "[::1]:0"} {
		t.Run(addr, func(t *testing.T) {
			opened := make(chan [2]string, 1) // local side, peer
			closed := make(chan string, 1)
			srv, err := NewServer(addr, &funcHandler{
				open:  func(c Conn) { opened <- [2]string{c.LocalAddr().String(), c.RemoteAddr().String()} },
				close: func(c Conn, err error) { closed <- c.RemoteAddr().String() },
			}, Options{NumLoops: 1})
			if err != nil {
				t.Skip(err) // this machine does not support that address family
			}
			served := make(chan error, 1)
			go func() { served <- srv.Serve() }()
			t.Cleanup(func() { srv.Close(); <-served })

			c := dial(t, srv)
			if got := <-opened; got != [2]string{c.RemoteAddr().String(), c.LocalAddr().String()} {
				t.Fatalf("OnOpen 中 LocalAddr/RemoteAddr = %v, 期望 %v/%v", got, c.RemoteAddr(), c.LocalAddr())
			}
			c.Close()
			if got := <-closed; got != c.LocalAddr().String() {
				t.Fatalf("OnClose 中 RemoteAddr = %s, 期望 %s", got, c.LocalAddr())
			}
		})
	}
}

func TestServerClose(t *testing.T) {
	var closed atomic.Int32
	opened := make(chan struct{}, 20)
	srv, err := NewServer("127.0.0.1:0", &funcHandler{
		open: func(c Conn) { opened <- struct{}{} },
		close: func(c Conn, err error) {
			if errors.Is(err, ErrServerClosed) {
				closed.Add(1)
			}
		},
	}, Options{})
	if err != nil {
		t.Fatal(err)
	}
	served := make(chan error, 1)
	go func() { served <- srv.Serve() }()
	var clients []net.Conn
	for range 20 {
		c, err := net.Dial("tcp", srv.Addr().String())
		if err != nil {
			t.Fatal(err)
		}
		defer c.Close()
		clients = append(clients, c)
		<-opened
	}
	srv.Close() // every OnClose has already run by the time it returns
	if n := closed.Load(); n != 20 {
		t.Fatalf("OnClose(ErrServerClosed) 次数 = %d", n)
	}
	if err := <-served; !errors.Is(err, ErrServerClosed) {
		t.Fatalf("Serve 返回 %v", err)
	}
	for _, c := range clients {
		c.SetReadDeadline(time.Now().Add(time.Second))
		if _, err := c.Read(make([]byte, 1)); err != io.EOF {
			t.Fatalf("客户端应读到 EOF, got %v", err)
		}
	}
	// A Server that was never Served can also be Closed directly.
	s2, err := NewServer("127.0.0.1:0", &funcHandler{}, Options{})
	if err != nil {
		t.Fatal(err)
	}
	s2.Close()
	if err := s2.Serve(); !errors.Is(err, ErrServerClosed) {
		t.Fatalf("Close 之后 Serve 返回 %v, 期望 ErrServerClosed", err)
	}
}

// TestDefaultNumLoopsFollowsGOMAXPROCS verifies the default number of event loops is bounded by the Ps the
// process may actually use: GOMAXPROCS can be lower than runtime.NumCPU() (taskset, a container CPU quota, an
// explicit setting), and loops beyond the Ps only compete with the tasks for them.
func TestDefaultNumLoopsFollowsGOMAXPROCS(t *testing.T) {
	defer runtime.GOMAXPROCS(runtime.GOMAXPROCS(3))
	if got := (Options{}).withDefaults().NumLoops; got != 3 {
		t.Fatalf("默认 NumLoops = %d, 期望 GOMAXPROCS(0) = 3", got)
	}
	if got := (Options{NumLoops: 5}).withDefaults().NumLoops; got != 5 {
		t.Fatalf("显式 NumLoops 被覆盖: %d", got)
	}
}

// TestDeadline verifies a connection whose deadline expires is closed with os.ErrDeadlineExceeded, while
// a connection whose deadline was cleared is unaffected.
func TestDeadline(t *testing.T) {
	var count atomic.Int32
	opened := make(chan struct{}, 2)
	reasons := make(chan error, 2)
	srv := startServer(t, &funcHandler{
		open: func(c Conn) {
			c.SetDeadline(time.Now().Add(100 * time.Millisecond))
			if count.Add(1) == 2 {
				c.SetDeadline(time.Time{})
			}
			opened <- struct{}{}
		},
		data: func(c Conn, b []byte) int {
			c.Write(b)
			return len(b)
		},
		close: func(c Conn, err error) { reasons <- err },
	})
	expired := dial(t, srv)
	<-opened
	kept := dial(t, srv)
	<-opened

	select {
	case err := <-reasons:
		if err != os.ErrDeadlineExceeded {
			t.Fatalf("期限到期: OnClose(%v)", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("期限到期后连接未关闭")
	}
	expired.SetReadDeadline(time.Now().Add(time.Second))
	if _, err := expired.Read(make([]byte, 1)); err != io.EOF {
		t.Fatalf("客户端应读到 EOF, got %v", err)
	}
	kept.SetDeadline(time.Now().Add(time.Second))
	kept.Write([]byte("x"))
	if _, err := io.ReadFull(kept, make([]byte, 1)); err != nil {
		t.Fatalf("取消期限的连接: %v", err)
	}
}

// TestSerialCallbacks verifies the callbacks of one connection run serially, OnOpen first and OnClose
// last, even when events arrive while a callback is running.
func TestSerialCallbacks(t *testing.T) {
	type state struct {
		active atomic.Int32
		closed atomic.Bool
		events atomic.Int32 // number of callbacks that have run
	}
	enter := func(t *testing.T, c Conn, first bool) {
		st := c.Context().(*state)
		if st.active.Add(1) != 1 {
			t.Error("同一连接的回调并发执行")
		}
		if st.closed.Load() {
			t.Error("OnClose 之后仍有回调")
		}
		if (st.events.Add(1) == 1) != first {
			t.Error("OnOpen 不是第一个回调")
		}
	}
	closed := make(chan struct{})
	srv := startServer(t, &funcHandler{
		open: func(c Conn) {
			c.SetContext(&state{})
			enter(t, c, true)
			time.Sleep(20 * time.Millisecond) // the client has already sent data while OnOpen runs
			c.Context().(*state).active.Add(-1)
		},
		data: func(c Conn, b []byte) int {
			enter(t, c, false)
			time.Sleep(time.Millisecond) // more data arrives while the callback runs
			c.Context().(*state).active.Add(-1)
			return len(b)
		},
		close: func(c Conn, err error) {
			enter(t, c, false)
			c.Context().(*state).closed.Store(true)
			close(closed)
		},
	})
	c := dial(t, srv)
	for range 100 {
		c.Write([]byte("x"))
		time.Sleep(200 * time.Microsecond)
	}
	c.Close()
	<-closed
}

// TestReadBufferSmaller sends far more data than the read buffer holds: edge-triggered mode does not
// notify again for data that already arrived, so once the buffer is full the task has to keep reading on
// its own until everything is read.
func TestReadBufferSmaller(t *testing.T) {
	srv := startServerWith(t, &funcHandler{data: echo}, Options{NumLoops: 1, ReadBufferSize: 512})
	c := dial(t, srv)
	msg := make([]byte, 4*units.MB)
	for i := range msg {
		msg[i] = byte(i * 31)
	}
	go c.Write(msg)
	got := make([]byte, len(msg))
	c.SetReadDeadline(time.Now().Add(10 * time.Second))
	if _, err := io.ReadFull(c, got); err != nil || !bytes.Equal(got, msg) {
		t.Fatalf("回显的数据不一致, err=%v", err)
	}
}

// TestCloseInCallback calls Close inside a callback: the reply already written is sent in full before the
// connection closes, and writes after that return net.ErrClosed.
func TestCloseInCallback(t *testing.T) {
	srv := startServer(t, &funcHandler{data: func(c Conn, b []byte) int {
		c.Write(b)
		c.Close()
		if _, err := c.Write(b); !errors.Is(err, net.ErrClosed) {
			t.Errorf("Close 之后写入: %v", err)
		}
		return len(b)
	}})
	c := dial(t, srv)
	c.Write([]byte("bye"))
	got, err := io.ReadAll(c)
	if err != nil || string(got) != "bye" {
		t.Fatalf("收到 %q, err=%v", got, err)
	}
}

// TestCloseAfterData has the peer close right after sending data: the FIN may be merged into the same
// notification as the data, so after reading the data the connection must still read EOF and close.
func TestCloseAfterData(t *testing.T) {
	const clients = 500
	var received, closed atomic.Int32
	srv := startServer(t, &funcHandler{
		data: func(c Conn, b []byte) int {
			received.Add(int32(len(b)))
			return len(b)
		},
		close: func(c Conn, err error) {
			if err == io.EOF {
				closed.Add(1)
			}
		},
	})
	for range clients {
		c := dial(t, srv)
		c.Write([]byte("x"))
		c.Close()
	}
	deadline := time.Now().Add(5 * time.Second)
	for received.Load() != clients || closed.Load() != clients {
		if time.Now().After(deadline) {
			t.Fatalf("收到 %d 字节, %d 个连接以 EOF 关闭, 期望都是 %d", received.Load(), closed.Load(), clients)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// TestCloseWhilePaused has the peer send data and close while reading is paused: after reading resumes,
// the data comes first, then EOF is read and the connection is closed.
func TestCloseWhilePaused(t *testing.T) {
	opened := make(chan Conn, 1)
	var received atomic.Int32
	reasons := make(chan error, 1)
	srv := startServer(t, &funcHandler{
		open: func(c Conn) {
			c.PauseRead()
			opened <- c
		},
		data: func(c Conn, b []byte) int {
			received.Add(int32(len(b)))
			return len(b)
		},
		close: func(c Conn, err error) { reasons <- err },
	})
	c := dial(t, srv)
	sc := <-opened
	c.Write([]byte("x"))
	c.Close()
	time.Sleep(100 * time.Millisecond) // notifications for both the data and the FIN arrived, but reading is paused
	sc.ResumeRead()
	select {
	case err := <-reasons:
		if err != io.EOF || received.Load() != 1 {
			t.Fatalf("OnClose(%v), 收到 %d 字节", err, received.Load())
		}
	case <-time.After(5 * time.Second):
		t.Fatalf("恢复读取后没有读到对端的关闭, 收到 %d 字节", received.Load())
	}
}

// TestPeerCloseKeepsBacklog has the peer shut down only the write direction (shutdown(SHUT_WR)) after
// sending its data while still reading: data written in OnData that is still in the send buffer must keep
// being sent after EOF is read and only then is the connection closed; OnClose's err is io.EOF, and the
// data must not be discarded because of the EOF.
func TestPeerCloseKeepsBacklog(t *testing.T) {
	payload := make([]byte, 8*units.MB) // far beyond the kernel buffer, so the send buffer must have a backlog
	for i := range payload {
		payload[i] = byte(i * 7)
	}
	reasons := make(chan error, 1)
	srv := startServer(t, &funcHandler{
		data: func(c Conn, b []byte) int {
			c.Write(payload)
			return len(b)
		},
		close: func(c Conn, err error) { reasons <- err },
	})
	c := dial(t, srv).(*net.TCPConn)
	c.Write([]byte("go"))
	c.CloseWrite()
	time.Sleep(100 * time.Millisecond) // don't read yet, leave the backlog on the server
	c.SetReadDeadline(time.Now().Add(5 * time.Second))
	got, err := io.ReadAll(c)
	if err != nil || !bytes.Equal(got, payload) {
		t.Fatalf("收到 %d 字节, 期望 %d, err=%v", len(got), len(payload), err)
	}
	select {
	case err := <-reasons:
		if err != io.EOF {
			t.Fatalf("OnClose(%v), 期望 io.EOF", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("没有回调 OnClose")
	}
}

// TestCloseThenPeerClose reads the peer's EOF only after a local Close: the data in the send buffer must
// still be sent in full, the EOF does not override the pending close request, and OnClose's err stays nil.
func TestCloseThenPeerClose(t *testing.T) {
	payload := make([]byte, 16*units.MB)
	for i := range payload {
		payload[i] = byte(i * 7)
	}
	reasons := make(chan error, 1)
	srv := startServer(t, &funcHandler{
		open: func(c Conn) {
			c.Write(payload)
			c.Close()
		},
		close: func(c Conn, err error) { reasons <- err },
	})
	c := dial(t, srv).(*net.TCPConn)
	c.CloseWrite() // when the server reads EOF, the data in the send buffer has not been fully sent yet
	time.Sleep(100 * time.Millisecond)
	c.SetReadDeadline(time.Now().Add(5 * time.Second))
	got, err := io.ReadAll(c)
	if err != nil || !bytes.Equal(got, payload) {
		t.Fatalf("收到 %d 字节, 期望 %d, err=%v", len(got), len(payload), err)
	}
	select {
	case err := <-reasons:
		if err != nil {
			t.Fatalf("OnClose(%v), 期望 nil", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("没有回调 OnClose")
	}
}

// TestBackloggedLeftoverIsSmall verifies a backed-up connection only holds memory for the leftover half
// frame: when the read buffer fills up the frame is almost always truncated, and the leftover data must not
// occupy another buffer-sized allocation (which would exhaust memory with many connections).
func TestBackloggedLeftoverIsSmall(t *testing.T) {
	const (
		conns  = 300
		stalls = 4 // pause reading after this many callbacks per connection, leaving the leftover data in c.in
	)
	var stalled sync.WaitGroup
	stalled.Add(conns)
	srv := startServerWith(t, &funcHandler{
		open: func(c Conn) { c.SetContext(new(int)) },
		data: func(c Conn, b []byte) int {
			consumed := 0
			for len(b)-consumed >= 4 { // frames with a 4-byte length prefix; consume only complete frames
				n := int(binary.BigEndian.Uint32(b[consumed:]))
				if len(b)-consumed-4 < n {
					break
				}
				consumed += 4 + n
			}
			if calls := c.Context().(*int); *calls < stalls {
				if *calls++; *calls == stalls {
					c.PauseRead()
					stalled.Done()
				}
			}
			return consumed
		},
	}, Options{NumLoops: 2, ReadBufferSize: 16 * units.KB})

	clients := make([]net.Conn, conns)
	for i := range clients {
		clients[i] = dial(t, srv)
	}
	time.Sleep(100 * time.Millisecond) // all connections are established
	var before, after runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&before)

	var stream []byte
	for range 200 { // about 200KB: every read fills the buffer
		stream = binary.BigEndian.AppendUint32(stream, 996)
		stream = append(stream, make([]byte, 996)...)
	}
	for _, c := range clients {
		c.Write(stream)
	}
	stalled.Wait()
	runtime.GC()
	runtime.ReadMemStats(&after)
	if perConn := (int64(after.HeapInuse) - int64(before.HeapInuse)) / conns; perConn > 8*units.KB {
		t.Fatalf("每个积压的连接占用 %dB，期望只为残留的半个帧占用内存", perConn)
	}
}
