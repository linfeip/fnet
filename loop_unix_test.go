//go:build linux || darwin

package fnet

import (
	"io"
	"net"
	"runtime"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/linfeip/fnet/poll"
)

// TestRunEventOwnLoopOnly checks that an event is delivered through the loop that owns the connection: a leftover
// event of another loop's poller for an fd that has since been reused by this connection must not notify it.
func TestRunEventOwnLoopOnly(t *testing.T) {
	var tasks atomic.Int32
	opened := make(chan Conn, 1)
	srv := startServerWith(t, &funcHandler{open: func(c Conn) { opened <- c }}, Options{NumLoops: 2, Executor: func(task func()) {
		tasks.Add(1)
		go task()
	}})
	dial(t, srv)
	c := (<-opened).(*conn)
	time.Sleep(100 * time.Millisecond) // let the connection settle: its open task has finished
	own, other := srv.loops[0], srv.loops[1]
	if c.loop == other {
		own, other = other, own
	}

	before := tasks.Load()
	(&worker{home: other}).runEvent(other, c.fd, poll.EventRead)
	if got := tasks.Load(); got != before {
		t.Fatalf("另一个 loop 的事件通知了连接：Executor 多被调用了 %d 次", got-before)
	}
	(&worker{home: own}).runEvent(own, c.fd, poll.EventRead)
	if got := tasks.Load(); got != before+1 {
		t.Fatalf("所属 loop 的事件没有通知连接：Executor 被调用了 %d 次, 期望 1 次", got-before)
	}
}

// loopFds returns the fds of the connections the loop still holds.
func loopFds(l *loop) []int {
	var fds []int
	for _, c := range l.collectConns(func(*conn) bool { return true }) {
		fds = append(fds, c.fd)
	}
	slices.Sort(fds)
	return fds
}

// TestCollectConnsDropsClosed checks that the loop's connection list catches up with connections that have closed:
// they are not returned and are dropped from the list, while the ones still open are returned.
func TestCollectConnsDropsClosed(t *testing.T) {
	opened := make(chan Conn, 3)
	closed := make(chan struct{}, 3)
	srv := startServerWith(t, &funcHandler{
		open:  func(c Conn) { opened <- c },
		close: func(Conn, error) { closed <- struct{}{} },
	}, Options{NumLoops: 1})
	clients := []net.Conn{dial(t, srv), dial(t, srv), dial(t, srv)}
	fdsByAddr := map[string]int{} // the workers open the connections in no particular order
	for range clients {
		c := <-opened
		fdsByAddr[c.RemoteAddr().String()] = c.(*conn).fd
	}
	var fds []int
	for _, client := range clients {
		fds = append(fds, fdsByAddr[client.LocalAddr().String()])
	}
	clients[1].Close()
	select {
	case <-closed:
	case <-time.After(5 * time.Second):
		t.Fatal("连接没有关闭")
	}

	l := srv.loops[0]
	want := slices.Sorted(slices.Values([]int{fds[0], fds[2]}))
	if got := loopFds(l); !slices.Equal(got, want) {
		t.Fatalf("返回了 %v, 期望 %v", got, want)
	}
	l.mu.Lock()
	n := len(l.conns)
	l.mu.Unlock()
	if n != 2 {
		t.Fatalf("列表里有 %d 个连接, 期望已关闭的被丢掉、剩 2 个", n)
	}
}

// TestConnChurn opens and closes connections from several goroutines while they echo, so that fds are reused all the
// time by connections of different loops: every echo must come back, and once all the connections have closed no loop
// may be left holding one.
func TestConnChurn(t *testing.T) {
	var closed atomic.Int32
	srv := startServerWith(t, &funcHandler{data: echo, close: func(Conn, error) { closed.Add(1) }}, Options{NumLoops: 4})
	const clients, rounds = 8, 100
	var wg sync.WaitGroup
	for range clients {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range rounds {
				c, err := net.Dial("tcp", srv.Addr().String())
				if err != nil {
					t.Error(err)
					return
				}
				c.SetDeadline(time.Now().Add(5 * time.Second))
				if _, err := c.Write([]byte("ping")); err != nil {
					t.Error(err)
					return
				}
				if _, err := io.ReadFull(c, make([]byte, 4)); err != nil {
					t.Errorf("echo 没有返回: %v", err)
					return
				}
				c.Close()
			}
		}()
	}
	wg.Wait()
	for deadline := time.Now().Add(5 * time.Second); closed.Load() != clients*rounds; time.Sleep(10 * time.Millisecond) {
		if time.Now().After(deadline) {
			t.Fatalf("只关闭了 %d/%d 个连接", closed.Load(), clients*rounds)
		}
	}
	for i, l := range srv.loops {
		if fds := loopFds(l); len(fds) != 0 {
			t.Fatalf("loop %d 在所有连接关闭之后仍留着 %d 个连接", i, len(fds))
		}
	}
}

// TestConnQueue checks the queue is FIFO, reports whether connections are left behind the one taken, and reuses its
// array once drained.
func TestConnQueue(t *testing.T) {
	var q connQueue
	conns := []*conn{{fd: 1}, {fd: 2}, {fd: 3}}
	for _, c := range conns {
		q.push(c)
	}
	for i, want := range conns {
		c, more := q.pop()
		if c != want || more != (i < len(conns)-1) {
			t.Fatalf("第 %d 次取出 %p more=%v, 期望 fd=%d", i, c, more, want.fd)
		}
	}
	if c, more := q.pop(); c != nil || more || q.length.Load() != 0 {
		t.Fatal("空队列应返回 nil")
	}
	if len(q.conns) != 0 || q.head != 0 {
		t.Fatal("队列取空后没有从头复用数组")
	}
}

// TestStuckWorkersRelieved blocks every worker of the only loop in a callback: the monitor must start an extra worker,
// which serves another connection.
func TestStuckWorkersRelieved(t *testing.T) {
	release := make(chan struct{})
	var stuck sync.WaitGroup
	srv := startServerWith(t, &funcHandler{data: func(c Conn, b []byte) int {
		if string(b) == "block" {
			stuck.Done()
			<-release
			return len(b)
		}
		return echo(c, b)
	}}, Options{NumLoops: 1})
	t.Cleanup(func() { close(release) }) // runs before the server's Close, which waits for the callbacks
	l := srv.loops[0]
	l.workersMu.Lock()
	base := l.baseWorkers
	l.workersMu.Unlock()
	stuck.Add(base)
	for range base {
		dial(t, srv).Write([]byte("block"))
	}
	stuck.Wait()

	c := dial(t, srv)
	c.SetDeadline(time.Now().Add(5 * time.Second))
	c.Write([]byte("ping"))
	if _, err := io.ReadFull(c, make([]byte, 4)); err != nil {
		t.Fatalf("所有 worker 卡在回调里时其他连接得不到服务: %v", err)
	}
}

// TestGoexitReplacesWorker has callbacks call runtime.Goexit more times than the loop has workers: every worker that
// exits that way is replaced, so the connections that come after are still served.
func TestGoexitReplacesWorker(t *testing.T) {
	srv := startServerWith(t, &funcHandler{data: func(c Conn, b []byte) int {
		if string(b) == "exit" {
			runtime.Goexit()
		}
		return echo(c, b)
	}}, Options{NumLoops: 1})
	l := srv.loops[0]
	l.workersMu.Lock()
	base := l.baseWorkers
	l.workersMu.Unlock()
	for range 2 * base {
		c := dial(t, srv)
		c.SetDeadline(time.Now().Add(5 * time.Second))
		c.Write([]byte("exit"))
		if _, err := c.Read(make([]byte, 1)); err != io.EOF {
			t.Fatalf("Goexit 的连接应被关闭, got %v", err)
		}
	}
	c := dial(t, srv)
	c.SetDeadline(time.Now().Add(5 * time.Second))
	c.Write([]byte("ping"))
	if _, err := io.ReadFull(c, make([]byte, 4)); err != nil {
		t.Fatalf("worker 因 Goexit 退出后没有被替换: %v", err)
	}
}

// TestCloseWhileOpening closes the server while the open task of a connection it has accepted is still pending, so the
// connection is not on any list when Serve reads them: once opened it must close itself, or Close never returns.
func TestCloseWhileOpening(t *testing.T) {
	held := make(chan func(), 1)
	var holding atomic.Bool
	holding.Store(true)
	closed := make(chan error, 1)
	srv, err := NewServer("127.0.0.1:0", &funcHandler{close: func(_ Conn, err error) { closed <- err }}, Options{
		NumLoops: 1,
		Executor: func(task func()) {
			if holding.CompareAndSwap(true, false) { // the first task is the open task of the first connection
				held <- task
				return
			}
			go task()
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	go srv.Serve()
	dial(t, srv)
	open := <-held
	done := make(chan struct{})
	go func() {
		srv.Close()
		close(done)
	}()
	for !srv.draining.Load() {
		time.Sleep(time.Millisecond)
	}
	time.Sleep(20 * time.Millisecond) // let Serve read the lists first
	go open()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("打开中的连接没有关闭, Close 没有返回")
	}
	if err := <-closed; err != ErrServerClosed {
		t.Fatalf("OnClose(%v), 期望 ErrServerClosed", err)
	}
}
