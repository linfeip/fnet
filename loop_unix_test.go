//go:build linux || darwin

package fnet

import (
	"io"
	"net"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/linfeip/fnet/poll"
)

// BenchmarkHandleEvent measures what the event loop does for every ready fd: finding the connection and marking the
// event pending. The Executor drops the task, so after the first event a connection already counts as scheduled and
// the rest cost only the lookup and the atomic update.
// The fds are 50 apart, the way the connections of one server are spread over the process's fds when 50 servers
// accept at the same time.
func BenchmarkHandleEvent(b *testing.B) {
	srv := &Server{opts: Options{Executor: func(func()) {}}.withDefaults()}
	l, err := newLoop(srv)
	if err != nil {
		b.Fatal(err)
	}
	defer l.close()
	const numConns = 2000
	fds := make([]int, numConns)
	for i := range fds {
		c := &conn{fd: 100 + i*50, loop: l}
		connsByFd.store(c)
		b.Cleanup(func() { connsByFd.remove(c) })
		fds[i] = c.fd
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := range b.N {
		l.handleEvent(fds[i%numConns], poll.EventRead)
	}
}

// TestHandleEventOwnLoopOnly checks that an event is delivered through the loop that owns the connection: a leftover
// event of another loop's poller for an fd that has since been reused by this connection must not notify it.
func TestHandleEventOwnLoopOnly(t *testing.T) {
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
	onLoop(other, func() { other.handleEvent(c.fd, poll.EventRead) })
	if got := tasks.Load(); got != before {
		t.Fatalf("另一个 loop 的事件通知了连接：Executor 多被调用了 %d 次", got-before)
	}
	onLoop(own, func() { own.handleEvent(c.fd, poll.EventRead) })
	if got := tasks.Load(); got != before+1 {
		t.Fatalf("所属 loop 的事件没有通知连接：Executor 被调用了 %d 次, 期望 1 次", got-before)
	}
}

// onLoop runs fn on the event loop, which owns the loop's state (such as what handleEvent records), and waits for it.
func onLoop(l *loop, fn func()) {
	done := make(chan struct{})
	l.trigger(func() {
		fn()
		close(done)
	})
	<-done
}

// loopFds returns the fds of the connections the loop still holds; it runs on the event loop, which owns the list.
func loopFds(l *loop) []int {
	result := make(chan []int, 1)
	l.trigger(func() {
		var fds []int
		l.forEachConn(func(c *conn) { fds = append(fds, c.fd) })
		slices.Sort(fds)
		result <- fds
	})
	return <-result
}

// TestForEachConnDropsClosed checks that the loop's connection list catches up with connections that have closed: they
// are not visited and are dropped from the list, while the ones still open are visited.
func TestForEachConnDropsClosed(t *testing.T) {
	opened := make(chan Conn, 3)
	closed := make(chan struct{}, 3)
	srv := startServerWith(t, &funcHandler{
		open:  func(c Conn) { opened <- c },
		close: func(Conn, error) { closed <- struct{}{} },
	}, Options{NumLoops: 1})
	clients := []net.Conn{dial(t, srv), dial(t, srv), dial(t, srv)}
	var fds []int
	for range clients {
		fds = append(fds, (<-opened).(*conn).fd)
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
		t.Fatalf("访问了 %v, 期望 %v", got, want)
	}
	listed := make(chan int, 1)
	l.trigger(func() { listed <- len(l.conns) })
	if n := <-listed; n != 2 {
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

// TestReadyTasksBatch 验证事件合并、批次上限和提交数组复用，不在 poller 回调里执行任务。
func TestReadyTasksBatch(t *testing.T) {
	var sizes []int
	var submitted []func()
	srv := &Server{
		opts: Options{Executor: func(func()) { t.Error("不应逐个提交默认执行器任务") }},
		submitBatch: func(tasks []func()) {
			sizes = append(sizes, len(tasks))
			submitted = append(submitted, tasks...)
		},
	}
	l, err := newLoop(srv)
	if err != nil {
		t.Fatal(err)
	}
	defer l.close()
	var runs int
	for i := range maxReadyTasks + 1 {
		c := &conn{fd: 400000 + i, loop: l, task: func() { runs++ }}
		if !connsByFd.store(c) {
			t.Fatal("连接表存储失败")
		}
		t.Cleanup(func() { connsByFd.remove(c) })
		l.handleEvent(c.fd, poll.EventRead)
		l.handleEvent(c.fd, poll.EventWrite) // 合并到已有任务，不重复提交。
		if c.state.Load() != scheduledBit|evRead|evWrite {
			t.Fatal("事件合并不符")
		}
	}
	if !slices.Equal(sizes, []int{maxReadyTasks}) || runs != 0 {
		t.Fatalf("批次 %v，任务提前执行 %d 次", sizes, runs)
	}
	l.submitReadyTasks()
	if !slices.Equal(sizes, []int{maxReadyTasks, 1}) || len(l.readyTasks) != 0 {
		t.Fatalf("批次 %v，剩余任务 %d", sizes, len(l.readyTasks))
	}
	for _, task := range l.readyTasks[:cap(l.readyTasks)] {
		if task != nil {
			t.Fatal("提交数组仍引用连接任务")
		}
	}
	for _, task := range submitted {
		task()
	}
	if runs != maxReadyTasks+1 {
		t.Fatalf("执行 %d 次，期望 %d", runs, maxReadyTasks+1)
	}
}

func TestDefaultExecutorBatchOnly(t *testing.T) {
	for _, custom := range []bool{false, true} {
		opts := Options{}
		if custom {
			opts.Executor = func(task func()) { go task() }
		}
		srv, err := NewServer("127.0.0.1:0", &funcHandler{}, opts)
		if err != nil {
			t.Fatal(err)
		}
		if (srv.submitBatch != nil) == custom {
			t.Error("批量路径改变了自定义 Executor 语义")
		}
		srv.Close()
	}
}

func TestYieldReadinessBudget(t *testing.T) {
	l := &loop{srv: &Server{submitBatch: func([]func()) {}}}
	l.submitted = true
	l.readyEventsSinceYield = maxReadyTasks - 1
	if l.takeYield() || l.submitted {
		t.Fatal("小批次过早让出或状态未清除")
	}
	l.readyEventsSinceYield++ // 没有新提交也必须让出，避免已排队任务饥饿。
	if !l.takeYield() || l.readyEventsSinceYield != 0 {
		t.Fatal("持续就绪未按预算让出")
	}
	l.srv.submitBatch = nil
	l.submitted = true
	if !l.takeYield() || l.takeYield() {
		t.Fatal("自定义 Executor 的逐轮让出语义改变")
	}
}
