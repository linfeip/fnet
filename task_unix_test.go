//go:build linux || darwin

package fnet

import (
	"io"
	"net"
	"os"
	"sync/atomic"
	"testing"
	"time"

	"github.com/linfeip/fnet/internal/units"
)

// TestExecutor verifies the connection's task is run by Options.Executor.
func TestExecutor(t *testing.T) {
	var tasks atomic.Int32
	srv := startServerWith(t, &funcHandler{data: echo}, Options{NumLoops: 1, Executor: func(task func()) {
		tasks.Add(1)
		go task()
	}})
	c := dial(t, srv)
	for range 3 {
		c.Write([]byte("hello"))
		if _, err := io.ReadFull(c, make([]byte, 5)); err != nil {
			t.Fatal(err)
		}
	}
	if tasks.Load() == 0 {
		t.Fatal("连接的任务没有经过 Executor")
	}
}

// TestHandlerPanic panics in a callback: the connection is closed with ErrHandlerPanic, the task's panic is
// recovered by the executor, and other connections are unaffected.
func TestHandlerPanic(t *testing.T) {
	reasons := make(chan error, 1)
	srv := startServerWith(t, &funcHandler{
		data: func(c Conn, b []byte) int {
			if string(b) == "panic" {
				panic("boom")
			}
			return echo(c, b)
		},
		close: func(c Conn, err error) { reasons <- err },
	}, Options{NumLoops: 1, Executor: func(task func()) {
		go func() {
			defer func() { recover() }()
			task()
		}()
	}})
	bad, good := dial(t, srv), dial(t, srv)
	bad.Write([]byte("panic"))
	if err := <-reasons; err != ErrHandlerPanic {
		t.Fatalf("OnClose(%v), 期望 ErrHandlerPanic", err)
	}
	bad.SetReadDeadline(time.Now().Add(time.Second))
	if _, err := bad.Read(make([]byte, 1)); err != io.EOF {
		t.Fatalf("panic 的连接应被关闭, got %v", err)
	}
	good.Write([]byte("ok"))
	if _, err := io.ReadFull(good, make([]byte, 2)); err != nil {
		t.Fatalf("其它连接: %v", err)
	}
}

// TestExecutorNotCalledUnderConnLock checks that the event loop does not hold the connection's mu when it calls the
// user-supplied Executor to request a connection close (deadline expired): the task the Executor starts takes that
// lock right away (see closeIfRequested), so once the Executor blocks until that task has made progress, the two would
// wait on each other.
func TestExecutorNotCalledUnderConnLock(t *testing.T) {
	var held, armed atomic.Bool
	var target atomic.Pointer[conn]
	opened := make(chan Conn, 1)
	srv := startServerWith(t, &funcHandler{open: func(c Conn) { opened <- c }}, Options{NumLoops: 1, Executor: func(task func()) {
		if c := target.Load(); c != nil && armed.Load() {
			if !c.mu.TryLock() {
				held.Store(true)
			} else {
				c.mu.Unlock()
			}
		}
		go task()
	}})
	dial(t, srv)
	sc := <-opened
	time.Sleep(100 * time.Millisecond) // let the connection settle: after this only the call below calls the Executor
	sc.SetDeadline(time.Now().Add(-time.Second))
	target.Store(sc.(*conn))
	armed.Store(true)
	l, checked := srv.loops[0], make(chan struct{})
	l.trigger(func() { // the connection list belongs to the event loop
		l.checkDeadlines()
		close(checked)
	})
	<-checked
	armed.Store(false)
	if held.Load() {
		t.Fatal("请求关闭到期的连接时，Executor 是在持有连接的 mu 的情况下被调用的")
	}
}

// TestPeerCloseThenDeadline verifies that after reading EOF the connection waits for the send buffer to
// drain before closing; when the peer stops reading, an error or an expired deadline must still be able to
// close it immediately, and the close reason must be that error rather than the EOF that arrived first.
func TestPeerCloseThenDeadline(t *testing.T) {
	reasons := make(chan error, 1)
	srv := startServer(t, &funcHandler{
		open: func(c Conn) {
			c.Write(make([]byte, 16*units.MB)) // the peer does not read, so the send buffer must have a backlog
			c.SetDeadline(time.Now().Add(200 * time.Millisecond))
		},
		close: func(c Conn, err error) { reasons <- err },
	})
	c := dial(t, srv).(*net.TCPConn)
	c.CloseWrite()
	select {
	case err := <-reasons:
		if err != os.ErrDeadlineExceeded {
			t.Fatalf("OnClose(%v), 期望 os.ErrDeadlineExceeded", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("读到 EOF 之后期限到期，连接没有关闭")
	}
}
