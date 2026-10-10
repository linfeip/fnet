//go:build linux || darwin

package fnet

import (
	"bytes"
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
	srv := startServerWith(t, &funcHandler{data: echo}, Options{NumLoops: 1, Executor: func(_ int, task func()) {
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
		t.Fatal("connection task did not go through the Executor")
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
	}, Options{NumLoops: 1, Executor: func(_ int, task func()) {
		go func() {
			defer func() { recover() }()
			task()
		}()
	}})
	bad, good := dial(t, srv), dial(t, srv)
	bad.Write([]byte("panic"))
	if err := <-reasons; err != ErrHandlerPanic {
		t.Fatalf("OnClose(%v), want ErrHandlerPanic", err)
	}
	bad.SetReadDeadline(time.Now().Add(time.Second))
	if _, err := bad.Read(make([]byte, 1)); err != io.EOF {
		t.Fatalf("connection that panicked should be closed, got %v", err)
	}
	good.Write([]byte("ok"))
	if _, err := io.ReadFull(good, make([]byte, 2)); err != nil {
		t.Fatalf("other connection: %v", err)
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
	srv := startServerWith(t, &funcHandler{open: func(c Conn) { opened <- c }}, Options{NumLoops: 1, Executor: func(_ int, task func()) {
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
	srv.loops[0].checkDeadlines()
	armed.Store(false)
	if held.Load() {
		t.Fatal("Executor was called while holding the connection's mu when requesting the close of an expired connection")
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
			t.Fatalf("OnClose(%v), want os.ErrDeadlineExceeded", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("deadline expired after reading EOF but the connection was not closed")
	}
}

// TestBatchResubmissionSerial verifies that batch readiness and re-notification from inside a callback do not run the
// callbacks of one connection concurrently.
func TestBatchResubmissionSerial(t *testing.T) {
	var active atomic.Int32
	var concurrent atomic.Bool
	srv := startServer(t, &funcHandler{data: func(c Conn, b []byte) int {
		if active.Add(1) != 1 {
			concurrent.Store(true)
		}
		defer active.Add(-1)
		c.PauseRead()
		c.ResumeRead() // produce events for the next round while the task is still running.
		return echo(c, b)
	}})
	c := dial(t, srv)
	c.SetDeadline(time.Now().Add(5 * time.Second))
	payload := bytes.Repeat([]byte("echo"), 32*units.KB)
	if n, err := c.Write(payload); err != nil || n != len(payload) {
		t.Fatalf("wrote %d/%d bytes, err=%v", n, len(payload), err)
	}
	reply := make([]byte, len(payload))
	if _, err := io.ReadFull(c, reply); err != nil {
		t.Fatal(err)
	}
	if concurrent.Load() || !bytes.Equal(reply, payload) {
		t.Fatal("callbacks ran concurrently or the echoed data does not match")
	}
}
