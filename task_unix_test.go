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

// TestTaskKey checks that every task of a connection is handed to the Executor with the same key.
func TestTaskKey(t *testing.T) {
	keys := make(chan int, 64)
	srv := startServerWith(t, &funcHandler{data: echo}, Options{NumLoops: 2, Executor: func(key int, task func()) {
		select {
		case keys <- key:
		default:
		}
		go task()
	}})
	c := dial(t, srv)
	for range 3 {
		c.Write([]byte("hello"))
		if _, err := io.ReadFull(c, make([]byte, 5)); err != nil {
			t.Fatal(err)
		}
	}
	want := <-keys
	for len(keys) > 0 {
		if key := <-keys; key != want {
			t.Fatalf("the tasks of one connection were handed to the Executor with keys %d and %d", want, key)
		}
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

// TestDeadlinePutOff puts the deadline off after the event loop has found it expired, before the connection's task
// runs, as a request arriving just then does: the task must see the new deadline and keep the connection open.
func TestDeadlinePutOff(t *testing.T) {
	var hold atomic.Bool
	tasks := make(chan func(), 1)
	opened := make(chan Conn, 1)
	srv := startServerWith(t, &funcHandler{open: func(c Conn) { opened <- c }, data: echo}, Options{NumLoops: 1,
		Executor: func(_ int, task func()) {
			if hold.Load() {
				tasks <- task
				return
			}
			go task()
		}})
	c := dial(t, srv)
	sc := <-opened
	time.Sleep(100 * time.Millisecond) // let the connection settle: after this only the call below calls the Executor
	sc.SetDeadline(time.Now().Add(-time.Second))
	hold.Store(true)
	srv.loops[0].checkDeadlines()
	hold.Store(false)
	task := <-tasks
	sc.SetDeadline(time.Time{})
	go task()
	c.SetDeadline(time.Now().Add(5 * time.Second))
	c.Write([]byte("ping"))
	if _, err := io.ReadFull(c, make([]byte, 4)); err != nil {
		t.Fatalf("the connection was closed though its deadline was put off: %v", err)
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

// TestLingerEndsOnPeerClose has the peer's FIN arrive before the server closes: the linger finds the EOF at once and
// closes the socket, rather than wait for its timeout.
func TestLingerEndsOnPeerClose(t *testing.T) {
	srv := startServerWith(t, &funcHandler{data: func(c Conn, b []byte) int {
		time.Sleep(50 * time.Millisecond) // the peer's FIN arrives meanwhile
		c.Close()
		return len(b)
	}}, Options{NumLoops: 1})
	client := dial(t, srv)
	client.Write([]byte("x"))
	client.Close()
	l := srv.loops[0]
	for deadline := time.Now().Add(500 * time.Millisecond); len(loopFds(l)) != 0; time.Sleep(10 * time.Millisecond) {
		if time.Now().After(deadline) {
			t.Fatal("the socket lingered although the peer had closed")
		}
	}
}

// TestLingerTimeout has the peer neither send nor close after the server closes: the socket lingers, but is closed once
// the linger times out, even though the deadline was cancelled after the close.
func TestLingerTimeout(t *testing.T) {
	closed := make(chan Conn, 1)
	srv := startServerWith(t, &funcHandler{
		open:  func(c Conn) { c.Close() },
		close: func(c Conn, err error) { closed <- c },
	}, Options{NumLoops: 1})
	dial(t, srv)
	sc := <-closed
	sc.SetDeadline(time.Time{})
	time.Sleep(500 * time.Millisecond)
	l := srv.loops[0]
	if len(loopFds(l)) != 1 {
		t.Fatal("the socket was closed at once, without lingering")
	}
	for deadline := time.Now().Add(3 * time.Second); len(loopFds(l)) != 0; time.Sleep(10 * time.Millisecond) {
		if time.Now().After(deadline) {
			t.Fatal("the socket lingered past its timeout")
		}
	}
}

// TestDetachDeadline checks that Detach writes out what the send buffer holds within the deadline: a peer that reads
// nothing makes it fail then, instead of blocking the caller for good.
func TestDetachDeadline(t *testing.T) {
	opened := make(chan Conn, 1)
	srv := startServer(t, &funcHandler{open: func(c Conn) {
		c.Write(make([]byte, 64*units.MB)) // far more than the socket takes
		c.PauseRead()
		c.SetDeadline(time.Now().Add(1500 * time.Millisecond)) // past the next deadline check, so Detach comes first
		opened <- c
	}})
	dial(t, srv) // never reads
	sc := <-opened
	detached := make(chan error, 1)
	go func() {
		nc, err := sc.Detach()
		if nc != nil {
			nc.Close()
		}
		detached <- err
	}()
	select {
	case err := <-detached:
		if err == nil {
			t.Fatal("Detach succeeded although the peer reads nothing")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Detach blocked past the deadline")
	}
}

// TestSetDeadlineOutOfRange checks that a deadline past what UnixNano counts (the year 2262) never expires, and one
// before it (the year 1678) has expired, rather than either wrapping around.
func TestSetDeadlineOutOfRange(t *testing.T) {
	var c conn
	c.SetDeadline(time.Unix(1<<62, 0))
	if c.expiredBy(time.Now().UnixNano()) {
		t.Fatal("a deadline in the far future has expired")
	}
	// Before 1678 UnixNano overflows, which must not turn a time long past into one far ahead.
	for _, past := range []time.Time{time.Date(1000, 1, 1, 0, 0, 0, 0, time.UTC), time.Unix(-1, 0)} {
		c.SetDeadline(past)
		if !c.expiredBy(time.Now().UnixNano()) || c.lingering() {
			t.Fatalf("a deadline of %v has not expired", past)
		}
	}
}

// writableHandler is a funcHandler that is a WritableHandler too.
type writableHandler struct {
	funcHandler
	writable chan Conn
}

func (h *writableHandler) OnWritable(c Conn) {
	select {
	case h.writable <- c:
	default:
	}
}

// TestOnWritable checks that Writable reports false while more than maxBacklog waits in the send buffer, and that
// OnWritable comes once the peer has taken enough of it.
func TestOnWritable(t *testing.T) {
	opened := make(chan bool, 1)
	h := &writableHandler{writable: make(chan Conn, 1)}
	h.open = func(c Conn) {
		c.Write(make([]byte, 16*units.MB)) // far more than the socket takes
		opened <- c.Writable()
	}
	client := dial(t, startServer(t, h))
	if <-opened {
		t.Fatal("Writable with 16MB waiting in the send buffer")
	}
	go io.Copy(io.Discard, client)
	select {
	case c := <-h.writable:
		if !c.Writable() {
			t.Fatal("OnWritable, but Writable reports false")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("OnWritable did not come once the peer read")
	}
}
