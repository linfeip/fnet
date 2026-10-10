//go:build linux || darwin

package poll

import (
	"sync"
	"testing"
	"time"

	"golang.org/x/sys/unix"
)

func newPoller(t *testing.T) *Poller {
	t.Helper()
	p, err := New()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { p.Close() })
	return p
}

func socketPair(t *testing.T) (int, int) {
	t.Helper()
	fds, err := unix.Socketpair(unix.AF_UNIX, unix.SOCK_STREAM, 0)
	if err != nil {
		t.Fatal(err)
	}
	for _, fd := range fds {
		unix.SetNonblock(fd, true)
	}
	t.Cleanup(func() { unix.Close(fds[0]); unix.Close(fds[1]) })
	return fds[0], fds[1]
}

// waitOnce calls Block in a loop until it gets an fd event or a wakeup signal.
func waitOnce(t *testing.T, p *Poller) (map[int]Event, bool) {
	t.Helper()
	var b Batch
	for {
		woken := p.Block(&b)
		if got := batchEvents(&b); woken || len(got) > 0 {
			return got, woken
		}
	}
}

// TestDelete verifies that after deregistration no more events arrive.
func TestDelete(t *testing.T) {
	p := newPoller(t)
	a, b := socketPair(t)
	if err := p.AddEdge(a); err != nil {
		t.Fatal(err)
	}
	waitOnce(t, p) // the writability reported at registration time

	// After deregistration there must be no more events even with data pending; Wake guarantees Block returns.
	if err := p.Delete(a); err != nil {
		t.Fatal(err)
	}
	unix.Write(b, []byte("y"))
	p.Wake()
	if got, woken := waitOnce(t, p); !woken || len(got) != 0 {
		t.Fatalf("no events should arrive after deregistration, got %v woken=%v", got, woken)
	}
}

// TestEdgeTriggered covers AddEdge's edge-triggered behavior: the already-ready writability is reported at
// registration time; readability is reported once per arrival of new data, and is not reported again while
// the data has not been read away.
func TestEdgeTriggered(t *testing.T) {
	p := newPoller(t)
	a, b := socketPair(t)
	if err := p.AddEdge(a); err != nil {
		t.Fatal(err)
	}
	if got, _ := waitOnce(t, p); got[a] != EventWrite {
		t.Fatalf("want writable reported at registration, got %v", got)
	}

	// epoll reports every event the fd is ready for at that moment, so a readability notification may carry
	// writability along with it.
	unix.Write(b, []byte("x"))
	if got, _ := waitOnce(t, p); got[a]&EventRead == 0 {
		t.Fatalf("want readable event, got %v", got)
	}
	p.Wake() // use Wake to guarantee Block returns
	if got, woken := waitOnce(t, p); !woken || len(got) != 0 {
		t.Fatalf("under edge-triggered mode there should be no repeat report while the data is unread, got %v woken=%v", got, woken)
	}

	unix.Write(b, []byte("y"))
	if got, _ := waitOnce(t, p); got[a]&EventRead == 0 {
		t.Fatalf("new data should be reported readable again, got %v", got)
	}
}

// batchEvents returns the events of b by fd.
func batchEvents(b *Batch) map[int]Event {
	got := map[int]Event{}
	for i := range b.Len() {
		fd, ev := b.Event(i)
		got[fd] |= ev
	}
	return got
}

// TestPoll runs Poll from another goroutine: it takes the ready edge-triggered event and the wake signal without
// blocking, and neither is reported again.
func TestPoll(t *testing.T) {
	p := newPoller(t)
	a, b := socketPair(t)
	if err := p.AddEdge(a); err != nil {
		t.Fatal(err)
	}
	var batch Batch
	p.Poll(&batch) // the writability reported at registration time

	unix.Write(b, []byte("x"))
	p.Wake()
	var got map[int]Event
	var woken bool
	done := make(chan struct{})
	go func() {
		defer close(done)
		woken = p.Poll(&batch)
		got = batchEvents(&batch)
	}()
	<-done
	if !woken || len(got) != 1 || got[a]&EventRead == 0 {
		t.Fatalf("want a readable event and a wakeup signal, got %v woken=%v", got, woken)
	}
	if woken := p.Poll(&batch); woken || batch.Len() != 0 {
		t.Fatalf("taken events and wakeup signals should not be reported again, got %v woken=%v", batchEvents(&batch), woken)
	}
}

// TestBlock verifies that Block, waiting in another goroutine, returns for a Wake and for an event arriving while it
// waits.
func TestBlock(t *testing.T) {
	p := newPoller(t)
	a, b := socketPair(t)
	if err := p.AddEdge(a); err != nil {
		t.Fatal(err)
	}
	var batch Batch
	p.Poll(&batch) // the writability reported at registration time
	for _, notify := range []func(){func() { p.Wake() }, func() { unix.Write(b, []byte("x")) }} {
		done := make(chan struct{})
		go func() {
			defer close(done)
			var batch Batch
			for !p.Block(&batch) && batch.Len() == 0 { // an interrupting signal returns an empty batch
			}
		}()
		time.Sleep(20 * time.Millisecond) // let Block enter the wait first
		notify()
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Fatal("Block did not return")
		}
	}
}

// TestEdgeHup has the peer close before the data is taken away: the data and the FIN are merged into one
// notification carrying EventHup, and once the data is read there are no further notifications.
func TestEdgeHup(t *testing.T) {
	p := newPoller(t)
	a, b := socketPair(t)
	if err := p.AddEdge(a); err != nil {
		t.Fatal(err)
	}
	waitOnce(t, p) // the writability reported at registration time

	unix.Write(b, []byte("x"))
	unix.Shutdown(b, unix.SHUT_WR)
	got, _ := waitOnce(t, p)
	if got[a]&(EventRead|EventHup) != EventRead|EventHup {
		t.Fatalf("want readable with EventHup, got %v", got)
	}
	if n, _ := unix.Read(a, make([]byte, 16)); n != 1 {
		t.Fatalf("want to read data, n=%d", n)
	}
	p.Wake()
	if got, woken := waitOnce(t, p); !woken || got[a]&EventRead != 0 {
		t.Fatalf("no readable notification should remain after the data is read, got %v woken=%v", got, woken)
	}
}

// TestEdgeWritable verifies that after writing until EAGAIN, writability is reported once the peer reads the
// data away and frees up the send buffer: resuming the send of backed-up data can only rely on this event.
func TestEdgeWritable(t *testing.T) {
	p := newPoller(t)
	a, b := socketPair(t)
	if err := p.AddEdge(a); err != nil {
		t.Fatal(err)
	}
	waitOnce(t, p) // the writability reported at registration time

	chunk := make([]byte, 64*1024)
	for {
		if _, err := unix.Write(a, chunk); err == unix.EAGAIN {
			break
		} else if err != nil {
			t.Fatal(err)
		}
	}
	for { // the peer reads all the data away
		if _, err := unix.Read(b, chunk); err != nil {
			break
		}
	}
	p.Wake() // the event was already ready while the peer read the data; Wake only guarantees Block returns
	if got, _ := waitOnce(t, p); got[a]&EventWrite == 0 {
		t.Fatalf("want writable event, got %v", got)
	}
}

// TestPeerCloseIsReadable verifies a peer close is reported as readable (reading yields EOF).
func TestPeerCloseIsReadable(t *testing.T) {
	p := newPoller(t)
	a, b := socketPair(t)
	p.AddEdge(a)
	unix.Close(b)
	if got, _ := waitOnce(t, p); got[a]&EventRead == 0 {
		t.Fatalf("peer close should be reported readable, got %v", got)
	}
	if n, _ := unix.Read(a, make([]byte, 16)); n != 0 {
		t.Fatalf("want EOF, n=%d", n)
	}
}

func TestConcurrentWake(t *testing.T) {
	p := newPoller(t)
	var wg sync.WaitGroup
	for range 100 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := p.Wake(); err != nil {
				t.Error(err)
			}
		}()
	}
	wg.Wait()
	if _, woken := waitOnce(t, p); !woken {
		t.Fatal("want to be woken")
	}
	// Once the wakeup flag has been consumed, a new Wake must take effect again.
	p.Wake()
	if _, woken := waitOnce(t, p); !woken {
		t.Fatal("second Wake was lost")
	}
}
