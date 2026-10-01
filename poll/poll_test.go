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

// waitOnce calls Wait in a loop until it gets an fd event or a wakeup signal.
func waitOnce(t *testing.T, p *Poller) (map[int]Event, bool) {
	t.Helper()
	got := map[int]Event{}
	for {
		woken, err := p.Wait(func(fd int, ev Event) { got[fd] |= ev })
		if err != nil {
			t.Fatal(err)
		}
		if woken || len(got) > 0 {
			return got, woken
		}
	}
}

// TestLevelTriggered covers AddRead's level-triggered behavior: readability keeps being reported until the
// data is read away, and after deregistration no more events arrive.
func TestLevelTriggered(t *testing.T) {
	p := newPoller(t)
	a, b := socketPair(t)
	if err := p.AddRead(a); err != nil {
		t.Fatal(err)
	}

	unix.Write(b, []byte("x"))
	if got, _ := waitOnce(t, p); got[a] != EventRead {
		t.Fatalf("期望可读事件, got %v", got)
	}
	if got, _ := waitOnce(t, p); got[a] != EventRead {
		t.Fatalf("水平触发下应再次可读, got %v", got)
	}
	unix.Read(a, make([]byte, 16))

	// After deregistration there must be no more events even with data pending; Wake guarantees Wait returns.
	if err := p.Delete(a); err != nil {
		t.Fatal(err)
	}
	unix.Write(b, []byte("y"))
	p.Wake()
	if got, woken := waitOnce(t, p); !woken || len(got) != 0 {
		t.Fatalf("注销后不应有事件, got %v woken=%v", got, woken)
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
		t.Fatalf("期望注册时报告可写, got %v", got)
	}

	// epoll reports every event the fd is ready for at that moment, so a readability notification may carry
	// writability along with it.
	unix.Write(b, []byte("x"))
	if got, _ := waitOnce(t, p); got[a]&EventRead == 0 {
		t.Fatalf("期望可读事件, got %v", got)
	}
	p.Wake() // use Wake to guarantee Wait returns
	if got, woken := waitOnce(t, p); !woken || len(got) != 0 {
		t.Fatalf("边沿触发下数据未读走也不应重复报告, got %v woken=%v", got, woken)
	}

	unix.Write(b, []byte("y"))
	if got, _ := waitOnce(t, p); got[a]&EventRead == 0 {
		t.Fatalf("新数据到达应再次报告可读, got %v", got)
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
		t.Fatalf("期望可读并带 EventHup, got %v", got)
	}
	if n, _ := unix.Read(a, make([]byte, 16)); n != 1 {
		t.Fatalf("期望读到数据, n=%d", n)
	}
	p.Wake()
	if got, woken := waitOnce(t, p); !woken || got[a]&EventRead != 0 {
		t.Fatalf("数据读完后不应再有可读通知, got %v woken=%v", got, woken)
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
	p.Wake() // the event was already ready while the peer read the data; Wake only guarantees Wait returns
	if got, _ := waitOnce(t, p); got[a]&EventWrite == 0 {
		t.Fatalf("期望可写事件, got %v", got)
	}
}

// TestPeerCloseIsReadable verifies a peer close is reported as readable (reading yields EOF), both
// level-triggered and edge-triggered.
func TestPeerCloseIsReadable(t *testing.T) {
	for name, add := range map[string]func(*Poller, int) error{
		"AddRead": (*Poller).AddRead,
		"AddEdge": (*Poller).AddEdge,
	} {
		t.Run(name, func(t *testing.T) {
			p := newPoller(t)
			a, b := socketPair(t)
			add(p, a)
			unix.Close(b)
			if got, _ := waitOnce(t, p); got[a]&EventRead == 0 {
				t.Fatalf("对端关闭应报告可读, got %v", got)
			}
			if n, _ := unix.Read(a, make([]byte, 16)); n != 0 {
				t.Fatalf("期望读到 EOF, n=%d", n)
			}
		})
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
		t.Fatal("期望被唤醒")
	}
	// Once the wakeup flag has been consumed, a new Wake must take effect again.
	p.Wake()
	if _, woken := waitOnce(t, p); !woken {
		t.Fatal("第二次 Wake 丢失")
	}
}

// TestNotifyWhileWaiting verifies that both events and wakeups arriving only after Wait is already waiting
// (on Linux the runtime has suspended the goroutine) must make it return.
func TestNotifyWhileWaiting(t *testing.T) {
	p := newPoller(t)
	a, b := socketPair(t)
	p.AddRead(a)
	for _, notify := range []func(){func() { p.Wake() }, func() { unix.Write(b, []byte("x")) }} {
		done := make(chan struct{})
		go func() {
			defer close(done)
			for got := false; !got; {
				woken, err := p.Wait(func(int, Event) { got = true })
				got = got || woken || err != nil
			}
		}()
		time.Sleep(20 * time.Millisecond) // let Wait enter the wait first
		notify()
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Fatal("Wait 未返回")
		}
	}
}
