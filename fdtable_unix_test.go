//go:build linux || darwin

package fnet

import (
	"sync"
	"sync/atomic"
	"testing"
)

// TestFdTable covers the table on its own: lookups of fds that no page covers yet, storing across the pages, an fd out
// of range, and removal.
func TestFdTable(t *testing.T) {
	var table fdTable
	for _, fd := range []int{-1, 0, 5, fdPageSize, maxFd - 1, maxFd} {
		if c := table.lookup(fd); c != nil {
			t.Fatalf("empty table lookup(%d) = %v, want nil", fd, c)
		}
	}
	table.remove(&conn{fd: 7}) // nothing to remove
	table.remove(&conn{fd: maxFd})

	// 0 and fdPageSize-1 share a page, fdPageSize is the first slot of the next one, and maxFd-1 is the last fd.
	fds := []int{0, 5, fdPageSize - 1, fdPageSize, 40 * fdPageSize, maxFd - 1}
	conns := make(map[int]*conn)
	for _, fd := range fds {
		conns[fd] = &conn{fd: fd}
		if !table.store(conns[fd]) {
			t.Fatalf("store(%d) failed", fd)
		}
		for stored, c := range conns {
			if got := table.lookup(stored); got != c {
				t.Fatalf("after storing fd %d, lookup(%d) = %p, want %p", fd, stored, got, c)
			}
		}
	}
	if c := table.lookup(6); c != nil {
		t.Fatalf("lookup of never-stored fd 6 returned %v", c)
	}
	for _, fd := range []int{-1, maxFd, maxFd + 1} {
		if table.store(&conn{fd: fd}) {
			t.Fatalf("store(%d) succeeded out of range", fd)
		}
	}

	table.remove(conns[5])
	if c := table.lookup(5); c != nil {
		t.Fatalf("after remove, lookup(5) = %v, want nil", c)
	}
	if c := table.lookup(0); c != conns[0] {
		t.Fatal("removing one fd affected another fd")
	}
}

// TestFdTableRemoveKeepsReusedFd checks that a connection can only remove itself: once the fd has been reused by a new
// connection, removing the old one leaves the new one in place.
func TestFdTableRemoveKeepsReusedFd(t *testing.T) {
	var table fdTable
	old, reused := &conn{fd: 9}, &conn{fd: 9}
	table.store(old)
	table.store(reused)
	table.remove(old)
	if c := table.lookup(9); c != reused {
		t.Fatalf("lookup(9) = %p, want the new connection that reused the fd %p", c, reused)
	}
	table.remove(reused)
	if c := table.lookup(9); c != nil {
		t.Fatalf("lookup(9) = %v, want nil", c)
	}
}

// TestFdTableConcurrentFirstStore has many goroutines store into the same page of a fresh table at the same time, so
// they all race to allocate it: whichever page ends up installed, every stored connection must be found in it. Run
// with -race.
func TestFdTableConcurrentFirstStore(t *testing.T) {
	const workers, pagesPerRound = 16, 64
	for round := range 20 {
		var table fdTable
		var wg sync.WaitGroup
		start := make(chan struct{})
		conns := make([]*conn, workers*pagesPerRound)
		for w := range workers {
			wg.Add(1)
			go func() {
				defer wg.Done()
				<-start
				for p := range pagesPerRound { // worker w owns slot w of every page; all workers share each page
					c := &conn{fd: p*fdPageSize + w}
					conns[p*workers+w] = c
					table.store(c)
				}
			}()
		}
		close(start)
		wg.Wait()
		for _, c := range conns {
			if got := table.lookup(c.fd); got != c {
				t.Fatalf("round %d: lookup(%d) = %p, want %p (a discarded page carried the stored connection away)", round, c.fd, got, c)
			}
		}
	}
}

// TestFdTableConcurrentReadWrite has readers look an fd up while a writer keeps handing it to fresh connections, the way
// a closed fd is reused: a reader sees nil or a connection, and a connection it sees must be fully initialised. Run with
// -race: reading c.fd is reported as a data race if store did not publish the connection to the reader.
func TestFdTableConcurrentReadWrite(t *testing.T) {
	const fd, readers, rounds = 77, 4, 20000
	var table fdTable
	var stop atomic.Bool
	var wg sync.WaitGroup
	for range readers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for !stop.Load() {
				if c := table.lookup(fd); c != nil && c.fd != fd {
					t.Errorf("lookup(%d) returned a connection that was not fully initialised: fd=%d", fd, c.fd)
					return
				}
			}
		}()
	}
	for range rounds {
		c := &conn{fd: fd} // initialised before it is stored
		table.store(c)
		table.remove(c)
	}
	stop.Store(true)
	wg.Wait()
}

// TestFdTableDeadlines checks page-level minDeadline tracking and pruning: a page whose minDeadline is 0 or in the
// future is skipped, an expired connection is notified, and scanning a page folds the earliest remaining deadline (including
// one that was pushed back) into minDeadlines.
func TestFdTableDeadlines(t *testing.T) {
	var table fdTable
	srv := &Server{opts: Options{Executor: func(_ int, _ func()) {}}}
	l := &loop{srv: srv}

	c1 := &conn{fd: 1, loop: l}
	c2 := &conn{fd: 2, loop: l}
	c3 := &conn{fd: fdPageSize + 1, loop: l}
	table.store(c1)
	table.store(c2)
	table.store(c3)
	c1.deadline.Store(100)
	table.noteDeadline(c1.fd, 100)
	c2.deadline.Store(300)
	table.noteDeadline(c2.fd, 300)
	c3.deadline.Store(500)
	table.noteDeadline(c3.fd, 500)

	if got := table.minDeadlines[0].Load(); got != 100 {
		t.Fatalf("page 0 minDeadline = %d, want 100", got)
	}
	if got := table.minDeadlines[1].Load(); got != 500 {
		t.Fatalf("page 1 minDeadline = %d, want 500", got)
	}

	// Before any deadline has passed: nothing is notified and minDeadlines stay intact.
	table.checkDeadlines(srv, 50)
	if c1.state.Load()&evExpired != 0 || table.minDeadlines[0].Load() != 100 {
		t.Fatal("checkDeadlines notified or cleared a page before its minDeadline")
	}

	// Put c1's deadline off to 400 without lowering minDeadlines[0]; scanning at 100 must see 400, notify nothing, and
	// advance minDeadlines[0] to c2's 300.
	c1.deadline.Store(400)
	table.checkDeadlines(srv, 100)
	if c1.state.Load()&evExpired != 0 || c2.state.Load()&evExpired != 0 {
		t.Fatal("a connection whose deadline was put off was notified")
	}
	if got := table.minDeadlines[0].Load(); got != 300 {
		t.Fatalf("page 0 minDeadline after scan = %d, want 300", got)
	}

	// At 300: c2 expires and is notified, minDeadlines[0] advances to c1's 400, and page 1 (500) is still untouched.
	table.checkDeadlines(srv, 300)
	if c2.state.Load()&evExpired == 0 {
		t.Fatal("expired c2 was not notified")
	}
	if c1.state.Load()&evExpired != 0 || c3.state.Load()&evExpired != 0 {
		t.Fatal("unexpired connection was notified")
	}
	if got := table.minDeadlines[0].Load(); got != 400 {
		t.Fatalf("page 0 minDeadline = %d, want 400", got)
	}
	if got := table.minDeadlines[1].Load(); got != 500 {
		t.Fatalf("page 1 minDeadline = %d, want 500", got)
	}
}
