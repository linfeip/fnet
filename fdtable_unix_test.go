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
