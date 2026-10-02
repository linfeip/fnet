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
			t.Fatalf("空表 lookup(%d) = %v, 期望 nil", fd, c)
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
			t.Fatalf("store(%d) 失败", fd)
		}
		for stored, c := range conns {
			if got := table.lookup(stored); got != c {
				t.Fatalf("存入 fd %d 之后 lookup(%d) = %p, 期望 %p", fd, stored, got, c)
			}
		}
	}
	if c := table.lookup(6); c != nil {
		t.Fatalf("没存过的 fd 6 查到了 %v", c)
	}
	for _, fd := range []int{-1, maxFd, maxFd + 1} {
		if table.store(&conn{fd: fd}) {
			t.Fatalf("store(%d) 超出范围却成功了", fd)
		}
	}

	table.remove(conns[5])
	if c := table.lookup(5); c != nil {
		t.Fatalf("remove 之后 lookup(5) = %v, 期望 nil", c)
	}
	if c := table.lookup(0); c != conns[0] {
		t.Fatal("remove 一个 fd 影响了别的 fd")
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
		t.Fatalf("lookup(9) = %p, 期望复用 fd 的新连接 %p", c, reused)
	}
	table.remove(reused)
	if c := table.lookup(9); c != nil {
		t.Fatalf("lookup(9) = %v, 期望 nil", c)
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
				t.Fatalf("第 %d 轮: lookup(%d) = %p, 期望 %p (存入的连接被丢弃的页带走了)", round, c.fd, got, c)
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
					t.Errorf("lookup(%d) 返回了没有初始化完整的连接: fd=%d", fd, c.fd)
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
