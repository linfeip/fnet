//go:build linux || darwin

package fnet

import "sync/atomic"

// connsByFd maps the fd of every connection registered with an event loop to its conn, so that the event loop finds the
// connection of a ready fd with two loads and no lock.
//
// It is shared by every server and loop of the process: an fd is unique in the process, and a table per loop would
// have to span the whole range of fds, because the fds of one loop's connections are interleaved with all the others.
var connsByFd fdTable

const (
	fdPageShift = 12
	fdPageSize  = 1 << fdPageShift // slots per page: 32KB of pointers
	fdPageMask  = fdPageSize - 1
	maxFd       = fdPageSize * fdPageSize // 16M fds, far above any realistic fd limit
)

// fdPage holds the slots of fdPageSize consecutive fds.
type fdPage [fdPageSize]atomic.Pointer[conn]

// fdTable is a two-level array indexed by fd: pages[fd>>12] is the page covering fd, and the slot is page[fd&4095].
//
// Why it needs no lock — three facts, each of which the code below relies on:
//
//  1. Nothing ever moves. The first level is a fixed array, and a page is allocated once and never freed or replaced,
//     so a pointer to a page or a slot stays valid for the process's life. There is no resizing for a reader to race with.
//
//  2. Every shared word is an atomic.Pointer. A reader sees either the old value or the new one, never a torn one, and
//     an atomic store publishes everything written before it: a reader that loads a *conn is guaranteed to see that
//     conn's fields (fd, loop, ...) as they were when it was stored.
//
//  3. Each word is written in a way that cannot lose an update:
//     - a page pointer goes from nil to a page exactly once, by compare-and-swap (see store);
//     - a slot has one writer at a time, because an fd belongs to one live connection at a time: a connection removes
//     itself from the table before it closes its fd (see conn.close), and only after the close can the kernel hand
//     the same fd to a new connection, which then stores into the slot. So a slot goes nil -> conn A -> nil -> conn B
//     in strict sequence, and remove's compare-and-swap is only a second line of defence.
//
// Memory grows with the fds in use, 8 bytes per fd plus the slack of the last page.
type fdTable struct {
	pages [fdPageSize]atomic.Pointer[fdPage]
}

// slot returns the slot of fd, or nil when fd is out of range or its page has not been allocated yet.
func (t *fdTable) slot(fd int) *atomic.Pointer[conn] {
	if uint(fd) >= maxFd { // the unsigned compare also rejects a negative fd
		return nil
	}
	page := t.pages[fd>>fdPageShift].Load()
	if page == nil {
		return nil
	}
	return &page[fd&fdPageMask]
}

// lookup returns the connection stored for fd, or nil.
func (t *fdTable) lookup(fd int) *conn {
	if s := t.slot(fd); s != nil {
		return s.Load()
	}
	return nil
}

// store records c as the connection of its fd; it reports false when the fd is beyond what the table can hold.
func (t *fdTable) store(c *conn) bool {
	if uint(c.fd) >= maxFd {
		return false
	}
	index := c.fd >> fdPageShift
	page := t.pages[index].Load()
	if page == nil {
		// Two goroutines can get here for the same page at once. Both build one, but only one compare-and-swap
		// succeeds; the loser throws its page away and uses the winner's, so a stored conn never ends up in a page
		// that was dropped.
		t.pages[index].CompareAndSwap(nil, new(fdPage))
		page = t.pages[index].Load()
	}
	page[c.fd&fdPageMask].Store(c)
	return true
}

// remove takes c out of the table. It leaves the slot alone when it no longer holds c, so it can never remove a
// connection that has taken over the fd in the meantime.
func (t *fdTable) remove(c *conn) {
	if s := t.slot(c.fd); s != nil {
		s.CompareAndSwap(c, nil)
	}
}
