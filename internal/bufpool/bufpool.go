// Package bufpool is a size-tiered byte buffer pool shared by the reactor's
// outbound queues and the TCP and WebSocket message paths.
package bufpool

import (
	"math/bits"
	"sync"
	"sync/atomic"
)

const (
	minShift = 7  // 128 B
	maxShift = 24 // 16 MiB

	// maxArena bounds the buffer an Arena starts for the rest of a read.
	maxArena = 16 << 10
	// tickets is how many holds an Arena takes up front on each buffer, so a
	// copy costs no atomic operation; Release returns the unused ones.
	tickets = 1 << 30
)

// Buffer is a pooled byte slice. B always has the full capacity of its tier;
// callers re-slice it. Every holder of a Buffer releases it with Put exactly
// once and does not touch it afterwards: the caller of Get holds it, and so
// does each copy an Arena makes into it.
type Buffer struct {
	B    []byte
	tier int8         // -1: not pooled
	refs atomic.Int32 // holders beyond the first
}

var tiers [maxShift - minShift + 1]sync.Pool

func init() {
	for i := range tiers {
		size, tier := 1<<(minShift+i), int8(i)
		tiers[i].New = func() any { return &Buffer{B: make([]byte, size), tier: tier} }
	}
}

// Get returns a buffer with len(B) == n and cap(B) >= n. Sizes above 16 MiB are
// allocated directly and not pooled.
func Get(n int) *Buffer {
	if n > 1<<maxShift {
		return &Buffer{B: make([]byte, n), tier: -1}
	}
	i := 0
	if n > 1<<minShift {
		i = bits.Len(uint(n-1)) - minShift
	}
	b := tiers[i].Get().(*Buffer)
	b.B = b.B[:n]
	return b
}

// Put drops a hold on b, and returns b to its tier once no holder is left. A
// nil b is ignored.
func Put(b *Buffer) { drop(b, 1) }

func drop(b *Buffer, holds int32) {
	if b == nil || b.refs.Add(-holds) >= 0 || b.tier < 0 {
		return
	}
	b.refs.Store(0)
	b.B = b.B[:cap(b.B)]
	tiers[b.tier].Put(b)
}

// Join returns a buffer holding parts back to back, for a caller that sends
// them in one write.
func Join(parts ...[]byte) *Buffer {
	n := 0
	for _, p := range parts {
		n += len(p)
	}
	b := Get(n)
	off := 0
	for _, p := range parts {
		off += copy(b.B[off:], p)
	}
	return b
}

// Arena copies the messages cut from one read into shared buffers, so a read
// carrying many messages costs one Get and one Put rather than one each. A
// buffer returns to the pool once its last copy is released, so one message
// held up keeps its whole buffer (at most maxArena, or its own size) alive.
type Arena struct {
	buf    *Buffer
	used   int
	copies int32 // tickets used
}

// Copy copies p into the arena, and returns the copy and the buffer holding
// it, for the caller to release with Put. rest is how many bytes the read being
// cut may still yield, p included: a new buffer is sized for them, up to
// maxArena.
func (a *Arena) Copy(p []byte, rest int) ([]byte, *Buffer) {
	if a.buf == nil || cap(a.buf.B)-a.used < len(p) {
		a.Release()
		a.buf, a.used, a.copies = Get(max(len(p), min(rest, maxArena))), 0, 0
		a.buf.refs.Store(tickets)
	}
	b := a.buf.B[a.used : a.used+len(p) : a.used+len(p)]
	copy(b, p)
	a.used += len(p)
	a.copies++
	return b, a.buf
}

// Release drops the arena's own hold on its buffer, and the tickets no copy
// took: call it once the read is cut. The copies keep the buffer until they
// are released, before or after this.
func (a *Arena) Release() {
	if a.buf != nil {
		drop(a.buf, tickets+1-a.copies)
		a.buf = nil
	}
}

// Queues pools queues of minPooledQueue to maxPooledQueue messages; smaller
// ones are allocated, larger ones left to the garbage collector.
const (
	minPooledQueue = 16
	maxPooledQueue = 1024
)

// Queues pools connections' message queues. A connection holds a queue only
// while messages wait for its worker, so an idle one pays a pointer. Loops take
// queues and workers return them on other Ps, so a pooled queue comes out of
// another P's cache: a single-message queue (request and reply) is cheaper
// allocated as one small object, and only burst queues are pooled.
type Queues[T any] struct{ p sync.Pool }

// one is a queue for a single message and its array, allocated together.
type one[T any] struct {
	s []T
	a [1]T
}

// Get returns an empty queue for n messages, the first to be queued.
func (q *Queues[T]) Get(n int) *[]T {
	if n <= 1 {
		o := new(one[T])
		o.s = o.a[:0]
		return &o.s
	}
	if s, ok := q.p.Get().(*[]T); ok {
		return s
	}
	s := make([]T, 0, max(n, minPooledQueue))
	return &s
}

// Put gives back s, whose elements the caller has cleared; nil is ignored.
func (q *Queues[T]) Put(s *[]T) {
	if s != nil && cap(*s) >= minPooledQueue && cap(*s) <= maxPooledQueue {
		*s = (*s)[:0]
		q.p.Put(s)
	}
}
