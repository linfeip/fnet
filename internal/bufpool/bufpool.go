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
	// tickets is how many holds an Arena takes on each buffer it starts, one
	// for each copy it may make: a copy then costs no atomic operation, and
	// Release gives back the ones not used.
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

// Arena copies the payloads cut from one read into shared buffers, so that a
// read carrying many messages costs one Get and one Put rather than one each.
// A buffer goes back to the pool once its last copy is released: one message
// held up keeps the rest of its buffer (at most maxArena, or its own size) in
// use until then.
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
