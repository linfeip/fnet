// Package bufpool is a size-tiered byte buffer pool shared by the reactor's
// outbound queues and the WebSocket message path.
package bufpool

import (
	"math/bits"
	"sync"
)

const (
	minShift = 7  // 128 B
	maxShift = 24 // 16 MiB
)

// Buffer is a pooled byte slice. B always has the full capacity of its tier;
// callers re-slice it. A Buffer obtained from Get must be returned with Put
// exactly once and not touched afterwards.
type Buffer struct {
	B    []byte
	tier int8 // -1: not pooled
}

var tiers [maxShift - minShift + 1]sync.Pool

func init() {
	for i := range tiers {
		size, tier := 1<<(minShift+i), int8(i) // per-iteration copies (go 1.21 loop semantics)
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

// Put returns b to its tier. A nil b is ignored.
func Put(b *Buffer) {
	if b == nil || b.tier < 0 {
		return
	}
	b.B = b.B[:cap(b.B)]
	tiers[b.tier].Put(b)
}
