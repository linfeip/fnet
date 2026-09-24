package reactor

import (
	"sync"
	"sync/atomic"
)

const (
	chunkShift = 11 // 2048 slots per chunk
	chunkSize  = 1 << chunkShift
	chunkMask  = chunkSize - 1
)

type chunk [chunkSize]atomic.Pointer[Conn]

// table maps fds to connections. Lookups are lock-free; the chunk directory
// only grows (under mu), so an idle connection costs one pointer slot.
type table struct {
	mu     sync.Mutex
	chunks atomic.Pointer[[]*chunk]
}

func (t *table) slot(fd int) *atomic.Pointer[Conn] {
	if fd < 0 {
		return nil
	}
	cp := t.chunks.Load()
	if cp == nil || fd>>chunkShift >= len(*cp) {
		return nil
	}
	ch := (*cp)[fd>>chunkShift]
	if ch == nil {
		return nil
	}
	return &ch[fd&chunkMask]
}

func (t *table) get(fd int) *Conn {
	if s := t.slot(fd); s != nil {
		return s.Load()
	}
	return nil
}

func (t *table) store(fd int, c *Conn) {
	if s := t.slot(fd); s != nil {
		s.Store(c)
		return
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	var old []*chunk
	if cp := t.chunks.Load(); cp != nil {
		old = *cp
	}
	idx := fd >> chunkShift
	if idx < len(old) && old[idx] != nil {
		old[idx][fd&chunkMask].Store(c)
		return
	}
	size := max(len(old), 32)
	for idx >= size {
		size *= 2
	}
	chunks := make([]*chunk, size)
	copy(chunks, old)
	if chunks[idx] == nil {
		chunks[idx] = new(chunk)
	}
	chunks[idx][fd&chunkMask].Store(c)
	t.chunks.Store(&chunks)
}

// delete clears fd's slot if it still holds c.
func (t *table) delete(fd int, c *Conn) {
	if s := t.slot(fd); s != nil {
		s.CompareAndSwap(c, nil)
	}
}

func (t *table) forEach(fn func(*Conn)) {
	cp := t.chunks.Load()
	if cp == nil {
		return
	}
	for _, ch := range *cp {
		if ch == nil {
			continue
		}
		for i := range ch {
			if c := ch[i].Load(); c != nil {
				fn(c)
			}
		}
	}
}
