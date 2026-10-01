// Package bytepool provides a byte buffer pool with size classes that are powers of two, used to reuse connection
// receive and send buffers as well as HTTP message buffers, lowering allocation and GC pressure with many connections.
package bytepool

import (
	"math/bits"
	"sync"
	"unsafe"
)

const (
	minShift = 9  // the smallest size class, 512B
	maxShift = 24 // the largest size class, 16MB; larger slices do not enter the pool
)

// The pool stores only the address of the underlying array (boxing an unsafe.Pointer into an interface does not
// allocate), and the slice is restored from the capacity of its size class when taken out.
var pools [maxShift + 1]sync.Pool

// Buffer is a byte buffer taken from the pool; its holder returns it by calling Release at the end of its lifetime.
// The field is unexported, so a Buffer cannot be built from an arbitrary byte slice outside the package, which means
// only buffers taken from the pool can be returned to it.
// The zero value is an empty buffer that can be appended to directly.
type Buffer struct {
	data []byte
}

// class returns the size class needed to hold size bytes (rounded up to a power of two).
func class(size int) int {
	if size <= 1<<minShift {
		return minShift
	}
	return bits.Len(uint(size - 1))
}

// Get returns a buffer of length 0 whose capacity is at least size.
func Get(size int) Buffer {
	c := class(size)
	if c > maxShift {
		return Buffer{data: make([]byte, 0, size)}
	}
	if p := pools[c].Get(); p != nil {
		return Buffer{data: unsafe.Slice((*byte)(p.(unsafe.Pointer)), 1<<c)[:0]}
	}
	return Buffer{data: make([]byte, 0, 1<<c)}
}

// Release returns the buffer to the pool and sets b to its zero value: b is then an empty buffer, and calling Release
// again does not return anything a second time.
// A slice obtained earlier from Bytes must not be used after the buffer is returned. A buffer whose capacity belongs to
// no size class (such as one above 16MB) is simply discarded.
//
// Not inlined: zeroing b requires a write barrier, and inlining would enlarge the stack frame of the caller (such as
// serve in websocket, which runs for every message).
//
//go:noinline
func (b *Buffer) Release() {
	data := b.data
	b.data = nil
	c := cap(data)
	if c < 1<<minShift || c > 1<<maxShift || c&(c-1) != 0 {
		return
	}
	pools[bits.Len(uint(c))-1].Put(unsafe.Pointer(unsafe.SliceData(data)))
}

// Bytes returns the data in the buffer, valid until the buffer is next modified or returned to the pool.
func (b *Buffer) Bytes() []byte { return b.data }

// Len returns the number of bytes of data in the buffer.
func (b *Buffer) Len() int { return len(b.data) }

// Reset clears the data while keeping the capacity.
func (b *Buffer) Reset() { b.data = b.data[:0] }

// Discard drops the first n bytes and moves the remaining data to the start of the buffer.
func (b *Buffer) Discard(n int) { b.data = b.data[:copy(b.data, b.data[n:])] }

// Append appends p to b, taking a larger buffer from the pool and returning the old one when the capacity is not enough.
//
// Buffers above the largest size class do not enter the pool, and Get allocates them at the exact size, with no slack
// left over from rounding up to a power of two: when such a buffer grows again, an extra 1/4 of slack is kept, since
// otherwise every append would reallocate and copy the whole block, making the total cost of successive appends grow
// with the square of the size.
func (b *Buffer) Append(p []byte) {
	if cap(b.data)-len(b.data) < len(p) {
		size := len(b.data) + len(p)
		if cap(b.data) > 1<<maxShift {
			size += size / 4
		}
		nb := append(Get(size).data, b.data...)
		b.Release()
		b.data = nb
	}
	b.data = append(b.data, p...)
}

// AppendString is the same as Append. s is handed to Append zero-copy, and Append only reads it without modifying it, so
// the string is never changed.
//
// No generic shared implementation: a generic instance called across packages has no escape information, so the compiler
// conservatively assumes the argument escapes, forcing data on the caller's stack to be allocated on the heap.
func (b *Buffer) AppendString(s string) {
	b.Append(unsafe.Slice(unsafe.StringData(s), len(s)))
}
