package bytepool

import (
	"bytes"
	"testing"

	"github.com/linfeip/fnet/internal/units"
)

func TestGetRelease(t *testing.T) {
	for _, n := range []int{0, 1, units.KB / 2, units.KB/2 + 1, 4 * units.KB, 16 * units.MB, 16*units.MB + 1} {
		b := Get(n)
		if b.Len() != 0 || cap(b.Bytes()) < n {
			t.Fatalf("Get(%d): len=%d cap=%d", n, b.Len(), cap(b.Bytes()))
		}
		b.Release()
	}
	b := Get(1000)
	b.AppendString("hello")
	b.Release()
	if b.Bytes() != nil {
		t.Fatal("Release 后缓冲应为零值")
	}
	b.Release() // releasing again is a no-op
	if b := Get(700); b.Len() != 0 || cap(b.Bytes()) != units.KB {
		t.Fatalf("复用缓冲: len=%d cap=%d", b.Len(), cap(b.Bytes()))
	}
	b = Buffer{data: make([]byte, 0, 1000)}
	b.Release() // a capacity outside the size classes must be ignored
}

func TestAppend(t *testing.T) {
	var buf Buffer
	var want []byte
	for i := range 10000 {
		chunk := []byte{byte(i), byte(i >> 8), 'x'}
		buf.Append(chunk)
		want = append(want, chunk...)
	}
	if !bytes.Equal(buf.Bytes(), want) {
		t.Fatal("Append 内容不一致")
	}
	buf.Release()
}

// TestAppendBeyondMaxClass appends repeatedly to a buffer beyond the largest size class: the capacity grows
// proportionally, so the number of reallocations is only proportional to the logarithm of the size.
// Otherwise every append would reallocate and copy the whole block, reaching 64MB would take thousands of
// reallocations, and the time would grow with the square of the size.
func TestAppendBeyondMaxClass(t *testing.T) {
	chunk := make([]byte, 32*units.KB)
	var buf Buffer
	reallocs := 0
	for buf.Len() < 4<<maxShift {
		before := cap(buf.Bytes())
		buf.Append(chunk)
		if cap(buf.Bytes()) != before {
			reallocs++
		}
	}
	if reallocs > 40 {
		t.Fatalf("追加到 %dMB 重新分配了 %d 次", buf.Len()>>20, reallocs)
	}
	buf.Release()
}

func TestDiscard(t *testing.T) {
	var buf Buffer
	buf.AppendString("hello world")
	buf.Discard(6)
	if string(buf.Bytes()) != "world" {
		t.Fatalf("Discard 后为 %q", buf.Bytes())
	}
	buf.Reset()
	if buf.Len() != 0 || cap(buf.Bytes()) == 0 {
		t.Fatalf("Reset 后 len=%d cap=%d", buf.Len(), cap(buf.Bytes()))
	}
	buf.Release()
}
