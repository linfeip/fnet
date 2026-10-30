package bytepool

import (
	"bytes"
	"testing"
)

func TestGetRelease(t *testing.T) {
	for _, n := range []int{0, 1, 512, 513, 4 << 10, 16 << 20, 16<<20 + 1} {
		b := Get(n)
		if b.Len() != 0 || cap(b.Bytes()) < n {
			t.Fatalf("Get(%d): len=%d cap=%d", n, b.Len(), cap(b.Bytes()))
		}
		b.Release()
	}
	b := Get(1000)
	b.WriteString("hello")
	b.Release()
	if b.Bytes() != nil {
		t.Fatal("buffer should be zero after Release")
	}
	b.Release() // releasing again is a no-op
	if b := Get(700); b.Len() != 0 || cap(b.Bytes()) != 1<<10 {
		t.Fatalf("reused buffer: len=%d cap=%d", b.Len(), cap(b.Bytes()))
	}
	b = Buffer{data: make([]byte, 0, 1000)}
	b.Release() // a capacity outside the size classes must be ignored
}

func TestWrite(t *testing.T) {
	var buf Buffer
	var want []byte
	for i := range 10000 {
		chunk := []byte{byte(i), byte(i >> 8), 'x'}
		buf.Write(chunk)
		want = append(want, chunk...)
	}
	if !bytes.Equal(buf.Bytes(), want) {
		t.Fatal("Write content mismatch")
	}
	buf.Release()
}

// TestWriteBeyondMaxClass appends repeatedly to a buffer beyond the largest size class: the capacity grows
// proportionally, so the number of reallocations is only proportional to the logarithm of the size.
// Otherwise every append would reallocate and copy the whole block, reaching 64MB would take thousands of
// reallocations, and the time would grow with the square of the size.
func TestWriteBeyondMaxClass(t *testing.T) {
	chunk := make([]byte, 32<<10)
	var buf Buffer
	reallocs := 0
	for buf.Len() < 4<<maxShift {
		before := cap(buf.Bytes())
		buf.Write(chunk)
		if cap(buf.Bytes()) != before {
			reallocs++
		}
	}
	if reallocs > 40 {
		t.Fatalf("appending to %dMB reallocated %d times", buf.Len()>>20, reallocs)
	}
	buf.Release()
}

func TestDiscard(t *testing.T) {
	var buf Buffer
	buf.WriteString("hello world")
	buf.Discard(6)
	if string(buf.Bytes()) != "world" {
		t.Fatalf("after Discard got %q", buf.Bytes())
	}
	buf.Reset()
	if buf.Len() != 0 || cap(buf.Bytes()) == 0 {
		t.Fatalf("after Reset len=%d cap=%d", buf.Len(), cap(buf.Bytes()))
	}
	buf.Release()
}

func TestJoinKeep(t *testing.T) {
	var in Buffer
	// Small leftover prepended into borrowed buffer, then partially consumed.
	data := in.Join([]byte("hello world"), 0)
	in.Keep(data, 6)
	if string(in.Bytes()) != "world" {
		t.Fatalf("got %q, want world", in.Bytes())
	}

	borrowed := make([]byte, 5+4)
	copy(borrowed[5:], "1234")
	data = in.Join(borrowed, 5)
	if string(data) != "world1234" {
		t.Fatalf("Join with pending got %q", data)
	}
	in.Keep(data, len(data))
	if in.Len() != 0 || in.Bytes() != nil {
		t.Fatalf("expected buffer released after full consume, len=%d", in.Len())
	}
}
