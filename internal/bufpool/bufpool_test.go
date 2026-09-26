package bufpool

import "testing"

func TestGetPut(t *testing.T) {
	for _, n := range []int{0, 1, 128, 129, 1000, 4096, 65536, 1 << 20, 16 << 20, 16<<20 + 1} {
		b := Get(n)
		if len(b.B) != n {
			t.Fatalf("Get(%d): len=%d", n, len(b.B))
		}
		if n <= 16<<20 && cap(b.B)&(cap(b.B)-1) != 0 {
			t.Fatalf("Get(%d): cap %d is not a tier size", n, cap(b.B))
		}
		Put(b)
	}
	Put(nil)
}

func BenchmarkGetPut1K(b *testing.B) {
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		buf := Get(1024)
		buf.B[0] = byte(i)
		Put(buf)
	}
}

// The messages cut from one read share a buffer, which goes back to the pool
// only when the last of them and the arena have let go of it.
func TestArenaSharesOneBufferUntilTheLastHolder(t *testing.T) {
	var a Arena
	data := []byte("onetwothree")
	p1, b1 := a.Copy(data[:3], len(data))
	p2, b2 := a.Copy(data[3:6], len(data)-3)
	p3, b3 := a.Copy(data[6:], len(data)-6)
	if b1 != b2 || b2 != b3 {
		t.Fatal("copies from one read went to different buffers")
	}
	a.Release()
	Put(b1)
	Put(b3)
	if string(p1) != "one" || string(p2) != "two" || string(p3) != "three" {
		t.Fatalf("copies = %q %q %q", p1, p2, p3)
	}
	if n := b2.refs.Load(); n != 0 {
		t.Fatalf("one holder left, refs = %d", n)
	}
	Put(b2)
	if n := b2.refs.Load(); n != 0 || len(b2.B) != cap(b2.B) {
		t.Fatalf("released buffer: refs = %d, len %d of %d", n, len(b2.B), cap(b2.B))
	}
}

// A copy that does not fit starts a new buffer; one larger than maxArena gets
// a buffer of its own size.
func TestArenaStartsNewBuffers(t *testing.T) {
	var a Arena
	small := make([]byte, 100)
	_, b1 := a.Copy(small, len(small)) // sized for the rest of the read only
	_, b2 := a.Copy(small, 1)
	if b1 == b2 {
		t.Fatal("a copy beyond the buffer went into it")
	}
	huge := make([]byte, 3*maxArena)
	p, b3 := a.Copy(huge, len(huge))
	if len(p) != len(huge) || cap(b3.B) < len(huge) {
		t.Fatalf("huge copy: len %d, buffer %d", len(p), cap(b3.B))
	}
	a.Release()
	for _, b := range []*Buffer{b1, b2, b3} {
		Put(b)
		if n := b.refs.Load(); n != 0 {
			t.Fatalf("refs = %d after the last Put", n)
		}
	}
}

// A copy released before the arena lets go of its buffer does not free the
// buffer under the copies still held, or under the arena.
func TestArenaCopyReleasedBeforeTheArena(t *testing.T) {
	var a Arena
	_, b1 := a.Copy([]byte("one"), 6)
	Put(b1) // handled before the rest of the read was cut
	p2, b2 := a.Copy([]byte("two"), 3)
	if b1 != b2 {
		t.Fatal("copies from one read went to different buffers")
	}
	a.Release()
	if n := b2.refs.Load(); n != 0 || string(p2) != "two" {
		t.Fatalf("one copy left: refs = %d, copy %q", n, p2)
	}
	Put(b2)
	if n := b2.refs.Load(); n != 0 || len(b2.B) != cap(b2.B) {
		t.Fatalf("released buffer: refs = %d, len %d of %d", n, len(b2.B), cap(b2.B))
	}
}
