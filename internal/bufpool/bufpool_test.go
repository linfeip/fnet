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
