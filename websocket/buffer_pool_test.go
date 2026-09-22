package websocket

import (
	"bytes"
	"testing"
)

func TestPayloadBufferPool(t *testing.T) {
	sizes := []int{10, 100, 128, 256, 512, 1000, 1024, 2048, 4096, 8192, 16384, 32768, 65536, 70000}
	for _, sz := range sizes {
		buf, pb := getPayloadBuffer(sz)
		if len(buf) != sz {
			t.Fatalf("expected len %d, got %d", sz, len(buf))
		}
		// Fill buffer to check write access
		for i := 0; i < sz; i++ {
			buf[i] = byte(i & 0xff)
		}
		putPayloadBuffer(pb)
	}
}

func BenchmarkPayloadBufferPool_1K(b *testing.B) {
	data := make([]byte, 1024)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		buf, pb := getPayloadBuffer(1024)
		copy(buf, data)
		putPayloadBuffer(pb)
	}
}

func BenchmarkPayloadBufferWithoutPool_1K(b *testing.B) {
	data := make([]byte, 1024)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		buf := make([]byte, 1024)
		copy(buf, data)
		_ = bytes.Equal(buf, data)
	}
}
