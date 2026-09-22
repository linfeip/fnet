package websocket

import (
	"sync"
)

type pooledBuffer struct {
	buf  []byte
	pool *sync.Pool
}

var (
	pool128 = sync.Pool{
		New: func() any {
			return &pooledBuffer{buf: make([]byte, 128)}
		},
	}
	pool512 = sync.Pool{
		New: func() any {
			return &pooledBuffer{buf: make([]byte, 512)}
		},
	}
	pool1k = sync.Pool{
		New: func() any {
			return &pooledBuffer{buf: make([]byte, 1024)}
		},
	}
	pool4k = sync.Pool{
		New: func() any {
			return &pooledBuffer{buf: make([]byte, 4096)}
		},
	}
	pool16k = sync.Pool{
		New: func() any {
			return &pooledBuffer{buf: make([]byte, 16384)}
		},
	}
	pool64k = sync.Pool{
		New: func() any {
			return &pooledBuffer{buf: make([]byte, 65536)}
		},
	}
)

func getPayloadBuffer(size int) ([]byte, *pooledBuffer) {
	var pb *pooledBuffer
	switch {
	case size <= 128:
		pb = pool128.Get().(*pooledBuffer)
		pb.pool = &pool128
	case size <= 512:
		pb = pool512.Get().(*pooledBuffer)
		pb.pool = &pool512
	case size <= 1024:
		pb = pool1k.Get().(*pooledBuffer)
		pb.pool = &pool1k
	case size <= 4096:
		pb = pool4k.Get().(*pooledBuffer)
		pb.pool = &pool4k
	case size <= 16384:
		pb = pool16k.Get().(*pooledBuffer)
		pb.pool = &pool16k
	case size <= 65536:
		pb = pool64k.Get().(*pooledBuffer)
		pb.pool = &pool64k
	default:
		return make([]byte, size), nil
	}
	return pb.buf[:size], pb
}

func putPayloadBuffer(pb *pooledBuffer) {
	if pb != nil && pb.pool != nil {
		pb.pool.Put(pb)
	}
}
