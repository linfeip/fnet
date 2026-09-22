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
	pool128k = sync.Pool{
		New: func() any {
			return &pooledBuffer{buf: make([]byte, 131072)}
		},
	}
	pool256k = sync.Pool{
		New: func() any {
			return &pooledBuffer{buf: make([]byte, 262144)}
		},
	}
	pool512k = sync.Pool{
		New: func() any {
			return &pooledBuffer{buf: make([]byte, 524288)}
		},
	}
	pool1m = sync.Pool{
		New: func() any {
			return &pooledBuffer{buf: make([]byte, 1048576)}
		},
	}
	pool2m = sync.Pool{
		New: func() any {
			return &pooledBuffer{buf: make([]byte, 2097152)}
		},
	}
	pool4m = sync.Pool{
		New: func() any {
			return &pooledBuffer{buf: make([]byte, 4194304)}
		},
	}
	pool8m = sync.Pool{
		New: func() any {
			return &pooledBuffer{buf: make([]byte, 8388608)}
		},
	}
	pool16m = sync.Pool{
		New: func() any {
			return &pooledBuffer{buf: make([]byte, 16777216)}
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
	case size <= 131072:
		pb = pool128k.Get().(*pooledBuffer)
		pb.pool = &pool128k
	case size <= 262144:
		pb = pool256k.Get().(*pooledBuffer)
		pb.pool = &pool256k
	case size <= 524288:
		pb = pool512k.Get().(*pooledBuffer)
		pb.pool = &pool512k
	case size <= 1048576:
		pb = pool1m.Get().(*pooledBuffer)
		pb.pool = &pool1m
	case size <= 2097152:
		pb = pool2m.Get().(*pooledBuffer)
		pb.pool = &pool2m
	case size <= 4194304:
		pb = pool4m.Get().(*pooledBuffer)
		pb.pool = &pool4m
	case size <= 8388608:
		pb = pool8m.Get().(*pooledBuffer)
		pb.pool = &pool8m
	case size <= 16777216:
		pb = pool16m.Get().(*pooledBuffer)
		pb.pool = &pool16m
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
