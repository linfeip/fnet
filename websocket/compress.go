package websocket

import (
	"bytes"
	"compress/flate"
	"io"
	"sync"
)

// permessage-deflate (RFC 7692) with no context takeover in either direction:
// every message is compressed on its own, so compressors are pooled freely.

var (
	flateTail     = []byte{0x00, 0x00, 0xff, 0xff}
	flateReadTail = []byte{0x00, 0x00, 0xff, 0xff, 0x01, 0x00, 0x00, 0xff, 0xff}

	flateReaders sync.Pool
	flateWriters [flate.BestCompression - flate.HuffmanOnly + 1]sync.Pool // by level
)

// decompress inflates a message, failing with ErrMessageTooBig once the output
// passes limit bytes (limit < 0: no limit).
func decompress(payload []byte, limit int64) ([]byte, error) {
	if len(payload) == 0 {
		return nil, nil
	}
	src := io.MultiReader(bytes.NewReader(payload), bytes.NewReader(flateReadTail))
	r, _ := flateReaders.Get().(io.ReadCloser)
	if r == nil {
		r = flate.NewReader(src)
	} else if err := r.(flate.Resetter).Reset(src, nil); err != nil {
		return nil, err
	}
	defer flateReaders.Put(r)

	var out bytes.Buffer
	if limit < 0 {
		if _, err := out.ReadFrom(r); err != nil {
			return nil, err
		}
		return out.Bytes(), nil
	}
	n, err := out.ReadFrom(io.LimitReader(r, limit+1))
	if err != nil {
		return nil, err
	}
	if n > limit {
		return nil, ErrMessageTooBig
	}
	return out.Bytes(), nil
}

// compress deflates a message and strips the trailing empty block.
func compress(payload []byte, level int) ([]byte, error) {
	if len(payload) == 0 {
		return nil, nil
	}
	var out bytes.Buffer
	var w *flate.Writer
	pool := (*sync.Pool)(nil)
	if level >= flate.HuffmanOnly && level <= flate.BestCompression {
		pool = &flateWriters[level-flate.HuffmanOnly]
		w, _ = pool.Get().(*flate.Writer)
	}
	if w == nil {
		var err error
		if w, err = flate.NewWriter(&out, level); err != nil {
			return nil, err
		}
	} else {
		w.Reset(&out)
	}
	_, err := w.Write(payload)
	if err == nil {
		err = w.Flush()
	}
	if pool != nil {
		w.Reset(nil)
		pool.Put(w)
	}
	if err != nil {
		return nil, err
	}
	return bytes.TrimSuffix(out.Bytes(), flateTail), nil
}
