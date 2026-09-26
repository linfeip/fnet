package fnet

import (
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"runtime/debug"
)

// LengthField frames messages by a length field in their header, the layout
// of most game and RPC protocols (Netty's LengthFieldBasedFrameDecoder,
// Skynet's netpack and Leaf's MsgParser describe the same thing). Use its
// Split method as Server.Split:
//
//	fnet.LengthField{Size: 4, Strip: 4}.Split                             // | len u32 | payload |
//	fnet.LengthField{Size: 2, Strip: 2}.Split                             // Skynet: | len u16 | payload |
//	fnet.LengthField{Size: 4, Order: binary.LittleEndian, Strip: 4}.Split // Unity's BitConverter
//	fnet.LengthField{Size: 4, Adjust: -4}.Split                           // the length counts itself
//
// A message spans Offset + Size + length + Adjust bytes, where length is the
// field's value; OnMessage gets it without its first Strip bytes.
type LengthField struct {
	// Offset is the number of bytes before the length field (magic, version).
	Offset int
	// Size is the width of the length field in bytes: 1, 2, 3, 4 or 8.
	Size int
	// Order is the field's byte order; nil means big-endian.
	Order binary.ByteOrder
	// Adjust is added to the length to get the number of bytes after the
	// field, e.g. -(Offset+Size) when the length counts the whole message.
	Adjust int
	// Strip is the number of leading bytes removed before OnMessage, e.g.
	// Offset+Size to hand over the payload alone.
	Strip int
}

var (
	errLengthField = errors.New("fnet: LengthField needs Size 1, 2, 3, 4 or 8 and no negative Offset or Strip")
	errBadLength   = errors.New("fnet: message length is shorter than its header")
)

// Split implements bufio.SplitFunc.
func (f LengthField) Split(data []byte, _ bool) (advance int, token []byte, err error) {
	if f.Offset < 0 || f.Strip < 0 {
		return 0, nil, errLengthField
	}
	hdr := f.Offset + f.Size
	if len(data) < hdr {
		return 0, nil, nil
	}
	n, err := f.length(data[f.Offset:hdr])
	if err != nil {
		return 0, nil, err
	}
	if n > math.MaxInt32 {
		return 0, nil, ErrMessageTooLarge
	}
	total := hdr + int(n) + f.Adjust
	if total < hdr || total < f.Strip {
		return 0, nil, errBadLength
	}
	if len(data) < total {
		return 0, nil, nil
	}
	return total, data[f.Strip:total], nil
}

func (f LengthField) length(b []byte) (uint64, error) {
	order := f.Order
	if order == nil {
		order = binary.BigEndian
	}
	switch f.Size {
	case 1:
		return uint64(b[0]), nil
	case 2:
		return uint64(order.Uint16(b)), nil
	case 3:
		// ByteOrder has no 24-bit read: ask the order which end comes first,
		// which works for any implementation, not just the standard ones.
		if order.Uint16([]byte{1, 0}) == 1 {
			return uint64(b[0]) | uint64(b[1])<<8 | uint64(b[2])<<16, nil
		}
		return uint64(b[0])<<16 | uint64(b[1])<<8 | uint64(b[2]), nil
	case 4:
		return uint64(order.Uint32(b)), nil
	case 8:
		return order.Uint64(b), nil
	}
	return 0, errLengthField
}

// PanicError is the error OnClose receives after Split, OnOpen or OnMessage
// panicked. The panic only closed that connection; the server keeps running.
type PanicError struct {
	Value any    // the value passed to panic
	Stack []byte // the stack of the goroutine that panicked
}

func newPanicError(v any) *PanicError { return &PanicError{Value: v, Stack: debug.Stack()} }

func (e *PanicError) Error() string { return fmt.Sprintf("fnet: panic: %v", e.Value) }

// Unwrap returns the panic value if it is an error.
func (e *PanicError) Unwrap() error {
	err, _ := e.Value.(error)
	return err
}
