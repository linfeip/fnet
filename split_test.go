package fnet

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"testing"
)

func TestLengthField(t *testing.T) {
	cases := []struct {
		name string
		f    LengthField
		in   []byte
		adv  int
		tok  []byte
		err  error
	}{
		{"u32 payload", LengthField{Size: 4, Strip: 4}, []byte{0, 0, 0, 3, 'a', 'b', 'c', 'x'}, 7, []byte("abc"), nil},
		{"short header", LengthField{Size: 4}, []byte{0, 0}, 0, nil, nil},
		{"short body", LengthField{Size: 4, Strip: 4}, []byte{0, 0, 0, 3, 'a'}, 0, nil, nil},
		{"u16 with header", LengthField{Size: 2}, []byte{0, 1, 'z'}, 3, []byte{0, 1, 'z'}, nil},
		{"u8", LengthField{Size: 1, Strip: 1}, []byte{2, 'h', 'i'}, 3, []byte("hi"), nil},
		{"u24 big-endian", LengthField{Size: 3, Strip: 3}, []byte{0, 0, 2, 'o', 'k'}, 5, []byte("ok"), nil},
		{"u24 little-endian", LengthField{Size: 3, Order: binary.LittleEndian, Strip: 3}, []byte{2, 0, 0, 'o', 'k'}, 5, []byte("ok"), nil},
		{"u32 little-endian", LengthField{Size: 4, Order: binary.LittleEndian, Strip: 4}, []byte{1, 0, 0, 0, '!'}, 5, []byte("!"), nil},
		{"u64", LengthField{Size: 8, Strip: 8}, []byte{0, 0, 0, 0, 0, 0, 0, 1, 'q'}, 9, []byte("q"), nil},
		{"magic before the length", LengthField{Offset: 2, Size: 2, Strip: 4}, []byte{0xCA, 0xFE, 0, 2, 'g', 'o'}, 6, []byte("go"), nil},
		{"length counts itself", LengthField{Size: 4, Adjust: -4, Strip: 4}, []byte{0, 0, 0, 6, 'h', 'i'}, 6, []byte("hi"), nil},
		{"empty payload", LengthField{Size: 2, Strip: 2}, []byte{0, 0}, 2, []byte{}, nil},
		{"shorter than its header", LengthField{Size: 4, Adjust: -8}, []byte{0, 0, 0, 1, 'x'}, 0, nil, errBadLength},
		{"strip beyond the message", LengthField{Size: 1, Strip: 5}, []byte{1, 'x'}, 0, nil, errBadLength},
		{"bad size", LengthField{Size: 5}, make([]byte, 8), 0, nil, errLengthField},
		{"negative offset", LengthField{Offset: -1, Size: 4}, make([]byte, 8), 0, nil, errLengthField},
		{"huge length", LengthField{Size: 8}, []byte{0x7f, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff}, 0, nil, ErrMessageTooLarge},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			adv, tok, err := tc.f.Split(tc.in, false)
			if !errors.Is(err, tc.err) || adv != tc.adv || !bytes.Equal(tok, tc.tok) || (tok == nil) != (tc.tok == nil) {
				t.Fatalf("Split = %d, %q, %v; want %d, %q, %v", adv, tok, err, tc.adv, tc.tok, tc.err)
			}
		})
	}
}

func TestPanicError(t *testing.T) {
	cause := errors.New("cause")
	pe := newPanicError(cause)
	if !errors.Is(pe, cause) || len(pe.Stack) == 0 {
		t.Fatalf("PanicError %v does not unwrap to its cause or lacks a stack", pe)
	}
	if got := fmt.Sprint(newPanicError("oops")); got != "fnet: panic: oops" {
		t.Fatalf("Error() = %q", got)
	}
}
