package websocket

import (
	"encoding/binary"
	"errors"
	"math"

	"github.com/gobwas/ws"
)

// maxSmallFrameSize is the buffer size on Conn for encoding a frame header (up to 10 bytes) or a complete small frame
// (a 2-byte header and a payload of up to 125 bytes, covering all control frames and 7-bit-length data frames) without
// borrowing from bytepool.
const maxSmallFrameSize = 128

var errLengthNotMinimal = errors.New("websocket: payload length not encoded in the fewest bytes")

// appendFrameHeader appends a frame header with FIN=1, RSV of 0 and no mask (RFC 6455 5.2); the payload length is
// encoded in 7, 16 or 64 bits.
func appendFrameHeader(b []byte, op ws.OpCode, length int) []byte {
	b = append(b, 0x80|byte(op))
	switch {
	case length <= 125:
		return append(b, byte(length))
	case length <= 0xffff:
		return binary.BigEndian.AppendUint16(append(b, 126), uint16(length))
	default:
		return binary.BigEndian.AppendUint64(append(b, 127), uint64(length))
	}
}

// parseHeader parses the frame header at the start of data and returns it together with its length in bytes; the
// length is 0 when data does not hold the whole header yet. It is ws.ReadHeader on a byte slice: that one reads
// through an io.Reader and allocates a scratch buffer for every frame, which, run once per received frame, was the
// biggest source of garbage under pipelined load.
// Like ws.ReadHeader it checks nothing but the encoding: a 64-bit length with its most significant bit set is
// rejected, and so is a length sent in more bytes than it needs (RFC 6455 5.2), both only once the whole header is
// there. The rest of the validation is ws.CheckHeader's.
func parseHeader(data []byte) (h ws.Header, n int, err error) {
	if len(data) < 2 {
		return h, 0, nil
	}
	h.Fin = data[0]&0x80 != 0
	h.Rsv = (data[0] & 0x70) >> 4
	h.OpCode = ws.OpCode(data[0] & 0x0f)
	h.Masked = data[1]&0x80 != 0
	size := 2 // header length: the two fixed bytes, the extended length and the mask
	length := data[1] & 0x7f
	switch length {
	case 126:
		size += 2
	case 127:
		size += 8
	}
	if h.Masked {
		size += 4
	}
	if len(data) < size {
		return h, 0, nil
	}
	switch length {
	case 126:
		if h.Length = int64(binary.BigEndian.Uint16(data[2:])); h.Length < 126 {
			return h, 0, errLengthNotMinimal
		}
	case 127:
		if data[2]&0x80 != 0 {
			return h, 0, ws.ErrHeaderLengthMSB
		}
		if h.Length = int64(binary.BigEndian.Uint64(data[2:])); h.Length <= math.MaxUint16 {
			return h, 0, errLengthNotMinimal
		}
	default:
		h.Length = int64(length)
	}
	if h.Masked {
		copy(h.Mask[:], data[size-4:size])
	}
	return h, size, nil
}

// applyMask unmasks payload in place using mask (RFC 6455 5.3).
func applyMask(b []byte, mask [4]byte) {
	mask32 := binary.LittleEndian.Uint32(mask[:])
	mask64 := uint64(mask32) | uint64(mask32)<<32
	for len(b) >= 32 {
		binary.LittleEndian.PutUint64(b, binary.LittleEndian.Uint64(b)^mask64)
		binary.LittleEndian.PutUint64(b[8:], binary.LittleEndian.Uint64(b[8:])^mask64)
		binary.LittleEndian.PutUint64(b[16:], binary.LittleEndian.Uint64(b[16:])^mask64)
		binary.LittleEndian.PutUint64(b[24:], binary.LittleEndian.Uint64(b[24:])^mask64)
		b = b[32:]
	}
	for len(b) >= 8 {
		binary.LittleEndian.PutUint64(b, binary.LittleEndian.Uint64(b)^mask64)
		b = b[8:]
	}
	for i := range b {
		b[i] ^= mask[i&3]
	}
}

// checkCloseFrame is ws.CheckCloseFrameData, which knows only the codes RFC 6455 defines and rejects those IANA has
// registered since for a peer to close with as well: 1012 (Service Restart), 1013 (Try Again Later) and 1014 (Bad
// Gateway).
func checkCloseFrame(code ws.StatusCode, reason string) error {
	if code >= 1012 && code <= 1014 {
		code = ws.StatusNormalClosure // checked as one of the defined codes: the reason must be UTF-8
	}
	return ws.CheckCloseFrameData(code, reason)
}
