package websocket

import (
	"encoding/binary"
	"errors"
	"math"
	"unicode/utf8"
)

// maxSmallFrameSize is the buffer size on Conn for encoding a frame header (up to 10 bytes) or a complete small frame
// (a 2-byte header and a payload of up to 125 bytes, covering all control frames and 7-bit-length data frames) without
// borrowing from bytepool.
const maxSmallFrameSize = 128

// maxControlPayload is the maximum payload length of a control frame in bytes (RFC 6455 5.5).
const maxControlPayload = 125

var (
	errLengthNotMinimal       = errors.New("websocket: payload length not encoded in the fewest bytes")
	errHeaderLengthMSB        = errors.New("websocket: most significant bit of 64-bit length is set")
	errReservedBits           = errors.New("websocket: reserved bits are non-zero")
	errMaskRequired           = errors.New("websocket: client frame is not masked")
	errReservedOpCode         = errors.New("websocket: reserved opcode")
	errControlFragmented      = errors.New("websocket: control frame is fragmented")
	errControlPayloadTooBig   = errors.New("websocket: control frame payload exceeds 125 bytes")
	errUnexpectedContinuation = errors.New("websocket: unexpected continuation frame")
	errUnexpectedDataFrame    = errors.New("websocket: new data frame during fragmented message")
	errInvalidCloseCode       = errors.New("websocket: invalid close status code")
	errInvalidUTF8            = errors.New("websocket: invalid UTF-8 in text or close frame")
)

// frameHeader is the parsed header of a WebSocket frame (RFC 6455 5.2).
type frameHeader struct {
	final    bool
	reserved byte
	opCode   OpCode
	masked   bool
	mask     [4]byte
	length   int64
}

// appendFrameHeader appends a frame header with FIN=1, RSV of 0 and no mask (RFC 6455 5.2); the payload length is
// encoded in 7, 16 or 64 bits.
func appendFrameHeader(b []byte, op OpCode, length int) []byte {
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
// length is 0 when data does not hold the whole header yet.
// It checks only the length encoding: a 64-bit length with its most significant bit set is rejected, and so is a
// length sent in more bytes than it needs (RFC 6455 5.2), both only once the whole header is there. The rest of the
// validation is in checkHeader.
func parseHeader(data []byte) (h frameHeader, n int, err error) {
	if len(data) < 2 {
		return h, 0, nil
	}
	h.final = data[0]&0x80 != 0
	h.reserved = (data[0] & 0x70) >> 4
	h.opCode = OpCode(data[0] & 0x0f)
	h.masked = data[1]&0x80 != 0
	size := 2 // header length: the two fixed bytes, the extended length and the mask
	length := data[1] & 0x7f
	switch length {
	case 126:
		size += 2
	case 127:
		size += 8
	}
	if h.masked {
		size += 4
	}
	if len(data) < size {
		return h, 0, nil
	}
	switch length {
	case 126:
		if h.length = int64(binary.BigEndian.Uint16(data[2:])); h.length < 126 {
			return h, 0, errLengthNotMinimal
		}
	case 127:
		if data[2]&0x80 != 0 {
			return h, 0, errHeaderLengthMSB
		}
		if h.length = int64(binary.BigEndian.Uint64(data[2:])); h.length <= math.MaxUint16 {
			return h, 0, errLengthNotMinimal
		}
	default:
		h.length = int64(length)
	}
	if h.masked {
		copy(h.mask[:], data[size-4:size])
	}
	return h, size, nil
}

// checkHeader validates a client frame header against RFC 6455 5.2–5.5. fragmented reports whether the connection is
// in the middle of receiving a fragmented message.
func checkHeader(h frameHeader, fragmented bool) error {
	if h.reserved != 0 {
		return errReservedBits
	}
	if !h.masked {
		return errMaskRequired
	}
	switch h.opCode {
	case OpContinuation:
		if !fragmented {
			return errUnexpectedContinuation
		}
	case OpText, OpBinary:
		if fragmented {
			return errUnexpectedDataFrame
		}
	case OpClose, OpPing, OpPong:
		if !h.final {
			return errControlFragmented
		}
		if h.length > maxControlPayload {
			return errControlPayloadTooBig
		}
	default:
		return errReservedOpCode
	}
	return nil
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

// parseClosePayload parses and validates the non-empty payload of a close frame (RFC 6455 5.5.1, 7.4).
func parseClosePayload(payload []byte) (StatusCode, string, error) {
	if len(payload) < 2 {
		return 0, "", errInvalidCloseCode
	}
	code := StatusCode(binary.BigEndian.Uint16(payload[:2]))
	if !validCloseCode(code) {
		return 0, "", errInvalidCloseCode
	}
	reason := string(payload[2:])
	if !utf8.ValidString(reason) {
		return 0, "", errInvalidUTF8
	}
	return code, reason, nil
}

// validCloseCode reports whether code may be sent in a close frame (RFC 6455 7.4 and IANA codes 1012–1014).
func validCloseCode(code StatusCode) bool {
	return code >= 1000 && code <= 1003 ||
		code >= 1007 && code <= 1014 ||
		code >= 3000 && code < 5000
}
