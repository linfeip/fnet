package websocket

import (
	"encoding/binary"
	"errors"
	"math"

	"github.com/gobwas/ws"
)

// maxFrameLength bounds one frame so payload lengths always fit in an int.
const maxFrameLength = math.MaxInt32

var errFrameTooLarge = errors.New("fnet/websocket: frame too large")

// parseHeader decodes a frame header from b without allocating. ok stays false
// (with a nil error) until the whole header is present; n is its size.
func parseHeader(b []byte) (h ws.Header, n int, ok bool, err error) {
	if len(b) < 2 {
		return
	}
	h.Fin = b[0]&0x80 != 0
	h.Rsv = (b[0] & 0x70) >> 4
	h.OpCode = ws.OpCode(b[0] & 0x0f)
	h.Masked = b[1]&0x80 != 0

	length := uint64(b[1] & 0x7f)
	n = 2
	switch length {
	case 126:
		if len(b) < 4 {
			return
		}
		length, n = uint64(binary.BigEndian.Uint16(b[2:4])), 4
	case 127:
		if len(b) < 10 {
			return
		}
		length, n = binary.BigEndian.Uint64(b[2:10]), 10
		if length>>63 != 0 {
			return h, 0, false, ws.ErrHeaderLengthMSB
		}
	}
	if h.Masked {
		if len(b) < n+4 {
			return
		}
		copy(h.Mask[:], b[n:n+4])
		n += 4
	}
	if length > maxFrameLength {
		return h, 0, false, errFrameTooLarge
	}
	h.Length = int64(length)
	return h, n, true, nil
}

// putHeader encodes an unmasked, final server frame header into dst (at least
// 10 bytes) and returns its size. rsv1 marks a permessage-deflate payload.
func putHeader(dst []byte, op OpCode, length int, rsv1 bool) int {
	dst[0] = 0x80 | byte(op)
	if rsv1 {
		dst[0] |= 0x40
	}
	switch {
	case length <= 125:
		dst[1] = byte(length)
		return 2
	case length <= math.MaxUint16:
		dst[1] = 126
		binary.BigEndian.PutUint16(dst[2:4], uint16(length))
		return 4
	default:
		dst[1] = 127
		binary.BigEndian.PutUint64(dst[2:10], uint64(length))
		return 10
	}
}

// protocolError is a peer violation and the close status sent in reply.
type protocolError struct {
	code   ws.StatusCode
	reason string
}

func (e *protocolError) Error() string {
	if e.reason == "" {
		return "fnet/websocket: protocol error"
	}
	return "fnet/websocket: " + e.reason
}

// checkHeader applies the client-frame rules of RFC 6455 5.1-5.5 and the RSV1
// rule of RFC 7692. fragmented reports whether a fragmented message is open.
func checkHeader(h ws.Header, compressed, fragmented bool) *protocolError {
	fail := func(reason string) *protocolError {
		return &protocolError{ws.StatusProtocolError, reason}
	}
	switch {
	case !h.Masked:
		return fail("unmasked client frame")
	case h.OpCode.IsReserved():
		return fail("reserved opcode")
	case h.OpCode.IsControl():
		if !h.Fin || h.Length > 125 || h.Rsv != 0 {
			return fail("invalid control frame")
		}
	case h.Rsv2() || h.Rsv3() || h.Rsv1() && (!compressed || h.OpCode == ws.OpContinuation):
		return fail("reserved bits set")
	case h.OpCode == ws.OpContinuation && !fragmented:
		return fail("unexpected continuation frame")
	case h.OpCode != ws.OpContinuation && fragmented:
		return fail("expected continuation frame")
	}
	return nil
}

// checkSize enforces the message size limit on a data frame of a message that
// already holds have bytes.
func checkSize(h ws.Header, have int, limit int64) *protocolError {
	if limit > 0 && int64(have)+h.Length > limit {
		return &protocolError{ws.StatusMessageTooBig, "message too large"}
	}
	return nil
}

// headerError maps a parseHeader failure to the close status to send.
func headerError(err error) *protocolError {
	if errors.Is(err, errFrameTooLarge) {
		return &protocolError{ws.StatusMessageTooBig, "frame too large"}
	}
	return &protocolError{ws.StatusProtocolError, "malformed frame header"}
}

// checkClosePayload validates a Close frame body (RFC 6455 5.5.1, 7.4) and
// returns the body to echo, or the status to fail with.
func checkClosePayload(p []byte) ([]byte, *protocolError) {
	switch {
	case len(p) == 0:
		return closeNormal, nil
	case len(p) == 1:
		return nil, &protocolError{ws.StatusProtocolError, "truncated close status"}
	}
	code := ws.StatusCode(binary.BigEndian.Uint16(p[:2]))
	if code >= 5000 {
		return nil, &protocolError{ws.StatusProtocolError, "invalid close status"}
	}
	if err := ws.CheckCloseFrameData(code, string(p[2:])); err != nil {
		if errors.Is(err, ws.ErrProtocolInvalidUTF8) {
			return nil, &protocolError{ws.StatusInvalidFramePayloadData, "invalid UTF-8 in close reason"}
		}
		return nil, &protocolError{ws.StatusProtocolError, "invalid close status"}
	}
	return p[:2], nil
}

var closeNormal = []byte{0x03, 0xE8} // status 1000
