package fnet

import (
	"encoding/binary"
	"errors"
	"math"

	"github.com/gobwas/ws"
)

// maxWSFrameLength bounds a single frame so that header.Length can never
// overflow int arithmetic on the reactor.
const maxWSFrameLength = math.MaxInt32

var errWSFrameTooLarge = errors.New("fnet: websocket frame too large")

// parseWSHeader decodes a WebSocket frame header from buf without allocating.
// complete reports whether the whole header was present; when false (and err
// is nil) the caller must wait for more bytes. size is the header length.
func parseWSHeader(buf []byte) (h ws.Header, size int, complete bool, err error) {
	if len(buf) < 2 {
		return
	}
	b0, b1 := buf[0], buf[1]
	h.Fin = b0&0x80 != 0
	h.Rsv = (b0 & 0x70) >> 4
	h.OpCode = ws.OpCode(b0 & 0x0f)
	h.Masked = b1&0x80 != 0

	length := uint64(b1 & 0x7f)
	size = 2
	switch length {
	case 126:
		if len(buf) < 4 {
			return
		}
		length = uint64(binary.BigEndian.Uint16(buf[2:4]))
		size = 4
	case 127:
		if len(buf) < 10 {
			return
		}
		length = binary.BigEndian.Uint64(buf[2:10])
		size = 10
		if length>>63 != 0 {
			return h, 0, false, ws.ErrHeaderLengthMSB
		}
	}
	if h.Masked {
		if len(buf) < size+4 {
			return
		}
		copy(h.Mask[:], buf[size:size+4])
		size += 4
	}
	if length > maxWSFrameLength {
		return h, 0, false, errWSFrameTooLarge
	}
	h.Length = int64(length)
	complete = true
	return
}

// encodeWSServerHeader writes an unmasked FIN frame header into dst (>= 10 bytes)
// and returns the number of bytes used (2, 4 or 10).
func encodeWSServerHeader(dst []byte, op ws.OpCode, length int) int {
	dst[0] = 0x80 | byte(op)
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

// writeWSFrame sends a single unmasked server frame through the VirtualConn.
func writeWSFrame(vc *VirtualConn, op ws.OpCode, payload []byte) error {
	var buf [256]byte
	if len(payload) <= len(buf)-10 {
		n := encodeWSServerHeader(buf[:10], op, len(payload))
		copy(buf[n:], payload)
		_, err := vc.Write(buf[:n+len(payload)])
		return err
	}
	var hdr [10]byte
	n := encodeWSServerHeader(hdr[:], op, len(payload))
	_, err := vc.WriteVector([][]byte{hdr[:n], payload})
	return err
}

// closeNormalBody is the 2-byte body for status 1000 (normal closure).
var closeNormalBody = [2]byte{0x03, 0xE8}
