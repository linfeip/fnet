package fh2

import (
	"encoding/binary"

	"github.com/linfeip/fnet/internal/bytepool"
)

const (
	clientPreface  = "PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n"
	frameHeaderLen = 9

	defaultHeaderTableSize   = 4096
	defaultInitialWindowSize = 65535
	minMaxFrameSize          = 16 << 10
	maxFrameSizeLimit        = 1<<24 - 1
	maxWindowSize            = 1<<31 - 1
)

// Frame types (RFC 9113 6).
const (
	frameData         uint8 = 0x0
	frameHeaders      uint8 = 0x1
	framePriority     uint8 = 0x2
	frameRSTStream    uint8 = 0x3
	frameSettings     uint8 = 0x4
	framePushPromise  uint8 = 0x5
	framePing         uint8 = 0x6
	frameGoAway       uint8 = 0x7
	frameWindowUpdate uint8 = 0x8
	frameContinuation uint8 = 0x9
)

// Frame flags.
const (
	flagEndStream  uint8 = 0x1
	flagAck        uint8 = 0x1
	flagEndHeaders uint8 = 0x4
	flagPadded     uint8 = 0x8
	flagPriority   uint8 = 0x20
)

// errCode is an HTTP/2 32-bit error code (RFC 9113 7).
type errCode uint32

const (
	errCodeNo                 errCode = 0x0
	errCodeProtocol           errCode = 0x1
	errCodeInternal           errCode = 0x2
	errCodeFlowControl        errCode = 0x3
	errCodeSettingsTimeout    errCode = 0x4
	errCodeStreamClosed       errCode = 0x5
	errCodeFrameSize          errCode = 0x6
	errCodeRefusedStream      errCode = 0x7
	errCodeCancel             errCode = 0x8
	errCodeCompression        errCode = 0x9
	errCodeConnect            errCode = 0xa
	errCodeEnhanceYourCalm    errCode = 0xb
	errCodeInadequateSecurity errCode = 0xc
	errCodeHTTP11Required     errCode = 0xd
)

// SETTINGS identifiers (RFC 9113 6.5.2, RFC 9218 5.1).
const (
	settingHeaderTableSize      uint16 = 0x1
	settingEnablePush           uint16 = 0x2
	settingMaxConcurrentStreams uint16 = 0x3
	settingInitialWindowSize    uint16 = 0x4
	settingMaxFrameSize         uint16 = 0x5
	settingMaxHeaderListSize    uint16 = 0x6
	settingNoRFC7540Priorities  uint16 = 0x9
)

type setting struct {
	id    uint16
	value uint32
}

// frameHeader is the 9-byte header of an HTTP/2 frame (RFC 9113 4.1).
type frameHeader struct {
	length    uint32
	streamID  uint32
	frameType uint8
	flags     uint8
}

// parseFrameHeader decodes the 9-byte header at the start of b.
func parseFrameHeader(b []byte) frameHeader {
	return frameHeader{
		length:    uint32(b[0])<<16 | uint32(b[1])<<8 | uint32(b[2]),
		frameType: b[3],
		flags:     b[4],
		streamID:  binary.BigEndian.Uint32(b[5:9]) & 0x7fffffff,
	}
}

// putFrameHeader writes a 9-byte frame header into dst[:9].
func putFrameHeader(dst []byte, length int, frameType, flags uint8, streamID uint32) {
	dst[0] = byte(length >> 16)
	dst[1] = byte(length >> 8)
	dst[2] = byte(length)
	dst[3] = frameType
	dst[4] = flags
	binary.BigEndian.PutUint32(dst[5:9], streamID&0x7fffffff)
}

func appendFrameHeader(b *bytepool.Buffer, length int, frameType, flags uint8, streamID uint32) {
	var hdr [frameHeaderLen]byte
	putFrameHeader(hdr[:], length, frameType, flags, streamID)
	b.Write(hdr[:])
}

// stripPadding removes the Pad Length byte and trailing padding from payload when flagPadded is set, returning the
// unpadded slice, the number of framing/padding bytes removed (counted in flow control but not delivered to the
// application), and whether the padding length is valid (RFC 9113 6.1, 6.2).
func stripPadding(payload []byte, flags uint8) (unpadded []byte, padBytes int, ok bool) {
	if flags&flagPadded == 0 {
		return payload, 0, true
	}
	if len(payload) == 0 {
		return nil, 0, false
	}
	pad := int(payload[0])
	if 1+pad > len(payload) {
		return nil, 0, false
	}
	return payload[1 : len(payload)-pad], 1 + pad, true
}

func appendSettings(b *bytepool.Buffer, settings ...setting) {
	appendFrameHeader(b, 6*len(settings), frameSettings, 0, 0)
	var item [6]byte
	for _, s := range settings {
		binary.BigEndian.PutUint16(item[0:2], s.id)
		binary.BigEndian.PutUint32(item[2:6], s.value)
		b.Write(item[:])
	}
}

func appendSettingsAck(b *bytepool.Buffer) {
	appendFrameHeader(b, 0, frameSettings, flagAck, 0)
}

func appendPing(b *bytepool.Buffer, ack bool, data []byte) {
	flags := uint8(0)
	if ack {
		flags = flagAck
	}
	appendFrameHeader(b, 8, framePing, flags, 0)
	b.Write(data[:8])
}

func appendRSTStream(b *bytepool.Buffer, streamID uint32, code errCode) {
	var raw [frameHeaderLen + 4]byte
	putFrameHeader(raw[:], 4, frameRSTStream, 0, streamID)
	binary.BigEndian.PutUint32(raw[frameHeaderLen:], uint32(code))
	b.Write(raw[:])
}

func appendGoAway(b *bytepool.Buffer, lastStreamID uint32, code errCode) {
	var raw [frameHeaderLen + 8]byte
	putFrameHeader(raw[:], 8, frameGoAway, 0, 0)
	binary.BigEndian.PutUint32(raw[frameHeaderLen:], lastStreamID&0x7fffffff)
	binary.BigEndian.PutUint32(raw[frameHeaderLen+4:], uint32(code))
	b.Write(raw[:])
}

func appendWindowUpdate(b *bytepool.Buffer, streamID, increment uint32) {
	var raw [frameHeaderLen + 4]byte
	putFrameHeader(raw[:], 4, frameWindowUpdate, 0, streamID)
	binary.BigEndian.PutUint32(raw[frameHeaderLen:], increment&0x7fffffff)
	b.Write(raw[:])
}

// appendHeaders writes headerBlock as a HEADERS frame followed by CONTINUATION frames as needed to stay within
// maxFrameSize.
func appendHeaders(b *bytepool.Buffer, streamID uint32, headerBlock []byte, endStream bool, maxFrameSize int) {
	flags := uint8(0)
	if endStream {
		flags |= flagEndStream
	}
	first := min(len(headerBlock), maxFrameSize)
	if first == len(headerBlock) {
		flags |= flagEndHeaders
	}
	appendFrameHeader(b, first, frameHeaders, flags, streamID)
	b.Write(headerBlock[:first])
	headerBlock = headerBlock[first:]

	for len(headerBlock) > 0 {
		chunk := min(len(headerBlock), maxFrameSize)
		contFlags := uint8(0)
		if chunk == len(headerBlock) {
			contFlags = flagEndHeaders
		}
		appendFrameHeader(b, chunk, frameContinuation, contFlags, streamID)
		b.Write(headerBlock[:chunk])
		headerBlock = headerBlock[chunk:]
	}
}
