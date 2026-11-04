package fh2

import (
	"bytes"
	"testing"

	"github.com/linfeip/fnet/internal/bytepool"
)

func TestFrameHeaderRoundTrip(t *testing.T) {
	var raw [frameHeaderLen]byte
	putFrameHeader(raw[:], 0x123456, frameHeaders, flagEndHeaders|flagEndStream, 0x7fffffff)
	// Set the reserved high bit on stream ID and verify parseFrameHeader masks it out.
	raw[5] |= 0x80

	fh := parseFrameHeader(raw[:])
	if fh.length != 0x123456 || fh.frameType != frameHeaders || fh.flags != (flagEndHeaders|flagEndStream) || fh.streamID != 0x7fffffff {
		t.Fatalf("unexpected parsed frame header: %+v", fh)
	}
}

func TestStripPadding(t *testing.T) {
	// Unpadded payload passes through untouched.
	payload := []byte("hello")
	got, padBytes, ok := stripPadding(payload, 0)
	if !ok || padBytes != 0 || !bytes.Equal(got, payload) {
		t.Fatalf("unpadded: ok=%v pad=%d got=%q", ok, padBytes, got)
	}

	// Zero-length payload with flagPadded is invalid.
	if _, _, ok := stripPadding(nil, flagPadded); ok {
		t.Fatal("expected empty padded payload to fail")
	}

	// Valid zero-byte padding (1 byte Pad Length = 0).
	got, padBytes, ok = stripPadding([]byte{0}, flagPadded)
	if !ok || padBytes != 1 || len(got) != 0 {
		t.Fatalf("zero pad: ok=%v pad=%d got=%q", ok, padBytes, got)
	}

	// Valid 3-byte padding around "abc".
	got, padBytes, ok = stripPadding([]byte{3, 'a', 'b', 'c', 0, 0, 0}, flagPadded)
	if !ok || padBytes != 4 || string(got) != "abc" {
		t.Fatalf("valid pad: ok=%v pad=%d got=%q", ok, padBytes, got)
	}

	// Padding length equal to payload length is invalid (RFC 9113 6.1).
	if _, _, ok := stripPadding([]byte{1}, flagPadded); ok {
		t.Fatal("expected pad >= payload length to fail")
	}
	if _, _, ok := stripPadding([]byte{4, 'a', 'b', 'c'}, flagPadded); ok {
		t.Fatal("expected pad exceeding payload to fail")
	}
}

func TestAppendHeadersSplitsContinuation(t *testing.T) {
	var buf bytepool.Buffer
	defer buf.Release()

	block := bytes.Repeat([]byte("x"), 40)
	appendHeaders(&buf, 3, block, true, 16)

	raw := buf.Bytes()
	// Expect: HEADERS(16B, flagEndStream) + CONTINUATION(16B, 0) + CONTINUATION(8B, flagEndHeaders).
	fh1 := parseFrameHeader(raw[:9])
	if fh1.frameType != frameHeaders || fh1.length != 16 || fh1.flags != flagEndStream || fh1.streamID != 3 {
		t.Fatalf("frame 1: %+v", fh1)
	}
	fh2 := parseFrameHeader(raw[25:34])
	if fh2.frameType != frameContinuation || fh2.length != 16 || fh2.flags != 0 || fh2.streamID != 3 {
		t.Fatalf("frame 2: %+v", fh2)
	}
	fh3 := parseFrameHeader(raw[50:59])
	if fh3.frameType != frameContinuation || fh3.length != 8 || fh3.flags != flagEndHeaders || fh3.streamID != 3 {
		t.Fatalf("frame 3: %+v", fh3)
	}
	if len(raw) != 67 {
		t.Fatalf("unexpected total encoded length %d", len(raw))
	}
}
