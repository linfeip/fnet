//go:build !race

package kcp

import "testing"

func TestZeroSteadyStateAllocs(t *testing.T) {
	var (
		wire []byte
		a, b arqState
	)
	cfg := arqConfig{
		conversationID: 7,
		mtu:            1400,
		sendWindow:     128,
		receiveWindow:  128,
		interval:       10,
		noDelay:        true,
	}
	wireBuf := make([]byte, 1400)
	a.init(cfg, func(pkt []byte) { wire = wireBuf[:copy(wireBuf, pkt)] })
	b.init(cfg, func(pkt []byte) { wire = wireBuf[:copy(wireBuf, pkt)] })
	defer a.release()
	defer b.release()

	payload := []byte("zero-alloc-kcp-ping-pong-payload-64-bytes-0123456789abcdef012345")
	nowMs := uint32(100)
	allocs := testing.AllocsPerRun(1000, func() {
		nowMs += 10
		_ = a.send(payload)
		a.flush(nowMs, false)
		b.input(wire, nowMs)
		msg, ok := b.popMessage()
		if !ok {
			t.Fatal("expected message")
		}
		_ = b.send(msg.Bytes())
		msg.Release()
		b.flush(nowMs, false)
		a.input(wire, nowMs)
		echo, ok := a.popMessage()
		if !ok {
			t.Fatal("expected echo")
		}
		echo.Release()
	})
	if allocs != 0 {
		t.Fatalf("steady-state ARQ round-trip allocated %.2f objects/op, want 0", allocs)
	}
}
