package kcp

import (
	"encoding/binary"
	"errors"

	"github.com/linfeip/fnet/internal/bytepool"
)

const (
	headerSize        = 24
	sequenceOffset    = 12
	defaultMTU        = 1400
	minMTU            = 128
	maxMTU            = 65500
	defaultWindowSize = 128

	cmdPush            uint8 = 81
	cmdAck             uint8 = 82
	cmdWindowProbeAsk  uint8 = 83
	cmdWindowProbeTell uint8 = 84

	probeAskSend uint32 = 1 << 0
	probeAskTell uint32 = 1 << 1

	rtoNoDelay uint32 = 30
	rtoMin     uint32 = 100
	rtoDefault uint32 = 200
	rtoMax     uint32 = 60000

	probeInit       uint32 = 500
	probeLimit      uint32 = 120000
	deadLinkLimit   uint32 = 20
	threshInit      uint32 = 2
	threshMin       uint32 = 2
	fastResendLimit uint32 = 2
)

// ErrMessageTooLarge is returned by Conn.Write in message mode (Options.StreamMode == false) when b splits into more
// segments than ReceiveWindow (or 256) allows to be reassembled at once. Enable Options.StreamMode for arbitrarily
// large writes.
var ErrMessageTooLarge = errors.New("kcp: message exceeds receive window in message mode")

type ackItem struct {
	sequence  uint32
	timestamp uint32
}

type segment struct {
	buf             bytepool.Buffer
	sequence        uint32
	timestamp       uint32
	resendTimestamp uint32
	rto             uint32
	fastAckCount    uint32
	transmissions   uint32
	fragment        uint8
}

type arqConfig struct {
	conversationID uint32
	mtu            int
	sendWindow     uint32
	receiveWindow  uint32
	interval       uint32
	noDelay        bool
	streamMode     bool
}

// arqState is a wire-compatible KCP ARQ state machine with zero interface boxing and zero resident packet buffers when
// idle: segment payloads and temporary encoding buffers come from bytepool and return as soon as acknowledged or read.
type arqState struct {
	conversationID     uint32
	mtu                int
	maxSegmentSize     int
	streamMode         bool
	noDelay            bool
	noCongestionWindow bool
	dead               bool

	sendUnacknowledged  uint32
	sendNext            uint32
	receiveNext         uint32
	sendWindow          uint32
	receiveWindow       uint32
	remoteWindow        uint32
	congestionWindow    uint32
	slowStartThreshold  uint32
	congestionIncrement uint32

	smoothedRTT          int32
	rttVariance          int32
	retransmitTimeout    uint32
	minRetransmitTimeout uint32
	interval             uint32
	nextFlushTimestamp   uint32
	flushInitialized     bool
	fastResend           uint32

	probeFlags     uint32
	probeTimestamp uint32
	probeWait      uint32

	sendQueue    []segment
	sendBuf      []segment
	receiveBuf   []segment
	receiveQueue []segment
	pendingAcks  []ackItem
	output       func([]byte)
}

func (a *arqState) init(cfg arqConfig, output func([]byte)) {
	minRTO := rtoMin
	var fastResend uint32
	if cfg.noDelay {
		minRTO = rtoNoDelay
		fastResend = fastResendLimit
	}
	*a = arqState{
		conversationID:       cfg.conversationID,
		mtu:                  cfg.mtu,
		maxSegmentSize:       cfg.mtu - headerSize,
		streamMode:           cfg.streamMode,
		noDelay:              cfg.noDelay,
		noCongestionWindow:   cfg.noDelay,
		sendWindow:           cfg.sendWindow,
		receiveWindow:        cfg.receiveWindow,
		remoteWindow:         cfg.receiveWindow,
		slowStartThreshold:   threshInit,
		retransmitTimeout:    rtoDefault,
		minRetransmitTimeout: minRTO,
		interval:             cfg.interval,
		fastResend:           fastResend,
		output:               output,
	}
}

func timeDiff(later, earlier uint32) int32 {
	return int32(later - earlier)
}

func (a *arqState) release() {
	for i := range a.sendQueue {
		a.sendQueue[i].buf.Release()
	}
	for i := range a.sendBuf {
		a.sendBuf[i].buf.Release()
	}
	for i := range a.receiveBuf {
		a.receiveBuf[i].buf.Release()
	}
	for i := range a.receiveQueue {
		a.receiveQueue[i].buf.Release()
	}
	a.sendQueue = nil
	a.sendBuf = nil
	a.receiveBuf = nil
	a.receiveQueue = nil
	a.pendingAcks = nil
}

func (a *arqState) send(data []byte) error {
	if a.streamMode {
		if n := len(a.sendQueue); n > 0 {
			last := &a.sendQueue[n-1]
			if room := a.maxSegmentSize - last.buf.Len(); room > 0 {
				take := min(len(data), room)
				last.buf.Write(data[:take])
				data = data[take:]
			}
		}
		for len(data) > 0 {
			take := min(len(data), a.maxSegmentSize)
			buf := bytepool.Get(take)
			buf.Write(data[:take])
			a.sendQueue = append(a.sendQueue, segment{buf: buf})
			data = data[take:]
		}
		return nil
	}

	count := 1
	if len(data) > a.maxSegmentSize {
		count = (len(data) + a.maxSegmentSize - 1) / a.maxSegmentSize
	}
	maxFragments := min(int(a.receiveWindow)-1, 255)
	if count-1 > maxFragments {
		return ErrMessageTooLarge
	}
	for i := range count {
		take := min(len(data), a.maxSegmentSize)
		var buf bytepool.Buffer
		if take > 0 {
			buf = bytepool.Get(take)
			buf.Write(data[:take])
		}
		a.sendQueue = append(a.sendQueue, segment{
			buf:      buf,
			fragment: uint8(count - i - 1),
		})
		data = data[take:]
	}
	return nil
}

// peekSize returns the byte length of the next complete message in receiveQueue, or -1 if none is ready yet.
func (a *arqState) peekSize() int {
	if len(a.receiveQueue) == 0 {
		return -1
	}
	first := &a.receiveQueue[0]
	if first.fragment == 0 {
		return first.buf.Len()
	}
	needed := int(first.fragment) + 1
	if len(a.receiveQueue) < needed {
		return -1
	}
	total := 0
	for i := range needed {
		total += a.receiveQueue[i].buf.Len()
	}
	return total
}

// popMessage extracts the next complete message as a single bytepool.Buffer, transferring ownership to the caller
// (which calls Release after OnData); ok reports whether a complete message was available.
func (a *arqState) popMessage() (msg bytepool.Buffer, ok bool) {
	size := a.peekSize()
	if size < 0 {
		return bytepool.Buffer{}, false
	}
	wasFull := uint32(len(a.receiveQueue)) >= a.receiveWindow

	if a.receiveQueue[0].fragment == 0 {
		// Single-segment fast path: hand the segment's buffer straight to OnData with zero copy.
		msg = a.receiveQueue[0].buf
		a.receiveQueue[0] = segment{}
		a.dropReceiveQueue(1)
	} else {
		needed := int(a.receiveQueue[0].fragment) + 1
		if size > 0 {
			msg = bytepool.Get(size)
		}
		for i := range needed {
			msg.Write(a.receiveQueue[i].buf.Bytes())
			a.receiveQueue[i].buf.Release()
			a.receiveQueue[i] = segment{}
		}
		a.dropReceiveQueue(needed)
	}

	a.promoteReceiveBuf()
	if wasFull && uint32(len(a.receiveQueue)) < a.receiveWindow {
		a.probeFlags |= probeAskTell
	}
	return msg, true
}

func (a *arqState) input(data []byte, currentMs uint32) bool {
	if len(data) < headerSize {
		return false
	}
	prevUna := a.sendUnacknowledged
	var (
		latestTS    uint32
		hasRTT      bool
		fastTrigger bool
	)

	for len(data) >= headerSize {
		conv := binary.LittleEndian.Uint32(data[0:4])
		cmd := data[4]
		frg := data[5]
		wnd := binary.LittleEndian.Uint16(data[6:8])
		ts := binary.LittleEndian.Uint32(data[8:12])
		sn := binary.LittleEndian.Uint32(data[12:16])
		una := binary.LittleEndian.Uint32(data[16:20])
		length := binary.LittleEndian.Uint32(data[20:24])
		data = data[headerSize:]

		if conv != a.conversationID || len(data) < int(length) {
			return false
		}
		if cmd != cmdPush && cmd != cmdAck && cmd != cmdWindowProbeAsk && cmd != cmdWindowProbeTell {
			return false
		}

		a.remoteWindow = uint32(wnd)
		if a.parseUnacknowledged(una) {
			fastTrigger = true
		}
		a.refreshSendUnacknowledged()

		switch cmd {
		case cmdAck:
			a.parseAck(sn)
			a.refreshSendUnacknowledged()
			if a.parseFastAck(sn) {
				fastTrigger = true
			}
			hasRTT = true
			latestTS = ts
		case cmdPush:
			if timeDiff(sn, a.receiveNext+a.receiveWindow) < 0 {
				a.pendingAcks = append(a.pendingAcks, ackItem{sequence: sn, timestamp: ts})
				if timeDiff(sn, a.receiveNext) >= 0 {
					a.insertDataSegment(sn, frg, data[:length])
				}
			}
		case cmdWindowProbeAsk:
			a.probeFlags |= probeAskTell
		case cmdWindowProbeTell:
		}

		data = data[length:]
	}

	if hasRTT && timeDiff(currentMs, latestTS) >= 0 {
		a.updateRTT(timeDiff(currentMs, latestTS))
	}
	if !a.noCongestionWindow && timeDiff(a.sendUnacknowledged, prevUna) > 0 {
		a.growCongestionWindow()
	}
	return fastTrigger
}

func (a *arqState) updateRTT(rtt int32) {
	if a.smoothedRTT == 0 {
		a.smoothedRTT = rtt
		a.rttVariance = rtt / 2
	} else {
		delta := rtt - a.smoothedRTT
		if delta < 0 {
			delta = -delta
		}
		a.rttVariance = (3*a.rttVariance + delta) / 4
		a.smoothedRTT = max((7*a.smoothedRTT+rtt)/8, 1)
	}
	rto := a.smoothedRTT + max(int32(a.interval), 4*a.rttVariance)
	a.retransmitTimeout = min(max(uint32(rto), a.minRetransmitTimeout), rtoMax)
}

func (a *arqState) growCongestionWindow() {
	if a.congestionWindow >= a.remoteWindow {
		return
	}
	mss := uint32(a.maxSegmentSize)
	if a.congestionWindow < a.slowStartThreshold {
		a.congestionWindow++
		a.congestionIncrement += mss
	} else {
		if a.congestionIncrement < mss {
			a.congestionIncrement = mss
		}
		a.congestionIncrement += (mss*mss)/a.congestionIncrement + mss/16
		if (a.congestionWindow+1)*mss <= a.congestionIncrement {
			a.congestionWindow = (a.congestionIncrement + mss - 1) / mss
		}
	}
	if a.congestionWindow > a.remoteWindow {
		a.congestionWindow = a.remoteWindow
		a.congestionIncrement = a.remoteWindow * mss
	}
}

func (a *arqState) parseUnacknowledged(una uint32) bool {
	removed := 0
	for i := range a.sendBuf {
		if timeDiff(una, a.sendBuf[i].sequence) <= 0 {
			break
		}
		a.sendBuf[i].buf.Release()
		a.sendBuf[i] = segment{}
		removed++
	}
	if removed == 0 {
		return false
	}
	n := copy(a.sendBuf, a.sendBuf[removed:])
	clear(a.sendBuf[n:])
	a.sendBuf = a.sendBuf[:n]
	return true
}

func (a *arqState) parseAck(sn uint32) {
	if timeDiff(sn, a.sendUnacknowledged) < 0 || timeDiff(sn, a.sendNext) >= 0 {
		return
	}
	for i := range a.sendBuf {
		segSeq := a.sendBuf[i].sequence
		if segSeq == sn {
			a.sendBuf[i].buf.Release()
			n := copy(a.sendBuf[i:], a.sendBuf[i+1:])
			a.sendBuf[i+n] = segment{}
			a.sendBuf = a.sendBuf[:i+n]
			return
		}
		if timeDiff(sn, segSeq) < 0 {
			return
		}
	}
}

func (a *arqState) parseFastAck(sn uint32) bool {
	if timeDiff(sn, a.sendUnacknowledged) < 0 || timeDiff(sn, a.sendNext) >= 0 {
		return false
	}
	triggered := false
	for i := range a.sendBuf {
		if timeDiff(sn, a.sendBuf[i].sequence) <= 0 {
			break
		}
		a.sendBuf[i].fastAckCount++
		if a.fastResend > 0 && a.sendBuf[i].fastAckCount >= a.fastResend {
			triggered = true
		}
	}
	return triggered
}

func (a *arqState) refreshSendUnacknowledged() {
	if len(a.sendBuf) > 0 {
		a.sendUnacknowledged = a.sendBuf[0].sequence
	} else {
		a.sendUnacknowledged = a.sendNext
	}
}

func (a *arqState) insertDataSegment(sn uint32, frg uint8, payload []byte) {
	// Fast path for in-order segments: append straight to receiveQueue without touching receiveBuf.
	if sn == a.receiveNext && len(a.receiveBuf) == 0 && uint32(len(a.receiveQueue)) < a.receiveWindow {
		var buf bytepool.Buffer
		if len(payload) > 0 {
			buf = bytepool.Get(len(payload))
			buf.Write(payload)
		}
		a.receiveQueue = append(a.receiveQueue, segment{buf: buf, sequence: sn, fragment: frg})
		a.receiveNext++
		return
	}

	insertIdx := len(a.receiveBuf)
	for insertIdx > 0 {
		prevSeq := a.receiveBuf[insertIdx-1].sequence
		if prevSeq == sn {
			return // duplicate segment
		}
		if timeDiff(sn, prevSeq) > 0 {
			break
		}
		insertIdx--
	}

	var buf bytepool.Buffer
	if len(payload) > 0 {
		buf = bytepool.Get(len(payload))
		buf.Write(payload)
	}
	seg := segment{buf: buf, sequence: sn, fragment: frg}
	a.receiveBuf = append(a.receiveBuf, segment{})
	copy(a.receiveBuf[insertIdx+1:], a.receiveBuf[insertIdx:])
	a.receiveBuf[insertIdx] = seg

	a.promoteReceiveBuf()
}

func (a *arqState) promoteReceiveBuf() {
	promoted := 0
	for promoted < len(a.receiveBuf) {
		if a.receiveBuf[promoted].sequence != a.receiveNext || uint32(len(a.receiveQueue)) >= a.receiveWindow {
			break
		}
		a.receiveQueue = append(a.receiveQueue, a.receiveBuf[promoted])
		a.receiveBuf[promoted] = segment{}
		a.receiveNext++
		promoted++
	}
	if promoted > 0 {
		n := copy(a.receiveBuf, a.receiveBuf[promoted:])
		clear(a.receiveBuf[n:])
		a.receiveBuf = a.receiveBuf[:n]
	}
}

func (a *arqState) dropReceiveQueue(count int) {
	n := copy(a.receiveQueue, a.receiveQueue[count:])
	clear(a.receiveQueue[n:])
	a.receiveQueue = a.receiveQueue[:n]
}

func (a *arqState) unusedReceiveWindow() uint16 {
	used := uint32(len(a.receiveQueue))
	if used >= a.receiveWindow {
		return 0
	}
	return uint16(min(a.receiveWindow-used, 0xFFFF))
}

func (a *arqState) update(currentMs uint32) {
	if !a.flushInitialized {
		a.flushInitialized = true
		a.nextFlushTimestamp = currentMs
	}
	diff := timeDiff(currentMs, a.nextFlushTimestamp)
	if diff >= 10000 || diff < -10000 {
		a.nextFlushTimestamp = currentMs
		diff = 0
	}
	if diff >= 0 {
		a.nextFlushTimestamp += a.interval
		if timeDiff(currentMs, a.nextFlushTimestamp) >= 0 {
			a.nextFlushTimestamp = currentMs + a.interval
		}
		a.flush(currentMs, false)
	}
}

func (a *arqState) hasPendingWork() bool {
	return len(a.pendingAcks) > 0 || len(a.sendQueue) > 0 || len(a.sendBuf) > 0 || a.probeFlags != 0
}

func encodeHeader(dst []byte, conv uint32, cmd, frg uint8, wnd uint16, ts, sn, una, length uint32) {
	binary.LittleEndian.PutUint32(dst[0:4], conv)
	dst[4] = cmd
	dst[5] = frg
	binary.LittleEndian.PutUint16(dst[6:8], wnd)
	binary.LittleEndian.PutUint32(dst[8:12], ts)
	binary.LittleEndian.PutUint32(dst[12:16], sn)
	binary.LittleEndian.PutUint32(dst[16:20], una)
	binary.LittleEndian.PutUint32(dst[20:24], length)
}

func (a *arqState) flush(currentMs uint32, ackOnly bool) {
	if !a.hasPendingWork() && a.remoteWindow > 0 {
		return
	}
	encBuf := bytepool.Get(a.mtu)
	defer encBuf.Release()
	pkt := encBuf.Bytes()[:a.mtu]
	offset := 0

	wnd := a.unusedReceiveWindow()
	for i, ack := range a.pendingAcks {
		if timeDiff(ack.sequence, a.receiveNext) >= 0 || i == len(a.pendingAcks)-1 {
			if offset+headerSize > a.mtu {
				a.output(pkt[:offset])
				offset = 0
			}
			encodeHeader(pkt[offset:], a.conversationID, cmdAck, 0, wnd, ack.timestamp, ack.sequence, a.receiveNext, 0)
			offset += headerSize
		}
	}
	a.pendingAcks = a.pendingAcks[:0]
	if ackOnly {
		if offset > 0 {
			a.output(pkt[:offset])
		}
		return
	}

	if a.remoteWindow == 0 {
		if a.probeWait == 0 {
			a.probeWait = probeInit
			a.probeTimestamp = currentMs + a.probeWait
		} else if timeDiff(currentMs, a.probeTimestamp) >= 0 {
			a.probeWait = min(max(a.probeWait, probeInit)+a.probeWait/2, probeLimit)
			a.probeTimestamp = currentMs + a.probeWait
			a.probeFlags |= probeAskSend
		}
	} else {
		a.probeTimestamp = 0
		a.probeWait = 0
	}

	if a.probeFlags&probeAskSend != 0 {
		if offset+headerSize > a.mtu {
			a.output(pkt[:offset])
			offset = 0
		}
		encodeHeader(pkt[offset:], a.conversationID, cmdWindowProbeAsk, 0, wnd, 0, 0, a.receiveNext, 0)
		offset += headerSize
	}
	if a.probeFlags&probeAskTell != 0 {
		if offset+headerSize > a.mtu {
			a.output(pkt[:offset])
			offset = 0
		}
		encodeHeader(pkt[offset:], a.conversationID, cmdWindowProbeTell, 0, wnd, 0, 0, a.receiveNext, 0)
		offset += headerSize
	}
	a.probeFlags = 0

	cwnd := min(a.sendWindow, a.remoteWindow)
	if !a.noCongestionWindow {
		cwnd = min(cwnd, a.congestionWindow)
	}
	moved := 0
	for moved < len(a.sendQueue) && timeDiff(a.sendNext, a.sendUnacknowledged+cwnd) < 0 {
		seg := a.sendQueue[moved]
		a.sendQueue[moved] = segment{}
		seg.sequence = a.sendNext
		a.sendNext++
		a.sendBuf = append(a.sendBuf, seg)
		moved++
	}
	if moved > 0 {
		n := copy(a.sendQueue, a.sendQueue[moved:])
		clear(a.sendQueue[n:])
		a.sendQueue = a.sendQueue[:n]
	}

	var (
		fastRetransmitted bool
		lost              bool
	)
	for i := range a.sendBuf {
		seg := &a.sendBuf[i]
		needSend := false
		switch {
		case seg.transmissions == 0:
			needSend = true
			seg.transmissions = 1
			seg.rto = a.retransmitTimeout
			seg.resendTimestamp = currentMs + seg.rto
		case timeDiff(currentMs, seg.resendTimestamp) >= 0:
			needSend = true
			seg.transmissions++
			if !a.noDelay {
				seg.rto += max(a.retransmitTimeout, uint32(a.interval))
			} else {
				step := a.retransmitTimeout / 2
				seg.rto += max(step, rtoNoDelay)
			}
			seg.rto = min(seg.rto, rtoMax)
			seg.resendTimestamp = currentMs + seg.rto
			lost = true
		case a.fastResend > 0 && seg.fastAckCount >= a.fastResend:
			needSend = true
			seg.transmissions++
			seg.fastAckCount = 0
			seg.resendTimestamp = currentMs + seg.rto
			fastRetransmitted = true
		}

		if needSend {
			seg.timestamp = currentMs
			payload := seg.buf.Bytes()
			if offset+headerSize+len(payload) > a.mtu && offset > 0 {
				a.output(pkt[:offset])
				offset = 0
			}
			encodeHeader(pkt[offset:], a.conversationID, cmdPush, seg.fragment, wnd, seg.timestamp, seg.sequence, a.receiveNext, uint32(len(payload)))
			offset += headerSize
			copy(pkt[offset:], payload)
			offset += len(payload)
			if seg.transmissions >= deadLinkLimit {
				a.dead = true
			}
		}
	}

	if offset > 0 {
		a.output(pkt[:offset])
	}

	if !a.noCongestionWindow {
		if fastRetransmitted {
			inflight := a.sendNext - a.sendUnacknowledged
			a.slowStartThreshold = max(inflight/2, threshMin)
			a.congestionWindow = a.slowStartThreshold + a.fastResend
			a.congestionIncrement = a.congestionWindow * uint32(a.maxSegmentSize)
		}
		if lost {
			a.slowStartThreshold = max(cwnd/2, threshMin)
			a.congestionWindow = 1
			a.congestionIncrement = uint32(a.maxSegmentSize)
		}
		if a.congestionWindow < 1 {
			a.congestionWindow = 1
			a.congestionIncrement = uint32(a.maxSegmentSize)
		}
	}
}
