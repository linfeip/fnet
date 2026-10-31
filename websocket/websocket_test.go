package websocket

import (
	"bufio"
	"bytes"
	"crypto/ecdsa"
	"crypto/elliptic"
	cryptorand "crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"log"
	"log/slog"
	"math"
	"math/big"
	"net"
	"net/http"
	"os"
	"runtime"
	"runtime/metrics"
	"slices"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/linfeip/fnet"
	"github.com/linfeip/fnet/fhttp"
	"github.com/linfeip/fnet/taskpool"
)

var testMask = [4]byte{0x12, 0x34, 0x56, 0x78}

// appendHeader encodes any frameHeader (including client mask and non-zero reserved bits) for tests.
func appendHeader(b []byte, h frameHeader) []byte {
	b0 := byte(h.opCode) | (h.reserved&0x07)<<4
	if h.final {
		b0 |= 0x80
	}
	var b1 byte
	if h.masked {
		b1 = 0x80
	}
	switch {
	case h.length <= 125:
		b = append(b, b0, b1|byte(h.length))
	case h.length <= 0xffff:
		b = binary.BigEndian.AppendUint16(append(b, b0, b1|126), uint16(h.length))
	default:
		b = binary.BigEndian.AppendUint64(append(b, b0, b1|127), uint64(h.length))
	}
	if h.masked {
		b = append(b, h.mask[:]...)
	}
	return b
}

// appendMaskedFrame appends one masked client frame to b.
func appendMaskedFrame(b []byte, op OpCode, final bool, payload []byte) []byte {
	b = appendHeader(b, frameHeader{
		final:  final,
		opCode: op,
		masked: true,
		mask:   testMask,
		length: int64(len(payload)),
	})
	start := len(b)
	b = append(b, payload...)
	applyMask(b[start:], testMask)
	return b
}

// writeClientMessage writes one masked, unfragmented client frame to w.
func writeClientMessage(w io.Writer, op OpCode, payload []byte) error {
	_, err := w.Write(appendMaskedFrame(nil, op, true, payload))
	return err
}

// encodeCloseBody encodes a close frame payload with code and reason.
func encodeCloseBody(code StatusCode, reason string) []byte {
	b := make([]byte, 2+len(reason))
	binary.BigEndian.PutUint16(b, uint16(code))
	copy(b[2:], reason)
	return b
}

// readServerFrame reads one frame sent by the server from r.
func readServerFrame(r io.Reader) (h frameHeader, payload []byte, err error) {
	var buf [14]byte
	if _, err := io.ReadFull(r, buf[:2]); err != nil {
		return h, nil, err
	}
	need := 2
	switch buf[1] & 0x7f {
	case 126:
		need += 2
	case 127:
		need += 8
	}
	if buf[1]&0x80 != 0 {
		need += 4
	}
	if _, err := io.ReadFull(r, buf[2:need]); err != nil {
		return h, nil, err
	}
	h, _, err = parseHeader(buf[:need])
	if err != nil {
		return h, nil, err
	}
	payload = make([]byte, h.length)
	if _, err := io.ReadFull(r, payload); err != nil {
		return h, nil, err
	}
	if h.masked {
		applyMask(payload, h.mask)
	}
	return h, payload, nil
}

// dialConn dials addr (over TLS when tlsConfig is non-nil) and completes the WebSocket opening handshake.
func dialConn(addr string, tlsConfig *tls.Config) (net.Conn, *bufio.Reader, error) {
	var conn net.Conn
	var err error
	if tlsConfig != nil {
		conn, err = tls.DialWithDialer(&net.Dialer{Timeout: 5 * time.Second}, "tcp", addr, tlsConfig)
	} else {
		conn, err = net.DialTimeout("tcp", addr, 5*time.Second)
	}
	if err != nil {
		return nil, nil, err
	}
	const key = "dGhlIHNhbXBsZSBub25jZQ=="
	req := "GET /ws HTTP/1.1\r\nHost: " + addr + "\r\nUpgrade: websocket\r\nConnection: Upgrade\r\n" +
		"Sec-WebSocket-Key: " + key + "\r\nSec-WebSocket-Version: 13\r\n\r\n"
	if _, err := io.WriteString(conn, req); err != nil {
		conn.Close()
		return nil, nil, err
	}
	br := bufio.NewReader(conn)
	resp, err := http.ReadResponse(br, nil)
	if err != nil {
		conn.Close()
		return nil, nil, err
	}
	if resp.StatusCode != http.StatusSwitchingProtocols || resp.Header.Get("Sec-WebSocket-Accept") != acceptKey(key) {
		conn.Close()
		return nil, nil, fmt.Errorf("unexpected handshake response: %s", resp.Status)
	}
	return conn, br, nil
}

// testHandler echoes the messages it receives; "close" triggers a close from the local side and "panic"
// triggers a panic.
type testHandler struct {
	openDelay time.Duration
	openPanic bool          // whether OnOpen panics
	blocked   chan struct{} // when non-nil, the callback for every message blocks until it is closed
	received  chan string   // when non-nil, records the messages received
	events    chan string   // when non-nil, records "open" and then "close"
	closed    chan error    // the OnClose reason of each connection
}

func (h *testHandler) OnOpen(c *Conn) {
	time.Sleep(h.openDelay)
	if h.openPanic {
		panic("open")
	}
	c.SetContext("opened")
	if h.events != nil {
		h.events <- "open"
	}
}

func (h *testHandler) OnMessage(c *Conn, op OpCode, data []byte) {
	if c.Context() != "opened" {
		panic("OnMessage before OnOpen")
	}
	if h.blocked != nil {
		<-h.blocked
	}
	if h.received != nil {
		h.received <- string(data)
	}
	switch string(data) {
	case "close":
		c.Close()
	case "panic":
		panic("boom")
	default:
		c.WriteMessage(op, data)
	}
}

func (h *testHandler) OnClose(c *Conn, err error) {
	if h.events != nil {
		h.events <- "close"
	}
	h.closed <- err
}

func (h *testHandler) waitClose(t *testing.T) error {
	t.Helper()
	select {
	case err := <-h.closed:
		return err
	case <-time.After(5 * time.Second):
		t.Fatal("OnClose was not called")
		return nil
	}
}

func newTestServer(t *testing.T, h *testHandler, opts Options) *fhttp.Server {
	t.Helper()
	h.closed = make(chan error, 128)
	mux := http.NewServeMux()
	mux.HandleFunc("/ws", func(w http.ResponseWriter, r *http.Request) {
		Upgrade(w, r, h, opts)
	})
	s, err := fhttp.NewServer("127.0.0.1:0", mux, fhttp.Options{})
	if err != nil {
		t.Fatal(err)
	}
	served := make(chan error, 1)
	go func() { served <- s.Serve() }()
	t.Cleanup(func() {
		s.Close()
		if err := <-served; !errors.Is(err, http.ErrServerClosed) {
			t.Errorf("Serve returned %v", err)
		}
	})
	return s
}

// client is a WebSocket client connection for tests; it first reads the buffered data that was over-read
// during the handshake.
type client struct {
	net.Conn
	reader io.Reader
}

func (c *client) Read(p []byte) (int, error) { return c.reader.Read(p) }

func dial(t *testing.T, s *fhttp.Server) *client {
	t.Helper()
	conn, br, err := dialConn(s.Addr().String(), nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { conn.Close() })
	conn.SetDeadline(time.Now().Add(5 * time.Second))
	return &client{Conn: conn, reader: br}
}

func (c *client) send(t *testing.T, op OpCode, final bool, payload string) {
	t.Helper()
	if _, err := c.Write(appendMaskedFrame(nil, op, final, []byte(payload))); err != nil {
		t.Fatal(err)
	}
}

func (c *client) expectMessage(t *testing.T, wantOp OpCode, want []byte) {
	t.Helper()
	h, data, err := readServerFrame(c)
	for err == nil && h.opCode.isControl() {
		h, data, err = readServerFrame(c)
	}
	if err != nil || !h.final || h.opCode != wantOp || !bytes.Equal(data, want) {
		t.Fatalf("got op=%v final=%v len=%d err=%v, want op=%v len=%d", h.opCode, h.final, len(data), err, wantOp, len(want))
	}
}

// expectClose skips data frames until it reads the server's close frame and checks its status code; after
// that the connection should be closed.
func (c *client) expectClose(t *testing.T, want StatusCode) {
	t.Helper()
	h, payload, err := readServerFrame(c)
	for err == nil && !h.opCode.isControl() {
		h, payload, err = readServerFrame(c)
	}
	if err != nil || h.opCode != OpClose {
		t.Fatalf("want a close frame, got %+v %v", h, err)
	}
	var code StatusCode
	if len(payload) >= 2 {
		code = StatusCode(binary.BigEndian.Uint16(payload[:2]))
	}
	if code != want {
		t.Fatalf("close frame status code %d, want %d", code, want)
	}
	// If the peer is still sending when the server closes (for example data sent after a protocol
	// error), the kernel ends the connection with RST, so the read returns ECONNRESET instead of EOF.
	if _, err := c.Read(make([]byte, 1)); err != io.EOF && !isConnReset(err) {
		t.Fatalf("want the connection to be closed, got %v", err)
	}
}

// isConnReset reports whether err is the connection having been reset by the peer. On Windows that is WSAECONNRESET,
// which does not match syscall.ECONNRESET (a placeholder value there).
func isConnReset(err error) bool {
	const wsaeconnreset = syscall.Errno(10054)
	return errors.Is(err, syscall.ECONNRESET) || runtime.GOOS == "windows" && errors.Is(err, wsaeconnreset)
}

func TestEcho(t *testing.T) {
	s := newTestServer(t, &testHandler{}, Options{})
	c := dial(t, s)

	writeClientMessage(c, OpText, []byte("hello"))
	c.expectMessage(t, OpText, []byte("hello"))
	writeClientMessage(c, OpBinary, nil)
	c.expectMessage(t, OpBinary, nil)
	big := bytes.Repeat([]byte("0123456789abcdef"), 32*(1<<10)) // 512KB, spans multiple reads
	writeClientMessage(c, OpBinary, big)
	c.expectMessage(t, OpBinary, big)

	// A ping inserted in the middle of a fragmented message: the pong arrives first, then the reassembled
	// message.
	c.send(t, OpText, false, "frag")
	c.send(t, OpPing, true, "p")
	c.send(t, OpContinuation, true, "ment")
	h, payload, err := readServerFrame(c)
	if err != nil || h.opCode != OpPong || string(payload) != "p" {
		t.Fatalf("want pong, got %+v %q %v", h, payload, err)
	}
	c.expectMessage(t, OpText, []byte("fragment"))
}

// TestEchoPipelined verifies a real WebSocket connection sending and receiving multi-frame batches back to back,
// covering Echo ordering across read rounds.
func TestEchoPipelined(t *testing.T) {
	for _, size := range []int{7, (1 << 10)} {
		t.Run(fmt.Sprintf("size=%d", size), func(t *testing.T) {
			s := newTestServer(t, &testHandler{}, Options{})
			c := dial(t, s)
			payloads := make([][]byte, 16)
			for i := range payloads {
				payloads[i] = bytes.Repeat([]byte{byte(i)}, size)
			}
			batch := clientFrames(OpBinary, payloads...)
			for range 3 {
				if n, err := c.Write(batch); err != nil || n != len(batch) {
					t.Fatalf("wrote %d/%d bytes, err=%v", n, len(batch), err)
				}
				for _, payload := range payloads {
					c.expectMessage(t, OpBinary, payload)
				}
			}
		})
	}
}

// TestWriteMessageInvalid checks that WriteMessage refuses what is no message (a close frame, a continuation, a reserved
// opcode) and control frames over 125 bytes (RFC 6455 5.5), before touching the connection.
func TestWriteMessageInvalid(t *testing.T) {
	var c Conn
	for _, tt := range []struct {
		opCode OpCode
		size   int
	}{{OpClose, 2}, {OpContinuation, 1}, {OpCode(3), 1}, {OpPing, 126}, {OpPong, 126}} {
		if err := c.WriteMessage(tt.opCode, make([]byte, tt.size)); err != errInvalidMessage {
			t.Errorf("WriteMessage(%v, %d bytes) = %v, want errInvalidMessage", tt.opCode, tt.size, err)
		}
	}
}

// TestApplyMask checks that applyMask produces identical output to the byte-by-byte RFC 6455 mask definition for all
// payload lengths.
func TestApplyMask(t *testing.T) {
	mask := [4]byte{0x12, 0x34, 0x56, 0x78}
	for size := 0; size <= 1024; size++ {
		src := make([]byte, size)
		for i := range src {
			src[i] = byte(i * 17)
		}
		want := slices.Clone(src)
		for i := range want {
			want[i] ^= mask[i&3]
		}
		got := slices.Clone(src)
		applyMask(got, mask)
		if !bytes.Equal(got, want) {
			t.Fatalf("size %d: got %x want %x", size, got, want)
		}
	}
}

// TestAppendFrameHeader checks that the frame header uses the shortest length encoding required by RFC 6455 5.2 and
// round-trips through parseHeader.
func TestAppendFrameHeader(t *testing.T) {
	for _, tc := range []struct {
		length     int
		headerSize int
	}{
		{0, 2},
		{125, 2},
		{126, 4},
		{64*(1<<10) - 1, 4},
		{64 * (1 << 10), 10},
		{(1 << 20), 10},
	} {
		encoded := appendFrameHeader(nil, OpText, tc.length)
		if len(encoded) != tc.headerSize {
			t.Fatalf("length %d: header size %d, want %d", tc.length, len(encoded), tc.headerSize)
		}
		got, n, err := parseHeader(encoded)
		want := frameHeader{final: true, opCode: OpText, length: int64(tc.length)}
		if err != nil || n != tc.headerSize || got != want {
			t.Errorf("length %d: got %+v n=%d err=%v, want %+v n=%d", tc.length, got, n, err, want, tc.headerSize)
		}
	}
}

// TestParseHeader checks parseHeader on every combination of header fields (every length encoding, FIN, RSV, opcode
// and mask), on every truncation of the encoded header, and with payload bytes following the header, which must not
// be consumed.
func TestParseHeader(t *testing.T) {
	lengths := []int64{0, 1, 125, 126, 127, 65535, 65536, 1 << 31, 1 << 40, math.MaxInt64}
	for _, length := range lengths {
		for _, masked := range []bool{false, true} {
			for _, final := range []bool{false, true} {
				for op := range OpCode(16) {
					for reserved := range byte(8) {
						want := frameHeader{
							final:    final,
							reserved: reserved,
							opCode:   op,
							masked:   masked,
							length:   length,
						}
						if masked {
							want.mask = testMask
						}
						header := appendHeader(nil, want)
						for n := range len(header) {
							if _, gotN, err := parseHeader(header[:n]); gotN != 0 || err != nil {
								t.Fatalf("%x[:%d]: incomplete header got n=%d err=%v, want n=0 err=nil", header, n, gotN, err)
							}
						}
						encoded := append(header, "payload"...)
						for n := len(header); n <= len(encoded); n++ {
							got, gotN, err := parseHeader(encoded[:n])
							if err != nil || gotN != len(header) || got != want {
								t.Fatalf("%x: got %+v n=%d err=%v, want %+v n=%d", encoded[:n], got, gotN, err, want, len(header))
							}
						}
					}
				}
			}
		}
	}
}

// TestParseHeaderMSB checks that a 64-bit length with its most significant bit set is rejected, and only once the
// whole header has arrived.
func TestParseHeaderMSB(t *testing.T) {
	data := []byte{0x82, 0xff, 0x80, 0, 0, 0, 0, 0, 0, 1, 1, 2, 3, 4}
	for n := range len(data) {
		if _, gotN, err := parseHeader(data[:n]); gotN != 0 || err != nil {
			t.Fatalf("with an incomplete header (len=%d) n=%d err=%v, want n=0 err=nil", n, gotN, err)
		}
	}
	if _, _, err := parseHeader(data); err != errHeaderLengthMSB {
		t.Fatalf("err = %v, want %v", err, errHeaderLengthMSB)
	}
}

// stubConnection is an fnet.Conn that discards what is written to it, so that the frame processing of a Conn can
// be driven (and measured) without a socket; only the methods used on the OnData path are implemented.
type stubConnection struct{ fnet.Conn }

func (stubConnection) Write(b []byte) (int, error) { return len(b), nil }

func (stubConnection) Writev(bs [][]byte) (n int, err error) {
	for _, b := range bs {
		n += len(b)
	}
	return n, nil
}

func (stubConnection) SetDeadline(time.Time) {}

// echoHandler echoes every message.
type echoHandler struct{}

func (echoHandler) OnOpen(*Conn)                              {}
func (echoHandler) OnMessage(c *Conn, op OpCode, data []byte) { c.WriteMessage(op, data) }
func (echoHandler) OnClose(*Conn, error)                      {}

// maskedFrames returns count masked binary frames of payload bytes each, back to back, as one client write.
func maskedFrames(count, payload int) []byte {
	var buf []byte
	body := make([]byte, payload)
	for range count {
		buf = appendMaskedFrame(buf, OpBinary, true, body)
	}
	return buf
}

// BenchmarkOnData measures the receive path of one connection for a client write of frames pipelined messages of
// 1KB each (frames=1 is the plain echo, frames=10 is the pipelined case): header parsing, unmasking, the echo
// into the cork buffer and the single write that goes out afterwards.
func BenchmarkOnData(b *testing.B) {
	for _, frames := range []int{1, 2, 10, 16} {
		b.Run(fmt.Sprintf("frames=%d", frames), func(b *testing.B) {
			batch := maskedFrames(frames, (1 << 10))
			c := &Conn{connection: stubConnection{}, handler: echoHandler{}, maxMessageSize: (1 << 20)}
			data := make([]byte, len(batch))
			b.SetBytes(int64(frames * (1 << 10)))
			b.ReportAllocs()
			for range b.N {
				copy(data, batch) // OnData unmasks in place
				if n := c.OnData(data); n != len(data) {
					b.Fatalf("OnData consumed %d bytes, want %d", n, len(data))
				}
			}
		})
	}
}

// TestOnDataAllocs verifies that single-frame and multi-frame Echo allocate nothing once the buffer pool is warm,
// covering the escape regression of the cork wrapper.
func TestOnDataAllocs(t *testing.T) {
	for _, frames := range []int{1, 2, 10, 16} {
		for _, size := range []int{7, (1 << 10)} {
			t.Run(fmt.Sprintf("frames=%d/size=%d", frames, size), func(t *testing.T) {
				batch := maskedFrames(frames, size)
				c := &Conn{connection: stubConnection{}, handler: echoHandler{}, maxMessageSize: (1 << 20)}
				data := make([]byte, len(batch))
				if allocs := testing.AllocsPerRun(1000, func() {
					copy(data, batch)
					if n := c.OnData(data); n != len(data) {
						t.Fatalf("consumed %d bytes, want %d", n, len(data))
					}
				}); allocs != 0 {
					t.Fatalf("processing %d frames allocated %v times, want 0", frames, allocs)
				}
				if c.corking || c.corkBuffer.Bytes() != nil {
					t.Fatal("the cork buffer was still retained after handling")
				}
			})
		}
	}
}

func TestOrdering(t *testing.T) {
	// Data arriving before OnOpen returns waits for OnOpen to return and is then delivered in order.
	s := newTestServer(t, &testHandler{openDelay: 50 * time.Millisecond}, Options{})
	c := dial(t, s)
	var buf bytes.Buffer
	for i := range 200 {
		writeClientMessage(&buf, OpText, fmt.Appendf(nil, "%d", i))
	}
	c.Write(buf.Bytes())
	for i := range 200 {
		c.expectMessage(t, OpText, fmt.Appendf(nil, "%d", i))
	}
}

// TestEarlyData covers a peer that violates RFC 6455 by sending frames before it receives the 101: they are
// never delivered before OnOpen, and the data is kept until the next time data arrives and is processed
// together with it.
func TestEarlyData(t *testing.T) {
	s := newTestServer(t, &testHandler{openDelay: 100 * time.Millisecond}, Options{})
	conn, err := net.Dial("tcp", s.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { conn.Close() })
	conn.SetDeadline(time.Now().Add(5 * time.Second))

	var b bytes.Buffer
	b.WriteString("GET /ws HTTP/1.1\r\nHost: a\r\nUpgrade: websocket\r\nConnection: Upgrade\r\n" +
		"Sec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\nSec-WebSocket-Version: 13\r\n\r\n")
	writeClientMessage(&b, OpText, []byte("early"))
	conn.Write(b.Bytes())
	br := bufio.NewReader(conn)
	if resp, err := http.ReadResponse(br, nil); err != nil || resp.StatusCode != http.StatusSwitchingProtocols {
		t.Fatalf("handshake: %v %v", resp, err)
	}
	time.Sleep(300 * time.Millisecond) // OnOpen has returned
	writeClientMessage(conn, OpText, []byte("late"))
	c := &client{Conn: conn, reader: br}
	c.expectMessage(t, OpText, []byte("early"))
	c.expectMessage(t, OpText, []byte("late"))
}

// gateHandler blocks OnOpen of the first connection until gate is closed; the rest echo as usual.
type gateHandler struct {
	testHandler
	gate  chan struct{}
	first atomic.Bool
}

func (h *gateHandler) OnOpen(c *Conn) {
	if h.first.CompareAndSwap(false, true) {
		<-h.gate
	}
	h.testHandler.OnOpen(c)
}

// newGateServer starts a server that handles /ws with h, using engine for the engine.
func newGateServer(t *testing.T, h Handler, engine fnet.Options) *fhttp.Server {
	t.Helper()
	mux := http.NewServeMux()
	mux.HandleFunc("/ws", func(w http.ResponseWriter, r *http.Request) { Upgrade(w, r, h, Options{}) })
	srv, err := fhttp.NewServer("127.0.0.1:0", mux, fhttp.Options{Engine: engine})
	if err != nil {
		t.Fatal(err)
	}
	go srv.Serve()
	t.Cleanup(func() { srv.Close() })
	return srv
}

// TestOpenDoesNotBlockExecutor checks that data sent by the peer while OnOpen runs stays in the kernel and
// does not occupy a goroutine of the executor: the executor has only one worker, yet the other connections
// are still handled as usual.
func TestOpenDoesNotBlockExecutor(t *testing.T) {
	tasks := make(chan func(), 1024)
	go func() {
		for task := range tasks {
			task()
		}
	}()
	t.Cleanup(func() { close(tasks) })

	h := &gateHandler{gate: make(chan struct{})}
	h.closed = make(chan error, 8)
	srv := newGateServer(t, h, fnet.Options{NumLoops: 1, Executor: func(_ int, task func()) { tasks <- task }})
	var release sync.Once
	t.Cleanup(func() { release.Do(func() { close(h.gate) }) }) // runs before srv.Close; failures never hang in OnOpen

	blocked := dial(t, srv) // OnOpen blocks on gate
	writeClientMessage(blocked, OpText, []byte("a"))
	time.Sleep(50 * time.Millisecond) // the data has arrived, but it must not occupy the only worker
	other := dial(t, srv)
	writeClientMessage(other, OpText, []byte("b"))
	other.expectMessage(t, OpText, []byte("b"))

	release.Do(func() { close(h.gate) })
	blocked.expectMessage(t, OpText, []byte("a"))
}

// TestServerCloseDuringOpen covers the server closing before OnOpen returns: the engine closes the
// connection first, Handler.OnClose is not called until OnOpen has returned, and it is still the last
// callback.
func TestServerCloseDuringOpen(t *testing.T) {
	h := &gateHandler{gate: make(chan struct{})}
	h.closed = make(chan error, 1)
	h.events = make(chan string, 2)
	srv := newGateServer(t, h, fnet.Options{NumLoops: 1})
	var release sync.Once
	t.Cleanup(func() { release.Do(func() { close(h.gate) }) })

	dial(t, srv) // OnOpen blocks on gate
	srv.Close()  // the engine closes the connection while OnOpen has not returned yet
	select {
	case err := <-h.closed:
		t.Fatalf("OnClose returned before OnOpen: %v", err)
	case <-time.After(100 * time.Millisecond):
	}
	release.Do(func() { close(h.gate) })
	if err := h.waitClose(t); err != fnet.ErrServerClosed {
		t.Fatalf("OnClose(%v)", err)
	}
	if got := []string{<-h.events, <-h.events}; !slices.Equal(got, []string{"open", "close"}) {
		t.Fatalf("callback order %v", got)
	}
}

// TestCloseDuringOpen covers the connection dropping before OnOpen returns: OnClose is not called until
// OnOpen has returned.
func TestCloseDuringOpen(t *testing.T) {
	h := &testHandler{openDelay: 100 * time.Millisecond, events: make(chan string, 2)}
	s := newTestServer(t, h, Options{})
	c := dial(t, s)
	c.Close()
	h.waitClose(t)
	if got := []string{<-h.events, <-h.events}; !slices.Equal(got, []string{"open", "close"}) {
		t.Fatalf("callback order %v", got)
	}
}

func TestClose(t *testing.T) {
	h := &testHandler{received: make(chan string, 16)}
	s := newTestServer(t, h, Options{})

	// The peer sends a close frame: the same status code is answered immediately (after which echoes can no
	// longer be written out), but messages received earlier are still delivered before OnClose.
	c := dial(t, s)
	var buf bytes.Buffer
	writeClientMessage(&buf, OpText, []byte("last"))
	writeClientMessage(&buf, OpClose, encodeCloseBody(StatusGoingAway, "bye"))
	c.Write(buf.Bytes())
	c.expectClose(t, StatusGoingAway)
	if err := h.waitClose(t); err != (ClosedError{Code: StatusGoingAway, Reason: "bye"}) {
		t.Fatalf("peer close: OnClose(%v)", err)
	}
	if len(h.received) != 1 || <-h.received != "last" {
		t.Fatal("a message before the close frame was not delivered before OnClose")
	}

	// A code IANA registered after RFC 6455, such as 1013 (Try Again Later), is answered like the others.
	c = dial(t, s)
	writeClientMessage(c, OpClose, encodeCloseBody(StatusTryAgainLater, "later"))
	c.expectClose(t, StatusTryAgainLater)
	if err := h.waitClose(t); err != (ClosedError{Code: StatusTryAgainLater, Reason: "later"}) {
		t.Fatalf("peer close with 1013: OnClose(%v)", err)
	}

	// Close from the local side.
	c = dial(t, s)
	writeClientMessage(c, OpText, []byte("close"))
	c.expectClose(t, StatusNormalClosure)
	if err := h.waitClose(t); err != nil {
		t.Fatalf("local close: OnClose(%v)", err)
	}
	<-h.received

	// The peer disconnects outright.
	c = dial(t, s)
	c.Close()
	if err := h.waitClose(t); err != io.EOF {
		t.Fatalf("peer disconnect: OnClose(%v)", err)
	}

	// The server closes.
	dial(t, s)
	s.Close()
	if err := h.waitClose(t); err != fnet.ErrServerClosed {
		t.Fatalf("server close: OnClose(%v)", err)
	}
}

func TestProtocolErrors(t *testing.T) {
	h := &testHandler{}
	s := newTestServer(t, h, Options{MaxMessageSize: (1 << 10)})
	long := bytes.Repeat([]byte("a"), 600)
	tests := []struct {
		name string
		raw  []byte
		code StatusCode
		err  error
	}{
		{
			name: "unmasked",
			raw:  append(appendFrameHeader(nil, OpText, 1), 'x'),
			code: StatusProtocolError,
			err:  errMaskRequired,
		},
		{
			name: "RSV non-zero",
			raw:  appendHeader(nil, frameHeader{final: true, reserved: 0x4, opCode: OpText, masked: true, mask: testMask}),
			code: StatusProtocolError,
			err:  errReservedBits,
		},
		{
			name: "reserved opcode",
			raw:  appendMaskedFrame(nil, OpCode(3), true, nil),
			code: StatusProtocolError,
			err:  errReservedOpCode,
		},
		{
			name: "fragmented control frame",
			raw:  appendMaskedFrame(nil, OpPing, false, nil),
			code: StatusProtocolError,
			err:  errControlFragmented,
		},
		{
			name: "control frame payload over 125 bytes",
			raw:  appendMaskedFrame(nil, OpPing, true, make([]byte, 126)),
			code: StatusProtocolError,
			err:  errControlPayloadTooBig,
		},
		{
			name: "unexpected continuation frame",
			raw:  appendMaskedFrame(nil, OpContinuation, true, nil),
			code: StatusProtocolError,
			err:  errUnexpectedContinuation,
		},
		{
			name: "new data frame during fragmented message",
			raw:  appendMaskedFrame(appendMaskedFrame(nil, OpText, false, []byte("a")), OpText, true, []byte("b")),
			code: StatusProtocolError,
			err:  errUnexpectedDataFrame,
		},
		{
			name: "close frame with a single byte",
			raw:  appendMaskedFrame(nil, OpClose, true, []byte{3}),
			code: StatusProtocolError,
			err:  errInvalidCloseCode,
		},
		{
			name: "close frame with reserved status code 1005",
			raw:  appendMaskedFrame(nil, OpClose, true, encodeCloseBody(StatusNoStatusRcvd, "")),
			code: StatusProtocolError,
			err:  errInvalidCloseCode,
		},
		{
			name: "close frame with invalid UTF-8 reason",
			raw:  appendMaskedFrame(nil, OpClose, true, encodeCloseBody(StatusNormalClosure, "\xff")),
			code: StatusProtocolError,
			err:  errInvalidUTF8,
		},
		{
			name: "invalid UTF-8",
			raw:  appendMaskedFrame(nil, OpText, true, []byte{0xff}),
			code: StatusInvalidFramePayloadData,
			err:  errInvalidUTF8,
		},
		{
			name: "message too big",
			raw:  appendMaskedFrame(nil, OpBinary, true, make([]byte, 2000)),
			code: StatusMessageTooBig,
			err:  errMessageTooBig,
		},
		{
			name: "fragments together too big",
			raw:  appendMaskedFrame(appendMaskedFrame(nil, OpText, false, long), OpContinuation, true, long),
			code: StatusMessageTooBig,
			err:  errMessageTooBig,
		},
	}
	for _, tt := range tests {
		c := dial(t, s)
		c.Write(tt.raw) // the server may close before reading it all; write errors surface in the checks below
		c.expectClose(t, tt.code)
		if err := h.waitClose(t); err != tt.err {
			t.Fatalf("%s: OnClose(%v), want %v", tt.name, err, tt.err)
		}
	}
}

func TestHandshakeErrors(t *testing.T) {
	s := newTestServer(t, &testHandler{}, Options{})
	const valid = "Upgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\n"
	tests := []struct {
		req    string
		status int
	}{
		{"POST /ws HTTP/1.1\r\nHost: a\r\n" + valid + "Sec-WebSocket-Version: 13\r\n\r\n", http.StatusMethodNotAllowed},
		{"GET /ws HTTP/1.1\r\nHost: a\r\nSec-WebSocket-Version: 13\r\n\r\n", http.StatusBadRequest},
		{"GET /ws HTTP/1.1\r\nHost: a\r\n" + valid + "\r\n", http.StatusBadRequest},
		{"GET /ws HTTP/1.1\r\nHost: a\r\n" + valid + "Sec-WebSocket-Version: 8\r\n\r\n", http.StatusUpgradeRequired},
		{"GET /ws HTTP/1.1\r\nHost: a\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Key: short\r\n" +
			"Sec-WebSocket-Version: 13\r\n\r\n", http.StatusBadRequest},
		// A key of 24 characters that is not the base64 encoding of 16 bytes (RFC 6455 4.1): no base64, or 18 bytes.
		{"GET /ws HTTP/1.1\r\nHost: a\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Key: !!!!!!!!!!!!!!!!!!!!!!!!\r\n" +
			"Sec-WebSocket-Version: 13\r\n\r\n", http.StatusBadRequest},
		{"GET /ws HTTP/1.1\r\nHost: a\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Key: aaaaaaaaaaaaaaaaaaaaaaaa\r\n" +
			"Sec-WebSocket-Version: 13\r\n\r\n", http.StatusBadRequest},
		// The key and the version may not be repeated (RFC 6455 11.3.1, 11.3.5), whatever the values.
		{"GET /ws HTTP/1.1\r\nHost: a\r\n" + valid + "Sec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\nSec-WebSocket-Version: 13\r\n\r\n",
			http.StatusBadRequest},
		{"GET /ws HTTP/1.1\r\nHost: a\r\n" + valid + "Sec-WebSocket-Version: 13\r\nSec-WebSocket-Version: 13\r\n\r\n",
			http.StatusBadRequest},
	}
	for _, tt := range tests {
		conn, err := net.Dial("tcp", s.Addr().String())
		if err != nil {
			t.Fatal(err)
		}
		conn.SetDeadline(time.Now().Add(5 * time.Second))
		io.WriteString(conn, tt.req)
		resp, err := http.ReadResponse(bufio.NewReader(conn), nil)
		if err != nil || resp.StatusCode != tt.status {
			t.Fatalf("%q: got %v %v, want %d", tt.req, resp, err, tt.status)
		}
		if tt.status == http.StatusUpgradeRequired && resp.Header.Get("Sec-WebSocket-Version") != "13" {
			t.Fatalf("426 should carry Sec-WebSocket-Version: %v", resp.Header)
		}
		conn.Close()
	}
}

// TestHandshakeUpgradeList checks that the Upgrade field may list protocols as long as websocket is one of them
// (RFC 6455 4.2.1).
func TestHandshakeUpgradeList(t *testing.T) {
	s := newTestServer(t, &testHandler{}, Options{})
	conn, err := net.Dial("tcp", s.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	conn.SetDeadline(time.Now().Add(5 * time.Second))
	io.WriteString(conn, "GET /ws HTTP/1.1\r\nHost: a\r\nUpgrade: h2c, WebSocket\r\nConnection: Upgrade\r\n"+
		"Sec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\nSec-WebSocket-Version: 13\r\n\r\n")
	resp, err := http.ReadResponse(bufio.NewReader(conn), nil)
	if err != nil || resp.StatusCode != http.StatusSwitchingProtocols ||
		resp.Header.Get("Sec-WebSocket-Accept") != "s3pPLMBiTxaQ9kYGzzhZRbK+xOo=" {
		t.Fatalf("got %v %v, want 101 with the accept value of RFC 6455 1.3", resp, err)
	}
}

// TestLengthNotMinimal checks that a frame whose payload length is sent in more bytes than it needs fails the
// connection with a protocol error (RFC 6455 5.2), in the 16-bit and the 64-bit form.
func TestLengthNotMinimal(t *testing.T) {
	h := &testHandler{}
	s := newTestServer(t, h, Options{})
	for name, header := range map[string][]byte{
		"1 in 16 bits":     {0x81, 0x80 | 126, 0, 1},
		"1 in 64 bits":     {0x81, 0x80 | 127, 0, 0, 0, 0, 0, 0, 0, 1},
		"65535 in 64 bits": {0x81, 0x80 | 127, 0, 0, 0, 0, 0, 0, 0xff, 0xff},
	} {
		t.Run(name, func(t *testing.T) {
			c := dial(t, s)
			c.Write(slices.Concat(header, []byte{1, 2, 3, 4, 'x' ^ 1})) // the header is rejected as soon as it is there
			c.expectClose(t, StatusProtocolError)
			if err := h.waitClose(t); err != errLengthNotMinimal {
				t.Fatalf("OnClose(%v), want %v", err, errLengthNotMinimal)
			}
		})
	}

	// The same lengths in their shortest form are served.
	for _, size := range []int{125, 126, math.MaxUint16, math.MaxUint16 + 1} {
		c := dial(t, s)
		c.send(t, OpBinary, true, string(make([]byte, size)))
		c.expectMessage(t, OpBinary, make([]byte, size))
	}
}

// logSignal signals when a log record arrives and does not output the log.
type logSignal chan struct{}

func (s logSignal) Write(p []byte) (int, error) {
	select {
	case s <- struct{}{}:
	default:
	}
	return len(p), nil
}

// expectPanicLog silences logs and waits for the panic to be logged before the test ends: the panic is only
// recovered and logged by the executor (or by fhttp) after the connection is closed.
// At the end it restores the output and flags of slog and of the log package (see fhttp's TestClient for why).
func expectPanicLog(t *testing.T) {
	logged := make(logSignal, 1)
	w, flags, logger := log.Writer(), log.Flags(), slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(logged, nil)))
	t.Cleanup(func() {
		select {
		case <-logged:
		case <-time.After(5 * time.Second):
			t.Error("panic was not logged")
		}
		slog.SetDefault(logger)
		log.SetOutput(w)
		log.SetFlags(flags)
	})
}

func TestPanic(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("the standard library implementation does not recover a panic in a callback (see fnet.ErrHandlerPanic)")
	}
	expectPanicLog(t)
	// Two messages arrive while OnOpen runs and are received in the same read: after the panic the
	// connection is closed with 1011, the remaining messages of that batch are not delivered, and
	// OnClose is called last.
	h := &testHandler{openDelay: 100 * time.Millisecond, received: make(chan string, 2)}
	s := newTestServer(t, h, Options{})
	c := dial(t, s)
	var b bytes.Buffer
	writeClientMessage(&b, OpText, []byte("panic"))
	writeClientMessage(&b, OpText, []byte("after"))
	c.Write(b.Bytes())
	c.expectClose(t, StatusInternalServerError)
	if err := h.waitClose(t); err != errHandlerPanic {
		t.Fatalf("OnClose(%v)", err)
	}
	if len(h.received) != 1 || <-h.received != "panic" {
		t.Fatal("no message should be delivered after a panic")
	}
}

// TestOpenPanic covers a panic in OnOpen: the connection is closed with 1011 and OnClose is called as usual.
func TestOpenPanic(t *testing.T) {
	expectPanicLog(t)
	h := &testHandler{openPanic: true}
	s := newTestServer(t, h, Options{})
	c := dial(t, s)
	c.expectClose(t, StatusInternalServerError)
	if err := h.waitClose(t); err != errHandlerPanic {
		t.Fatalf("OnClose(%v)", err)
	}
}

// TestSlowHandler checks that reading stops while the application callback is slow: the peer's writes are
// blocked by TCP flow control, the connection is not closed because of that, and nothing is timed while the
// callback runs; once the application recovers, all messages are delivered in order.
func TestSlowHandler(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("a Windows TCP send does not block while the peer does not read, so the peer cannot be blocked")
	}
	h := &testHandler{blocked: make(chan struct{})}
	s := newTestServer(t, h, Options{MessageTimeout: 300 * time.Millisecond})
	var unblock sync.Once
	t.Cleanup(func() { unblock.Do(func() { close(h.blocked) }) }) // before the server closes: a failure never hangs
	c := dial(t, s)

	head, rest := backlog()
	c.Write(head)
	time.Sleep(1500 * time.Millisecond) // more than MessageTimeout plus one deadline check interval

	// Later data stays in the kernel buffer; once that is full the peer is blocked.
	stream := bytes.NewBuffer(rest)
	payload := bytes.Repeat([]byte("x"), 64*(1<<10))
	for range 512 { // 32MB, far beyond the kernel buffer
		writeClientMessage(stream, OpBinary, payload)
	}
	c.SetWriteDeadline(time.Now().Add(500 * time.Millisecond))
	n, err := c.Write(stream.Bytes())
	if !errors.Is(err, os.ErrDeadlineExceeded) {
		t.Fatalf("peer write should block, wrote %d bytes, err=%v", n, err)
	}
	select {
	case err := <-h.closed:
		t.Fatalf("connection was closed during the callback: %v", err)
	default:
	}

	unblock.Do(func() { close(h.blocked) })
	c.SetDeadline(time.Now().Add(10 * time.Second))
	written := make(chan error, 1)
	go func() {
		_, err := c.Write(stream.Bytes()[n:])
		written <- err
	}()
	for range backlogMessages {
		c.expectMessage(t, OpBinary, nil)
	}
	c.expectMessage(t, OpText, []byte("tail"))
	for range 512 {
		c.expectMessage(t, OpBinary, payload)
	}
	if err := <-written; err != nil {
		t.Fatal(err)
	}
}

// backlogMessages is the number of empty messages received in a single read.
const backlogMessages = 1000

// backlog returns backlogMessages empty messages, ending with an incomplete text frame "tail" that rest
// completes. When head is written in one go, the server receives them in a single read, and once the
// callbacks are done the connection still holds an incompletely received message.
func backlog() (head, rest []byte) {
	var b []byte
	for range backlogMessages {
		b = appendMaskedFrame(b, OpBinary, true, nil)
	}
	tail := appendMaskedFrame(nil, OpText, true, []byte("tail"))
	b = append(b, tail[:len(tail)-2]...)
	return b, tail[len(tail)-2:]
}

type nopHandler struct{}

func (nopHandler) OnOpen(*Conn)                    {}
func (nopHandler) OnMessage(*Conn, OpCode, []byte) {}
func (nopHandler) OnClose(*Conn, error)            {}

// recordConn is a fake connection that records writes and closes; each Write or Writev is one syscall.
type recordConn struct {
	fnet.Conn
	events []writeEvent
	closed bool
}

type writeEvent struct {
	data     []byte
	vectored bool // written out by Writev (the payload is not copied)
	close    bool // closes the connection
}

func (r *recordConn) Write(b []byte) (int, error) { return r.record(slices.Clone(b), false) }

func (r *recordConn) Writev(bs [][]byte) (int, error) { return r.record(slices.Concat(bs...), true) }

func (r *recordConn) record(b []byte, vectored bool) (int, error) {
	if r.closed {
		return 0, net.ErrClosed
	}
	r.events = append(r.events, writeEvent{data: b, vectored: vectored})
	return len(b), nil
}

func (r *recordConn) Close() error {
	r.closed = true
	r.events = append(r.events, writeEvent{close: true})
	return nil
}

func (r *recordConn) SetDeadline(time.Time) {}

// raceDeadlineConn is a recordConn that keeps the deadline set last, and closes conn from within its first SetDeadline
// before that deadline lands: the order of a Close on another goroutine overtaking the deadline OnData sets.
type raceDeadlineConn struct {
	recordConn
	conn     *Conn
	deadline time.Time
	raced    bool
}

func (r *raceDeadlineConn) SetDeadline(t time.Time) {
	if !r.raced {
		r.raced = true
		r.conn.Close()
	}
	r.deadline = t
}

// TestCloseTimeoutStands has a Close set its closeTimeout just before the deadline OnData sets for the next message
// lands: closeTimeout must stand, or a peer that reads nothing keeps the closing connection and its send buffer for good.
func TestCloseTimeoutStands(t *testing.T) {
	rc := &raceDeadlineConn{}
	c := &Conn{connection: rc, handler: nopHandler{}, maxMessageSize: (1 << 20)}
	rc.conn = c
	c.OnData(clientFrames(OpText, []byte("hi")))
	if until := time.Until(rc.deadline); until < closeTimeout/2 {
		t.Fatalf("deadline %v after a concurrent Close, want closeTimeout from now", rc.deadline)
	}
}

// newCorkConn returns a connection whose writes are recorded; OnData's callbacks run directly in the test.
func newCorkConn() (*Conn, *recordConn) {
	rc := &recordConn{}
	c := &Conn{connection: rc, handler: &testHandler{closed: make(chan error, 1)}, maxMessageSize: (1 << 20)}
	c.SetContext("opened")
	return c, rc
}

func frame(op OpCode, payload []byte) []byte {
	return append(appendFrameHeader(nil, op, len(payload)), payload...)
}

// clientFrames lays out the client frames (masked) for payloads, in order.
func clientFrames(op OpCode, payloads ...[]byte) []byte {
	var b []byte
	for _, p := range payloads {
		b = appendMaskedFrame(b, op, true, p)
	}
	return b
}

// TestCorkWrites covers receiving several frames in one OnData: the replies written while handling them are
// merged into a single write. With only one frame the reply is written out directly as usual (see TestWriteFrame).
func TestCorkWrites(t *testing.T) {
	c, rc := newCorkConn()
	var payloads [][]byte
	var want []byte
	for i := range 10 {
		data := fmt.Appendf(nil, "m%d", i)
		payloads = append(payloads, data)
		want = append(want, frame(OpText, data)...)
	}
	in := clientFrames(OpText, payloads...)
	if n := c.OnData(in); n != len(in) {
		t.Fatalf("consumed %d bytes, want %d", n, len(in))
	}
	if len(rc.events) != 1 || !bytes.Equal(rc.events[0].data, want) {
		t.Fatalf("10 frames were written %d times, want them merged into 1", len(rc.events))
	}

	c.OnData(clientFrames(OpText, []byte("one")))
	if len(rc.events) != 2 || rc.events[1].vectored || !bytes.Equal(rc.events[1].data, frame(OpText, []byte("one"))) {
		t.Fatalf("single frame: %+v", rc.events[1:])
	}
	if c.corkBuffer.Bytes() != nil {
		t.Fatal("still corking after handling")
	}
}

// TestCorkConcurrentWrites verifies that concurrent writes of one connection share the cork buffer and return its
// underlying array afterwards.
func TestCorkConcurrentWrites(t *testing.T) {
	c, rc := newCorkConn()
	c.cork(512)
	payload := []byte("concurrent")
	var wg sync.WaitGroup
	for range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range 16 {
				if err := c.WriteMessage(OpText, payload); err != nil {
					t.Error(err)
				}
			}
		}()
	}
	wg.Wait()
	c.uncork()
	want := bytes.Repeat(frame(OpText, payload), 8*16)
	if len(rc.events) != 1 || !bytes.Equal(rc.events[0].data, want) {
		t.Fatalf("concurrent corking result mismatch, wrote %d times", len(rc.events))
	}
	if c.corking || c.corkBuffer.Bytes() != nil {
		t.Fatal("the buffer was still retained after concurrent corking finished")
	}
}

// TestWriteFrame checks how a single frame goes out: a small one is copied next to its header and written in one
// piece, a larger one is written with writev without copying the payload; the bytes are the same either way.
func TestWriteFrame(t *testing.T) {
	for _, tc := range []struct {
		size     int
		vectored bool
	}{{0, false}, {125, false}, {maxCopiedPayload, false}, {maxCopiedPayload + 1, true}, {(1 << 20), true}} {
		rc := &recordConn{}
		c := &Conn{connection: rc}
		payload := bytes.Repeat([]byte("x"), tc.size)
		if err := c.WriteMessage(OpBinary, payload); err != nil {
			t.Fatal(err)
		}
		if len(rc.events) != 1 || !bytes.Equal(rc.events[0].data, frame(OpBinary, payload)) {
			t.Fatalf("%dB: wrote %d times, data mismatch", tc.size, len(rc.events))
		}
		if rc.events[0].vectored != tc.vectored {
			t.Fatalf("%dB: vectored=%v, want %v", tc.size, rc.events[0].vectored, tc.vectored)
		}
	}
}

// TestCorkLimit covers a frame that does not fit in the cork buffer: it is written out together with the
// already corked frames in a single writev, without copying that frame. The order matches the order in
// which the callbacks wrote them.
func TestCorkLimit(t *testing.T) {
	c, rc := newCorkConn()
	var payloads [][]byte
	for range 100 {
		payloads = append(payloads, bytes.Repeat([]byte("a"), 1000))
	}
	payloads = append(payloads, bytes.Repeat([]byte("b"), maxCorkBytes), []byte("tail"))
	var want []byte
	for _, p := range payloads {
		want = append(want, frame(OpBinary, p)...)
	}
	c.OnData(clientFrames(OpBinary, payloads...))

	var got []byte
	for _, e := range rc.events {
		if !e.vectored && len(e.data) > maxCorkBytes {
			t.Fatalf("a single write of %d bytes exceeds the cork limit", len(e.data))
		}
		got = append(got, e.data...)
	}
	if !bytes.Equal(got, want) {
		t.Fatal("the written data does not match the order in which the callbacks wrote it")
	}
	// After 65 frames of 1000B are corked the 66th does not fit and is written out together with them; the
	// remaining 34 are corked and written out with the big frame that does not fit; tail comes last.
	if len(rc.events) != 3 {
		t.Fatalf("wrote %d times, want 3", len(rc.events))
	}
	if c.corking || c.corkBuffer.Bytes() != nil {
		t.Fatal("the cork buffer was still retained after handling a large batch")
	}
}

// TestCorkPerOnData checks that the corked frames are written out at the end of every OnData, so a reply is
// never held over to the next OnData.
func TestCorkPerOnData(t *testing.T) {
	c, rc := newCorkConn()
	c.OnData(clientFrames(OpText, []byte("a"), []byte("b")))
	c.OnData(clientFrames(OpText, []byte("c")))
	want := slices.Concat(frame(OpText, []byte("a")), frame(OpText, []byte("b")))
	if len(rc.events) != 2 || !bytes.Equal(rc.events[0].data, want) ||
		rc.events[1].vectored || !bytes.Equal(rc.events[1].data, frame(OpText, []byte("c"))) {
		t.Fatalf("writes: %+v", rc.events)
	}
}

// TestCorkEveryOnData checks that every OnData that receives several frames corks anew: the corking state of one
// OnData must not leak into the next, or the replies of the later batches would go out one write each.
func TestCorkEveryOnData(t *testing.T) {
	c, rc := newCorkConn()
	want := slices.Concat(frame(OpText, []byte("a")), frame(OpText, []byte("b")), frame(OpText, []byte("c")))
	for round := range 3 {
		c.OnData(clientFrames(OpText, []byte("a"), []byte("b"), []byte("c")))
		if len(rc.events) != round+1 || !bytes.Equal(rc.events[round].data, want) {
			t.Fatalf("batch %d: writes %+v, want each batch merged into 1", round, rc.events)
		}
		if c.corking || c.corkBuffer.Bytes() != nil {
			t.Fatal("still corking after handling")
		}
	}
}

// TestCorkClose covers closing in the middle of handling: the already corked replies are written out first,
// then the close frame, and only then is the underlying connection closed; later writes return net.ErrClosed.
func TestCorkClose(t *testing.T) {
	c, rc := newCorkConn()
	c.OnData(clientFrames(OpText, []byte("a"), []byte("b"), []byte("close"), []byte("c")))
	want := slices.Concat(frame(OpText, []byte("a")), frame(OpText, []byte("b")))
	closeFrame := frame(OpClose, encodeCloseBody(StatusNormalClosure, ""))
	if len(rc.events) != 3 || !bytes.Equal(rc.events[0].data, want) || !bytes.Equal(rc.events[1].data, closeFrame) ||
		!rc.events[2].close {
		t.Fatalf("close during handling: %+v", rc.events)
	}
	if err := c.WriteMessage(OpText, []byte("after")); err != net.ErrClosed {
		t.Fatalf("write after close: %v", err)
	}
	if c.corkBuffer.Bytes() != nil {
		t.Fatal("still corking after handling")
	}
}

// TestNoMessageAfterClose checks that OnMessage is no longer called once a callback has called Close: the
// remaining messages of that batch are dropped, and so is data arriving later, and all of it counts as
// consumed so nothing is left in the engine.
func TestNoMessageAfterClose(t *testing.T) {
	c, _ := newCorkConn()
	h := &testHandler{received: make(chan string, 16)}
	c.handler = h
	in := clientFrames(OpText, []byte("a"), []byte("close"), []byte("b"), []byte("c"))
	if n := c.OnData(in); n != len(in) {
		t.Fatalf("consumed %d bytes, want %d", n, len(in))
	}
	if got := len(h.received); got != 2 { // "a" and "close"
		t.Fatalf("messages left in the same batch were still delivered after Close: %d delivered, want 2", got)
	}
	later := clientFrames(OpText, []byte("d"))
	if n := c.OnData(later); n != len(later) || len(h.received) != 2 {
		t.Fatalf("data arriving after Close: consumed %d/%d bytes, %d delivered", n, len(later), len(h.received))
	}
	// A Close on another goroutine may come while a frame is being unmasked and checked, after OnData last looked.
	c, _ = newCorkConn()
	c.handler = h
	c.Close()
	if c.deliver(OpText, []byte("e")); len(h.received) != 2 {
		t.Fatal("the message of a frame received during Close was delivered")
	}
}

// TestCorkPanic covers a panic in a callback: the already corked replies are written out first, then the
// connection is closed with 1011, and the panic keeps propagating up to be handled by the caller.
func TestCorkPanic(t *testing.T) {
	c, rc := newCorkConn()
	func() {
		defer func() {
			if recover() == nil {
				t.Error("panic should keep propagating up")
			}
		}()
		c.OnData(clientFrames(OpText, []byte("a"), []byte("b"), []byte("panic"), []byte("c")))
	}()
	want := slices.Concat(frame(OpText, []byte("a")), frame(OpText, []byte("b")))
	closeFrame := frame(OpClose, encodeCloseBody(StatusInternalServerError, ""))
	if len(rc.events) != 3 || !bytes.Equal(rc.events[0].data, want) || !bytes.Equal(rc.events[1].data, closeFrame) ||
		!rc.events[2].close || c.corkBuffer.Bytes() != nil {
		t.Fatalf("after panic: %+v", rc.events)
	}
	if c.closeErr != errHandlerPanic {
		t.Fatalf("close reason %v", c.closeErr)
	}
}

func TestConcurrentClients(t *testing.T) {
	s := newTestServer(t, &testHandler{}, Options{})
	var wg sync.WaitGroup
	for range 100 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			conn, br, err := dialConn(s.Addr().String(), nil)
			if err != nil {
				t.Error(err)
				return
			}
			defer conn.Close()
			conn.SetDeadline(time.Now().Add(5 * time.Second))
			for i := range 50 {
				msg := fmt.Appendf(nil, "message %d", i)
				if err := writeClientMessage(conn, OpText, msg); err != nil {
					t.Error(err)
					return
				}
				_, data, err := readServerFrame(br)
				if err != nil || !bytes.Equal(data, msg) {
					t.Errorf("got %q %v, want %q", data, err, msg)
					return
				}
			}
		}()
	}
	wg.Wait()
}

// TestTimeouts covers the timeouts: they are checked with a precision of about 1 second and the scenarios
// run in parallel.
func TestTimeouts(t *testing.T) {
	// ping sends a ping periodically until stop is closed or a write fails; the returned channel is closed
	// once it has stopped.
	ping := func(c *client, stop <-chan struct{}) <-chan struct{} {
		stopped := make(chan struct{})
		go func() {
			defer close(stopped)
			for writeClientMessage(c, OpPing, nil) == nil {
				select {
				case <-stop:
					return
				case <-time.After(100 * time.Millisecond):
				}
			}
		}()
		return stopped
	}
	t.Run("incomplete message", func(t *testing.T) {
		t.Parallel()
		h := &testHandler{}
		s := newTestServer(t, h, Options{MessageTimeout: 200 * time.Millisecond})
		c := dial(t, s)
		c.send(t, OpText, false, "part")
		ping(c, nil) // control frames received in the meantime do not extend the deadline
		if err := h.waitClose(t); err != os.ErrDeadlineExceeded {
			t.Fatalf("OnClose(%v)", err)
		}
	})
	t.Run("idle and a continuous message stream", func(t *testing.T) {
		t.Parallel()
		h := &testHandler{}
		s := newTestServer(t, h, Options{MessageTimeout: 500 * time.Millisecond})
		c := dial(t, s)
		c.SetDeadline(time.Now().Add(10 * time.Second))
		time.Sleep(1200 * time.Millisecond) // idle time is not limited by default
		// Every write stops in the middle of a frame, so the connection always holds an incomplete
		// message, yet each one is received in full within MessageTimeout.
		// The whole run takes about 1.6s, more than MessageTimeout plus one check interval: without
		// restarting the timer for each message the connection would certainly be closed.
		var frames [][]byte
		for i := range 16 {
			frames = append(frames, appendMaskedFrame(nil, OpText, true, fmt.Appendf(nil, "m%02d", i)))
		}
		c.Write(frames[0][:3])
		for i := range frames {
			time.Sleep(100 * time.Millisecond)
			chunk := frames[i][3:]
			if i+1 < len(frames) {
				chunk = slices.Concat(chunk, frames[i+1][:3])
			}
			c.Write(chunk)
		}
		for i := range frames {
			c.expectMessage(t, OpText, fmt.Appendf(nil, "m%02d", i))
		}
	})
	t.Run("timer restarts after a callback", func(t *testing.T) {
		t.Parallel()
		h := &testHandler{blocked: make(chan struct{})}
		s := newTestServer(t, h, Options{MessageTimeout: 200 * time.Millisecond})
		c := dial(t, s)
		head, _ := backlog()
		c.Write(head)
		time.Sleep(100 * time.Millisecond)
		close(h.blocked) // the incomplete message is timed only after this batch; the peer sends no more
		if err := h.waitClose(t); err != os.ErrDeadlineExceeded {
			t.Fatalf("OnClose(%v)", err)
		}
	})
	t.Run("idle timeout", func(t *testing.T) {
		t.Parallel()
		h := &testHandler{}
		s := newTestServer(t, h, Options{IdleTimeout: 500 * time.Millisecond})
		dial(t, s)
		active := dial(t, s)
		stop := make(chan struct{})
		stopped := ping(active, stop) // the client's pings refresh the timer
		if err := h.waitClose(t); err != os.ErrDeadlineExceeded {
			t.Fatalf("idle connection: OnClose(%v)", err)
		}
		time.Sleep(time.Second) // let one more check pass
		close(stop)
		<-stopped
		writeClientMessage(active, OpText, []byte("alive"))
		active.expectMessage(t, OpText, []byte("alive"))
	})
}

// BenchmarkEchoScheduling compares the batch path with the per-task path while also scanning NumLoops and pipeline
// separately. Clients handshake before timing, use fixed frames, and verify every Echo; in-process results do not
// replace benchmarks on separate Linux machines.
func BenchmarkEchoScheduling(b *testing.B) {
	for _, loops := range []int{8, 16, 32} {
		for _, pipeline := range []int{1, 16} {
			for _, batch := range []bool{false, true} {
				b.Run(fmt.Sprintf("loops=%d/pipeline=%d/batch=%t", loops, pipeline, batch), func(b *testing.B) {
					engine := fnet.Options{NumLoops: loops}
					if !batch {
						pool := taskpool.NewDefault()
						engine.Executor = func(_ int, task func()) { pool.Submit(task) }
					}
					mux := http.NewServeMux()
					mux.HandleFunc("/ws", func(w http.ResponseWriter, r *http.Request) {
						if err := Upgrade(w, r, echoHandler{}, Options{}); err != nil {
							b.Error(err)
						}
					})
					srv, err := fhttp.NewServer("127.0.0.1:0", mux, fhttp.Options{Engine: engine})
					if err != nil {
						b.Fatal(err)
					}
					served := make(chan error, 1)
					go func() { served <- srv.Serve() }()
					clients := make([]struct {
						connection net.Conn
						reader     io.Reader
						reply      []byte
					}, 8*runtime.GOMAXPROCS(0))
					b.Cleanup(func() {
						for _, client := range clients {
							if client.connection != nil {
								client.connection.Close()
							}
						}
						srv.Close()
						if err := <-served; !errors.Is(err, http.ErrServerClosed) {
							b.Error(err)
						}
					})
					payload := make([]byte, 7)
					request := maskedFrames(pipeline, len(payload))
					want := bytes.Repeat(frame(OpBinary, payload), pipeline)
					for i := range clients {
						conn, br, err := dialConn(srv.Addr().String(), nil)
						if err != nil {
							b.Fatal(err)
						}
						clients[i].connection = conn
						clients[i].reader = br
						clients[i].reply = make([]byte, len(want))
						conn.SetDeadline(time.Now().Add(30 * time.Second))
					}
					var next atomic.Int32
					samples := []metrics.Sample{{Name: "/sched/latencies:seconds"}}
					metrics.Read(samples)
					var scheduled uint64
					for _, count := range samples[0].Value.Float64Histogram().Counts {
						scheduled += count
					}
					b.SetParallelism(8)
					b.ReportAllocs()
					b.ResetTimer()
					b.RunParallel(func(pb *testing.PB) {
						client := &clients[int(next.Add(1))-1]
						for pb.Next() {
							if n, err := client.connection.Write(request); err != nil || n != len(request) {
								b.Errorf("wrote %d/%d bytes, err=%v", n, len(request), err)
								return
							}
							if _, err := io.ReadFull(client.reader, client.reply); err != nil {
								b.Error(err)
								return
							}
							if !bytes.Equal(client.reply, want) {
								b.Error("Echo data mismatch")
								return
							}
						}
					})
					b.StopTimer()
					b.ReportMetric(float64(b.N*pipeline)/b.Elapsed().Seconds(), "msg/s")
					metrics.Read(samples)
					var finished uint64
					for _, count := range samples[0].Value.Float64Histogram().Counts {
						finished += count
					}
					// runtime samples scheduling once every eight times; this includes both client and server and is
					// not the exact number of worker wakeups.
					b.ReportMetric(float64((finished-scheduled)*8)/float64(b.N*pipeline), "sched/msg")
				})
			}
		}
	}
}

// tlsServerConfig returns a server configuration with a self-signed certificate, which the clients do not verify.
func tlsServerConfig(t *testing.T) *tls.Config {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), cryptorand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	template := &x509.Certificate{SerialNumber: big.NewInt(1)}
	der, err := x509.CreateCertificate(cryptorand.Reader, template, template, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	return &tls.Config{Certificates: []tls.Certificate{{Certificate: [][]byte{der}, PrivateKey: key}}}
}

func TestWSS(t *testing.T) {
	h := &testHandler{closed: make(chan error, 1)}
	mux := http.NewServeMux()
	mux.HandleFunc("/ws", func(w http.ResponseWriter, r *http.Request) {
		Upgrade(w, r, h, Options{})
	})

	s, err := fhttp.NewServer("127.0.0.1:0", mux, fhttp.Options{TLSConfig: tlsServerConfig(t)})
	if err != nil {
		t.Fatal(err)
	}
	served := make(chan error, 1)
	go func() { served <- s.Serve() }()
	t.Cleanup(func() {
		s.Close()
		if err := <-served; !errors.Is(err, http.ErrServerClosed) {
			t.Errorf("Serve returned %v", err)
		}
	})

	conn, br, err := dialConn(s.Addr().String(), &tls.Config{InsecureSkipVerify: true})
	if err != nil {
		t.Fatalf("dial wss: %v", err)
	}
	defer conn.Close()

	msg := []byte("hello wss over reactor")
	if err := writeClientMessage(conn, OpText, msg); err != nil {
		t.Fatalf("write: %v", err)
	}

	replyHeader, reply, err := readServerFrame(br)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if replyHeader.opCode != OpText || !bytes.Equal(reply, msg) {
		t.Fatalf("got %s op=%v, want %s", reply, replyHeader.opCode, msg)
	}

	conn.Close()
	h.waitClose(t)
}
