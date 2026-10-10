package websocket

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"log/slog"
	"math"
	"math/rand/v2"
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
	"unsafe"

	"github.com/linfeip/fnet"
	"github.com/linfeip/fnet/fhttp"
	"github.com/linfeip/fnet/internal/units"
	"github.com/linfeip/fnet/taskpool"

	"github.com/gobwas/ws"
	"github.com/gobwas/ws/wsutil"
)

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

func (h *testHandler) OnMessage(c *Conn, op ws.OpCode, data []byte) {
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
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	conn, br, _, err := ws.Dial(ctx, "ws://"+s.Addr().String()+"/ws")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { conn.Close() })
	conn.SetDeadline(time.Now().Add(5 * time.Second))
	c := &client{Conn: conn, reader: conn}
	if br != nil {
		c.reader = io.MultiReader(br, conn)
	}
	return c
}

func (c *client) send(t *testing.T, op ws.OpCode, fin bool, payload string) {
	t.Helper()
	if err := ws.WriteFrame(c, ws.MaskFrame(ws.NewFrame(op, fin, []byte(payload)))); err != nil {
		t.Fatal(err)
	}
}

func (c *client) expectMessage(t *testing.T, wantOp ws.OpCode, want []byte) {
	t.Helper()
	data, op, err := wsutil.ReadServerData(c)
	if err != nil || op != wantOp || !bytes.Equal(data, want) {
		t.Fatalf("got op=%v len=%d err=%v, want op=%v len=%d", op, len(data), err, wantOp, len(want))
	}
}

// expectClose skips data frames until it reads the server's close frame and checks its status code; after
// that the connection should be closed.
func (c *client) expectClose(t *testing.T, want ws.StatusCode) {
	t.Helper()
	f, err := ws.ReadFrame(c)
	for err == nil && f.Header.OpCode.IsData() {
		f, err = ws.ReadFrame(c)
	}
	if err != nil || f.Header.OpCode != ws.OpClose {
		t.Fatalf("want a close frame, got %v %v", f.Header, err)
	}
	if code, _ := ws.ParseCloseFrameData(f.Payload); code != want {
		t.Fatalf("close frame status code %d, want %d", code, want)
	}
	// If the peer is still sending when the server closes (for example data sent after a protocol
	// error), the kernel ends the connection with RST, so the read returns ECONNRESET instead of EOF.
	if _, err := c.Read(make([]byte, 1)); err != io.EOF && !errors.Is(err, syscall.ECONNRESET) {
		t.Fatalf("want the connection to be closed, got %v", err)
	}
}

func TestEcho(t *testing.T) {
	s := newTestServer(t, &testHandler{}, Options{})
	c := dial(t, s)

	wsutil.WriteClientMessage(c, ws.OpText, []byte("hello"))
	c.expectMessage(t, ws.OpText, []byte("hello"))
	wsutil.WriteClientMessage(c, ws.OpBinary, nil)
	c.expectMessage(t, ws.OpBinary, nil)
	big := bytes.Repeat([]byte("0123456789abcdef"), 32*units.KB) // 512KB, spans multiple reads
	wsutil.WriteClientMessage(c, ws.OpBinary, big)
	c.expectMessage(t, ws.OpBinary, big)

	// A ping inserted in the middle of a fragmented message: the pong arrives first, then the reassembled
	// message.
	c.send(t, ws.OpText, false, "frag")
	c.send(t, ws.OpPing, true, "p")
	c.send(t, ws.OpContinuation, true, "ment")
	f, err := ws.ReadFrame(c)
	if err != nil || f.Header.OpCode != ws.OpPong || string(f.Payload) != "p" {
		t.Fatalf("want pong, got %v %q %v", f.Header, f.Payload, err)
	}
	c.expectMessage(t, ws.OpText, []byte("fragment"))
}

// TestEchoPipelined verifies a real WebSocket connection sending and receiving multi-frame batches back to back,
// covering Echo ordering across read rounds.
func TestEchoPipelined(t *testing.T) {
	for _, size := range []int{7, units.KB} {
		t.Run(fmt.Sprintf("size=%d", size), func(t *testing.T) {
			s := newTestServer(t, &testHandler{}, Options{})
			c := dial(t, s)
			payloads := make([][]byte, 16)
			for i := range payloads {
				payloads[i] = bytes.Repeat([]byte{byte(i)}, size)
			}
			batch := clientFrames(ws.OpBinary, payloads...)
			for range 3 {
				if n, err := c.Write(batch); err != nil || n != len(batch) {
					t.Fatalf("wrote %d/%d bytes, err=%v", n, len(batch), err)
				}
				for _, payload := range payloads {
					c.expectMessage(t, ws.OpBinary, payload)
				}
			}
		})
	}
}

// TestConnSize checks that Conn stays within the 256B size class, since connections are numerous: when
// adding, removing or reordering fields pushes it over, the fields have to be rearranged.
func TestConnSize(t *testing.T) {
	if size := unsafe.Sizeof(Conn{}); unsafe.Sizeof(uintptr(0)) == 8 && size > 256 {
		t.Fatalf("Conn is %dB, over the 256B size class", size)
	}
}

// TestApplyMaskMatchesCipher checks that applyMask produces identical output to ws.Cipher for all payload lengths.
func TestApplyMaskMatchesCipher(t *testing.T) {
	mask := [4]byte{0x12, 0x34, 0x56, 0x78}
	for size := 0; size <= 1024; size++ {
		src := make([]byte, size)
		for i := range src {
			src[i] = byte(i * 17)
		}
		want := slices.Clone(src)
		ws.Cipher(want, mask, 0)
		got := slices.Clone(src)
		applyMask(got, mask)
		if !bytes.Equal(got, want) {
			t.Fatalf("size %d: got %x want %x", size, got, want)
		}
	}
}

// TestAppendFrameHeader checks that the frame header encoding is byte-for-byte identical to ws.WriteHeader:
// RFC 6455 requires the shortest encoding for the payload length, which the peer does not necessarily check
// when parsing.
func TestAppendFrameHeader(t *testing.T) {
	for _, n := range []int{0, 125, 126, 64*units.KB - 1, 64 * units.KB, units.MB} {
		var want bytes.Buffer
		ws.WriteHeader(&want, ws.Header{Fin: true, OpCode: ws.OpText, Length: int64(n)})
		if got := appendFrameHeader(nil, ws.OpText, n); !bytes.Equal(got, want.Bytes()) {
			t.Errorf("length %d: got %x, want %x", n, got, want.Bytes())
		}
	}
}

// checkParseHeader checks that parseHeader agrees with ws.ReadHeader on data: the same header and header length, the
// same error, and "incomplete" exactly when ws.ReadHeader hits the end of data.
func checkParseHeader(t *testing.T, data []byte) {
	t.Helper()
	var r bytes.Reader
	r.Reset(data)
	want, wantErr := ws.ReadHeader(&r)
	got, n, err := parseHeader(data)
	if wantErr == io.EOF || wantErr == io.ErrUnexpectedEOF {
		if n != 0 || err != nil {
			t.Fatalf("%x: incomplete header, got n=%d err=%v, want n=0 err=nil", data, n, err)
		}
		return
	}
	if err != wantErr {
		t.Fatalf("%x: err = %v, want %v", data, err, wantErr)
	}
	if err != nil {
		return // the header is undefined on error
	}
	if wantN := len(data) - r.Len(); got != want || n != wantN {
		t.Fatalf("%x: got %+v n=%d, want %+v n=%d", data, got, n, want, wantN)
	}
}

// TestParseHeader checks parseHeader against ws.ReadHeader on every combination of the header fields (every
// length encoding, FIN, RSV, opcode and mask), on every truncation of the encoded header, and with payload bytes
// following the header, which must not be consumed.
func TestParseHeader(t *testing.T) {
	lengths := []int64{0, 1, 125, 126, 127, 65535, 65536, 1 << 31, 1 << 40, math.MaxInt64}
	for _, length := range lengths {
		for _, masked := range []bool{false, true} {
			for _, fin := range []bool{false, true} {
				for op := range ws.OpCode(16) {
					for rsv := range byte(8) {
						h := ws.Header{Fin: fin, Rsv: rsv, OpCode: op, Masked: masked, Length: length}
						if masked {
							h.Mask = [4]byte{0x12, 0x34, 0x56, 0x78}
						}
						var buf bytes.Buffer
						ws.WriteHeader(&buf, h)
						encoded := append(buf.Bytes(), "payload"...)
						for n := range len(encoded) + 1 {
							checkParseHeader(t, encoded[:n])
						}
					}
				}
			}
		}
	}
}

// TestParseHeaderMSB checks that a 64-bit length with its most significant bit set is rejected, and only once the
// whole header has arrived, as ws.ReadHeader does.
func TestParseHeaderMSB(t *testing.T) {
	data := []byte{0x82, 0xff, 0x80, 0, 0, 0, 0, 0, 0, 1, 1, 2, 3, 4}
	for n := range len(data) + 1 {
		checkParseHeader(t, data[:n])
	}
	if _, _, err := parseHeader(data); err != ws.ErrHeaderLengthMSB {
		t.Fatalf("err = %v, want %v", err, ws.ErrHeaderLengthMSB)
	}
	if _, n, err := parseHeader(data[:len(data)-1]); n != 0 || err != nil {
		t.Fatalf("with an incomplete header n=%d err=%v, want n=0 err=nil", n, err)
	}
}

// TestParseHeaderRandom checks parseHeader against ws.ReadHeader on random bytes, which also covers the headers a
// well-behaved peer never sends (non-shortest length encodings, for example). The length code is picked among the
// three length encodings, since a uniformly random byte would almost never pick the 16 and 64-bit ones.
func TestParseHeaderRandom(t *testing.T) {
	rng := rand.New(rand.NewPCG(1, 2))
	for range 200000 {
		data := make([]byte, rng.IntN(17))
		for i := range data {
			data[i] = byte(rng.Uint32())
		}
		if len(data) >= 2 {
			code := [3]byte{byte(rng.IntN(126)), 126, 127}[rng.IntN(3)]
			data[1] = data[1]&0x80 | code
		}
		checkParseHeader(t, data)
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

func (echoHandler) OnOpen(*Conn)                                 {}
func (echoHandler) OnMessage(c *Conn, op ws.OpCode, data []byte) { c.WriteMessage(op, data) }
func (echoHandler) OnClose(*Conn, error)                         {}

// maskedFrames returns count masked binary frames of payload bytes each, back to back, as one client write.
func maskedFrames(count, payload int) []byte {
	var buf bytes.Buffer
	body := make([]byte, payload)
	for range count {
		ws.WriteFrame(&buf, ws.MaskFrame(ws.NewFrame(ws.OpBinary, true, body)))
	}
	return buf.Bytes()
}

// BenchmarkOnData measures the receive path of one connection for a client write of frames pipelined messages of
// 1KB each (frames=1 is the plain echo, frames=10 is the pipelined case): header parsing, unmasking, the echo
// into the cork buffer and the single write that goes out afterwards.
func BenchmarkOnData(b *testing.B) {
	for _, frames := range []int{1, 2, 10, 16} {
		b.Run(fmt.Sprintf("frames=%d", frames), func(b *testing.B) {
			batch := maskedFrames(frames, units.KB)
			c := &Conn{connection: stubConnection{}, handler: echoHandler{}, maxMessageSize: units.MB}
			data := make([]byte, len(batch))
			b.SetBytes(int64(frames * units.KB))
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
		for _, size := range []int{7, units.KB} {
			t.Run(fmt.Sprintf("frames=%d/size=%d", frames, size), func(t *testing.T) {
				batch := maskedFrames(frames, size)
				c := &Conn{connection: stubConnection{}, handler: echoHandler{}, maxMessageSize: units.MB}
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
		wsutil.WriteClientMessage(&buf, ws.OpText, fmt.Appendf(nil, "%d", i))
	}
	c.Write(buf.Bytes())
	for i := range 200 {
		c.expectMessage(t, ws.OpText, fmt.Appendf(nil, "%d", i))
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
	wsutil.WriteClientMessage(&b, ws.OpText, []byte("early"))
	conn.Write(b.Bytes())
	br := bufio.NewReader(conn)
	if resp, err := http.ReadResponse(br, nil); err != nil || resp.StatusCode != http.StatusSwitchingProtocols {
		t.Fatalf("handshake: %v %v", resp, err)
	}
	time.Sleep(300 * time.Millisecond) // OnOpen has returned
	wsutil.WriteClientMessage(conn, ws.OpText, []byte("late"))
	c := &client{Conn: conn, reader: br}
	c.expectMessage(t, ws.OpText, []byte("early"))
	c.expectMessage(t, ws.OpText, []byte("late"))
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
	wsutil.WriteClientMessage(blocked, ws.OpText, []byte("a"))
	time.Sleep(50 * time.Millisecond) // the data has arrived, but it must not occupy the only worker
	other := dial(t, srv)
	wsutil.WriteClientMessage(other, ws.OpText, []byte("b"))
	other.expectMessage(t, ws.OpText, []byte("b"))

	release.Do(func() { close(h.gate) })
	blocked.expectMessage(t, ws.OpText, []byte("a"))
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
	wsutil.WriteClientMessage(&buf, ws.OpText, []byte("last"))
	wsutil.WriteClientMessage(&buf, ws.OpClose, ws.NewCloseFrameBody(ws.StatusGoingAway, "bye"))
	c.Write(buf.Bytes())
	c.expectClose(t, ws.StatusGoingAway)
	if err := h.waitClose(t); err != (wsutil.ClosedError{Code: ws.StatusGoingAway, Reason: "bye"}) {
		t.Fatalf("peer close: OnClose(%v)", err)
	}
	if len(h.received) != 1 || <-h.received != "last" {
		t.Fatal("a message before the close frame was not delivered before OnClose")
	}

	// Close from the local side.
	c = dial(t, s)
	wsutil.WriteClientMessage(c, ws.OpText, []byte("close"))
	c.expectClose(t, ws.StatusNormalClosure)
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
	s := newTestServer(t, h, Options{MaxMessageSize: units.KB})
	long := string(bytes.Repeat([]byte("a"), 600))
	tests := []struct {
		name     string
		frames   []ws.Frame // masked before sending unless unmasked is set
		unmasked bool
		code     ws.StatusCode
		err      error
	}{
		{"unmasked", []ws.Frame{ws.NewTextFrame([]byte("x"))}, true, ws.StatusProtocolError, ws.ErrProtocolMaskRequired},
		{"RSV non-zero", []ws.Frame{{Header: ws.Header{Fin: true, Rsv: ws.Rsv(true, false, false), OpCode: ws.OpText}}},
			false, ws.StatusProtocolError, ws.ErrProtocolNonZeroRsv},
		{"unexpected continuation frame", []ws.Frame{ws.NewFrame(ws.OpContinuation, true, nil)},
			false, ws.StatusProtocolError, ws.ErrProtocolContinuationUnexpected},
		{"close frame with a single byte", []ws.Frame{ws.NewCloseFrame([]byte{3})},
			false, ws.StatusProtocolError, ws.ErrProtocolStatusCodeNotInUse},
		{"invalid UTF-8", []ws.Frame{ws.NewTextFrame([]byte{0xff})},
			false, ws.StatusInvalidFramePayloadData, wsutil.ErrInvalidUTF8},
		{"message too big", []ws.Frame{ws.NewBinaryFrame(make([]byte, 2000))}, false, ws.StatusMessageTooBig, errMessageTooBig},
		{"fragments together too big", []ws.Frame{ws.NewFrame(ws.OpText, false, []byte(long)), ws.NewFrame(ws.OpContinuation, true, []byte(long))},
			false, ws.StatusMessageTooBig, errMessageTooBig},
	}
	for _, tt := range tests {
		c := dial(t, s)
		for _, f := range tt.frames {
			if !tt.unmasked {
				f = ws.MaskFrame(f)
			}
			ws.WriteFrame(c, f) // the server may close before reading it all; write errors surface in the checks below
		}
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
	expectPanicLog(t)
	// Two messages arrive while OnOpen runs and are received in the same read: after the panic the
	// connection is closed with 1011, the remaining messages of that batch are not delivered, and
	// OnClose is called last.
	h := &testHandler{openDelay: 100 * time.Millisecond, received: make(chan string, 2)}
	s := newTestServer(t, h, Options{})
	c := dial(t, s)
	var b bytes.Buffer
	wsutil.WriteClientMessage(&b, ws.OpText, []byte("panic"))
	wsutil.WriteClientMessage(&b, ws.OpText, []byte("after"))
	c.Write(b.Bytes())
	c.expectClose(t, ws.StatusInternalServerError)
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
	c.expectClose(t, ws.StatusInternalServerError)
	if err := h.waitClose(t); err != errHandlerPanic {
		t.Fatalf("OnClose(%v)", err)
	}
}

// TestSlowHandler checks that reading stops while the application callback is slow: the peer's writes are
// blocked by TCP flow control, the connection is not closed because of that, and nothing is timed while the
// callback runs; once the application recovers, all messages are delivered in order.
func TestSlowHandler(t *testing.T) {
	h := &testHandler{blocked: make(chan struct{})}
	s := newTestServer(t, h, Options{MessageTimeout: 300 * time.Millisecond})
	c := dial(t, s)

	head, rest := backlog()
	c.Write(head)
	time.Sleep(1500 * time.Millisecond) // more than MessageTimeout plus one deadline check interval

	// Later data stays in the kernel buffer; once that is full the peer is blocked.
	stream := bytes.NewBuffer(rest)
	payload := bytes.Repeat([]byte("x"), 64*units.KB)
	for range 512 { // 32MB, far beyond the kernel buffer
		wsutil.WriteClientMessage(stream, ws.OpBinary, payload)
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

	close(h.blocked)
	c.SetDeadline(time.Now().Add(10 * time.Second))
	written := make(chan error, 1)
	go func() {
		_, err := c.Write(stream.Bytes()[n:])
		written <- err
	}()
	for range backlogMessages {
		c.expectMessage(t, ws.OpBinary, nil)
	}
	c.expectMessage(t, ws.OpText, []byte("tail"))
	for range 512 {
		c.expectMessage(t, ws.OpBinary, payload)
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
	var b, tail bytes.Buffer
	for range backlogMessages {
		wsutil.WriteClientMessage(&b, ws.OpBinary, nil)
	}
	ws.WriteFrame(&tail, ws.MaskFrame(ws.NewTextFrame([]byte("tail"))))
	b.Write(tail.Bytes()[:tail.Len()-2])
	return b.Bytes(), tail.Bytes()[tail.Len()-2:]
}

type nopHandler struct{}

func (nopHandler) OnOpen(*Conn)                       {}
func (nopHandler) OnMessage(*Conn, ws.OpCode, []byte) {}
func (nopHandler) OnClose(*Conn, error)               {}

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

// newCorkConn returns a connection whose writes are recorded; OnData's callbacks run directly in the test.
func newCorkConn() (*Conn, *recordConn) {
	rc := &recordConn{}
	c := &Conn{connection: rc, handler: &testHandler{closed: make(chan error, 1)}, maxMessageSize: units.MB}
	c.SetContext("opened")
	return c, rc
}

func frame(op ws.OpCode, payload []byte) []byte {
	return append(appendFrameHeader(nil, op, len(payload)), payload...)
}

// clientFrames lays out the client frames (masked) for payloads, in order.
func clientFrames(op ws.OpCode, payloads ...[]byte) []byte {
	var b bytes.Buffer
	for _, p := range payloads {
		wsutil.WriteClientMessage(&b, op, p)
	}
	return b.Bytes()
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
		want = append(want, frame(ws.OpText, data)...)
	}
	in := clientFrames(ws.OpText, payloads...)
	if n := c.OnData(in); n != len(in) {
		t.Fatalf("consumed %d bytes, want %d", n, len(in))
	}
	if len(rc.events) != 1 || !bytes.Equal(rc.events[0].data, want) {
		t.Fatalf("10 frames were written %d times, want them merged into 1", len(rc.events))
	}

	c.OnData(clientFrames(ws.OpText, []byte("one")))
	if len(rc.events) != 2 || rc.events[1].vectored || !bytes.Equal(rc.events[1].data, frame(ws.OpText, []byte("one"))) {
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
				if err := c.WriteMessage(ws.OpText, payload); err != nil {
					t.Error(err)
				}
			}
		}()
	}
	wg.Wait()
	c.uncork()
	want := bytes.Repeat(frame(ws.OpText, payload), 8*16)
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
	}{{0, false}, {125, false}, {maxCopiedPayload, false}, {maxCopiedPayload + 1, true}, {units.MB, true}} {
		rc := &recordConn{}
		c := &Conn{connection: rc}
		payload := bytes.Repeat([]byte("x"), tc.size)
		if err := c.WriteMessage(ws.OpBinary, payload); err != nil {
			t.Fatal(err)
		}
		if len(rc.events) != 1 || !bytes.Equal(rc.events[0].data, frame(ws.OpBinary, payload)) {
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
		want = append(want, frame(ws.OpBinary, p)...)
	}
	c.OnData(clientFrames(ws.OpBinary, payloads...))

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
	c.OnData(clientFrames(ws.OpText, []byte("a"), []byte("b")))
	c.OnData(clientFrames(ws.OpText, []byte("c")))
	want := slices.Concat(frame(ws.OpText, []byte("a")), frame(ws.OpText, []byte("b")))
	if len(rc.events) != 2 || !bytes.Equal(rc.events[0].data, want) ||
		rc.events[1].vectored || !bytes.Equal(rc.events[1].data, frame(ws.OpText, []byte("c"))) {
		t.Fatalf("writes: %+v", rc.events)
	}
}

// TestCorkEveryOnData checks that every OnData that receives several frames corks anew: the corking state of one
// OnData must not leak into the next, or the replies of the later batches would go out one write each.
func TestCorkEveryOnData(t *testing.T) {
	c, rc := newCorkConn()
	want := slices.Concat(frame(ws.OpText, []byte("a")), frame(ws.OpText, []byte("b")), frame(ws.OpText, []byte("c")))
	for round := range 3 {
		c.OnData(clientFrames(ws.OpText, []byte("a"), []byte("b"), []byte("c")))
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
	c.OnData(clientFrames(ws.OpText, []byte("a"), []byte("b"), []byte("close"), []byte("c")))
	want := slices.Concat(frame(ws.OpText, []byte("a")), frame(ws.OpText, []byte("b")))
	closeFrame := frame(ws.OpClose, ws.NewCloseFrameBody(ws.StatusNormalClosure, ""))
	if len(rc.events) != 3 || !bytes.Equal(rc.events[0].data, want) || !bytes.Equal(rc.events[1].data, closeFrame) ||
		!rc.events[2].close {
		t.Fatalf("close during handling: %+v", rc.events)
	}
	if err := c.WriteMessage(ws.OpText, []byte("after")); err != net.ErrClosed {
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
	in := clientFrames(ws.OpText, []byte("a"), []byte("close"), []byte("b"), []byte("c"))
	if n := c.OnData(in); n != len(in) {
		t.Fatalf("consumed %d bytes, want %d", n, len(in))
	}
	if got := len(h.received); got != 2 { // "a" and "close"
		t.Fatalf("messages left in the same batch were still delivered after Close: %d delivered, want 2", got)
	}
	later := clientFrames(ws.OpText, []byte("d"))
	if n := c.OnData(later); n != len(later) || len(h.received) != 2 {
		t.Fatalf("data arriving after Close: consumed %d/%d bytes, %d delivered", n, len(later), len(h.received))
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
		c.OnData(clientFrames(ws.OpText, []byte("a"), []byte("b"), []byte("panic"), []byte("c")))
	}()
	want := slices.Concat(frame(ws.OpText, []byte("a")), frame(ws.OpText, []byte("b")))
	closeFrame := frame(ws.OpClose, ws.NewCloseFrameBody(ws.StatusInternalServerError, ""))
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
			c, _, _, err := ws.Dial(context.Background(), "ws://"+s.Addr().String()+"/ws")
			if err != nil {
				t.Error(err)
				return
			}
			defer c.Close()
			c.SetDeadline(time.Now().Add(5 * time.Second))
			for i := range 50 {
				msg := fmt.Appendf(nil, "message %d", i)
				if err := wsutil.WriteClientMessage(c, ws.OpText, msg); err != nil {
					t.Error(err)
					return
				}
				data, _, err := wsutil.ReadServerData(c)
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
			for ws.WriteFrame(c, ws.MaskFrame(ws.NewPingFrame(nil))) == nil {
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
		c.send(t, ws.OpText, false, "part")
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
			var b bytes.Buffer
			ws.WriteFrame(&b, ws.MaskFrame(ws.NewTextFrame(fmt.Appendf(nil, "m%02d", i))))
			frames = append(frames, b.Bytes())
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
			c.expectMessage(t, ws.OpText, fmt.Appendf(nil, "m%02d", i))
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
		wsutil.WriteClientMessage(active, ws.OpText, []byte("alive"))
		active.expectMessage(t, ws.OpText, []byte("alive"))
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
						engine.Executor = func(_ int, task func()) { taskpool.DefaultTaskPool.Submit(task) }
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
					want := bytes.Repeat(frame(ws.OpBinary, payload), pipeline)
					for i := range clients {
						ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
						conn, buffered, _, err := ws.Dial(ctx, "ws://"+srv.Addr().String()+"/ws")
						cancel()
						if err != nil {
							b.Fatal(err)
						}
						clients[i].connection = conn
						clients[i].reader = conn
						if buffered != nil {
							clients[i].reader = io.MultiReader(buffered, conn)
						}
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
