package websocket_test

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"net"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/gobwas/httphead"
	"github.com/gobwas/ws"
	"github.com/gobwas/ws/wsflate"
	"github.com/gobwas/ws/wsutil"
	"github.com/linfeip/fnet"
	"github.com/linfeip/fnet/websocket"
)

// helper to start an echo server in either Event-Driven mode or Goroutine mode
func startAutobahnEchoServer(t *testing.T, eventDriven bool, compression bool) (string, func()) {
	t.Helper()
	port := getFreePort(t)
	addr := fmt.Sprintf("127.0.0.1:%d", port)
	mux := http.NewServeMux()

	if eventDriven {
		upgrader := &websocket.Upgrader{
			EnableCompression: compression,
			OnMessage: func(conn *websocket.Conn, op websocket.OpCode, msg []byte) {
				_ = conn.WriteMessage(op, msg)
			},
		}
		mux.HandleFunc("/ws", func(w http.ResponseWriter, r *http.Request) {
			_, _ = upgrader.Upgrade(w, r)
		})
	} else {
		upgrader := &websocket.Upgrader{
			EnableCompression: compression,
		}
		mux.HandleFunc("/ws", func(w http.ResponseWriter, r *http.Request) {
			conn, err := upgrader.Upgrade(w, r)
			if err != nil {
				return
			}
			defer conn.Close()
			_ = conn.Handle(func(op websocket.OpCode, msg []byte) error {
				return conn.WriteMessage(op, msg)
			})
		})
	}

	srv := &fnet.Server{Addr: addr, Handler: mux}
	go func() { _ = srv.ListenAndServe() }()
	time.Sleep(50 * time.Millisecond)

	return addr, func() { _ = srv.Close() }
}

func dialAutobahnClient(t *testing.T, addr string, compression bool) net.Conn {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	dialer := ws.DefaultDialer
	if compression {
		dialer = ws.Dialer{
			Extensions: []httphead.Option{
				wsflate.DefaultParameters.Option(),
			},
		}
	}

	conn, _, _, err := dialer.Dial(ctx, "ws://"+addr+"/ws")
	if err != nil {
		t.Fatalf("dial failed: %v", err)
	}
	return conn
}

func readCloseFrame(t *testing.T, conn net.Conn) (ws.StatusCode, string) {
	t.Helper()
	_ = conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	frame, err := ws.ReadFrame(conn)
	if err != nil {
		t.Fatalf("read frame failed: %v", err)
	}
	if frame.Header.OpCode != ws.OpClose {
		t.Fatalf("expected OpClose, got %v", frame.Header.OpCode)
	}
	if len(frame.Payload) == 0 {
		return ws.StatusNormalClosure, ""
	}
	if len(frame.Payload) < 2 {
		t.Fatalf("close frame payload too short: %d", len(frame.Payload))
	}
	code := ws.StatusCode(binary.BigEndian.Uint16(frame.Payload[:2]))
	reason := string(frame.Payload[2:])
	return code, reason
}

// ---------------------------------------------------------------------------
// Case 1: Framing & Payload Sizes (0, 125, 126, 127, 65535, 65536)
// ---------------------------------------------------------------------------
func TestAutobahn_Case1_Framing(t *testing.T) {
	modes := []struct {
		name        string
		eventDriven bool
	}{
		{"EventDriven", true},
		{"Goroutine", false},
	}

	sizes := []int{0, 1, 125, 126, 127, 1024, 65535, 65536}

	for _, m := range modes {
		t.Run(m.name, func(t *testing.T) {
			addr, cleanup := startAutobahnEchoServer(t, m.eventDriven, false)
			defer cleanup()

			conn := dialAutobahnClient(t, addr, false)
			defer conn.Close()

			for _, size := range sizes {
				// 1. Text payload
				textPayload := []byte(strings.Repeat("A", size))
				if err := wsutil.WriteClientText(conn, textPayload); err != nil {
					t.Fatalf("write text size %d failed: %v", size, err)
				}
				_ = conn.SetReadDeadline(time.Now().Add(3 * time.Second))
				replyText, err := wsutil.ReadServerText(conn)
				if err != nil {
					t.Fatalf("read text size %d failed: %v", size, err)
				}
				if !bytes.Equal(replyText, textPayload) {
					t.Fatalf("text payload size %d echoed mismatch", size)
				}

				// 2. Binary payload
				binPayload := make([]byte, size)
				for i := range binPayload {
					binPayload[i] = byte(i % 256)
				}
				if err := wsutil.WriteClientBinary(conn, binPayload); err != nil {
					t.Fatalf("write binary size %d failed: %v", size, err)
				}
				_ = conn.SetReadDeadline(time.Now().Add(3 * time.Second))
				replyBin, err := wsutil.ReadServerBinary(conn)
				if err != nil {
					t.Fatalf("read binary size %d failed: %v", size, err)
				}
				if !bytes.Equal(replyBin, binPayload) {
					t.Fatalf("binary payload size %d echoed mismatch", size)
				}
			}
		})
	}
}

// ---------------------------------------------------------------------------
// Case 2: Ping & Pong
// ---------------------------------------------------------------------------
func TestAutobahn_Case2_PingPong(t *testing.T) {
	modes := []struct {
		name        string
		eventDriven bool
	}{
		{"EventDriven", true},
		{"Goroutine", false},
	}

	for _, m := range modes {
		t.Run(m.name, func(t *testing.T) {
			addr, cleanup := startAutobahnEchoServer(t, m.eventDriven, false)
			defer cleanup()

			t.Run("PingWithPayloads", func(t *testing.T) {
				conn := dialAutobahnClient(t, addr, false)
				defer conn.Close()

				for _, size := range []int{0, 10, 125} {
					pingData := make([]byte, size)
					for i := range pingData {
						pingData[i] = byte(i)
					}
					pingFrame := ws.MaskFrame(ws.NewPingFrame(pingData))
					if err := ws.WriteFrame(conn, pingFrame); err != nil {
						t.Fatalf("write ping size %d failed: %v", size, err)
					}

					_ = conn.SetReadDeadline(time.Now().Add(2 * time.Second))
					pongFrame, err := ws.ReadFrame(conn)
					if err != nil {
						t.Fatalf("read pong failed: %v", err)
					}
					if pongFrame.Header.OpCode != ws.OpPong {
						t.Fatalf("expected OpPong, got %v", pongFrame.Header.OpCode)
					}
					if !bytes.Equal(pongFrame.Payload, pingData) {
						t.Fatalf("pong payload mismatch for size %d", size)
					}
				}
			})

			t.Run("UnsolicitedPongIgnored", func(t *testing.T) {
				conn := dialAutobahnClient(t, addr, false)
				defer conn.Close()

				// Send unsolicited pong
				pongFrame := ws.MaskFrame(ws.NewPongFrame([]byte("unsolicited-pong")))
				if err := ws.WriteFrame(conn, pongFrame); err != nil {
					t.Fatalf("write pong failed: %v", err)
				}

				// Connection should remain healthy: send normal text frame
				msg := []byte("still-alive")
				if err := wsutil.WriteClientText(conn, msg); err != nil {
					t.Fatalf("write text failed: %v", err)
				}
				reply, err := wsutil.ReadServerText(conn)
				if err != nil || !bytes.Equal(reply, msg) {
					t.Fatalf("connection broken after unsolicited pong: %v", err)
				}
			})

			t.Run("PingPayloadOverflowFailsWith1002", func(t *testing.T) {
				conn := dialAutobahnClient(t, addr, false)
				defer conn.Close()

				// Control frame length > 125 bytes is illegal (RFC 6455 5.5)
				illegalPing := ws.MaskFrame(ws.Frame{
					Header: ws.Header{
						Fin:    true,
						OpCode: ws.OpPing,
						Masked: true,
						Length: 126,
					},
					Payload: make([]byte, 126),
				})
				_ = ws.WriteFrame(conn, illegalPing)

				code, _ := readCloseFrame(t, conn)
				if code != ws.StatusProtocolError {
					t.Fatalf("expected close code 1002 (ProtocolError), got %d", code)
				}
			})

			t.Run("FragmentedPingFailsWith1002", func(t *testing.T) {
				conn := dialAutobahnClient(t, addr, false)
				defer conn.Close()

				// Control frames MUST NOT be fragmented (Fin must be true)
				illegalPing := ws.MaskFrame(ws.Frame{
					Header: ws.Header{
						Fin:    false, // ILLEGAL
						OpCode: ws.OpPing,
						Masked: true,
						Length: 4,
					},
					Payload: []byte("ping"),
				})
				_ = ws.WriteFrame(conn, illegalPing)

				code, _ := readCloseFrame(t, conn)
				if code != ws.StatusProtocolError {
					t.Fatalf("expected close code 1002 (ProtocolError), got %d", code)
				}
			})
		})
	}
}

// ---------------------------------------------------------------------------
// Case 3: Reserved Bits & Masking
// ---------------------------------------------------------------------------
func TestAutobahn_Case3_ReservedBitsAndMasking(t *testing.T) {
	modes := []struct {
		name        string
		eventDriven bool
	}{
		{"EventDriven", true},
		{"Goroutine", false},
	}

	for _, m := range modes {
		t.Run(m.name, func(t *testing.T) {
			addr, cleanup := startAutobahnEchoServer(t, m.eventDriven, false)
			defer cleanup()

			t.Run("UnmaskedClientFrameFailsWith1002", func(t *testing.T) {
				conn := dialAutobahnClient(t, addr, false)
				defer conn.Close()

				// Unmasked frame from client (RFC 6455 5.1 MUST be masked)
				unmaskedFrame := ws.Frame{
					Header: ws.Header{
						Fin:    true,
						OpCode: ws.OpText,
						Masked: false, // ILLEGAL
						Length: 5,
					},
					Payload: []byte("hello"),
				}
				_ = ws.WriteFrame(conn, unmaskedFrame)

				code, _ := readCloseFrame(t, conn)
				if code != ws.StatusProtocolError {
					t.Fatalf("expected 1002 on unmasked frame, got %d", code)
				}
			})

			t.Run("RSV1WithoutCompressionFailsWith1002", func(t *testing.T) {
				conn := dialAutobahnClient(t, addr, false)
				defer conn.Close()

				frame := ws.MaskFrame(ws.Frame{
					Header: ws.Header{
						Fin:    true,
						Rsv:    ws.Rsv(true, false, false), // RSV1 = 1 without extension
						OpCode: ws.OpText,
						Masked: true,
						Length: 4,
					},
					Payload: []byte("test"),
				})
				_ = ws.WriteFrame(conn, frame)

				code, _ := readCloseFrame(t, conn)
				if code != ws.StatusProtocolError {
					t.Fatalf("expected 1002 on RSV1 without extension, got %d", code)
				}
			})

			t.Run("RSV2FailsWith1002", func(t *testing.T) {
				conn := dialAutobahnClient(t, addr, false)
				defer conn.Close()

				frame := ws.MaskFrame(ws.Frame{
					Header: ws.Header{
						Fin:    true,
						Rsv:    ws.Rsv(false, true, false), // RSV2 = 1
						OpCode: ws.OpText,
						Masked: true,
						Length: 4,
					},
					Payload: []byte("test"),
				})
				_ = ws.WriteFrame(conn, frame)

				code, _ := readCloseFrame(t, conn)
				if code != ws.StatusProtocolError {
					t.Fatalf("expected 1002 on RSV2, got %d", code)
				}
			})

			t.Run("RSV3FailsWith1002", func(t *testing.T) {
				conn := dialAutobahnClient(t, addr, false)
				defer conn.Close()

				frame := ws.MaskFrame(ws.Frame{
					Header: ws.Header{
						Fin:    true,
						Rsv:    ws.Rsv(false, false, true), // RSV3 = 1
						OpCode: ws.OpText,
						Masked: true,
						Length: 4,
					},
					Payload: []byte("test"),
				})
				_ = ws.WriteFrame(conn, frame)

				code, _ := readCloseFrame(t, conn)
				if code != ws.StatusProtocolError {
					t.Fatalf("expected 1002 on RSV3, got %d", code)
				}
			})

			t.Run("RSVOnControlFrameFailsWith1002", func(t *testing.T) {
				conn := dialAutobahnClient(t, addr, false)
				defer conn.Close()

				frame := ws.MaskFrame(ws.Frame{
					Header: ws.Header{
						Fin:    true,
						Rsv:    ws.Rsv(true, false, false), // RSV on Ping
						OpCode: ws.OpPing,
						Masked: true,
						Length: 0,
					},
				})
				_ = ws.WriteFrame(conn, frame)

				code, _ := readCloseFrame(t, conn)
				if code != ws.StatusProtocolError {
					t.Fatalf("expected 1002 on RSV control frame, got %d", code)
				}
			})
		})
	}
}

// ---------------------------------------------------------------------------
// Case 4: Reserved Opcodes
// ---------------------------------------------------------------------------
func TestAutobahn_Case4_ReservedOpcodes(t *testing.T) {
	modes := []struct {
		name        string
		eventDriven bool
	}{
		{"EventDriven", true},
		{"Goroutine", false},
	}

	reservedOpcodes := []ws.OpCode{3, 4, 5, 6, 7, 11, 12, 13, 14, 15}

	for _, m := range modes {
		t.Run(m.name, func(t *testing.T) {
			addr, cleanup := startAutobahnEchoServer(t, m.eventDriven, false)
			defer cleanup()

			for _, op := range reservedOpcodes {
				t.Run(fmt.Sprintf("Opcode_%d", op), func(t *testing.T) {
					conn := dialAutobahnClient(t, addr, false)
					defer conn.Close()

					frame := ws.MaskFrame(ws.Frame{
						Header: ws.Header{
							Fin:    true,
							OpCode: op,
							Masked: true,
							Length: 0,
						},
					})
					_ = ws.WriteFrame(conn, frame)

					code, _ := readCloseFrame(t, conn)
					if code != ws.StatusProtocolError {
						t.Fatalf("expected 1002 on reserved opcode %d, got %d", op, code)
					}
				})
			}
		})
	}
}

// ---------------------------------------------------------------------------
// Case 5: Fragmentation
// ---------------------------------------------------------------------------
func TestAutobahn_Case5_Fragmentation(t *testing.T) {
	modes := []struct {
		name        string
		eventDriven bool
	}{
		{"EventDriven", true},
		{"Goroutine", false},
	}

	for _, m := range modes {
		t.Run(m.name, func(t *testing.T) {
			addr, cleanup := startAutobahnEchoServer(t, m.eventDriven, false)
			defer cleanup()

			t.Run("ValidFragmentedMessage", func(t *testing.T) {
				conn := dialAutobahnClient(t, addr, false)
				defer conn.Close()

				// Frame 1: OpText, Fin=false
				f1 := ws.MaskFrame(ws.Frame{
					Header: ws.Header{
						Fin:    false,
						OpCode: ws.OpText,
						Masked: true,
						Length: 5,
					},
					Payload: []byte("Hello"),
				})
				if err := ws.WriteFrame(conn, f1); err != nil {
					t.Fatalf("write f1: %v", err)
				}

				// Frame 2: OpContinuation, Fin=false
				f2 := ws.MaskFrame(ws.Frame{
					Header: ws.Header{
						Fin:    false,
						OpCode: ws.OpContinuation,
						Masked: true,
						Length: 2,
					},
					Payload: []byte(", "),
				})
				if err := ws.WriteFrame(conn, f2); err != nil {
					t.Fatalf("write f2: %v", err)
				}

				// Frame 3: OpContinuation, Fin=true
				f3 := ws.MaskFrame(ws.Frame{
					Header: ws.Header{
						Fin:    true,
						OpCode: ws.OpContinuation,
						Masked: true,
						Length: 6,
					},
					Payload: []byte("World!"),
				})
				if err := ws.WriteFrame(conn, f3); err != nil {
					t.Fatalf("write f3: %v", err)
				}

				reply, err := wsutil.ReadServerText(conn)
				if err != nil {
					t.Fatalf("read server text failed: %v", err)
				}
				if string(reply) != "Hello, World!" {
					t.Fatalf("expected 'Hello, World!', got %q", string(reply))
				}
			})

			t.Run("PingInterleavedBetweenFragments", func(t *testing.T) {
				conn := dialAutobahnClient(t, addr, false)
				defer conn.Close()

				// Fragment 1
				f1 := ws.MaskFrame(ws.Frame{
					Header: ws.Header{
						Fin:    false,
						OpCode: ws.OpText,
						Masked: true,
						Length: 4,
					},
					Payload: []byte("Part"),
				})
				_ = ws.WriteFrame(conn, f1)

				// Interleaved Ping frame
				pingData := []byte("interleaved-ping")
				_ = ws.WriteFrame(conn, ws.MaskFrame(ws.NewPingFrame(pingData)))

				// Should receive Pong immediately
				_ = conn.SetReadDeadline(time.Now().Add(2 * time.Second))
				pong, err := ws.ReadFrame(conn)
				if err != nil || pong.Header.OpCode != ws.OpPong || !bytes.Equal(pong.Payload, pingData) {
					t.Fatalf("failed to receive pong during fragmentation: %v", err)
				}

				// Fragment 2 (Final)
				f2 := ws.MaskFrame(ws.Frame{
					Header: ws.Header{
						Fin:    true,
						OpCode: ws.OpContinuation,
						Masked: true,
						Length: 4,
					},
					Payload: []byte("Two!"),
				})
				_ = ws.WriteFrame(conn, f2)

				// Complete message echo
				reply, err := wsutil.ReadServerText(conn)
				if err != nil || string(reply) != "PartTwo!" {
					t.Fatalf("fragmented message corrupted by ping: got %q, err %v", string(reply), err)
				}
			})

			t.Run("UnexpectedContinuationFailsWith1002", func(t *testing.T) {
				conn := dialAutobahnClient(t, addr, false)
				defer conn.Close()

				// Sending continuation when no fragment started
				f := ws.MaskFrame(ws.Frame{
					Header: ws.Header{
						Fin:    true,
						OpCode: ws.OpContinuation, // ILLEGAL without start
						Masked: true,
						Length: 4,
					},
					Payload: []byte("test"),
				})
				_ = ws.WriteFrame(conn, f)

				code, _ := readCloseFrame(t, conn)
				if code != ws.StatusProtocolError {
					t.Fatalf("expected 1002 on unexpected continuation, got %d", code)
				}
			})

			t.Run("NewDataFrameBeforeFinishFailsWith1002", func(t *testing.T) {
				conn := dialAutobahnClient(t, addr, false)
				defer conn.Close()

				// Start fragment 1
				f1 := ws.MaskFrame(ws.Frame{
					Header: ws.Header{
						Fin:    false,
						OpCode: ws.OpText,
						Masked: true,
						Length: 4,
					},
					Payload: []byte("frag"),
				})
				_ = ws.WriteFrame(conn, f1)

				// Send new text frame instead of continuation (ILLEGAL)
				f2 := ws.MaskFrame(ws.Frame{
					Header: ws.Header{
						Fin:    true,
						OpCode: ws.OpText,
						Masked: true,
						Length: 4,
					},
					Payload: []byte("oops"),
				})
				_ = ws.WriteFrame(conn, f2)

				code, _ := readCloseFrame(t, conn)
				if code != ws.StatusProtocolError {
					t.Fatalf("expected 1002 on new frame before continuation finished, got %d", code)
				}
			})
		})
	}
}

// ---------------------------------------------------------------------------
// Case 6: UTF-8 Handling
// ---------------------------------------------------------------------------
func TestAutobahn_Case6_UTF8(t *testing.T) {
	modes := []struct {
		name        string
		eventDriven bool
	}{
		{"EventDriven", true},
		{"Goroutine", false},
	}

	for _, m := range modes {
		t.Run(m.name, func(t *testing.T) {
			addr, cleanup := startAutobahnEchoServer(t, m.eventDriven, false)
			defer cleanup()

			t.Run("ValidUTF8MultibyteAndEmoji", func(t *testing.T) {
				conn := dialAutobahnClient(t, addr, false)
				defer conn.Close()

				validStrings := []string{
					"Hello World",
					"你好世界！这是一段中文测试",
					"🚀 Special Emojis: 🎉🔥❤️ 👍",
					"Mixed 123 汉字 🚀 and Latin ñ, é, ü",
				}
				for _, s := range validStrings {
					if err := wsutil.WriteClientText(conn, []byte(s)); err != nil {
						t.Fatalf("write valid text failed: %v", err)
					}
					reply, err := wsutil.ReadServerText(conn)
					if err != nil || string(reply) != s {
						t.Fatalf("expected %q, got %q (err: %v)", s, string(reply), err)
					}
				}
			})

			t.Run("ValidUTF8SplitAcrossFrames", func(t *testing.T) {
				conn := dialAutobahnClient(t, addr, false)
				defer conn.Close()

				// "世" is 3 bytes: 0xE4, 0xB8, 0x96
				// Frame 1 has 0xE4, 0xB8 (incomplete sequence)
				// Frame 2 has 0x96 (completes sequence)
				charBytes := []byte("世")
				f1 := ws.MaskFrame(ws.Frame{
					Header: ws.Header{
						Fin:    false,
						OpCode: ws.OpText,
						Masked: true,
						Length: 2,
					},
					Payload: charBytes[:2],
				})
				_ = ws.WriteFrame(conn, f1)

				f2 := ws.MaskFrame(ws.Frame{
					Header: ws.Header{
						Fin:    true,
						OpCode: ws.OpContinuation,
						Masked: true,
						Length: 1,
					},
					Payload: charBytes[2:],
				})
				_ = ws.WriteFrame(conn, f2)

				reply, err := wsutil.ReadServerText(conn)
				if err != nil || string(reply) != "世" {
					t.Fatalf("failed to assemble split UTF-8 character: got %q, err %v", string(reply), err)
				}
			})

			invalidUTF8Samples := []struct {
				name string
				data []byte
			}{
				{"InvalidByte0xFF", []byte{0xFF}},
				{"InvalidContinuationByte", []byte{0x80}},
				{"Incomplete2ByteSeq", []byte{0xC3}},
				{"OverlongASCII_Slash", []byte{0xC0, 0xAF}},
				{"SurrogatePair_D800", []byte{0xED, 0xA0, 0x80}},
				{"CodepointBeyondMax_U10FFFF", []byte{0xF4, 0x90, 0x80, 0x80}},
			}

			for _, sample := range invalidUTF8Samples {
				t.Run(sample.name, func(t *testing.T) {
					conn := dialAutobahnClient(t, addr, false)
					defer conn.Close()

					frame := ws.MaskFrame(ws.Frame{
						Header: ws.Header{
							Fin:    true,
							OpCode: ws.OpText,
							Masked: true,
							Length: int64(len(sample.data)),
						},
						Payload: sample.data,
					})
					_ = ws.WriteFrame(conn, frame)

					code, _ := readCloseFrame(t, conn)
					if code != ws.StatusInvalidFramePayloadData {
						t.Fatalf("expected 1007 on %s, got %d", sample.name, code)
					}
				})
			}
		})
	}
}

// ---------------------------------------------------------------------------
// Case 7: Close Handshake
// ---------------------------------------------------------------------------
func TestAutobahn_Case7_CloseHandshake(t *testing.T) {
	modes := []struct {
		name        string
		eventDriven bool
	}{
		{"EventDriven", true},
		{"Goroutine", false},
	}

	for _, m := range modes {
		t.Run(m.name, func(t *testing.T) {
			addr, cleanup := startAutobahnEchoServer(t, m.eventDriven, false)
			defer cleanup()

			t.Run("NormalClose1000", func(t *testing.T) {
				conn := dialAutobahnClient(t, addr, false)
				defer conn.Close()

				closeFrame := ws.MaskFrame(ws.NewCloseFrame(ws.NewCloseFrameBody(ws.StatusNormalClosure, "normal-bye")))
				_ = ws.WriteFrame(conn, closeFrame)

				code, _ := readCloseFrame(t, conn)
				if code != ws.StatusNormalClosure {
					t.Fatalf("expected 1000, got %d", code)
				}
			})

			t.Run("EmptyPayloadCloseFrame", func(t *testing.T) {
				conn := dialAutobahnClient(t, addr, false)
				defer conn.Close()

				// 0-byte close frame payload is valid
				closeFrame := ws.MaskFrame(ws.Frame{
					Header: ws.Header{
						Fin:    true,
						OpCode: ws.OpClose,
						Masked: true,
						Length: 0,
					},
				})
				_ = ws.WriteFrame(conn, closeFrame)

				code, _ := readCloseFrame(t, conn)
				if code != ws.StatusNormalClosure {
					t.Fatalf("expected 1000 on empty close, got %d", code)
				}
			})

			t.Run("Invalid1ByteClosePayloadFailsWith1002", func(t *testing.T) {
				conn := dialAutobahnClient(t, addr, false)
				defer conn.Close()

				// 1-byte close frame payload is illegal (RFC 6455 5.5.1)
				closeFrame := ws.MaskFrame(ws.Frame{
					Header: ws.Header{
						Fin:    true,
						OpCode: ws.OpClose,
						Masked: true,
						Length: 1,
					},
					Payload: []byte{0x03},
				})
				_ = ws.WriteFrame(conn, closeFrame)

				code, _ := readCloseFrame(t, conn)
				if code != ws.StatusProtocolError {
					t.Fatalf("expected 1002 on 1-byte close payload, got %d", code)
				}
			})

			invalidCodes := []ws.StatusCode{
				0, 999, 1004, 1005, 1006, 1014, 1015, 2000, 2999, 5000, 9999,
			}
			for _, invCode := range invalidCodes {
				t.Run(fmt.Sprintf("InvalidCode_%d", invCode), func(t *testing.T) {
					conn := dialAutobahnClient(t, addr, false)
					defer conn.Close()

					var body [2]byte
					binary.BigEndian.PutUint16(body[:], uint16(invCode))

					closeFrame := ws.MaskFrame(ws.Frame{
						Header: ws.Header{
							Fin:    true,
							OpCode: ws.OpClose,
							Masked: true,
							Length: 2,
						},
						Payload: body[:],
					})
					_ = ws.WriteFrame(conn, closeFrame)

					code, _ := readCloseFrame(t, conn)
					if code != ws.StatusProtocolError {
						t.Fatalf("expected 1002 on invalid close code %d, got %d", invCode, code)
					}
				})
			}

			t.Run("InvalidUTF8InCloseReasonFailsWith1007", func(t *testing.T) {
				conn := dialAutobahnClient(t, addr, false)
				defer conn.Close()

				body := []byte{0x03, 0xE8, 0xFF, 0xFE} // 1000 + invalid UTF-8 bytes
				closeFrame := ws.MaskFrame(ws.Frame{
					Header: ws.Header{
						Fin:    true,
						OpCode: ws.OpClose,
						Masked: true,
						Length: int64(len(body)),
					},
					Payload: body,
				})
				_ = ws.WriteFrame(conn, closeFrame)

				code, _ := readCloseFrame(t, conn)
				if code != ws.StatusInvalidFramePayloadData {
					t.Fatalf("expected 1007 on invalid UTF-8 in close reason, got %d", code)
				}
			})
		})
	}
}

// ---------------------------------------------------------------------------
// Case 9: High Volume / Large Payloads
// ---------------------------------------------------------------------------
func TestAutobahn_Case9_HighVolume(t *testing.T) {
	modes := []struct {
		name        string
		eventDriven bool
	}{
		{"EventDriven", true},
		{"Goroutine", false},
	}

	largeSizes := []int{64 * 1024, 256 * 1024, 1024 * 1024}

	for _, m := range modes {
		t.Run(m.name, func(t *testing.T) {
			addr, cleanup := startAutobahnEchoServer(t, m.eventDriven, false)
			defer cleanup()

			conn := dialAutobahnClient(t, addr, false)
			defer conn.Close()

			for _, size := range largeSizes {
				t.Run(fmt.Sprintf("%d_Bytes", size), func(t *testing.T) {
					payload := make([]byte, size)
					for i := range payload {
						payload[i] = byte(i % 251)
					}

					if err := wsutil.WriteClientBinary(conn, payload); err != nil {
						t.Fatalf("write %d bytes failed: %v", size, err)
					}

					_ = conn.SetReadDeadline(time.Now().Add(5 * time.Second))
					echoed, err := wsutil.ReadServerBinary(conn)
					if err != nil {
						t.Fatalf("read %d bytes failed: %v", size, err)
					}
					if !bytes.Equal(echoed, payload) {
						t.Fatalf("payload mismatch for %d bytes", size)
					}
				})
			}
		})
	}
}

// ---------------------------------------------------------------------------
// Case 12 & 13: Permessage-Deflate Compression
// ---------------------------------------------------------------------------
func TestAutobahn_Case12_13_Compression(t *testing.T) {
	modes := []struct {
		name        string
		eventDriven bool
	}{
		{"EventDriven", true},
		{"Goroutine", false},
	}

	for _, m := range modes {
		t.Run(m.name, func(t *testing.T) {
			addr, cleanup := startAutobahnEchoServer(t, m.eventDriven, true)
			defer cleanup()

			t.Run("CompressedTextAndBinaryEcho", func(t *testing.T) {
				conn := dialAutobahnClient(t, addr, true)
				defer conn.Close()

				msg := []byte("Autobahn compression echo test: " + strings.Repeat("ABCDEF123456 ", 50))
				f := ws.NewTextFrame(msg)
				compFrame, err := compressClientFrame(f)
				if err != nil {
					t.Fatalf("compress: %v", err)
				}
				compFrame = ws.MaskFrame(compFrame)
				if err := ws.WriteFrame(conn, compFrame); err != nil {
					t.Fatalf("write comp frame: %v", err)
				}

				respFrame, err := ws.ReadFrame(conn)
				if err != nil {
					t.Fatalf("read resp: %v", err)
				}
				decomp, err := decompressServerFrame(respFrame)
				if err != nil {
					t.Fatalf("decompress: %v", err)
				}
				if !bytes.Equal(decomp.Payload, msg) {
					t.Fatalf("decompressed message mismatch")
				}
			})

			t.Run("RSV1OnContinuationFrameFailsWith1002", func(t *testing.T) {
				conn := dialAutobahnClient(t, addr, true)
				defer conn.Close()

				// RFC 7692 5.1: RSV1 MUST NOT be set on continuation frames!
				f1 := ws.MaskFrame(ws.Frame{
					Header: ws.Header{
						Fin:    false,
						Rsv:    ws.Rsv(true, false, false), // RSV1=1 on first fragment (valid)
						OpCode: ws.OpBinary,
						Masked: true,
						Length: 4,
					},
					Payload: []byte("comp"),
				})
				_ = ws.WriteFrame(conn, f1)

				f2 := ws.MaskFrame(ws.Frame{
					Header: ws.Header{
						Fin:    true,
						Rsv:    ws.Rsv(true, false, false), // RSV1=1 on continuation (ILLEGAL)
						OpCode: ws.OpContinuation,
						Masked: true,
						Length: 4,
					},
					Payload: []byte("tail"),
				})
				_ = ws.WriteFrame(conn, f2)

				code, _ := readCloseFrame(t, conn)
				if code != ws.StatusProtocolError {
					t.Fatalf("expected 1002 on RSV1 on continuation frame, got %d", code)
				}
			})
		})
	}
}
