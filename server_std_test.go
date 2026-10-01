//go:build !linux && !darwin

package fnet

import (
	"context"
	"errors"
	"io"
	"log"
	"log/slog"
	"net"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// flakyListener returns a temporary error from its first failures calls to Accept, then accepts connections
// normally.
type flakyListener struct {
	net.Listener
	failures atomic.Int32
}

func (l *flakyListener) Accept() (net.Conn, error) {
	if l.failures.Add(-1) >= 0 {
		return nil, temporaryError{}
	}
	return l.Listener.Accept()
}

type temporaryError struct{}

func (temporaryError) Error() string   { return "accept: too many open files" }
func (temporaryError) Temporary() bool { return true }
func (temporaryError) Timeout() bool   { return false }

// retryRecorder is a slog.Handler that records nothing but the retryIn values from log records.
type retryRecorder struct {
	mu     sync.Mutex
	delays []time.Duration
}

func (h *retryRecorder) Enabled(context.Context, slog.Level) bool { return true }
func (h *retryRecorder) WithAttrs([]slog.Attr) slog.Handler       { return h }
func (h *retryRecorder) WithGroup(string) slog.Handler            { return h }

func (h *retryRecorder) Handle(_ context.Context, r slog.Record) error {
	r.Attrs(func(a slog.Attr) bool {
		if a.Key == "retryIn" {
			h.mu.Lock()
			h.delays = append(h.delays, a.Value.Duration())
			h.mu.Unlock()
		}
		return true
	})
	return nil
}

// TestServeRetriesTemporaryAcceptError verifies a temporary Accept error (such as running out of file
// descriptors) does not make Serve exit: it logs, backs off with an interval starting at 5ms and doubling
// each time, and then keeps accepting connections.
func TestServeRetriesTemporaryAcceptError(t *testing.T) {
	// slog.SetDefault also rewrites the log package's output and flags, so both must be restored at the end
	// (for the reason, see fhttp's TestClient).
	defer func(w io.Writer, flags int) { log.SetOutput(w); log.SetFlags(flags) }(log.Writer(), log.Flags())
	defer slog.SetDefault(slog.Default())
	records := new(retryRecorder)
	slog.SetDefault(slog.New(records))

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	flaky := &flakyListener{Listener: ln}
	flaky.failures.Store(3)
	srv := &Server{handler: &funcHandler{data: echo}, ln: flaky, conns: make(map[*stdConn]struct{})}
	served := make(chan error, 1)
	go func() { served <- srv.Serve() }()

	c, err := net.Dial("tcp", ln.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	c.SetDeadline(time.Now().Add(5 * time.Second))
	c.Write([]byte("hello"))
	if _, err := io.ReadFull(c, make([]byte, 5)); err != nil {
		t.Fatalf("临时错误之后没有继续接受连接: %v", err)
	}
	srv.Close()
	if err := <-served; !errors.Is(err, ErrServerClosed) {
		t.Fatalf("Serve 返回 %v, 期望 ErrServerClosed", err)
	}

	records.mu.Lock()
	defer records.mu.Unlock()
	if want := []time.Duration{5 * time.Millisecond, 10 * time.Millisecond, 20 * time.Millisecond}; !slices.Equal(records.delays, want) {
		t.Fatalf("日志中的退避时间 %v, 期望 %v", records.delays, want)
	}
}
