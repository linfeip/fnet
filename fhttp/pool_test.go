package fhttp_test

import (
	"fmt"
	"io"
	"net"
	"net/http"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/linfeip/fnet/fhttp"
	"github.com/linfeip/fnet/pool"
)

func getFreePort(t *testing.T) int {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("getFreePort: %v", err)
	}
	defer l.Close()
	return l.Addr().(*net.TCPAddr).Port
}

func TestHTTPWorkerPool_SlowBusinessDoesNotBlockReactor(t *testing.T) {
	// Scenario:
	// Client A hits a slow/blocking HTTP handler (simulating a 150ms DB/RPC business operation).
	// Client B concurrently sends fast HTTP GET requests to the same server.
	//
	// Because all HTTP business handling is dispatched to the built-in WorkerPool,
	// Client A's slow execution will never block the IO Reactor thread, and Client B
	// will receive responses instantaneously without latency spikes.

	port := getFreePort(t)
	addr := fmt.Sprintf("127.0.0.1:%d", port)

	var (
		blockingRequestReady atomic.Bool
		blockingRequestDone  atomic.Bool
	)

	mux := http.NewServeMux()
	mux.HandleFunc("/slow", func(w http.ResponseWriter, r *http.Request) {
		blockingRequestReady.Store(true)
		time.Sleep(150 * time.Millisecond) // Simulating slow business logic
		blockingRequestDone.Store(true)
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("SLOW_OK"))
	})
	mux.HandleFunc("/fast", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("FAST_OK"))
	})

	srv := &fhttp.Server{
		Addr:    addr,
		Handler: mux,
	}

	go func() { _ = srv.ListenAndServe() }()
	defer srv.Close()

	time.Sleep(50 * time.Millisecond)

	// 1. Client A sends slow request in background
	var clientAWg sync.WaitGroup
	clientAWg.Add(1)
	go func() {
		defer clientAWg.Done()
		resp, err := http.Get("http://" + addr + "/slow")
		if err != nil {
			t.Errorf("client A get /slow failed: %v", err)
			return
		}
		defer resp.Body.Close()
		body, _ := io.ReadAll(resp.Body)
		if string(body) != "SLOW_OK" {
			t.Errorf("client A expected 'SLOW_OK', got %q", string(body))
		}
	}()

	// Wait for client A's request to reach the slow handler
	deadline := time.Now().Add(time.Second)
	for !blockingRequestReady.Load() && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	if !blockingRequestReady.Load() {
		t.Fatal("client A did not start slow handler in time")
	}

	// 2. Client B sends 10 rapid HTTP requests while Client A is still blocking in business logic
	fastClient := &http.Client{
		Timeout: 2 * time.Second,
	}
	const fastRequests = 10
	for i := 0; i < fastRequests; i++ {
		start := time.Now()
		resp, err := fastClient.Get("http://" + addr + "/fast")
		elapsed := time.Since(start)
		if err != nil {
			t.Fatalf("client B get %d failed: %v", i, err)
		}
		body, _ := io.ReadAll(resp.Body)
		_ = resp.Body.Close()
		if string(body) != "FAST_OK" {
			t.Fatalf("client B expected 'FAST_OK', got %q", string(body))
		}
		// IO Reactor should not be blocked at all; each request should be well under 80ms
		if elapsed > 80*time.Millisecond {
			t.Fatalf("CRITICAL: IO reactor was stalled by slow HTTP business handler! Client B took %v (expected < 80ms)", elapsed)
		}
	}

	clientAWg.Wait()
	if !blockingRequestDone.Load() {
		t.Fatal("expected client A slow request to complete")
	}
}

func TestHTTPWorkerPool_KeepAliveIdleZeroGoroutines(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("the Windows socket emulation holds a pump goroutine per connection")
	}
	// Scenario:
	// Establish 100 Keep-Alive HTTP connections. Each connection sends an initial HTTP request,
	// receives the response, and then stays open in idle keep-alive state.
	//
	// In fnet's reactor architecture with WorkerPool:
	// 1. Idle keep-alive connections are held exclusively by the poller (0 goroutines per idle conn).
	// 2. WorkerPool goroutines automatically exit after idleTimeout, returning total active workers to 0.

	port := getFreePort(t)
	addr := fmt.Sprintf("127.0.0.1:%d", port)

	mux := http.NewServeMux()
	mux.HandleFunc("/ping", func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte("PONG"))
	})

	shortIdleTimeout := 100 * time.Millisecond
	testPool := pool.New(pool.Config{
		IdleTimeout: shortIdleTimeout,
	})
	defer testPool.Close()

	srv := &fhttp.Server{
		Addr:       addr,
		Handler:    mux,
		WorkerPool: testPool.SubmitConn,
	}

	go func() { _ = srv.ListenAndServe() }()
	defer srv.Close()

	time.Sleep(50 * time.Millisecond)

	initialGoroutines := runtime.NumGoroutine()

	const connCount = 100
	conns := make([]net.Conn, connCount)
	for i := 0; i < connCount; i++ {
		c, err := net.DialTimeout("tcp", addr, time.Second)
		if err != nil {
			t.Fatalf("dial %d failed: %v", i, err)
		}
		conns[i] = c
		defer c.Close()

		// Send valid HTTP/1.1 request without Connection: close (keep-alive)
		req := "GET /ping HTTP/1.1\r\nHost: localhost\r\n\r\n"
		if _, err := c.Write([]byte(req)); err != nil {
			t.Fatalf("write %d failed: %v", i, err)
		}

		// Read response until PONG is received
		buf, err := readUntil(c, []byte("PONG"))
		if err != nil {
			t.Fatalf("read %d failed: %v", i, err)
		}
		if !contains(buf, []byte("PONG")) {
			t.Fatalf("expected PONG in response %d, got %s", i, string(buf))
		}
	}

	// 1. Verify that while 100 connections are idle, running workers in pool drops to 0 after idle timeout
	time.Sleep(shortIdleTimeout * 3)

	if remainingWorkers := testPool.RunningWorkers(); remainingWorkers != 0 {
		t.Fatalf("expected 0 running workers after idle timeout, got %d", remainingWorkers)
	}

	currentGoroutines := runtime.NumGoroutine()
	diff := currentGoroutines - initialGoroutines
	t.Logf("100 idle keep-alive connections goroutine delta after worker pool reclamation: %d", diff)
	if diff > 10 {
		t.Fatalf("Keep-alive idle goroutine leak: initial=%d, current=%d, diff=%d (expected <= 10)",
			initialGoroutines, currentGoroutines, diff)
	}

	// 2. Verify connections are still alive by sending a second request on the idle connections
	for i := 0; i < 10; i++ {
		c := conns[i]
		req := "GET /ping HTTP/1.1\r\nHost: localhost\r\n\r\n"
		if _, err := c.Write([]byte(req)); err != nil {
			t.Fatalf("second write %d failed: %v", i, err)
		}
		buf, err := readUntil(c, []byte("PONG"))
		if err != nil {
			t.Fatalf("second read %d failed: %v", i, err)
		}
		if !contains(buf, []byte("PONG")) {
			t.Fatalf("expected PONG on second request %d", i)
		}
	}
}

func readUntil(c net.Conn, target []byte) ([]byte, error) {
	var total []byte
	buf := make([]byte, 256)
	_ = c.SetReadDeadline(time.Now().Add(2 * time.Second))
	for {
		n, err := c.Read(buf)
		if n > 0 {
			total = append(total, buf[:n]...)
			if contains(total, target) {
				return total, nil
			}
		}
		if err != nil {
			return total, err
		}
	}
}

func contains(b, sub []byte) bool {
	if len(sub) == 0 {
		return true
	}
	if len(b) < len(sub) {
		return false
	}
	for i := 0; i <= len(b)-len(sub); i++ {
		match := true
		for j := 0; j < len(sub); j++ {
			if b[i+j] != sub[j] {
				match = false
				break
			}
		}
		if match {
			return true
		}
	}
	return false
}

func TestHTTPWorkerPool_CustomPool(t *testing.T) {
	port := getFreePort(t)
	addr := fmt.Sprintf("127.0.0.1:%d", port)

	var customPoolDispatched atomic.Int64
	customPool := func(connID uint64, task func()) error {
		customPoolDispatched.Add(1)
		go task()
		return nil
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/test", func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte("CUSTOM_POOL_OK"))
	})

	srv := &fhttp.Server{
		Addr:       addr,
		Handler:    mux,
		WorkerPool: customPool,
	}

	go func() { _ = srv.ListenAndServe() }()
	defer srv.Close()

	time.Sleep(50 * time.Millisecond)

	for i := 0; i < 5; i++ {
		resp, err := http.Get("http://" + addr + "/test")
		if err != nil {
			t.Fatalf("request %d failed: %v", i, err)
		}
		body, _ := io.ReadAll(resp.Body)
		_ = resp.Body.Close()
		if string(body) != "CUSTOM_POOL_OK" {
			t.Fatalf("expected 'CUSTOM_POOL_OK', got %q", string(body))
		}
	}

	if customPoolDispatched.Load() == 0 {
		t.Fatal("expected custom WorkerPool to be invoked")
	}
	t.Logf("Custom worker pool dispatched tasks: %d", customPoolDispatched.Load())
}

func TestHTTPWorkerPool_PanicRecovery(t *testing.T) {
	port := getFreePort(t)
	addr := fmt.Sprintf("127.0.0.1:%d", port)

	mux := http.NewServeMux()
	mux.HandleFunc("/panic", func(w http.ResponseWriter, r *http.Request) {
		panic("simulated HTTP handler panic")
	})
	mux.HandleFunc("/healthy", func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte("HEALTHY"))
	})

	srv := &fhttp.Server{
		Addr:    addr,
		Handler: mux,
	}

	go func() { _ = srv.ListenAndServe() }()
	defer srv.Close()

	time.Sleep(50 * time.Millisecond)

	// Send request that panics in business logic: connection should close, but server remains alive
	_, _ = http.Get("http://" + addr + "/panic")

	// Server and worker pool must still be healthy
	resp, err := http.Get("http://" + addr + "/healthy")
	if err != nil {
		t.Fatalf("healthy request failed after panic: %v", err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	if string(body) != "HEALTHY" {
		t.Fatalf("expected 'HEALTHY', got %q", string(body))
	}
}

func TestWorkerPool_ServerAndUpgraderCustomPool(t *testing.T) {
	// Test passing custom *WorkerPool directly via Server.WorkerPool (SubmitConn)
	customPool := pool.New(pool.Config{
		MaxWorkers:  32,
		IdleTimeout: time.Second,
	})
	defer customPool.Close()

	port := getFreePort(t)
	addr := fmt.Sprintf("127.0.0.1:%d", port)

	var handled atomic.Int64
	mux := http.NewServeMux()
	mux.HandleFunc("/test", func(w http.ResponseWriter, r *http.Request) {
		handled.Add(1)
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("CUSTOM_POOL_OK"))
	})

	srv := &fhttp.Server{
		Addr:       addr,
		Handler:    mux,
		WorkerPool: customPool.SubmitConn,
	}

	go func() { _ = srv.ListenAndServe() }()
	defer srv.Close()

	time.Sleep(50 * time.Millisecond)

	resp, err := http.Get("http://" + addr + "/test")
	if err != nil {
		t.Fatalf("request failed: %v", err)
	}
	defer resp.Body.Close()

	if handled.Load() != 1 {
		t.Fatalf("expected 1 handled request, got %d", handled.Load())
	}

	if running := customPool.RunningWorkers(); running == 0 {
		t.Fatalf("expected customPool to have active running workers, got %d", running)
	}
}
