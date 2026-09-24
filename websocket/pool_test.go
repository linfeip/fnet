package websocket_test

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gobwas/ws"
	"github.com/gobwas/ws/wsutil"
	"github.com/linfeip/fnet"
	"github.com/linfeip/fnet/websocket"
)

func TestWorkerPool_SubmitAndDispatch(t *testing.T) {
	pool := websocket.NewWorkerPool(websocket.WorkerPoolConfig{
		Shards:             8,
		MaxWorkersPerShard: 16,
		QueueSizePerShard:  128,
		IdleTimeout:        time.Second,
	})
	defer pool.Close()

	const totalTasks = 1000
	var counter atomic.Int64
	var wg sync.WaitGroup
	wg.Add(totalTasks)

	for i := 0; i < totalTasks; i++ {
		connID := uint64(i % 50)
		pool.SubmitConn(connID, func() {
			defer wg.Done()
			counter.Add(1)
		})
	}

	wg.Wait()
	if counter.Load() != totalTasks {
		t.Fatalf("expected counter %d, got %d", totalTasks, counter.Load())
	}

	// Test panic recovery inside worker pool: worker must survive
	var panicHandled atomic.Bool
	wg.Add(1)
	pool.Submit(func() {
		defer wg.Done()
		panicHandled.Store(true)
		panic("simulated business handler panic")
	})
	wg.Wait()

	if !panicHandled.Load() {
		t.Fatal("expected panic task to run")
	}

	// Submit another normal task to ensure pool is still healthy
	var afterPanic atomic.Bool
	wg.Add(1)
	pool.Submit(func() {
		defer wg.Done()
		afterPanic.Store(true)
	})
	wg.Wait()

	if !afterPanic.Load() {
		t.Fatal("expected pool to execute tasks normally after panic")
	}
}

func TestWorkerPool_IdleWorkerReclamation(t *testing.T) {
	idleTimeout := 100 * time.Millisecond
	pool := websocket.NewWorkerPool(websocket.WorkerPoolConfig{
		Shards:             4,
		MaxWorkersPerShard: 8,
		QueueSizePerShard:  64,
		IdleTimeout:        idleTimeout,
	})
	defer pool.Close()

	// Initially zero running workers
	if workers := pool.RunningWorkers(); workers != 0 {
		t.Fatalf("expected 0 running workers initially, got %d", workers)
	}

	// Dispatch tasks to spawn workers
	var wg sync.WaitGroup
	const tasks = 50
	wg.Add(tasks)
	for i := 0; i < tasks; i++ {
		pool.Submit(func() {
			defer wg.Done()
			time.Sleep(10 * time.Millisecond)
		})
	}
	wg.Wait()

	// Some workers must have been spawned
	running := pool.RunningWorkers()
	if running == 0 {
		t.Fatal("expected running workers > 0 after executing tasks")
	}

	// Wait for idleTimeout to elapse
	time.Sleep(idleTimeout * 3)

	// All idle workers should have exited
	remaining := pool.RunningWorkers()
	if remaining != 0 {
		t.Fatalf("expected all idle workers to be reaped (0 remaining), got %d", remaining)
	}
}

func TestWebSocketDefaultWorkerPool_BlockingBusinessDoesNotBlockReactor(t *testing.T) {
	// Scenario:
	// Connection A has a slow/blocking OnMessage handler (e.g. simulated DB/RPC call taking 100ms).
	// Connection B sends fast echo messages concurrently.
	//
	// Without a worker pool (i.e. running OnMessage inline in the IO reactor thread),
	// Connection A's block stalls the entire subReactor event loop, freezing Connection B.
	// With the built-in WorkerPool, Connection B must receive responses instantaneously!

	port := getFreePort(t)
	addr := fmt.Sprintf("127.0.0.1:%d", port)

	var (
		blockingConnReady atomic.Bool
		blockingConnDone  atomic.Bool
	)

	upgrader := &websocket.Upgrader{
		CheckOrigin: func(r *http.Request) bool { return true },
		OnMessage: func(c *websocket.Conn, op websocket.OpCode, msg []byte) {
			text := string(msg)
			if text == "BLOCK_ME" {
				blockingConnReady.Store(true)
				time.Sleep(150 * time.Millisecond) // Simulating slow DB query or heavy processing
				blockingConnDone.Store(true)
				_ = c.WriteText("BLOCKED_DONE")
				return
			}
			// Normal fast echo
			_ = c.WriteMessage(op, msg)
		},
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/ws", func(w http.ResponseWriter, r *http.Request) {
		_, _ = upgrader.Upgrade(w, r)
	})

	srv := &fnet.Server{Addr: addr, Handler: mux}
	go func() { _ = srv.ListenAndServe() }()
	defer srv.Close()

	time.Sleep(50 * time.Millisecond)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// 1. Dial Client A (Slow/Blocking Connection)
	clientA, _, _, err := ws.DefaultDialer.Dial(ctx, "ws://"+addr+"/ws")
	if err != nil {
		t.Fatalf("dial client A failed: %v", err)
	}
	defer clientA.Close()

	// 2. Dial Client B (Fast Connection on the same server)
	clientB, _, _, err := ws.DefaultDialer.Dial(ctx, "ws://"+addr+"/ws")
	if err != nil {
		t.Fatalf("dial client B failed: %v", err)
	}
	defer clientB.Close()

	// Client A triggers the 150ms slow business operation
	if err := wsutil.WriteClientText(clientA, []byte("BLOCK_ME")); err != nil {
		t.Fatalf("client A write BLOCK_ME: %v", err)
	}

	// Wait briefly for Client A's message to reach OnMessage
	time.Sleep(10 * time.Millisecond)
	if !blockingConnReady.Load() {
		t.Fatal("Client A's blocking task has not started yet")
	}

	// While Client A is still blocking in OnMessage, Client B sends rapid requests
	const fastRequests = 10
	for i := 0; i < fastRequests; i++ {
		msg := fmt.Sprintf("FAST_PING_%d", i)
		start := time.Now()
		if err := wsutil.WriteClientText(clientB, []byte(msg)); err != nil {
			t.Fatalf("client B write %d: %v", i, err)
		}
		reply, err := wsutil.ReadServerText(clientB)
		elapsed := time.Since(start)
		if err != nil {
			t.Fatalf("client B read %d: %v", i, err)
		}
		if string(reply) != msg {
			t.Fatalf("client B unexpected echo %q, expected %q", string(reply), msg)
		}
		// Because IO reactor is NOT blocked, each roundtrip must be sub-millisecond (or well under 50ms)
		if elapsed > 80*time.Millisecond {
			t.Fatalf("CRITICAL: IO reactor was stalled by slow OnMessage! Client B took %v (expected < 80ms)", elapsed)
		}
	}

	// Verify that Client A finishes cleanly
	replyA, err := wsutil.ReadServerText(clientA)
	if err != nil {
		t.Fatalf("client A read result: %v", err)
	}
	if string(replyA) != "BLOCKED_DONE" {
		t.Fatalf("client A expected 'BLOCKED_DONE', got %q", string(replyA))
	}
	if !blockingConnDone.Load() {
		t.Fatal("expected Client A to finish blocking task")
	}
}

func TestWebSocketWorkerPool_StrictPerConnOrdering(t *testing.T) {
	// Verify that when OnMessage is executed in the worker pool,
	// strict FIFO order per connection is 100% preserved even with large batches of messages.
	port := getFreePort(t)
	addr := fmt.Sprintf("127.0.0.1:%d", port)

	const totalMessages = 200
	receivedIDs := make([]int, 0, totalMessages)
	var mu sync.Mutex
	doneCh := make(chan struct{})

	upgrader := &websocket.Upgrader{
		CheckOrigin: func(r *http.Request) bool { return true },
		OnMessage: func(c *websocket.Conn, op websocket.OpCode, msg []byte) {
			var id int
			_, _ = fmt.Sscanf(string(msg), "SEQ_%d", &id)
			mu.Lock()
			receivedIDs = append(receivedIDs, id)
			if len(receivedIDs) == totalMessages {
				close(doneCh)
			}
			mu.Unlock()
		},
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/ws", func(w http.ResponseWriter, r *http.Request) {
		_, _ = upgrader.Upgrade(w, r)
	})

	srv := &fnet.Server{Addr: addr, Handler: mux}
	go func() { _ = srv.ListenAndServe() }()
	defer srv.Close()

	time.Sleep(50 * time.Millisecond)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	conn, _, _, err := ws.DefaultDialer.Dial(ctx, "ws://"+addr+"/ws")
	if err != nil {
		t.Fatalf("dial failed: %v", err)
	}
	defer conn.Close()

	for i := 0; i < totalMessages; i++ {
		msg := fmt.Sprintf("SEQ_%d", i)
		if err := wsutil.WriteClientText(conn, []byte(msg)); err != nil {
			t.Fatalf("write msg %d: %v", i, err)
		}
	}

	select {
	case <-doneCh:
		mu.Lock()
		defer mu.Unlock()
		for i, id := range receivedIDs {
			if id != i {
				t.Fatalf("FIFO violation: index %d has message id %d", i, id)
			}
		}
		t.Logf("Successfully verified strict per-connection FIFO order for %d messages", totalMessages)
	case <-time.After(3 * time.Second):
		mu.Lock()
		got := len(receivedIDs)
		mu.Unlock()
		t.Fatalf("timed out waiting for messages: received %d / %d", got, totalMessages)
	}
}

func TestWebSocket1MScaleSimulation(t *testing.T) {
	skipOnPumpEmulation(t)
	// Simulate connection lifecycle and worker pool behavior for high-concurrency systems.
	// 1. 100 idle connections should produce 0 worker goroutines and negligible heap overhead.
	// 2. Active burst on a subset of connections dispatches through worker pool without unbounded growth.

	port := getFreePort(t)
	addr := fmt.Sprintf("127.0.0.1:%d", port)

	var activeMessageCount atomic.Int64

	upgrader := &websocket.Upgrader{
		CheckOrigin: func(r *http.Request) bool { return true },
		OnMessage: func(c *websocket.Conn, op websocket.OpCode, msg []byte) {
			activeMessageCount.Add(1)
			_ = c.WriteMessage(op, msg)
		},
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/ws", func(w http.ResponseWriter, r *http.Request) {
		_, _ = upgrader.Upgrade(w, r)
	})

	srv := &fnet.Server{Addr: addr, Handler: mux}
	go func() { _ = srv.ListenAndServe() }()
	defer srv.Close()

	time.Sleep(50 * time.Millisecond)

	initialGoroutines := runtime.NumGoroutine()

	const connCount = 100
	conns := make([]net.Conn, connCount)
	for i := 0; i < connCount; i++ {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		c, _, _, err := ws.DefaultDialer.Dial(ctx, "ws://"+addr+"/ws")
		cancel()
		if err != nil {
			t.Fatalf("dial %d failed: %v", i, err)
		}
		conns[i] = c
		defer c.Close()
	}

	time.Sleep(100 * time.Millisecond)

	// Step 1: Verify 100 idle connections hold almost zero extra goroutines
	idleGoroutines := runtime.NumGoroutine()
	poolWorkers := websocket.DefaultWorkerPool.RunningWorkers()
	diff := (idleGoroutines - poolWorkers) - initialGoroutines
	if diff > 15 {
		t.Fatalf("Idle goroutine count grew excessively: initial=%d, idle=%d, workers=%d, diff=%d",
			initialGoroutines, idleGoroutines, poolWorkers, diff)
	}
	t.Logf("100 idle connections goroutine delta (excluding worker pool): %d, active workers: %d", diff, poolWorkers)

	// Step 2: Send messages concurrently from 20 connections
	var wg sync.WaitGroup
	const activeConns = 20
	const msgsPerConn = 10
	wg.Add(activeConns)

	for i := 0; i < activeConns; i++ {
		c := conns[i]
		go func(conn net.Conn, idx int) {
			defer wg.Done()
			for m := 0; m < msgsPerConn; m++ {
				text := fmt.Sprintf("burst_%d_%d", idx, m)
				if err := wsutil.WriteClientText(conn, []byte(text)); err != nil {
					return
				}
				reply, err := wsutil.ReadServerText(conn)
				if err != nil || string(reply) != text {
					return
				}
			}
		}(c, i)
	}

	wg.Wait()

	expectedTotal := int64(activeConns * msgsPerConn)
	if activeMessageCount.Load() != expectedTotal {
		t.Fatalf("expected %d messages processed, got %d", expectedTotal, activeMessageCount.Load())
	}

	t.Logf("Successfully verified %d burst messages dispatched cleanly through worker pool", expectedTotal)
}

func TestWebSocketWorkerPool_AdaptPool(t *testing.T) {
	var count atomic.Int64
	simpleSubmit := func(task func()) {
		count.Add(1)
		task()
	}

	adapted := websocket.AdaptPool(simpleSubmit)
	if adapted == nil {
		t.Fatal("expected non-nil adapted pool")
	}

	var executed atomic.Bool
	adapted(8888, func() {
		executed.Store(true)
	})

	if count.Load() != 1 || !executed.Load() {
		t.Fatalf("expected simpleSubmit to run, count=%d, executed=%v", count.Load(), executed.Load())
	}
}
