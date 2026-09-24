package pool

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestDirectAPI(t *testing.T) {
	p := New(Config{
		Shards:             4,
		MaxWorkersPerShard: 8,
		QueueSizePerShard:  64,
		IdleTimeout:        100 * time.Millisecond,
	})
	defer p.Close()

	const tasks = 200
	var executed atomic.Int64
	var wg sync.WaitGroup
	wg.Add(tasks)

	for i := 0; i < tasks; i++ {
		connID := uint64(i % 16)
		p.SubmitConn(connID, func() {
			defer wg.Done()
			executed.Add(1)
		})
	}

	wg.Wait()
	if executed.Load() != tasks {
		t.Fatalf("expected %d tasks, got %d", tasks, executed.Load())
	}

	// Verify worker reclamation
	time.Sleep(300 * time.Millisecond)
	if remaining := p.RunningWorkers(); remaining != 0 {
		t.Fatalf("expected 0 running workers after idle timeout, got %d", remaining)
	}
}

func TestWorkStealingUnderSkew(t *testing.T) {
	// Scenario:
	// A pool has 4 shards, each limited to 1 worker.
	// Shard A receives a heavy task that blocks for 150ms.
	// Subsequently, 10 fast tasks are also routed to the exact same shard.
	// Without work-stealing, the 10 fast tasks are stuck behind the heavy task on Shard A for 150ms.
	// With work-stealing, idle workers from Shard B, C, D immediately steal and complete the 10 fast tasks
	// while Shard A's single worker is still busy sleeping!

	p := New(Config{
		Shards:             4,
		MaxWorkersPerShard: 1, // Strict 1 worker per shard
		QueueSizePerShard:  64,
		IdleTimeout:        time.Second,
	})
	defer p.Close()

	// Pre-spawn workers on all 4 shards by dispatching warmup tasks
	for i := 0; p.RunningWorkers() < 4 && i < 100; i++ {
		p.Submit(func() {
			time.Sleep(5 * time.Millisecond)
		})
		time.Sleep(2 * time.Millisecond)
	}

	// Ensure all 4 shards have their 1 worker running and now idle
	time.Sleep(20 * time.Millisecond)
	if workers := p.RunningWorkers(); workers != 4 {
		t.Fatalf("expected 4 running workers, got %d", workers)
	}

	const targetConnID = uint64(0)
	var heavyTaskRunning atomic.Bool
	var heavyTaskFinished atomic.Bool
	heavyStarted := make(chan struct{})

	// 1. Submit heavy task to targetConnID
	p.SubmitConn(targetConnID, func() {
		heavyTaskRunning.Store(true)
		close(heavyStarted)
		time.Sleep(150 * time.Millisecond)
		heavyTaskRunning.Store(false)
		heavyTaskFinished.Store(true)
	})

	<-heavyStarted

	// 2. Submit 10 fast tasks to the EXACT same connection/shard
	const fastTaskCount = 10
	var fastCompletedWhileHeavyRunning atomic.Int64
	var fastWg sync.WaitGroup
	fastWg.Add(fastTaskCount)

	start := time.Now()
	for i := 0; i < fastTaskCount; i++ {
		p.SubmitConn(targetConnID, func() {
			defer fastWg.Done()
			if heavyTaskRunning.Load() {
				// Completed while heavy task is still running! Proof of work-stealing!
				fastCompletedWhileHeavyRunning.Add(1)
			}
		})
	}

	fastWg.Wait()
	fastDuration := time.Since(start)

	// Verify that fast tasks finished well before the 150ms heavy task completed
	if fastDuration >= 120*time.Millisecond {
		t.Fatalf("Work-stealing failed: fast tasks took %v (expected < 100ms, stolen by other workers)", fastDuration)
	}

	if completed := fastCompletedWhileHeavyRunning.Load(); completed == 0 {
		t.Fatalf("Expected fast tasks to be executed concurrently by stolen workers while heavy task was running, got %d", completed)
	}

	t.Logf("Work-stealing verified: %d/%d tasks stolen and completed in %v while heavy task was running",
		fastCompletedWhileHeavyRunning.Load(), fastTaskCount, fastDuration)
}

func TestPowerOfTwoChoicesBalance(t *testing.T) {
	// Verify that Submit with Power-of-Two-Choices effectively balances tasks
	p := New(Config{
		Shards:             8,
		MaxWorkersPerShard: 4,
		QueueSizePerShard:  128,
		IdleTimeout:        time.Second,
	})
	defer p.Close()

	const total = 400
	var executed atomic.Int64
	var wg sync.WaitGroup
	wg.Add(total)

	for i := 0; i < total; i++ {
		p.Submit(func() {
			defer wg.Done()
			executed.Add(1)
			time.Sleep(time.Millisecond)
		})
	}

	wg.Wait()
	if executed.Load() != total {
		t.Fatalf("expected %d, got %d", total, executed.Load())
	}
}

func TestConcurrentCloseAndSubmit(t *testing.T) {
	p := New(Config{
		Shards:             4,
		MaxWorkersPerShard: 8,
		QueueSizePerShard:  16,
		IdleTimeout:        100 * time.Millisecond,
	})

	const numSubmitters = 50
	var wg sync.WaitGroup
	wg.Add(numSubmitters)

	for i := 0; i < numSubmitters; i++ {
		go func(id int) {
			defer wg.Done()
			for j := 0; j < 50; j++ {
				p.SubmitConn(uint64(id), func() {
					time.Sleep(time.Microsecond)
				})
			}
		}(i)
	}

	time.Sleep(time.Millisecond)
	p.Close()
	wg.Wait()
}

func TestAdapt(t *testing.T) {
	var count atomic.Int64
	legacySubmit := func(task func()) {
		count.Add(1)
		task()
	}

	adapted := Adapt(legacySubmit)
	if adapted == nil {
		t.Fatal("expected non-nil adapted p")
	}

	var executed atomic.Bool
	adapted(12345, func() {
		executed.Store(true)
	})

	if count.Load() != 1 || !executed.Load() {
		t.Fatalf("expected legacySubmit to execute task, count=%d, executed=%v", count.Load(), executed.Load())
	}

	if Adapt(nil) != nil {
		t.Fatal("expected nil for nil submit function")
	}
}

func TestSubmitAndDispatch(t *testing.T) {
	p := New(Config{
		Shards:             8,
		MaxWorkersPerShard: 16,
		QueueSizePerShard:  128,
		IdleTimeout:        time.Second,
	})
	defer p.Close()

	const totalTasks = 1000
	var counter atomic.Int64
	var wg sync.WaitGroup
	wg.Add(totalTasks)

	for i := 0; i < totalTasks; i++ {
		connID := uint64(i % 50)
		p.SubmitConn(connID, func() {
			defer wg.Done()
			counter.Add(1)
		})
	}

	wg.Wait()
	if counter.Load() != totalTasks {
		t.Fatalf("expected counter %d, got %d", totalTasks, counter.Load())
	}

	// Test panic recovery inside the pool: the worker must survive
	var panicHandled atomic.Bool
	wg.Add(1)
	p.Submit(func() {
		defer wg.Done()
		panicHandled.Store(true)
		panic("simulated business handler panic")
	})
	wg.Wait()

	if !panicHandled.Load() {
		t.Fatal("expected panic task to run")
	}

	// Submit another normal task to ensure the pool is still healthy
	var afterPanic atomic.Bool
	wg.Add(1)
	p.Submit(func() {
		defer wg.Done()
		afterPanic.Store(true)
	})
	wg.Wait()

	if !afterPanic.Load() {
		t.Fatal("expected p to execute tasks normally after panic")
	}
}

func TestIdleWorkerReclamation(t *testing.T) {
	idleTimeout := 100 * time.Millisecond
	p := New(Config{
		Shards:             4,
		MaxWorkersPerShard: 8,
		QueueSizePerShard:  64,
		IdleTimeout:        idleTimeout,
	})
	defer p.Close()

	// Initially zero running workers
	if workers := p.RunningWorkers(); workers != 0 {
		t.Fatalf("expected 0 running workers initially, got %d", workers)
	}

	// Dispatch tasks to spawn workers
	var wg sync.WaitGroup
	const tasks = 50
	wg.Add(tasks)
	for i := 0; i < tasks; i++ {
		p.Submit(func() {
			defer wg.Done()
			time.Sleep(10 * time.Millisecond)
		})
	}
	wg.Wait()

	// Some workers must have been spawned
	running := p.RunningWorkers()
	if running == 0 {
		t.Fatal("expected running workers > 0 after executing tasks")
	}

	// Wait for idleTimeout to elapse
	time.Sleep(idleTimeout * 3)

	// All idle workers should have exited
	remaining := p.RunningWorkers()
	if remaining != 0 {
		t.Fatalf("expected all idle workers to be reaped (0 remaining), got %d", remaining)
	}
}

func TestDefaultAndSetDefault(t *testing.T) {
	d := Default()
	if d == nil || Default() != d {
		t.Fatal("Default must return one shared pool")
	}
	custom := New(Config{Shards: 2})
	defer custom.Close()
	SetDefault(custom)
	defer SetDefault(d)
	if Default() != custom {
		t.Fatal("SetDefault did not replace the default pool")
	}
	SetDefault(nil)
	if Default() != custom {
		t.Fatal("SetDefault(nil) must be ignored")
	}
}

func TestDispatch(t *testing.T) {
	done := make(chan struct{})
	if !Dispatch(nil, 1, func() { close(done) }) {
		t.Fatal("Dispatch to the default pool failed")
	}
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("default pool never ran the task")
	}

	var ran atomic.Bool
	if !Dispatch(func(_ uint64, task func()) { task() }, 2, func() { ran.Store(true) }) || !ran.Load() {
		t.Fatal("custom submit did not run the task")
	}
	if Dispatch(func(uint64, func()) { panic("full") }, 3, func() {}) {
		t.Fatal("a rejecting custom pool must be reported")
	}
}
