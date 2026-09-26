package pool

import (
	"errors"
	"math/rand"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestRunsEveryTaskAndReclaimsWorkers(t *testing.T) {
	p := New(Config{MaxWorkers: 8, IdleTimeout: 100 * time.Millisecond})
	defer p.Close()

	const tasks = 200
	var executed atomic.Int64
	var wg sync.WaitGroup
	wg.Add(tasks)
	for i := range tasks {
		if err := p.SubmitConn(uint64(i%16), func() {
			defer wg.Done()
			executed.Add(1)
		}); err != nil {
			t.Fatal(err)
		}
	}
	wg.Wait()
	if executed.Load() != tasks {
		t.Fatalf("expected %d tasks, got %d", tasks, executed.Load())
	}

	time.Sleep(300 * time.Millisecond)
	if n := p.RunningWorkers(); n != 0 {
		t.Fatalf("expected 0 workers after the idle timeout, got %d", n)
	}
}

// Tasks beyond MaxWorkers wait for a worker instead of starting goroutines,
// and run as soon as one frees up.
func TestWorkersAreBounded(t *testing.T) {
	p := New(Config{MaxWorkers: 4, IdleTimeout: time.Second})
	defer p.Close()

	release := make(chan struct{})
	var started, peak atomic.Int32
	var wg sync.WaitGroup
	const tasks = 40
	wg.Add(tasks)
	for range tasks {
		_ = p.Submit(func() {
			defer wg.Done()
			n := started.Add(1)
			for {
				old := peak.Load()
				if n <= old || peak.CompareAndSwap(old, n) {
					break
				}
			}
			<-release
			started.Add(-1)
		})
	}
	time.Sleep(50 * time.Millisecond)
	if w := p.RunningWorkers(); w != 4 {
		t.Fatalf("running workers = %d, want 4", w)
	}
	close(release)
	wg.Wait()
	if peak.Load() > 4 {
		t.Fatalf("%d tasks ran at once, more than MaxWorkers", peak.Load())
	}
}

// A slow task does not hold up the ones submitted after it while other
// workers are free.
func TestSlowTaskDoesNotBlockOthers(t *testing.T) {
	p := New(Config{MaxWorkers: 4, IdleTimeout: time.Second})
	defer p.Close()

	heavyStarted := make(chan struct{})
	var heavyRunning atomic.Bool
	heavyRunning.Store(true)
	_ = p.Submit(func() {
		close(heavyStarted)
		time.Sleep(150 * time.Millisecond)
		heavyRunning.Store(false)
	})
	<-heavyStarted

	const fast = 10
	var wg sync.WaitGroup
	var whileHeavy atomic.Int64
	wg.Add(fast)
	start := time.Now()
	for range fast {
		_ = p.Submit(func() {
			defer wg.Done()
			if heavyRunning.Load() {
				whileHeavy.Add(1)
			}
		})
	}
	wg.Wait()
	if d := time.Since(start); d >= 120*time.Millisecond || whileHeavy.Load() != fast {
		t.Fatalf("fast tasks took %v, %d of %d ran beside the slow one", d, whileHeavy.Load(), fast)
	}
}

// A task submitted just as the only worker's idle timer fires must still run:
// the worker either takes it or leaves before it is chosen, never both.
func TestTaskNotStrandedByIdleExit(t *testing.T) {
	p := New(Config{MaxWorkers: 1, IdleTimeout: 2 * time.Millisecond})
	defer p.Close()
	deadline := time.Now().Add(2 * time.Second)
	for i := 0; time.Now().Before(deadline); i++ {
		done := make(chan struct{})
		time.Sleep(time.Duration(1000+rand.Intn(2000)) * time.Microsecond) // around the idle timer
		_ = p.Submit(func() { close(done) })
		select {
		case <-done:
		case <-time.After(time.Second):
			t.Fatalf("round %d: task stranded (workers=%d idle=%d)", i, p.RunningWorkers(), p.IdleWorkers())
		}
	}
}

// The shards split MaxWorkers between them, each getting minShardWorkers at
// least unless there is only one, and there are no more shards than cores
// (rounded up to a power of two).
func TestShardsSplitMaxWorkers(t *testing.T) {
	procs := runtime.GOMAXPROCS(0)
	for _, maxWorkers := range []int{1, 8, 255, 256, 1000, 4096, 1024 * procs, 100003} {
		p := New(Config{MaxWorkers: maxWorkers})
		n, total := len(p.shards), 0
		for i := range p.shards {
			s := &p.shards[i]
			total += s.max
			if n > 1 && s.max < minShardWorkers {
				t.Errorf("MaxWorkers %d: shard %d of %d gets %d workers", maxWorkers, i, n, s.max)
			}
		}
		if total != maxWorkers || n&(n-1) != 0 || n > 2*procs {
			t.Errorf("MaxWorkers %d: %d shards with %d workers in all", maxWorkers, n, total)
		}
		p.Close()
	}
}

// With every shard saturated the pool runs exactly MaxWorkers tasks at once,
// and every queued task runs once workers free up.
func TestShardedSaturation(t *testing.T) {
	const maxWorkers = 4 * minShardWorkers
	p := New(Config{MaxWorkers: maxWorkers, IdleTimeout: 100 * time.Millisecond})
	defer p.Close()
	release := make(chan struct{})
	var running, peak atomic.Int32
	var wg sync.WaitGroup
	const tasks = 3 * maxWorkers
	wg.Add(tasks)
	for i := range tasks {
		_ = p.SubmitConn(uint64(i), func() {
			defer wg.Done()
			n := running.Add(1)
			for {
				old := peak.Load()
				if n <= old || peak.CompareAndSwap(old, n) {
					break
				}
			}
			<-release
			running.Add(-1)
		})
	}
	deadline := time.Now().Add(5 * time.Second)
	for running.Load() < maxWorkers && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if w := p.RunningWorkers(); w != maxWorkers {
		t.Fatalf("running workers = %d, want %d", w, maxWorkers)
	}
	close(release)
	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("queued tasks stranded")
	}
	if peak.Load() > maxWorkers {
		t.Fatalf("%d tasks ran at once, more than MaxWorkers", peak.Load())
	}
	time.Sleep(300 * time.Millisecond)
	if n := p.RunningWorkers(); n != 0 {
		t.Fatalf("%d workers left after the idle timeout", n)
	}
}

// Bursts of short tasks are carried by a few busy workers that take the next
// task without parking, not by a goroutine per task: each worker holds a
// stack, and a pool that grows to its backlog costs memory and locality. With
// one P every burst is queued before a worker runs, so that is deterministic.
func TestBurstsOfShortTasksNeedFewWorkers(t *testing.T) {
	defer runtime.GOMAXPROCS(runtime.GOMAXPROCS(1))
	p := New(Config{MaxWorkers: 400, IdleTimeout: time.Minute}) // one shard
	defer p.Close()
	const size = 200
	var wg sync.WaitGroup
	for range 10 {
		wg.Add(size)
		for range size {
			_ = p.Submit(wg.Done)
		}
		wg.Wait()
	}
	if n := p.RunningWorkers(); n > 4 {
		t.Fatalf("%d workers for bursts of %d short tasks", n, size)
	}
}

// A worker about to park takes a task another shard has waiting, so a shard
// whose workers are all busy does not hold its queue while others idle.
func TestIdleWorkerTakesTaskFromBusyShard(t *testing.T) {
	defer runtime.GOMAXPROCS(runtime.GOMAXPROCS(4))
	p := New(Config{MaxWorkers: 2 * minShardWorkers, IdleTimeout: time.Minute})
	defer p.Close()
	if len(p.shards) != 2 {
		t.Fatalf("%d shards, want 2", len(p.shards))
	}
	// A task waits in shard 1 with no worker coming for it, as it would
	// behind busy workers.
	ran := make(chan struct{})
	s := &p.shards[1]
	s.mu.Lock()
	s.queue.push(func() { close(ran) })
	s.mu.Unlock()
	var id uint64
	for id*0x9e3779b97f4a7c15>>p.shift != 0 {
		id++
	}
	_ = p.SubmitConn(id, func() {}) // shard 0's worker looks around before parking
	select {
	case <-ran:
	case <-time.After(5 * time.Second):
		t.Fatal("the task waiting in a busy shard was left there")
	}
}

func TestConcurrentCloseAndSubmit(t *testing.T) {
	p := New(Config{MaxWorkers: 32, IdleTimeout: 100 * time.Millisecond})
	var ran, refused atomic.Int64
	var wg sync.WaitGroup
	for i := range 50 {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for range 50 {
				err := p.SubmitConn(uint64(id), func() {
					time.Sleep(time.Microsecond)
					ran.Add(1)
				})
				if err != nil {
					if !errors.Is(err, ErrClosed) {
						t.Errorf("Submit: %v", err)
					}
					refused.Add(1)
				}
			}
		}(i)
	}
	time.Sleep(time.Millisecond)
	p.Close()
	wg.Wait()
	if ran.Load()+refused.Load() != 50*50 {
		t.Fatalf("ran %d + refused %d, want every task accounted for", ran.Load(), refused.Load())
	}
	if n := p.RunningWorkers(); n != 0 {
		t.Fatalf("%d workers left after Close", n)
	}
}

func TestPanicIsContained(t *testing.T) {
	p := New(Config{MaxWorkers: 2})
	defer p.Close()
	done := make(chan struct{})
	_ = p.Submit(func() { panic("simulated business handler panic") })
	_ = p.Submit(func() { close(done) })
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("pool stopped running tasks after a panic")
	}
}

func TestAdapt(t *testing.T) {
	var count atomic.Int64
	plain := func(task func()) error {
		count.Add(1)
		task()
		return nil
	}
	var executed atomic.Bool
	if err := Adapt(plain)(12345, func() { executed.Store(true) }); err != nil || count.Load() != 1 || !executed.Load() {
		t.Fatalf("adapted submit: err=%v count=%d executed=%v", err, count.Load(), executed.Load())
	}
	if Adapt(nil) != nil {
		t.Fatal("expected nil for nil submit function")
	}
}

func TestDefaultAndSetDefault(t *testing.T) {
	d := Default()
	if d == nil || Default() != d {
		t.Fatal("Default must return one shared pool")
	}
	custom := New(Config{MaxWorkers: 2})
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
	if !Dispatch(func(_ uint64, task func()) error { task(); return nil }, 2, func() { ran.Store(true) }) || !ran.Load() {
		t.Fatal("custom submit did not run the task")
	}
	if Dispatch(func(uint64, func()) error { return errors.New("full") }, 3, func() {}) {
		t.Fatal("a refusing custom pool must be reported")
	}
	if Dispatch(func(uint64, func()) error { panic("broken pool") }, 4, func() {}) {
		t.Fatal("a panicking custom pool must be reported as refusing")
	}
}

func BenchmarkSubmit(b *testing.B) {
	p := New(Config{})
	defer p.Close()
	var wg sync.WaitGroup
	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			wg.Add(1)
			_ = p.Submit(wg.Done)
		}
	})
	wg.Wait()
}
