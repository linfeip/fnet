package taskpool

import (
	"fmt"
	"io"
	"log/slog"
	"math"
	"math/bits"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"
	"unsafe"
)

// waitGroup waits for wg to finish and fails on timeout (which means a task was lost or got stuck).
func waitGroup(t *testing.T, wg *sync.WaitGroup) {
	t.Helper()
	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("not all tasks ran")
	}
}

// TestEveryTaskRunsOnce submits from several submitters concurrently: every task runs exactly once and the
// number of workers does not exceed the limit.
func TestEveryTaskRunsOnce(t *testing.T) {
	const submitters, tasksPerSubmitter = 8, 20000
	runs := make([]atomic.Int32, submitters*tasksPerSubmitter)
	var wg sync.WaitGroup
	wg.Add(len(runs))
	p := New(4, 4, 64)
	for s := range submitters {
		go func() {
			for i := range tasksPerSubmitter {
				p.Submit(func() {
					runs[s*tasksPerSubmitter+i].Add(1)
					wg.Done()
				})
			}
		}()
	}
	waitGroup(t, &wg)
	for i := range runs {
		if n := runs[i].Load(); n != 1 {
			t.Fatalf("task %d ran %d times", i, n)
		}
	}
	for i := range p.shards {
		if n := p.shards[i].liveWorkers.Load(); n > 4 {
			t.Fatalf("shard %d started %d workers, over the limit", i, n)
		}
	}
}

// TestSubmitToKeyShard checks that SubmitTo runs the tasks of a key on the shard the key selects modulo the number of
// shards, negative keys included: submitted one at a time, so that the shard never has to borrow, they start workers in
// that shard only.
func TestSubmitToKeyShard(t *testing.T) {
	p := New(4, 1, 64)
	for _, key := range []int{1, 5, -3, 1<<40 + 1} { // all select shard 1
		done := make(chan struct{})
		p.SubmitTo(key, func() { close(done) })
		select {
		case <-done:
		case <-time.After(10 * time.Second):
			t.Fatalf("task for key %d did not run", key)
		}
	}
	for i := range p.shards {
		want := int32(0)
		if i == 1 {
			want = 1
		}
		if n := p.shards[i].liveWorkers.Load(); n != want {
			t.Fatalf("shard %d started %d workers, want %d", i, n, want)
		}
	}
}

// TestRandomKey checks that the keys stay in range and, taken modulo the number of shards, reach every shard.
func TestRandomKey(t *testing.T) {
	const shards = 4
	reached := make(map[int]bool)
	for range 1000 {
		key := RandomKey()
		if key < 0 || key >= math.MaxInt32 {
			t.Fatalf("key %d is out of range", key)
		}
		reached[key%shards] = true
	}
	if len(reached) != shards {
		t.Fatalf("the keys reached %d of %d shards", len(reached), shards)
	}
}

// TestWakeAfterIdle submits only after the worker has suspended every time: no wakeup may be lost.
func TestWakeAfterIdle(t *testing.T) {
	done := make(chan struct{})
	p := New(2, 2, 8)
	for i := range 2000 {
		p.Submit(func() { done <- struct{}{} })
		select {
		case <-done:
		case <-time.After(10 * time.Second):
			t.Fatalf("task %d did not run", i)
		}
		if i%100 == 0 {
			time.Sleep(time.Millisecond) // let the worker actually suspend
		}
	}
}

// TestFullQueue checks that when the only worker is blocked and the queue is full, a new task is run by a
// temporary goroutine, which also takes along the tasks queued up behind it. The last ones queued, as many as the queue
// holds, may wait for the worker once the temporary goroutines are done.
func TestFullQueue(t *testing.T) {
	release := make(chan struct{})
	started := make(chan struct{})
	const capacity = 2
	p := New(1, 1, capacity)
	p.Submit(func() {
		close(started)
		<-release
	})
	<-started
	const tasks = 100
	var ran atomic.Int32
	for range tasks {
		p.Submit(func() { ran.Add(1) })
	}
	for deadline := time.Now().Add(10 * time.Second); ran.Load() < tasks-capacity; time.Sleep(time.Millisecond) {
		if time.Now().After(deadline) {
			t.Fatalf("%d of %d tasks ran while the worker was blocked", ran.Load(), tasks)
		}
	}
	close(release)
	for deadline := time.Now().Add(10 * time.Second); ran.Load() < tasks; time.Sleep(time.Millisecond) {
		if time.Now().After(deadline) {
			t.Fatalf("%d of %d tasks ran once the worker was released", ran.Load(), tasks)
		}
	}
}

// TestPanicTask verifies a panicking task is logged and the worker goes on running the tasks after it.
func TestPanicTask(t *testing.T) {
	defer slog.SetDefault(slog.Default())
	slog.SetDefault(slog.New(slog.NewTextHandler(io.Discard, nil)))

	p := New(1, 1, 8)
	var wg sync.WaitGroup
	wg.Add(1)
	p.Submit(func() { panic("boom") })
	p.Submit(wg.Done)
	waitGroup(t, &wg)
}

// TestGoexitTask verifies that a task calling runtime.Goexit ends the worker running it, that the pool
// replaces it with a new worker, and that later tasks run as usual.
func TestGoexitTask(t *testing.T) {
	p := New(1, 1, 8)
	for range 3 {
		p.Submit(runtime.Goexit)
	}
	var wg sync.WaitGroup
	wg.Add(1)
	p.Submit(wg.Done)
	waitGroup(t, &wg)
	if n := p.shards[0].liveWorkers.Load(); n != 1 {
		t.Fatalf("liveWorkers = %d, want 1", n)
	}
}

// waitGoroutines waits until no more goroutines are running than before, which is when the workers a pool started are all
// gone.
func waitGoroutines(t *testing.T, before int) {
	t.Helper()
	waitUntil(t, func() bool { return runtime.NumGoroutine() <= before }, "the workers of the closed pool did not exit")
}

// TestClose checks that closing the pool ends its workers, the suspended ones and one that is running a task, and that a
// task submitted afterwards, alone or in a batch, still runs.
func TestClose(t *testing.T) {
	before := runtime.NumGoroutine()
	p := New(4, 8, 64)
	var wg sync.WaitGroup
	wg.Add(200)
	for range 200 {
		p.Submit(wg.Done)
	}
	waitGroup(t, &wg)
	waitQuiescent(t, p)

	release, running := make(chan struct{}), make(chan struct{})
	p.Submit(func() {
		close(running)
		<-release
	})
	<-running
	p.Close()
	p.Close() // may be repeated

	wg.Add(3)
	for range 3 {
		p.Submit(wg.Done)
	}
	waitGroup(t, &wg)
	close(release) // the worker that was running a task leaves as soon as it is done
	waitGoroutines(t, before)
}

// TestCloseReplacedWorker checks that the replacement of a worker whose task called runtime.Goexit leaves with the others.
func TestCloseReplacedWorker(t *testing.T) {
	before := runtime.NumGoroutine()
	p := New(1, 2, 8)
	for range 3 {
		p.Submit(runtime.Goexit)
	}
	var wg sync.WaitGroup
	wg.Add(1)
	p.Submit(wg.Done)
	waitGroup(t, &wg)
	p.Close()
	waitGoroutines(t, before)
}

// TestCloseStraggler checks the task of a submitter that saw the pool open before it closed, and queues it only after the
// workers have left, with every slot of the shard used up: no worker could be found for it, nor the next to suspend, so a
// goroutine of its own runs it. submitTo queues it the way Submit does once it has found the pool open.
func TestCloseStraggler(t *testing.T) {
	before := runtime.NumGoroutine()
	p := New(1, 1, 8)
	var wg sync.WaitGroup
	wg.Add(1)
	p.Submit(wg.Done)
	waitGroup(t, &wg)
	p.Close()
	waitGoroutines(t, before)

	wg.Add(1)
	submitTo(p, 0, wg.Done)
	waitGroup(t, &wg)
}

// TestCloseWhileSubmitting closes the pool while submitters are at work: every task still runs exactly once, whether it
// was queued for a worker before the close or is run by a goroutine of its own after it, and no worker is left.
func TestCloseWhileSubmitting(t *testing.T) {
	const submitters, tasksPerSubmitter = 8, 5000
	for range 20 {
		before := runtime.NumGoroutine()
		runs := make([]atomic.Int32, submitters*tasksPerSubmitter)
		var wg sync.WaitGroup
		wg.Add(len(runs))
		p := New(4, 2, 64)
		for s := range submitters {
			go func() {
				for i := range tasksPerSubmitter {
					p.Submit(func() {
						runs[s*tasksPerSubmitter+i].Add(1)
						wg.Done()
					})
					if s == 0 && i == tasksPerSubmitter/2 {
						p.Close()
					}
				}
			}()
		}
		waitGroup(t, &wg)
		for i := range runs {
			if n := runs[i].Load(); n != 1 {
				t.Fatalf("task %d ran %d times", i, n)
			}
		}
		waitGoroutines(t, before)
	}
}

// TestSubmitNoAlloc verifies Submit itself does not allocate (once the worker has been started).
func TestSubmitNoAlloc(t *testing.T) {
	p := New(1, 1, 1024)
	var wg sync.WaitGroup
	task := wg.Done // take the method value just once, here
	wg.Add(1)
	p.Submit(task) // start the worker
	waitGroup(t, &wg)
	wg.Add(101) // AllocsPerRun does one warm-up run before the 100 measured ones
	if allocs := testing.AllocsPerRun(100, func() { p.Submit(task) }); allocs != 0 {
		t.Fatalf("Submit allocated %v times per call", allocs)
	}
	waitGroup(t, &wg)
}

// TestBlockedCallbacksUpToLimit verifies that the number of callbacks blocking at the same time is limited only by
// shards * maxWorkers: each callback holds one worker, and all of them must start running at the same time instead of
// queuing behind one of them returning.
func TestBlockedCallbacksUpToLimit(t *testing.T) {
	p := New(2, 16, 1024)
	const blocked = 32 // after random shard placement one shard exceeds 16, so it borrows room from another shard
	release := make(chan struct{})
	defer close(release)
	var started sync.WaitGroup
	started.Add(blocked)
	for range blocked {
		p.Submit(func() { started.Done(); <-release })
	}
	waitGroup(t, &started)
}

// TestBlockedWorkerIsolation: while the queue is not full, tasks behind a blocking callback must also be run by a new
// worker.
func TestBlockedWorkerIsolation(t *testing.T) {
	p := New(1, 2, 1024)
	release, started := make(chan struct{}), make(chan struct{})
	defer close(release)
	p.Submit(func() { close(started); <-release })
	<-started
	var wg sync.WaitGroup
	wg.Add(32)
	for range 32 {
		p.Submit(wg.Done)
	}
	waitGroup(t, &wg)
	if workers := p.shards[0].liveWorkers.Load(); workers != 2 {
		t.Fatalf("blocking isolation started %d workers, want 2", workers)
	}
}

// submitTo submits task to the shard with index index, the way Submit does to the shard it picks.
func submitTo(p *Pool, index int, task func()) {
	s := &p.shards[index]
	if !s.push(task) {
		go s.help(task)
		return
	}
	s.notify()
}

// waitUntil polls condition until it holds, and fails with message after 10 seconds.
func waitUntil(t *testing.T, condition func() bool, message string) {
	t.Helper()
	for deadline := time.Now().Add(10 * time.Second); !condition(); time.Sleep(time.Millisecond) {
		if time.Now().After(deadline) {
			t.Fatal(message)
		}
	}
}

// waitQuiescent waits until every worker is suspended, the stall monitor has exited, and every shard's wakingWorkers
// and servingWorkers are back to 0; otherwise a wakeup or a suspension was leaked.
func waitQuiescent(t *testing.T, p *Pool) {
	t.Helper()
	waitUntil(t, func() bool {
		if p.monitoring.Load() {
			return false
		}
		for i := range p.shards {
			s := &p.shards[i]
			idle := 0
			for w := range s.idleWorkers {
				idle += bits.OnesCount64(s.idleWorkers[w].Load())
			}
			if s.wakingWorkers.Load() != 0 || s.servingWorkers.Load() != 0 || idle != int(s.liveWorkers.Load()) {
				return false
			}
		}
		return true
	}, "wakeup rights or suspension bits leaked")
}

// TestBorrowIdleWorker: shard 0 is at its worker limit and blocking, while shard 1 has a suspended worker; that worker
// runs shard 0's task.
func TestBorrowIdleWorker(t *testing.T) {
	p := New(2, 1, 64)
	var wg sync.WaitGroup
	wg.Add(1)
	submitTo(p, 1, wg.Done)
	waitGroup(t, &wg)
	waitUntil(t, func() bool { return p.shards[1].idleWorkers[0].Load() != 0 }, "shard 1's worker did not suspend")
	release, started := make(chan struct{}), make(chan struct{})
	submitTo(p, 0, func() { close(started); <-release })
	<-started
	wg.Add(1)
	submitTo(p, 0, wg.Done)
	waitGroup(t, &wg)
	if a, b := p.shards[0].liveWorkers.Load(), p.shards[1].liveWorkers.Load(); a != 1 || b != 1 {
		t.Fatalf("liveWorkers = %d, %d, want 1, 1", a, b)
	}
	close(release)
	waitQuiescent(t, p)
}

// TestStartWorkerElsewhere: shard 0 is at its limit and blocking, no worker is suspended elsewhere, but shard 1 still
// has room, so a new worker is started there.
func TestStartWorkerElsewhere(t *testing.T) {
	p := New(2, 1, 64)
	release, started := make(chan struct{}), make(chan struct{})
	submitTo(p, 0, func() { close(started); <-release })
	<-started
	var wg sync.WaitGroup
	wg.Add(1)
	submitTo(p, 0, wg.Done)
	waitGroup(t, &wg)
	if n := p.shards[1].liveWorkers.Load(); n != 1 {
		t.Fatalf("shard 1 liveWorkers = %d, want 1", n)
	}
	close(release)
	waitQuiescent(t, p)
}

// TestRescueStarvingShard: when every worker in the pool is blocking, a task is queued and starving is set; as soon as
// shard 1's worker frees up it takes over shard 0's task while shard 0's own worker is still blocking.
func TestRescueStarvingShard(t *testing.T) {
	p := New(2, 1, 64)
	releaseOwn, releaseOther := make(chan struct{}), make(chan struct{})
	defer close(releaseOwn)
	var started, wg sync.WaitGroup
	started.Add(2)
	submitTo(p, 0, func() { started.Done(); <-releaseOwn })
	submitTo(p, 1, func() { started.Done(); <-releaseOther })
	waitGroup(t, &started)
	var ran atomic.Bool
	wg.Add(1)
	submitTo(p, 0, func() { ran.Store(true); wg.Done() })
	if !p.starving.Load() {
		t.Fatal("starving was not set when the whole pool had no available worker")
	}
	time.Sleep(10 * time.Millisecond)
	if ran.Load() {
		t.Fatal("the task ran even though no worker was available")
	}
	close(releaseOther)
	waitGroup(t, &wg)
}

// TestBlockedCallbacksUpToPoolLimit: even when all blocking tasks are submitted to one shard, the pool can still use
// shards * maxWorkers workers.
func TestBlockedCallbacksUpToPoolLimit(t *testing.T) {
	p := New(4, 4, 1024)
	release := make(chan struct{})
	var started sync.WaitGroup
	started.Add(16)
	for range 16 {
		submitTo(p, 0, func() { started.Done(); <-release })
	}
	waitGroup(t, &started)
	close(release)
	waitQuiescent(t, p)
}

// TestSaturatedStress mixes short tasks, blocking tasks, and tasks calling Goexit (run with -race) while the workers
// are often saturated: every task runs exactly once, no shard exceeds its worker limit, and no wakeup right or
// suspension bit leaks. A small queue makes overflow frequent as well; a large queue keeps tasks in the queue and
// exercises cross-shard borrowing and starving the most.
func TestSaturatedStress(t *testing.T) {
	for _, capacity := range []int{16, 1 << 16} {
		t.Run(fmt.Sprintf("capacity=%d", capacity), func(t *testing.T) { saturatedStress(t, capacity) })
	}
}

func saturatedStress(t *testing.T, capacity int) {
	const submitters, tasksPerSubmitter = 8, 5000
	p := New(4, 2, capacity)
	runs := make([]atomic.Int32, submitters*tasksPerSubmitter)
	var wg sync.WaitGroup
	wg.Add(len(runs))
	for submitter := range submitters {
		go func() {
			for i := range tasksPerSubmitter {
				index := submitter*tasksPerSubmitter + i
				switch {
				case i%64 == 0:
					p.Submit(func() { defer wg.Done(); runs[index].Add(1); runtime.Goexit() })
				case i%8 == 0:
					p.Submit(func() { runs[index].Add(1); time.Sleep(100 * time.Microsecond); wg.Done() })
				default:
					p.Submit(func() { runs[index].Add(1); wg.Done() })
				}
			}
		}()
	}
	waitGroup(t, &wg)
	for i := range runs {
		if n := runs[i].Load(); n != 1 {
			t.Fatalf("task %d ran %d times", i, n)
		}
	}
	for i := range p.shards {
		if n := p.shards[i].liveWorkers.Load(); n > 2 {
			t.Fatalf("shard %d started %d workers, over the limit", i, n)
		}
	}
	waitQuiescent(t, p)
}

// TestSoftLimitHoldsShortBursts verifies that a burst of fast non-blocking tasks stays within softWorkers instead of
// waking up to maxWorkers goroutines, and that once blocking tasks stall the shard the monitor scales workers up and
// subsequent fast bursts stay bounded by softWorkers again.
func TestSoftLimitHoldsShortBursts(t *testing.T) {
	p := New(1, 32, 1024)
	defer p.Close()

	var wg sync.WaitGroup
	wg.Add(500)
	for range 500 {
		submitTo(p, 0, wg.Done)
	}
	waitGroup(t, &wg)
	if live := p.shards[0].liveWorkers.Load(); live > softWorkers {
		t.Fatalf("fast burst started %d workers, want at most %d", live, softWorkers)
	}

	// Now block 16 tasks at once: the monitor scales past softWorkers so all 16 run concurrently.
	release := make(chan struct{})
	var started sync.WaitGroup
	started.Add(16)
	for range 16 {
		submitTo(p, 0, func() { started.Done(); <-release })
	}
	waitGroup(t, &started)
	close(release)
	waitQuiescent(t, p)

	// With 16+ resident idle workers, another fast burst still wakes at most softWorkers at a time.
	var concurrent, peak atomic.Int32
	wg.Add(500)
	for range 500 {
		submitTo(p, 0, func() {
			defer wg.Done()
			if cur := concurrent.Add(1); cur > peak.Load() {
				for {
					old := peak.Load()
					if cur <= old || peak.CompareAndSwap(old, cur) {
						break
					}
				}
			}
			concurrent.Add(-1)
		})
	}
	waitGroup(t, &wg)
	if got := peak.Load(); got > softWorkers {
		t.Fatalf("after scaling, fast burst ran %d workers concurrently, want at most %d", got, softWorkers)
	}
	waitQuiescent(t, p)
}

func TestShardCacheLineSeparation(t *testing.T) {
	var s shard
	if unsafe.Offsetof(s.head)-unsafe.Offsetof(s.tail) < cacheLineSize ||
		unsafe.Offsetof(s.wakingWorkers)-unsafe.Offsetof(s.head) < cacheLineSize ||
		unsafe.Sizeof(s)+unsafe.Offsetof(s.tail)-(unsafe.Offsetof(s.idleWorkers)+unsafe.Sizeof(s.idleWorkers)) < cacheLineSize {
		t.Fatal("the queue head/tail or the worker state of adjacent shards is not isolated on its own cache line")
	}
	var p Pool
	if unsafe.Offsetof(p.starving)-unsafe.Offsetof(p.closed) < cacheLineSize {
		t.Fatal("starving shares a cache line with shards, which every Submit reads")
	}
}

// BenchmarkSubmitRounds simulates readiness rounds and compares random submission with even individual submission.
func BenchmarkSubmitRounds(b *testing.B) {
	for _, size := range []int{1, 32, 256} {
		for _, mode := range []string{"random", "round_robin"} {
			b.Run(fmt.Sprintf("size=%d/mode=%s", size, mode), func(b *testing.B) {
				p := New(runtime.GOMAXPROCS(0), 4, 8192)
				var wg sync.WaitGroup
				tasks := make([]func(), size)
				for i := range tasks {
					tasks[i] = wg.Done
				}
				b.ReportAllocs()
				b.ResetTimer()
				for range b.N {
					wg.Add(len(tasks))
					switch mode {
					case "round_robin":
						for i, task := range tasks {
							s := &p.shards[i%len(p.shards)]
							if !s.push(task) {
								go s.help(task)
							}
							s.notify()
						}
					default:
						for _, task := range tasks {
							p.Submit(task)
						}
					}
					wg.Wait()
				}
			})
		}
	}
}
