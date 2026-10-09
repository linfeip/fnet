package taskpool

import (
	"fmt"
	"io"
	"log/slog"
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
		t.Fatal("任务未全部执行")
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
			t.Fatalf("任务 %d 执行了 %d 次", i, n)
		}
	}
	for i := range p.shards {
		if n := p.shards[i].liveWorkers.Load(); n > 4 {
			t.Fatalf("分片 %d 启动了 %d 个 worker，超过上限", i, n)
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
			t.Fatalf("key %d 的任务没有执行", key)
		}
	}
	for i := range p.shards {
		want := int32(0)
		if i == 1 {
			want = 1
		}
		if n := p.shards[i].liveWorkers.Load(); n != want {
			t.Fatalf("分片 %d 启动了 %d 个 worker, 期望 %d", i, n, want)
		}
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
			t.Fatalf("第 %d 个任务未执行", i)
		}
		if i%100 == 0 {
			time.Sleep(time.Millisecond) // let the worker actually suspend
		}
	}
}

// TestFullQueue checks that when the only worker is blocked and the queue is full, a new task is run by a
// temporary goroutine, which also takes along the tasks queued up behind it.
func TestFullQueue(t *testing.T) {
	release := make(chan struct{})
	started := make(chan struct{})
	p := New(1, 1, 2)
	p.Submit(func() {
		close(started)
		<-release
	})
	<-started
	const tasks = 100
	var wg sync.WaitGroup
	wg.Add(tasks)
	for range tasks {
		p.Submit(wg.Done)
	}
	waitGroup(t, &wg) // the worker is still blocked
	close(release)
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
		t.Fatalf("每次投递分配 %v 次", allocs)
	}
	waitGroup(t, &wg)
}

// TestSubmitBatchEveryTaskRunsOnce 覆盖并发批量提交和溢出，每个任务只执行一次。
func TestSubmitBatchEveryTaskRunsOnce(t *testing.T) {
	const submitters, batches, batchSize = 8, 100, 32
	p := New(4, 4, 64)
	runs := make([]atomic.Int32, submitters*batches*batchSize)
	var wg sync.WaitGroup
	wg.Add(len(runs))
	for producer := range submitters {
		go func() {
			tasks := make([]func(), batchSize)
			for batch := range batches {
				for i := range tasks {
					index := (producer*batches+batch)*batchSize + i
					tasks[i] = func() { runs[index].Add(1); wg.Done() }
				}
				p.SubmitBatch(tasks)
				clear(tasks) // 提交后不能继续引用调用者的切片。
			}
		}()
	}
	waitGroup(t, &wg)
	for i := range runs {
		if n := runs[i].Load(); n != 1 {
			t.Fatalf("任务 %d 执行了 %d 次", i, n)
		}
	}
}

// TestSubmitBatchNoAlloc 验证预热后提交批次不产生临时切片或包装对象。
func TestSubmitBatchNoAlloc(t *testing.T) {
	p := New(4, 1, 8192)
	var wg sync.WaitGroup
	tasks := make([]func(), 128)
	for i := range tasks {
		tasks[i] = wg.Done
	}
	wg.Add(len(tasks))
	p.SubmitBatch(tasks)
	waitGroup(t, &wg)
	wg.Add(101 * len(tasks))
	if allocs := testing.AllocsPerRun(100, func() { p.SubmitBatch(tasks) }); allocs != 0 {
		t.Fatalf("每批提交分配 %v 次", allocs)
	}
	waitGroup(t, &wg)
}

// TestBlockedCallbacksUpToLimit 验证同时阻塞的回调数只受 分片数 × maxWorkers 限制：每个回调都占着一个 worker，
// 全部必须同时开始运行，而不是排队等其中某一个返回。
func TestBlockedCallbacksUpToLimit(t *testing.T) {
	p := New(2, 16, 1024)
	const blocked = 32 // 随机分到分片后某个分片超过 16 个时，借别的分片的余量
	release := make(chan struct{})
	defer close(release)
	var started sync.WaitGroup
	started.Add(blocked)
	for range blocked {
		p.Submit(func() { started.Done(); <-release })
	}
	waitGroup(t, &started)
}

// TestBlockedWorkerIsolation 队列未满时，阻塞回调后面的任务也必须由新的 worker 执行。
func TestBlockedWorkerIsolation(t *testing.T) {
	p := New(1, 2, 1024)
	release, started := make(chan struct{}), make(chan struct{})
	defer close(release)
	p.Submit(func() { close(started); <-release })
	<-started
	var wg sync.WaitGroup
	wg.Add(32)
	tasks := make([]func(), 32)
	for i := range tasks {
		tasks[i] = wg.Done
	}
	p.SubmitBatch(tasks)
	waitGroup(t, &wg)
	if workers := p.shards[0].liveWorkers.Load(); workers != 2 {
		t.Fatalf("阻塞隔离启动 %d 个 worker，期望 2", workers)
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

// waitQuiescent waits until every worker is suspended and every shard's wakingWorkers is back to 0; otherwise a wakeup
// or a suspension was leaked.
func waitQuiescent(t *testing.T, p *Pool) {
	t.Helper()
	waitUntil(t, func() bool {
		for i := range p.shards {
			s := &p.shards[i]
			idle := 0
			for w := range s.idleWorkers {
				idle += bits.OnesCount64(s.idleWorkers[w].Load())
			}
			if s.wakingWorkers.Load() != 0 || idle != int(s.liveWorkers.Load()) {
				return false
			}
		}
		return true
	}, "唤醒权或挂起位泄漏")
}

// TestBorrowIdleWorker 分片 0 的 worker 已达上限且在阻塞，分片 1 有挂起的 worker：分片 0 的任务由它来执行。
func TestBorrowIdleWorker(t *testing.T) {
	p := New(2, 1, 64)
	var wg sync.WaitGroup
	wg.Add(1)
	submitTo(p, 1, wg.Done)
	waitGroup(t, &wg)
	waitUntil(t, func() bool { return p.shards[1].idleWorkers[0].Load() != 0 }, "分片 1 的 worker 没有挂起")
	release, started := make(chan struct{}), make(chan struct{})
	submitTo(p, 0, func() { close(started); <-release })
	<-started
	wg.Add(1)
	submitTo(p, 0, wg.Done)
	waitGroup(t, &wg)
	if a, b := p.shards[0].liveWorkers.Load(), p.shards[1].liveWorkers.Load(); a != 1 || b != 1 {
		t.Fatalf("liveWorkers = %d, %d，期望 1, 1", a, b)
	}
	close(release)
	waitQuiescent(t, p)
}

// TestStartWorkerElsewhere 分片 0 已达上限且在阻塞，别处没有挂起的 worker，但分片 1 还有余量：在分片 1 新建一个。
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
		t.Fatalf("分片 1 liveWorkers = %d，期望 1", n)
	}
	close(release)
	waitQuiescent(t, p)
}

// TestRescueStarvingShard 整个池的 worker 都在阻塞时任务排队并置 starving；分片 1 的 worker 一空出来就接手分片 0
// 的任务，而分片 0 自己的 worker 仍在阻塞。
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
		t.Fatal("整个池没有可用 worker 时没有置 starving")
	}
	time.Sleep(10 * time.Millisecond)
	if ran.Load() {
		t.Fatal("没有可用 worker 时任务却执行了")
	}
	close(releaseOther)
	waitGroup(t, &wg)
}

// TestBlockedCallbacksUpToPoolLimit 阻塞任务全部提交到一个分片，也能用满 分片数 × maxWorkers 个 worker。
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

// TestSaturatedStress 在 worker 经常用满时混合提交短任务、阻塞任务和调用 Goexit 的任务（配合 -race）：每个任务
// 恰好执行一次，每个分片的 worker 不超过上限，最后唤醒权和挂起位都没有泄漏。小队列下溢出也频繁发生，大队列下任务
// 留在队列里，跨分片借用和 starving 的路径用得最多。
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
			t.Fatalf("任务 %d 执行了 %d 次", i, n)
		}
	}
	for i := range p.shards {
		if n := p.shards[i].liveWorkers.Load(); n > 2 {
			t.Fatalf("分片 %d 启动了 %d 个 worker，超过上限", i, n)
		}
	}
	waitQuiescent(t, p)
}

func TestShardCacheLineSeparation(t *testing.T) {
	var s shard
	if unsafe.Offsetof(s.head)-unsafe.Offsetof(s.tail) < cacheLineSize ||
		unsafe.Offsetof(s.wakingWorkers)-unsafe.Offsetof(s.head) < cacheLineSize ||
		unsafe.Sizeof(s)+unsafe.Offsetof(s.tail)-(unsafe.Offsetof(s.idleWorkers)+unsafe.Sizeof(s.idleWorkers)) < cacheLineSize {
		t.Fatal("队列头尾或相邻分片的 worker 状态未隔离缓存行")
	}
	var p Pool
	if unsafe.Offsetof(p.starving)-unsafe.Offsetof(p.shards) < cacheLineSize {
		t.Fatal("starving 与每次 Submit 都读的 shards 在同一缓存行")
	}
}

// BenchmarkSubmitRounds 模拟 readiness 回合，分别比较随机提交、均匀单个提交和均匀批量提交。
func BenchmarkSubmitRounds(b *testing.B) {
	for _, size := range []int{1, 32, 256} {
		for _, mode := range []string{"random", "round_robin", "batch"} {
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
					case "batch":
						p.SubmitBatch(tasks)
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
