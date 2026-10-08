package taskpool

import (
	"fmt"
	"io"
	"log/slog"
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

// TestStealFromBusyShard checks that a task queued behind a shard's blocked worker is run by the worker of another
// shard instead of waiting for the blocked one.
func TestStealFromBusyShard(t *testing.T) {
	p := New(2, 1, 16)
	release, started := make(chan struct{}), make(chan struct{})
	defer close(release)
	busy, other := &p.shards[0], &p.shards[1]
	busy.push(func() { close(started); <-release })
	busy.notify(false)
	<-started
	done := make(chan struct{})
	busy.push(func() { close(done) })
	other.push(func() {})
	other.notify(false)
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("忙分片排队的任务没有被其他分片的 worker 取走")
	}
}

// TestOverflowTailTasks 验证临时协助退出后、resident worker 仍阻塞时，新入队的尾部任务也能执行。
func TestOverflowTailTasks(t *testing.T) {
	p := New(1, 1, 2)
	release, started := make(chan struct{}), make(chan struct{})
	defer close(release)
	p.Submit(func() { close(started); <-release })
	<-started
	var wg sync.WaitGroup
	wg.Add(3)
	p.SubmitBatch([]func(){wg.Done, wg.Done, wg.Done})
	waitGroup(t, &wg)
	// 确认上一轮已完成，再提交不足以填满队列的一个任务。
	wg.Add(1)
	p.Submit(wg.Done)
	waitGroup(t, &wg)
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
	deadline := time.Now().Add(time.Second)
	for p.runningWorkers.Load() != 0 && time.Now().Before(deadline) {
		runtime.Gosched()
	}
	if count := p.runningWorkers.Load(); count != 0 {
		t.Fatalf("Goexit 替换后泄漏 %d 个运行名额", count)
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

func TestWorkerBudget(t *testing.T) {
	p := New(2, 4, 16)
	for range p.targetWorkers {
		if !p.reserveWorker(false) {
			t.Fatal("预算尚未耗尽却拒绝预留")
		}
	}
	if p.reserveWorker(false) {
		t.Fatal("普通预留超过运行预算")
	}
	if !p.reserveWorker(true) || p.runningWorkers.Load() != p.targetWorkers+1 {
		t.Fatal("首个 worker 或阻塞隔离不能越过软预算")
	}
}

// TestBlockedWorkerIsolation 预算耗尽且队列未满时，也必须让阻塞回调后面的任务执行。
func TestBlockedWorkerIsolation(t *testing.T) {
	p := New(1, 2, 1024)
	p.targetWorkers = 1
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

func TestWorkerBudgetReleasedAfterIdle(t *testing.T) {
	p := New(4, 4, 1024)
	var wg sync.WaitGroup
	tasks := make([]func(), 256)
	for i := range tasks {
		tasks[i] = wg.Done
	}
	for range 10 {
		wg.Add(len(tasks))
		p.SubmitBatch(tasks)
		waitGroup(t, &wg)
		deadline := time.Now().Add(time.Second)
		for p.runningWorkers.Load() != 0 && time.Now().Before(deadline) {
			runtime.Gosched()
		}
		if count := p.runningWorkers.Load(); count != 0 {
			t.Fatalf("空闲后仍占用 %d 个运行名额", count)
		}
	}
}

func TestShardCacheLineSeparation(t *testing.T) {
	var s shard
	if unsafe.Offsetof(s.head)-unsafe.Offsetof(s.tail) < cacheLineSize ||
		unsafe.Offsetof(s.wakingWorkers)-unsafe.Offsetof(s.head) < cacheLineSize ||
		unsafe.Sizeof(s)+unsafe.Offsetof(s.tail)-unsafe.Offsetof(s.runningWorkers) < cacheLineSize {
		t.Fatal("队列头尾或相邻分片的 worker 状态未隔离缓存行")
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
							s.notify(false)
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
