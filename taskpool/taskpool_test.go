package taskpool

import (
	"io"
	"log/slog"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"
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
