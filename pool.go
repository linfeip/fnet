package fnet

import (
	"runtime"
	"sync"
	"sync/atomic"
	"time"
)

// WorkerPoolConfig defines configurations for WorkerPool.
type WorkerPoolConfig struct {
	// Shards specifies the number of independent worker shards.
	// Defaults to next power of 2 of runtime.GOMAXPROCS(0)*4 (clamped between 16 and 256).
	Shards int

	// MaxWorkersPerShard limits the maximum number of worker goroutines per shard.
	// Defaults to 256 (e.g. 64 shards * 256 = 16384 total workers max).
	MaxWorkersPerShard int

	// QueueSizePerShard specifies the task channel buffer capacity per shard.
	// Defaults to 2048 (e.g. 64 shards * 2048 = 131072 queue capacity).
	QueueSizePerShard int

	// IdleTimeout specifies how long an idle worker goroutine waits for new tasks before exiting.
	// Defaults to 5 seconds. Idle workers exit to reclaim memory, allowing 1M idle connections
	// to hold zero worker goroutines.
	IdleTimeout time.Duration
}

func nextPowerOfTwo(n int) int {
	if n <= 1 {
		return 1
	}
	n--
	n |= n >> 1
	n |= n >> 2
	n |= n >> 4
	n |= n >> 8
	n |= n >> 16
	n++
	return n
}

func defaultShards() int {
	n := nextPowerOfTwo(runtime.GOMAXPROCS(0) * 4)
	if n < 16 {
		return 16
	}
	if n > 256 {
		return 256
	}
	return n
}

// WorkerPool is a high-concurrency, sharded, elastic goroutine worker pool
// designed for massive scale (1M+ concurrent HTTP/WebSocket connections) without
// any external dependencies.
//
// Key architectural features:
//   - Zero external dependencies: pure Go standard library (sync, atomic, runtime).
//   - Multi-shard queues: eliminates global lock and channel contention across CPU cores.
//   - Connection affinity: tasks for the same connection hash to the same shard, improving CPU cache locality.
//   - Lazy worker spawning & idle reaping: idle connections hold zero worker goroutines.
//   - Non-blocking reactor offload: never blocks the I/O reactor event loop under heavy load.
//   - Panic protection: user handler panics are caught and do not kill worker threads or the process.
type WorkerPool struct {
	shards    []*workerShard
	shardMask uint64
	round     atomic.Uint64
	closed    atomic.Bool
	wg        sync.WaitGroup
}

type workerShard struct {
	pool        *WorkerPool
	tasks       chan func()
	maxWorkers  int32
	curWorkers  atomic.Int32
	idleWorkers atomic.Int32
	idleTimeout time.Duration
}

// NewWorkerPool creates a new high-concurrency sharded worker pool.
func NewWorkerPool(cfgs ...WorkerPoolConfig) *WorkerPool {
	var cfg WorkerPoolConfig
	if len(cfgs) > 0 {
		cfg = cfgs[0]
	}

	numShards := cfg.Shards
	if numShards <= 0 {
		numShards = defaultShards()
	} else {
		numShards = nextPowerOfTwo(numShards)
	}

	maxWorkers := int32(cfg.MaxWorkersPerShard)
	if maxWorkers <= 0 {
		maxWorkers = 256
	}

	queueSize := cfg.QueueSizePerShard
	if queueSize <= 0 {
		queueSize = 2048
	}

	idleTimeout := cfg.IdleTimeout
	if idleTimeout <= 0 {
		idleTimeout = 5 * time.Second
	}

	p := &WorkerPool{
		shards:    make([]*workerShard, numShards),
		shardMask: uint64(numShards - 1),
	}

	for i := 0; i < numShards; i++ {
		p.shards[i] = &workerShard{
			pool:        p,
			tasks:       make(chan func(), queueSize),
			maxWorkers:  maxWorkers,
			idleTimeout: idleTimeout,
		}
	}

	return p
}

// Submit dispatches a task to the pool using round-robin shard distribution.
func (p *WorkerPool) Submit(task func()) {
	if task == nil || p.closed.Load() {
		return
	}
	idx := p.round.Add(1) & p.shardMask
	p.shards[idx].submit(task)
}

// SubmitConn dispatches a task with connection affinity based on connID (e.g. socket fd).
// All tasks for the same connection hash to the same worker shard, maximizing CPU cache locality.
func (p *WorkerPool) SubmitConn(connID uint64, task func()) {
	if task == nil || p.closed.Load() {
		return
	}
	// SplitMix64 / Murmur3 64-bit mixer for uniform bit distribution
	h := connID
	h ^= h >> 33
	h *= 0xff51afd7ed558ccd
	h ^= h >> 33
	idx := h & p.shardMask
	p.shards[idx].submit(task)
}

// Close gracefully closes the worker pool and waits for running tasks to complete.
func (p *WorkerPool) Close() {
	if p.closed.CompareAndSwap(false, true) {
		for _, s := range p.shards {
			close(s.tasks)
		}
		p.wg.Wait()
	}
}

// RunningWorkers returns the total number of currently active worker goroutines.
func (p *WorkerPool) RunningWorkers() int {
	var total int
	for _, s := range p.shards {
		total += int(s.curWorkers.Load())
	}
	return total
}

// IdleWorkers returns the total number of currently idle worker goroutines waiting for tasks.
func (p *WorkerPool) IdleWorkers() int {
	var total int
	for _, s := range p.shards {
		total += int(s.idleWorkers.Load())
	}
	return total
}

func (s *workerShard) submit(task func()) {
	if s.pool.closed.Load() {
		return
	}

	// 1. Try non-blocking enqueue into the shard task queue
	select {
	case s.tasks <- task:
		// Task enqueued. If no workers are currently idle and we haven't reached maxWorkers,
		// spawn an additional worker to consume tasks.
		if s.idleWorkers.Load() == 0 && s.curWorkers.Load() < s.maxWorkers {
			s.maybeSpawnWorker(nil)
		}
		return
	default:
	}

	// 2. Queue is full. Try spawning a new worker directly carrying this task
	if s.curWorkers.Load() < s.maxWorkers {
		if s.maybeSpawnWorker(task) {
			return
		}
	}

	// 3. Fallback under extreme saturation: never block the caller (especially IO reactor loop).
	// Run the task on a detached goroutine with panic protection.
	go runSafe(task)
}

func (s *workerShard) maybeSpawnWorker(firstTask func()) bool {
	for {
		cur := s.curWorkers.Load()
		if cur >= s.maxWorkers {
			return false
		}
		if s.curWorkers.CompareAndSwap(cur, cur+1) {
			s.pool.wg.Add(1)
			go s.workerLoop(firstTask)
			return true
		}
	}
}

func (s *workerShard) workerLoop(firstTask func()) {
	defer func() {
		s.curWorkers.Add(-1)
		s.pool.wg.Done()
	}()

	if firstTask != nil {
		runSafe(firstTask)
	}

	timer := time.NewTimer(s.idleTimeout)
	defer timer.Stop()
	lastReset := time.Now()
	halfTimeout := s.idleTimeout / 2

	for {
		s.idleWorkers.Add(1)
		select {
		case task, ok := <-s.tasks:
			s.idleWorkers.Add(-1)
			if !ok {
				return
			}
			runSafe(task)

			// Drain already queued tasks in batch without touching runtime timer
			for {
				select {
				case nextTask, ok := <-s.tasks:
					if !ok {
						return
					}
					runSafe(nextTask)
				default:
					goto drained
				}
			}

		drained:
			// Only refresh timer if at least half of idleTimeout has elapsed,
			// eliminating millions of timer.Stop/Reset calls under continuous traffic.
			now := time.Now()
			if now.Sub(lastReset) >= halfTimeout {
				if !timer.Stop() {
					select {
					case <-timer.C:
					default:
					}
				}
				timer.Reset(s.idleTimeout)
				lastReset = now
			}

		case <-timer.C:
			s.idleWorkers.Add(-1)
			return
		}
	}
}

func runSafe(fn func()) {
	defer func() {
		_ = recover()
	}()
	fn()
}

// DefaultWorkerPool is the globally shared, highly scalable default WorkerPool.
// Idle connections hold zero goroutines inside this pool.
var DefaultWorkerPool = NewWorkerPool()
