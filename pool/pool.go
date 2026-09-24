// Package pool is the sharded worker pool that runs fnet's business code:
// HTTP handlers and WebSocket OnMessage callbacks. It knows nothing about
// networking; fhttp and websocket hand it tasks keyed by connection.
package pool

import (
	"runtime"
	"sync"
	"sync/atomic"
	"time"
)

// Config configures a Pool. Zero fields take the defaults below.
type Config struct {
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

// Pool is a high-concurrency, sharded, elastic goroutine worker pool
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
type Pool struct {
	shards            []*workerShard
	shardMask         uint64
	round             atomic.Uint64
	closed            atomic.Bool
	globalIdleWorkers atomic.Int32
	wg                sync.WaitGroup
}

type workerShard struct {
	pool        *Pool
	id          int
	mu          sync.RWMutex
	closed      bool
	tasks       chan func()
	maxWorkers  int32
	curWorkers  atomic.Int32
	idleWorkers atomic.Int32
	idleTimeout time.Duration
	stealRound  atomic.Uint32
}

// New creates a sharded worker pool.
func New(cfgs ...Config) *Pool {
	var cfg Config
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

	p := &Pool{
		shards:    make([]*workerShard, numShards),
		shardMask: uint64(numShards - 1),
	}

	for i := 0; i < numShards; i++ {
		p.shards[i] = &workerShard{
			pool:        p,
			id:          i,
			tasks:       make(chan func(), queueSize),
			maxWorkers:  maxWorkers,
			idleTimeout: idleTimeout,
		}
	}

	return p
}

// Submit dispatches a task to the pool using power-of-two-choices load balancing.
// It inspects two pseudo-random shards and assigns the task to the one with lower load
// (more idle workers or fewer queued tasks), mitigating shard skew and hotspot buildup.
func (p *Pool) Submit(task func()) {
	if task == nil {
		return
	}
	if p.closed.Load() {
		go runSafe(task)
		return
	}
	r := p.round.Add(1)
	idx1 := r & p.shardMask
	idx2 := (r + 7) & p.shardMask

	s1 := p.shards[idx1]
	s2 := p.shards[idx2]

	var chosen *workerShard
	i1 := s1.idleWorkers.Load()
	i2 := s2.idleWorkers.Load()
	if i1 > i2 {
		chosen = s1
	} else if i2 > i1 {
		chosen = s2
	} else {
		if len(s1.tasks) <= len(s2.tasks) {
			chosen = s1
		} else {
			chosen = s2
		}
	}
	chosen.submit(task)
}

// SubmitConn dispatches a task with connection affinity based on connID (e.g. socket fd).
// All tasks for the same connection hash to the same worker shard, maximizing CPU cache locality.
func (p *Pool) SubmitConn(connID uint64, task func()) {
	if task == nil {
		return
	}
	if p.closed.Load() {
		go runSafe(task)
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
func (p *Pool) Close() {
	if p.closed.CompareAndSwap(false, true) {
		for _, s := range p.shards {
			s.mu.Lock()
			s.closed = true
			close(s.tasks)
			s.mu.Unlock()
		}
		p.wg.Wait()
	}
}

// RunningWorkers returns the total number of currently active worker goroutines.
func (p *Pool) RunningWorkers() int {
	var total int
	for _, s := range p.shards {
		total += int(s.curWorkers.Load())
	}
	return total
}

// IdleWorkers returns the total number of currently idle worker goroutines waiting for tasks.
func (p *Pool) IdleWorkers() int {
	var total int
	for _, s := range p.shards {
		total += int(s.idleWorkers.Load())
	}
	return total
}

// trySteal attempts to steal a task from another shard that has queued tasks.
// Returns nil if no tasks could be stolen.
func (p *Pool) trySteal(myShardID int) func() {
	if p.closed.Load() {
		return nil
	}
	numShards := len(p.shards)
	if numShards <= 1 {
		return nil
	}

	myShard := p.shards[myShardID]
	start := (uint32(myShardID) + myShard.stealRound.Add(1)) & uint32(p.shardMask)

	for i := 0; i < numShards-1; i++ {
		victimIdx := (start + uint32(i) + 1) & uint32(p.shardMask)
		victim := p.shards[victimIdx]

		// Fast path: avoid channel lock if queue is empty
		if len(victim.tasks) == 0 {
			continue
		}

		select {
		case task, ok := <-victim.tasks:
			if ok {
				return task
			}
		default:
		}
	}
	return nil
}

// tryOffloadToIdle attempts to push a task to another shard that has idle workers waiting.
func (p *Pool) tryOffloadToIdle(task func(), myShardID int) bool {
	if p.closed.Load() || p.globalIdleWorkers.Load() == 0 {
		return false
	}
	numShards := len(p.shards)
	if numShards <= 1 {
		return false
	}

	myShard := p.shards[myShardID]
	start := (uint32(myShardID) + myShard.stealRound.Add(1)) & uint32(p.shardMask)

	for i := 0; i < numShards-1; i++ {
		targetIdx := (start + uint32(i) + 1) & uint32(p.shardMask)
		target := p.shards[targetIdx]

		if target.idleWorkers.Load() > 0 {
			target.mu.RLock()
			if !target.closed {
				select {
				case target.tasks <- task:
					target.mu.RUnlock()
					return true
				default:
				}
			}
			target.mu.RUnlock()
		}
	}
	return false
}

// tryOffload attempts to push a task to any other shard with spare capacity.
func (p *Pool) tryOffload(task func(), myShardID int) bool {
	if p.closed.Load() {
		return false
	}
	numShards := len(p.shards)
	if numShards <= 1 {
		return false
	}

	myShard := p.shards[myShardID]
	start := (uint32(myShardID) + myShard.stealRound.Add(1)) & uint32(p.shardMask)

	for i := 0; i < numShards-1; i++ {
		targetIdx := (start + uint32(i) + 1) & uint32(p.shardMask)
		target := p.shards[targetIdx]

		// Fast path: avoid lock if target queue is full
		if len(target.tasks) >= cap(target.tasks) {
			continue
		}

		target.mu.RLock()
		if !target.closed {
			select {
			case target.tasks <- task:
				if target.idleWorkers.Load() == 0 && target.curWorkers.Load() < target.maxWorkers {
					target.maybeSpawnWorker(nil)
				}
				target.mu.RUnlock()
				return true
			default:
			}
		}
		target.mu.RUnlock()
	}
	return false
}

func (s *workerShard) submit(task func()) {
	if s.pool.closed.Load() {
		go runSafe(task)
		return
	}

	s.mu.RLock()
	if s.closed {
		s.mu.RUnlock()
		go runSafe(task)
		return
	}

	// 1. If this shard has an idle worker ready to execute immediately, wake it up (affinity path)
	if s.idleWorkers.Load() > 0 {
		select {
		case s.tasks <- task:
			s.mu.RUnlock()
			return
		default:
		}
	}

	// 2. No idle workers on this shard, but we can spawn another worker up to maxWorkers
	if s.curWorkers.Load() < s.maxWorkers {
		select {
		case s.tasks <- task:
			s.mu.RUnlock()
			s.maybeSpawnWorker(nil)
			return
		default:
			if s.maybeSpawnWorker(task) {
				s.mu.RUnlock()
				return
			}
		}
	}

	s.mu.RUnlock()

	// 3. This shard is at capacity. If other shards have idle workers, offload to them immediately.
	// Fast O(1) check: if globalIdleWorkers is 0, skip the 63-shard loop entirely!
	if s.pool.globalIdleWorkers.Load() > 0 && s.pool.tryOffloadToIdle(task, s.id) {
		return
	}

	// 4. No idle workers anywhere across the pool. Buffer in local queue if space allows.
	s.mu.RLock()
	if !s.closed {
		select {
		case s.tasks <- task:
			s.mu.RUnlock()
			return
		default:
		}
	}
	s.mu.RUnlock()

	// 5. Local queue is full. Try offload to any shard with spare queue capacity.
	if s.pool.tryOffload(task, s.id) {
		return
	}

	// 6. Absolute saturation fallback: never block the caller (especially IO reactor loop).
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
		// 1. Fast path: drain local shard tasks first (maximizes CPU cache locality)
		for {
			select {
			case task, ok := <-s.tasks:
				if !ok {
					return
				}
				runSafe(task)
			default:
				goto checkSteal
			}
		}

	checkSteal:
		// 2. Local queue is empty: try stealing tasks from other busy shards before going to sleep.
		// This eliminates shard skew where one shard is overloaded while others sit idle.
		for {
			stolen := s.pool.trySteal(s.id)
			if stolen == nil {
				break
			}
			runSafe(stolen)

			// If new local tasks arrived while executing stolen work, switch back to local
			// queue immediately to maintain connection and core affinity.
			if len(s.tasks) > 0 {
				break
			}
		}

		if len(s.tasks) > 0 {
			continue
		}

		// 3. All queues across all shards are drained.
		// Refresh idle timer if needed before sleeping.
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

		// Enter idle wait state
		s.idleWorkers.Add(1)
		s.pool.globalIdleWorkers.Add(1)
		select {
		case task, ok := <-s.tasks:
			s.idleWorkers.Add(-1)
			s.pool.globalIdleWorkers.Add(-1)
			if !ok {
				return
			}
			runSafe(task)

		case <-timer.C:
			s.idleWorkers.Add(-1)
			s.pool.globalIdleWorkers.Add(-1)
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

// Default returns the process-wide pool used when no custom pool is
// configured. It is created on first use.
func Default() *Pool {
	if p := defaultPool.Load(); p != nil {
		return p
	}
	defaultOnce.Do(func() { defaultPool.CompareAndSwap(nil, New()) })
	return defaultPool.Load()
}

// SetDefault replaces the default pool. The previous one is not closed.
func SetDefault(p *Pool) {
	if p != nil {
		defaultPool.Store(p)
	}
}

var (
	defaultOnce sync.Once
	defaultPool atomic.Pointer[Pool]
)

// Adapt turns a plain submit function (ants.Submit, a custom scheduler) into
// the connection-keyed form fhttp and websocket take, ignoring the key.
func Adapt(submit func(task func())) func(connID uint64, task func()) {
	if submit == nil {
		return nil
	}
	return func(_ uint64, task func()) { submit(task) }
}

// Dispatch runs task through submit, or through Default when submit is nil,
// keyed by connID. It reports false when submit panicked, i.e. a custom pool
// rejected the task; the caller should then give up on the connection.
func Dispatch(submit func(connID uint64, task func()), connID uint64, task func()) (ok bool) {
	if submit == nil {
		Default().SubmitConn(connID, task)
		return true
	}
	defer func() {
		if recover() != nil {
			ok = false
		}
	}()
	submit(connID, task)
	return true
}
