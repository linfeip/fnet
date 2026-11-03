// Package taskpool is a goroutine pool organized into shards that reuses the goroutines running short tasks, saving the
// cost of creating and destroying a goroutine per task.
//
// Each shard uses a bounded lock-free queue and keeps at most maxWorkers workers; after enqueuing a task the shard wakes
// a worker only when none is already on its way to the queue.
// Under normal non-blocking load each shard keeps at most softWorkers workers serving its queue at once, so short tasks
// stay in the FIFO ring buffer instead of waking hundreds of competing goroutines into the scheduler. When a shard's
// serving workers take fewer tasks than their number in a stallInterval while tasks remain queued (for example because
// callbacks block on database or network I/O) and fewer goroutines than GOMAXPROCS wait in run queues, a background
// monitor doubles the workers allowed to serve the shard, so additional workers wake or start up to maxWorkers.
// The total worker limit is shards * maxWorkers, shared by all shards: when every worker of a shard is running a task
// (the shard has reached its own limit), its tasks are taken by a suspended worker of another shard, or a new worker
// is started in a shard that still has room (see Pool.borrow); when no worker is available anywhere in the pool the
// task is queued and the next worker to suspend comes for it (see Pool.starving). Only a saturated shard crosses
// shards; under normal load a worker takes tasks only from its own shard, because always-on work stealing raised tail
// latency in measurements. To accommodate N callbacks that block at the same time the total limit has to be greater
// than N.
// When the queue is full it falls back to running the task in a temporary goroutine that helps drain the queue, so
// submitters never block.
// A worker stays resident once started, until the pool is closed (see Pool.Close).
// When a task panics it is logged (with the stack trace), and the goroutine running it goes on with the following tasks.
package taskpool

import (
	"log/slog"
	"math"
	"math/bits"
	"runtime"
	"runtime/metrics"
	"sync/atomic"
	"time"
	_ "unsafe"
)

// MaxWorkers is the upper bound on the number of workers per shard: suspended workers are recorded in a bitmap of
// idleWords 64-bit words.
const MaxWorkers = 512

// idleWords is the number of words in a shard's bitmap of suspended workers.
const idleWords = MaxWorkers / 64

const (
	// softWorkers is the number of workers serving a shard concurrently while tasks complete within stallInterval.
	softWorkers          = 2
	stallInterval        = time.Millisecond
	defaultQueueCapacity = 8192
)

// NewDefault creates a Pool with one shard per P, MaxWorkers workers per shard, and a queue capacity of 8192 per shard.
// Workers start only once there are tasks, and a worker started stays resident until the pool is closed.
func NewDefault() *Pool {
	return New(runtime.GOMAXPROCS(0), MaxWorkers, defaultQueueCapacity)
}

// Pool is a goroutine pool.
type Pool struct {
	shards []shard
	closed atomic.Bool         // set by Close; read by every Submit along with shards, so it shares their cache line
	_      [cacheLineSize]byte // every Submit reads shards: keep it off the cache line of starving and monitoring
	// starving is set when a shard had tasks queued and found no suspended worker and no room for a new one anywhere in
	// the pool; the next worker to suspend clears it and notifies every shard once (see shard.work). It is written only
	// while the whole pool is saturated.
	starving   atomic.Bool
	monitoring atomic.Bool // set while the stall monitor goroutine is active (see watchStall)
	_          [cacheLineSize - 2]byte
}

// New creates a Pool: shards shards, at most maxWorkers workers per shard (1 to MaxWorkers), and a queue capacity of
// capacity rounded up to a power of two. Workers start only once there are tasks.
func New(shards, maxWorkers, capacity int) *Pool {
	maxWorkers = min(max(maxWorkers, 1), MaxWorkers)
	size := 1 << bits.Len(uint(max(capacity, 2)-1))
	p := &Pool{shards: make([]shard, max(shards, 1))}
	for i := range p.shards {
		s := &p.shards[i]
		s.pool = p
		s.slots = make([]slot, size)
		for j := range s.slots {
			s.slots[j].sequence.Store(uint64(j))
		}
		s.mask = uint64(size - 1)
		s.wakeups = make([]chan *shard, maxWorkers)
		s.maxWorkers = int32(maxWorkers)
		s.servingLimit.Store(softWorkers)
	}
	return p
}

// Submit submits a task asynchronously and does not block the caller; it allocates nothing when a worker is already
// started and neither growth nor overflow is triggered.
// A panicking task is logged and does not affect the following tasks.
func (p *Pool) Submit(task func()) {
	p.shards[cheaprandn(uint32(len(p.shards)))].submit(task)
}

// SubmitTo is like Submit but submits to the shard selected by key (key modulo the number of shards): tasks with the
// same key always go to the same queue, run by that shard's workers, or by workers it borrows from other shards once all
// of its own are busy (see Pool.borrow).
func (p *Pool) SubmitTo(key int, task func()) {
	p.shards[uint(key)%uint(len(p.shards))].submit(task)
}

// RandomKey returns a random key, with which SubmitTo (or an executor choosing its queue by the key modulo the number of
// queues, such as fnet.Options.Executor) places a task in a random queue, the way Submit does. The key does not depend on
// the number of shards, so it suits an executor of any size.
func RandomKey() int { return int(cheaprandn(math.MaxInt32)) }

// Close ends the pool's workers: a suspended one exits at once, one running a task when it finds the queue empty, so
// the tasks submitted before all run. A task submitted afterwards runs in a goroutine of its own, as when the queue is
// full. Close does not wait for the workers, and may be called more than once.
func (p *Pool) Close() {
	p.closed.Store(true)
	for i := range p.shards {
		for p.shards[i].wakeIdle(nil) { // a worker sent no shard exits
		}
	}
}

// submit puts the task into this shard's queue and makes sure a worker comes for it; when the queue is full, or the pool
// closed, a temporary goroutine runs the task and helps drain the queue.
func (s *shard) submit(task func()) {
	if s.pool.closed.Load() || !s.push(task) {
		go s.help(task)
		return
	}
	s.notify()
}

// cheaprandn returns a random number in [0, n): it is the runtime's own fast random number generator, whose state
// belongs to the current thread (M) so that submitters do not contend, and it is more than twice as fast as
// math/rand/v2. The runtime explicitly reserved it for linkname (see go.dev/issue/67401).
//
//go:linkname cheaprandn runtime.cheaprandn
func cheaprandn(n uint32) uint32

// run runs a task; when the task panics it is logged (with the stack trace), and the caller goes on running the
// following tasks as usual.
func run(task func()) {
	defer func() {
		if err := recover(); err != nil {
			buf := make([]byte, 64<<10)
			slog.Error("taskpool: panic in task", "panic", err, "stack", string(buf[:runtime.Stack(buf, false)]))
		}
	}()
	task()
}

// slot is one cell of the queue. For task number pos (at position pos&mask): sequence equal to pos means it can be
// written, equal to pos+1 means it has been written and can be taken out, and after it is taken out sequence is set to
// pos+capacity, left for the next lap.
type slot struct {
	sequence atomic.Uint64
	task     func()
}

// shard is one shard of the pool. Submitters write tail, workers write head, and the worker state occupies yet another
// cache line; keeping the three apart means modifying one does not invalidate the cache lines holding the other two.
const cacheLineSize = 128 // covering both the common 64B and 128B cache lines

type shard struct {
	slots      []slot
	mask       uint64
	wakeups    []chan *shard // indexed by worker id; a suspended worker receives on its own channel the shard to serve next
	maxWorkers int32
	pool       *Pool
	_          [cacheLineSize]byte
	tail       atomic.Uint64 // the position the next task is written to
	_          [cacheLineSize - 8]byte
	head       atomic.Uint64 // the position the next task is taken from; Pool.monitor samples it to see the queue move
	_          [cacheLineSize - 8]byte
	// wakingWorkers is the number of workers (of this shard, or lent by another) that have been woken or newly started
	// for this queue but have not reached it yet: once they get there they will take the queued tasks, so there is no
	// need to wake another worker.
	wakingWorkers  atomic.Int32
	servingWorkers atomic.Int32             // workers currently woken for or draining this shard's queue
	servingLimit   atomic.Int32             // servingWorkers notify wakes up to: softWorkers unless Pool.monitor raised it
	liveWorkers    atomic.Int32             // the number of started workers, numbered 0 through liveWorkers-1
	idleWorkers    [idleWords]atomic.Uint64 // the suspended workers, bit i%64 of word i/64 standing for id i
	_              [cacheLineSize]byte
}

// push puts a task into the queue and returns false when the queue is full.
func (s *shard) push(task func()) bool {
	pos := s.tail.Load()
	for {
		sl := &s.slots[pos&s.mask]
		switch seq := sl.sequence.Load(); {
		case seq == pos:
			if s.tail.CompareAndSwap(pos, pos+1) {
				sl.task = task
				sl.sequence.Store(pos + 1)
				return true
			}
			pos = s.tail.Load()
		case int64(seq-pos) < 0: // the task from the previous lap in this cell has not been taken out yet
			return false
		default: // another submitter has already claimed this cell
			pos = s.tail.Load()
		}
	}
}

// pop takes out a task and returns nil when the queue is empty.
func (s *shard) pop() func() {
	pos := s.head.Load()
	for {
		sl := &s.slots[pos&s.mask]
		switch seq := sl.sequence.Load(); {
		case seq == pos+1:
			if s.head.CompareAndSwap(pos, pos+1) {
				task := sl.task
				sl.task = nil // stop referencing the task that was taken out
				sl.sequence.Store(pos + s.mask + 1)
				return task
			}
			pos = s.head.Load()
		case int64(seq-(pos+1)) < 0: // not written yet (or being written)
			return nil
		default: // another worker has already taken this cell
			pos = s.head.Load()
		}
	}
}

// hasTask reports whether a task is available at the head of the queue.
func (s *shard) hasTask() bool {
	pos := s.head.Load()
	return s.slots[pos&s.mask].sequence.Load() == pos+1
}

// notify makes sure a worker comes for the tasks in the queue: when a worker is already on its way (holding
// wakingWorkers) it does nothing, nor while starving is set (the next worker to suspend notifies every shard); when
// servingLimit workers already serve this shard it leaves the tasks to them and has Pool.monitor watch for a stall (see
// watchStall); otherwise it wakes a suspended worker of this shard or starts a new one, and when the shard is at its
// limit with every worker running a task it borrows one from another shard (see Pool.borrow). The worker it finds takes
// over wakingWorkers and servingWorkers; on the failure path this function releases them. When the whole pool has no
// available worker it sets starving, and the next worker to suspend comes for the tasks.
//
// Wakeups are never lost. Within a shard: the submitter writes the task before looking at the workers and a worker
// registers as suspended and decrements servingWorkers before looking at the queue, so at least one side sees the other.
// Across shards: this sets wakingWorkers and starving before looking once more for suspended workers pool-wide, while a
// worker registers as suspended before reading starving, so either it sees the flag or the check here sees it.
func (s *shard) notify() {
	p := s.pool
	if !s.hasTask() || (p.starving.Load() && !p.closed.Load()) || s.wakingWorkers.Load() != 0 {
		return
	}
	if s.servingWorkers.Load() >= s.servingLimit.Load() && !p.closed.Load() {
		p.watchStall()
		return
	}
	if !s.wakingWorkers.CompareAndSwap(0, 1) {
		return
	}
	s.servingWorkers.Add(1)
	if s.wakeIdle(s) || s.start(s) || p.borrow(s) {
		return
	}
	s.servingWorkers.Add(-1)
	s.wakingWorkers.Add(-1)
	if p.closed.Load() { // the workers have left, so the next to suspend would not come: a goroutine of its own does
		go s.help(s.pop())
		return
	}
	p.starving.Store(true)
	if p.hasIdleWorker() && p.starving.CompareAndSwap(true, false) {
		for i := range p.shards {
			p.shards[i].notify()
		}
	}
}

// watchStall starts the stall monitor goroutine when none is running yet.
func (p *Pool) watchStall() {
	if !p.monitoring.Load() && p.monitoring.CompareAndSwap(false, true) {
		go p.monitor()
	}
}

// monitor runs while tasks wait behind the servingLimit of some shard: every stallInterval it raises the limit of the
// shards whose serving workers stopped taking tasks (see boost). It exits once no shard has such a backlog, or the pool
// is closed.
func (p *Pool) monitor() {
	heads := make([]uint64, len(p.shards))
	for i := range p.shards {
		heads[i] = p.shards[i].head.Load()
	}
	var cpu cpuSampler
	ticker := time.NewTicker(stallInterval)
	defer ticker.Stop()
	for range ticker.C {
		if !p.closed.Load() && p.boost(heads, &cpu) {
			continue
		}
		p.monitoring.Store(false)
		// A notify that found monitoring still set left its backlog to this monitor: look once more before exiting.
		if p.closed.Load() || !p.hasBacklog() || !p.monitoring.CompareAndSwap(false, true) {
			return
		}
	}
}

// boost raises the servingLimit of every shard whose queue stalls behind it: its serving workers took fewer tasks than
// their number since the last round, blocking on a database or network call for example. It doubles the workers allowed
// to serve the shard, up to the number of queued tasks, provided the run queues have room for them, which it samples at
// most once per round and only for a stalled shard (see cpuSampler.hasHeadroom). It reports whether any shard has tasks
// queued behind its servingLimit.
func (p *Pool) boost(heads []uint64, cpu *cpuSampler) bool {
	backlog, sampled, headroom := false, false, false
	for i := range p.shards {
		s := &p.shards[i]
		head := s.head.Load()
		taken := head - heads[i]
		heads[i] = head
		serving := s.servingWorkers.Load()
		if !s.hasTask() || serving < s.servingLimit.Load() {
			continue
		}
		backlog = true
		if taken >= uint64(serving) {
			continue
		}
		if !sampled {
			sampled, headroom = true, cpu.hasHeadroom()
		}
		if headroom {
			s.servingLimit.Store(serving + int32(min(uint64(serving), s.tail.Load()-head)))
			s.notify()
		}
	}
	return backlog
}

// cpuSampler reads the scheduler's runnable goroutine count, reusing its sample slice so reading allocates nothing.
type cpuSampler struct {
	samples [1]metrics.Sample
}

// hasHeadroom reports whether fewer than GOMAXPROCS goroutines are waiting in run queues: waking more workers when every P
// already has runnable work queued behind it only lengthens scheduling tail latency; when the runtime does not export the
// metric, it reports true.
func (c *cpuSampler) hasHeadroom() bool {
	c.samples[0].Name = "/sched/goroutines/runnable:goroutines"
	metrics.Read(c.samples[:])
	runnable := c.samples[0].Value
	if runnable.Kind() != metrics.KindUint64 {
		return true
	}
	return runnable.Uint64() < uint64(runtime.GOMAXPROCS(0))
}

// hasBacklog reports whether any shard has tasks queued behind its servingLimit, which notify leaves to Pool.monitor.
func (p *Pool) hasBacklog() bool {
	for i := range p.shards {
		s := &p.shards[i]
		if s.hasTask() && s.servingWorkers.Load() >= s.servingLimit.Load() {
			return true
		}
	}
	return false
}

// wakeIdle wakes the suspended worker of s with the lowest id (its stack and caches are the hottest) to serve target,
// handing it the target's wakingWorkers and servingWorkers the caller holds; it reports false when s has none suspended.
func (s *shard) wakeIdle(target *shard) bool {
	for w := range (s.liveWorkers.Load() + 63) / 64 {
		word := &s.idleWorkers[w]
		for {
			idle := word.Load()
			if idle == 0 {
				break
			}
			bit := bits.TrailingZeros64(idle)
			if word.CompareAndSwap(idle, idle&^(1<<bit)) {
				s.wakeups[int(w)*64+bit] <- target // only the one that clears the bit sends: the buffer of 1 never blocks
				return true
			}
		}
	}
	return false
}

// start starts a new worker of s that serves target first, handing it the target's wakingWorkers and servingWorkers the
// caller holds; it reports false when s has maxWorkers workers already.
func (s *shard) start(target *shard) bool {
	for {
		live := s.liveWorkers.Load()
		if live >= s.maxWorkers {
			return false
		}
		if s.liveWorkers.CompareAndSwap(live, live+1) {
			wakeup := make(chan *shard, 1)
			s.wakeups[live] = wakeup
			go s.work(live, wakeup, target)
			return true
		}
	}
}

// borrow finds a worker for target, a shard whose workers are all running tasks with no room for another: a suspended
// worker of another shard, or else a new worker started in another shard that has room. The worker takes over the
// target's wakingWorkers and servingWorkers the caller holds; borrow reports false when the whole pool has none to give.
func (p *Pool) borrow(target *shard) bool {
	count := len(p.shards)
	first := int(cheaprandn(uint32(count)))
	for i := range count {
		if s := &p.shards[(first+i)%count]; s != target && s.wakeIdle(target) {
			return true
		}
	}
	for i := range count {
		if s := &p.shards[(first+i)%count]; s != target && s.start(target) {
			return true
		}
	}
	return false
}

// hasIdleWorker reports whether any shard has a suspended worker.
func (p *Pool) hasIdleWorker() bool {
	for i := range p.shards {
		s := &p.shards[i]
		for w := range (s.liveWorkers.Load() + 63) / 64 {
			if s.idleWorkers[w].Load() != 0 {
				return true
			}
		}
	}
	return false
}

// work is the worker with id id of shard s: it takes and runs the tasks of target (s itself, or a shard s lent it to)
// until that queue is empty, then suspends in s waiting for a wakeup, which names the shard to serve next.
func (s *shard) work(id int32, wakeup chan *shard, target *shard) {
	// The loop ends only with Pool.Close. The other way out is a task calling runtime.Goexit (which recover cannot stop).
	// A replacement worker is then started with the same id, on its way back to the same queue: it is still counted in
	// liveWorkers and target.servingWorkers, and without the replacement this shard would be one worker short forever.
	closed := false
	defer func() {
		if !closed {
			target.wakingWorkers.Add(1)
			go s.work(id, wakeup, target)
		}
	}()
	p := s.pool
	word, bit := &s.idleWorkers[id/64], uint64(1)<<(id%64)
	for {
		target.wakingWorkers.Add(-1) // the queue has been reached
		for task := target.pop(); task != nil; task = target.pop() {
			// While tasks are still queued, first make sure a worker comes to take them (up to target's servingLimit),
			// then run this one.
			if target.hasTask() {
				target.notify()
			}
			run(task)
		}
		// The queue is drained, and with it any backlog for which Pool.monitor raised servingLimit.
		if target.servingLimit.Load() != softWorkers {
			target.servingLimit.Store(softWorkers)
		}
		// Registered as suspended before leaving servingWorkers: a notify that sees room under servingLimit then finds this
		// worker to wake, instead of starting another.
		word.Or(bit)
		target.servingWorkers.Add(-1)
		// A shard found no worker anywhere in the pool: now that this one is suspended, notify every shard once. The
		// flag is cleared first, so a shard that still finds none sets it again.
		if p.starving.Load() && p.starving.CompareAndSwap(true, false) {
			for i := range p.shards {
				p.shards[i].notify()
			}
		} else {
			// Look at the queues once more after registering as suspended and decrementing servingWorkers: a submitter
			// may have enqueued before this worker was visible as suspended or just before servingWorkers dropped.
			if target != s && target.hasTask() {
				target.notify()
			}
			if s.hasTask() {
				s.notify()
			}
		}
		// Once the pool is closed this worker leaves, unless a waker has taken its bit: a shard, or Close's nil, is then on
		// its way to it. Checked after registering as suspended, like the queue, so that Close and the worker see each other.
		if p.closed.Load() && word.And(^bit)&bit != 0 {
			break
		}
		if target = <-wakeup; target == nil {
			break
		}
	}
	closed = true
}

// help runs task when the queue is full, then helps take the remaining tasks in the queue before exiting.
func (s *shard) help(task func()) {
	for ; task != nil; task = s.pop() {
		run(task)
	}
}
