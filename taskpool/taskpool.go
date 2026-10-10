// Package taskpool is a goroutine pool organized into shards that reuses the goroutines running short tasks, saving the
// cost of creating and destroying a goroutine per task. DefaultTaskPool can be used directly, and websocket uses it to
// run callbacks by default.
//
// Each shard uses a bounded lock-free queue and keeps at most maxWorkers workers. Large batches are spread evenly
// across the shards; small batches keep their random placement and merge notifications, and each shard decides once
// whether to wake a worker after enqueuing (when one is already on its way there is no second wakeup).
// The number of running workers is bounded only by maxWorkers; there is no GOMAXPROCS soft budget any more: a worker
// whose callback blocks still counts as running, and a soft budget keeps the pool from growing enough to absorb the
// blocking while leaving ready tasks waiting in the queue, which in measurements only raised tail latency without
// saving measurable scheduling cost.
// The total worker limit is shards * maxWorkers, shared by all shards: when every worker of a shard is running a task
// (the shard has reached its own limit), its tasks are taken by a suspended worker of another shard, or a new worker
// is started in a shard that still has room (see Pool.borrow); when no worker is available anywhere in the pool the
// task is queued and the next worker to suspend comes for it (see Pool.starving). Only a saturated shard crosses
// shards; under normal load a worker takes tasks only from its own shard, because always-on work stealing raised tail
// latency in measurements. To accommodate N callbacks that block at the same time the total limit has to be greater
// than N.
// When the queue is full it falls back to running the task in a temporary goroutine that helps drain the queue, so
// submitters never block.
// A worker stays resident once started (it is never reclaimed).
// When a task panics it is logged (with the stack trace), and the goroutine running it goes on with the following tasks.
package taskpool

import (
	"log/slog"
	"math/bits"
	"runtime"
	"sync/atomic"
	_ "unsafe"

	"github.com/linfeip/fnet/internal/units"
)

// MaxWorkers is the upper bound on the number of workers per shard: suspended workers are recorded in a bitmap of
// idleWords 64-bit words.
const MaxWorkers = 512

// idleWords is the number of words in a shard's bitmap of suspended workers.
const idleWords = MaxWorkers / 64

// DefaultTaskPool is the default goroutine pool: one shard per P (based on GOMAXPROCS at package initialization), with
// at most MaxWorkers workers and a queue capacity of 8192 per shard (about 128KB per shard), so at most
// 512 × GOMAXPROCS workers, which any shard may use (see Pool.borrow). Workers start only once there are tasks, and a
// worker started stays resident, so the number of goroutines grows with the number of callbacks that block at the same
// time; a pool that only runs short tasks keeps a handful.
//
// The worker limit has to accommodate the number of callbacks that block at the same time: with the 4 workers per shard
// it used to have, a workload where every callback blocked for 1ms or more reached only 10% to 17% of the throughput of
// one goroutine per task, and with 64 about 100% (measured on 24 CPUs with 1024 connections).
//
// The capacity has to accommodate the number of connections that have a task at the same time (each fnet connection has
// at most one task at a time): when the capacity is enough the backlog stays in the queue at 16 bytes per task; only
// once it is full does each overflowing task get its own temporary goroutine, which is far more expensive, and with many
// backed-up connections the goroutines and the buffers they borrow show up by the thousand.
var DefaultTaskPool = New(runtime.GOMAXPROCS(0), MaxWorkers, 8192)

// Pool is a goroutine pool.
type Pool struct {
	shards []shard
	_      [cacheLineSize]byte // every Submit reads shards: keep it off the cache line of starving
	// starving is set when a shard had tasks queued and found no suspended worker and no room for a new one anywhere in
	// the pool; the next worker to suspend clears it and notifies every shard once (see shard.work). It is written only
	// while the whole pool is saturated.
	starving atomic.Bool
	_        [cacheLineSize - 4]byte
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
			s.slots[j].seq.Store(uint64(j))
		}
		s.mask = uint64(size - 1)
		s.wakeups = make([]chan *shard, maxWorkers)
		s.maxWorkers = int32(maxWorkers)
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
// same key always go to the same queue and are run by that shard's workers.
func (p *Pool) SubmitTo(key int, task func()) {
	p.shards[uint(key)%uint(len(p.shards))].submit(task)
}

// submit puts the task into this shard's queue and makes sure a worker comes for it; when the queue is full a
// temporary goroutine runs the task and helps drain the queue.
func (s *shard) submit(task func()) {
	if !s.push(task) {
		go s.help(task)
		return
	}
	s.notify()
}

// SubmitBatch submits a batch asynchronously; it keeps no reference to the tasks slice after returning, so the caller
// may immediately clear and reuse it.
// Within a shard the tasks still run one by one, a batch never reserves tasks that have not run yet, and it shares
// the same overflow-helping policy as Submit.
func (p *Pool) SubmitBatch(tasks []func()) {
	if len(tasks) == 0 {
		return
	}
	if len(tasks) == 1 {
		p.Submit(tasks[0])
		return
	}
	count := len(p.shards)
	// Small batches keep their random placement: force-spreading them across shards would only increase the number of
	// wakeups for queued work.
	// The common case of <=64 shards merges notifications with a stack bitmap; wider pools fall back to the
	// one-task-at-a-time path.
	if len(tasks) < count {
		if count > 64 {
			for _, task := range tasks {
				p.Submit(task)
			}
			return
		}
		var ready uint64
		for _, task := range tasks {
			index := cheaprandn(uint32(count))
			s := &p.shards[index]
			if !s.push(task) {
				go s.help(task)
			} else {
				ready |= 1 << index
			}
		}
		for ready != 0 {
			p.shards[bits.TrailingZeros64(ready)].notify()
			ready &= ready - 1
		}
		return
	}
	start := int(cheaprandn(uint32(count)))
	for i := range min(count, len(tasks)) {
		s := &p.shards[(start+i)%count]
		for j := i; j < len(tasks); j += count {
			if !s.push(tasks[j]) {
				go s.help(tasks[j])
			}
		}
		s.notify()
	}
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
			buf := make([]byte, 64*units.KB)
			slog.Error("taskpool: panic in task", "panic", err, "stack", string(buf[:runtime.Stack(buf, false)]))
		}
	}()
	task()
}

// slot is one cell of the queue. For task number pos (at position pos&mask): seq equal to pos means it can be written,
// equal to pos+1 means it has been written and can be taken out, and after it is taken out seq is set to pos+capacity,
// left for the next lap.
type slot struct {
	seq  atomic.Uint64
	task func()
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
	head       atomic.Uint64 // the position the next task is taken from
	_          [cacheLineSize - 8]byte
	// wakingWorkers is the number of workers (of this shard, or lent by another) that have been woken or newly started
	// for this queue but have not reached it yet: once they get there they will take the queued tasks, so there is no
	// need to wake another worker.
	wakingWorkers atomic.Int32
	liveWorkers   atomic.Int32             // the number of started workers, numbered 0 through liveWorkers-1
	idleWorkers   [idleWords]atomic.Uint64 // the suspended workers, bit i%64 of word i/64 standing for id i
	_             [cacheLineSize]byte
}

// push puts a task into the queue and returns false when the queue is full.
func (s *shard) push(task func()) bool {
	pos := s.tail.Load()
	for {
		sl := &s.slots[pos&s.mask]
		switch seq := sl.seq.Load(); {
		case seq == pos:
			if s.tail.CompareAndSwap(pos, pos+1) {
				sl.task = task
				sl.seq.Store(pos + 1)
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
		switch seq := sl.seq.Load(); {
		case seq == pos+1:
			if s.head.CompareAndSwap(pos, pos+1) {
				task := sl.task
				sl.task = nil // stop referencing the task that was taken out
				sl.seq.Store(pos + s.mask + 1)
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
	return s.slots[pos&s.mask].seq.Load() == pos+1
}

// notify makes sure a worker comes for the tasks in the queue: when a worker is already on its way (holding
// wakingWorkers) it does nothing; otherwise it wakes a suspended worker of this shard or starts a new one, and when
// the shard is at its limit with every worker running a task it borrows one from another shard (see Pool.borrow). The
// worker it finds takes over the wakingWorkers this function acquired and releases them on reaching the queue; on the
// failure path this function releases them. When the whole pool has no available worker it sets starving, and the next
// worker to suspend comes for the tasks.
//
// Wakeups are never lost. Within a shard: the submitter writes the task before looking at the workers and a worker
// registers as suspended before looking at the queue, so at least one side sees the other. Across shards: this sets
// wakingWorkers and starving before looking once more for suspended workers pool-wide, while a worker registers as
// suspended before reading starving, so either it sees the flag (by then wakingWorkers has been released, and its
// notify can acquire it) or the second pass here sees it and retries.
func (s *shard) notify() {
	p := s.pool
	for s.hasTask() && s.wakingWorkers.Load() == 0 && s.wakingWorkers.CompareAndSwap(0, 1) {
		if s.wakeIdle(s) || s.start(s) || p.borrow(s) {
			return
		}
		s.wakingWorkers.Add(-1)
		if !p.starving.Load() {
			p.starving.Store(true)
		}
		if !p.hasIdleWorker() {
			return
		}
	}
}

// wakeIdle wakes the suspended worker of s with the lowest id (its stack and caches are the hottest) to serve target,
// handing it the target's wakingWorkers the caller holds; it reports false when s has none suspended.
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

// start starts a new worker of s that serves target first, handing it the target's wakingWorkers the caller holds; it
// reports false when s has maxWorkers workers already.
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
// target's wakingWorkers the caller holds; borrow reports false when the whole pool has none to give.
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
	// The loop never ends; the only way out is a task calling runtime.Goexit (which recover cannot stop). A replacement
	// worker is then started with the same id, on its way back to the same queue: it is still counted in liveWorkers,
	// and without the replacement this shard would be one worker short forever.
	defer func() {
		target.wakingWorkers.Add(1)
		go s.work(id, wakeup, target)
	}()
	p := s.pool
	word, bit := &s.idleWorkers[id/64], uint64(1)<<(id%64)
	for {
		target.wakingWorkers.Add(-1) // the queue has been reached
		for task := target.pop(); task != nil; task = target.pop() {
			// While tasks are still queued, first make sure a worker comes to take them, then run this one: it may take
			// a while, and the tasks behind it need not wait for it.
			if target.hasTask() {
				target.notify()
			}
			run(task)
		}
		word.Or(bit)
		// Look at the queue once more after registering as suspended: a submitter may have enqueued only after the last
		// pop above, at a time when this worker was not yet visible as suspended.
		if s.hasTask() {
			s.notify()
		}
		// A shard found no worker anywhere in the pool: now that this one is suspended, notify every shard once. The
		// flag is cleared first, so a shard that still finds none sets it again.
		if p.starving.Load() && p.starving.CompareAndSwap(true, false) {
			for i := range p.shards {
				p.shards[i].notify()
			}
		}
		target = <-wakeup
	}
}

// help runs task when the queue is full, then helps take the remaining tasks in the queue before exiting.
func (s *shard) help(task func()) {
	for ; task != nil; task = s.pop() {
		run(task)
	}
}
