// Package taskpool is a goroutine pool organized into shards that reuses the goroutines running short tasks, saving the
// cost of creating and destroying a goroutine per task. DefaultTaskPool can be used directly, and websocket uses it to
// run callbacks by default.
//
// Each shard has one bounded lock-free ring queue (multi-producer, multi-consumer) and at most maxWorkers workers:
//   - a submission picks a random shard to enqueue into (the random number comes from the current thread), goes through
//     no lock at all, and submitters do not write shared data either;
//   - a suspended worker is woken only when no worker is already on its way to the queue, and if none is suspended a new
//     one is started (up to the limit);
//   - once a worker has taken a task, if the queue still holds tasks and no worker is on its way, it wakes the next one
//     before running, spreading hop by hop;
//     while tasks keep arriving a worker takes and runs them back to back without suspending, and only suspends to wait
//     for a wakeup once the queue is empty;
//   - when the queue is full a temporary goroutine is started to run the task; it then helps drain the queue before
//     exiting, so submitters never block.
//
// A worker stays resident once started and only occupies its stack while suspended; the total never exceeds the number
// of shards × maxWorkers. Tasks should finish quickly: when all the workers of one shard are running blocking tasks,
// the tasks queued behind them have to wait for one of them to finish, or for the queue to fill up and a temporary
// goroutine to run them.
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

// MaxWorkers is the upper bound on the number of workers per shard: suspended workers are recorded in a 64-bit bitmap.
const MaxWorkers = 64

// DefaultTaskPool is the default goroutine pool: one shard per P (based on GOMAXPROCS at package initialization), with
// at most 4 workers and a queue capacity of 8192 per shard (about 128KB per shard). Workers start only once there are
// tasks.
//
// The capacity has to accommodate the number of connections that have a task at the same time (each fnet connection has
// at most one task at a time): when the capacity is enough the backlog stays in the queue at 16 bytes per task; only
// once it is full does each overflowing task get its own temporary goroutine, which is far more expensive, and with many
// backed-up connections the goroutines and the buffers they borrow show up by the thousand.
var DefaultTaskPool = New(runtime.GOMAXPROCS(0), 4, 8192)

// Pool is a goroutine pool.
type Pool struct {
	shards []shard
}

// New creates a Pool: shards shards, at most maxWorkers workers per shard (1 to MaxWorkers), and a queue capacity of
// capacity rounded up to a power of two. Workers start only once there are tasks.
func New(shards, maxWorkers, capacity int) *Pool {
	maxWorkers = min(max(maxWorkers, 1), MaxWorkers)
	size := 1 << bits.Len(uint(max(capacity, 2)-1))
	p := &Pool{shards: make([]shard, max(shards, 1))}
	for i := range p.shards {
		s := &p.shards[i]
		s.slots = make([]slot, size)
		for j := range s.slots {
			s.slots[j].seq.Store(uint64(j))
		}
		s.mask = uint64(size - 1)
		s.wakeups = make([]chan struct{}, maxWorkers)
		s.maxWorkers = int32(maxWorkers)
	}
	return p
}

// Submit submits a task; it may be called from any goroutine, never blocks, and allocates no memory itself. When task
// panics it is logged, without affecting the other tasks.
func (p *Pool) Submit(task func()) {
	s := &p.shards[cheaprandn(uint32(len(p.shards)))]
	if !s.push(task) {
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
type shard struct {
	slots      []slot
	mask       uint64
	wakeups    []chan struct{} // indexed by worker id; a suspended worker waits for a wakeup on its own channel
	maxWorkers int32
	tail       atomic.Uint64 // the position the next task is written to
	head       atomic.Uint64 // the position the next task is taken from
	// wakingWorkers is the number of workers that have been woken or newly started but have not reached the queue yet:
	// once they get there they will take the queued tasks, so there is no need to wake another worker.
	wakingWorkers atomic.Int32
	liveWorkers   atomic.Int32  // the number of started workers, numbered 0 through liveWorkers-1
	idleWorkers   atomic.Uint64 // the suspended workers, bit i standing for id i
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

// notify makes sure a worker comes to take the tasks in the queue: it does nothing if a worker is already on its way to
// the queue, otherwise it wakes a suspended worker, or starts a new one if none is suspended; when all workers are
// running tasks and the limit has been reached, they come and take them once they finish their current task.
//
// No wakeup is lost: a submitter writes the task before looking at the worker state, while a worker registers itself as
// suspended before looking at the queue (see work), and the global order of the atomic operations guarantees that at
// least one of the two sides sees the other.
func (s *shard) notify() {
	if s.wakingWorkers.Load() > 0 {
		return
	}
	for {
		idle := s.idleWorkers.Load()
		if idle == 0 {
			break
		}
		// Prefer waking the lowest id: a few workers get used over and over, so their stacks and caches stay hot.
		id := bits.TrailingZeros64(idle)
		if s.idleWorkers.CompareAndSwap(idle, idle&^(1<<id)) {
			s.wakingWorkers.Add(1)
			s.wakeups[id] <- struct{}{}
			return
		}
	}
	for {
		live := s.liveWorkers.Load()
		if live >= s.maxWorkers {
			return
		}
		if s.liveWorkers.CompareAndSwap(live, live+1) {
			s.wakingWorkers.Add(1)
			wakeup := make(chan struct{}, 1)
			s.wakeups[live] = wakeup
			go s.work(live, wakeup)
			return
		}
	}
}

// work is the worker with id id: it takes and runs tasks until the queue is empty, then suspends waiting for a wakeup.
func (s *shard) work(id int32, wakeup chan struct{}) {
	// The loop never ends; the only way out is a task calling runtime.Goexit (which recover cannot stop). A replacement
	// worker is then started with the same id: it is still counted in liveWorkers, and without the replacement this
	// shard would be one worker short forever.
	defer func() {
		s.wakingWorkers.Add(1)
		go s.work(id, wakeup)
	}()
	for {
		s.wakingWorkers.Add(-1) // the queue has been reached
		for task := s.pop(); task != nil; task = s.pop() {
			// While tasks are still queued, first make sure a worker comes to take them, then run this one: it may take
			// a while, and the tasks behind it need not wait for it.
			if s.hasTask() {
				s.notify()
			}
			run(task)
		}
		s.idleWorkers.Or(1 << id)
		// Look at the queue once more after registering as suspended: a submitter may have enqueued only after the last
		// pop above, at a time when this worker was not yet visible as suspended.
		if s.hasTask() {
			s.notify()
		}
		<-wakeup
	}
}

// help runs task when the queue is full, then helps take the remaining tasks in the queue before exiting.
func (s *shard) help(task func()) {
	for ; task != nil; task = s.pop() {
		run(task)
	}
}
