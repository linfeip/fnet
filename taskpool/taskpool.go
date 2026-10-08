// Package taskpool is a goroutine pool organized into shards that reuses the goroutines running short tasks, saving the
// cost of creating and destroying a goroutine per task. DefaultTaskPool can be used directly, and websocket uses it to
// run callbacks by default.
//
// 每个分片使用有界无锁队列，最多保留 maxWorkers 个 worker。大批次均匀分到各分片，
// 小批次保留随机选择并合并通知；每个分片入队后只判断一次唤醒。本分片队列空后 worker 先从其他分片取任务，
// 所有队列都空才挂起。普通负载的运行 worker
// 软预算为创建时的 GOMAXPROCS；
// 没有 worker 的分片始终可以启动一个，避免其他分片耗尽预算后饿死它。
// 队列积压且停止推进时，按需监测允许在分片原有硬上限内扩容，隔离阻塞回调；空队列不自旋。
// 队列满时沿用临时 goroutine 执行并协助排空的策略，提交者不阻塞。
// worker 启动后保持驻留；监测 goroutine 每个池至多一个，只在有积压时定时检查，否则挂起。
// When a task panics it is logged (with the stack trace), and the goroutine running it goes on with the following tasks.
package taskpool

import (
	"log/slog"
	"math/bits"
	"runtime"
	"sync"
	"sync/atomic"
	"time"
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
	shards        []shard
	targetWorkers int32
	monitorActive atomic.Bool
	monitorOnce   sync.Once
	monitorWake   chan struct{}
	_             [cacheLineSize]byte
	// runningWorkers changes on every worker wakeup and suspension, so it keeps off the cache line of the fields above,
	// which every submission and every steal reads.
	runningWorkers atomic.Int32 // 已预留、待唤醒或正在执行的 worker；不包含队列溢出的临时 goroutine
	_              [cacheLineSize - 4]byte
}

// New creates a Pool: shards shards, at most maxWorkers workers per shard (1 to MaxWorkers), and a queue capacity of
// capacity rounded up to a power of two. Workers start only once there are tasks.
func New(shards, maxWorkers, capacity int) *Pool {
	maxWorkers = min(max(maxWorkers, 1), MaxWorkers)
	size := 1 << bits.Len(uint(max(capacity, 2)-1))
	p := &Pool{
		shards:        make([]shard, max(shards, 1)),
		targetWorkers: int32(min(runtime.GOMAXPROCS(0), max(shards, 1)*maxWorkers)),
		monitorWake:   make(chan struct{}, 1),
	}
	for i := range p.shards {
		s := &p.shards[i]
		s.pool = p
		s.targetWorkers = int32(min(maxWorkers, (int(p.targetWorkers)+len(p.shards)-1)/len(p.shards)))
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

// Submit 异步提交任务，不阻塞调用者；worker 和监测已预热、不触发扩容或溢出时不分配。
// 任务 panic 会被记录，不影响后续任务。
func (p *Pool) Submit(task func()) {
	s := &p.shards[cheaprandn(uint32(len(p.shards)))]
	if !s.push(task) {
		s.overflowed.Store(true)
		go s.help(task)
		return
	}
	s.notify(false)
}

// SubmitBatch 异步提交一个批次；返回前不保留 tasks 切片，调用方可以立即清空并复用。
// 分片内任务仍逐个执行，每批不独占尚未执行的任务，与 Submit 共享同一溢出协助策略。
func (p *Pool) SubmitBatch(tasks []func()) {
	if len(tasks) == 0 {
		return
	}
	if len(tasks) == 1 {
		p.Submit(tasks[0])
		return
	}
	count := len(p.shards)
	// 小批次保留随机选择，避免强行散到不同分片反而增加挂起队列的唤醒数。
	// 常见 <=64 分片使用栈上位图合并通知；更宽的池退回原有单任务路径。
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
				s.overflowed.Store(true)
				go s.help(task)
			} else {
				ready |= 1 << index
			}
		}
		for ready != 0 {
			p.shards[bits.TrailingZeros64(ready)].notify(false)
			ready &= ready - 1
		}
		return
	}
	start := int(cheaprandn(uint32(count)))
	for i := range min(count, len(tasks)) {
		s := &p.shards[(start+i)%count]
		for j := i; j < len(tasks); j += count {
			if !s.push(tasks[j]) {
				s.overflowed.Store(true)
				go s.help(tasks[j])
			}
		}
		s.notify(false)
	}
}

// reserveWorker 预留运行名额；首个 worker 或积压监测可以越过软预算，分片硬上限仍由 notify 检查。
func (p *Pool) reserveWorker(force bool) bool {
	if force {
		p.runningWorkers.Add(1)
		return true
	}
	for {
		running := p.runningWorkers.Load()
		if running >= p.targetWorkers {
			return false
		}
		if p.runningWorkers.CompareAndSwap(running, running+1) {
			return true
		}
	}
}

func (p *Pool) watchBacklog() {
	if p.monitorActive.Load() || !p.monitorActive.CompareAndSwap(false, true) {
		return
	}
	p.monitorOnce.Do(func() { go p.monitor() })
	p.monitorWake <- struct{}{}
}

// monitor 只对有待取任务的队列采样 head；连续两次没有推进才认为需要额外 worker。
// head 是原有出队计数，无需在每个任务上新增进度原子操作或读取时间。
func (p *Pool) monitor() {
	heads := make([]uint64, len(p.shards))
	for range p.monitorWake {
		for i := range p.shards {
			heads[i] = p.shards[i].head.Load()
		}
		for {
			time.Sleep(time.Millisecond)
			pending := false
			for i := range p.shards {
				s := &p.shards[i]
				head := s.head.Load()
				if s.hasTask() && (s.runningWorkers.Load() < s.maxWorkers || s.overflowed.Load()) {
					pending = true
					if head == heads[i] {
						s.notify(true)
						if s.overflowed.Load() && s.runningWorkers.Load() >= s.maxWorkers {
							if task := s.pop(); task != nil {
								go s.help(task)
							}
						}
					} else if p.runningWorkers.Load() < p.targetWorkers {
						s.notify(false)
					}
				}
				heads[i] = head
			}
			if pending {
				continue
			}
			p.monitorActive.Store(false)
			// 与提交者配对再检查，避免它在 active 清零前看到旧值而漏掉监测。
			for i := range p.shards {
				s := &p.shards[i]
				if s.hasTask() && (s.runningWorkers.Load() < s.maxWorkers || s.overflowed.Load()) {
					p.watchBacklog()
					break
				}
			}
			break
		}
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
const cacheLineSize = 128 // 同时覆盖常见的 64B 和 128B 缓存行

type shard struct {
	slots         []slot
	mask          uint64
	wakeups       []chan struct{} // indexed by worker id; a suspended worker waits for a wakeup on its own channel
	maxWorkers    int32
	targetWorkers int32 // 本分片在正常负载下的软额度；阻塞监测可以越过
	pool          *Pool
	_             [cacheLineSize]byte
	tail          atomic.Uint64 // the position the next task is written to
	_             [cacheLineSize - 8]byte
	head          atomic.Uint64 // the position the next task is taken from
	_             [cacheLineSize - 8]byte
	// wakingWorkers is the number of workers that have been woken or newly started but have not reached the queue yet:
	// once they get there they will take the queued tasks, so there is no need to wake another worker.
	wakingWorkers  atomic.Int32
	liveWorkers    atomic.Int32  // the number of started workers, numbered 0 through liveWorkers-1
	idleWorkers    atomic.Uint64 // the suspended workers, bit i standing for id i
	runningWorkers atomic.Int32
	overflowed     atomic.Bool // 溢出协助持续到正常 worker 恢复；避免协助退出后尾部任务滞留
	_              [cacheLineSize]byte
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

// notify 先取得唤醒权，再预留运行预算；成功后把这两个名额显式移交给 worker。
// worker 到达队列释放 wakingWorkers，挂起时释放 runningWorkers；失败路径由本函数释放。
func (s *shard) notify(force bool) {
	if !s.hasTask() || s.wakingWorkers.Load() > 0 || !s.wakingWorkers.CompareAndSwap(0, 1) {
		return
	}
	if s.liveWorkers.Load() >= s.maxWorkers && s.idleWorkers.Load() == 0 {
		s.wakingWorkers.Add(-1)
		if s.idleWorkers.Load() != 0 {
			s.notify(force)
		} else if s.overflowed.Load() && s.hasTask() {
			s.pool.watchBacklog()
		}
		return
	}
	if (!force && s.runningWorkers.Load() >= s.targetWorkers) ||
		!s.pool.reserveWorker(force || s.runningWorkers.Load() == 0) {
		s.wakingWorkers.Add(-1)
		// 最后一个 worker 可能刚挂起；立即重试，不把普通唤醒延迟到监测 tick。
		if s.runningWorkers.Load() == 0 {
			s.notify(false)
		} else if s.hasTask() {
			s.pool.watchBacklog()
		}
		return
	}
	s.runningWorkers.Add(1)
	for {
		idle := s.idleWorkers.Load()
		if idle == 0 {
			break
		}
		id := bits.TrailingZeros64(idle)
		if s.idleWorkers.CompareAndSwap(idle, idle&^(1<<id)) {
			s.wakeups[id] <- struct{}{}
			return
		}
	}
	for {
		live := s.liveWorkers.Load()
		if live >= s.maxWorkers {
			s.runningWorkers.Add(-1)
			s.pool.runningWorkers.Add(-1)
			s.wakingWorkers.Add(-1)
			// worker 可能刚公布 idle，却因本次唤醒权尚未释放而没能通知自己。
			if s.idleWorkers.Load() != 0 {
				s.notify(force)
			}
			return
		}
		if s.liveWorkers.CompareAndSwap(live, live+1) {
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
	// Goexit 的替代 worker 继承当前运行名额，只重新登记唤醒，不重复预留预算。
	defer func() {
		s.wakingWorkers.Add(1)
		go s.work(id, wakeup)
	}()
	for {
		s.wakingWorkers.Add(-1) // the queue has been reached
		for task := s.take(); task != nil; task = s.take() {
			// While tasks are still queued, first make sure a worker comes to take them, then run this one: it may take
			// a while, and the tasks behind it need not wait for it.
			if s.hasTask() {
				s.notify(false)
			}
			run(task)
		}
		s.overflowed.Store(false)
		s.runningWorkers.Add(-1)
		s.pool.runningWorkers.Add(-1)
		s.idleWorkers.Or(1 << id)
		// Look at the queue once more after registering as suspended: a submitter may have enqueued only after the last
		// pop above, at a time when this worker was not yet visible as suspended.
		if s.hasTask() {
			s.notify(false)
		}
		<-wakeup
	}
}

// take pops a task from this shard's queue or, once that is empty, steals one from another shard, so a worker only
// suspends when every queue is empty: otherwise a task waits behind its shard's busy worker while the workers of
// other shards suspend and have to be woken again, which under load costs more than the tasks themselves. Each shard
// still wakes its own workers for its own tasks (see notify), so stealing only adds consumers and loses no wakeup.
func (s *shard) take() func() {
	if task := s.pop(); task != nil {
		return task
	}
	shards := s.pool.shards
	start := int(cheaprandn(uint32(len(shards))))
	for i := range shards {
		if other := &shards[(start+i)%len(shards)]; other != s && other.hasTask() {
			if task := other.pop(); task != nil {
				return task
			}
		}
	}
	return nil
}

// help runs task when the queue is full, then helps take the remaining tasks in the queue before exiting.
func (s *shard) help(task func()) {
	for ; task != nil; task = s.pop() {
		run(task)
	}
}
