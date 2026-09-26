// Package pool runs fnet's business code: HTTP handlers and the WebSocket and
// TCP callbacks. It knows nothing about networking. The servers hand it at
// most one task per connection at a time (a connection runs its callbacks in
// order on its own), so the pool only bounds concurrency, without ever
// blocking the event loop that submits.
package pool

import (
	"errors"
	"log"
	"math/bits"
	"math/rand/v2"
	"runtime"
	"runtime/debug"
	"sync"
	"sync/atomic"
	"time"

	"golang.org/x/sys/cpu"
)

// ErrClosed is returned by Submit and SubmitConn after Close.
var ErrClosed = errors.New("fnet/pool: pool is closed")

// minShardWorkers is the fewest workers a shard is given: a pool too small to
// give each shard that many runs on fewer shards, down to one.
const minShardWorkers = 256

// Config configures a Pool. Zero fields take the defaults.
type Config struct {
	// MaxWorkers bounds the goroutines running tasks. The shards share it out
	// equally, and a task whose shard has all of its part busy waits, in
	// submission order, for one of them to finish. 0 means 1024 per
	// GOMAXPROCS, and at least 4096.
	MaxWorkers int
	// IdleTimeout is how long a worker without a task waits for one before
	// exiting (it exits within half as long again), so an idle pool holds no
	// goroutine. 0 means 5s.
	IdleTimeout time.Duration
}

// Pool is a bounded, elastic goroutine pool. Workers start on demand, up to
// MaxWorkers, and exit once idle for IdleTimeout; tasks beyond the bound queue
// instead of spawning more goroutines. A panicking task is logged and does not
// take its worker down. Submit never blocks.
//
// The pool is split into shards, one per core or so, each with its own lock,
// queue and workers, so that event loops and workers on different cores rarely
// meet on a lock. SubmitConn keeps a connection on one shard; each shard is
// given an equal part of MaxWorkers, at least minShardWorkers.
//
// A shard keeps as few workers as keep its queue moving: a worker that
// finishes a task takes the next one without parking, a parked worker is woken
// only for a task no other worker is coming for, and a new one is started only
// when every worker the shard has is busy, one at a time. Under load the busy
// workers carry the tasks; a burst, or tasks that block, bring in more. A
// worker about to park first takes a task another shard has waiting, so no
// shard's queue waits behind its own busy workers while others idle.
type Pool struct {
	shards []shard
	shift  uint // 64 - log2(len(shards)): an index is the top bits of a hash
}

// shard is one independent part of a Pool. Every decision is made under mu: to
// queue a task, to wake or start a worker for it, and to retire one. So a
// queued task always has a worker coming for it (woken, starting, or busy and
// about to look again), and none is ever waiting on a worker that left.
type shard struct {
	pool    *Pool
	mu      sync.Mutex
	queue   fifo      // tasks not taken yet; its length may be read without mu
	idle    []*worker // parked workers, oldest first; the last one parked is woken first
	running int       // live workers: busy, parked or on their way
	waking  int       // workers woken or started that have not looked at the queue yet
	max     int
	closed  bool
	reaping bool   // reaper is armed: it is while running > 0
	tick    uint64 // reaper ticks so far

	period time.Duration // between reaper ticks: half of IdleTimeout
	reaper *time.Timer
	done   sync.WaitGroup
	_      cpu.CacheLinePad
}

// worker is a parked goroutine's wake-up call: true to look for work, false to
// exit. It gets at most one while parked.
type worker struct {
	wake chan bool
	tick uint64 // the shard's tick when it parked
}

// New creates a pool.
func New(cfg Config) *Pool {
	procs := runtime.GOMAXPROCS(0)
	maxWorkers := cfg.MaxWorkers
	if maxWorkers <= 0 {
		maxWorkers = max(4096, 1024*procs)
	}
	idleTimeout := cfg.IdleTimeout
	if idleTimeout <= 0 {
		idleTimeout = 5 * time.Second
	}
	// The largest power of two within both the core count (rounded up) and
	// the number of full-sized shards the bound allows.
	n := min(1<<bits.Len(uint(procs-1)), max(1, maxWorkers/minShardWorkers))
	n = 1 << (bits.Len(uint(n)) - 1)
	p := &Pool{shards: make([]shard, n), shift: uint(65 - bits.Len(uint(n)))}
	for i := range p.shards {
		s := &p.shards[i]
		s.pool = p
		s.max = maxWorkers / n
		if i < maxWorkers%n {
			s.max++
		}
		s.period = max(idleTimeout/2, time.Millisecond)
	}
	return p
}

// Submit runs task on a worker of a shard picked at random: a busy one that
// finishes, a parked one, or a new one while the shard has room. It fails only
// after Close.
func (p *Pool) Submit(task func()) error {
	return p.shards[rand.Uint64()>>p.shift].submit(task)
}

// SubmitConn is Submit for the servers' WorkerPool: the tasks of one
// connection stay on one shard.
func (p *Pool) SubmitConn(connID uint64, task func()) error {
	// Fibonacci hashing spreads sequential ids (fds, counters) evenly.
	return p.shards[connID*0x9e3779b97f4a7c15>>p.shift].submit(task)
}

func (s *shard) submit(task func()) error {
	if task == nil {
		return nil
	}
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return ErrClosed
	}
	s.queue.push(task)
	w, spawn := s.wakeLocked()
	s.mu.Unlock()
	s.start(w, spawn)
	return nil
}

// wakeLocked gets workers on their way to the queue: a parked worker for each
// queued task no worker is coming for yet, or, with none parked, a new worker
// while the shard has room, one at a time: a pool grows only while every
// worker it has is busy. With every worker busy the first to finish looks at
// the queue. The caller wakes or starts the worker with start, after
// unlocking.
func (s *shard) wakeLocked() (w *worker, spawn bool) {
	switch {
	case s.waking >= s.queue.len():
	case len(s.idle) > 0:
		w = s.idle[len(s.idle)-1]
		s.idle[len(s.idle)-1] = nil
		s.idle = s.idle[:len(s.idle)-1]
		s.waking++
	case s.waking == 0 && s.running < s.max:
		s.running++
		s.waking++
		s.done.Add(1)
		if !s.reaping {
			s.reaping = true
			if s.reaper == nil {
				s.reaper = time.AfterFunc(s.period, s.reap)
			} else {
				s.reaper.Reset(s.period)
			}
		}
		spawn = true
	}
	return w, spawn
}

func (s *shard) start(w *worker, spawn bool) {
	switch {
	case w != nil:
		w.wake <- true // never blocks: a parked worker has nothing pending
	case spawn:
		go s.work()
	}
}

func (s *shard) work() {
	defer s.done.Done()
	var w *worker
	s.mu.Lock()
	s.waking--
	for {
		task := s.queue.pop()
		if task == nil && !s.closed {
			// Nothing here: help a shard whose tasks wait before parking.
			// Meanwhile this worker counts as on its way, so a task queued
			// here now does not wake or start another.
			s.waking++
			s.mu.Unlock()
			task = s.pool.steal(s)
			s.mu.Lock()
			s.waking--
			if task == nil {
				task = s.queue.pop() // queued here while it looked
			}
		}
		if task != nil {
			// Any still waiting here get a worker moving before this task
			// runs, however long it takes.
			next, spawn := s.wakeLocked()
			s.mu.Unlock()
			s.start(next, spawn)
			runSafe(task)
			s.mu.Lock()
			continue
		}
		if s.closed {
			s.running--
			s.mu.Unlock()
			return
		}
		if w == nil {
			w = &worker{wake: make(chan bool, 1)}
		}
		w.tick = s.tick
		s.idle = append(s.idle, w)
		s.mu.Unlock()
		if !<-w.wake {
			return // retired: whoever said so counted it out
		}
		s.mu.Lock()
		s.waking--
	}
}

// steal takes a task queued in a shard other than from, if there is one.
func (p *Pool) steal(from *shard) func() {
	n := len(p.shards)
	start := int(rand.Uint64() >> p.shift)
	for i := range n {
		v := &p.shards[(start+i)&(n-1)]
		if v == from || v.queue.len() == 0 {
			continue
		}
		v.mu.Lock()
		task := v.queue.pop()
		v.mu.Unlock()
		if task != nil {
			return task
		}
	}
	return nil
}

// reap retires the workers that have been parked for a whole IdleTimeout: the
// ones parked three ticks ago or earlier, which sit at the bottom of the idle
// stack. It runs every period while the shard has workers.
func (s *shard) reap() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return
	}
	s.tick++
	n := 0
	for n < len(s.idle) && s.idle[n].tick+3 <= s.tick {
		s.idle[n].wake <- false
		n++
	}
	if n > 0 {
		s.running -= n
		rest := copy(s.idle, s.idle[n:])
		clear(s.idle[rest:])
		s.idle = s.idle[:rest]
		if cap(s.idle) > 64 && rest < cap(s.idle)/4 {
			s.idle = append([]*worker(nil), s.idle...) // a burst is over: give its array back
		}
	}
	if s.reaping = s.running > 0; s.reaping {
		s.reaper.Reset(s.period)
	}
}

// Close stops the pool: tasks already submitted still run, later ones are
// refused, and Close returns once every worker has exited.
func (p *Pool) Close() {
	for i := range p.shards {
		s := &p.shards[i]
		s.mu.Lock()
		if !s.closed {
			s.closed = true
			for _, w := range s.idle {
				w.wake <- false
			}
			s.running -= len(s.idle)
			s.idle = nil
			if s.reaper != nil {
				s.reaper.Stop()
			}
		}
		s.mu.Unlock()
	}
	for i := range p.shards {
		p.shards[i].done.Wait()
	}
}

// RunningWorkers returns the number of live workers, busy or idle.
func (p *Pool) RunningWorkers() int {
	n := 0
	for i := range p.shards {
		s := &p.shards[i]
		s.mu.Lock()
		n += s.running
		s.mu.Unlock()
	}
	return n
}

// IdleWorkers returns the number of workers waiting for a task.
func (p *Pool) IdleWorkers() int {
	n := 0
	for i := range p.shards {
		s := &p.shards[i]
		s.mu.Lock()
		n += len(s.idle)
		s.mu.Unlock()
	}
	return n
}

// runSafe runs a task, logging a panic instead of losing the worker. The
// servers recover their callbacks' panics themselves; one reaching here is a
// task submitted directly, or a bug.
func runSafe(task func()) {
	defer func() {
		if r := recover(); r != nil {
			log.Printf("fnet/pool: task panicked: %v\n%s", r, debug.Stack())
		}
	}()
	task()
}

// fifo is a growable ring buffer of tasks. Only len may be called without the
// shard's lock.
type fifo struct {
	buf  []func()
	head int
	n    atomic.Int32
}

func (q *fifo) len() int { return int(q.n.Load()) }

func (q *fifo) push(t func()) {
	n := q.len()
	if n == len(q.buf) {
		buf := make([]func(), max(16, 2*len(q.buf)))
		for i := range n {
			buf[i] = q.buf[(q.head+i)%len(q.buf)]
		}
		q.buf, q.head = buf, 0
	}
	q.buf[(q.head+n)%len(q.buf)] = t
	q.n.Store(int32(n + 1))
}

// pop removes the oldest task, or returns nil when there is none.
func (q *fifo) pop() func() {
	n := q.len()
	if n == 0 {
		return nil
	}
	t := q.buf[q.head]
	q.buf[q.head] = nil
	q.head = (q.head + 1) % len(q.buf)
	q.n.Store(int32(n - 1))
	if n == 1 && len(q.buf) > 1024 {
		q.buf, q.head = nil, 0 // a burst is over: give its array back
	}
	return t
}

// Default returns the process-wide pool used when no custom pool is
// configured. It is created on first use.
func Default() *Pool {
	if p := defaultPool.Load(); p != nil {
		return p
	}
	defaultOnce.Do(func() { defaultPool.CompareAndSwap(nil, New(Config{})) })
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

// Adapt turns a plain submit function, such as ants.Submit, into the
// connection-keyed form the servers take as their WorkerPool, ignoring the
// key.
func Adapt(submit func(task func()) error) func(connID uint64, task func()) error {
	if submit == nil {
		return nil
	}
	return func(_ uint64, task func()) error { return submit(task) }
}

// Dispatch runs task through submit, or through Default when submit is nil,
// keyed by connID. It reports whether the task was accepted; a submit that
// fails, or panics, refuses it and the caller should give up on the
// connection.
func Dispatch(submit func(connID uint64, task func()) error, connID uint64, task func()) (ok bool) {
	if submit == nil {
		return Default().SubmitConn(connID, task) == nil
	}
	defer func() {
		if recover() != nil {
			ok = false
		}
	}()
	return submit(connID, task) == nil
}
