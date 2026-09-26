package reactor

import (
	"sync"
	"sync/atomic"
	"time"
)

// Close deadlines. Every loop keeps a hashed timing wheel of the connections it
// must close at a given time: input that sat too long (set by the protocol with
// SetCloseDeadline), or a Close whose queued output the peer stopped reading.
// A connection sits in at most one slot, on an intrusive list, so arming,
// moving and cancelling a deadline are O(1) and allocate nothing. The loop only
// wakes per tick while its wheel is non-empty.

const (
	tickNanos  = int64(100 * time.Millisecond)
	wheelSlots = 2048 // one rotation spans ~205s; later deadlines wait a round
	wheelMask  = wheelSlots - 1
)

// Variables only so tests can shorten them.
var (
	// drainStall is how long a closing connection, or a blocking writer, may
	// go without the peer taking any queued output before it gives up.
	drainStall = 30 * time.Second
	// lingerTimeout is how long a closed connection whose output is delivered
	// waits for the peer to finish sending (see Conn.drained).
	lingerTimeout = 500 * time.Millisecond
)

type wheel struct {
	mu    sync.Mutex
	slots [wheelSlots]*Conn
	cur   int64        // last tick processed
	n     atomic.Int64 // connections on the wheel
}

// set moves c to deadline (unix nanoseconds); 0 takes it off the wheel. It
// reports whether the wheel was empty before, i.e. the loop may be blocked
// without a timeout and must be woken.
func (w *wheel) set(c *Conn, deadline int64) (wasEmpty bool) {
	w.mu.Lock()
	w.unlinkLocked(c)
	if deadline > 0 {
		t := (deadline + tickNanos - 1) / tickNanos
		if t <= w.cur {
			t = w.cur + 1
		}
		slot := t & wheelMask
		c.tdeadline, c.tslot = deadline, int32(slot)+1
		c.tprev, c.tnext = nil, w.slots[slot]
		if c.tnext != nil {
			c.tnext.tprev = c
		}
		w.slots[slot] = c
		wasEmpty = w.n.Add(1) == 1
	}
	w.mu.Unlock()
	return wasEmpty
}

func (w *wheel) unlinkLocked(c *Conn) {
	if c.tslot == 0 {
		return
	}
	if c.tprev != nil {
		c.tprev.tnext = c.tnext
	} else {
		w.slots[c.tslot-1] = c.tnext
	}
	if c.tnext != nil {
		c.tnext.tprev = c.tprev
	}
	c.tprev, c.tnext, c.tslot, c.tdeadline = nil, nil, 0, 0
	w.n.Add(-1)
}

// expire takes every connection due at now off the wheel and appends it to dst.
func (w *wheel) expire(now int64, dst []*Conn) []*Conn {
	target := now / tickNanos
	w.mu.Lock()
	defer w.mu.Unlock()
	for i, steps := int64(1), min(target-w.cur, wheelSlots); i <= steps; i++ {
		for c := w.slots[(w.cur+i)&wheelMask]; c != nil; {
			next := c.tnext
			if c.tdeadline <= now {
				w.unlinkLocked(c)
				dst = append(dst, c)
			}
			c = next
		}
	}
	w.cur = max(w.cur, target)
	return dst
}

// untilTick reports how long the loop may block before the wheel needs it, and
// false when the wheel is empty.
func (w *wheel) untilTick(now int64) (time.Duration, bool) {
	if w.n.Load() == 0 {
		return 0, false
	}
	w.mu.Lock()
	next := (w.cur + 1) * tickNanos
	w.mu.Unlock()
	return time.Duration(max(next-now, 0)), true
}
