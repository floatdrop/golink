package grpcproc

import (
	"slices"
	"sync"
	"sync/atomic"
	"time"
)

// queue is an unbounded multi-producer, single-consumer queue. Unbounded is
// deliberate: like Erlang, a send never blocks, which rules out distributed
// deadlocks and keeps one slow process from stalling the link it shares.
//
// Producers append to in under the mutex. The consumer owns out: when it
// runs dry, it swaps in for it, so the consumer takes the lock once per batch
// rather than once per item, and the two buffers are reused, so a queue that
// is kept up with allocates nothing.
//
// A stamped queue also knows how long its oldest item has waited and when
// the consumer took what it is working on, reading the clock once per batch
// rather than per item: when in goes from empty to holding something (the
// oldest item of that batch, exactly), and when the consumer takes a batch.
type queue[T any] struct {
	mu     sync.Mutex
	in     []T
	closed bool
	notify chan struct{} // capacity 1; single consumer

	out  []T // consumer only
	head int // consumer only

	// Producers count under mu; the consumer publishes what it has taken
	// with a plain store. No counter is written by both sides, so the send
	// path has no contended atomic.
	pushed int64        // guarded by mu
	popped atomic.Int64 // written by the consumer only
	peak   atomic.Int64 // the most queued at a batch swap

	stamped  bool
	inSince  int64        // guarded by mu: when in stopped being empty
	outSince atomic.Int64 // when the consumer's batch started queueing; 0 when used up
	takenAt  atomic.Int64 // when the consumer took its batch; 0 before the first
}

// newQueue returns a queue; a stamped one tracks the ages oldestStamp and
// takenAt report.
func newQueue[T any](stamped bool) *queue[T] {
	return &queue[T]{notify: make(chan struct{}, 1), stamped: stamped}
}

func (q *queue[T]) push(v T) bool {
	q.mu.Lock()
	if q.closed {
		q.mu.Unlock()
		return false
	}
	if q.stamped && len(q.in) == 0 {
		q.inSince = time.Now().UnixNano()
	}
	q.in = append(q.in, v)
	q.pushed++
	q.mu.Unlock()
	select {
	case q.notify <- struct{}{}:
	default:
	}
	return true
}

// len is how many items are queued, taken by neither tryPop nor drain.
func (q *queue[T]) len() int {
	q.mu.Lock()
	defer q.mu.Unlock()
	return int(q.pushed - q.popped.Load())
}

// tryPop takes the next item. Consumer only.
func (q *queue[T]) tryPop() (T, bool) {
	var zero T
	if q.head == len(q.out) && !q.refill() {
		return zero, false
	}
	v := q.out[q.head]
	q.out[q.head] = zero
	q.head++
	q.popped.Store(q.popped.Load() + 1)
	if q.stamped && q.head == len(q.out) {
		q.outSince.Store(0)
	}
	return v, true
}

// refill swaps the producers' buffer for the consumer's used-up one, whose
// slots tryPop has already cleared. Consumer only.
func (q *queue[T]) refill() bool {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.out, q.in = q.in, q.out[:0]
	q.head = 0
	if n := int64(len(q.out)); n > q.peak.Load() {
		q.peak.Store(n)
	}
	if q.stamped && len(q.out) > 0 {
		q.outSince.Store(q.inSince)
		q.takenAt.Store(time.Now().UnixNano())
	}
	return len(q.out) > 0
}

// oldestStamp is when the oldest queued item was queued, in unix nanos, 0
// if none: exactly for the first item of a batch, and at most that long ago
// for the rest. Stamped queues only.
func (q *queue[T]) oldestStamp() int64 {
	q.mu.Lock()
	defer q.mu.Unlock()
	if o := q.outSince.Load(); o != 0 {
		return o
	}
	if len(q.in) > 0 {
		return q.inSince
	}
	return 0
}

// drain takes everything queued, for a consumer that handles batches. The
// slice belongs to the queue: use it before the next drain. Consumer only;
// do not mix with tryPop.
func (q *queue[T]) drain() []T {
	clear(q.out)
	q.mu.Lock()
	q.out, q.in = q.in, q.out[:0]
	q.popped.Store(q.popped.Load() + int64(len(q.out)))
	q.mu.Unlock()
	return q.out
}

// putBack returns items the consumer took but did not use to the front of
// the queue, unless it is closed; it reports whether it did. Consumer only;
// not for stamped queues.
func (q *queue[T]) putBack(items []T) bool {
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.closed {
		return false
	}
	q.in = append(slices.Clone(items), q.in...) // items may share the consumer's buffer
	q.pushed += int64(len(items))
	return true
}

// sealIfEmpty stops accepting items if none are waiting, and reports
// whether it did. Consumer only.
func (q *queue[T]) sealIfEmpty() bool {
	q.mu.Lock()
	defer q.mu.Unlock()
	if len(q.in) > 0 {
		return false
	}
	q.closed = true
	return true
}

// close stops accepting items and returns those not yet swapped to the
// consumer. Any goroutine.
func (q *queue[T]) close() []T {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.closed = true
	rest := q.in
	q.in = nil
	q.pushed -= int64(len(rest))
	return rest
}

// taken returns what the consumer swapped in but has not popped. Consumer only.
func (q *queue[T]) taken() []T {
	rest := q.out[q.head:]
	q.popped.Store(q.popped.Load() + int64(len(rest)))
	q.out, q.head = nil, 0
	return rest
}
