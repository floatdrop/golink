package golink

import (
	"context"
	"sync"
)

// queue is an unbounded multi-producer, single-consumer queue. Unbounded is
// deliberate: like Erlang, a send never blocks, which rules out distributed
// deadlocks and keeps one slow process from stalling the link it shares.
type queue[T any] struct {
	mu     sync.Mutex
	items  []T
	head   int
	closed bool
	notify chan struct{} // capacity 1; single consumer
}

func newQueue[T any]() *queue[T] { return &queue[T]{notify: make(chan struct{}, 1)} }

func (q *queue[T]) push(v T) bool {
	q.mu.Lock()
	if q.closed {
		q.mu.Unlock()
		return false
	}
	q.items = append(q.items, v)
	q.mu.Unlock()
	select {
	case q.notify <- struct{}{}:
	default:
	}
	return true
}

func (q *queue[T]) len() int {
	q.mu.Lock()
	defer q.mu.Unlock()
	return len(q.items) - q.head
}

func (q *queue[T]) tryPop() (T, bool) {
	q.mu.Lock()
	defer q.mu.Unlock()
	var zero T
	if q.head == len(q.items) {
		return zero, false
	}
	v := q.items[q.head]
	q.items[q.head] = zero
	q.head++
	switch {
	case q.head == len(q.items):
		q.items, q.head = q.items[:0], 0
	case q.head > 1024 && q.head*2 > len(q.items):
		n := copy(q.items, q.items[q.head:])
		clear(q.items[n:])
		q.items, q.head = q.items[:n], 0
	}
	return v, true
}

// pop blocks until an item is available or ctx is done.
func (q *queue[T]) pop(ctx context.Context) (T, error) {
	for {
		if v, ok := q.tryPop(); ok {
			return v, nil
		}
		select {
		case <-q.notify:
		case <-ctx.Done():
			var zero T
			return zero, context.Cause(ctx)
		}
	}
}

// drain removes and returns everything queued.
func (q *queue[T]) drain() []T {
	q.mu.Lock()
	defer q.mu.Unlock()
	out := q.items[q.head:]
	q.items, q.head = nil, 0
	return out
}

// close stops accepting items and returns what was left.
func (q *queue[T]) close() []T {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.closed = true
	rest := q.items[q.head:]
	q.items, q.head = nil, 0
	return rest
}
