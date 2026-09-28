package grpcproc

import (
	"slices"
	"testing"
	"testing/synctest"
	"time"
)

func TestQueue(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		q := newQueue[int](true)
		if _, ok := q.tryPop(); ok || q.oldestStamp() != 0 || q.takenAt.Load() != 0 {
			t.Fatal("empty")
		}
		before := time.Now().UnixNano()
		for i := 1; i <= 3; i++ {
			q.push(i)
		}
		// Before the consumer takes a batch, the oldest is when the first came.
		first := q.oldestStamp()
		if q.len() != 3 || first < before || first > time.Now().UnixNano() {
			t.Fatalf("len %d oldest %d", q.len(), first)
		}
		if v, _ := q.tryPop(); v != 1 || q.oldestStamp() != first || q.takenAt.Load() < first {
			t.Fatalf("pop %d oldest %d", v, q.oldestStamp())
		}
		q.push(4) // lands in the producers' buffer while the consumer holds 2, 3
		for want := 2; want <= 4; want++ {
			if v, ok := q.tryPop(); !ok || v != want {
				t.Fatalf("pop %d: %d %v", want, v, ok)
			}
		}
		if q.len() != 0 || q.oldestStamp() != 0 {
			t.Fatal("drained queue reports items")
		}
		// Buffers are reused: a queue that is kept up with stops allocating.
		if n := testing.AllocsPerRun(100, func() { q.push(1); q.tryPop() }); n != 0 {
			t.Fatalf("%v allocs per push and pop", n)
		}
		// What the consumer swapped in but did not pop, and what producers
		// queued since, are both returned once the queue closes.
		q.push(5)
		q.push(6)
		q.tryPop()
		q.push(7)
		rest := append(q.taken(), q.close()...)
		if len(rest) != 2 || rest[0] != 6 || rest[1] != 7 || q.len() != 0 {
			t.Fatalf("rest %v, len %d", rest, q.len())
		}
		if q.push(8) {
			t.Fatal("push after close")
		}
		// drain hands over batches.
		d := newQueue[int](false)
		d.push(1)
		d.push(2)
		if b := d.drain(); len(b) != 2 || d.len() != 0 {
			t.Fatalf("batch %v", b)
		}
		d.push(3)
		if b := d.drain(); len(b) != 1 || b[0] != 3 {
			t.Fatalf("batch %v", b)
		}
	})
}

// putBack returns items to the front of an open queue, and refuses a closed
// one.
func TestQueuePutBack(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		q := newQueue[int](false)
		q.push(3)
		if !q.putBack([]int{1, 2}) || q.len() != 3 {
			t.Fatal("put back refused")
		}
		if got := q.drain(); !slices.Equal(got, []int{1, 2, 3}) {
			t.Fatalf("got %v", got)
		}
		q.close()
		if q.putBack([]int{4}) {
			t.Fatal("put back into a closed queue")
		}
	})
}

// A queue sealed once its consumer has everything refuses what comes later,
// and one that still holds items stays open.
func TestQueueSealIfEmpty(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		q := newQueue[int](false)
		q.push(1)
		if q.sealIfEmpty() {
			t.Fatal("sealed with an item waiting")
		}
		q.drain()
		if !q.sealIfEmpty() || q.push(2) {
			t.Fatal("a sealed queue took an item")
		}
	})
}
