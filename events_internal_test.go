package grpcproc

import (
	"context"
	"runtime"
	"testing"
	"testing/synctest"
	"time"
)

func TestSubscribersEdges(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var s subscribers
		sub := &subscriber{ch: make(chan Event, 1)}
		s.remove(sub) // nothing registered yet
		s.add(sub)
		sub.close()
		sub.close()        // idempotent
		s.publish(Event{}) // delivering to a closed subscriber is a no-op
		if len(sub.ch) != 0 {
			t.Fatal("delivered to a closed subscriber")
		}
	})
}

// A subscription lets go of its subscriber and buffer as soon as it ends,
// whichever ends it: its ctx, before the node stops, or the node, while a
// long-lived ctx goes on.
func TestSubscribeLetsGoWhenItEnds(t *testing.T) {
	// On real time, not in a synctest bubble: runtime.AddCleanup runs its
	// function outside any bubble, and could not close a channel made in one.
	func() {
		for _, nodeFirst := range []bool{false, true} {
			n := newTestNode(t, "a")
			ctx, cancel := context.WithCancel(context.Background())
			events := n.Subscribe(ctx, 16)
			released := make(chan struct{})
			last := func() *subscriber { subs := *n.subs.list.Load(); return subs[len(subs)-1] }
			runtime.AddCleanup(last(), func(struct{}) { close(released) }, struct{}{})
			if nodeFirst {
				_ = n.Stop(context.Background())
			} else {
				cancel()
			}
			for range events {
			}
			for deadline := time.Now().Add(5 * time.Second); ; {
				runtime.GC()
				select {
				case <-released:
				case <-time.After(10 * time.Millisecond):
					if time.Now().After(deadline) {
						t.Fatalf("node first %v: the subscriber is still referenced", nodeFirst)
					}
					continue
				}
				break
			}
			cancel()
		}
	}()
}
