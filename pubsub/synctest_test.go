package pubsub_test

import (
	"testing"
	"testing/synctest"

	"github.com/floatdrop/grpcproc"
	"github.com/floatdrop/grpcproc/grpcproctest"
	"github.com/floatdrop/grpcproc/internal/testpb"
	"github.com/floatdrop/grpcproc/pubsub"
)

// Topics and relays run inside a testing/synctest bubble: once
// synctest.Wait returns, every event published has reached every
// subscriber, on its node or another.
func TestTopicInSynctestBubble(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		c := grpcproctest.New(t, "a", "b")
		topic, err := pubsub.Spawn[*testpb.Ping](c.Node("a"), pubsub.Config{Buffer: 2}, grpcproc.WithName("pings"))
		if err != nil {
			t.Fatal(err)
		}
		got := make(chan int64, 8)
		_, err = c.Node("b").Spawn(func(p *grpcproc.Process[*testpb.Ping]) error {
			if _, err := pubsub.Named[*testpb.Ping]("a", "pings").Subscribe(t.Context(), p); err != nil {
				return err
			}
			for {
				m, err := p.Receive()
				if err != nil {
					return err
				}
				if m.Body != nil {
					got <- m.Body.N
				}
			}
		})
		if err != nil {
			t.Fatal(err)
		}
		synctest.Wait()
		for i := range int64(3) {
			if err := topic.Publish(t.Context(), c.Node("a"), &testpb.Ping{N: i + 1}); err != nil {
				t.Fatal(err)
			}
		}
		synctest.Wait()
		if len(got) != 3 {
			t.Fatalf("%d of 3 events arrived once the bubble settled", len(got))
		}
		for want := range int64(3) {
			if n := <-got; n != want+1 {
				t.Fatalf("event %d, want %d", n, want+1)
			}
		}
	})
}
