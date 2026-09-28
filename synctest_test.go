package grpcproc_test

import (
	"context"
	"errors"
	"testing"
	"testing/synctest"
	"time"

	"google.golang.org/protobuf/proto"

	"github.com/floatdrop/grpcproc"
	"github.com/floatdrop/grpcproc/internal/testpb"
)

// A node runs inside a testing/synctest bubble: its processes block where
// the bubble can see them, so synctest.Wait returns once every one of them
// waits, and its timers run on the bubble's clock, which moves only when
// the test lets it.
func TestNodeInSynctestBubble(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		n, err := grpcproc.NewNode(grpcproc.Config{Name: "a", Resolver: grpcproc.StaticResolver{}})
		if err != nil {
			t.Fatal(err)
		}
		e, _ := n.Spawn(echo)
		if r, err := e.Call[*testpb.Pong](t.Context(), n, &testpb.Ping{N: 1}); err != nil || r.N != 2 {
			t.Fatalf("call %v %v", r, err)
		}

		start := time.Now()
		fired := make(chan time.Duration, 1)
		timedOut := make(chan time.Duration, 1)
		_, _ = n.Spawn(func(p *grpcproc.Process[proto.Message]) error {
			if _, err := p.ReceiveTimeout(time.Minute); !errors.Is(err, context.DeadlineExceeded) {
				return err
			}
			timedOut <- time.Since(start)
			p.SendAfter(time.Hour, p.Addr(), proto.Message(&testpb.Ping{}))
			if _, err := p.Receive(); err != nil {
				return err
			}
			fired <- time.Since(start)
			return nil
		})
		synctest.Wait()
		if len(timedOut) != 0 {
			t.Fatal("ReceiveTimeout returned before its time")
		}
		time.Sleep(time.Minute)
		synctest.Wait()
		if d := <-timedOut; d != time.Minute {
			t.Fatalf("ReceiveTimeout after %v", d)
		}
		time.Sleep(time.Hour - time.Nanosecond)
		synctest.Wait()
		if len(fired) != 0 {
			t.Fatal("SendAfter fired before its time")
		}
		time.Sleep(time.Nanosecond)
		synctest.Wait()
		if d := <-fired; d != time.Minute+time.Hour {
			t.Fatalf("SendAfter fired after %v", d)
		}

		if err := n.Stop(context.Background()); err != nil {
			t.Fatal(err)
		}
	})
}

// A bubble's clock stands still until every goroutine in it waits, and the
// default incarnation is the start time: a node of the same name made again
// at the same instant still gets a newer one, so the old one's PIDs reach
// none of the new one's processes.
func TestIncarnationsGrowWhileTheClockStandsStill(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		first, err := grpcproc.NewNode(grpcproc.Config{Name: "a", Resolver: grpcproc.StaticResolver{}})
		if err != nil {
			t.Fatal(err)
		}
		_ = first.Stop(context.Background())
		again, err := grpcproc.NewNode(grpcproc.Config{Name: "a", Resolver: grpcproc.StaticResolver{}})
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = again.Stop(context.Background()) }()
		if again.ID().Incarnation <= first.ID().Incarnation {
			t.Fatalf("incarnation %d after %d", again.ID().Incarnation, first.ID().Incarnation)
		}
	})
}
