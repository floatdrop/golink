package grpcproctest_test

import (
	"errors"
	"testing"
	"testing/synctest"

	"google.golang.org/protobuf/proto"

	"github.com/floatdrop/grpcproc"
	"github.com/floatdrop/grpcproc/grpcproctest"
	"github.com/floatdrop/grpcproc/internal/testpb"
)

// A cluster runs inside a testing/synctest bubble, links and all: its
// in-memory connections block where the bubble sees them, so synctest.Wait
// returns once a partition's Downs have arrived, with no sleep to guess at.
func TestClusterInSynctestBubble(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		c := grpcproctest.New(t, "a", "b")
		a := c.Node("a")
		echo, err := c.Node("b").Spawn(func(p *grpcproc.Process[*testpb.Ping]) error {
			for {
				m, err := p.Receive()
				if err != nil {
					return err
				}
				_ = m.Reply(&testpb.Pong{N: m.Body.N + 1}, nil)
			}
		}, grpcproc.WithName("echo"))
		if err != nil {
			t.Fatal(err)
		}
		downs := make(chan string, 1)
		_, _ = a.Spawn(func(p *grpcproc.Process[proto.Message]) error {
			p.Monitor(echo)
			m, err := p.Receive()
			if err != nil {
				return err
			}
			downs <- m.Down.Reason
			return nil
		})
		synctest.Wait()

		c.Partition("a", "b")
		synctest.Wait()
		select {
		case reason := <-downs:
			if reason != grpcproc.ReasonNoConnection {
				t.Fatalf("down %q", reason)
			}
		default:
			t.Fatal("no Down once the partition settled")
		}
		if _, err := echo.Call[*testpb.Pong](t.Context(), a, &testpb.Ping{N: 1}); !errors.Is(err, grpcproc.ErrNoConnection) {
			t.Fatalf("call across a partition: %v", err)
		}

		c.Heal("a", "b")
		if r, err := echo.Call[*testpb.Pong](t.Context(), a, &testpb.Ping{N: 1}); err != nil || r.N != 2 {
			t.Fatalf("call after heal: %v %v", r, err)
		}

		c.Kill("b")
		c.Restart("b")
		if _, err := echo.Call[*testpb.Pong](t.Context(), a, &testpb.Ping{N: 1}); !errors.Is(err, grpcproc.ErrNoProc) {
			t.Fatalf("call to a process of a killed incarnation: %v", err)
		}
	})
}
