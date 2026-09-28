package leader_test

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/wrapperspb"

	"github.com/floatdrop/grpcproc"
	"github.com/floatdrop/grpcproc/grpcproctest"
	"github.com/floatdrop/grpcproc/leader"
)

// ask is leader.Call to the test singleton, from from, with v.
func ask(ctx context.Context, from grpcproc.Caller, v int64) (int64, error) {
	got, err := leader.Call[counter](ctx, from, "test", "singleton", wrapperspb.Int64(v))
	return got.GetValue(), err
}

func within(t *testing.T, d time.Duration) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), d)
	t.Cleanup(cancel)
	return ctx
}

func TestCallFollowsTheLeader(t *testing.T) {
	cluster(t, spec, func(t *testing.T, c *grpcproctest.Cluster, j *journal) {
		// Before anyone leads, it waits for someone to.
		if got, err := ask(within(t, 5*time.Second), c.Node("a"), 1); err != nil || got != 1 {
			t.Fatalf("%d, %v", got, err)
		}
		first, _ := elected(t, c, "a", "b", "c")
		f := others(first)[0]

		// From a process on a follower, as from inside its handler.
		answer := make(chan int64, 1)
		if _, err := c.Node(f).Spawn(func(p *grpcproc.Process[proto.Message]) error {
			got, err := ask(within(t, 5*time.Second), p, 2)
			if err != nil {
				return err
			}
			answer <- got
			return nil
		}); err != nil {
			t.Fatal(err)
		}
		if got := <-answer; got != 2 {
			t.Fatalf("from a process: %d", got)
		}

		// Across a hand-over: the old leader's node answers that it has no
		// singleton, and the call goes on to the next.
		if err := leader.Resign(t.Context(), c.Node(first), "test"); err != nil {
			t.Fatal(err)
		}
		if got, err := ask(within(t, 5*time.Second), c.Node(f), 3); err != nil || got != 3 {
			t.Fatalf("%d, %v", got, err)
		}
		if second, _ := elected(t, c, "a", "b", "c"); second == first {
			t.Errorf("%s leads again", first)
		}
	})
}

func TestCallErrors(t *testing.T) {
	voters := func(j *journal, _ string) leader.Spec[counter] { return spec(j) }
	nodes(t, []string{"a", "b", "c", "d"}, []string{"a", "b", "c"}, voters, func(t *testing.T, c *grpcproctest.Cluster, j *journal) {
		settle(2 * time.Second)
		first, _ := elected(t, c, "a", "b", "c")
		f := others(first)[0]

		// The singleton's own error comes back as it is, even one that says
		// there is no such process: it answered.
		if _, err := ask(t.Context(), c.Node(f), refuse); err == nil || !strings.Contains(err.Error(), "refused") {
			t.Errorf("refused: %v", err)
		}
		if _, err := ask(within(t, time.Second), c.Node(f), noproc); !errors.Is(err, grpcproc.ErrNoProc) || errors.Is(err, context.DeadlineExceeded) {
			t.Errorf("answered with ErrNoProc's text: %v", err)
		}
		// A node that runs no elector cannot say who leads.
		if _, err := ask(t.Context(), c.Node("d"), 1); !errors.Is(err, grpcproc.ErrNoProc) {
			t.Errorf("from a node with no elector: %v", err)
		}
		// Nobody is called that: it is asked again until ctx ends.
		_, err := leader.Call[counter](within(t, time.Second), c.Node(f), "test", "nobody", wrapperspb.Int64(1))
		if !errors.Is(err, grpcproc.ErrNoProc) || !errors.Is(err, context.DeadlineExceeded) {
			t.Errorf("a singleton of another name: %v", err)
		}

		// Cut off from the leader, which the others still follow: the call
		// never leaves, and is tried again until ctx ends.
		c.Partition(f, first)
		_, err = ask(within(t, time.Second), c.Node(f), 1)
		if le, ok := errors.AsType[*grpcproc.LinkError](err); !ok || !le.Unsent || !errors.Is(err, context.DeadlineExceeded) {
			t.Errorf("cut off: %v", err)
		}
		c.Heal(f, first)
		settle(time.Second)

		// A call that left, and whose link broke while it waited, may have
		// been handled: it is not made again.
		lead, _ := elected(t, c, "a", "b", "c")
		caller := others(lead)[0]
		done := make(chan error, 1)
		go func() {
			_, err := ask(within(t, time.Minute), c.Node(caller), ignore)
			done <- err
		}()
		settle(10 * time.Millisecond)
		c.Partition(caller, lead)
		err = <-done
		if le, ok := errors.AsType[*grpcproc.LinkError](err); !ok || le.Unsent || errors.Is(err, context.DeadlineExceeded) {
			t.Errorf("broken while it waited: %v", err)
		}
	})
}
