package singleton

import (
	"testing"
	"testing/synctest"

	"google.golang.org/protobuf/types/known/emptypb"
	"google.golang.org/protobuf/types/known/wrapperspb"

	"github.com/floatdrop/grpcproc"
	"github.com/floatdrop/grpcproc/actor"
	"github.com/floatdrop/grpcproc/grpcproctest"
	"github.com/floatdrop/grpcproc/leader"
)

// ids hands out numbers that never repeat, from whichever node leads. Each
// is checkpointed to a majority of the cluster before it is handed out, so
// whichever node leads next starts past it.
func ids(l *leader.Lease[*wrapperspb.UInt64Value], last *wrapperspb.UInt64Value) (actor.ChildSpec, error) {
	next := last.GetValue()
	return actor.ChildFunc("ids", func(p *grpcproc.Process[*emptypb.Empty]) error {
		for {
			m, err := p.Receive()
			if err != nil {
				return err
			}
			next++
			if err := l.Checkpoint(p.Context(), wrapperspb.UInt64(next)); err != nil {
				// No longer the leader: the call goes unanswered, and the
				// caller's leader.Call asks the next one.
				return err
			}
			_ = m.Reply(wrapperspb.UInt64(next), nil)
		}
	}), nil
}

// nextID asks whichever node leads for a number, from any node that takes
// part in the election.
func nextID(t *testing.T, from grpcproc.Caller) uint64 {
	id, err := leader.Call[*wrapperspb.UInt64Value](t.Context(), from, "ids", "ids", &emptypb.Empty{})
	if err != nil {
		t.Fatal(err)
	}
	return id.GetValue()
}

func TestIDsNeverRepeat(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		c := grpcproctest.New(t, "a", "b", "c")
		for _, node := range []string{"a", "b", "c"} {
			spec := leader.Spec[*wrapperspb.UInt64Value]{Cluster: "ids", Voters: []string{"a", "b", "c"}, Singleton: ids}
			if _, err := leader.Start(c.Node(node), spec); err != nil {
				t.Fatal(err)
			}
		}
		// The first call waits for the first election.
		var got []uint64
		for _, from := range []string{"a", "b", "c"} {
			got = append(got, nextID(t, c.Node(from)))
		}
		// The leader crashes; the next starts from the last number handed
		// out, and callers find it.
		info, err := leader.Status(t.Context(), c.Node("a"), "ids")
		if err != nil {
			t.Fatal(err)
		}
		c.Kill(info.Leader)
		for _, from := range []string{"a", "b", "c"} {
			if from != info.Leader {
				got = append(got, nextID(t, c.Node(from)))
			}
		}
		for i, id := range got {
			if id != uint64(i+1) {
				t.Fatalf("ids %v: want 1, 2, 3, 4, 5", got)
			}
		}
	})
}
