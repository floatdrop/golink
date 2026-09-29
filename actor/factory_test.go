package actor_test

import (
	"errors"
	"testing"
	"testing/synctest"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"

	"github.com/floatdrop/grpcproc"
	"github.com/floatdrop/grpcproc/actor"
	"github.com/floatdrop/grpcproc/grpcproctest"
	"github.com/floatdrop/grpcproc/internal/testpb"
	actorv1 "github.com/floatdrop/grpcproc/proto/grpcproc/actor/v1"
)

// errFull is the factory's refusal of a Reserve for "full": the caller
// tries elsewhere.
var errFull = errors.New("full")

// factories are those of the supervisor on b in these tests: "worker" starts
// a temporary worker named after the Reserve's id, or refuses "full"; "early"
// starts one that exits at once; "tree" starts a supervisor.
var factories = map[string]actor.Factory{
	"worker": actor.ChildFactory(func(r *testpb.Reserve) (actor.ChildSpec, error) {
		if r.GetId() == "full" {
			return actor.ChildSpec{}, errFull
		}
		return actor.ChildFunc(r.GetId(), worker).WithRestart(actor.Temporary), nil
	}),
	"early": actor.ChildFactory(func(*testpb.Reserve) (actor.ChildSpec, error) {
		return actor.ChildFunc("", func(*P) error { return errors.New("bad codec") }).WithRestart(actor.Temporary), nil
	}),
	"tree": actor.ChildFactory(func(r *testpb.Reserve) (actor.ChildSpec, error) {
		return actor.ChildSupervisor(r.GetId(), actor.Spec{Children: []actor.ChildSpec{actor.ChildFunc("", worker)}}).WithRestart(actor.Temporary), nil
	}),
}

// placing is a cluster of a and b with a supervisor of factories on b, and
// a process on a that asks it for children, whose mailbox the returned
// channel has.
func placing(t *testing.T) (*grpcproctest.Cluster, grpcproc.PID, *grpcproc.Process[proto.Message], <-chan grpcproc.Msg[proto.Message]) {
	t.Helper()
	c := grpcproctest.New(t, "a", "b")
	sup, err := actor.Supervise(c.Node("b"), actor.Spec{Factories: factories})
	if err != nil {
		t.Fatal(err)
	}
	ch := make(chan grpcproc.Msg[proto.Message], 16)
	ready := make(chan *grpcproc.Process[proto.Message], 1)
	_, err = c.Node("a").Spawn[proto.Message](func(p *grpcproc.Process[proto.Message]) error {
		ready <- p
		for {
			m, err := p.Receive()
			if err != nil {
				return err
			}
			ch <- m
		}
	})
	if err != nil {
		t.Fatal(err)
	}
	return c, sup, <-ready, ch
}

// downOf takes the next message from ch, which must be the Down of ref, for
// pid, with reason.
func downOf(t *testing.T, ch <-chan grpcproc.Msg[proto.Message], ref grpcproc.Ref, pid grpcproc.PID, reason string) {
	t.Helper()
	if m := within(t, ch); m.Down == nil || m.Down.Ref != ref || m.Down.PID != pid || m.Down.Reason != reason {
		t.Fatalf("got %+v, want the Down of %v with %q", m.Down, pid, reason)
	}
}

// A process on another node asks a supervisor for a child by a factory's
// name, and monitors the child from before it runs: the child is the
// supervisor's, and the caller hears of its exit.
func TestStartChildFrom(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		c, sup, p, got := placing(t)
		b := c.Node("b")
		pid, ref, err := actor.StartChildFrom(t.Context(), p, sup, "worker", &testpb.Reserve{Id: "w1"}, actor.WithMonitor())
		if err != nil || pid != pidOf(t, b, "w1") || ref.Node != "a" {
			t.Fatalf("%v, %v, %v", pid, ref, err)
		}
		if kids, err := actor.Children(t.Context(), p, sup); err != nil || len(kids) != 1 || kids[0].PID != pid || kids[0].Restart != actor.Temporary {
			t.Fatalf("children %+v, %v", kids, err)
		}
		if got := children(t, b, sup); got["w1"] == "" {
			t.Fatalf("inspect %v", got)
		}
		if insp, _ := b.Inspect(t.Context(), sup); insp["factories"] != "early tree worker" {
			t.Fatalf("inspect %v", insp)
		}
		send(t, b, "w1", -1)
		downOf(t, got, ref, pid, "crash")

		// Without WithMonitor, nothing is monitored.
		pid, ref, err = actor.StartChildFrom(t.Context(), p, sup, "worker", &testpb.Reserve{Id: "w2"})
		if err != nil || ref != (grpcproc.Ref{}) {
			t.Fatalf("%v, %v, %v", pid, ref, err)
		}
		send(t, b, "w2", -1)
		synctest.Wait()
		if len(got) != 0 {
			t.Fatalf("a Down unasked for: %+v", <-got)
		}
	})
}

// A child that exits before its start is even answered is reported with its
// reason, never noproc: a start that failed and a child that started and
// exited are told apart.
func TestStartChildFromOfAChildThatExitsAtOnce(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		_, sup, p, got := placing(t)
		pid, ref, err := actor.StartChildFrom(t.Context(), p, sup, "early", &testpb.Reserve{}, actor.WithMonitor())
		if err != nil {
			t.Fatal(err)
		}
		downOf(t, got, ref, pid, "bad codec")
	})
}

// What a factory refuses with is StartChildFrom's error, as the factory
// returned it; so is the lack of a factory of the name, or an argument it
// does not take. None leaves a monitor behind.
func TestStartChildFromRefusals(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		c, sup, p, got := placing(t)
		for _, tc := range []struct {
			factory string
			arg     proto.Message
			want    error
		}{
			{"worker", &testpb.Reserve{Id: "full"}, errFull},
			{"nope", &testpb.Reserve{}, actor.ErrNoFactory},
			{"worker", &testpb.Ping{}, nil},
			{"worker", nil, nil},
		} {
			_, ref, err := actor.StartChildFrom(t.Context(), p, sup, tc.factory, tc.arg, actor.WithMonitor())
			if err == nil || tc.want != nil && !errors.Is(err, tc.want) || ref != (grpcproc.Ref{}) {
				t.Fatalf("%s(%v): %v, %v", tc.factory, tc.arg, ref, err)
			}
		}
		if _, _, err := actor.StartChildFrom(t.Context(), p, sup, "worker", &testpb.Reserve{Id: "x"}, actor.WithMonitor(), actor.WithLink()); err == nil {
			t.Fatal("a monitor and a link")
		}
		if _, _, err := actor.StartChildFrom(t.Context(), c.Node("a"), sup, "worker", &testpb.Reserve{Id: "x"}, actor.WithMonitor()); err == nil {
			t.Fatal("a node monitors")
		}
		// An argument of a type the supervisor's node does not know.
		unknown := &actorv1.Control{Op: &actorv1.Control_StartFrom{StartFrom: &actorv1.StartFrom{Factory: "worker", Arg: &anypb.Any{TypeUrl: "type.googleapis.com/no.Such"}}}}
		if _, err := c.Node("a").CallTo[*actorv1.Started](t.Context(), sup, unknown); err == nil {
			t.Fatal("an unknown argument accepted")
		}
		if kids, err := actor.Children(t.Context(), p, sup); err != nil || len(kids) != 0 {
			t.Fatalf("children %+v, %v", kids, err)
		}
		synctest.Wait()
		if len(got) != 0 {
			t.Fatalf("a Down of a refused start: %+v", <-got)
		}
	})
}

// A named child that runs is not started again: its PID comes back with
// ErrAlreadyStarted, monitored, so a caller whose first answer was lost
// finds it again.
func TestStartChildFromOfARunningName(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		c, sup, p, got := placing(t)
		first, _, err := actor.StartChildFrom(t.Context(), p, sup, "worker", &testpb.Reserve{Id: "w"})
		if err != nil {
			t.Fatal(err)
		}
		pid, ref, err := actor.StartChildFrom(t.Context(), p, sup, "worker", &testpb.Reserve{Id: "w"}, actor.WithMonitor())
		if !errors.Is(err, actor.ErrAlreadyStarted) || pid != first || ref == (grpcproc.Ref{}) {
			t.Fatalf("%v, %v, %v", pid, ref, err)
		}
		send(t, c.Node("b"), "w", 0)
		downOf(t, got, ref, first, grpcproc.ReasonNormal)
	})
}

// WithLink links the caller to the child: its exit ends the caller, or
// reaches it as an Exited if it traps exits.
func TestStartChildFromWithLink(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		c, sup, p, got := placing(t)
		p.SetTrapExit(true)
		pid, _, err := actor.StartChildFrom(t.Context(), p, sup, "worker", &testpb.Reserve{Id: "w"}, actor.WithLink())
		if err != nil {
			t.Fatal(err)
		}
		send(t, c.Node("b"), "w", -1)
		if m := within(t, got); m.Exited == nil || m.Exited.PID != pid || m.Exited.Reason != "crash" {
			t.Fatalf("%+v", m)
		}
	})
}

// A factory can build a supervisor, which gets the monitor as a worker
// does.
func TestStartChildFromOfASupervisor(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		c, sup, p, got := placing(t)
		pid, ref, err := actor.StartChildFrom(t.Context(), p, sup, "tree", &testpb.Reserve{Id: "t"}, actor.WithMonitor())
		if err != nil {
			t.Fatal(err)
		}
		if kids, err := actor.Children(t.Context(), p, pid); err != nil || len(kids) != 1 {
			t.Fatalf("%+v, %v", kids, err)
		}
		_ = c.Node("b").Exit(t.Context(), pid, "gone")
		downOf(t, got, ref, pid, "gone")
	})
}

// A factory is named, and built with ChildFactory.
func TestFactoryValidation(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		n := grpcproctest.New(t, "a").Node("a")
		for _, f := range []map[string]actor.Factory{
			{"": factories["worker"]},
			{"raw": {}},
		} {
			if _, err := actor.Supervise(n, actor.Spec{Factories: f}); err == nil {
				t.Errorf("accepted %v", f)
			}
		}
	})
}
