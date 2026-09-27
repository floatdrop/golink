package actor

import (
	"errors"
	"testing"
	"time"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/emptypb"

	"github.com/floatdrop/grpcproc"
	"github.com/floatdrop/grpcproc/grpcproctest"
)

// A Down that was already queued when its child was stopped or replaced
// (two children crashing together under one_for_all) is ignored.
func TestStaleDownIsIgnored(t *testing.T) {
	s := &supervisor{kids: []*kid{
		{spec: ChildSpec{Name: "stopped"}, ref: grpcproc.Ref{ID: 1}},
		{spec: ChildSpec{Name: "replaced"}, ref: grpcproc.Ref{ID: 3}, running: true},
	}}
	for _, ref := range []grpcproc.Ref{{ID: 1}, {ID: 2}} {
		if err := s.exited(grpcproc.Down{Ref: ref, Reason: "crash"}); err != nil {
			t.Fatal(err)
		}
	}
	if !s.kids[1].running {
		t.Fatal("a stale Down touched a running child")
	}
}

// A restart whose start fails counts against the intensity, so a child that
// cannot come back ends the supervisor instead of looping.
func TestFailedRestartEndsSupervisor(t *testing.T) {
	c := grpcproctest.New(t, "a")
	n := c.Node("a")
	starts := 0
	flaky := ChildSpec{Name: "flaky", start: func(sup *grpcproc.Process[proto.Message]) (grpcproc.PID, grpcproc.Ref, error) {
		starts++
		if starts > 1 {
			return grpcproc.PID{}, grpcproc.Ref{}, errors.New("cannot start")
		}
		a, ref, err := sup.SpawnMonitor[proto.Message](func(p *grpcproc.Process[proto.Message]) error {
			_, err := p.Receive()
			if err != nil {
				return err
			}
			return errors.New("crash")
		})
		return a.PID(), ref, err
	}}
	sup, err := Supervise(n, Spec{MaxRestarts: 2, Children: []ChildSpec{flaky}})
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan grpcproc.Down, 1)
	_, _ = n.Spawn[proto.Message](func(p *grpcproc.Process[proto.Message]) error {
		p.Monitor(sup)
		for {
			m, err := p.Receive()
			if err != nil {
				return err
			}
			if m.Down != nil {
				done <- *m.Down
				return nil
			}
		}
	})
	time.Sleep(10 * time.Millisecond)
	for _, pr := range n.Processes() {
		if pr.Parent == sup {
			_ = n.SendTo(t.Context(), pr.PID, &emptypb.Empty{})
		}
	}
	select {
	case d := <-done:
		if d.Reason != ReasonMaxRestarts {
			t.Fatalf("supervisor exited with %q", d.Reason)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("supervisor kept going")
	}
	if starts != 3 { // the first start, then two failed restarts within the limit
		t.Fatalf("starts = %d", starts)
	}
}

// A restart that fails partway through its group retries it, and the retry
// brings back every child the first attempt stopped, wherever it stands
// relative to the one that failed, and still leaves down those the restart
// rules keep down: a temporary child, and a transient one that finished.
func TestFailedRestartBringsBackTheWholeGroup(t *testing.T) {
	crash := func(p *grpcproc.Process[proto.Message]) error {
		if _, err := p.Receive(); err != nil {
			return err
		}
		return errors.New("crash")
	}
	finish := func(*grpcproc.Process[proto.Message]) error { return nil }
	policies := func(flaky ChildSpec) []ChildSpec {
		return []ChildSpec{
			ChildFunc("first", crash), flaky,
			ChildFunc("temp", crash).WithRestart(Temporary),
			ChildFunc("trans", finish).WithRestart(Transient),
			ChildFunc("transrun", crash).WithRestart(Transient),
			ChildFunc("last", crash),
		}
	}
	cases := []struct {
		name       string
		strategy   Strategy
		children   func(flaky ChildSpec) []ChildSpec
		crash      string
		back, gone []string
	}{
		{"one_for_all/crash_before", OneForAll, func(f ChildSpec) []ChildSpec {
			return []ChildSpec{ChildFunc("first", crash), f, ChildFunc("last", crash)}
		}, "first", []string{"first", "flaky", "last"}, nil},
		{"rest_for_one/crash_before", RestForOne, func(f ChildSpec) []ChildSpec {
			return []ChildSpec{ChildFunc("first", crash), f, ChildFunc("last", crash)}
		}, "first", []string{"first", "flaky", "last"}, nil},
		{"one_for_all/crash_after", OneForAll, func(f ChildSpec) []ChildSpec {
			return []ChildSpec{f, ChildFunc("mid", crash), ChildFunc("crasher", crash)}
		}, "crasher", []string{"flaky", "mid", "crasher"}, nil},
		{"one_for_all/policies", OneForAll, policies, "first", []string{"first", "flaky", "transrun", "last"}, []string{"temp", "trans"}},
		{"rest_for_one/policies", RestForOne, policies, "first", []string{"first", "flaky", "transrun", "last"}, []string{"temp", "trans"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			n := grpcproctest.New(t, "a").Node("a")
			flaky := ChildFunc("flaky", crash)
			start, starts := flaky.start, 0
			flaky.start = func(sup *grpcproc.Process[proto.Message]) (grpcproc.PID, grpcproc.Ref, error) {
				if starts++; starts == 2 {
					return grpcproc.PID{}, grpcproc.Ref{}, errors.New("cannot start, once")
				}
				return start(sup)
			}
			if _, err := Supervise(n, Spec{Strategy: tc.strategy, MaxRestarts: 5, Children: tc.children(flaky)}); err != nil {
				t.Fatal(err)
			}
			eventually := func(what string, cond func() bool) {
				t.Helper()
				for deadline := time.Now().Add(5 * time.Second); !cond(); time.Sleep(5 * time.Millisecond) {
					if time.Now().After(deadline) {
						t.Fatal(what)
					}
				}
			}
			eventually("trans did not finish", func() bool { _, ok := n.Whereis("trans"); return !ok })
			before := map[string]grpcproc.PID{}
			for _, name := range tc.back {
				pid, ok := n.Whereis(name)
				if !ok {
					t.Fatalf("%s is not running", name)
				}
				before[name] = pid
			}
			if err := n.SendTo(t.Context(), before[tc.crash], &emptypb.Empty{}); err != nil {
				t.Fatal(err)
			}
			for _, name := range tc.back {
				eventually(name+" never came back", func() bool { pid, ok := n.Whereis(name); return ok && pid != before[name] })
			}
			for _, name := range tc.gone {
				if _, ok := n.Whereis(name); ok {
					t.Fatalf("%s came back", name)
				}
			}
		})
	}
}
