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
			_ = n.SendTo(pr.PID, &emptypb.Empty{})
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
