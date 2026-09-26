package actor

import (
	"errors"
	"testing"
	"time"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/emptypb"

	"github.com/floatdrop/golink"
	"github.com/floatdrop/golink/golinktest"
)

// A Down that was already queued when its child was stopped or replaced
// (two children crashing together under one_for_all) is ignored.
func TestStaleDownIsIgnored(t *testing.T) {
	s := &supervisor{kids: []*kid{
		{spec: ChildSpec{Name: "stopped"}, ref: golink.Ref{ID: 1}},
		{spec: ChildSpec{Name: "replaced"}, ref: golink.Ref{ID: 3}, running: true},
	}}
	for _, ref := range []golink.Ref{{ID: 1}, {ID: 2}} {
		if err := s.exited(golink.Down{Ref: ref, Reason: "crash"}); err != nil {
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
	c := golinktest.New(t, "a")
	n := c.Node("a")
	starts := 0
	flaky := ChildSpec{Name: "flaky", start: func(sup *golink.Process[proto.Message]) (golink.PID, golink.Ref, error) {
		starts++
		if starts > 1 {
			return golink.PID{}, golink.Ref{}, errors.New("cannot start")
		}
		a, ref, err := sup.SpawnMonitor[proto.Message](func(p *golink.Process[proto.Message]) error {
			_, err := p.Receive()
			if err != nil {
				return err
			}
			return errors.New("crash")
		}, golink.WithParent(sup.PID()))
		return a.PID(), ref, err
	}}
	sup, err := Supervise(n, Spec{MaxRestarts: 2, Children: []ChildSpec{flaky}})
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan golink.Down, 1)
	_, _ = golink.Spawn[proto.Message](n, func(p *golink.Process[proto.Message]) error {
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
