package actor_test

import (
	"errors"
	"testing"
	"testing/synctest"

	"google.golang.org/protobuf/proto"

	"github.com/floatdrop/grpcproc"
	"github.com/floatdrop/grpcproc/actor"
	"github.com/floatdrop/grpcproc/grpcproctest"
	"github.com/floatdrop/grpcproc/internal/testpb"
)

// trapping traps exits and links to target in Init, then reports what it
// handles.
type trapping struct {
	target grpcproc.Target
	ready  chan struct{}
	seen   chan string
	ended  chan error
}

func (h *trapping) Init(p *P) error {
	p.SetTrapExit(true)
	if h.target != nil {
		p.Link(h.target)
	}
	close(h.ready)
	return nil
}

func (h *trapping) HandleMessage(_ *P, m M) error {
	h.seen <- "message"
	return nil
}

func (h *trapping) Terminate(_ *P, err error) { h.ended <- err }

// exits adds HandleExited to trapping.
type exits struct{ *trapping }

func (h exits) HandleExited(_ *P, e grpcproc.Exited) error {
	h.seen <- "exit " + e.Reason
	return nil
}

// newTrapping returns a trapping that links to target, unless it is nil.
func newTrapping(target grpcproc.Target) *trapping {
	return &trapping{target: target, ready: make(chan struct{}), seen: make(chan string, 4), ended: make(chan error, 1)}
}

// An actor that traps exits hands them to HandleExited, and carries on;
// without HandleExited they are ignored.
func TestHandleExited(t *testing.T) {
	for _, tc := range []struct {
		name string
		want string
		run  func(*trapping) func(*P) error
	}{
		{"handled", "exit gone", func(h *trapping) func(*P) error { return actor.Run[*testpb.Ping](exits{h}) }},
		{"ignored", "", func(h *trapping) func(*P) error { return actor.Run[*testpb.Ping](h) }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				n := grpcproctest.New(t, "a").Node("a")
				target, _ := n.Spawn[*testpb.Ping](func(p *P) error { _, err := p.Receive(); return err })
				h := newTrapping(target)
				a, err := n.Spawn(tc.run(h))
				if err != nil {
					t.Fatal(err)
				}
				within(t, h.ready)
				_ = n.Exit(t.Context(), target, "gone")
				// The Exited is the actor's first item: once it is taken, the
				// message sent next is handled after it.
				until(t, "the Exited is taken", func() bool { info, _ := n.Process(a.PID()); return info.Received == 1 })
				if tc.want != "" {
					if got := within(t, h.seen); got != tc.want {
						t.Fatal(got)
					}
				}
				_ = a.Send(t.Context(), n, &testpb.Ping{N: 1})
				if got := within(t, h.seen); got != "message" {
					t.Fatalf("after the Exited: %s", got)
				}
			})
		})
	}
}

// An Exited from its parent ends an actor that traps exits, with the parent's
// reason: a child spawned with LinkParent never outlives its parent.
func TestExitFromParentEndsTheActor(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		c := grpcproctest.New(t, "a")
		n := c.Node("a")
		h := newTrapping(nil)
		release := make(chan struct{})
		if _, err := n.Spawn[proto.Message](func(p *grpcproc.Process[proto.Message]) error {
			if _, err := p.Spawn(actor.Run[*testpb.Ping](exits{h}), grpcproc.LinkParent()); err != nil {
				return err
			}
			<-release
			return errors.New("bye")
		}); err != nil {
			t.Fatal(err)
		}
		within(t, h.ready)
		close(release)
		err := within(t, h.ended)
		if ee, ok := errors.AsType[*grpcproc.ExitError](err); !ok || ee.Reason != "bye" {
			t.Fatalf("ended with %v", err)
		}
		select {
		case s := <-h.seen:
			t.Fatalf("HandleExited ran for the parent's exit: %s", s)
		default:
		}
	})
}

// A supervisor's children, and a child supervisor's, are linked to their
// supervisor: whatever ends it tells them to exit too.
func TestChildrenAreLinkedToTheirSupervisor(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		c := grpcproctest.New(t, "a")
		n := c.Node("a")
		sup, err := actor.Supervise(n, actor.Spec{Children: []actor.ChildSpec{
			actor.ChildFunc("w", worker),
			actor.ChildSupervisor("inner", actor.Spec{Children: []actor.ChildSpec{actor.ChildFunc("w2", worker)}}),
		}})
		if err != nil {
			t.Fatal(err)
		}
		inner := pidOf(t, n, "inner")
		for _, tc := range []struct {
			child  string
			parent grpcproc.PID
		}{{"w", sup}, {"inner", sup}, {"w2", inner}} {
			info, _ := n.Process(pidOf(t, n, tc.child))
			if info.Links != 1 || info.Parent != tc.parent {
				t.Errorf("%s: links %d, parent %v", tc.child, info.Links, info.Parent)
			}
		}
		if info, _ := n.Process(sup); info.Links != 0 || info.Watchers != 2 || info.Monitors != 2 {
			t.Errorf("supervisor: %+v", info)
		}
	})
}

// Restarts leave the supervisor with a link and a monitor per child, not
// more.
func TestRestartsLeaveOneLinkPerChild(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		c := grpcproctest.New(t, "a")
		n := c.Node("a")
		sup, err := actor.Supervise(n, actor.Spec{Strategy: actor.OneForAll, MaxRestarts: 20, Children: []actor.ChildSpec{
			actor.ChildFunc("w1", worker), actor.ChildFunc("w2", worker),
		}})
		if err != nil {
			t.Fatal(err)
		}
		for range 5 {
			old := pidOf(t, n, "w1")
			send(t, n, "w1", -1)
			restarted(t, n, "w1", old)
		}
		until(t, "the supervisor settles at two watchers and two monitors", func() bool {
			info, _ := n.Process(sup)
			return info.Watchers == 2 && info.Monitors == 2
		})
	})
}

// An actor with no parent hands every Exited to HandleExited, even one with
// no PID: a link to no node at all.
func TestNoParentNoParentRule(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		c := grpcproctest.New(t, "a")
		n := c.Node("a")
		h := newTrapping(grpcproc.PID{})
		if _, err := n.Spawn(actor.Run[*testpb.Ping](exits{h})); err != nil {
			t.Fatal(err)
		}
		within(t, h.ready)
		if got := within(t, h.seen); got != "exit "+grpcproc.ReasonNoConnection {
			t.Fatal(got)
		}
	})
}
