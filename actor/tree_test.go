package actor_test

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/floatdrop/grpcproc"
	"github.com/floatdrop/grpcproc/actor"
	"github.com/floatdrop/grpcproc/grpcproctest"
	"github.com/floatdrop/grpcproc/internal/testpb"
	actorv1 "github.com/floatdrop/grpcproc/proto/grpcproc/actor/v1"
	grpcprocv1 "github.com/floatdrop/grpcproc/proto/grpcproc/v1"
)

func until(t *testing.T, what string, cond func() bool) {
	t.Helper()
	for deadline := time.Now().Add(5 * time.Second); !cond(); time.Sleep(2 * time.Millisecond) {
		if time.Now().After(deadline) {
			t.Fatalf("never: %s", what)
		}
	}
}

// draining exits, once told to, only after d: a graceful drain.
func draining(d time.Duration) func(*P) error {
	return func(p *P) error {
		<-p.Context().Done()
		time.Sleep(d)
		return errors.New("drained")
	}
}

type logBuf struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (l *logBuf) Write(p []byte) (int, error) { l.mu.Lock(); defer l.mu.Unlock(); return l.b.Write(p) }
func (l *logBuf) String() string              { l.mu.Lock(); defer l.mu.Unlock(); return l.b.String() }

func logged(t *testing.T) (*grpcproc.Node, *logBuf) {
	t.Helper()
	buf := &logBuf{}
	c := grpcproctest.NewWith(t, []grpcproctest.Option{grpcproctest.WithConfig(func(_ string, cfg *grpcproc.Config) {
		cfg.Logger = slog.New(slog.NewTextHandler(buf, nil))
	})}, "a")
	return c.Node("a"), buf
}

func children(t *testing.T, n *grpcproc.Node, sup grpcproc.PID) map[string]string {
	t.Helper()
	insp, err := n.Inspect(t.Context(), sup)
	if err != nil {
		t.Fatal(err)
	}
	out := map[string]string{}
	for k, v := range insp {
		if name, ok := strings.CutPrefix(k, "child."); ok {
			out[name] = v
		}
	}
	return out
}

// A parent restarts a child supervisor only once its whole subtree has
// stopped, however long that takes past the parent's Shutdown, so the new
// subtree never meets the old one: one crash stays one restart.
func TestSubtreeStopsBeforeItRestarts(t *testing.T) {
	n, logs := logged(t)
	top, err := actor.Supervise(n, actor.Spec{Strategy: actor.OneForAll, Shutdown: 50 * time.Millisecond, Children: []actor.ChildSpec{
		actor.ChildSupervisor("inner", actor.Spec{Shutdown: time.Second, Children: []actor.ChildSpec{
			actor.ChildFunc("s1", draining(150*time.Millisecond)), actor.ChildFunc("s2", draining(150*time.Millisecond)),
		}}),
		actor.ChildFunc("crasher", worker),
	}})
	if err != nil {
		t.Fatal(err)
	}
	inner, s1 := pidOf(t, n, "inner"), pidOf(t, n, "s1")
	send(t, n, "crasher", -1)
	restarted(t, n, "inner", inner)
	restarted(t, n, "s1", s1)
	if _, alive := n.Process(top); !alive {
		t.Fatalf("one crash ended the tree:\n%s", logs)
	}
	if strings.Contains(logs.String(), "level=ERROR") || strings.Contains(logs.String(), "did not exit in time") {
		t.Fatalf("logs:\n%s", logs)
	}
}

// A child's own Shutdown overrides the supervisor's: shorter for one that
// ignores Exit, Infinity for one whose drain takes long.
func TestChildShutdown(t *testing.T) {
	n, logs := logged(t)
	release := make(chan struct{})
	defer close(release)
	stubborn := func(p *P) error { <-release; return nil } // never looks at Exit
	_, err := actor.Supervise(n, actor.Spec{Strategy: actor.OneForAll, Shutdown: 20 * time.Millisecond, Children: []actor.ChildSpec{
		actor.ChildFunc("stubborn", stubborn).WithRestart(actor.Temporary).WithShutdown(40 * time.Millisecond),
		actor.ChildFunc("slow", draining(120*time.Millisecond)).WithShutdown(actor.Infinity),
		actor.ChildFunc("w", worker),
	}})
	if err != nil {
		t.Fatal(err)
	}
	slow, w := pidOf(t, n, "slow"), pidOf(t, n, "w")
	start := time.Now()
	send(t, n, "w", -1)
	restarted(t, n, "w", w)
	restarted(t, n, "slow", slow) // its name was free: it was waited for
	if took := time.Since(start); took < 160*time.Millisecond || took > 5*time.Second {
		t.Fatalf("the restart took %v", took)
	}
	if s := logs.String(); !strings.Contains(s, "child=stubborn") || strings.Contains(s, "child=slow") || strings.Contains(s, "level=ERROR") {
		t.Fatalf("logs:\n%s", s)
	}
}

// When the node stops, a supervisor waiting for a slow child keeps waiting
// until it is gone.
func TestStopNodeWhileAChildDrains(t *testing.T) {
	n, logs := logged(t)
	if _, err := actor.Supervise(n, actor.Spec{Children: []actor.ChildSpec{
		actor.ChildFunc("slow", draining(200*time.Millisecond)).WithShutdown(actor.Infinity),
	}}); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := n.Stop(ctx); err != nil {
		t.Fatal(err)
	}
	if strings.Contains(logs.String(), "did not exit in time") {
		t.Fatalf("gave up on the child:\n%s", logs)
	}
}

// A supervisor's children can be anonymous: known by their PIDs, registered
// under no name.
func TestAnonymousChildren(t *testing.T) {
	c := grpcproctest.New(t, "a")
	n := c.Node("a")
	sup, err := actor.Supervise(n, actor.Spec{Children: []actor.ChildSpec{
		actor.ChildFunc("", worker), actor.ChildFunc("", worker),
	}})
	if err != nil {
		t.Fatal(err)
	}
	kids := children(t, n, sup)
	if len(kids) != 2 {
		t.Fatalf("%v", kids)
	}
	for name := range kids {
		if !strings.HasPrefix(name, "<a.") {
			t.Fatalf("an anonymous child shown as %q", name)
		}
	}
	for _, p := range n.Processes() {
		if p.Name != "" {
			t.Fatalf("%v registered as %q", p.PID, p.Name)
		}
	}
}

// StartChild adds children at runtime: a pool of anonymous ones, each
// forgotten once it ends for good, and named ones, which StopChild stops.
func TestStartAndStopChild(t *testing.T) {
	c := grpcproctest.New(t, "a", "b")
	n := c.Node("a")
	sup, err := actor.Supervise(n, actor.Spec{})
	if err != nil {
		t.Fatal(err)
	}
	var pool []grpcproc.PID
	for range 3 {
		pid, err := actor.StartChild(t.Context(), n, sup, actor.ChildFunc("", worker).WithRestart(actor.Temporary))
		if err != nil {
			t.Fatal(err)
		}
		if info, _ := n.Process(pid); info.Parent != sup || info.Links != 1 {
			t.Fatalf("%+v", info)
		}
		pool = append(pool, pid)
	}
	_ = n.Send(t.Context(), grpcproc.AddrOf[*testpb.Ping](pool[0]), &testpb.Ping{N: -1}) // temporary: gone for good
	until(t, "the supervisor forgets its temporary child", func() bool { return len(children(t, n, sup)) == 2 })

	// A transient one comes back after a crash, and is forgotten after a
	// normal exit.
	trans, err := actor.StartChild(t.Context(), n, sup, actor.ChildFunc("", worker).WithRestart(actor.Transient))
	if err != nil {
		t.Fatal(err)
	}
	_ = n.Send(t.Context(), grpcproc.AddrOf[*testpb.Ping](trans), &testpb.Ping{N: -1})
	until(t, "the transient child restarts", func() bool {
		for _, v := range children(t, n, sup) {
			if strings.Contains(v, "restarts=1") {
				return true
			}
		}
		return false
	})
	for name, v := range children(t, n, sup) {
		if strings.Contains(v, "restarts=1") {
			pid := strings.Fields(v)[0]
			if pid == trans.String() || name != pid {
				t.Fatalf("%s: %s", name, v)
			}
		}
	}

	// A named one is registered; StopChild stops it and the supervisor
	// forgets it.
	dyn, err := actor.StartChild(t.Context(), n, sup, actor.ChildFunc("dyn", worker))
	if err != nil {
		t.Fatal(err)
	}
	if pid, _ := n.Whereis("dyn"); pid != dyn {
		t.Fatalf("dyn is %v", pid)
	}
	if _, err := actor.StartChild(t.Context(), n, sup, actor.ChildFunc("dyn", worker)); err == nil {
		t.Fatal("two children named dyn")
	}
	downs := watch(t, n, dyn)
	if err := actor.StopChild(t.Context(), n, sup, dyn); err != nil {
		t.Fatal(err)
	}
	if d := down(t, downs); d.Reason != grpcproc.ReasonShutdown {
		t.Fatalf("%+v", d)
	}
	if _, ok := children(t, n, sup)["dyn"]; ok {
		t.Fatal("dyn is still a child")
	}
	if err := actor.StopChild(t.Context(), n, sup, dyn); err == nil {
		t.Fatal("stopped a child twice")
	}
	// From another node, too.
	if err := actor.StopChild(t.Context(), c.Node("b"), sup, pool[1]); err != nil {
		t.Fatal(err)
	}
	if _, alive := n.Process(pool[1]); alive {
		t.Fatal("still running")
	}
}

// What StartChild refuses: a supervisor on another node, a spec it would
// refuse in Spec, a child that cannot start; and what a supervisor refuses
// from a call.
func TestStartChildRefusals(t *testing.T) {
	c := grpcproctest.New(t, "a", "b")
	n := c.Node("a")
	sup, err := actor.Supervise(n, actor.Spec{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := actor.StartChild(t.Context(), c.Node("b"), sup, actor.ChildFunc("", worker)); err == nil {
		t.Fatal("started a child from another node")
	}
	if _, err := actor.StartChild(t.Context(), n, sup, actor.ChildFunc("x", worker).WithRestart(actor.Transient).WithSignificant(true)); err == nil {
		t.Fatal("a significant child under a supervisor that never shuts down")
	}
	_, _ = n.Spawn(worker, grpcproc.WithName("taken"))
	if _, err := actor.StartChild(t.Context(), n, sup, actor.ChildFunc("taken", worker)); err == nil {
		t.Fatal("started a child under a taken name")
	}
	for _, req := range []*actorv1.Control{{}, {Op: &actorv1.Control_Start{Start: 1 << 60}}} {
		if _, err := n.CallTo[*grpcprocv1.PID](t.Context(), sup, req); err == nil {
			t.Fatalf("%v accepted", req)
		}
	}
	if len(children(t, n, sup)) != 0 {
		t.Fatal("a refused child was kept")
	}
}

// A group restart stops a temporary child for good; one StartChild added is
// then forgotten.
func TestGroupRestartForgetsTemporaryChildren(t *testing.T) {
	c := grpcproctest.New(t, "a")
	n := c.Node("a")
	sup, err := actor.Supervise(n, actor.Spec{Strategy: actor.OneForAll, Children: []actor.ChildSpec{actor.ChildFunc("w", worker)}})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := actor.StartChild(t.Context(), n, sup, actor.ChildFunc("", worker).WithRestart(actor.Temporary)); err != nil {
		t.Fatal(err)
	}
	w := pidOf(t, n, "w")
	send(t, n, "w", -1)
	restarted(t, n, "w", w)
	until(t, "the temporary child is forgotten", func() bool { return len(children(t, n, sup)) == 1 })
}

// Significant children end their supervisor, with reason shutdown: when any
// ends for good, or when all have, as AutoShutdown says. A significant
// child that is restarted, or that StopChild stops, does not.
func TestSignificantChildren(t *testing.T) {
	c := grpcproctest.New(t, "a")
	n := c.Node("a")
	t.Run("any", func(t *testing.T) {
		sup, err := actor.Supervise(n, actor.Spec{AutoShutdown: actor.AnySignificant, Children: []actor.ChildSpec{
			actor.ChildFunc("job", worker).WithRestart(actor.Transient).WithSignificant(true),
			actor.ChildFunc("other", worker),
		}})
		if err != nil {
			t.Fatal(err)
		}
		downs := watch(t, n, sup)
		other := watch(t, n, pidOf(t, n, "other"))
		job := pidOf(t, n, "job")
		send(t, n, "job", -1) // a crash: restarted
		restarted(t, n, "job", job)
		if insp, _ := n.Inspect(t.Context(), sup); insp["auto_shutdown"] != "any_significant" {
			t.Fatalf("%v", insp)
		}
		send(t, n, "job", 0) // done: the supervisor ends
		if d := down(t, downs); d.Reason != grpcproc.ReasonShutdown {
			t.Fatalf("%+v", d)
		}
		if d := down(t, other); d.Reason != grpcproc.ReasonShutdown {
			t.Fatalf("other: %+v", d)
		}
	})
	t.Run("all", func(t *testing.T) {
		sup, err := actor.Supervise(n, actor.Spec{AutoShutdown: actor.AllSignificant, Children: []actor.ChildSpec{
			actor.ChildFunc("j1", worker).WithRestart(actor.Temporary).WithSignificant(true),
			actor.ChildFunc("j2", worker).WithRestart(actor.Temporary).WithSignificant(true),
			actor.ChildFunc("j3", worker).WithRestart(actor.Temporary).WithSignificant(true),
		}})
		if err != nil {
			t.Fatal(err)
		}
		downs := watch(t, n, sup)
		if err := actor.StopChild(t.Context(), n, sup, pidOf(t, n, "j3")); err != nil {
			t.Fatal(err)
		}
		send(t, n, "j1", 0)
		until(t, "the supervisor saw j1 end", func() bool { return strings.HasPrefix(children(t, n, sup)["j1"], "stopped") })
		if _, alive := n.Process(sup); !alive {
			t.Fatal("ended with a significant child left")
		}
		send(t, n, "j2", -1)
		if d := down(t, downs); d.Reason != grpcproc.ReasonShutdown {
			t.Fatalf("%+v", d)
		}
	})
}

// A named child that outlives its Shutdown keeps its name until it exits:
// a restart that reaches it waits for that, in order, while the supervisor
// goes on answering, instead of failing on the name and giving up.
func TestRestartWaitsForAStuckChild(t *testing.T) {
	n, logs := logged(t)
	sup, err := actor.Supervise(n, actor.Spec{Strategy: actor.OneForAll, Shutdown: 30 * time.Millisecond, Children: []actor.ChildSpec{
		actor.ChildFunc("stuck", draining(200*time.Millisecond)),
		actor.ChildFunc("crasher", worker),
	}})
	if err != nil {
		t.Fatal(err)
	}
	stuck, crasher := pidOf(t, n, "stuck"), pidOf(t, n, "crasher")
	send(t, n, "crasher", -1)
	until(t, "the restart waits", func() bool { return strings.Contains(logs.String(), "restart waits") })
	if kids := children(t, n, sup); !strings.HasPrefix(kids["crasher"], "stopped") || !strings.HasPrefix(kids["stuck"], "waiting "+stuck.String()) {
		t.Fatalf("started before the child before it: %v", kids) // and the supervisor answered
	}
	if _, err := actor.StartChild(t.Context(), n, sup, actor.ChildFunc("", worker)); err == nil || !strings.Contains(err.Error(), "restart is waiting") {
		t.Fatalf("StartChild while a restart waits: %v", err)
	}
	restarted(t, n, "stuck", stuck)
	restarted(t, n, "crasher", crasher)
	if strings.Contains(logs.String(), "level=ERROR") {
		t.Fatalf("logs:\n%s", logs)
	}
}

// The same one level down: a child supervisor ends only once its stuck
// child has, so its parent never starts a new subtree beside it.
func TestStuckChildInASubtree(t *testing.T) {
	n, logs := logged(t)
	top, err := actor.Supervise(n, actor.Spec{Strategy: actor.OneForAll, Children: []actor.ChildSpec{
		actor.ChildSupervisor("inner", actor.Spec{Shutdown: 30 * time.Millisecond, Children: []actor.ChildSpec{
			actor.ChildFunc("s1", draining(200*time.Millisecond)),
		}}),
		actor.ChildFunc("crasher", worker),
	}})
	if err != nil {
		t.Fatal(err)
	}
	s1 := pidOf(t, n, "s1")
	send(t, n, "crasher", -1)
	restarted(t, n, "s1", s1)
	if _, alive := n.Process(top); !alive || strings.Contains(logs.String(), "level=ERROR") {
		t.Fatalf("alive %v, logs:\n%s", alive, logs)
	}
}

// StopChild of a stuck named child returns after its Shutdown; the
// supervisor keeps it, stopped, until it exits, then forgets it.
func TestStopAStuckChild(t *testing.T) {
	c := grpcproctest.New(t, "a")
	n := c.Node("a")
	sup, err := actor.Supervise(n, actor.Spec{})
	if err != nil {
		t.Fatal(err)
	}
	stuck, err := actor.StartChild(t.Context(), n, sup, actor.ChildFunc("stuck", draining(150*time.Millisecond)).WithShutdown(20*time.Millisecond))
	if err != nil {
		t.Fatal(err)
	}
	if err := actor.StopChild(t.Context(), n, sup, stuck); err != nil {
		t.Fatal(err)
	}
	if _, alive := n.Process(stuck); !alive {
		t.Fatal("not stuck after all")
	}
	if kids := children(t, n, sup); !strings.HasPrefix(kids["stuck"], "stopped") {
		t.Fatalf("%v", kids)
	}
	until(t, "the supervisor forgets it", func() bool { return len(children(t, n, sup)) == 0 })
}

// A child supervisor can be started and stopped at runtime; stopping it
// waits for its subtree, past the supervisor's Shutdown.
func TestStartAndStopAChildSupervisor(t *testing.T) {
	c := grpcproctest.New(t, "a")
	n := c.Node("a")
	sup, err := actor.Supervise(n, actor.Spec{Shutdown: 20 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	inner, err := actor.StartChild(t.Context(), n, sup, actor.ChildSupervisor("inner", actor.Spec{Children: []actor.ChildSpec{
		actor.ChildFunc("leaf", draining(100*time.Millisecond)),
	}}))
	if err != nil {
		t.Fatal(err)
	}
	leaf := pidOf(t, n, "leaf")
	if err := actor.StopChild(t.Context(), n, sup, inner); err != nil {
		t.Fatal(err)
	}
	if _, alive := n.Process(leaf); alive {
		t.Fatal("StopChild returned before the subtree stopped")
	}
}

// A StartChild that fails, here because its caller gave up while the
// supervisor waited, or because the supervisor was busy, starts nothing;
// one sent from another node is refused.
func TestStartChildThatFailsStartsNothing(t *testing.T) {
	c := grpcproctest.New(t, "a", "b")
	n := c.Node("a")
	sup, err := actor.Supervise(n, actor.Spec{Children: []actor.ChildSpec{
		actor.ChildFunc("slow", draining(200*time.Millisecond)).WithShutdown(actor.Infinity),
	}})
	if err != nil {
		t.Fatal(err)
	}
	slow := pidOf(t, n, "slow")
	stopped := make(chan error, 1)
	go func() { stopped <- actor.StopChild(context.Background(), n, sup, slow) }()
	until(t, "the supervisor is busy", func() bool {
		ctx, cancel := context.WithTimeout(t.Context(), 10*time.Millisecond)
		defer cancel()
		_, err := n.Inspect(ctx, sup)
		return err != nil
	})
	short, cancel := context.WithTimeout(t.Context(), 10*time.Millisecond)
	defer cancel()
	if _, err := actor.StartChild(short, n, sup, actor.ChildFunc("late", worker)); !errors.Is(err, context.DeadlineExceeded) && !errors.Is(err, actor.ErrBusy) {
		t.Fatalf("got %v", err)
	}
	if err := <-stopped; err != nil {
		t.Fatal(err)
	}
	if kids := children(t, n, sup); len(kids) != 0 {
		t.Fatalf("started after all: %v", kids)
	}
	req := &actorv1.Control{Op: &actorv1.Control_Start{Start: 1}}
	if _, err := c.Node("b").CallTo[*grpcprocv1.PID](t.Context(), sup, req); err == nil || !strings.Contains(err.Error(), "only from the supervisor's node") {
		t.Fatalf("got %v", err)
	}
}

// Children StartChild adds come after the others in RestForOne.
func TestRestForOneWithAddedChildren(t *testing.T) {
	c := grpcproctest.New(t, "a")
	n := c.Node("a")
	sup, err := actor.Supervise(n, actor.Spec{Strategy: actor.RestForOne, Children: []actor.ChildSpec{actor.ChildFunc("w", worker)}})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := actor.StartChild(t.Context(), n, sup, actor.ChildFunc("added", worker)); err != nil {
		t.Fatal(err)
	}
	w, added := pidOf(t, n, "w"), pidOf(t, n, "added")
	send(t, n, "w", -1)
	restarted(t, n, "w", w)
	restarted(t, n, "added", added)
	old := pidOf(t, n, "w")
	send(t, n, "added", -1)
	restarted(t, n, "added", pidOf(t, n, "added"))
	if pidOf(t, n, "w") != old {
		t.Fatal("a child before the crash was restarted")
	}
}

// A significant child StartChild added counts; one a group restart stops
// for good does not end the supervisor.
func TestSignificantChildrenAddedOrStopped(t *testing.T) {
	c := grpcproctest.New(t, "a")
	n := c.Node("a")
	sup, err := actor.Supervise(n, actor.Spec{Strategy: actor.OneForAll, AutoShutdown: actor.AnySignificant, Children: []actor.ChildSpec{
		actor.ChildFunc("temp", worker).WithRestart(actor.Temporary).WithSignificant(true),
		actor.ChildFunc("w", worker),
	}})
	if err != nil {
		t.Fatal(err)
	}
	downs := watch(t, n, sup)
	w := pidOf(t, n, "w")
	send(t, n, "w", -1) // the group restart stops temp for good
	restarted(t, n, "w", w)
	until(t, "temp stopped", func() bool { return strings.HasPrefix(children(t, n, sup)["temp"], "stopped") })
	if _, alive := n.Process(sup); !alive {
		t.Fatal("ended by a significant child it stopped itself")
	}
	if _, err := actor.StartChild(t.Context(), n, sup, actor.ChildFunc("job", worker).WithRestart(actor.Transient).WithSignificant(true)); err != nil {
		t.Fatal(err)
	}
	send(t, n, "job", 0)
	if d := down(t, downs); d.Reason != grpcproc.ReasonShutdown {
		t.Fatalf("%+v", d)
	}
}

// A child that calls its supervisor while it exits, as a Terminate may, is
// answered at once, and what it sends is handled after: the supervisor's
// wait for it does not wait for its call.
func TestChildCallingItsSupervisorWhileItExits(t *testing.T) {
	c := grpcproctest.New(t, "a")
	n := c.Node("a")
	answered := make(chan error, 1)
	polite := func(p *P) error {
		<-p.Context().Done()
		_ = n.SendTo(context.Background(), p.Parent(), &testpb.Ping{N: 1})
		answered <- actor.StopChild(context.Background(), n, p.Parent(), p.PID())
		return nil
	}
	sup, err := actor.Supervise(n, actor.Spec{Strategy: actor.OneForAll, Children: []actor.ChildSpec{
		actor.ChildFunc("polite", polite).WithShutdown(actor.Infinity),
		actor.ChildFunc("crasher", worker),
	}})
	if err != nil {
		t.Fatal(err)
	}
	polite1, crasher := pidOf(t, n, "polite"), pidOf(t, n, "crasher")
	send(t, n, "crasher", -1)
	if err := within(t, answered); !errors.Is(err, actor.ErrBusy) {
		t.Fatalf("the call got %v", err)
	}
	restarted(t, n, "polite", polite1)
	restarted(t, n, "crasher", crasher)
	downs := watch(t, n, sup)
	_ = n.Exit(t.Context(), sup, "bye") // and on its own way out
	if err := within(t, answered); err == nil {
		t.Fatal("answered")
	}
	if d := down(t, downs); d.Reason != "bye" {
		t.Fatalf("%+v", d)
	}
}

// An anonymous child supervisor's subtree holds names too: its parent
// starts it again only once it has ended, however long past its Shutdown.
func TestAnonymousChildSupervisorOutlivesItsShutdown(t *testing.T) {
	n, logs := logged(t)
	top, err := actor.Supervise(n, actor.Spec{Strategy: actor.OneForAll, Children: []actor.ChildSpec{
		actor.ChildSupervisor("", actor.Spec{Shutdown: 20 * time.Millisecond, Children: []actor.ChildSpec{
			actor.ChildFunc("s1", draining(200*time.Millisecond)),
		}}).WithShutdown(40 * time.Millisecond),
		actor.ChildFunc("crasher", worker),
	}})
	if err != nil {
		t.Fatal(err)
	}
	s1 := pidOf(t, n, "s1")
	send(t, n, "crasher", -1)
	restarted(t, n, "s1", s1)
	if _, alive := n.Process(top); !alive || strings.Contains(logs.String(), "level=ERROR") {
		t.Fatalf("alive %v, logs:\n%s", alive, logs)
	}
}

// Calls that come while a supervisor stops a child quickly are answered
// once it has, not refused.
func TestCallsDuringAQuickStop(t *testing.T) {
	c := grpcproctest.New(t, "a")
	n := c.Node("a")
	told := make(chan struct{})
	quick := func(p *P) error {
		<-p.Context().Done()
		close(told) // the supervisor now waits for it
		time.Sleep(60 * time.Millisecond)
		return nil
	}
	sup, err := actor.Supervise(n, actor.Spec{Children: []actor.ChildSpec{actor.ChildFunc("quick", quick)}})
	if err != nil {
		t.Fatal(err)
	}
	pid := pidOf(t, n, "quick")
	stopped := make(chan error, 1)
	go func() { stopped <- actor.StopChild(context.Background(), n, sup, pid) }()
	<-told
	started := make(chan error, 1)
	go func() {
		_, err := actor.StartChild(context.Background(), n, sup, actor.ChildFunc("", worker))
		started <- err
	}()
	// Once the call is queued, an event wakes the waiting supervisor, which
	// takes it, if it has not already.
	time.Sleep(10 * time.Millisecond)
	if _, err := n.Spawn(func(*P) error { return nil }); err != nil {
		t.Fatal(err)
	}
	if err := within(t, started); err != nil {
		t.Fatal(err)
	}
	if err := within(t, stopped); err != nil {
		t.Fatal(err)
	}
}
