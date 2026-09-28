package actor_test

import (
	"errors"
	"strings"
	"testing"
	"testing/synctest"
	"time"

	"github.com/floatdrop/grpcproc"
	"github.com/floatdrop/grpcproc/actor"
	"github.com/floatdrop/grpcproc/grpcproctest"
	"github.com/floatdrop/grpcproc/internal/testpb"
)

// worker exits normally on N == 0, fails on N < 0, and otherwise waits.
func worker(p *P) error {
	for {
		m, err := p.Receive()
		if err != nil {
			return err
		}
		switch n := m.Body.GetN(); {
		case n == 0:
			return nil
		case n < 0:
			return errors.New("crash")
		}
	}
}

func pidOf(t *testing.T, n *grpcproc.Node, name string) grpcproc.PID {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		if pid, ok := n.Whereis(name); ok {
			return pid
		}
		if time.Now().After(deadline) {
			t.Fatalf("%s never registered", name)
		}
		time.Sleep(2 * time.Millisecond)
	}
}

// restarted waits until name is registered to a PID other than old.
func restarted(t *testing.T, n *grpcproc.Node, name string, old grpcproc.PID) grpcproc.PID {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		if pid, ok := n.Whereis(name); ok && pid != old {
			return pid
		}
		if time.Now().After(deadline) {
			t.Fatalf("%s was not restarted", name)
		}
		time.Sleep(2 * time.Millisecond)
	}
}

func send(t *testing.T, n *grpcproc.Node, name string, v int64) {
	_ = grpcproc.Named[*testpb.Ping](n.Name(), name).Send(t.Context(), n, &testpb.Ping{N: v})
}

// settle lets everything the test started run until it waits, then 50ms of
// the bubble's time pass, for a supervisor's timers. In a synctest bubble
// that is exact, and takes no time: every test here runs in one.
func settle() { time.Sleep(50 * time.Millisecond) }

func TestStrategies(t *testing.T) {
	names := []string{"w1", "w2", "w3"}
	children := func() []actor.ChildSpec {
		var out []actor.ChildSpec
		for _, name := range names {
			out = append(out, actor.ChildFunc(name, worker))
		}
		return out
	}
	cases := []struct {
		strategy actor.Strategy
		changed  []bool // after w2 crashes
	}{
		{actor.OneForOne, []bool{false, true, false}},
		{actor.OneForAll, []bool{true, true, true}},
		{actor.RestForOne, []bool{false, true, true}},
	}
	for _, tc := range cases {
		t.Run(tc.strategy.String(), func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				c := grpcproctest.New(t, "a")
				n := c.Node("a")
				sup, err := actor.Supervise(n, actor.Spec{Strategy: tc.strategy, Children: children()})
				if err != nil {
					t.Fatal(err)
				}
				before := map[string]grpcproc.PID{}
				for _, name := range names {
					before[name] = pidOf(t, n, name)
				}
				send(t, n, "w2", -1)
				restarted(t, n, "w2", before["w2"])
				settle()
				for i, name := range names {
					now := pidOf(t, n, name)
					if (now != before[name]) != tc.changed[i] {
						t.Errorf("%s: restarted=%v, want %v", name, now != before[name], tc.changed[i])
					}
				}
				info, _ := n.Process(pidOf(t, n, "w2"))
				if info.Parent != sup {
					t.Fatalf("parent %v, want %v", info.Parent, sup)
				}
				insp, err := n.Inspect(t.Context(), sup)
				if err != nil || insp["strategy"] != tc.strategy.String() || !strings.Contains(insp["child.w2"], "restarts=1") || !strings.HasPrefix(insp["restarts"], "1/3") {
					t.Fatalf("inspect %v %v", insp, err)
				}
			})
		})
	}
}

func TestRestartPolicies(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		c := grpcproctest.New(t, "a")
		n := c.Node("a")
		_, err := actor.Supervise(n, actor.Spec{Strategy: actor.OneForOne, Children: []actor.ChildSpec{
			actor.ChildFunc("perm", worker),
			actor.ChildFunc("trans", worker).WithRestart(actor.Transient),
			actor.ChildFunc("temp", worker).WithRestart(actor.Temporary),
		}})
		if err != nil {
			t.Fatal(err)
		}
		// A permanent child comes back even after a normal exit.
		old := pidOf(t, n, "perm")
		send(t, n, "perm", 0)
		restarted(t, n, "perm", old)
		// A transient child comes back after a crash, not after a normal exit.
		old = pidOf(t, n, "trans")
		send(t, n, "trans", -1)
		old = restarted(t, n, "trans", old)
		send(t, n, "trans", 0)
		settle()
		if _, ok := n.Whereis("trans"); ok {
			t.Fatal("transient child restarted after a normal exit")
		}
		// A temporary child never comes back.
		send(t, n, "temp", -1)
		settle()
		if _, ok := n.Whereis("temp"); ok {
			t.Fatal("temporary child restarted")
		}
		_ = old
	})
}

func TestOneForAllKeepsFinishedChildrenFinished(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		c := grpcproctest.New(t, "a")
		n := c.Node("a")
		_, err := actor.Supervise(n, actor.Spec{Strategy: actor.OneForAll, Children: []actor.ChildSpec{
			actor.ChildFunc("done", worker).WithRestart(actor.Transient),
			actor.ChildFunc("temp", worker).WithRestart(actor.Temporary),
			actor.ChildFunc("main", worker),
		}})
		if err != nil {
			t.Fatal(err)
		}
		send(t, n, "done", 0) // finishes normally: stays finished
		settle()
		old := pidOf(t, n, "main")
		send(t, n, "main", -1)
		restarted(t, n, "main", old)
		settle()
		if _, ok := n.Whereis("done"); ok {
			t.Fatal("a finished transient child was brought back by a sibling's crash")
		}
		if _, ok := n.Whereis("temp"); ok {
			t.Fatal("a temporary child was restarted with the group")
		}
	})
}

func TestIntensityAndEscalation(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		c := grpcproctest.New(t, "a")
		n := c.Node("a")
		// The inner supervisor gives up after one restart; the outer restarts it.
		outer, err := actor.Supervise(n, actor.Spec{Children: []actor.ChildSpec{
			actor.ChildSupervisor("inner", actor.Spec{MaxRestarts: 1, Within: time.Minute, Children: []actor.ChildSpec{
				actor.ChildFunc("leaf", worker),
			}}),
		}})
		if err != nil {
			t.Fatal(err)
		}
		inner := pidOf(t, n, "inner")
		innerDowns := watch(t, n, inner)
		leaf := pidOf(t, n, "leaf")
		send(t, n, "leaf", -1)
		leaf = restarted(t, n, "leaf", leaf) // first restart: allowed
		send(t, n, "leaf", -1)               // second: over the limit
		if d := down(t, innerDowns); d.Reason != actor.ReasonMaxRestarts {
			t.Fatalf("inner exited with %q", d.Reason)
		}
		// The outer supervisor restarted the inner one, which started a new leaf.
		restarted(t, n, "inner", inner)
		restarted(t, n, "leaf", leaf)
		insp, _ := n.Inspect(t.Context(), outer)
		if !strings.Contains(insp["child.inner"], "restarts=1") {
			t.Fatalf("%v", insp)
		}
	})
}

func TestNoRestartsAllowed(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		c := grpcproctest.New(t, "a")
		n := c.Node("a")
		sup, err := actor.Supervise(n, actor.Spec{MaxRestarts: -1, Children: []actor.ChildSpec{actor.ChildFunc("w", worker)}})
		if err != nil {
			t.Fatal(err)
		}
		downs := watch(t, n, sup)
		send(t, n, "w", -1)
		if d := down(t, downs); d.Reason != actor.ReasonMaxRestarts {
			t.Fatalf("%+v", d)
		}
	})
}

func TestSupervisorExitStopsChildren(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		c := grpcproctest.New(t, "a")
		n := c.Node("a")
		sup, err := actor.Supervise(n, actor.Spec{Children: []actor.ChildSpec{
			actor.ChildFunc("w1", worker), actor.ChildFunc("w2", worker),
		}}, grpcproc.WithName("sup"))
		if err != nil {
			t.Fatal(err)
		}
		w1 := watch(t, n, pidOf(t, n, "w1"))
		w2 := watch(t, n, pidOf(t, n, "w2"))
		// Calls to a supervisor are refused; other messages are ignored.
		if _, err := n.CallTo[*testpb.Pong](t.Context(), sup, &testpb.Ping{}); err == nil {
			t.Fatal("a supervisor answered a call")
		}
		_ = n.SendTo(t.Context(), sup, &testpb.Ping{})
		_ = n.Exit(t.Context(), grpcproc.Name{Node: "a", Name: "sup"}, grpcproc.ReasonKilled)
		for _, ch := range []<-chan grpcproc.Down{w1, w2} {
			if d := down(t, ch); d.Reason != grpcproc.ReasonShutdown {
				t.Fatalf("child exited with %q", d.Reason)
			}
		}
	})
}

func TestChildIgnoringExit(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		c := grpcproctest.New(t, "a")
		n := c.Node("a")
		release := make(chan struct{})
		defer close(release)
		stubborn := func(p *P) error {
			m, err := p.Receive()
			if err != nil {
				return err
			}
			if m.Body.GetN() == 5 {
				<-release // never looks at Exit
			}
			return nil
		}
		_, err := actor.Supervise(n, actor.Spec{Strategy: actor.OneForAll, Shutdown: 30 * time.Millisecond, Children: []actor.ChildSpec{
			actor.ChildFunc("stubborn", stubborn).WithRestart(actor.Temporary),
			actor.ChildFunc("w", worker),
		}})
		if err != nil {
			t.Fatal(err)
		}
		send(t, n, "stubborn", 5)
		settle()
		old := pidOf(t, n, "w")
		start := time.Now()
		send(t, n, "w", -1)
		restarted(t, n, "w", old)
		if time.Since(start) < 30*time.Millisecond {
			t.Fatal("did not wait for the stubborn child")
		}
	})
}

func TestStartFailures(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		c := grpcproctest.New(t, "a")
		n := c.Node("a")
		// A name already taken: Supervise fails and stops what it started.
		_, _ = n.Spawn(worker, grpcproc.WithName("taken"))
		_, err := actor.Supervise(n, actor.Spec{Children: []actor.ChildSpec{
			actor.ChildFunc("first", worker), actor.ChildFunc("taken", worker),
		}})
		if err == nil || !strings.Contains(err.Error(), `start child 1, "taken"`) {
			t.Fatalf("got %v", err)
		}
		settle()
		if _, ok := n.Whereis("first"); ok {
			t.Fatal("first child left running")
		}
		// A nested supervisor that cannot start fails its parent's start too.
		_, err = actor.Supervise(n, actor.Spec{Children: []actor.ChildSpec{
			actor.ChildSupervisor("inner", actor.Spec{Children: []actor.ChildSpec{actor.ChildFunc("taken", worker)}}),
		}})
		if err == nil {
			t.Fatal("nested start failure not reported")
		}
	})
}

func TestSpecValidation(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		c := grpcproctest.New(t, "a")
		n := c.Node("a")
		for _, spec := range []actor.Spec{
			{Children: []actor.ChildSpec{{Name: "raw"}}},
			{Children: []actor.ChildSpec{actor.ChildFunc("x", worker), actor.ChildFunc("x", worker)}},
			{Strategy: 9},
			{AutoShutdown: 9},
			{Children: []actor.ChildSpec{actor.ChildFunc("x", worker).WithRestart(9)}},
			{Children: []actor.ChildSpec{actor.ChildFunc("x", worker).WithShutdown(-5)}},
			{Shutdown: -5},
			{AutoShutdown: actor.AnySignificant, Children: []actor.ChildSpec{actor.ChildFunc("x", worker).WithSignificant(true)}},
			{Children: []actor.ChildSpec{actor.ChildFunc("x", worker).WithRestart(actor.Transient).WithSignificant(true)}},
		} {
			if _, err := actor.Supervise(n, spec); err == nil {
				t.Errorf("accepted %+v", spec)
			}
		}
		if s := actor.Strategy(9).String() + actor.Restart(9).String() + actor.AutoShutdown(9).String(); s != "Strategy(9)Restart(9)AutoShutdown(9)" {
			t.Fatal(s)
		}
		c.Stop("a")
		if _, err := actor.Supervise(n, actor.Spec{}); err == nil {
			t.Fatal("started on a stopped node")
		}
	})
}

func TestChildHandlerIsFreshOnRestart(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		c := grpcproctest.New(t, "a")
		n := c.Node("a")
		built := 0
		_, err := actor.Supervise(n, actor.Spec{Children: []actor.ChildSpec{
			actor.Child[*testpb.Ping]("counter", func() *counter { built++; return &counter{} }),
		}})
		if err != nil {
			t.Fatal(err)
		}
		addr := grpcproc.Named[*testpb.Ping]("a", "counter")
		pidOf(t, n, "counter")
		_ = addr.Send(t.Context(), n, &testpb.Ping{N: 1})
		_ = addr.Send(t.Context(), n, &testpb.Ping{N: 1})
		if r, _ := addr.Call[*testpb.Pong](t.Context(), n, &testpb.Ping{}); r.GetN() != 2 {
			t.Fatalf("count %d", r.GetN())
		}
		old := pidOf(t, n, "counter")
		_ = addr.Send(t.Context(), n, &testpb.Ping{N: -1})
		restarted(t, n, "counter", old)
		if r, _ := addr.Call[*testpb.Pong](t.Context(), n, &testpb.Ping{}); r.GetN() != 0 {
			t.Fatalf("state survived a restart: %d", r.GetN())
		}
		if built != 2 {
			t.Fatalf("built %d handlers", built)
		}
		if actor.Permanent.String() != "permanent" || actor.Transient.String() != "transient" || actor.Temporary.String() != "temporary" {
			t.Fatal("restart strings")
		}
	})
}
