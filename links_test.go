package grpcproc_test

import (
	"errors"
	"testing"
	"testing/synctest"

	"google.golang.org/protobuf/proto"

	"github.com/floatdrop/grpcproc"
	"github.com/floatdrop/grpcproc/grpcproctest"
	"github.com/floatdrop/grpcproc/internal/testpb"
)

// observed is a process that forwards what it receives, and whose own exit
// reason the returned function waits for.
func observed(t *testing.T, n *grpcproc.Node) (*grpcproc.Process[proto.Message], <-chan grpcproc.Msg[proto.Message], func() string) {
	t.Helper()
	p, ch := watcher(t, n)
	obs, downs := watcher(t, n)
	ref := obs.Monitor(p.PID())
	return p, ch, func() string {
		t.Helper()
		m := recv(t, downs)
		if m.Down == nil || m.Down.Ref != ref {
			t.Fatalf("not the Down of %v: %+v", p.PID(), m)
		}
		return m.Down.Reason
	}
}

func spawnEcho(t *testing.T, n *grpcproc.Node, opts ...grpcproc.SpawnOption) grpcproc.Addr[*testpb.Ping] {
	t.Helper()
	a, err := n.Spawn(echo, opts...)
	if err != nil {
		t.Fatal(err)
	}
	return a
}

// A process linked to another exits when it does, with its reason, however
// it exits: normally, with an error, or asked to.
func TestLinkEndsTheLinker(t *testing.T) {
	type ender = func(t *testing.T, a *grpcproc.Node, e grpcproc.Addr[*testpb.Ping])
	for _, tc := range []struct {
		name, reason string
		end          ender
	}{
		{"normal", grpcproc.ReasonNormal, func(t *testing.T, a *grpcproc.Node, e grpcproc.Addr[*testpb.Ping]) {
			_ = e.Send(ctx(t), a, &testpb.Ping{N: 0})
		}},
		{"error", "boom", func(t *testing.T, a *grpcproc.Node, e grpcproc.Addr[*testpb.Ping]) {
			_ = e.Send(ctx(t), a, &testpb.Ping{N: -100})
		}},
		{"exit", "closing", func(t *testing.T, a *grpcproc.Node, e grpcproc.Addr[*testpb.Ping]) { _ = a.Exit(ctx(t), e, "closing") }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				a := grpcproctest.New(t, "a").Node("a")
				target := spawnEcho(t, a)
				p, _, reason := observed(t, a)
				p.Link(target)
				tc.end(t, a, target)
				if got := reason(); got != tc.reason {
					t.Fatalf("linker exited with %q, want %q", got, tc.reason)
				}
			})
		})
	}
}

// A link is one way: the linker's exit leaves its target alone.
func TestLinkIsOneWay(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		c := grpcproctest.New(t, "a")
		a := c.Node("a")
		target := spawnEcho(t, a)
		p, _, reason := observed(t, a)
		p.Link(target)
		_ = a.Exit(ctx(t), p.PID(), "bye")
		if got := reason(); got != "bye" {
			t.Fatal(got)
		}
		if resp, err := target.Call[*testpb.Pong](ctx(t), a, &testpb.Ping{N: 1}); err != nil || resp.GetN() != 2 {
			t.Fatalf("the target went with its linker: %v, %v", resp, err)
		}
	})
}

// A process that traps exits receives them, and carries on.
func TestTrapExit(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		c := grpcproctest.New(t, "a")
		a := c.Node("a")
		target := spawnEcho(t, a)
		p, ch := watcher(t, a)
		p.SetTrapExit(true)
		if !p.TrapExit() || !p.Parent().IsZero() {
			t.Fatal("not trapping, or a parent for a process Node.Spawn started")
		}
		p.Link(target)
		_ = target.Send(ctx(t), a, &testpb.Ping{N: -100})
		m := recv(t, ch)
		if m.Exited == nil || m.From != target.PID() || *m.Exited != (grpcproc.Exited{PID: target.PID(), Reason: "boom"}) {
			t.Fatalf("got %+v", m)
		}
		info, alive := a.Process(p.PID())
		if !alive || info.Links != 0 || !info.TrapExit {
			t.Fatalf("after the Exited: alive %v, %+v", alive, info)
		}
		// A request to exit is not trapped.
		_ = a.Exit(ctx(t), p.PID(), "closing")
		eventually(t, "the trapping process exits", func() bool { _, alive := a.Process(p.PID()); return !alive })
	})
}

// A link to a process that does not exist ends the linker with noproc; by
// name, the Exit names what was linked to.
func TestLinkToNothing(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		c := grpcproctest.New(t, "a")
		a := c.Node("a")
		p, _, reason := observed(t, a)
		gone := a.PID()
		gone.ID = 1 << 40
		p.Link(gone)
		if got := reason(); got != grpcproc.ReasonNoProc {
			t.Fatal(got)
		}
		trap, ch := watcher(t, a)
		trap.SetTrapExit(true)
		trap.Link(grpcproc.Name{Node: "a", Name: "nobody"})
		if m := recv(t, ch); m.Exited == nil || *m.Exited != (grpcproc.Exited{PID: grpcproc.PID{Node: "a"}, Name: "nobody", Reason: grpcproc.ReasonNoProc}) {
			t.Fatalf("got %+v", m)
		}
	})
}

// A link by name follows the process that holds the name when it is linked.
func TestLinkByName(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		c := grpcproctest.New(t, "a")
		a := c.Node("a")
		target := spawnEcho(t, a, grpcproc.WithName("echo"))
		p, ch := watcher(t, a)
		p.SetTrapExit(true)
		p.Link(grpcproc.Named[*testpb.Ping]("a", "echo"))
		waitWatchers(t, a, target.PID(), 1)
		_ = target.Send(ctx(t), a, &testpb.Ping{N: 0})
		if m := recv(t, ch); m.Exited == nil || *m.Exited != (grpcproc.Exited{PID: target.PID(), Name: "echo", Reason: grpcproc.ReasonNormal}) {
			t.Fatalf("got %+v", m)
		}
	})
}

// Linking twice to a target is one link, linking to itself none; Unlink
// removes the link, after which the target's exit is nothing to the linker.
func TestLinkOnceAndUnlink(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		c := grpcproctest.New(t, "a")
		a := c.Node("a")
		target := spawnEcho(t, a)
		p, ch := watcher(t, a)
		p.SetTrapExit(true)
		p.Link(target)
		p.Link(target.PID())
		p.Link(p.PID())
		if info, _ := a.Process(p.PID()); info.Links != 1 {
			t.Fatalf("links %d", info.Links)
		}
		waitWatchers(t, a, target.PID(), 1)
		p.Unlink(p.PID())                             // not linked: nothing
		p.Unlink(grpcproc.Name{Node: "a", Name: "x"}) // nor by name
		p.Unlink(target)
		waitWatchers(t, a, target.PID(), 0)
		if info, _ := a.Process(p.PID()); info.Links != 0 {
			t.Fatalf("links %d", info.Links)
		}
		// The target knows of no link any more, so its exit sends none: what p
		// receives next is what the test sends it once the target is gone.
		obs, downs := watcher(t, a)
		obs.Monitor(target)
		_ = target.Send(ctx(t), a, &testpb.Ping{N: -100})
		recv(t, downs)
		_ = grpcproc.AddrOf[proto.Message](p.PID()).Send(ctx(t), a, &testpb.Pong{N: 7})
		if m := recv(t, ch); m.Body == nil {
			t.Fatalf("an Exited after Unlink: %+v", m)
		}
	})
}

// Across nodes a link is the same: the target's reason, or noconnection when
// its node cannot be reached, now or later.
func TestLinkAcrossNodes(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		c := grpcproctest.New(t, "a", "b")
		a, b := c.Node("a"), c.Node("b")
		target := spawnEcho(t, b)
		p, _, reason := observed(t, a)
		p.Link(target)
		waitWatchers(t, b, target.PID(), 1)
		_ = target.Send(ctx(t), a, &testpb.Ping{N: -100})
		if got := reason(); got != "boom" {
			t.Fatal(got)
		}

		target = spawnEcho(t, b)
		trap, ch := watcher(t, a)
		trap.SetTrapExit(true)
		trap.Link(target)
		waitWatchers(t, b, target.PID(), 1)
		c.Partition("a", "b")
		if m := recv(t, ch); m.Exited == nil || *m.Exited != (grpcproc.Exited{PID: target.PID(), Reason: grpcproc.ReasonNoConnection}) {
			t.Fatalf("got %+v", m)
		}
		// b dropped the link too: it cannot deliver an exit to a.
		waitWatchers(t, b, target.PID(), 0)
		trap.Link(grpcproc.Name{Node: "nowhere", Name: "x"})
		if m := recv(t, ch); m.Exited == nil || m.Exited.Reason != grpcproc.ReasonNoConnection || m.Exited.Name != "x" {
			t.Fatalf("got %+v", m)
		}
	})
}

// LinkParent and LinkChild link a child and its parent before the child runs.
func TestSpawnLinks(t *testing.T) {
	t.Run("parent", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) { // the child goes with its parent
			a := grpcproctest.New(t, "a").Node("a")
			p, _ := watcher(t, a)
			child, err := p.Spawn(echo, grpcproc.LinkParent())
			if err != nil {
				t.Fatal(err)
			}
			obs, downs := watcher(t, a)
			obs.Monitor(child)
			if info, _ := a.Process(child.PID()); info.Links != 1 || info.Parent != p.PID() {
				t.Fatalf("%+v", info)
			}
			_ = a.Exit(ctx(t), p.PID(), "bye")
			if m := recv(t, downs); m.Down == nil || m.Down.Reason != "bye" {
				t.Fatalf("got %+v", m)
			}
		})
	})

	t.Run("child", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) { // the parent goes with its child, however soon it exits
			a := grpcproctest.New(t, "a").Node("a")
			p, _, reason := observed(t, a)
			if _, err := p.Spawn(func(*grpcproc.Process[*testpb.Ping]) error { return errors.New("boom") }, grpcproc.LinkChild()); err != nil {
				t.Fatal(err)
			}
			if got := reason(); got != "boom" {
				t.Fatal(got)
			}
			trap, ch := watcher(t, a)
			trap.SetTrapExit(true)
			for range 20 { // never noproc: the link is there before the child runs
				child, err := trap.Spawn(func(*grpcproc.Process[*testpb.Ping]) error { return errors.New("boom") }, grpcproc.LinkChild())
				if err != nil {
					t.Fatal(err)
				}
				if m := recv(t, ch); m.Exited == nil || *m.Exited != (grpcproc.Exited{PID: child.PID(), Reason: "boom"}) {
					t.Fatalf("got %+v", m)
				}
			}
		})
	})

	t.Run("both ways", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) { // Erlang's spawn_link, with a monitor as well
			a := grpcproctest.New(t, "a").Node("a")
			p, ch := watcher(t, a)
			p.SetTrapExit(true)
			child, ref, err := p.SpawnMonitor(echo, grpcproc.LinkParent(), grpcproc.LinkChild())
			if err != nil {
				t.Fatal(err)
			}
			if info, _ := a.Process(p.PID()); info.Links != 1 || info.Monitors != 1 || info.Watchers != 1 {
				t.Fatalf("parent %+v", info)
			}
			_ = child.Send(ctx(t), a, &testpb.Ping{N: -100})
			var down, exit bool
			for range 2 {
				switch m := recv(t, ch); {
				case m.Down != nil && m.Down.Ref == ref && m.Down.Reason == "boom":
					down = true
				case m.Exited != nil && m.Exited.PID == child.PID() && m.Exited.Reason == "boom":
					exit = true
				default:
					t.Fatalf("got %+v", m)
				}
			}
			if !down || !exit {
				t.Fatal("missing the Down or the Exited")
			}
			if info, _ := a.Process(p.PID()); info.Links != 0 || info.Monitors != 0 {
				t.Fatalf("parent after %+v", info)
			}
			waitWatchers(t, a, p.PID(), 0) // the child's exit unlinks it from the parent
		})
	})

	t.Run("no parent", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			a := grpcproctest.New(t, "a").Node("a")
			for _, opt := range []grpcproc.SpawnOption{grpcproc.LinkParent(), grpcproc.LinkChild()} {
				if _, err := a.Spawn(echo, opt); err == nil {
					t.Fatal("Node.Spawn linked to no parent")
				}
			}
		})
	})
}

// Across nodes, a link by name reports the process that held the name; a
// link to no process, or to a stale incarnation, is noproc; and a node that
// stops gracefully ends its linkers with shutdown.
func TestLinkAcrossNodesByNameAndToNothing(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		c := grpcproctest.New(t, "a", "b")
		a, b := c.Node("a"), c.Node("b")
		target := spawnEcho(t, b, grpcproc.WithName("echo"))
		p, ch := watcher(t, a)
		p.SetTrapExit(true)
		p.Link(grpcproc.Name{Node: "b", Name: "echo"})
		waitWatchers(t, b, target.PID(), 1)
		_ = target.Send(ctx(t), a, &testpb.Ping{N: 0})
		if m := recv(t, ch); m.Exited == nil || *m.Exited != (grpcproc.Exited{PID: target.PID(), Name: "echo", Reason: grpcproc.ReasonNormal}) {
			t.Fatalf("by name: %+v", m)
		}
		stale := b.PID()
		stale.Incarnation--
		stale.ID = target.PID().ID
		for _, tc := range []struct {
			to   grpcproc.Target
			want grpcproc.Exited
		}{
			{target.PID(), grpcproc.Exited{PID: target.PID(), Reason: grpcproc.ReasonNoProc}}, // gone
			{stale, grpcproc.Exited{PID: stale, Reason: grpcproc.ReasonNoProc}},
			{grpcproc.Name{Node: "b", Name: "echo"}, grpcproc.Exited{PID: grpcproc.PID{Node: "b"}, Name: "echo", Reason: grpcproc.ReasonNoProc}},
		} {
			p.Link(tc.to)
			if m := recv(t, ch); m.Exited == nil || *m.Exited != tc.want {
				t.Fatalf("%v: %+v", tc.to, m)
			}
		}

		target = spawnEcho(t, b)
		linker, _, reason := observed(t, a)
		linker.Link(target)
		waitWatchers(t, b, target.PID(), 1)
		c.Stop("b")
		if got := reason(); got != grpcproc.ReasonShutdown {
			t.Fatal(got)
		}
	})
}

// Trapping takes effect for the exits that come after it changes.
func TestTrapExitToggles(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		c := grpcproctest.New(t, "a")
		a := c.Node("a")
		p, ch, reason := observed(t, a)
		p.SetTrapExit(true)
		first, second := spawnEcho(t, a), spawnEcho(t, a)
		p.Link(first)
		p.Link(second)
		_ = first.Send(ctx(t), a, &testpb.Ping{N: 0})
		if m := recv(t, ch); m.Exited == nil || m.Exited.PID != first.PID() {
			t.Fatalf("trapping: %+v", m)
		}
		p.SetTrapExit(false)
		_ = second.Send(ctx(t), a, &testpb.Ping{N: -100})
		if got := reason(); got != "boom" {
			t.Fatal(got)
		}
	})
}

type exitedHooks struct {
	grpcproc.NopHooks
	seen chan grpcproc.Exited
}

func (h exitedHooks) OnReceive(r grpcproc.ReceiveInfo, md grpcproc.Metadata) (grpcproc.Metadata, grpcproc.Done) {
	if r.Exited != nil {
		h.seen <- *r.Exited
	}
	return md, nil
}

// OnReceive sees an Exited as it sees a Down.
func TestOnReceiveSeesExited(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		h := exitedHooks{seen: make(chan grpcproc.Exited, 1)}
		c := grpcproctest.NewWith(t, []grpcproctest.Option{grpcproctest.WithHooks(h)}, "a")
		a := c.Node("a")
		target := spawnEcho(t, a)
		p, _ := watcher(t, a)
		p.SetTrapExit(true)
		p.Link(target)
		_ = target.Send(ctx(t), a, &testpb.Ping{N: -100})
		if e := within(t, h.seen, "Exited"); e.PID != target.PID() || e.Reason != "boom" {
			t.Fatalf("got %+v", e)
		}
	})
}
