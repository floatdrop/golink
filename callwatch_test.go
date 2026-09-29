package grpcproc_test

import (
	"context"
	"errors"
	"testing"
	"testing/synctest"

	"google.golang.org/protobuf/proto"

	"github.com/floatdrop/grpcproc"
	"github.com/floatdrop/grpcproc/grpcproctest"
	"github.com/floatdrop/grpcproc/internal/testpb"
)

// spawned is a call a spawner took, the child it spawned for it with the
// watch its caller asked for, or why the spawn failed. The test answers it.
type spawned struct {
	m     grpcproc.Msg[*testpb.Ping]
	child grpcproc.PID
	err   error
}

// spawner spawns child for every call it takes, with the watch the call's
// caller asked for, and hands the call over to be answered.
func spawner(t *testing.T, n *grpcproc.Node, child func(*grpcproc.Process[*testpb.Ping]) error) (grpcproc.Addr[*testpb.Ping], <-chan spawned) {
	t.Helper()
	ch := make(chan spawned, 8)
	addr, err := n.Spawn(func(p *grpcproc.Process[*testpb.Ping]) error {
		for {
			m, err := p.Receive()
			if err != nil {
				return err
			}
			a, err := p.Spawn(child, grpcproc.WatchedBy(m))
			ch <- spawned{m, a.PID(), err}
		}
	})
	if err != nil {
		t.Fatal(err)
	}
	return addr, ch
}

// called is what a CallMonitor or CallLink returned.
type called struct {
	resp *testpb.Pong
	ref  grpcproc.Ref
	err  error
}

// callMonitor calls to with CallMonitor, as w, from a goroutine of its own:
// the test answers the call meanwhile.
func callMonitor(ctx context.Context, w *grpcproc.Process[proto.Message], to grpcproc.Addr[*testpb.Ping]) <-chan called {
	ch := make(chan called, 1)
	go func() {
		resp, ref, err := to.CallMonitor[*testpb.Pong](ctx, w, &testpb.Ping{N: 1})
		ch <- called{resp, ref, err}
	}()
	return ch
}

// callLink is callMonitor with CallLink.
func callLink(ctx context.Context, w *grpcproc.Process[proto.Message], to grpcproc.Addr[*testpb.Ping]) <-chan called {
	ch := make(chan called, 1)
	go func() {
		resp, err := to.CallLink[*testpb.Pong](ctx, w, &testpb.Ping{N: 1})
		ch <- called{resp: resp, err: err}
	}()
	return ch
}

// early exits at once, before its spawner can answer for it.
func early(*grpcproc.Process[*testpb.Ping]) error { return errors.New("early") }

// A call's caller monitors the process the callee spawns for it from before
// that process runs, on the callee's node or another: however soon it exits,
// the Down comes after the answer, with the real reason. A call that fails
// leaves no monitor behind, and no Down comes of it.
func TestCallMonitor(t *testing.T) {
	for _, node := range []string{"a", "b"} {
		t.Run(node, func(t *testing.T) {
			setup := func(t *testing.T, child func(*grpcproc.Process[*testpb.Ping]) error) (*grpcproc.Node, *grpcproc.Process[proto.Message], <-chan grpcproc.Msg[proto.Message], grpcproc.Addr[*testpb.Ping], <-chan spawned) {
				c := grpcproctest.New(t, "a", "b")
				w, got := watcher(t, c.Node("a"))
				to, spawns := spawner(t, c.Node(node), child)
				return c.Node(node), w, got, to, spawns
			}
			down := func(t *testing.T, got <-chan grpcproc.Msg[proto.Message], ref grpcproc.Ref, pid grpcproc.PID, reason string) {
				t.Helper()
				if m := recv(t, got); m.Down == nil || m.Down.Ref != ref || m.Down.PID != pid || m.Down.Reason != reason {
					t.Fatalf("got %+v, want the Down of %v with %q", m.Down, pid, reason)
				}
			}
			t.Run("exits after the answer", func(t *testing.T) {
				synctest.Test(t, func(t *testing.T) {
					n, w, got, to, spawns := setup(t, echo)
					res := callMonitor(ctx(t), w, to)
					s := within(t, spawns, "spawn")
					if s.err != nil {
						t.Fatal(s.err)
					}
					_ = s.m.Reply(&testpb.Pong{N: 7}, nil)
					r := within(t, res, "answer")
					if r.err != nil || r.resp.GetN() != 7 || r.ref.Node != "a" {
						t.Fatalf("%+v", r)
					}
					waitWatchers(t, n, s.child, 1)
					_ = grpcproc.AddrOf[*testpb.Ping](s.child).Send(ctx(t), w, &testpb.Ping{N: -100})
					down(t, got, r.ref, s.child, "boom")
				})
			})
			t.Run("exits before the answer", func(t *testing.T) {
				synctest.Test(t, func(t *testing.T) {
					n, w, got, to, spawns := setup(t, early)
					res := callMonitor(ctx(t), w, to)
					s := within(t, spawns, "spawn")
					eventually(t, "the child to exit", func() bool { _, ok := n.Process(s.child); return !ok })
					synctest.Wait() // its Down has reached the caller's node, and waits there
					noMore(t, got)
					_ = s.m.Reply(&testpb.Pong{}, nil)
					r := within(t, res, "answer")
					if r.err != nil {
						t.Fatal(r.err)
					}
					down(t, got, r.ref, s.child, "early") // not noproc
				})
			})
			t.Run("demonitor", func(t *testing.T) {
				synctest.Test(t, func(t *testing.T) {
					n, w, got, to, spawns := setup(t, echo)
					res := callMonitor(ctx(t), w, to)
					s := within(t, spawns, "spawn")
					_ = s.m.Reply(&testpb.Pong{}, nil)
					w.Demonitor(within(t, res, "answer").ref)
					waitWatchers(t, n, s.child, 0)
					_ = grpcproc.AddrOf[*testpb.Ping](s.child).Send(ctx(t), w, &testpb.Ping{N: 0})
					noMore(t, got)
				})
			})
			// An error answer takes the monitor back, and a Down that came
			// before it is dropped: the error says the call failed.
			for _, before := range []bool{false, true} {
				name := map[bool]string{false: "error answer", true: "error answer after the child exited"}[before]
				t.Run(name, func(t *testing.T) {
					synctest.Test(t, func(t *testing.T) {
						child := echo
						if before {
							child = early
						}
						n, w, got, to, spawns := setup(t, child)
						res := callMonitor(ctx(t), w, to)
						s := within(t, spawns, "spawn")
						if before {
							eventually(t, "the child to exit", func() bool { _, ok := n.Process(s.child); return !ok })
							synctest.Wait()
						}
						_ = s.m.Reply(nil, errNegativeTwo)
						if r := within(t, res, "answer"); !errors.Is(r.err, errNegativeTwo) || r.ref != (grpcproc.Ref{}) {
							t.Fatalf("%+v", r)
						}
						if !before {
							waitWatchers(t, n, s.child, 0)
							_ = grpcproc.AddrOf[*testpb.Ping](s.child).Send(ctx(t), w, &testpb.Ping{N: 0})
						}
						noMore(t, got)
					})
				})
			}
			// An answer the caller cannot take is ErrType, and the monitor
			// the callee placed is taken back.
			t.Run("answer of another type", func(t *testing.T) {
				synctest.Test(t, func(t *testing.T) {
					n, w, got, to, spawns := setup(t, echo)
					res := callMonitor(ctx(t), w, to)
					s := within(t, spawns, "spawn")
					_ = s.m.Reply(&testpb.Ping{}, nil)
					if r := within(t, res, "answer"); !errors.Is(r.err, grpcproc.ErrType) {
						t.Fatal(r.err)
					}
					waitWatchers(t, n, s.child, 0)
					noMore(t, got)
				})
			})
			// A caller that gives up takes no monitor: the child's Down, when
			// it comes, is dropped.
			t.Run("caller gives up", func(t *testing.T) {
				synctest.Test(t, func(t *testing.T) {
					_, w, got, to, spawns := setup(t, echo)
					short, cancel := context.WithCancel(ctx(t))
					res := callMonitor(short, w, to)
					s := within(t, spawns, "spawn")
					cancel()
					if r := within(t, res, "answer"); !errors.Is(r.err, context.Canceled) {
						t.Fatal(r.err)
					}
					_ = s.m.Reply(&testpb.Pong{}, nil)
					_ = grpcproc.AddrOf[*testpb.Ping](s.child).Send(ctx(t), w, &testpb.Ping{N: 0})
					noMore(t, got)
				})
			})
			// The callee's exit answers ErrNoProc, which takes the monitor
			// back.
			t.Run("callee exits", func(t *testing.T) {
				synctest.Test(t, func(t *testing.T) {
					n, w, got, to, spawns := setup(t, echo)
					res := callMonitor(ctx(t), w, to)
					s := within(t, spawns, "spawn")
					_ = w.Exit(to, grpcproc.ReasonKilled)
					if r := within(t, res, "answer"); !errors.Is(r.err, grpcproc.ErrNoProc) {
						t.Fatal(r.err)
					}
					waitWatchers(t, n, s.child, 0)
					noMore(t, got)
				})
			})
		})
	}
}

// A callee that places no monitor answers with none: the caller's monitor is
// Down at once, with noproc, as a monitor of nothing is.
func TestCallMonitorOfNothing(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		c := grpcproctest.New(t, "a", "b")
		w, got := watcher(t, c.Node("a"))
		to, _ := answering(t, c.Node("b"))
		_, ref, err := to.CallMonitor[*testpb.Pong](ctx(t), w, &testpb.Ping{})
		if err != nil {
			t.Fatal(err)
		}
		if m := recv(t, got); m.Down == nil || m.Down.Ref != ref || m.Down.PID != (grpcproc.PID{Node: "b"}) || m.Down.Reason != grpcproc.ReasonNoProc {
			t.Fatalf("%+v", m.Down)
		}
	})
}

// Only a process can watch: from the node, or from a process that has
// exited, nothing is called.
func TestCallMonitorNeedsAProcess(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		a := grpcproctest.New(t, "a").Node("a")
		to, got := answering(t, a)
		if _, _, err := to.CallMonitor[*testpb.Pong](ctx(t), a, &testpb.Ping{}); err == nil {
			t.Fatal("a node monitors")
		}
		if _, err := to.CallLink[*testpb.Pong](ctx(t), a, &testpb.Ping{}); err == nil {
			t.Fatal("a node links")
		}
		w, _ := watcher(t, a)
		_ = a.Exit(ctx(t), w.PID(), grpcproc.ReasonKilled)
		eventually(t, "the process to exit", func() bool { _, ok := a.Process(w.PID()); return !ok })
		if _, _, err := to.CallMonitor[*testpb.Pong](ctx(t), w, &testpb.Ping{}); !errors.Is(err, grpcproc.ErrNoProc) {
			t.Fatal(err)
		}
		noMore(t, got)
	})
}

// A process that exits while its call waits, made from another goroutine,
// monitors nothing once the answer comes. The monitor the callee placed stays
// until the child exits, and its Down goes nowhere.
func TestCallMonitorOfAnExitedCaller(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		c := grpcproctest.New(t, "a", "b")
		w, _ := watcher(t, c.Node("a"))
		to, spawns := spawner(t, c.Node("b"), echo)
		res := callMonitor(context.WithoutCancel(ctx(t)), w, to)
		s := within(t, spawns, "spawn")
		_ = c.Node("a").Exit(ctx(t), w.PID(), grpcproc.ReasonKilled)
		eventually(t, "the caller to exit", func() bool { _, ok := c.Node("a").Process(w.PID()); return !ok })
		_ = s.m.Reply(&testpb.Pong{}, nil)
		if r := within(t, res, "answer"); r.err != nil {
			t.Fatal(r.err)
		}
		_ = grpcproc.AddrOf[*testpb.Ping](s.child).Send(ctx(t), c.Node("b"), &testpb.Ping{N: 0})
		eventually(t, "the child to exit", func() bool { _, ok := c.Node("b").Process(s.child); return !ok })
		if info := c.Node("a").Info(); info.DeadLetters != 0 {
			t.Fatalf("%d dead letters", info.DeadLetters)
		}
	})
}

// When the link to the callee's node breaks before the answer, the call
// fails and no Down comes of it; once the answer has come, the Down does,
// with noconnection.
func TestCallMonitorAcrossABrokenLink(t *testing.T) {
	t.Run("before the answer", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			c := grpcproctest.New(t, "a", "b")
			w, got := watcher(t, c.Node("a"))
			to, spawns := spawner(t, c.Node("b"), echo)
			res := callMonitor(ctx(t), w, to)
			within(t, spawns, "spawn")
			c.Node("a").Disconnect("b")
			if r := within(t, res, "answer"); !errors.Is(r.err, grpcproc.ErrNoConnection) {
				t.Fatal(r.err)
			}
			noMore(t, got)
		})
	})
	t.Run("after the answer", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			c := grpcproctest.New(t, "a", "b")
			w, got := watcher(t, c.Node("a"))
			to, spawns := spawner(t, c.Node("b"), echo)
			res := callMonitor(ctx(t), w, to)
			s := within(t, spawns, "spawn")
			_ = s.m.Reply(&testpb.Pong{}, nil)
			r := within(t, res, "answer")
			c.Node("a").Disconnect("b")
			if m := recv(t, got); m.Down == nil || m.Down.Ref != r.ref || m.Down.PID != s.child || m.Down.Reason != grpcproc.ReasonNoConnection {
				t.Fatalf("%+v", m.Down)
			}
		})
	})
}

// CallLink links the caller to the process the callee spawns for it, from
// before that process runs: its exit ends the caller, or reaches it as an
// Exited, however soon it comes.
func TestCallLink(t *testing.T) {
	for _, node := range []string{"a", "b"} {
		t.Run(node, func(t *testing.T) {
			t.Run("ends the caller", func(t *testing.T) {
				synctest.Test(t, func(t *testing.T) {
					c := grpcproctest.New(t, "a", "b")
					w, _, reason := observed(t, c.Node("a"))
					to, spawns := spawner(t, c.Node(node), echo)
					res := callLink(ctx(t), w, to)
					s := within(t, spawns, "spawn")
					_ = s.m.Reply(&testpb.Pong{}, nil)
					if r := within(t, res, "answer"); r.err != nil {
						t.Fatal(r.err)
					}
					_ = c.Node(node).Exit(ctx(t), s.child, "done")
					if got := reason(); got != "done" {
						t.Fatal(got)
					}
				})
			})
			t.Run("exits before the answer", func(t *testing.T) {
				synctest.Test(t, func(t *testing.T) {
					c := grpcproctest.New(t, "a", "b")
					w, got := watcher(t, c.Node("a"))
					w.SetTrapExit(true)
					to, spawns := spawner(t, c.Node(node), early)
					res := callLink(ctx(t), w, to)
					s := within(t, spawns, "spawn")
					eventually(t, "the child to exit", func() bool { _, ok := c.Node(node).Process(s.child); return !ok })
					synctest.Wait()
					_ = s.m.Reply(&testpb.Pong{}, nil)
					if r := within(t, res, "answer"); r.err != nil {
						t.Fatal(r.err)
					}
					if m := recv(t, got); m.Exited == nil || m.Exited.PID != s.child || m.Exited.Reason != "early" {
						t.Fatalf("%+v", m.Exited)
					}
				})
			})
		})
	}
	// A link to a process that was linked to already is the same link: its
	// exit arrives once.
	t.Run("once", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			a := grpcproctest.New(t, "a").Node("a")
			w, got := watcher(t, a)
			w.SetTrapExit(true)
			target := spawnEcho(t, a)
			w.Link(target)
			to, calls := held(t, a)
			res := callLink(ctx(t), w, to)
			m := within(t, calls, "call")
			if err := m.Watch(target.PID()); err != nil {
				t.Fatal(err)
			}
			waitWatchers(t, a, target.PID(), 2)
			_ = m.Reply(&testpb.Pong{}, nil)
			if r := within(t, res, "answer"); r.err != nil {
				t.Fatal(r.err)
			}
			waitWatchers(t, a, target.PID(), 1)
			_ = target.Send(ctx(t), a, &testpb.Ping{N: 0})
			if m := recv(t, got); m.Exited == nil || m.Exited.PID != target.PID() {
				t.Fatalf("%+v", m)
			}
			noMore(t, got)
		})
	})
	// A callee that places no link answers with none: the caller is linked
	// to nothing, which ends it with noproc.
	t.Run("to nothing", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			c := grpcproctest.New(t, "a", "b")
			w, _, reason := observed(t, c.Node("a"))
			to, _ := answering(t, c.Node("b"))
			if _, err := to.CallLink[*testpb.Pong](ctx(t), w, &testpb.Ping{}); err != nil {
				t.Fatal(err)
			}
			if got := reason(); got != grpcproc.ReasonNoProc {
				t.Fatal(got)
			}
		})
	})
}

// held hands over the calls it takes, unanswered.
func held(t *testing.T, n *grpcproc.Node) (grpcproc.Addr[*testpb.Ping], <-chan grpcproc.Msg[*testpb.Ping]) {
	t.Helper()
	ch := make(chan grpcproc.Msg[*testpb.Ping], 8)
	addr, err := n.Spawn(func(p *grpcproc.Process[*testpb.Ping]) error {
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
	return addr, ch
}

// Msg.Watch places the caller's monitor on a process that runs, the callee
// itself included, and refuses what it cannot place.
func TestMsgWatch(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		c := grpcproctest.New(t, "a", "b")
		a, b := c.Node("a"), c.Node("b")
		w, got := watcher(t, a)
		to, calls := held(t, b)
		target := spawnEcho(t, b)
		other := spawnEcho(t, a)

		res := callMonitor(ctx(t), w, to)
		m := within(t, calls, "call")
		for _, pid := range []grpcproc.PID{
			{Node: "b", Incarnation: b.ID().Incarnation, ID: 424242}, // no such process
			other.PID(), // of another node
		} {
			if err := m.Watch(pid); !errors.Is(err, grpcproc.ErrNoProc) {
				t.Fatalf("%v: %v", pid, err)
			}
		}
		if err := m.Watch(target.PID()); err != nil {
			t.Fatal(err)
		}
		if err := m.Watch(target.PID()); err == nil {
			t.Fatal("a watch placed twice")
		}
		_ = m.Reply(&testpb.Pong{}, nil)
		ref := within(t, res, "answer").ref
		if err := m.Watch(target.PID()); err == nil {
			t.Fatal("a watch placed for a call answered already")
		}
		_ = target.Send(ctx(t), a, &testpb.Ping{N: 0})
		if m := recv(t, got); m.Down == nil || m.Down.Ref != ref || m.Down.PID != target.PID() || m.Down.Reason != grpcproc.ReasonNormal {
			t.Fatalf("%+v", m.Down)
		}

		// The callee itself.
		res = callMonitor(ctx(t), w, to)
		m = within(t, calls, "call")
		if err := m.Watch(to.PID()); err != nil {
			t.Fatal(err)
		}
		_ = m.Reply(&testpb.Pong{}, nil)
		ref = within(t, res, "answer").ref
		_ = b.Exit(ctx(t), to, "gone")
		if m := recv(t, got); m.Down == nil || m.Down.Ref != ref || m.Down.PID != to.PID() || m.Down.Reason != "gone" {
			t.Fatalf("%+v", m.Down)
		}
	})
}

// Placing a watch is for a call whose caller asked for one: a plain message
// is refused, and a call that asked for none places nothing.
func TestWatchOfWhatAskedNone(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		a := grpcproctest.New(t, "a").Node("a")
		to, calls := held(t, a)
		target := spawnEcho(t, a)

		_ = to.Send(ctx(t), a, &testpb.Ping{})
		m := within(t, calls, "message")
		if err := m.Watch(target.PID()); !errors.Is(err, grpcproc.ErrNotCall) {
			t.Fatal(err)
		}
		if _, err := a.Spawn(echo, grpcproc.WatchedBy(m)); !errors.Is(err, grpcproc.ErrNotCall) {
			t.Fatal(err)
		}

		go func() { _, _ = to.Call[*testpb.Pong](ctx(t), a, &testpb.Ping{}) }()
		m = within(t, calls, "call")
		if err := m.Watch(target.PID()); err != nil {
			t.Fatal(err)
		}
		child, err := a.Spawn(echo, grpcproc.WatchedBy(m))
		if err != nil {
			t.Fatal(err)
		}
		for _, pid := range []grpcproc.PID{target.PID(), child.PID()} {
			if info, _ := a.Process(pid); info.Watchers != 0 {
				t.Fatalf("%v has %d watchers", pid, info.Watchers)
			}
		}
		_ = m.Reply(&testpb.Pong{}, nil)
	})
}

// WatchedBy places a watch once, for a call not answered yet, held by a
// process of the spawning node: a spawn that cannot place it fails, and one
// that fails otherwise places none.
func TestWatchedByRefuses(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		c := grpcproctest.New(t, "a", "b")
		a, b := c.Node("a"), c.Node("b")
		w, got := watcher(t, a)
		to, calls := held(t, a)
		spawnEcho(t, a, grpcproc.WithName("taken"))

		res := callMonitor(ctx(t), w, to)
		m := within(t, calls, "call")
		if _, err := b.Spawn(echo, grpcproc.WatchedBy(m)); err == nil {
			t.Fatal("a watch placed on another node than the call's")
		}
		if _, err := a.Spawn(echo, grpcproc.WatchedBy(m), grpcproc.WithName("taken")); !errors.Is(err, grpcproc.ErrNameTaken) {
			t.Fatal(err)
		}
		// Node.Spawn: the call is held by a process that is not the parent.
		child, err := a.Spawn(echo, grpcproc.WatchedBy(m))
		if err != nil {
			t.Fatal(err)
		}
		if _, err := a.Spawn(echo, grpcproc.WatchedBy(m)); err == nil {
			t.Fatal("a watch placed twice")
		}
		_ = m.Reply(&testpb.Pong{}, nil)
		ref := within(t, res, "answer").ref
		if _, err := a.Spawn(echo, grpcproc.WatchedBy(m)); err == nil {
			t.Fatal("a watch placed for a call answered already")
		}
		_ = child.Send(ctx(t), a, &testpb.Ping{N: 0})
		if m := recv(t, got); m.Down == nil || m.Down.Ref != ref || m.Down.PID != child.PID() {
			t.Fatalf("%+v", m.Down)
		}
	})
}
