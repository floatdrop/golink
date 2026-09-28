package grpcproc_test

import (
	"errors"
	"runtime"
	"testing"
	"testing/synctest"
	"time"

	"google.golang.org/protobuf/proto"

	"github.com/floatdrop/grpcproc"
	"github.com/floatdrop/grpcproc/grpcproctest"
	"github.com/floatdrop/grpcproc/internal/testpb"
)

func TestMonitorReasons(t *testing.T) {
	type watching = *grpcproc.Process[proto.Message]
	setup := func(t *testing.T) (a, b *grpcproc.Node, w watching, ch <-chan grpcproc.Msg[proto.Message]) {
		c := grpcproctest.New(t, "a", "b")
		w, ch = watcher(t, c.Node("a"))
		return c.Node("a"), c.Node("b"), w, ch
	}
	cases := []struct {
		name   string
		kill   func(w watching, e grpcproc.Addr[*testpb.Ping])
		reason string
	}{
		{"normal", func(w watching, e grpcproc.Addr[*testpb.Ping]) { _ = e.Send(w.Context(), w, &testpb.Ping{N: 0}) }, grpcproc.ReasonNormal},
		{"error", func(w watching, e grpcproc.Addr[*testpb.Ping]) { _ = e.Send(w.Context(), w, &testpb.Ping{N: -100}) }, "boom"},
		{"panic", func(w watching, e grpcproc.Addr[*testpb.Ping]) { _ = e.Send(w.Context(), w, &testpb.Ping{N: -200}) }, "panic: kaboom"},
		{"exit", func(w watching, e grpcproc.Addr[*testpb.Ping]) { _ = w.Exit(e, grpcproc.ReasonKilled) }, grpcproc.ReasonKilled},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				_, b, w, ch := setup(t)
				e, _ := b.Spawn(echo)
				ref := w.Monitor(e)
				// A call round-trips on the same link, so the monitor is installed first.
				if _, err := e.Call[*testpb.Pong](ctx(t), w, &testpb.Ping{N: 1}); err != nil {
					t.Fatal(err)
				}
				tc.kill(w, e)
				m := recv(t, ch)
				if m.Down == nil || m.Down.Ref != ref || m.Down.PID != e.PID() || m.Down.Reason != tc.reason {
					t.Fatalf("got %+v, want reason %q", m.Down, tc.reason)
				}
			})
		})
	}
	t.Run("noproc", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			_, b, w, ch := setup(t)
			ref := w.Monitor(grpcproc.PID{Node: "b", Incarnation: b.ID().Incarnation, ID: 424242})
			m := recv(t, ch)
			if m.Down == nil || m.Down.Ref != ref || m.Down.Reason != grpcproc.ReasonNoProc {
				t.Fatalf("got %+v", m.Down)
			}
		})
	})
	t.Run("stale incarnation", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			_, b, w, ch := setup(t)
			e, _ := b.Spawn(echo)
			old := e.PID()
			old.Incarnation--
			w.Monitor(old)
			if m := recv(t, ch); m.Down == nil || m.Down.Reason != grpcproc.ReasonNoProc {
				t.Fatalf("got %+v", m.Down)
			}
		})
	})
	t.Run("by name", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			_, b, w, ch := setup(t)
			e, _ := b.Spawn(echo, grpcproc.WithName("named"))
			ref := w.Monitor(grpcproc.Name{Node: "b", Name: "named"})
			if _, err := e.Call[*testpb.Pong](ctx(t), w, &testpb.Ping{N: 1}); err != nil {
				t.Fatal(err)
			}
			_ = e.Send(w.Context(), w, &testpb.Ping{N: 0})
			if m := recv(t, ch); m.Down == nil || m.Down.Ref != ref || m.Down.PID != e.PID() || m.Down.Name != "named" {
				t.Fatalf("got %+v", m.Down)
			}
		})
	})
	t.Run("by name, node lost", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			a, b, w, ch := setup(t)
			e, _ := b.Spawn(echo, grpcproc.WithName("lost"))
			ref := w.Monitor(grpcproc.Name{Node: "b", Name: "lost"})
			if _, err := e.Call[*testpb.Pong](ctx(t), w, &testpb.Ping{N: 1}); err != nil {
				t.Fatal(err)
			}
			a.Disconnect("b")
			// Which process held the name is unknown here: only the name is.
			if m := recv(t, ch); m.Down == nil || m.Down.Ref != ref || m.Down.Name != "lost" || m.Down.PID != (grpcproc.PID{Node: "b"}) || m.Down.Reason != grpcproc.ReasonNoConnection {
				t.Fatalf("got %+v", m.Down)
			}
		})
	})
}

// Demonitor removes the watcher on the target's side, however the monitor
// was placed and wherever the target runs; and no Down follows.
func TestDemonitor(t *testing.T) {
	for _, node := range []string{"a", "b"} {
		for _, kind := range []string{"pid", "name", "named", "addr-of-pid", "addr-of-name"} {
			t.Run(node+"/"+kind, func(t *testing.T) {
				synctest.Test(t, func(t *testing.T) {
					c := grpcproctest.New(t, "a", "b")
					a := c.Node("a")
					target := c.Node(node)
					name := "e-" + node + "-" + kind
					e, err := target.Spawn(echo, grpcproc.WithName(name))
					if err != nil {
						t.Fatal(err)
					}
					to := map[string]grpcproc.Target{
						"pid":          e.PID(),
						"name":         grpcproc.Name{Node: node, Name: name},
						"named":        grpcproc.Named[*testpb.Ping](node, name),
						"addr-of-pid":  grpcproc.AddrOf[*testpb.Ping](e.PID()),
						"addr-of-name": grpcproc.AddrOf[*testpb.Ping](grpcproc.Name{Node: node, Name: name}),
					}[kind]
					w, ch := watcher(t, a)
					ref := w.Monitor(to)
					waitWatchers(t, target, e.PID(), 1)
					w.Demonitor(ref)
					waitWatchers(t, target, e.PID(), 0)
					_ = e.Send(w.Context(), w, &testpb.Ping{N: 0}) // echo exits on 0
					select {
					case m := <-ch:
						t.Fatalf("unexpected %+v", m)
					case <-time.After(150 * time.Millisecond):
					}
				})
			})
		}
	}
}

// A watcher that exits holding monitors removes their watchers on the
// target's side, by PID and by name.
func TestExitDemonitors(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		c := grpcproctest.New(t, "a", "b")
		a, b := c.Node("a"), c.Node("b")
		e, _ := b.Spawn(echo, grpcproc.WithName("x"))
		w, _ := watcher(t, a)
		w.Monitor(e.PID())
		w.Monitor(grpcproc.Name{Node: "b", Name: "x"})
		waitWatchers(t, b, e.PID(), 2)
		_ = a.Exit(t.Context(), w.PID(), "bye")
		waitWatchers(t, b, e.PID(), 0)
	})
}

func TestSpawnMonitorSeesInstantExit(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		c := grpcproctest.New(t, "a")
		a := c.Node("a")
		w, ch := watcher(t, a)
		// The child is gone before SpawnMonitor even returns, and still the
		// Down carries its real reason, never noproc.
		for range 20 {
			child, ref, err := w.SpawnMonitor[*testpb.Ping](func(*grpcproc.Process[*testpb.Ping]) error {
				return errors.New("boom")
			})
			if err != nil {
				t.Fatal(err)
			}
			m := recv(t, ch)
			if m.Down == nil || m.Down.Ref != ref || m.Down.PID != child.PID() || m.Down.Reason != "boom" {
				t.Fatalf("got %+v", m.Down)
			}
		}
		if info, _ := a.Process(w.PID()); info.Monitors != 0 {
			t.Fatalf("monitors left: %d", info.Monitors)
		}
	})
}

func TestSpawnRecordsParent(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		c := grpcproctest.New(t, "a")
		a := c.Node("a")
		w, _ := watcher(t, a)
		orphan, err1 := a.Spawn(echo)
		child, err2 := w.Spawn(echo)
		watched, _, err3 := w.SpawnMonitor(echo)
		if err := errors.Join(err1, err2, err3); err != nil {
			t.Fatal(err)
		}
		// Only SpawnMonitor monitors: one watcher, on one child.
		for _, tc := range []struct {
			addr     grpcproc.Addr[*testpb.Ping]
			parent   grpcproc.PID
			watchers int
		}{{orphan, grpcproc.PID{}, 0}, {child, w.PID(), 0}, {watched, w.PID(), 1}} {
			info, ok := a.Process(tc.addr.PID())
			if !ok || info.Parent != tc.parent || info.Watchers != tc.watchers {
				t.Errorf("%v: %+v, want parent %v and %d watchers", tc.addr, info, tc.parent, tc.watchers)
			}
		}
		if info, _ := a.Process(w.PID()); info.Monitors != 1 {
			t.Fatalf("monitors: %d", info.Monitors)
		}
	})
}

func TestSpawnMonitorFailures(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		c := grpcproctest.New(t, "a")
		a := c.Node("a")
		w, downs := watcher(t, a)
		_, _ = a.Spawn(echo, grpcproc.WithName("taken"))
		if _, _, err := w.SpawnMonitor[*testpb.Ping](echo, grpcproc.WithName("taken")); !errors.Is(err, grpcproc.ErrNameTaken) {
			t.Fatalf("got %v", err)
		}
		if info, _ := a.Process(w.PID()); info.Monitors != 0 {
			t.Fatalf("a failed spawn left a monitor: %d", info.Monitors)
		}
		// A process that has exited spawns nothing.
		gone := make(chan *grpcproc.Process[proto.Message], 1)
		_, _, _ = w.SpawnMonitor[proto.Message](func(p *grpcproc.Process[proto.Message]) error { gone <- p; return nil })
		dead := <-gone
		if m := recv(t, downs); m.Down == nil {
			t.Fatalf("got %+v", m)
		}
		spawned := a.Info().Spawned
		if _, _, err := dead.SpawnMonitor[*testpb.Ping](echo); !errors.Is(err, grpcproc.ErrNoProc) {
			t.Fatalf("got %v", err)
		}
		if _, err := dead.Spawn[*testpb.Ping](echo); !errors.Is(err, grpcproc.ErrNoProc) {
			t.Fatalf("got %v", err)
		}
		if n := a.Info().Spawned; n != spawned {
			t.Fatalf("spawned %d, then %d", spawned, n)
		}
		c.Stop("a")
		if _, _, err := w.SpawnMonitor[*testpb.Ping](echo); err == nil {
			t.Fatal("spawn on a stopped node")
		}
	})
}

// A parent that exits while SpawnMonitor runs on another goroutine leaves
// no child watching it: each spawn either fails or is demonitored.
func TestSpawnMonitorRacesParentExit(t *testing.T) {
	// On real time, not in a synctest bubble: its contenders never wait,
	// so a bubble's clock would never move for the polls below.
	func() {
		c := grpcproctest.New(t, "a")
		a := c.Node("a")
		w, downs := watcher(t, a)
		// Contend Node.mu so a spawn stalls between its steps. Each contender
		// yields between lookups: four that spin on one CPU starve every other
		// goroutine for a time slice each, and the test took minutes to hours.
		stop := make(chan struct{})
		defer close(stop)
		for range 4 {
			go func() {
				for {
					select {
					case <-stop:
						return
					default:
						a.Whereis("x")
						runtime.Gosched()
					}
				}
			}()
		}
		for i := range 2000 {
			ready := make(chan *grpcproc.Process[proto.Message], 1)
			pa, _ := a.Spawn[proto.Message](func(p *grpcproc.Process[proto.Message]) error {
				ready <- p
				_, err := p.Receive()
				return err
			})
			p := <-ready
			w.Monitor(pa)
			kids := make(chan []grpcproc.PID, 1)
			// Spawns until the parent's exit refuses one, yielding after each so
			// that the exit lands among them even on one CPU, and 64 at most: a
			// spawner that ran a whole time slice made thousands a round, each
			// to be polled and killed, until the race detector ran out of memory.
			go func() {
				var ps []grpcproc.PID
				for len(ps) < 64 {
					k, _, err := p.SpawnMonitor[*testpb.Ping](echo)
					if err != nil {
						break
					}
					ps = append(ps, k.PID())
					runtime.Gosched()
				}
				kids <- ps
			}()
			_ = a.Exit(t.Context(), pa, grpcproc.ReasonKilled)
			ps := <-kids
			recv(t, downs)
			deadline := time.Now().Add(2 * time.Second)
			for _, k := range ps {
				for {
					info, _ := a.Process(k)
					if info.Watchers == 0 {
						break
					}
					if time.Now().After(deadline) {
						t.Fatalf("iteration %d: child %v of %d still watched by its dead parent", i, k, len(ps))
					}
					time.Sleep(time.Millisecond)
				}
				_ = a.Exit(t.Context(), k, grpcproc.ReasonKilled)
			}
		}
	}()
}
