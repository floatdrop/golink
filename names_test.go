package grpcproc_test

import (
	"context"
	"errors"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"google.golang.org/protobuf/proto"

	"github.com/floatdrop/grpcproc"
	"github.com/floatdrop/grpcproc/grpcproctest"
	"github.com/floatdrop/grpcproc/internal/testpb"
)

type claimed struct {
	c   *grpcproc.Claim
	err error
}

// holding spawns on n a process that claims name with opts and then answers
// as echo does. Its claim, or the claim's error, comes on the channel.
func holding(t *testing.T, n *grpcproc.Node, name string, opts ...grpcproc.ClaimOption) (grpcproc.Addr[*testpb.Ping], <-chan claimed) {
	t.Helper()
	ch := make(chan claimed, 1)
	addr, err := n.Spawn(func(p *grpcproc.Process[*testpb.Ping]) error {
		c, err := p.Claim(p.Context(), name, opts...)
		ch <- claimed{c, err}
		if err != nil {
			return err
		}
		return echo(p)
	})
	if err != nil {
		t.Fatal(err)
	}
	return addr, ch
}

// claim waits for the claim of a process holding started.
func claim(t *testing.T, ch <-chan claimed) *grpcproc.Claim {
	t.Helper()
	got := within(t, ch, "claim")
	if got.err != nil {
		t.Fatalf("claim: %v", got.err)
	}
	return got.c
}

// A Global reaches whichever process holds the name, wherever it runs, for
// every kind of request; one no one holds is a process that does not exist.
func TestGlobal(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		c := grpcproctest.New(t, "a", "b")
		a, b := c.Node("a"), c.Node("b")
		ledger, ch := holding(t, b, "ledger")
		cl := claim(t, ch)
		if cl.Name() != "ledger" || !cl.Held() || cl.Revision() == 0 {
			t.Fatalf("claim %s held=%v revision=%d", cl.Name(), cl.Held(), cl.Revision())
		}
		g := grpcproc.AddrOf[*testpb.Ping](grpcproc.Global{Name: "ledger"})
		if g.String() != "{ledger@global}" || g.Name() != "ledger" || (grpcproc.Global{Name: "x"}).String() != "{x@global}" {
			t.Fatalf("%v %q", g, g.Name())
		}
		if r, err := g.Call[*testpb.Pong](ctx(t), a, &testpb.Ping{N: 1}); err != nil || r.N != 2 {
			t.Fatalf("call: %v %v", r, err)
		}
		dead := a.Info().DeadLetters
		if err := a.SendTo(ctx(t), grpcproc.Global{Name: "nobody"}, &testpb.Ping{}); err != nil {
			t.Fatal(err) // no one holds it: a dead letter
		}
		if a.Info().DeadLetters != dead+1 {
			t.Fatal("no dead letter")
		}
		if info, _ := b.Process(ledger.PID()); len(info.Globals) != 1 || info.Globals[0] != "ledger" {
			t.Fatalf("globals %v", info.Globals)
		}

		// Monitors and links of a Global follow the process they found, and
		// their Down names the global.
		w, downs := watcher(t, a)
		w.Monitor(grpcproc.Global{Name: "ledger"})
		ref := w.Monitor(grpcproc.Global{Name: "nobody"})
		if m := recv(t, downs); m.Down == nil || m.Down.Ref != ref || m.Down.Name != "nobody" || m.Down.Reason != grpcproc.ReasonNoProc {
			t.Fatalf("down of nobody: %+v", m.Down)
		}
		w.SetTrapExit(true)
		w.Link(grpcproc.Global{Name: "ledger"})
		w.Unlink(grpcproc.Global{Name: "ledger"})
		w.Link(grpcproc.Global{Name: "ledger"})
		waitWatchers(t, b, ledger.PID(), 2)
		// echo places no watch for a CallMonitor: its monitor is Down at once.
		r, ref2, err := g.CallMonitor[*testpb.Pong](ctx(t), w, &testpb.Ping{N: 1})
		if err != nil || r.N != 2 {
			t.Fatalf("call monitor: %v %v", r, err)
		}
		if m := recv(t, downs); m.Down == nil || m.Down.Ref != ref2 || m.Down.Reason != grpcproc.ReasonNoProc {
			t.Fatalf("call monitor's down: %+v", m.Down)
		}
		if err := a.Exit(ctx(t), grpcproc.Global{Name: "ledger"}, "done"); err != nil {
			t.Fatal(err)
		}
		var down, exited int
		for range 2 { // the monitor's Down, the link's Exited
			switch m := recv(t, downs); {
			case m.Exited != nil && m.Exited.Reason == "done" && m.Exited.Name == "ledger":
				exited++
			case m.Down != nil && m.Down.Reason == "done" && m.Down.PID == ledger.PID() && m.Down.Name == "ledger":
				down++
			default:
				t.Fatalf("after exit: %+v %+v", m.Down, m.Exited)
			}
		}
		if down != 1 || exited != 1 {
			t.Fatalf("down %d, exited %d", down, exited)
		}

		// Its name went with it.
		eventually(t, "ledger to be free", func() bool { _, ok := b.Names().Lookup("ledger"); return !ok })
		if _, err := g.Call[*testpb.Pong](ctx(t), a, &testpb.Ping{N: 1}); !errors.Is(err, grpcproc.ErrNoProc) {
			t.Fatalf("call after exit: %v", err)
		}
	})
}

// A claim of a held name fails with the holder, and the error matches
// ErrTaken on another node too, where only its text arrives.
func TestClaimTaken(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		c := grpcproctest.New(t, "a", "b")
		a, b := c.Node("a"), c.Node("b")
		first, ch := holding(t, a, "ledger")
		claim(t, ch)
		_, ch = holding(t, b, "ledger")
		got := within(t, ch, "second claim")
		var taken *grpcproc.TakenError
		if !errors.As(got.err, &taken) || taken.Holder != first.PID() || taken.Name != "ledger" || !errors.Is(got.err, grpcproc.ErrTaken) {
			t.Fatalf("second claim: %v", got.err)
		}
		// A callee that answers with it: the caller, on another node, matches it.
		asker, err := b.Spawn(func(p *grpcproc.Process[*testpb.Ping]) error {
			m, err := p.Receive()
			if err != nil {
				return err
			}
			_, err = p.Claim(p.Context(), "ledger")
			return m.Reply(nil, err)
		})
		if err != nil {
			t.Fatal(err)
		}
		if _, err := asker.Call[*testpb.Pong](ctx(t), a, &testpb.Ping{}); !errors.Is(err, grpcproc.ErrTaken) {
			t.Fatalf("across the wire: %v", err)
		}
		if pid, ok, err := b.Names().Resolve(ctx(t), "ledger"); err != nil || !ok || pid != first.PID() {
			t.Fatalf("resolve: %v %v %v", pid, ok, err)
		}
	})
}

// A claim that waits takes the name over when its holder exits: a standby.
func TestClaimWaits(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		c := grpcproctest.New(t, "a", "b")
		a, b := c.Node("a"), c.Node("b")
		first, ch := holding(t, a, "gateway")
		held := claim(t, ch)
		standby, ch := holding(t, b, "gateway", grpcproc.WaitForName())
		synctest.Wait()
		if pid, _ := a.Names().Lookup("gateway"); pid != first.PID() {
			t.Fatalf("holder %v", pid)
		}
		if err := first.Send(ctx(t), a, &testpb.Ping{N: 0}); err != nil { // ends it
			t.Fatal(err)
		}
		next := claim(t, ch)
		if next.Revision() <= held.Revision() {
			t.Fatalf("revision %d after %d", next.Revision(), held.Revision())
		}
		if pid, _ := a.Names().Lookup("gateway"); pid != standby.PID() {
			t.Fatalf("holder %v, want the standby", pid)
		}
		// A wait ends with the process that waits.
		waiting, ch := holding(t, a, "gateway", grpcproc.WaitForName())
		synctest.Wait()
		if err := a.Exit(ctx(t), waiting.PID(), "give up"); err != nil {
			t.Fatal(err)
		}
		var ee *grpcproc.ExitError
		if got := within(t, ch, "a wait that ended"); !errors.As(got.err, &ee) || ee.Reason != "give up" {
			t.Fatalf("wait: %v", got.err)
		}
	})
}

// Release gives the name up while the process goes on.
func TestClaimRelease(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		c := grpcproctest.New(t, "a")
		a := c.Node("a")
		holder, ch := holding(t, a, "ledger")
		cl := claim(t, ch)
		if err := cl.Release(ctx(t)); err != nil {
			t.Fatal(err)
		}
		if err := cl.Release(ctx(t)); err != nil { // again: nothing
			t.Fatal(err)
		}
		if _, ok := a.Names().Lookup("ledger"); ok || cl.Held() {
			t.Fatal("still held")
		}
		if info, _ := a.Process(holder.PID()); len(info.Globals) != 0 {
			t.Fatalf("globals %v", info.Globals)
		}
		if _, err := holder.Call[*testpb.Pong](ctx(t), a, &testpb.Ping{N: 1}); err != nil {
			t.Fatalf("the process after release: %v", err)
		}
	})
}

// A claim lost, its node cut off from the store, ends its holder before
// another can claim the name, unless it was made with KeepOnLoss: then it
// runs on, not held, until it holds the name again, or finds it taken.
func TestClaimLost(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		c := grpcproctest.New(t, "a", "b")
		a, b := c.Node("a"), c.Node("b")
		strict, ch := holding(t, b, "ledger")
		claim(t, ch)
		room, ch := holding(t, b, "room:1", grpcproc.KeepOnLoss())
		kept := claim(t, ch)
		other, ch := holding(t, b, "room:2", grpcproc.KeepOnLoss())
		claim(t, ch)
		w, downs := watcher(t, a)
		w.Monitor(strict)
		w.Monitor(other)
		waitWatchers(t, b, strict.PID(), 1)
		waitWatchers(t, b, other.PID(), 1)

		c.CutNames("b")
		if m := recv(t, downs); m.Down == nil || m.Down.Reason != grpcproc.ReasonNameLost {
			t.Fatalf("strict holder: %+v", m.Down)
		}
		if kept.Held() {
			t.Fatal("held while cut off")
		}
		if _, err := room.Call[*testpb.Pong](ctx(t), a, &testpb.Ping{N: 1}); err != nil {
			t.Fatalf("the room runs on: %v", err)
		}
		if _, _, err := b.Names().Resolve(ctx(t), "room:1"); !errors.Is(err, grpcproctest.ErrNamesUnreachable) {
			t.Fatalf("resolve while cut off: %v", err)
		}
		// Meanwhile another node starts room:2 again.
		_, ch = holding(t, a, "room:2")
		claim(t, ch)

		before := kept.Revision()
		c.RestoreNames("b")
		if !kept.Held() || kept.Revision() <= before {
			t.Fatalf("room:1 held=%v revision %d after %d", kept.Held(), kept.Revision(), before)
		}
		if m := recv(t, downs); m.Down == nil || m.Down.Reason != grpcproc.ReasonNameConflict {
			t.Fatalf("room:2 on b: %+v", m.Down)
		}
	})
}

// Without Config.Names, nothing can be claimed, and no global name is held.
func TestNoNames(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		c := grpcproctest.NewWith(t, []grpcproctest.Option{grpcproctest.WithConfig(func(_ string, cfg *grpcproc.Config) {
			cfg.Names = nil
		})}, "a")
		a := c.Node("a")
		_, ch := holding(t, a, "ledger")
		if got := within(t, ch, "claim"); !errors.Is(got.err, grpcproc.ErrNoNames) {
			t.Fatalf("claim: %v", got.err)
		}
		if _, err := grpcproc.AddrOf[*testpb.Ping](grpcproc.Global{Name: "ledger"}).Call[*testpb.Pong](ctx(t), a, &testpb.Ping{}); !errors.Is(err, grpcproc.ErrNoProc) {
			t.Fatalf("call: %v", err)
		}
		if a.Names() != nil {
			t.Fatal("Names")
		}
	})
}

// The in-memory store lists its names, and frees a killed node's.
func TestTestNames(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		c := grpcproctest.New(t, "a", "b")
		for _, name := range []string{"room:2", "room:1", "ledger"} {
			_, ch := holding(t, c.Node("b"), name)
			claim(t, ch)
		}
		list := c.Names().List("room:", 1)
		if len(list) != 1 || list[0].Name != "room:1" {
			t.Fatalf("list %v", list)
		}
		lister := c.Node("a").Names().(grpcproc.NameLister)
		if all := lister.List("", 0); len(all) != 3 {
			t.Fatalf("all %v", all)
		}
		c.Kill("b")
		if all := c.Names().List("", 0); len(all) != 0 {
			t.Fatalf("after kill %v", all)
		}
	})
}

// fakeNames is a Names store whose every step a test controls.
type fakeNames struct {
	watch   func(ctx context.Context) error
	claim   func(ctx context.Context) error
	notify  chan func(grpcproc.ClaimEvent)
	mu      sync.Mutex
	release func(ctx context.Context) error // guarded by mu: releases run in the background
}

func (f *fakeNames) setRelease(fn func(ctx context.Context) error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.release = fn
}

func (f *fakeNames) Watch(ctx context.Context) error { return f.watch(ctx) }
func (f *fakeNames) Lookup(string) (grpcproc.PID, bool) {
	return grpcproc.PID{}, false
}
func (f *fakeNames) Resolve(context.Context, string) (grpcproc.PID, bool, error) {
	return grpcproc.PID{}, false, nil
}
func (f *fakeNames) Claim(ctx context.Context, _ string, _ grpcproc.PID, _ grpcproc.ClaimOptions, notify func(grpcproc.ClaimEvent)) (grpcproc.NameClaim, error) {
	if err := f.claim(ctx); err != nil {
		return nil, err
	}
	f.notify <- notify
	return fakeClaim{f}, nil
}

type fakeClaim struct{ f *fakeNames }

func (fakeClaim) Revision() int64 { return 1 }
func (c fakeClaim) Release(ctx context.Context) error {
	c.f.mu.Lock()
	release := c.f.release
	c.f.mu.Unlock()
	return release(ctx)
}

func newFakeNames() *fakeNames {
	return &fakeNames{
		watch:   func(context.Context) error { return nil },
		claim:   func(context.Context) error { return nil },
		release: func(context.Context) error { return nil },
		notify:  make(chan func(grpcproc.ClaimEvent), 4),
	}
}

func fakeNode(t *testing.T, names grpcproc.Names) *grpcproc.Node {
	t.Helper()
	n, err := grpcproc.NewNode(grpcproc.Config{Name: "a", Resolver: grpcproc.StaticResolver{}, Names: names, DialTimeout: time.Second, Logger: slog.New(slog.DiscardHandler)})
	if err != nil {
		t.Fatal(err)
	}
	return n
}

// Start fails when the store cannot be watched, or not before ctx is done.
func TestNamesWatch(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		f := newFakeNames()
		f.watch = func(context.Context) error { return errors.New("no etcd") }
		if err := fakeNode(t, f).Start(t.Context()); err == nil || !strings.Contains(err.Error(), "names: no etcd") {
			t.Fatalf("start: %v", err)
		}
		f.watch = func(ctx context.Context) error { <-ctx.Done(); return nil }
		ctx, cancel := context.WithTimeout(t.Context(), time.Second)
		defer cancel()
		if err := fakeNode(t, f).Start(ctx); !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("start: %v", err)
		}
	})
}

// What the store says of a claim that is over changes nothing; a claim the
// store makes after its process exited is given back; a release that fails
// is logged, and one that hangs holds Stop up only until its ctx is done.
func TestClaimEdges(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		f := newFakeNames()
		n := fakeNode(t, f)
		if err := n.Start(t.Context()); err != nil {
			t.Fatal(err)
		}
		procs := make(chan *grpcproc.Process[proto.Message], 1)
		addr, err := n.Spawn(func(p *grpcproc.Process[proto.Message]) error {
			procs <- p
			c, err := p.Claim(p.Context(), "x")
			if err != nil {
				return err
			}
			if err := c.Release(p.Context()); err != nil {
				return err
			}
			_, err = p.Receive()
			return err
		})
		if err != nil {
			t.Fatal(err)
		}
		p := <-procs
		notify := <-f.notify
		synctest.Wait()
		notify(grpcproc.ClaimEvent{Kind: grpcproc.ClaimLost}) // released: nothing happens
		if _, ok := n.Process(addr.PID()); !ok {
			t.Fatal("the process ended")
		}
		_ = n.Exit(t.Context(), addr.PID(), "done")
		synctest.Wait()
		if _, err := p.Claim(context.Background(), "x"); !errors.Is(err, grpcproc.ErrNoProc) {
			t.Fatalf("claim after exit: %v", err)
		}
		<-f.notify

		// A process exits holding a name whose release fails, and another
		// whose release hangs.
		f.setRelease(func(context.Context) error { return errors.New("no etcd") })
		hold := func() {
			if _, err := n.Spawn(func(p *grpcproc.Process[proto.Message]) error {
				_, err := p.Claim(p.Context(), "y")
				return err
			}); err != nil {
				t.Fatal(err)
			}
			<-f.notify
		}
		hold()
		synctest.Wait()
		f.setRelease(func(ctx context.Context) error { <-ctx.Done(); return ctx.Err() })
		hold()
		ctx, cancel := context.WithTimeout(t.Context(), 100*time.Millisecond)
		defer cancel()
		if err := n.Stop(ctx); !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("stop: %v", err)
		}
		time.Sleep(time.Second) // the hung release gives up at DialTimeout
		synctest.Wait()
	})
}
