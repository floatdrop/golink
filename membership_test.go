package grpcproc_test

import (
	"context"
	"errors"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/floatdrop/grpcproc"
	"github.com/floatdrop/grpcproc/grpcproctest"
	"github.com/floatdrop/grpcproc/internal/testpb"
)

type fakeMembership struct {
	events chan grpcproc.MemberEvent
	err    error
}

func (f *fakeMembership) Watch(ctx context.Context) (<-chan grpcproc.MemberEvent, error) {
	if f.err != nil {
		return nil, f.err
	}
	out := make(chan grpcproc.MemberEvent)
	go func() {
		defer close(out)
		for {
			select {
			case ev := <-f.events:
				select {
				case out <- ev:
				case <-ctx.Done():
					return
				}
			case <-ctx.Done():
				return
			}
		}
	}()
	return out, nil
}

type fakeRegistrar struct {
	mu          sync.Mutex
	members     []grpcproc.Member
	withdrawn   int
	err         error
	withdrawErr error
	onWithdraw  func()
}

func (f *fakeRegistrar) Register(_ context.Context, self grpcproc.Member) (func(context.Context) error, error) {
	if f.err != nil {
		return nil, f.err
	}
	f.mu.Lock()
	f.members = append(f.members, self)
	f.mu.Unlock()
	return func(context.Context) error {
		if f.onWithdraw != nil {
			f.onWithdraw()
		}
		f.mu.Lock()
		f.withdrawn++
		f.mu.Unlock()
		return f.withdrawErr
	}, nil
}

func TestMembershipDropsLinks(t *testing.T) {
	m := &fakeMembership{events: make(chan grpcproc.MemberEvent)}
	c := grpcproctest.NewWith(t, []grpcproctest.Option{grpcproctest.WithConfig(func(name string, cfg *grpcproc.Config) {
		if name == "a" {
			cfg.Membership = m
		}
	})}, "a", "b", "c")
	a, b := c.Node("a"), c.Node("b")
	w, ch := watcher(t, a)
	silent, _ := b.Spawn[*testpb.Ping](func(p *grpcproc.Process[*testpb.Ping]) error {
		for {
			if _, err := p.Receive(); err != nil {
				return err
			}
		}
	})
	w.Monitor(silent)
	if _, err := w.Call[*testpb.Pong](ctx(t), mustEcho(t, b), &testpb.Ping{N: 1}); err != nil {
		t.Fatal(err)
	}
	inc := b.ID().Incarnation

	// Events that settle nothing: about this node, about a node a has no
	// link to, about another incarnation of b going away.
	m.events <- grpcproc.MemberEvent{Member: grpcproc.Member{Name: "a", Incarnation: a.ID().Incarnation}}
	m.events <- grpcproc.MemberEvent{Member: grpcproc.Member{Name: "zzz", Incarnation: 9}}
	m.events <- grpcproc.MemberEvent{Member: grpcproc.Member{Name: "b", Incarnation: inc + 100}}
	m.events <- grpcproc.MemberEvent{Member: grpcproc.Member{Name: "b", Incarnation: inc}, Up: true}
	time.Sleep(20 * time.Millisecond)
	if len(a.Peers()) != 1 {
		t.Fatalf("links dropped: %v", a.Peers())
	}

	// b leaves the cluster: its links go, and with them the monitor.
	callErr := make(chan error, 1)
	go func() { _, err := a.CallTo[*testpb.Pong](context.Background(), silent, &testpb.Ping{}); callErr <- err }()
	time.Sleep(20 * time.Millisecond)
	m.events <- grpcproc.MemberEvent{Member: grpcproc.Member{Name: "b", Incarnation: inc}}
	if d := recv(t, ch).Down; d == nil || d.Reason != grpcproc.ReasonNoConnection {
		t.Fatalf("got %+v", d)
	}
	if err := <-callErr; !errors.Is(err, grpcproc.ErrNoConnection) || !strings.Contains(err.Error(), "left the cluster") {
		t.Fatalf("pending call: %v", err)
	}

	// b is seen again as a new incarnation while a still links to the old one.
	if _, err := a.Call[*testpb.Pong](ctx(t), mustEcho(t, b), &testpb.Ping{N: 1}); err != nil {
		t.Fatal(err)
	}
	m.events <- grpcproc.MemberEvent{Member: grpcproc.Member{Name: "b", Incarnation: inc + 1}, Up: true}
	waitNoPeer(t, a, "b")

	// A peer a only hears from (an inbound link, no outbound) leaves too.
	cn := c.Node("c")
	sink, got := collector(t, a)
	_ = cn.SendTo(sink, &testpb.Ping{})
	recv(t, got)
	// Incarnation 0: whichever it was.
	m.events <- grpcproc.MemberEvent{Member: grpcproc.Member{Name: "c"}}
	waitNoPeer(t, a, "c")
}

func waitNoPeer(t *testing.T, n *grpcproc.Node, peer string) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for slices.Contains(n.Peers(), peer) {
		if time.Now().After(deadline) {
			t.Fatalf("links to %s kept: %+v", peer, n.Info().Links)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func mustEcho(t *testing.T, n *grpcproc.Node) grpcproc.Addr[*testpb.Ping] {
	t.Helper()
	e, err := n.Spawn(echo)
	if err != nil {
		t.Fatal(err)
	}
	return e
}

func TestRegistrarLifecycle(t *testing.T) {
	r := &fakeRegistrar{}
	c := grpcproctest.NewWith(t, []grpcproctest.Option{grpcproctest.WithConfig(func(name string, cfg *grpcproc.Config) {
		if name == "a" {
			cfg.Registrar = r
		}
	})}, "a", "b")
	a, b := c.Node("a"), c.Node("b")
	if err := a.Start(t.Context()); err != nil { // a second Start does nothing
		t.Fatal(err)
	}
	if len(r.members) != 1 || r.members[0] != (grpcproc.Member{Name: "a", Incarnation: a.ID().Incarnation, Addr: "a"}) {
		t.Fatalf("registered %+v", r.members)
	}
	// Withdrawing comes last: by then a's processes are gone and b has seen
	// their Down{shutdown}.
	e, _ := a.Spawn(echo)
	w, ch := watcher(t, b)
	w.Monitor(e)
	if _, err := w.Call[*testpb.Pong](ctx(t), e, &testpb.Ping{N: 1}); err != nil {
		t.Fatal(err)
	}
	var procsAtWithdraw int
	r.onWithdraw = func() { procsAtWithdraw = len(a.Processes()) }
	c.Stop("a")
	if d := recv(t, ch).Down; d == nil || d.Reason != grpcproc.ReasonShutdown {
		t.Fatalf("got %+v", d)
	}
	if r.withdrawn != 1 || procsAtWithdraw != 0 {
		t.Fatalf("withdrawn %d times, with %d processes left", r.withdrawn, procsAtWithdraw)
	}
}

func TestStartAndStopErrors(t *testing.T) {
	boom := errors.New("etcd down")
	n, _ := grpcproc.NewNode(grpcproc.Config{Name: "a", Resolver: grpcproc.StaticResolver{}, Membership: &fakeMembership{err: boom}})
	if err := n.Start(t.Context()); !errors.Is(err, boom) || !strings.Contains(err.Error(), "membership") {
		t.Fatalf("got %v", err)
	}
	n, _ = grpcproc.NewNode(grpcproc.Config{Name: "a", Resolver: grpcproc.StaticResolver{}, Registrar: &fakeRegistrar{err: boom}})
	if err := n.Start(t.Context()); !errors.Is(err, boom) || !strings.Contains(err.Error(), "register") {
		t.Fatalf("got %v", err)
	}
	r := &fakeRegistrar{withdrawErr: boom}
	n, _ = grpcproc.NewNode(grpcproc.Config{Name: "a", Resolver: grpcproc.StaticResolver{}, Registrar: r})
	if err := n.Start(t.Context()); err != nil {
		t.Fatal(err)
	}
	if err := n.Stop(t.Context()); !errors.Is(err, boom) || !strings.Contains(err.Error(), "withdraw") {
		t.Fatalf("got %v", err)
	}
}
