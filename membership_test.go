package grpcproc_test

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"sync"
	"testing"
	"testing/synctest"
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

// flush returns once a has handled the events sent to m before it: the
// second of two events is taken only once the one before them is handled.
func flush(m *fakeMembership) {
	for range 2 {
		m.events <- grpcproc.MemberEvent{Member: grpcproc.Member{Name: "zzz"}}
	}
}

type fakeRegistrar struct {
	mu          sync.Mutex
	members     []grpcproc.Member
	withdrawn   int
	err         error
	withdrawErr error
	onWithdraw  func()
	onRegister  func()
}

func (f *fakeRegistrar) Register(_ context.Context, self grpcproc.Member) (func(context.Context) error, error) {
	if f.onRegister != nil {
		f.onRegister()
	}
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
	synctest.Test(t, func(t *testing.T) {
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
		if _, err := spawnEcho(t, b).Call[*testpb.Pong](ctx(t), w, &testpb.Ping{N: 1}); err != nil {
			t.Fatal(err)
		}
		inc := b.ID().Incarnation

		// Events that settle nothing: about this node, about a node a has no
		// link to, about another incarnation of b going away, about the one a
		// links with, or an older one, being up.
		m.events <- grpcproc.MemberEvent{Member: grpcproc.Member{Name: "a", Incarnation: a.ID().Incarnation}}
		m.events <- grpcproc.MemberEvent{Member: grpcproc.Member{Name: "zzz", Incarnation: 9}}
		m.events <- grpcproc.MemberEvent{Member: grpcproc.Member{Name: "b", Incarnation: inc + 100}}
		m.events <- grpcproc.MemberEvent{Member: grpcproc.Member{Name: "b", Incarnation: inc}, Up: true}
		m.events <- grpcproc.MemberEvent{Member: grpcproc.Member{Name: "b", Incarnation: inc - 1}, Up: true}
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
		if _, err := spawnEcho(t, b).Call[*testpb.Pong](ctx(t), a, &testpb.Ping{N: 1}); err != nil {
			t.Fatal(err)
		}
		m.events <- grpcproc.MemberEvent{Member: grpcproc.Member{Name: "b", Incarnation: inc + 1}, Up: true}
		waitNoPeer(t, a, "b")
		// The b a linked with is now an old incarnation.
		if _, err := spawnEcho(t, b).Call[*testpb.Pong](ctx(t), a, &testpb.Ping{N: 1}); err == nil || !strings.Contains(err.Error(), fmt.Sprintf("b#%d is an old incarnation", inc)) {
			t.Fatalf("a calls the old b: %v", err)
		}

		// A peer a only hears from (an inbound link, no outbound) leaves too.
		cn := c.Node("c")
		sink, got := collector(t, a)
		_ = cn.SendTo(t.Context(), sink, &testpb.Ping{})
		recv(t, got)
		// Incarnation 0: whichever it was.
		m.events <- grpcproc.MemberEvent{Member: grpcproc.Member{Name: "c"}}
		waitNoPeer(t, a, "c")
	})
}

func TestRegistrarLifecycle(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		r := &fakeRegistrar{}
		c := grpcproctest.NewWith(t, []grpcproctest.Option{grpcproctest.WithConfig(func(name string, cfg *grpcproc.Config) {
			if name == "a" {
				cfg.Registrar = r
				cfg.Metadata = map[string]string{"version": "1.2.3"}
			}
		})}, "a", "b")
		a, b := c.Node("a"), c.Node("b")
		if err := a.Start(t.Context()); err != nil { // a second Start does nothing
			t.Fatal(err)
		}
		want := grpcproc.Member{Name: "a", Incarnation: a.ID().Incarnation, Addr: "a", Metadata: map[string]string{"version": "1.2.3"}}
		if len(r.members) != 1 || !reflect.DeepEqual(r.members[0], want) {
			t.Fatalf("registered %+v", r.members)
		}
		// Withdrawing comes last: by then a's processes are gone and b has seen
		// their Down{shutdown}.
		e, _ := a.Spawn(echo)
		w, ch := watcher(t, b)
		w.Monitor(e)
		if _, err := e.Call[*testpb.Pong](ctx(t), w, &testpb.Ping{N: 1}); err != nil {
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
	})
}

func TestStartAndStopErrors(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		boom := errors.New("etcd down")
		down := &fakeMembership{err: boom, events: make(chan grpcproc.MemberEvent)}
		n, _ := grpcproc.NewNode(grpcproc.Config{Name: "a", Resolver: grpcproc.StaticResolver{}, Membership: down})
		if err := n.Start(t.Context()); !errors.Is(err, boom) || !strings.Contains(err.Error(), "membership") {
			t.Fatalf("got %v", err)
		}
		down.err = nil
		if err := n.Start(t.Context()); err != nil || n.Info().StartedAt.IsZero() {
			t.Fatalf("the retry: %v", err)
		}
		_ = n.Stop(t.Context())
		n, _ = grpcproc.NewNode(grpcproc.Config{Name: "a", Resolver: grpcproc.StaticResolver{}, Registrar: &fakeRegistrar{err: boom}})
		if err := n.Start(t.Context()); !errors.Is(err, boom) || !strings.Contains(err.Error(), "register") {
			t.Fatalf("got %v", err)
		}
		// A Start that failed stops the watch it began, and the next Start tries
		// again.
		var watches []context.Context
		watch := membershipFunc(func(ctx context.Context) (<-chan grpcproc.MemberEvent, error) {
			watches = append(watches, ctx)
			out := make(chan grpcproc.MemberEvent)
			context.AfterFunc(ctx, func() { close(out) })
			return out, nil
		})
		flaky := &fakeRegistrar{err: boom}
		n, _ = grpcproc.NewNode(grpcproc.Config{Name: "a", Resolver: grpcproc.StaticResolver{}, Membership: watch, Registrar: flaky})
		if err := n.Start(t.Context()); !errors.Is(err, boom) {
			t.Fatalf("got %v", err)
		}
		if watches[0].Err() == nil {
			t.Fatal("a failed Start left its watch running")
		}
		flaky.err = nil
		if err := n.Start(t.Context()); err != nil || len(flaky.members) != 1 || len(watches) != 2 || watches[1].Err() != nil {
			t.Fatalf("the second Start: %v, registered %d times, watched %d times", err, len(flaky.members), len(watches))
		}
		if n.Info().StartedAt.IsZero() {
			t.Fatal("a started node has no start time")
		}
		_ = n.Stop(t.Context())

		r := &fakeRegistrar{withdrawErr: boom}
		n, _ = grpcproc.NewNode(grpcproc.Config{Name: "a", Resolver: grpcproc.StaticResolver{}, Registrar: r})
		if err := n.Start(t.Context()); err != nil {
			t.Fatal(err)
		}
		if err := n.Stop(t.Context()); !errors.Is(err, boom) || !strings.Contains(err.Error(), "withdraw") {
			t.Fatalf("got %v", err)
		}
	})
}

// membershipFunc adapts a function to grpcproc.Membership.
type membershipFunc func(ctx context.Context) (<-chan grpcproc.MemberEvent, error)

func (f membershipFunc) Watch(ctx context.Context) (<-chan grpcproc.MemberEvent, error) {
	return f(ctx)
}

// Start after Stop refuses, and a registration that completes while Stop
// runs is withdrawn by Start itself: Stop has already withdrawn what it knew.
func TestStartAndStopTogether(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		n, _ := grpcproc.NewNode(grpcproc.Config{Name: "a", Resolver: grpcproc.StaticResolver{}})
		_ = n.Stop(t.Context())
		if err := n.Start(t.Context()); !errors.Is(err, grpcproc.ErrNodeStopped) {
			t.Fatalf("got %v", err)
		}

		entered, release := make(chan struct{}), make(chan struct{})
		r := &fakeRegistrar{onRegister: func() { close(entered); <-release }}
		n, _ = grpcproc.NewNode(grpcproc.Config{Name: "a", Resolver: grpcproc.StaticResolver{}, Registrar: r})
		started := make(chan error, 1)
		go func() { started <- n.Start(t.Context()) }()
		<-entered
		if err := n.Stop(t.Context()); err != nil {
			t.Fatal(err)
		}
		close(release)
		if err := <-started; !errors.Is(err, grpcproc.ErrNodeStopped) || r.withdrawn != 1 {
			t.Fatalf("got %v, withdrawn %d times", err, r.withdrawn)
		}
	})
}

// Start's ctx bounds Membership.Watch: a Watch that returns only after it
// ended fails Start, and its watch is stopped.
func TestStartBoundsTheWatch(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var watching context.Context
		slow := membershipFunc(func(ctx context.Context) (<-chan grpcproc.MemberEvent, error) {
			watching = ctx
			<-ctx.Done() // ended by Start's deadline
			out := make(chan grpcproc.MemberEvent)
			close(out)
			return out, nil
		})
		n, _ := grpcproc.NewNode(grpcproc.Config{Name: "a", Resolver: grpcproc.StaticResolver{}, Membership: slow})
		ctx, cancel := context.WithTimeout(t.Context(), 50*time.Millisecond)
		defer cancel()
		if err := n.Start(ctx); !errors.Is(err, context.DeadlineExceeded) || watching.Err() == nil {
			t.Fatalf("got %v", err)
		}
	})
}

func TestMembershipIsTheConfigs(t *testing.T) {
	m := &fakeMembership{}
	with, err := grpcproc.NewNode(grpcproc.Config{Name: "a", Resolver: grpcproc.StaticResolver{}, Membership: m})
	if err != nil {
		t.Fatal(err)
	}
	without, err := grpcproc.NewNode(grpcproc.Config{Name: "b", Resolver: grpcproc.StaticResolver{}})
	if err != nil {
		t.Fatal(err)
	}
	if with.Membership() != m || without.Membership() != nil {
		t.Fatalf("%v %v", with.Membership(), without.Membership())
	}
}

// Members is what Membership reports up, with the Metadata each member
// registered: a newer incarnation replaces an older one, which is not let
// back, and a member goes when its incarnation, or whichever, is reported
// down.
func TestMembers(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		m := &fakeMembership{events: make(chan grpcproc.MemberEvent)}
		c := grpcproctest.NewWith(t, []grpcproctest.Option{grpcproctest.WithConfig(func(name string, cfg *grpcproc.Config) {
			cfg.Membership = m
		})}, "a")
		a := c.Node("a")
		members := func() string {
			var out []string
			for _, mb := range a.Members() {
				out = append(out, fmt.Sprintf("%s#%d%v", mb.Name, mb.Incarnation, mb.Metadata))
			}
			return strings.Join(out, " ")
		}
		up := func(name string, inc uint64, version string) {
			m.events <- grpcproc.MemberEvent{Up: true, Member: grpcproc.Member{Name: name, Incarnation: inc, Metadata: map[string]string{"version": version}}}
		}
		down := func(name string, inc uint64) {
			m.events <- grpcproc.MemberEvent{Member: grpcproc.Member{Name: name, Incarnation: inc}}
		}
		check := func(want string) {
			t.Helper()
			flush(m) // a down event for zzz, which a never had
			if got := members(); got != want {
				t.Fatalf("members %q, want %q", got, want)
			}
		}

		up("a", a.ID().Incarnation, "2")
		up("x", 5, "1")
		check("a#" + fmt.Sprint(a.ID().Incarnation) + "map[version:2] x#5map[version:1]")
		up("x", 4, "0") // older: not let back
		up("x", 6, "2")
		down("x", 5) // not the one kept
		check("a#" + fmt.Sprint(a.ID().Incarnation) + "map[version:2] x#6map[version:2]")
		if mb, ok := a.Member("x"); !ok || mb.Metadata["version"] != "2" {
			t.Fatalf("Member(x): %+v, %v", mb, ok)
		}
		down("x", 0) // whichever
		up("y", 0, "3")
		check("a#" + fmt.Sprint(a.ID().Incarnation) + "map[version:2] y#0map[version:3]")
		if _, ok := a.Member("x"); ok {
			t.Fatal("x is still a member")
		}
	})
}

// A node's Metadata is its own copy, in NodeInfo too.
func TestNodeMetadata(t *testing.T) {
	md := map[string]string{"zone": "eu-1"}
	n, err := grpcproc.NewNode(grpcproc.Config{Name: "a", Resolver: grpcproc.StaticResolver{}, Metadata: md})
	if err != nil {
		t.Fatal(err)
	}
	md["zone"] = "us-1"
	info := n.Info()
	info.Metadata["zone"] = "ap-1"
	if got := n.Info().Metadata["zone"]; got != "eu-1" {
		t.Fatalf("zone %q", got)
	}
	if len(n.Members()) != 0 {
		t.Fatalf("members without Membership: %v", n.Members())
	}
}
