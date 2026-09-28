package grpcproc_test

import (
	"context"
	"errors"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"google.golang.org/protobuf/proto"

	"github.com/floatdrop/grpcproc"
	"github.com/floatdrop/grpcproc/grpcproctest"
	"github.com/floatdrop/grpcproc/internal/testpb"
)

func withBackoff(limit time.Duration, more ...func(name string, cfg *grpcproc.Config)) []grpcproctest.Option {
	return []grpcproctest.Option{grpcproctest.WithConfig(func(name string, cfg *grpcproc.Config) {
		cfg.DialBackoff = limit
		for _, fn := range more {
			fn(name, cfg)
		}
	})}
}

// downLink is a's outbound link to b while dials to b are backed off.
func downLink(t *testing.T, a *grpcproc.Node) grpcproc.LinkInfo {
	t.Helper()
	for _, l := range a.Info().Links {
		if l.Peer.Name == "b" && l.Outbound && l.State == grpcproc.LinkDown {
			return l
		}
	}
	t.Fatalf("no down link to b in %+v", a.Info().Links)
	return grpcproc.LinkInfo{}
}

func backedOff(err error) bool {
	le, ok := errors.AsType[*grpcproc.LinkError](err)
	return ok && le.Unsent && errors.Is(err, grpcproc.ErrNoConnection) && strings.Contains(err.Error(), "next dial in")
}

// eventually polls cond until it holds, for up to 5s.
func eventually(t *testing.T, what string, cond func() bool) {
	t.Helper()
	for deadline := time.Now().Add(5 * time.Second); !cond(); {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func TestDialBackoff(t *testing.T) {
	// The first wait is a 32nd of an hour, longer than the test.
	c := grpcproctest.NewWith(t, withBackoff(time.Hour), "a", "b")
	a, b := c.Node("a"), c.Node("b")
	toB := grpcproc.Named[*testpb.Ping]("b", "x")
	send := func() error { return a.SendTo(t.Context(), toB, &testpb.Ping{}) }

	c.Partition("a", "b")
	if err := send(); !errors.Is(err, grpcproc.ErrNoConnection) || backedOff(err) {
		t.Fatalf("first send must dial, and fail: %v", err)
	}
	// The next one does not dial: it fails at once, saying why the dial did.
	if err := send(); !backedOff(err) || !strings.Contains(err.Error(), "partitioned") {
		t.Fatalf("got %v", err)
	}
	if l := downLink(t, a); !strings.Contains(l.LastError, "partitioned") || time.Until(l.RetryAt) < time.Minute {
		t.Fatalf("%+v", l)
	}

	// The network heals, which a cannot know: it keeps failing at once,
	// until b reaches it.
	c.Heal("a", "b")
	if err := send(); !backedOff(err) {
		t.Fatalf("got %v", err)
	}
	if err := b.SendTo(t.Context(), grpcproc.Named[*testpb.Ping]("a", "x"), &testpb.Ping{}); err != nil {
		t.Fatal(err)
	}
	eventually(t, "b's link to a", func() bool { return slices.Contains(a.Peers(), "b") })
	if err := send(); err != nil {
		t.Fatal(err)
	}
	for _, l := range a.Info().Links {
		if l.State == grpcproc.LinkDown {
			t.Fatalf("a link is still down: %+v", l)
		}
	}
}

// Membership reporting the peer up ends the wait.
func TestDialBackoffEndsWhenMembershipSaysUp(t *testing.T) {
	m := &fakeMembership{events: make(chan grpcproc.MemberEvent)}
	c := grpcproctest.NewWith(t, withBackoff(time.Hour, func(name string, cfg *grpcproc.Config) {
		if name == "a" {
			cfg.Membership = m
		}
	}), "a", "b")
	a := c.Node("a")
	send := func() error { return a.SendTo(t.Context(), grpcproc.Named[*testpb.Ping]("b", "x"), &testpb.Ping{}) }

	c.Partition("a", "b")
	_ = send()
	c.Heal("a", "b")
	if err := send(); !backedOff(err) {
		t.Fatalf("got %v", err)
	}
	m.events <- grpcproc.MemberEvent{Member: grpcproc.Member{Name: "b", Incarnation: c.Node("b").ID().Incarnation}, Up: true}
	eventually(t, "a send to b", func() bool { return send() == nil })
}

// So does Membership reporting it up with no incarnation, after a has seen
// one.
func TestDialBackoffEndsWhenMembershipSaysUpAsWhicheverIncarnation(t *testing.T) {
	m := &fakeMembership{events: make(chan grpcproc.MemberEvent)}
	c := grpcproctest.NewWith(t, withBackoff(time.Hour, func(name string, cfg *grpcproc.Config) {
		if name == "a" {
			cfg.Membership = m
		}
	}), "a", "b")
	a := c.Node("a")
	send := func() error { return a.SendTo(t.Context(), grpcproc.Named[*testpb.Ping]("b", "x"), &testpb.Ping{}) }
	c.Partition("a", "b")
	_ = send()
	// a hears of b's incarnation, which ends the wait; the next dial fails.
	m.events <- grpcproc.MemberEvent{Member: grpcproc.Member{Name: "b", Incarnation: c.Node("b").ID().Incarnation}, Up: true}
	flush(m)
	_ = send()
	c.Heal("a", "b")
	if err := send(); !backedOff(err) {
		t.Fatalf("got %v", err)
	}
	m.events <- grpcproc.MemberEvent{Member: grpcproc.Member{Name: "b"}, Up: true}
	eventually(t, "a send to b", func() bool { return send() == nil })
}

// The first send after the wait dials again.
func TestDialBackoffRetriesAfterTheWait(t *testing.T) {
	c := grpcproctest.NewWith(t, withBackoff(3200*time.Millisecond), "a", "b") // the first wait is 100ms
	a := c.Node("a")
	send := func() error { return a.SendTo(t.Context(), grpcproc.Named[*testpb.Ping]("b", "x"), &testpb.Ping{}) }

	c.Partition("a", "b")
	for i := range 3 {
		if err := send(); !errors.Is(err, grpcproc.ErrNoConnection) || backedOff(err) {
			t.Fatalf("send %d must dial, and fail: %v", i, err)
		}
		time.Sleep(time.Until(downLink(t, a).RetryAt))
	}
	c.Heal("a", "b")
	if err := send(); err != nil {
		t.Fatal(err)
	}
}

// unreachable makes a's dials to b fail, while b reaches a: a one-way
// partition, or a peer behind an address this node cannot route to. gate, if
// set, holds each dial until it returns.
func unreachable(gate func()) func(name string, cfg *grpcproc.Config) {
	return func(name string, cfg *grpcproc.Config) {
		if name != "a" {
			return
		}
		cfg.Resolver = grpcproc.ResolverFunc(func(_ context.Context, node string) (string, error) {
			if node != "b" {
				return "passthrough:///" + node, nil
			}
			if gate != nil {
				gate()
			}
			return "", errors.New("no route to b")
		})
	}
}

// A reply or a Down that a cannot route to b would be lost without b ever
// knowing, since b's own link to a stays up. a cuts that link instead, so b
// sees a as unreachable: its call fails, its monitor fires.
func TestUnroutableReplyOrDownCutsThePeersLink(t *testing.T) {
	c := grpcproctest.NewWith(t, []grpcproctest.Option{grpcproctest.WithConfig(unreachable(nil))}, "a", "b")
	a, b := c.Node("a"), c.Node("b")
	echo, err := a.Spawn(func(p *grpcproc.Process[*testpb.Ping]) error {
		for {
			m, err := p.Receive()
			if err != nil {
				return err
			}
			_ = m.Reply(m.Body, nil)
		}
	}, grpcproc.WithName("echo"))
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()

	// A reply from a process. b is told why its link went, and that its
	// call may have been handled: it was.
	_, err = grpcproc.Named[*testpb.Ping]("a", "echo").Call[*testpb.Ping](ctx, b, &testpb.Ping{})
	if le, ok := errors.AsType[*grpcproc.LinkError](err); !ok || le.Unsent || !strings.Contains(err.Error(), "a cannot reach b back: no route to b") {
		t.Fatalf("call: %v", err)
	}
	// A reply a sends while dispatching b's frame: no such process.
	if _, err := b.CallTo[*testpb.Ping](ctx, grpcproc.Named[*testpb.Ping]("a", "nobody"), &testpb.Ping{}); !errors.Is(err, grpcproc.ErrNoConnection) {
		t.Fatalf("call to nobody: %v", err)
	}
	// A Down.
	downs := make(chan grpcproc.Down, 1)
	if _, err := b.Spawn(func(p *grpcproc.Process[proto.Message]) error {
		p.Monitor(echo)
		m, err := p.Receive()
		if err != nil {
			return err
		}
		downs <- *m.Down
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	eventually(t, "b's link to a", func() bool { return slices.Contains(a.Peers(), "b") })
	if err := a.Exit(ctx, echo, "bye"); err != nil {
		t.Fatal(err)
	}
	select {
	case d := <-downs:
		if d.Reason != grpcproc.ReasonNoConnection {
			t.Fatalf("down: %+v", d)
		}
	case <-ctx.Done():
		t.Fatal("the monitor never fired")
	}
}

// While b reaches a, a shows both links to b: b's, up, and its own, down.
// Disconnect forgets the failed dials.
func TestDialBackoffWhileThePeerReachesUs(t *testing.T) {
	c := grpcproctest.NewWith(t, withBackoff(time.Hour, unreachable(nil)), "a", "b")
	a, b := c.Node("a"), c.Node("b")
	send := func() error { return a.SendTo(t.Context(), grpcproc.Named[*testpb.Ping]("b", "x"), &testpb.Ping{}) }

	if err := b.SendTo(t.Context(), grpcproc.Named[*testpb.Ping]("a", "x"), &testpb.Ping{}); err != nil {
		t.Fatal(err)
	}
	eventually(t, "b's link to a", func() bool { return slices.Contains(a.Peers(), "b") })
	if err := send(); !errors.Is(err, grpcproc.ErrNoConnection) || backedOff(err) {
		t.Fatalf("got %v", err)
	}
	links := a.Info().Links
	if len(links) != 2 || !links[0].Outbound || links[0].State != grpcproc.LinkDown || links[1].Outbound || links[1].State != grpcproc.LinkUp {
		t.Fatalf("%+v", links)
	}
	if err := send(); !backedOff(err) {
		t.Fatalf("got %v", err)
	}
	if !a.Disconnect("b") {
		t.Fatal("nothing to disconnect")
	}
	if err := send(); !errors.Is(err, grpcproc.ErrNoConnection) || backedOff(err) {
		t.Fatalf("after Disconnect, the send must dial: %v", err)
	}
}

// Once the wait has passed, one send dials again, and the others do not wait
// for it: a hung peer holds one sender at a time.
func TestDialBackoffProbesAlone(t *testing.T) {
	var calls atomic.Int32
	entered, release := make(chan struct{}), make(chan struct{})
	n, err := grpcproc.NewNode(grpcproc.Config{Name: "a", DialBackoff: 3200 * time.Millisecond,
		Resolver: grpcproc.ResolverFunc(func(context.Context, string) (string, error) {
			if calls.Add(1) == 2 {
				close(entered)
				<-release
			}
			return "", errors.New("no route")
		})})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = n.Stop(context.Background()) })
	send := func() error { return n.SendTo(t.Context(), grpcproc.Named[*testpb.Ping]("b", "x"), &testpb.Ping{}) }

	_ = send()
	time.Sleep(time.Until(downLink(t, n).RetryAt))
	probe := make(chan error, 1)
	go func() { probe <- send() }()
	<-entered
	// Bounded: a send that joined the dial would wait for release forever.
	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
	defer cancel()
	if err := n.SendTo(ctx, grpcproc.Named[*testpb.Ping]("b", "x"), &testpb.Ping{}); !errors.Is(err, grpcproc.ErrNoConnection) || !strings.Contains(err.Error(), "the next dial is under way") {
		t.Fatalf("got %v", err)
	}
	close(release)
	if err := <-probe; !errors.Is(err, grpcproc.ErrNoConnection) || backedOff(err) {
		t.Fatalf("probe: %v", err)
	}
}

// A dial under way when Disconnect forgets the backoff starts none when it
// fails: it began before.
func TestDisconnectForgetsADialUnderWay(t *testing.T) {
	entered, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	gate := func() {
		once.Do(func() { close(entered) })
		<-release
	}
	c := grpcproctest.NewWith(t, withBackoff(time.Hour, unreachable(gate)), "a", "b")
	a := c.Node("a")
	send := func() error { return a.SendTo(t.Context(), grpcproc.Named[*testpb.Ping]("b", "x"), &testpb.Ping{}) }

	dialed := make(chan error, 1)
	go func() { dialed <- send() }()
	<-entered
	a.Disconnect("b")
	close(release)
	if err := <-dialed; !errors.Is(err, grpcproc.ErrNoConnection) {
		t.Fatalf("got %v", err)
	}
	if err := send(); !errors.Is(err, grpcproc.ErrNoConnection) || backedOff(err) {
		t.Fatalf("the send must dial: %v", err)
	}
}

// In a one-way partition the peer keeps reaching this node, and each time the
// next send dials it at once; but the wait keeps doubling, so the peer's
// reconnects do not turn every reply into a dial of its own.
func TestDialBackoffInAOneWayPartition(t *testing.T) {
	c := grpcproctest.NewWith(t, withBackoff(time.Hour, unreachable(nil)), "a", "b")
	a, b := c.Node("a"), c.Node("b")
	if _, err := a.Spawn(func(p *grpcproc.Process[*testpb.Ping]) error {
		for {
			m, err := p.Receive()
			if err != nil {
				return err
			}
			_ = m.Reply(m.Body, nil)
		}
	}, grpcproc.WithName("echo")); err != nil {
		t.Fatal(err)
	}
	var waits []time.Duration
	for range 2 {
		_, err := grpcproc.Named[*testpb.Ping]("a", "echo").Call[*testpb.Ping](t.Context(), b, &testpb.Ping{})
		if !errors.Is(err, grpcproc.ErrNoConnection) || !strings.Contains(err.Error(), "a cannot reach b back: no route to b") {
			t.Fatalf("got %v", err)
		}
		waits = append(waits, time.Until(downLink(t, a).RetryAt))
	}
	// A 32nd of an hour, less up to a fifth; then twice that.
	if waits[0] > 113*time.Second || waits[1] < 150*time.Second {
		t.Fatalf("waits %v", waits)
	}
}
