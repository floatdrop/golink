package actor_test

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"testing/synctest"
	"time"

	"google.golang.org/protobuf/proto"

	"github.com/floatdrop/grpcproc"
	"github.com/floatdrop/grpcproc/actor"
	"github.com/floatdrop/grpcproc/grpcproctest"
	"github.com/floatdrop/grpcproc/internal/testpb"
)

// plain handles messages only.
type plain struct{ seen chan int64 }

func (p plain) HandleMessage(_ *P, m M) error {
	p.seen <- m.Body.GetN()
	return nil
}

func TestRunLifecycle(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		c := grpcproctest.New(t, "a")
		n := c.Node("a")
		h := &counter{}
		addr, err := n.Spawn(actor.Run[*testpb.Ping](h))
		if err != nil {
			t.Fatal(err)
		}
		downs := watch(t, n, addr)
		for range 3 {
			_ = addr.Send(t.Context(), n, &testpb.Ping{N: 1})
		}
		if r, err := addr.Call[*testpb.Pong](t.Context(), n, &testpb.Ping{}); err != nil || r.GetN() != 3 {
			t.Fatalf("call: %v %v", r, err)
		}
		// An error from HandleCall goes to the caller; the actor carries on.
		if _, err := addr.Call[*testpb.Pong](t.Context(), n, &testpb.Ping{N: -1}); err == nil || err.Error() != "bad call" {
			t.Fatalf("call error: %v", err)
		}
		// A deferred reply, from another goroutine.
		if r, err := addr.Call[*testpb.Pong](t.Context(), n, &testpb.Ping{N: -4}); err != nil || r.GetN() != 42 {
			t.Fatalf("deferred: %v %v", r, err)
		}
		// ErrStop from a call replies first, then stops normally.
		if r, err := addr.Call[*testpb.Pong](t.Context(), n, &testpb.Ping{N: -2}); err != nil || r.GetN() != 3 {
			t.Fatalf("stop call: %v %v", r, err)
		}
		if d := within(t, downs); d.Reason != grpcproc.ReasonNormal {
			t.Fatalf("%+v", d)
		}
		if got := h.Log(); got != "init; terminate" {
			t.Fatalf("log: %s", got)
		}
	})
}

func TestRunExits(t *testing.T) {
	cases := []struct {
		name   string
		act    func(n *grpcproc.Node, a grpcproc.Addr[*testpb.Ping])
		reason string
		log    string
	}{
		{"message error", func(n *grpcproc.Node, a grpcproc.Addr[*testpb.Ping]) {
			_ = a.Send(context.Background(), n, &testpb.Ping{N: -1})
		}, "bad message", "init; terminate: bad message"},
		{"stop", func(n *grpcproc.Node, a grpcproc.Addr[*testpb.Ping]) {
			_ = a.Send(context.Background(), n, &testpb.Ping{N: -2})
		}, grpcproc.ReasonNormal, "init; terminate"},
		{"panic", func(n *grpcproc.Node, a grpcproc.Addr[*testpb.Ping]) {
			_ = a.Send(context.Background(), n, &testpb.Ping{N: -3})
		}, "panic: kaboom", "init; terminate: panic: kaboom"},
		{"exit", func(n *grpcproc.Node, a grpcproc.Addr[*testpb.Ping]) { _ = n.Exit(context.Background(), a, "bye") }, "bye", "init; terminate: grpcproc: exit: bye"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				c := grpcproctest.New(t, "a")
				n := c.Node("a")
				h := &counter{}
				addr, _ := n.Spawn(actor.Run[*testpb.Ping](h))
				downs := watch(t, n, addr)
				tc.act(n, addr)
				if d := within(t, downs); d.Reason != tc.reason {
					t.Fatalf("reason %q", d.Reason)
				}
				if got := h.Log(); got != tc.log {
					t.Fatalf("log: %s", got)
				}
			})
		})
	}
}

func TestInitFailureSkipsTerminate(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		c := grpcproctest.New(t, "a")
		n := c.Node("a")
		h := &counter{failInit: true}
		addr, _ := n.Spawn(actor.Run[*testpb.Ping](h))
		downs := watch(t, n, addr)
		if d := within(t, downs); d.Reason != "init failed" && d.Reason != grpcproc.ReasonNoProc {
			t.Fatalf("%+v", d)
		}
		time.Sleep(10 * time.Millisecond)
		if got := h.Log(); got != "" {
			t.Fatalf("log: %s", got)
		}
	})
}

func TestDownsAndOptionalInterfaces(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		c := grpcproctest.New(t, "a")
		n := c.Node("a")
		// With a DownHandler, Downs reach it.
		h := &counter{}
		target, _ := n.Spawn[*testpb.Ping](func(p *P) error { _, err := p.Receive(); return err })
		_, _ = n.Spawn(actor.Run[*testpb.Ping](&monitoring{target: target, counter: h}))
		time.Sleep(10 * time.Millisecond)
		_ = n.Exit(t.Context(), target, "gone")
		until(t, "HandleDown gets the Down", func() bool { return strings.Contains(h.Log(), "down gone") })
		// Without a CallHandler, calls are answered with an error.
		seen := make(chan int64, 4)
		pa, _ := n.Spawn(actor.Run[*testpb.Ping](plain{seen: seen}))
		if _, err := pa.Call[*testpb.Pong](t.Context(), n, &testpb.Ping{}); err == nil || !strings.Contains(err.Error(), "does not handle calls") {
			t.Fatalf("got %v", err)
		}
	})
}

// monitoring monitors target in Init and hands Downs to counter.
type monitoring struct {
	target grpcproc.Addr[*testpb.Ping]
	*counter
}

func (m *monitoring) Init(p *P) error {
	p.Monitor(m.target)
	return nil
}

func TestPlainIgnoresDowns(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		c := grpcproctest.New(t, "a")
		n := c.Node("a")
		seen := make(chan int64, 4)
		target, _ := n.Spawn[*testpb.Ping](func(p *P) error { _, err := p.Receive(); return err })
		pa, _ := n.Spawn(actor.Run[*testpb.Ping](&monitoringPlain{plain: plain{seen: seen}, target: target}))
		_ = n.Exit(t.Context(), target, "gone")
		time.Sleep(20 * time.Millisecond)
		_ = pa.Send(t.Context(), n, &testpb.Ping{N: 9})
		if got := <-seen; got != 9 {
			t.Fatal(got) // the Down was skipped, the process is alive
		}
	})
}

type monitoringPlain struct {
	plain
	target grpcproc.Addr[*testpb.Ping]
}

func (m *monitoringPlain) Init(p *P) error {
	p.Monitor(m.target)
	return nil
}

// quoter only answers calls.
type quoter struct{ actor.CallsOnly[*testpb.Ping] }

func (quoter) HandleCall(_ *P, m M) (proto.Message, error) {
	return &testpb.Pong{N: m.Body.GetN() + 1}, nil
}

func TestCallsOnly(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		n, logs := logged(t)
		addr, err := n.Spawn(actor.Run(quoter{}))
		if err != nil {
			t.Fatal(err)
		}
		// A plain send is dropped with a warning; the actor carries on.
		if err := addr.Send(t.Context(), n, &testpb.Ping{N: 1}); err != nil {
			t.Fatal(err)
		}
		r, err := addr.Call[*testpb.Pong](t.Context(), n, &testpb.Ping{N: 2})
		if err != nil || r.GetN() != 3 {
			t.Fatalf("%v %v", r, err)
		}
		if out := logs.String(); !strings.Contains(out, "dropped a message sent without a call") || !strings.Contains(out, "grpcproc.test.v1.Ping") {
			t.Fatalf("log:\n%s", out)
		}
	})
}

// quoterByValue has a pointer-receiver HandleCall: spawned by value, it
// would answer nothing.
type quoterByValue struct{ actor.CallsOnly[*testpb.Ping] }

func (*quoterByValue) HandleCall(*P, M) (proto.Message, error) { return &testpb.Pong{}, nil }

func TestCallsOnlyWithoutHandleCallPanics(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		defer func() {
			if r := recover(); r == nil || !strings.Contains(fmt.Sprint(r), "quoterByValue") {
				t.Fatalf("recovered %v", r)
			}
		}()
		_ = actor.Run(quoterByValue{})
	})
}
