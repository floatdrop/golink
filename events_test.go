package grpcproc_test

import (
	"context"
	"slices"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"google.golang.org/protobuf/proto"

	"github.com/floatdrop/grpcproc"
	"github.com/floatdrop/grpcproc/grpcproctest"
	"github.com/floatdrop/grpcproc/internal/testpb"
)

func nextEvent(t *testing.T, ch <-chan grpcproc.Event, kind grpcproc.EventKind) grpcproc.Event {
	t.Helper()
	deadline := time.After(5 * time.Second)
	for {
		select {
		case ev, ok := <-ch:
			if !ok {
				t.Fatalf("channel closed waiting for %v", kind)
			}
			if ev.Kind == kind {
				return ev
			}
		case <-deadline:
			t.Fatalf("timeout waiting for %v", kind)
		}
	}
}

func TestSubscribe(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		c := grpcproctest.New(t, "a", "b")
		a := c.Node("a")
		events := a.Subscribe(t.Context(), 64)

		e, _ := a.Spawn(echo, grpcproc.WithLabel("echo"))
		if ev := nextEvent(t, events, grpcproc.EventSpawn); ev.Process.PID != e.PID() || ev.Process.Label != "echo" || ev.Time.IsZero() {
			t.Fatalf("%+v", ev)
		}
		_ = a.SendTo(t.Context(), e.PID(), &testpb.Pong{})
		if ev := nextEvent(t, events, grpcproc.EventDeadLetter); ev.To != e.PID() || ev.Type != "grpcproc.test.v1.Pong" || ev.Reason != grpcproc.ReasonType {
			t.Fatalf("%+v", ev)
		}
		_ = e.Send(t.Context(), a, &testpb.Ping{N: -100})
		if ev := nextEvent(t, events, grpcproc.EventExit); ev.Process.PID != e.PID() || ev.Reason != "boom" {
			t.Fatalf("%+v", ev)
		}
		e2, _ := c.Node("b").Spawn(echo)
		if _, err := e2.Call[*testpb.Pong](t.Context(), a, &testpb.Ping{N: 1}); err != nil {
			t.Fatal(err)
		}
		if ev := nextEvent(t, events, grpcproc.EventLinkUp); ev.Peer.Name != "b" {
			t.Fatalf("%+v", ev)
		}
		c.Kill("b")
		if ev := nextEvent(t, events, grpcproc.EventLinkDown); ev.Peer.Name != "b" || ev.Err == "" {
			t.Fatalf("%+v", ev)
		}
		for _, k := range []grpcproc.EventKind{grpcproc.EventSpawn, grpcproc.EventExit, grpcproc.EventLinkUp, grpcproc.EventLinkDown, grpcproc.EventDeadLetter} {
			if k.String() == "" {
				t.Fatal("kind string")
			}
		}
	})
}

func TestSubscribeMissedAndClose(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		c := grpcproctest.New(t, "a")
		a := c.Node("a")
		ctx, cancel := context.WithCancel(t.Context())
		events := a.Subscribe(ctx, 1)
		quiet := a.Subscribe(t.Context(), 0) // buffer rounds up to 1
		for range 5 {
			_ = a.SendTo(t.Context(), grpcproc.PID{Node: "a"}, proto.Message(&testpb.Ping{}))
		}
		<-events // the first one fitted
		_ = a.SendTo(t.Context(), grpcproc.PID{Node: "a"}, &testpb.Ping{})
		if ev := <-events; ev.Missed != 4 {
			t.Fatalf("missed %d", ev.Missed)
		}
		cancel()
		for range events {
		}
		// A second subscriber ends when the node stops.
		c.Stop("a")
		for range quiet {
		}
	})
}

func TestLinkDownWithoutError(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		// A peer that closes its side cleanly still produces a link-down event.
		c := grpcproctest.New(t, "a", "b")
		a := c.Node("a")
		events := a.Subscribe(t.Context(), 64)
		e, _ := c.Node("b").Spawn(echo)
		if _, err := e.Call[*testpb.Pong](t.Context(), a, &testpb.Ping{N: 1}); err != nil {
			t.Fatal(err)
		}
		c.Stop("b")
		nextEvent(t, events, grpcproc.EventLinkDown)
	})
}

// orderHooks records spawns and exits in the order the node reports them,
// taking its time over each exit.
type orderHooks struct {
	grpcproc.NopHooks
	mu  sync.Mutex
	log []string
}

func (h *orderHooks) OnSpawn(info grpcproc.ProcessInfo) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.log = append(h.log, "spawn "+info.Label)
}

func (h *orderHooks) OnExit(info grpcproc.ProcessInfo, _ string) {
	time.Sleep(10 * time.Millisecond)
	h.mu.Lock()
	defer h.mu.Unlock()
	h.log = append(h.log, "exit "+info.Label)
}

func TestExitReportedBeforeItsConsequences(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		// A watcher that replaces what it monitors, as a supervisor does: the
		// exit must be reported (hooks, then events) before the replacement's
		// spawn, however long reporting it takes.
		h := &orderHooks{}
		a := grpcproctest.NewWith(t, []grpcproctest.Option{grpcproctest.WithHooks(h)}, "a").Node("a")
		victim, _ := a.Spawn(echo, grpcproc.WithLabel("victim"))
		done := make(chan struct{})
		_, _ = a.Spawn(func(p *grpcproc.Process[proto.Message]) error {
			defer close(done)
			p.Monitor(victim)
			_ = p.Exit(victim, "boom")
			if _, err := p.Receive(); err != nil {
				return err
			}
			_, err := a.Spawn(echo, grpcproc.WithLabel("replacement"))
			return err
		}, grpcproc.WithLabel("watcher"))
		<-done
		h.mu.Lock()
		defer h.mu.Unlock()
		if i, j := slices.Index(h.log, "exit victim"), slices.Index(h.log, "spawn replacement"); i < 0 || j < 0 || i > j {
			t.Fatal(h.log)
		}
	})
}
