package golink_test

import (
	"context"
	"testing"
	"time"

	"google.golang.org/protobuf/proto"

	"github.com/floatdrop/golink"
	"github.com/floatdrop/golink/golinktest"
	"github.com/floatdrop/golink/internal/testpb"
)

func nextEvent(t *testing.T, ch <-chan golink.Event, kind golink.EventKind) golink.Event {
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
	c := golinktest.New(t, "a", "b")
	a := c.Node("a")
	events := a.Subscribe(t.Context(), 64)

	e, _ := golink.Spawn(a, echo, golink.WithLabel("echo"))
	if ev := nextEvent(t, events, golink.EventSpawn); ev.Process.PID != e.PID() || ev.Process.Label != "echo" || ev.Time.IsZero() {
		t.Fatalf("%+v", ev)
	}
	_ = a.SendTo(e.PID(), &testpb.Pong{})
	if ev := nextEvent(t, events, golink.EventDeadLetter); ev.To != e.PID() || ev.Type != "golink.test.v1.Pong" || ev.Reason != golink.ReasonType {
		t.Fatalf("%+v", ev)
	}
	_ = a.Send(e, &testpb.Ping{N: -100})
	if ev := nextEvent(t, events, golink.EventExit); ev.Process.PID != e.PID() || ev.Reason != "boom" {
		t.Fatalf("%+v", ev)
	}
	e2, _ := golink.Spawn(c.Node("b"), echo)
	if _, err := a.Call[*testpb.Pong](t.Context(), e2, &testpb.Ping{N: 1}); err != nil {
		t.Fatal(err)
	}
	if ev := nextEvent(t, events, golink.EventLinkUp); ev.Peer.Name != "b" {
		t.Fatalf("%+v", ev)
	}
	c.Kill("b")
	if ev := nextEvent(t, events, golink.EventLinkDown); ev.Peer.Name != "b" || ev.Err == "" {
		t.Fatalf("%+v", ev)
	}
	for _, k := range []golink.EventKind{golink.EventSpawn, golink.EventExit, golink.EventLinkUp, golink.EventLinkDown, golink.EventDeadLetter} {
		if k.String() == "" {
			t.Fatal("kind string")
		}
	}
}

func TestSubscribeMissedAndClose(t *testing.T) {
	c := golinktest.New(t, "a")
	a := c.Node("a")
	ctx, cancel := context.WithCancel(t.Context())
	events := a.Subscribe(ctx, 1)
	quiet := a.Subscribe(t.Context(), 0) // buffer rounds up to 1
	for range 5 {
		_ = a.SendTo(golink.PID{Node: "a"}, proto.Message(&testpb.Ping{}))
	}
	<-events // the first one fitted
	_ = a.SendTo(golink.PID{Node: "a"}, &testpb.Ping{})
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
}

func TestLinkDownWithoutError(t *testing.T) {
	// A peer that closes its side cleanly still produces a link-down event.
	c := golinktest.New(t, "a", "b")
	a := c.Node("a")
	events := a.Subscribe(t.Context(), 64)
	e, _ := golink.Spawn(c.Node("b"), echo)
	if _, err := a.Call[*testpb.Pong](t.Context(), e, &testpb.Ping{N: 1}); err != nil {
		t.Fatal(err)
	}
	c.Stop("b")
	nextEvent(t, events, golink.EventLinkDown)
}
