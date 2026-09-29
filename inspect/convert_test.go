package inspect_test

import (
	"log/slog"
	"testing"
	"testing/synctest"
	"time"

	"github.com/floatdrop/grpcproc"
	"github.com/floatdrop/grpcproc/inspect"
	inspectv1 "github.com/floatdrop/grpcproc/proto/grpcproc/inspect/v1"
)

func TestEventConversions(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		now := time.Now()
		pid := grpcproc.PID{Node: "a", Incarnation: 1, ID: 2}
		info := grpcproc.ProcessInfo{PID: pid, Label: "l", StartedAt: now, LogLevel: slog.LevelWarn, Links: 2, TrapExit: true}
		events := []grpcproc.Event{
			{Kind: grpcproc.EventSpawn, Process: info},
			{Kind: grpcproc.EventExit, Process: info, Reason: "boom"},
			{Kind: grpcproc.EventLinkUp, Peer: grpcproc.NodeID{Name: "b", Incarnation: 3}},
			{Kind: grpcproc.EventLinkDown, Peer: grpcproc.NodeID{Name: "b"}, Err: "eof"},
			{Kind: grpcproc.EventDeadLetter, From: pid, To: pid, Type: "x.Y", Reason: "type"},
		}
		for _, ev := range events {
			ev.Time, ev.Missed = now, 3
			got := inspect.Event(inspect.EventToProto(ev))
			if got.Kind != ev.Kind || got.Reason != ev.Reason || got.Peer != ev.Peer || got.Err != ev.Err ||
				got.From != ev.From || got.Type != ev.Type || got.Missed != 3 || !got.Time.Equal(now) ||
				got.Process.PID != ev.Process.PID || got.Process.LogLevel != ev.Process.LogLevel || !got.Process.StartedAt.Equal(ev.Process.StartedAt) ||
				got.Process.Links != ev.Process.Links || got.Process.TrapExit != ev.Process.TrapExit {
				t.Errorf("%v: got %+v", ev.Kind, got)
			}
		}
		// A wire state no state has, unspecified or from a newer node, becomes
		// 255 rather than wrap around into a real one.
		for _, wire := range []int32{0, 257, -1} {
			p := inspect.ProcessInfo(&inspectv1.ProcessInfo{State: inspectv1.ProcessState(wire)})
			n := inspect.NodeInfo(&inspectv1.NodeInfo{Links: []*inspectv1.Link{{State: inspectv1.LinkState(wire)}}})
			if p.State != 255 || n.Links[0].State != 255 {
				t.Errorf("wire state %d: process %v, link %v", wire, p.State, n.Links[0].State)
			}
		}
		if p := inspect.ProcessInfo(&inspectv1.ProcessInfo{State: inspectv1.ProcessState_PROCESS_STATE_EXITING}); p.State != grpcproc.StateExiting {
			t.Errorf("exiting became %v", p.State)
		}
		// A zero time goes as an absent timestamp, and an absent one comes back
		// zero, in every message: a process's start and an event's time used to
		// come back as the Unix epoch.
		if e := inspect.EventToProto(grpcproc.Event{Kind: grpcproc.EventSpawn}); e.GetTime() != nil || e.GetSpawned().GetStartedAt() != nil {
			t.Errorf("%v", e)
		}
		if p := inspect.ProcessInfo(&inspectv1.ProcessInfo{}); !p.StartedAt.IsZero() || !p.Parent.IsZero() {
			t.Errorf("%+v", p)
		}
		if e := inspect.Event(&inspectv1.Event{}); !e.Time.IsZero() {
			t.Errorf("%+v", e)
		}
		// Zero times stay zero across the wire, and set ones cross it.
		if n := inspect.NodeInfo(inspect.NodeInfoToProto(grpcproc.NodeInfo{Links: []grpcproc.LinkInfo{{}}})); !n.StartedAt.IsZero() || !n.Links[0].EstablishedAt.IsZero() || !n.Links[0].RetryAt.IsZero() {
			t.Fatalf("%+v", n)
		}
		down := grpcproc.LinkInfo{Peer: grpcproc.NodeID{Name: "b"}, Outbound: true, State: grpcproc.LinkDown, LastError: "refused", RetryAt: now, Queued: 5, QueuedBytes: 7}
		if n := inspect.NodeInfo(inspect.NodeInfoToProto(grpcproc.NodeInfo{Links: []grpcproc.LinkInfo{down}})); !n.Links[0].RetryAt.Equal(now) || n.Links[0].State != grpcproc.LinkDown || n.Links[0].Queued != 5 || n.Links[0].QueuedBytes != 7 {
			t.Fatalf("%+v", n.Links[0])
		}
	})
}
