package golink_test

import (
	"errors"
	"testing"
	"time"

	"google.golang.org/protobuf/proto"

	"github.com/floatdrop/golink"
	"github.com/floatdrop/golink/golinktest"
	"github.com/floatdrop/golink/internal/testpb"
)

// parent runs fn as a process and hands back the process, then forwards
// everything it receives.
func parent(t *testing.T, n *golink.Node) (*golink.Process[proto.Message], <-chan golink.Msg[proto.Message]) {
	t.Helper()
	return watcher(t, n)
}

func TestSpawnMonitorSeesInstantExit(t *testing.T) {
	c := golinktest.New(t, "a")
	a := c.Node("a")
	w, ch := parent(t, a)
	// The child is gone before SpawnMonitor even returns, and still the
	// Down carries its real reason, never noproc.
	for range 20 {
		child, ref, err := w.SpawnMonitor[*testpb.Ping](func(*golink.Process[*testpb.Ping]) error {
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
}

func TestSpawnMonitorFailures(t *testing.T) {
	c := golinktest.New(t, "a")
	a := c.Node("a")
	w, _ := parent(t, a)
	_, _ = golink.Spawn(a, echo, golink.WithName("taken"))
	if _, _, err := w.SpawnMonitor[*testpb.Ping](echo, golink.WithName("taken")); !errors.Is(err, golink.ErrNameTaken) {
		t.Fatalf("got %v", err)
	}
	if info, _ := a.Process(w.PID()); info.Monitors != 0 {
		t.Fatalf("a failed spawn left a monitor: %d", info.Monitors)
	}
	// A watcher that has exited cannot monitor anything.
	gone := make(chan *golink.Process[proto.Message], 1)
	_, _ = golink.Spawn[proto.Message](a, func(p *golink.Process[proto.Message]) error { gone <- p; return nil })
	dead := <-gone
	time.Sleep(20 * time.Millisecond)
	if _, _, err := dead.SpawnMonitor[*testpb.Ping](echo); !errors.Is(err, golink.ErrNoProc) {
		t.Fatalf("got %v", err)
	}
	c.Stop("a")
	if _, _, err := w.SpawnMonitor[*testpb.Ping](echo); err == nil {
		t.Fatal("spawn on a stopped node")
	}
}

func TestSendAfter(t *testing.T) {
	c := golinktest.New(t, "a")
	a := c.Node("a")
	sink, got := collector(t, a)
	type result struct {
		stopped, stoppedLate bool
		late                 *golink.Timer
	}
	res := make(chan result, 1)
	p, _ := golink.Spawn[*testpb.Ping](a, func(p *golink.Process[*testpb.Ping]) error {
		if _, err := p.Receive(); err != nil { // carries tenant=first
			return err
		}
		fired := p.SendAfter(10*time.Millisecond, sink, proto.Message(&testpb.Ping{N: 1}))
		cancelled := p.SendAfter(10*time.Millisecond, sink, proto.Message(&testpb.Ping{N: 2}))
		pending := p.SendAfter(time.Hour, sink, proto.Message(&testpb.Ping{N: 3}))
		var r result
		r.stopped = cancelled.Stop()
		if _, err := p.Receive(); err != nil { // tenant=second, while the timer is pending
			return err
		}
		time.Sleep(30 * time.Millisecond)
		r.stoppedLate = fired.Stop()
		r.late = pending
		res <- r
		_, err := p.Receive() // until told to exit
		return err
	})
	_ = a.SendContext(golink.WithMetadata(t.Context(), golink.Metadata{"tenant": "first"}), p, &testpb.Ping{})
	_ = a.SendContext(golink.WithMetadata(t.Context(), golink.Metadata{"tenant": "second"}), p, &testpb.Ping{})
	m := recv(t, got)
	if m.Body.(*testpb.Ping).GetN() != 1 || m.From != p.PID() {
		t.Fatalf("got %+v", m)
	}
	// Scheduled while handling "first": fired while handling "second".
	if m.Metadata["tenant"] != "first" {
		t.Fatalf("timer carried %v", m.Metadata)
	}
	r := <-res
	if !r.stopped || r.stoppedLate {
		t.Fatalf("Stop: before firing %v, after firing %v", r.stopped, r.stoppedLate)
	}
	// The process exits: its pending timer goes with it.
	_ = a.Exit(p, golink.ReasonKilled)
	time.Sleep(20 * time.Millisecond)
	if r.late.Stop() {
		t.Fatal("timer of an exited process still pending")
	}
	select {
	case m := <-got:
		t.Fatalf("unexpected %+v", m)
	case <-time.After(50 * time.Millisecond):
	}
}

func TestSendAfterFromExitedProcess(t *testing.T) {
	c := golinktest.New(t, "a")
	a := c.Node("a")
	gone := make(chan *golink.Process[*testpb.Ping], 1)
	_, _ = golink.Spawn[*testpb.Ping](a, func(p *golink.Process[*testpb.Ping]) error { gone <- p; return nil })
	p := <-gone
	time.Sleep(20 * time.Millisecond)
	if p.SendAfter(time.Millisecond, p.Addr(), &testpb.Ping{}).Stop() {
		t.Fatal("an exited process scheduled a timer")
	}
}
