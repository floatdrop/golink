package grpcproc_test

import (
	"context"
	"errors"
	"strconv"
	"strings"
	"testing"
	"testing/synctest"
	"time"

	"google.golang.org/protobuf/proto"

	"github.com/floatdrop/grpcproc"
	"github.com/floatdrop/grpcproc/grpcproctest"
	"github.com/floatdrop/grpcproc/internal/testpb"
)

func TestInspectAndInfo(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		c := grpcproctest.New(t, "a")
		a := c.Node("a")
		type state struct{ handled int }
		s := &state{}
		release := make(chan struct{})
		e, _ := a.Spawn[*testpb.Ping](func(p *grpcproc.Process[*testpb.Ping]) error {
			for {
				m, err := p.Receive()
				if err != nil {
					return err
				}
				s.handled++
				if m.Body.N == 7 {
					<-release // simulate a long handler
				}
			}
		}, grpcproc.WithName("insp"), grpcproc.WithLabel("order"), grpcproc.WithInspect(func() map[string]string {
			return map[string]string{"handled": strconv.Itoa(s.handled)}
		}))
		for range 3 {
			_ = a.SendTo(t.Context(), e, &testpb.Ping{N: 1})
		}
		time.Sleep(20 * time.Millisecond)
		got, err := a.Inspect(ctx(t), e.PID())
		if err != nil || got["handled"] != "3" {
			t.Fatalf("inspect: %v %v", got, err)
		}
		info, _ := a.Process(e.PID())
		if info.Label != "order" || info.Name != "insp" || info.Received != 3 || info.State != grpcproc.StateIdle ||
			info.LastMessage != "grpcproc.test.v1.Ping" || info.Type != "*testpb.Ping" {
			t.Fatalf("info %+v", info)
		}
		// Busy process: inspect times out with a reason, and the mailbox shows the backlog.
		_ = a.SendTo(t.Context(), e, &testpb.Ping{N: 7})
		_ = a.SendTo(t.Context(), e, &testpb.Ping{N: 1})
		time.Sleep(20 * time.Millisecond)
		short, cancel := context.WithTimeout(t.Context(), 50*time.Millisecond)
		defer cancel()
		if _, err := a.Inspect(short, e.PID()); err == nil || !strings.Contains(err.Error(), "busy") {
			t.Fatalf("want busy error, got %v", err)
		}
		info, _ = a.Process(e.PID())
		if info.State != grpcproc.StateRunning || info.Mailbox.Depth != 1 || info.Mailbox.OldestAge <= 0 {
			t.Fatalf("busy info %+v", info)
		}
		close(release)
		ni := a.Info()
		if ni.ID.Name != "a" || ni.Processes != 1 || ni.Spawned != 1 {
			t.Fatalf("node info %+v", ni)
		}
		if all := a.Processes(); len(all) != 1 || all[0].PID != e.PID() {
			t.Fatalf("processes %+v", all)
		}
	})
}

func TestInspectWhileBacklogged(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		c := grpcproctest.New(t, "a")
		a := c.Node("a")
		release := make(chan struct{})
		entered := make(chan struct{}, 1)
		handled := 0
		e, _ := a.Spawn[*testpb.Ping](func(p *grpcproc.Process[*testpb.Ping]) error {
			for {
				m, err := p.Receive()
				if err != nil {
					return err
				}
				handled++
				if m.Body.N == 7 {
					entered <- struct{}{}
					<-release
				}
			}
		}, grpcproc.WithInspect(func() map[string]string { return map[string]string{"n": "x"} }))
		_ = a.SendTo(t.Context(), e, &testpb.Ping{N: 7})
		<-entered
		_ = a.SendTo(t.Context(), e, &testpb.Ping{N: 1}) // queued behind the busy handler
		got := make(chan error, 1)
		go func() { _, err := a.Inspect(ctx(t), e.PID()); got <- err }()
		time.Sleep(20 * time.Millisecond)
		close(release) // the pending inspect is served before the queued message
		if err := <-got; err != nil {
			t.Fatal(err)
		}
		// The inspect function itself blocking: the caller's ctx bounds the wait.
		blockInspect := make(chan struct{})
		e2, _ := a.Spawn[*testpb.Ping](func(p *grpcproc.Process[*testpb.Ping]) error {
			_, err := p.Receive()
			return err
		}, grpcproc.WithInspect(func() map[string]string { <-blockInspect; return nil }))
		short, cancel := context.WithTimeout(t.Context(), 30*time.Millisecond)
		defer cancel()
		if _, err := a.Inspect(short, e2.PID()); !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("got %v", err)
		}
		close(blockInspect)
	})
}

func TestBusyMeasuresTheCurrentMessage(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		c := grpcproctest.New(t, "a")
		a := c.Node("a")
		release := make(chan struct{})
		defer close(release)
		p, _ := a.Spawn[*testpb.Ping](func(p *grpcproc.Process[*testpb.Ping]) error {
			for {
				m, err := p.Receive()
				if err != nil {
					return err
				}
				if m.Body.GetN() == 1 {
					<-release
				}
			}
		})
		time.Sleep(300 * time.Millisecond) // old, but idle
		_ = p.Send(t.Context(), a, &testpb.Ping{N: 1})
		time.Sleep(20 * time.Millisecond)
		short, cancel := context.WithTimeout(t.Context(), 10*time.Millisecond)
		defer cancel()
		_, err := a.Inspect(short, p.PID())
		if d := busyFor(t, err); d > 200*time.Millisecond {
			t.Fatalf("busy must count from the message, not the start: %v", err)
		}
		// A process that never took a message counts from its start.
		stuck, _ := a.Spawn[*testpb.Ping](func(*grpcproc.Process[*testpb.Ping]) error { <-release; return nil })
		time.Sleep(50 * time.Millisecond)
		short2, cancel2 := context.WithTimeout(t.Context(), 10*time.Millisecond)
		defer cancel2()
		_, err = a.Inspect(short2, stuck.PID())
		if d := busyFor(t, err); d < 50*time.Millisecond {
			t.Fatalf("busy since start: %v", err)
		}
		// Untyped processes are named proto.Message, not by the alias's target.
		u, _ := a.Spawn[proto.Message](func(p *grpcproc.Process[proto.Message]) error { _, err := p.Receive(); return err })
		if info, _ := a.Process(u.PID()); info.Type != "proto.Message" || info.Label != "proto.Message" {
			t.Fatalf("%+v", info)
		}
	})
}

func busyFor(t *testing.T, err error) time.Duration {
	t.Helper()
	if err == nil {
		t.Fatal("inspect of a busy process answered")
	}
	_, rest, ok := strings.Cut(err.Error(), "busy for ")
	d, perr := time.ParseDuration(strings.SplitN(rest, ":", 2)[0])
	if !ok || perr != nil {
		t.Fatalf("no duration in %v", err)
	}
	return d
}

// An inspect function that panics ends its process, as any panic in it
// does, and Inspect says so at once rather than wait on an answer that will
// not come.
func TestInspectOfAProcessThatPanicsAnswering(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		a := grpcproctest.New(t, "a").Node("a")
		events := a.Subscribe(t.Context(), 16)
		addr, err := a.Spawn(func(p *grpcproc.Process[proto.Message]) error {
			_, err := p.Receive()
			return err
		}, grpcproc.WithInspect(func() map[string]string { panic("inspect") }))
		if err != nil {
			t.Fatal(err)
		}
		if _, err := a.Inspect(ctx(t), addr.PID()); !errors.Is(err, grpcproc.ErrNoProc) || !strings.Contains(err.Error(), "inspect function panicked") {
			t.Fatalf("got %v", err)
		}
		for e := range events {
			if e.Kind == grpcproc.EventExit && e.Process.PID == addr.PID() {
				if e.Reason != "panic: inspect" {
					t.Fatalf("exited with %q", e.Reason)
				}
				return
			}
		}
	})
}

func TestOrderingOfSnapshots(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		c := grpcproctest.New(t, "a", "b", "c")
		a := c.Node("a")
		e1, _ := a.Spawn(echo)
		e2, _ := a.Spawn(echo)
		if all := a.Processes(); len(all) != 2 || all[0].PID != e1.PID() || all[1].PID != e2.PID() {
			t.Fatalf("%+v", all)
		}
		for _, peer := range []string{"c", "b"} {
			e, _ := c.Node(peer).Spawn(echo)
			if _, err := a.CallTo[*testpb.Pong](ctx(t), e, &testpb.Ping{N: 1}); err != nil {
				t.Fatal(err)
			}
		}
		links := a.Info().Links
		if len(links) != 4 || links[0].Peer.Name != "b" || !links[0].Outbound || links[1].Outbound || links[2].Peer.Name != "c" {
			t.Fatalf("%+v", links)
		}
		if peers := a.Peers(); len(peers) != 2 || peers[0] != "b" {
			t.Fatalf("%v", peers)
		}
		// A local monitor, demonitored.
		w, ch := watcher(t, a)
		ref := w.Monitor(e1)
		if info, _ := a.Process(e1.PID()); info.Watchers != 1 {
			t.Fatalf("%+v", info)
		}
		w.Demonitor(ref)
		if info, _ := a.Process(e1.PID()); info.Watchers != 0 {
			t.Fatalf("%+v", info)
		}
		_ = a.SendTo(t.Context(), e1, &testpb.Ping{N: 0})
		select {
		case m := <-ch:
			t.Fatalf("unexpected %+v", m)
		case <-time.After(100 * time.Millisecond):
		}
	})
}
