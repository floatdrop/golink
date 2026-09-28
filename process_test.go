package grpcproc_test

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"strings"
	"testing"
	"testing/synctest"
	"time"

	"google.golang.org/protobuf/proto"

	"github.com/floatdrop/grpcproc"
	"github.com/floatdrop/grpcproc/grpcproctest"
	"github.com/floatdrop/grpcproc/internal/testpb"
)

func TestProcessAccessorsAndNames(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var buf bytes.Buffer
		log := slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelInfo}))
		c := grpcproctest.NewWith(t, []grpcproctest.Option{grpcproctest.WithLogger(log)}, "a")
		a := c.Node("a")
		ready := make(chan *grpcproc.Process[*testpb.Ping], 1)
		addr, _ := a.Spawn[*testpb.Ping](func(p *grpcproc.Process[*testpb.Ping]) error {
			ready <- p
			_, err := p.Receive()
			return err
		}, grpcproc.WithName("one"))
		p := <-ready
		if p.PID() != addr.PID() || p.Node() != a || p.Addr() != addr || p.Context().Err() != nil {
			t.Fatal("accessors")
		}
		if pid, ok := a.Whereis("one"); !ok || pid != addr.PID() {
			t.Fatal("whereis")
		}
		info, _ := a.Process(addr.PID())
		if !info.Parent.IsZero() || info.Name != "one" || info.LogLevel != slog.LevelInfo {
			t.Fatalf("%+v", info)
		}
		// Per-process log level: the node's handler is at Info, so debug is dropped
		// by default; a process can be made more verbose, or quieter, on its own.
		p.Log().Debug("hidden")
		if err := a.SetLogLevel(addr.PID(), slog.LevelDebug); err != nil {
			t.Fatal(err)
		}
		p.Log().WithGroup("g").Debug("shown", "k", "v")
		if info, _ = a.Process(addr.PID()); info.LogLevel != slog.LevelDebug {
			t.Fatalf("%+v", info)
		}
		if err := a.SetLogLevel(addr.PID(), slog.LevelError); err != nil {
			t.Fatal(err)
		}
		p.Log().Info("dropped by process threshold")
		out := buf.String()
		if strings.Contains(out, "hidden") || !strings.Contains(out, "shown") || strings.Contains(out, "dropped") {
			t.Fatalf("log output:\n%s", out)
		}
		if err := a.SetLogLevel(grpcproc.PID{Node: "a"}, slog.LevelDebug); !errors.Is(err, grpcproc.ErrNoProc) {
			t.Fatal(err)
		}
		if _, ok := a.Process(grpcproc.PID{Node: "zzz"}); ok {
			t.Fatal("foreign pid")
		}
		if _, err := a.Inspect(ctx(t), grpcproc.PID{Node: "a"}); !errors.Is(err, grpcproc.ErrNoProc) {
			t.Fatal(err)
		}
		// No WithInspect: an empty answer, but an answer.
		if m, err := a.Inspect(ctx(t), addr.PID()); err != nil || m != nil {
			t.Fatalf("%v %v", m, err)
		}
		// The exit event is published once the process is gone and its name free.
		events := a.Subscribe(t.Context(), 16)
		if err := a.Exit(t.Context(), addr, grpcproc.ReasonKilled); err != nil {
			t.Fatal(err)
		}
		for e := range events {
			if e.Kind == grpcproc.EventExit && e.Process.PID == addr.PID() {
				break
			}
		}
		if _, ok := a.Whereis("one"); ok {
			t.Fatal("a name outlived its process")
		}
		if _, err := a.Inspect(ctx(t), addr.PID()); !errors.Is(err, grpcproc.ErrNoProc) {
			t.Fatal(err)
		}
		// Monitor from an exited process is a no-op that still returns a ref.
		if r := p.Monitor(addr); r.ID == 0 {
			t.Fatal("ref")
		}
	})
}

func TestRegistry(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		c := grpcproctest.New(t, "a")
		a := c.Node("a")
		e, _ := a.Spawn(echo, grpcproc.WithName("svc"))
		if _, err := a.Spawn(echo, grpcproc.WithName("svc")); !errors.Is(err, grpcproc.ErrNameTaken) {
			t.Fatalf("got %v", err)
		}
		if got, ok := a.Whereis("svc"); !ok || got != e.PID() {
			t.Fatal("whereis")
		}
		w, ch := watcher(t, a)
		w.Monitor(e)
		_ = a.SendTo(t.Context(), grpcproc.Name{Node: "a", Name: "svc"}, &testpb.Ping{N: 0})
		recv(t, ch)
		if _, ok := a.Whereis("svc"); ok {
			t.Fatal("name not released on exit")
		}
	})
}

func TestReceiveTimeoutAndExitError(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		c := grpcproctest.New(t, "a")
		a := c.Node("a")
		w, ch := watcher(t, a)
		results := make(chan error, 2)
		addr, _ := a.Spawn[*testpb.Ping](func(p *grpcproc.Process[*testpb.Ping]) error {
			_, err := p.ReceiveTimeout(10 * time.Millisecond)
			results <- err
			_, err = p.ReceiveTimeout(time.Minute)
			results <- err
			return err
		})
		w.Monitor(addr)
		if err := <-results; !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("timeout: %v", err)
		}
		time.Sleep(10 * time.Millisecond)
		_ = a.Exit(t.Context(), addr, "bye")
		if err := <-results; err == nil {
			t.Fatal("want exit error")
		} else if ee, ok := errors.AsType[*grpcproc.ExitError](err); !ok || ee.Reason != "bye" {
			t.Fatalf("exit: %v", err)
		}
		if m := recv(t, ch); m.Down == nil || m.Down.Reason != "bye" {
			t.Fatalf("%+v", m.Down)
		}
		// A process may also return an ExitError itself.
		addr2, _ := a.Spawn[*testpb.Ping](func(p *grpcproc.Process[*testpb.Ping]) error {
			return &grpcproc.ExitError{Reason: "custom"}
		})
		w.Monitor(addr2)
		if m := recv(t, ch); m.Down == nil || m.Down.Reason != "custom" && m.Down.Reason != grpcproc.ReasonNoProc {
			t.Fatalf("%+v", m.Down)
		}
	})
}

func TestProcessSendReplyAndMonitorVariants(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		c := grpcproctest.New(t, "a", "b")
		a, b := c.Node("a"), c.Node("b")
		col, ch := collector(t, b)
		e, _ := b.Spawn(echo, grpcproc.WithName("echo"))
		done := make(chan struct{})
		_, _ = a.Spawn[proto.Message](func(p *grpcproc.Process[proto.Message]) error {
			defer close(done)
			if err := col.Send(p.Context(), p, &testpb.Ping{N: 1}); err != nil {
				return err
			}
			// CallTo from a process, to an untyped target.
			if r, err := p.CallTo[*testpb.Pong](t.Context(), grpcproc.Name{Node: "b", Name: "echo"}, &testpb.Ping{N: 1}); err != nil || r.N != 2 {
				t.Errorf("CallTo: %v %v", r, err)
			}
			// Reply to something that is not a call.
			if err := (grpcproc.Msg[proto.Message]{}).Reply(nil, nil); !errors.Is(err, grpcproc.ErrNotCall) {
				t.Errorf("got %v", err)
			}
			// Monitor by name and demonitor it: the by-name branch.
			ref := p.Monitor(grpcproc.Name{Node: "b", Name: "echo"})
			p.Demonitor(ref)
			p.Demonitor(ref) // unknown ref: no-op
			// Monitoring through a node that cannot be reached is an immediate noconnection.
			ref = p.Monitor(grpcproc.Named[*testpb.Ping]("nowhere", "x"))
			m, err := p.Receive()
			if err != nil {
				return err
			}
			if m.Down == nil || m.Down.Ref != ref || m.Down.Reason != grpcproc.ReasonNoConnection {
				t.Errorf("got %+v", m.Down)
			}
			// Exit a remote process by name, from a process.
			return p.Exit(grpcproc.Name{Node: "b", Name: "echo"}, grpcproc.ReasonKilled)
		})
		if m := recv(t, ch); len(m.Metadata) != 0 || !proto.Equal(m.Body, &testpb.Ping{N: 1}) {
			t.Fatalf("%+v", m)
		}
		<-done
		// The echo was told to exit.
		eventually(t, "the echo to exit", func() bool { _, alive := b.Process(e.PID()); return !alive })
		// A process exiting while it monitors others (by name and by pid) cleans up.
		e2, _ := b.Spawn(echo, grpcproc.WithName("echo2"))
		exit := make(chan struct{})
		_, _ = a.Spawn[proto.Message](func(p *grpcproc.Process[proto.Message]) error {
			p.Monitor(e2)
			p.Monitor(grpcproc.Name{Node: "b", Name: "echo2"})
			<-exit
			return nil
		})
		waitWatchers(t, b, e2.PID(), 2)
		close(exit)
		waitWatchers(t, b, e2.PID(), 0)
	})
}

func TestSendAfter(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		c := grpcproctest.New(t, "a")
		a := c.Node("a")
		sink, got := collector(t, a)
		type result struct {
			stopped, stoppedLate bool
			late                 *grpcproc.Timer
		}
		res := make(chan result, 1)
		p, _ := a.Spawn[*testpb.Ping](func(p *grpcproc.Process[*testpb.Ping]) error {
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
			if _, err := p.Receive(); err != nil { // the test has the fired message
				return err
			}
			r.stoppedLate = fired.Stop()
			r.late = pending
			res <- r
			_, err := p.Receive() // until told to exit
			return err
		})
		_ = p.Send(grpcproc.WithMetadata(t.Context(), grpcproc.Metadata{"tenant": "first"}), a, &testpb.Ping{})
		_ = p.Send(grpcproc.WithMetadata(t.Context(), grpcproc.Metadata{"tenant": "second"}), a, &testpb.Ping{})
		m := recv(t, got)
		if m.Body.(*testpb.Ping).GetN() != 1 || m.From != p.PID() {
			t.Fatalf("got %+v", m)
		}
		// Scheduled while handling "first": fired while handling "second".
		if m.Metadata["tenant"] != "first" {
			t.Fatalf("timer carried %v", m.Metadata)
		}
		_ = p.Send(t.Context(), a, &testpb.Ping{})
		r := <-res
		if !r.stopped || r.stoppedLate {
			t.Fatalf("Stop: before firing %v, after firing %v", r.stopped, r.stoppedLate)
		}
		// The process exits: its pending timer goes with it.
		_ = a.Exit(t.Context(), p, grpcproc.ReasonKilled)
		time.Sleep(20 * time.Millisecond)
		if r.late.Stop() {
			t.Fatal("timer of an exited process still pending")
		}
		select {
		case m := <-got:
			t.Fatalf("unexpected %+v", m)
		case <-time.After(50 * time.Millisecond):
		}
	})
}

func TestSendAfterFromExitedProcess(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		c := grpcproctest.New(t, "a")
		a := c.Node("a")
		gone := make(chan *grpcproc.Process[*testpb.Ping], 1)
		_, _ = a.Spawn[*testpb.Ping](func(p *grpcproc.Process[*testpb.Ping]) error { gone <- p; return nil })
		p := <-gone
		time.Sleep(20 * time.Millisecond)
		if p.SendAfter(time.Millisecond, p.Addr(), &testpb.Ping{}).Stop() {
			t.Fatal("an exited process scheduled a timer")
		}
	})
}
