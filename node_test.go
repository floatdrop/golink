package grpcproc_test

import (
	"context"
	"errors"
	"strings"
	"testing"
	"testing/synctest"
	"time"

	"github.com/floatdrop/grpcproc"
	"github.com/floatdrop/grpcproc/grpcproctest"
	"github.com/floatdrop/grpcproc/internal/testpb"
)

func TestNewNodeValidation(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		if _, err := grpcproc.NewNode(grpcproc.Config{}); err == nil {
			t.Fatal("name required")
		}
		if _, err := grpcproc.NewNode(grpcproc.Config{Name: "a"}); err == nil {
			t.Fatal("resolver required")
		}
		n, err := grpcproc.NewNode(grpcproc.Config{Name: "a", Resolver: grpcproc.StaticResolver{}})
		if err != nil {
			t.Fatal(err)
		}
		if n.Name() != "a" || n.ID().Incarnation == 0 || n.PID().Node != "a" || !n.Info().StartedAt.IsZero() {
			t.Fatalf("defaults: %+v", n.Info())
		}
		// Unknown peer: the resolver refuses, the error is a connection error.
		if err := n.SendTo(t.Context(), grpcproc.Named[*testpb.Ping]("nowhere", "x"), &testpb.Ping{}); !errors.Is(err, grpcproc.ErrNoConnection) {
			t.Fatalf("got %v", err)
		}
		if err := n.SendTo(t.Context(), grpcproc.PID{}, &testpb.Ping{}); err == nil {
			t.Fatal("empty node must be an error")
		}
		if err := n.Stop(context.Background()); err != nil {
			t.Fatal(err)
		}
		if err := n.Stop(context.Background()); err != nil {
			t.Fatal("second stop")
		}
		if _, err := n.Spawn(echo); !errors.Is(err, grpcproc.ErrNodeStopped) {
			t.Fatalf("spawn after stop: %v", err)
		}
		if err := n.SendTo(t.Context(), grpcproc.Named[*testpb.Ping]("b", "x"), &testpb.Ping{}); !errors.Is(err, grpcproc.ErrNodeStopped) {
			t.Fatalf("send after stop: %v", err)
		}
	})
}

func TestGracefulStopSendsShutdown(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		c := grpcproctest.New(t, "a", "b")
		a, b := c.Node("a"), c.Node("b")
		w, ch := watcher(t, a)
		e, _ := b.Spawn(echo)
		w.Monitor(e)
		if _, err := e.Call[*testpb.Pong](ctx(t), w, &testpb.Ping{N: 1}); err != nil {
			t.Fatal(err)
		}
		c.Stop("b")
		if m := recv(t, ch); m.Down == nil || m.Down.Reason != grpcproc.ReasonShutdown {
			t.Fatalf("got %+v", m.Down)
		}
	})
}

func TestStopTimesOut(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		n, _ := grpcproc.NewNode(grpcproc.Config{Name: "a", Resolver: grpcproc.StaticResolver{}})
		block := make(chan struct{})
		_, _ = n.Spawn[*testpb.Ping](func(p *grpcproc.Process[*testpb.Ping]) error { <-block; return nil })
		ctx, cancel := context.WithCancel(t.Context())
		cancel()
		if err := n.Stop(ctx); err == nil || !strings.Contains(err.Error(), "stop") {
			t.Fatalf("got %v", err)
		}
		close(block)
	})
}

// dialStuck sends from n to a peer whose dial is in progress, and gives up
// waiting for it once the resolver has been entered.
func dialStuck(t *testing.T, n *grpcproc.Node, entered <-chan struct{}) {
	t.Helper()
	ctx, cancel := context.WithCancel(t.Context())
	go func() { <-entered; cancel() }()
	if err := n.SendTo(ctx, grpcproc.Named[*testpb.Ping]("b", "x"), &testpb.Ping{}); !errors.Is(err, context.Canceled) {
		t.Fatalf("got %v", err)
	}
}

func TestStopWaitsForDials(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		// A peer that never resolves: the dial ends at its timeout, and Stop
		// returns only after that.
		entered, ended := make(chan struct{}), make(chan struct{})
		n, err := grpcproc.NewNode(grpcproc.Config{Name: "a", DialTimeout: 50 * time.Millisecond,
			Resolver: grpcproc.ResolverFunc(func(ctx context.Context, _ string) (string, error) {
				close(entered)
				<-ctx.Done()
				close(ended)
				return "", ctx.Err()
			})})
		if err != nil {
			t.Fatal(err)
		}
		dialStuck(t, n, entered)
		if err := n.Stop(t.Context()); err != nil {
			t.Fatal(err)
		}
		select {
		case <-ended:
		default:
			t.Fatal("Stop returned with a dial in flight")
		}
	})
}

func TestStopGivesUpOnADialThatWillNotEnd(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		// A resolver that ignores its ctx: Stop waits for it only as long as its
		// own ctx allows.
		entered, release := make(chan struct{}), make(chan struct{})
		n, err := grpcproc.NewNode(grpcproc.Config{Name: "a", Resolver: grpcproc.ResolverFunc(func(context.Context, string) (string, error) {
			close(entered)
			<-release
			return "", errors.New("gone")
		})})
		if err != nil {
			t.Fatal(err)
		}
		defer close(release)
		dialStuck(t, n, entered)
		stop, cancel := context.WithTimeout(t.Context(), 50*time.Millisecond)
		defer cancel()
		if err := n.Stop(stop); !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("got %v", err)
		}
	})
}

// A call still waiting on a peer when its node stops fails, rather than wait
// for an answer that no link will bring.
func TestStopFailsCallsWaitingOnPeers(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		c := grpcproctest.New(t, "a", "b")
		called := make(chan struct{})
		if _, err := c.Node("b").Spawn(func(p *grpcproc.Process[*testpb.Ping]) error {
			if _, err := p.Receive(); err != nil {
				return err
			}
			close(called)
			_, err := p.Receive() // never answers
			return err
		}, grpcproc.WithName("silent")); err != nil {
			t.Fatal(err)
		}
		a := c.Node("a")
		failed := make(chan error, 1)
		go func() {
			_, err := grpcproc.Named[*testpb.Ping]("b", "silent").Call[*testpb.Ping](context.Background(), a, &testpb.Ping{})
			failed <- err
		}()
		<-called
		c.Stop("a")
		if err := <-failed; !errors.Is(err, grpcproc.ErrNodeStopped) {
			t.Fatalf("got %v", err)
		}
	})
}

// So does a call still waiting on a process of the node that outlives Stop,
// taken or still queued. A call made after Stop goes on as before: a process
// gone is ErrNoProc, and one still running may answer it.
func TestStopFailsLocalCallsStillWaiting(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		n, err := grpcproc.NewNode(grpcproc.Config{Name: "a", Resolver: grpcproc.StaticResolver{}})
		if err != nil {
			t.Fatal(err)
		}
		called, release := make(chan struct{}), make(chan struct{})
		defer close(release)
		stuck, err := n.Spawn(func(p *grpcproc.Process[*testpb.Ping]) error {
			if _, err := p.Receive(); err != nil {
				return err
			}
			close(called)
			<-release // ignores Stop
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}
		// Receive still returns what is queued once the process is told to exit.
		lingers, err := n.Spawn(func(p *grpcproc.Process[*testpb.Ping]) error {
			for {
				m, err := p.Receive()
				if err != nil {
					select {
					case <-release:
						return nil
					case <-time.After(time.Millisecond):
					}
					continue
				}
				_ = m.Reply(m.Body, nil)
			}
		})
		if err != nil {
			t.Fatal(err)
		}
		failed := make(chan error, 2)
		call := func() {
			_, err := stuck.Call[*testpb.Ping](context.Background(), n, &testpb.Ping{})
			failed <- err
		}
		go call()
		<-called
		go call() // stays queued
		eventually(t, "the second call to be queued", func() bool {
			info, _ := n.Process(stuck.PID())
			return info.Mailbox.Depth == 1
		})
		ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
		defer cancel()
		if err := n.Stop(ctx); !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("stop: %v", err)
		}
		for range 2 { // taken, and queued
			if err := <-failed; !errors.Is(err, grpcproc.ErrNodeStopped) {
				t.Fatalf("got %v", err)
			}
		}
		if _, err := grpcproc.Named[*testpb.Ping]("a", "nobody").Call[*testpb.Ping](context.Background(), n, &testpb.Ping{}); !errors.Is(err, grpcproc.ErrNoProc) {
			t.Fatalf("after stop, no process: %v", err)
		}
		if resp, err := lingers.Call[*testpb.Ping](context.Background(), n, &testpb.Ping{N: 42}); err != nil || resp.GetN() != 42 {
			t.Fatalf("after stop, a process still running: %v, %v", resp, err)
		}
	})
}
