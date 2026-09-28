package grpcproc_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/floatdrop/grpcproc"
	"github.com/floatdrop/grpcproc/grpcproctest"
	"github.com/floatdrop/grpcproc/internal/testpb"
)

// A call's LinkError is Unsent when the message never left the node: the
// peer could not be reached. When the link broke under a call waiting for
// its reply, the peer may have handled it, and the error says nothing more.
func TestLinkErrorUnsent(t *testing.T) {
	c := grpcproctest.New(t, "a", "b")
	a := c.Node("a")
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
	silent := grpcproc.Named[*testpb.Ping]("b", "silent")

	failed := make(chan error, 1)
	go func() {
		_, err := silent.Call[*testpb.Ping](t.Context(), a, &testpb.Ping{})
		failed <- err
	}()
	<-called
	c.Partition("a", "b")
	le, ok := errors.AsType[*grpcproc.LinkError](<-failed)
	if !ok || le.Unsent || !errors.Is(le, grpcproc.ErrNoConnection) {
		t.Fatalf("a call the link broke under: %v", le)
	}

	_, err := silent.Call[*testpb.Ping](t.Context(), a, &testpb.Ping{})
	le, ok = errors.AsType[*grpcproc.LinkError](err)
	if !ok || !le.Unsent || !errors.Is(err, grpcproc.ErrNoConnection) {
		t.Fatalf("a call that never left: %v", err)
	}
}

// A call whose ctx is already done sends nothing, even over a live link.
func TestCallWithADoneContextSendsNothing(t *testing.T) {
	c := grpcproctest.New(t, "a", "b")
	a := c.Node("a")
	got := make(chan struct{}, 1)
	echo, err := c.Node("b").Spawn(func(p *grpcproc.Process[*testpb.Ping]) error {
		for {
			m, err := p.Receive()
			if err != nil {
				return err
			}
			got <- struct{}{}
			_ = m.Reply(m.Body, nil)
		}
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := echo.Call[*testpb.Ping](t.Context(), a, &testpb.Ping{}); err != nil {
		t.Fatal(err) // the link is up
	}
	<-got
	done, cancel := context.WithCancel(t.Context())
	cancel()
	if _, err := echo.Call[*testpb.Ping](done, a, &testpb.Ping{}); !errors.Is(err, context.Canceled) {
		t.Fatalf("got %v", err)
	}
	// Anything sent would arrive before this call's own message.
	if _, err := echo.Call[*testpb.Ping](t.Context(), a, &testpb.Ping{}); err != nil {
		t.Fatal(err)
	}
	<-got
	select {
	case <-got:
		t.Fatal("the call with a done ctx was sent")
	default:
	}
}

// A call still waiting on a peer when its node stops fails, rather than wait
// for an answer that no link will bring.
func TestStopFailsCallsWaitingOnPeers(t *testing.T) {
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
}

// So does a call still waiting on a process of the node that outlives Stop,
// taken or still queued. A call made after Stop goes on as before: a process
// gone is ErrNoProc, and one still running may answer it.
func TestStopFailsLocalCallsStillWaiting(t *testing.T) {
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
	for deadline := time.Now().Add(5 * time.Second); ; time.Sleep(time.Millisecond) {
		if info, _ := n.Process(stuck.PID()); info.Mailbox.Depth == 1 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("the second call was never queued")
		}
	}
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
}

// A process can hand a call to another to answer, on its node or another:
// the answer leaves from the node that took the call.
func TestReplyThroughAnotherProcess(t *testing.T) {
	c := grpcproctest.New(t, "a", "b")
	for _, answerer := range []string{"a", "b"} {
		for _, caller := range []string{"a", "b"} {
			jobs := make(chan grpcproc.Msg[*testpb.Ping], 1)
			front, err := c.Node("a").Spawn(func(p *grpcproc.Process[*testpb.Ping]) error {
				m, err := p.Receive()
				if err != nil {
					return err
				}
				jobs <- m
				_, err = p.Receive() // stays up: nothing answers for it on exit
				return err
			})
			if err != nil {
				t.Fatal(err)
			}
			if _, err := c.Node(answerer).Spawn(func(p *grpcproc.Process[*testpb.Ping]) error {
				return (<-jobs).Reply(&testpb.Ping{N: 7}, nil)
			}); err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
			resp, err := front.Call[*testpb.Ping](ctx, c.Node(caller), &testpb.Ping{N: 1})
			cancel()
			if err != nil || resp.GetN() != 7 {
				t.Fatalf("taken on a, answered on %s, called from %s: %v, %v", answerer, caller, resp, err)
			}
		}
	}
}
