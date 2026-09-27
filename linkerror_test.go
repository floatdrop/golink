package grpcproc_test

import (
	"context"
	"errors"
	"testing"

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
		_, err := a.Call[*testpb.Ping](t.Context(), silent, &testpb.Ping{})
		failed <- err
	}()
	<-called
	c.Partition("a", "b")
	le, ok := errors.AsType[*grpcproc.LinkError](<-failed)
	if !ok || le.Unsent || !errors.Is(le, grpcproc.ErrNoConnection) {
		t.Fatalf("a call the link broke under: %v", le)
	}

	_, err := a.Call[*testpb.Ping](t.Context(), silent, &testpb.Ping{})
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
			_ = p.Reply(m, m.Body, nil)
		}
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := a.Call[*testpb.Ping](t.Context(), echo, &testpb.Ping{}); err != nil {
		t.Fatal(err) // the link is up
	}
	<-got
	done, cancel := context.WithCancel(t.Context())
	cancel()
	if _, err := a.Call[*testpb.Ping](done, echo, &testpb.Ping{}); !errors.Is(err, context.Canceled) {
		t.Fatalf("got %v", err)
	}
	// Anything sent would arrive before this call's own message.
	if _, err := a.Call[*testpb.Ping](t.Context(), echo, &testpb.Ping{}); err != nil {
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
		_, err := a.Call[*testpb.Ping](context.Background(), grpcproc.Named[*testpb.Ping]("b", "silent"), &testpb.Ping{})
		failed <- err
	}()
	<-called
	c.Stop("a")
	if err := <-failed; !errors.Is(err, grpcproc.ErrNodeStopped) {
		t.Fatalf("got %v", err)
	}
}
