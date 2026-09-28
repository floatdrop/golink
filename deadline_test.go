package grpcproc_test

import (
	"context"
	"errors"
	"testing"
	"testing/synctest"
	"time"

	"github.com/floatdrop/grpcproc"
	"github.com/floatdrop/grpcproc/grpcproctest"
	"github.com/floatdrop/grpcproc/internal/testpb"
)

// answering passes on every message it gets and answers the calls.
func answering(t *testing.T, n *grpcproc.Node) (grpcproc.Addr[*testpb.Ping], <-chan grpcproc.Msg[*testpb.Ping]) {
	t.Helper()
	got := make(chan grpcproc.Msg[*testpb.Ping], 8)
	addr, err := n.Spawn(func(p *grpcproc.Process[*testpb.Ping]) error {
		for {
			m, err := p.Receive()
			if err != nil {
				return err
			}
			got <- m
			if m.IsCall() {
				_ = m.Reply(&testpb.Pong{}, nil)
			}
		}
	})
	if err != nil {
		t.Fatal(err)
	}
	return addr, got
}

func TestCallDeadlineReachesCallee(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		c := grpcproctest.New(t, "a", "b")
		a, b := c.Node("a"), c.Node("b")
		local, gotLocal := answering(t, a)
		remote, gotRemote := answering(t, b)

		d := time.Now().Add(time.Minute)
		ctx, cancel := context.WithDeadline(t.Context(), d)
		defer cancel()

		// Locally the callee sees the caller's own deadline.
		if _, err := local.Call[*testpb.Pong](ctx, a, &testpb.Ping{N: 1}); err != nil {
			t.Fatal(err)
		}
		if got, ok := recv(t, gotLocal).Deadline(); !ok || !got.Equal(d) {
			t.Fatalf("local deadline %v %v, want %v", got, ok, d)
		}

		// Remotely it is the time left, counted from arrival: the caller's
		// deadline, or a little after it.
		if _, err := remote.Call[*testpb.Pong](ctx, a, &testpb.Ping{N: 1}); err != nil {
			t.Fatal(err)
		}
		if got, ok := recv(t, gotRemote).Deadline(); !ok || got.Before(d) || got.Sub(d) > time.Second {
			t.Fatalf("remote deadline %v %v, want from %v", got, ok, d)
		}

		// A deadline too far off for unix nanoseconds, remote or local, is
		// still one in the future, not an overflow into the past.
		far, cancelFar := context.WithDeadline(t.Context(), time.Date(3000, 1, 1, 0, 0, 0, 0, time.UTC))
		defer cancelFar()
		for _, to := range []struct {
			addr grpcproc.Addr[*testpb.Ping]
			got  <-chan grpcproc.Msg[*testpb.Ping]
		}{{local, gotLocal}, {remote, gotRemote}} {
			if _, err := to.addr.Call[*testpb.Pong](far, a, &testpb.Ping{N: 1}); err != nil {
				t.Fatal(err)
			}
			m := recv(t, to.got)
			if got, ok := m.Deadline(); !ok || got.Year() < 2262 {
				t.Fatalf("far deadline became %v %v", got, ok)
			}
			mctx, cancel := m.Context(t.Context())
			if mctx.Err() != nil {
				t.Fatal("a far deadline ended the context at once")
			}
			cancel()
		}

		// A caller with no deadline, and a send, give none.
		for _, to := range []struct {
			addr grpcproc.Addr[*testpb.Ping]
			got  <-chan grpcproc.Msg[*testpb.Ping]
		}{{local, gotLocal}, {remote, gotRemote}} {
			if _, err := to.addr.Call[*testpb.Pong](context.Background(), a, &testpb.Ping{N: 1}); err != nil {
				t.Fatal(err)
			}
			if got, ok := recv(t, to.got).Deadline(); ok {
				t.Fatalf("a call with no deadline has one: %v", got)
			}
			if err := to.addr.Send(ctx, a, &testpb.Ping{N: 1}); err != nil {
				t.Fatal(err)
			}
			m := recv(t, to.got)
			if got, ok := m.Deadline(); ok || m.IsCall() {
				t.Fatalf("a send has a deadline: %v", got)
			}
		}
	})
}

// A callee's work for a caller that gave up can stop: the message's
// context ends when the caller stops waiting, and keeps its metadata.
func TestMsgContextEndsWithTheCaller(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		c := grpcproctest.New(t, "a", "b")
		a, b := c.Node("a"), c.Node("b")
		ended := make(chan error, 1)
		slow, err := b.Spawn(func(p *grpcproc.Process[*testpb.Ping]) error {
			m, err := p.Receive()
			if err != nil {
				return err
			}
			ctx, cancel := m.Context(p.Context())
			defer cancel()
			if grpcproc.MetadataFrom(ctx)["tenant"] != "acme" {
				t.Error("metadata lost")
			}
			<-ctx.Done() // work that outlives its caller's patience
			ended <- ctx.Err()
			return m.Reply(&testpb.Pong{}, nil) // nobody waits for it
		})
		if err != nil {
			t.Fatal(err)
		}
		ctx, cancel := context.WithTimeout(grpcproc.WithMetadata(t.Context(), grpcproc.Metadata{"tenant": "acme"}), 50*time.Millisecond)
		defer cancel()
		if _, err := slow.Call[*testpb.Pong](ctx, a, &testpb.Ping{N: 1}); !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("want the caller's deadline, got %v", err)
		}
		select {
		case err := <-ended:
			if !errors.Is(err, context.DeadlineExceeded) {
				t.Fatalf("the callee's context ended with %v", err)
			}
		case <-time.After(5 * time.Second):
			t.Fatal("the callee's context outlived its caller")
		}
	})
}
