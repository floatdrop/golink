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

// echoAddr is an address with its protocol as methods, as a contract
// package writes one.
type echoAddr struct{ grpcproc.Addr[*testpb.Ping] }

func (e echoAddr) Bump(ctx context.Context, from grpcproc.Caller, n int64) (*testpb.Pong, error) {
	return e.Call[*testpb.Pong](ctx, from, &testpb.Ping{N: n})
}

func TestAddrCallAndSend(t *testing.T) {
	c := grpcproctest.New(t, "a", "b")
	a, b := c.Node("a"), c.Node("b")
	addr, err := b.Spawn(echo)
	if err != nil {
		t.Fatal(err)
	}
	e := echoAddr{addr}

	// From a node, as Node.Call.
	if r, err := e.Bump(ctx(t), a, 9); err != nil || r.N != 10 {
		t.Fatalf("bump: %v %v", r, err)
	}
	if _, err := e.Bump(ctx(t), a, -1); err == nil {
		t.Fatal("want the handler's error")
	}
	if _, err := e.Call[*testpb.Ping](ctx(t), a, &testpb.Ping{N: 1}); !errors.Is(err, grpcproc.ErrType) {
		t.Fatalf("reply typed wrongly should be ErrType, got %v", err)
	}

	// probe passes on what it gets and answers calls.
	got := make(chan grpcproc.Msg[*testpb.Ping], 4)
	probe, err := b.Spawn(func(p *grpcproc.Process[*testpb.Ping]) error {
		for {
			m, err := p.Receive()
			if err != nil {
				return err
			}
			got <- m
			if m.IsCall() {
				_ = p.Reply(m, &testpb.Pong{}, nil)
			}
		}
	})
	if err != nil {
		t.Fatal(err)
	}
	tenant := grpcproc.WithMetadata(t.Context(), grpcproc.Metadata{"tenant": "acme"})

	// From a node, as Node.Send.
	if err := probe.Send(tenant, a, &testpb.Ping{N: 1}); err != nil {
		t.Fatal(err)
	}
	if m := recv(t, got); m.From != a.PID() || m.IsCall() || m.Metadata["tenant"] != "acme" {
		t.Fatalf("send from node: %+v", m)
	}

	// From a process, as Process.Call: what it inherited from the message
	// it handles, and what ctx adds.
	bumped := make(chan int64, 1)
	caller, err := a.Spawn(func(p *grpcproc.Process[*testpb.Ping]) error {
		if _, err := p.Receive(); err != nil {
			return err
		}
		ctx := grpcproc.WithMetadata(t.Context(), grpcproc.Metadata{"extra": "1"})
		if _, err := probe.Call[*testpb.Pong](ctx, p, &testpb.Ping{N: 1}); err != nil {
			return err
		}
		if err := probe.Send(ctx, p, &testpb.Ping{N: 1}); err != nil {
			return err
		}
		r, err := e.Bump(ctx, p, 41)
		if err != nil {
			return err
		}
		bumped <- r.N
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := a.Send(tenant, caller, &testpb.Ping{N: 1}); err != nil {
		t.Fatal(err)
	}
	for _, call := range []bool{true, false} {
		m := recv(t, got)
		if m.From != caller.PID() || m.IsCall() != call || m.Metadata["tenant"] != "acme" || m.Metadata["extra"] != "1" {
			t.Fatalf("from process, call %v: %+v", call, m)
		}
	}
	select {
	case n := <-bumped:
		if n != 42 {
			t.Fatalf("bump from process: %d", n)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timeout waiting for the bump")
	}
}
