package grpcproc_test

import (
	"context"
	"errors"
	"io"
	"strings"
	"testing"
	"testing/synctest"
	"time"

	"google.golang.org/protobuf/proto"

	"github.com/floatdrop/grpcproc"
	"github.com/floatdrop/grpcproc/grpcproctest"
	"github.com/floatdrop/grpcproc/internal/testpb"
)

func TestLocalTypedSendAndCall(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		c := grpcproctest.New(t, "a")
		a := c.Node("a")
		e, _ := a.Spawn(echo)
		// Sending from outside any process: the Pong reply has nowhere to go, so
		// it is a dead letter, not an error.
		if err := e.Send(t.Context(), a, &testpb.Ping{N: 1}); err != nil {
			t.Fatal(err)
		}
		// From inside a process the reply comes back to it.
		gotc := make(chan grpcproc.Msg[proto.Message], 1)
		_, err := a.Spawn[proto.Message](func(p *grpcproc.Process[proto.Message]) error {
			if err := e.Send(p.Context(), p, &testpb.Ping{N: 41}); err != nil {
				return err
			}
			m, err := p.Receive()
			if err != nil {
				return err
			}
			gotc <- m
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}
		r, err := e.Call[*testpb.Pong](ctx(t), a, &testpb.Ping{N: 9})
		if err != nil || r.N != 10 {
			t.Fatalf("call: %v %v", r, err)
		}
		got := recv(t, gotc)
		if pong, ok := got.Body.(*testpb.Pong); !ok || pong.N != 42 || got.From != e.PID() {
			t.Fatalf("got %+v", got)
		}
		if _, err := e.Call[*testpb.Ping](ctx(t), a, &testpb.Ping{N: 1}); !errors.Is(err, grpcproc.ErrType) {
			t.Fatalf("reply typed wrongly should be ErrType, got %v", err)
		}
	})
}

func TestRemoteByPIDAndName(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		c := grpcproctest.New(t, "a", "b")
		a, b := c.Node("a"), c.Node("b")
		e, err := b.Spawn(echo, grpcproc.WithName("echo"))
		if err != nil {
			t.Fatal(err)
		}
		col, ch := collector(t, a)

		// Same API as local: the address points at another node.
		w, _ := watcher(t, a)
		if err := e.Send(w.Context(), w, &testpb.Ping{N: 1}); err != nil {
			t.Fatal(err)
		}
		_ = col
		_ = ch
		r, err := a.CallTo[*testpb.Pong](ctx(t), grpcproc.Named[*testpb.Ping]("b", "echo"), &testpb.Ping{N: 9})
		if err != nil || r.N != 10 {
			t.Fatalf("call by name: %v %v", r, err)
		}
		// A typed call from inside a process, reply type explicit, request inferred.
		_, err = a.Spawn[proto.Message](func(p *grpcproc.Process[proto.Message]) error {
			r, err := e.Call[*testpb.Pong](ctx(t), p, &testpb.Ping{N: 99})
			if err != nil || r.N != 100 {
				t.Errorf("process call: %v %v", r, err)
			}
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}
		// Errors from handlers cross the wire.
		_, err = a.CallTo[*testpb.Pong](ctx(t), e, &testpb.Ping{N: -1})
		if re, ok := errors.AsType[*grpcproc.RemoteError](err); !ok || re.Msg != "negative: -1" {
			t.Fatalf("want remote error, got %v", err)
		}
		// A sentinel a handler answers with is that sentinel on the caller's side.
		if _, err = a.CallTo[*testpb.Pong](ctx(t), e, &testpb.Ping{N: -2}); !errors.Is(err, errNegativeTwo) {
			t.Fatalf("want the handler's sentinel, got %v", err)
		}
		// Unknown process, by PID and by name.
		if _, err = a.CallTo[*testpb.Pong](ctx(t), grpcproc.PID{Node: "b", Incarnation: e.PID().Incarnation, ID: 9999}, &testpb.Ping{}); !errors.Is(err, grpcproc.ErrNoProc) {
			t.Fatalf("want ErrNoProc, got %v", err)
		}
		if _, err = a.CallTo[*testpb.Pong](ctx(t), grpcproc.Named[*testpb.Ping]("b", "nope"), &testpb.Ping{}); !errors.Is(err, grpcproc.ErrNoProc) {
			t.Fatalf("want ErrNoProc by name, got %v", err)
		}
		if peers := a.Peers(); len(peers) != 1 || peers[0] != "b" {
			t.Fatalf("peers %v", peers)
		}
	})
}

// echoAddr is an address with its protocol as methods, as a contract
// package writes one.
type echoAddr struct{ grpcproc.Addr[*testpb.Ping] }

func (e echoAddr) Bump(ctx context.Context, from grpcproc.Caller, n int64) (*testpb.Pong, error) {
	return e.Call[*testpb.Pong](ctx, from, &testpb.Ping{N: n})
}

func TestAddrCallAndSend(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
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
					_ = m.Reply(&testpb.Pong{}, nil)
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
		if err := caller.Send(tenant, a, &testpb.Ping{N: 1}); err != nil {
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
	})
}

func TestAddrForms(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		pid := grpcproc.PID{Node: "n", Incarnation: 3, ID: 7}
		byPID := grpcproc.AddrOf[*testpb.Ping](pid)
		if byPID.PID() != pid || byPID.Node() != "n" || byPID.Name() != "" || byPID.String() != "<n.3.7>" {
			t.Fatalf("%v %v", byPID, byPID.PID())
		}
		byName := grpcproc.AddrOf[*testpb.Ping](grpcproc.Name{Node: "n", Name: "svc"})
		if byName.Name() != "svc" || byName.Node() != "n" || byName.String() != "{svc@n}" || !byName.PID().IsZero() == false && byName.PID().Node != "n" {
			t.Fatalf("%v", byName)
		}
		if !(grpcproc.PID{}).IsZero() || pid.IsZero() {
			t.Fatal("IsZero")
		}
		if (grpcproc.Ref{Node: "n", ID: 4}).String() != "#n.4" || (grpcproc.NodeID{Name: "n", Incarnation: 2}).String() != "n#2" {
			t.Fatal("String")
		}
		// A value outside the enum, as a peer or a newer Inspector may send, is
		// named by its number rather than a panic.
		if s := grpcproc.ProcessState(255).String() + grpcproc.LinkState(9).String() + grpcproc.EventKind(0).String() + grpcproc.EventKind(9).String(); s != "ProcessState(255)LinkState(9)EventKind(0)EventKind(9)" {
			t.Fatal(s)
		}
		if grpcproc.StateIdle.String() != "idle" || grpcproc.StateExiting.String() != "exiting" || grpcproc.LinkUp.String() != "up" {
			t.Fatal("state strings")
		}
		if (&grpcproc.RemoteError{Msg: "x"}).Error() != "x" || (&grpcproc.ExitError{Reason: "r"}).Error() != "grpcproc: exit: r" {
			t.Fatal("error strings")
		}
		le := &grpcproc.LinkError{Peer: "b", Err: io.EOF}
		if !errors.Is(le, io.EOF) || !errors.Is(le, grpcproc.ErrNoConnection) || !strings.Contains(le.Error(), "link to b") {
			t.Fatal("LinkError")
		}
		re := &grpcproc.RemoteError{Msg: grpcproc.ErrNoProc.Error()}
		if !errors.Is(re, grpcproc.ErrNoProc) || errors.Is(re, grpcproc.ErrType) {
			t.Fatal("RemoteError is the sentinel with its text, and no other")
		}
		full := grpcproc.PID{Node: "n", Incarnation: 2, ID: 3}
		if grpcproc.PIDFromProto(full.Proto()) != full || (grpcproc.PID{}).Proto() != nil || !grpcproc.PIDFromProto(nil).IsZero() {
			t.Fatal("PID to proto and back")
		}
	})
}

func TestTypeMismatchIsDeadLetter(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		h := &countingHooks{}
		c := grpcproctest.NewWith(t, []grpcproctest.Option{grpcproctest.WithHooks(h)}, "a", "b")
		a, b := c.Node("a"), c.Node("b")
		e, _ := b.Spawn(echo, grpcproc.WithName("echo"))

		// A remote sender addresses the process with the wrong type.
		wrong := grpcproc.Named[*testpb.Pong]("b", "echo")
		if err := a.SendTo(t.Context(), wrong, &testpb.Pong{N: 1}); err != nil {
			t.Fatal(err)
		}
		if _, err := a.CallTo[*testpb.Pong](ctx(t), wrong, &testpb.Pong{}); !errors.Is(err, grpcproc.ErrType) {
			t.Fatalf("want ErrType, got %v", err)
		}
		// Locally too, through an untyped address.
		if err := b.SendTo(t.Context(), e.PID(), &testpb.Pong{}); err != nil {
			t.Fatal(err)
		}
		deadline := time.Now().Add(2 * time.Second)
		for h.deadLetters.Load() < 3 && time.Now().Before(deadline) {
			time.Sleep(5 * time.Millisecond)
		}
		if got := h.deadLetters.Load(); got != 3 {
			t.Fatalf("dead letters = %d, want 3", got)
		}
		if r := h.lastDead.Load(); r == nil || *r != grpcproc.ReasonType {
			t.Fatalf("reason %v", r)
		}
		if b.Info().DeadLetters != 3 {
			t.Fatalf("node info dead letters = %d", b.Info().DeadLetters)
		}
		// The process is unharmed.
		if r, err := a.CallTo[*testpb.Pong](ctx(t), e, &testpb.Ping{N: 1}); err != nil || r.N != 2 {
			t.Fatalf("%v %v", r, err)
		}
	})
}

func TestRemoteOrdering(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		c := grpcproctest.New(t, "a", "b")
		a, b := c.Node("a"), c.Node("b")
		col, ch := collector(t, b)
		const N = 20000
		_, _ = a.Spawn[proto.Message](func(p *grpcproc.Process[proto.Message]) error {
			for i := range N {
				if err := col.Send(p.Context(), p, &testpb.Ping{N: int64(i)}); err != nil {
					return err
				}
			}
			return nil
		})
		for i := range N {
			if m := recv(t, ch); m.Body.(*testpb.Ping).N != int64(i) {
				t.Fatalf("out of order: want %d got %v", i, m.Body)
			}
		}
	})
}

func TestEncodeErrors(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		// Invalid UTF-8 in a proto3 string cannot be marshalled.
		c := grpcproctest.New(t, "a", "b")
		a, b := c.Node("a"), c.Node("b")
		bad := &testpb.Reserve{Id: "\xff"}
		col, _ := collector(t, b)
		if err := a.SendTo(t.Context(), col, bad); err == nil || !strings.Contains(err.Error(), "encode") {
			t.Fatalf("send: %v", err)
		}
		if _, err := a.CallTo[*testpb.Pong](ctx(t), col, bad); err == nil || !strings.Contains(err.Error(), "encode") {
			t.Fatalf("call: %v", err)
		}
		// A reply that cannot be encoded is reported to the replier.
		replyErr := make(chan error, 1)
		svc, _ := b.Spawn[*testpb.Ping](func(p *grpcproc.Process[*testpb.Ping]) error {
			m, err := p.Receive()
			if err != nil {
				return err
			}
			replyErr <- m.Reply(bad, nil)
			return nil
		})
		go func() { _, _ = a.CallTo[*testpb.Pong](ctx(t), svc, &testpb.Ping{}) }()
		if err := <-replyErr; err == nil || !strings.Contains(err.Error(), "encode") {
			t.Fatalf("reply: %v", err)
		}
	})
}

func TestCopyLocal(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		n, err := grpcproc.NewNode(grpcproc.Config{Name: "a", Resolver: grpcproc.StaticResolver{}, CopyLocal: true})
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = n.Stop(context.Background()) })
		e, _ := n.Spawn(echo)
		msg := &testpb.Ping{N: 1}
		if err := n.SendTo(t.Context(), e, msg); err != nil {
			t.Fatal(err)
		}
		msg.N = -100 // would crash the echo if the pointer were shared
		req := &testpb.Ping{N: 1}
		r, err := n.CallTo[*testpb.Pong](ctx(t), e, req)
		if err != nil || r.N != 2 {
			t.Fatalf("%v %v", r, err)
		}
		if _, ok := n.Process(e.PID()); !ok {
			t.Fatal("echo crashed on a mutated message")
		}
	})
}
