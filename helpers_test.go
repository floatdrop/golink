package grpcproc_test

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"google.golang.org/protobuf/proto"

	"github.com/floatdrop/grpcproc"
	"github.com/floatdrop/grpcproc/internal/testpb"
)

// errNegativeTwo is the sentinel echo answers a call with N == -2 with.
var errNegativeTwo = errors.New("negative two")

// echo replies Pong{N+1} to Ping messages and calls, errors on N < 0
// (errNegativeTwo on N == -2), stops on N == 0 with "normal", crashes with
// "boom" on N == -100, and panics with "kaboom" on N == -200.
func echo(p *grpcproc.Process[*testpb.Ping]) error {
	for {
		m, err := p.Receive()
		if err != nil {
			return err
		}
		if m.Down != nil {
			continue
		}
		switch {
		case m.Body.N == 0:
			return nil
		case m.Body.N == -100:
			return errors.New("boom")
		case m.Body.N == -200:
			panic("kaboom")
		case m.IsCall() && m.Body.N == -2:
			_ = m.Reply(nil, errNegativeTwo)
		case m.IsCall() && m.Body.N < 0:
			_ = m.Reply(nil, errors.New("negative: "+strconv.FormatInt(m.Body.N, 10)))
		case m.IsCall():
			_ = m.Reply(&testpb.Pong{N: m.Body.N + 1}, nil)
		default:
			_ = p.SendTo(m.From, &testpb.Pong{N: m.Body.N + 1})
		}
	}
}

// spawnEcho spawns echo on n.
func spawnEcho(t *testing.T, n *grpcproc.Node, opts ...grpcproc.SpawnOption) grpcproc.Addr[*testpb.Ping] {
	t.Helper()
	a, err := n.Spawn(echo, opts...)
	if err != nil {
		t.Fatal(err)
	}
	return a
}

// collector is an untyped process that forwards everything it receives to a channel.
func collector(t testing.TB, n *grpcproc.Node) (grpcproc.Addr[proto.Message], <-chan grpcproc.Msg[proto.Message]) {
	t.Helper()
	ch := make(chan grpcproc.Msg[proto.Message], 4096)
	addr, err := n.Spawn[proto.Message](func(p *grpcproc.Process[proto.Message]) error {
		for {
			m, err := p.Receive()
			if err != nil {
				return err
			}
			ch <- m
		}
	}, grpcproc.WithLabel("collector"))
	if err != nil {
		t.Fatal(err)
	}
	return addr, ch
}

// watcher is a process that monitors whatever PIDs it is sent and forwards Downs.
func watcher(t testing.TB, n *grpcproc.Node) (*grpcproc.Process[proto.Message], <-chan grpcproc.Msg[proto.Message]) {
	t.Helper()
	ch := make(chan grpcproc.Msg[proto.Message], 64)
	ready := make(chan *grpcproc.Process[proto.Message], 1)
	_, err := n.Spawn[proto.Message](func(p *grpcproc.Process[proto.Message]) error {
		ready <- p
		for {
			m, err := p.Receive()
			if err != nil {
				return err
			}
			ch <- m
		}
	})
	if err != nil {
		t.Fatal(err)
	}
	return <-ready, ch
}

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

// recv returns the next message on ch.
func recv[M proto.Message](t testing.TB, ch <-chan grpcproc.Msg[M]) grpcproc.Msg[M] {
	t.Helper()
	return within(t, ch, "message")
}

// within returns the next value on ch, what it is named in the failure.
func within[T any](t testing.TB, ch <-chan T, what string) T {
	t.Helper()
	select {
	case v := <-ch:
		return v
	case <-time.After(5 * time.Second):
		t.Fatalf("no %s", what)
		panic("unreachable")
	}
}

// noMore fails if a message arrives on ch soon.
func noMore[M proto.Message](t *testing.T, ch <-chan grpcproc.Msg[M]) {
	t.Helper()
	select {
	case m := <-ch:
		t.Fatalf("unexpected %+v", m)
	case <-time.After(50 * time.Millisecond):
	}
}

// nextEvent returns the next event on ch of one of kinds, skipping the rest.
func nextEvent(t *testing.T, ch <-chan grpcproc.Event, kinds ...grpcproc.EventKind) grpcproc.Event {
	t.Helper()
	deadline := time.After(5 * time.Second)
	for {
		select {
		case ev, ok := <-ch:
			if !ok {
				t.Fatalf("channel closed waiting for %v", kinds)
			}
			if slices.Contains(kinds, ev.Kind) {
				return ev
			}
		case <-deadline:
			t.Fatalf("timeout waiting for %v", kinds)
		}
	}
}

// ctx is t's context, ended after 5s at the latest.
func ctx(t testing.TB) context.Context {
	c, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	t.Cleanup(cancel)
	return c
}

// eventually polls cond until it holds, for up to 5s.
func eventually(t *testing.T, what string, cond func() bool) {
	t.Helper()
	for deadline := time.Now().Add(5 * time.Second); !cond(); {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// waitWatchers waits until pid on n has want watchers.
func waitWatchers(t *testing.T, n *grpcproc.Node, pid grpcproc.PID, want int) {
	t.Helper()
	eventually(t, fmt.Sprintf("%v to have %d watchers", pid, want), func() bool {
		info, ok := n.Process(pid)
		return ok && info.Watchers == want
	})
}

// waitNoPeer waits until n has no link with peer.
func waitNoPeer(t *testing.T, n *grpcproc.Node, peer string) {
	t.Helper()
	eventually(t, n.Name()+"'s links to "+peer+" to go", func() bool { return !slices.Contains(n.Peers(), peer) })
}

// countingHooks counts what each hook is called for.
type countingHooks struct {
	grpcproc.NopHooks
	spawns, exits, sends, receives, deadLetters, linkUps, linkDowns atomic.Int64
	lastDead                                                        atomic.Pointer[string]
}

func (h *countingHooks) OnSpawn(grpcproc.ProcessInfo)        { h.spawns.Add(1) }
func (h *countingHooks) OnExit(grpcproc.ProcessInfo, string) { h.exits.Add(1) }
func (h *countingHooks) OnSend(_ grpcproc.SendInfo, md grpcproc.Metadata) (grpcproc.Metadata, grpcproc.Done) {
	h.sends.Add(1)
	return md, nil
}
func (h *countingHooks) OnReceive(_ grpcproc.ReceiveInfo, md grpcproc.Metadata) (grpcproc.Metadata, grpcproc.Done) {
	h.receives.Add(1)
	return md, nil
}
func (h *countingHooks) OnDeadLetter(_, _ grpcproc.PID, _ proto.Message, reason string) {
	h.deadLetters.Add(1)
	h.lastDead.Store(&reason)
}
func (h *countingHooks) OnLinkUp(grpcproc.NodeID)          { h.linkUps.Add(1) }
func (h *countingHooks) OnLinkDown(grpcproc.NodeID, error) { h.linkDowns.Add(1) }
