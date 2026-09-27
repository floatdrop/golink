package grpcproc

import (
	"context"
	"errors"
	"testing"
	"time"

	grpcprocv1 "github.com/floatdrop/grpcproc/proto/grpcproc/v1"
	"google.golang.org/protobuf/proto"
)

// Every way a call to a peer can end leaves nothing in pending: an answer
// takes its entry, and a call that ends unanswered removes its own. (A call
// to a process of the node never enters it.)
func TestCallsLeaveNothingPending(t *testing.T) {
	n := newTestNode(t, "a")
	l := &outLink{node: n, peer: NodeID{Name: "b", Incarnation: 2}, cc: testConn(t), q: newQueue[*grpcprocv1.Envelope](false),
		done: make(chan struct{}), cancel: func() {}}
	n.mu.Lock()
	n.out["b"] = l // no writer: calls stay queued
	n.mu.Unlock()
	t.Cleanup(func() { // before Stop, which would flush it
		n.mu.Lock()
		delete(n.out, "b")
		n.mu.Unlock()
		l.close(nil)
	})
	type ping = grpcprocv1.Hello
	peer := Addr[*ping]{pid: PID{Node: "b", Incarnation: 2, ID: 1}}
	queued := func() *grpcprocv1.Envelope {
		for deadline := time.Now().Add(5 * time.Second); l.q.len() == 0; time.Sleep(time.Millisecond) {
			if time.Now().After(deadline) {
				t.Fatal("the call was never queued")
			}
		}
		return l.q.drain()[0]
	}

	answered := make(chan error, 1)
	go func() {
		_, err := n.Call[*ping](t.Context(), peer, &ping{})
		answered <- err
	}()
	call := queued()
	reply := &grpcprocv1.Envelope{Kind: grpcprocv1.Kind_KIND_REPLY, ToIncarnation: call.GetFromIncarnation(), ToId: call.GetFromId(),
		FromIncarnation: 2, FromId: 1, Ref: call.GetRef(), Status: grpcprocv1.Status_STATUS_OK}
	if err := encodeBody(reply, &ping{}); err != nil {
		t.Fatal(err)
	}
	n.dispatch("b", reply)
	if err := <-answered; err != nil {
		t.Fatal(err)
	}

	short, cancel := context.WithTimeout(t.Context(), 10*time.Millisecond)
	defer cancel()
	if _, err := n.Call[*ping](short, peer, &ping{}); !errors.Is(err, context.DeadlineExceeded) { // ctx
		t.Fatal(err)
	}
	if _, err := n.Call[*ping](t.Context(), Named[*ping]("", "x"), &ping{}); err == nil { // route: no node
		t.Fatal("routed to no node")
	}
	n.pendingMu.Lock()
	defer n.pendingMu.Unlock()
	if len(n.pending) != 0 {
		t.Fatalf("%d calls left pending", len(n.pending))
	}
}

// haltingHooks halts the node as a call finds no process: between the call's
// look at halted and its answer.
type haltingHooks struct {
	NopHooks
	n *Node
}

func (h *haltingHooks) OnDeadLetter(PID, PID, proto.Message, string) { close(h.n.halted) }

// A local call answered as the node halts keeps its answer, whichever its
// wait sees first.
func TestLocalCallAnsweredAsTheNodeHalts(t *testing.T) {
	for range 64 {
		h := &haltingHooks{}
		n, err := NewNode(Config{Name: "a", Resolver: StaticResolver{}, Hooks: h})
		if err != nil {
			t.Fatal(err)
		}
		h.n = n
		if _, err := n.Call[*grpcprocv1.Hello](t.Context(), Named[*grpcprocv1.Hello]("a", "nobody"), &grpcprocv1.Hello{}); !errors.Is(err, ErrNoProc) {
			t.Fatalf("got %v", err)
		}
	}
}

// A second answer to a local call is dropped rather than block: it cannot
// happen, but a blocked send would hang a process's exit.
func TestSecondLocalAnswerIsDropped(t *testing.T) {
	n := newTestNode(t, "a")
	ch := make(chan callResult, 1)
	me := n.PID()
	for range 2 {
		if err := n.reply(me, me, 1, ch, nil, grpcprocv1.Status_STATUS_NOPROC, "", false); err != nil {
			t.Fatal(err)
		}
	}
	if r := <-ch; !errors.Is(r.err, ErrNoProc) {
		t.Fatal(r.err)
	}
}

// A local call gives up when its ctx ends; an answer after that goes into
// its channel, where nothing waits for it.
func TestLocalCallGivesUpWithItsCtx(t *testing.T) {
	n := newTestNode(t, "a")
	type ping = grpcprocv1.Hello
	ctx, cancel := context.WithCancel(t.Context())
	late, replied := make(chan struct{}), make(chan error, 1)
	p, err := n.Spawn(func(p *Process[*ping]) error {
		m, err := p.Receive()
		if err != nil {
			return err
		}
		cancel() // the caller gives up once the call is taken
		<-late
		replied <- p.Reply(m, &ping{}, nil)
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := n.Call[*ping](ctx, p, &ping{}); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	close(late)
	if err := <-replied; err != nil {
		t.Fatal(err)
	}
}
