package grpcproc

import (
	"errors"
	"testing"
	"testing/synctest"

	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/proto"

	"github.com/floatdrop/grpcproc/internal/testpb"
	grpcprocv1 "github.com/floatdrop/grpcproc/proto/grpcproc/v1"
)

func TestDispatchMalformed(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		n := newTestNode(t, "a")
		me := n.PID()
		unknown := func(kind grpcprocv1.Kind, ref uint64) *grpcprocv1.Envelope {
			return &grpcprocv1.Envelope{Kind: kind, FromIncarnation: 1, FromId: 1, ToIncarnation: 1, ToId: 1, Ref: ref,
				BodyType: "no.such.Type", Body: []byte{1}}
		}
		n.dispatch("b", nil, unknown(grpcprocv1.Kind_KIND_SEND, 0))
		n.dispatch("b", nil, unknown(grpcprocv1.Kind_KIND_CALL, 1))
		if n.deadLetters.Load() != 1 {
			t.Fatalf("dead letters %d", n.deadLetters.Load())
		}
		// A reply with an undecodable body fails the pending call with ErrType;
		// one nobody waits for is dropped.
		pc := &pendingCall{node: "b", ch: make(chan callResult, 1)}
		n.pending[7] = pc
		n.dispatch("b", nil, unknown(grpcprocv1.Kind_KIND_REPLY, 7))
		if r := <-pc.ch; !errors.Is(r.err, ErrType) {
			t.Fatalf("%v", r.err)
		}
		n.dispatch("b", nil, &grpcprocv1.Envelope{Kind: grpcprocv1.Kind_KIND_REPLY, ToIncarnation: 1, Ref: 8, Status: grpcprocv1.Status_STATUS_OK})
		// A reply to an earlier incarnation of this node leaves a call of this
		// one with the same ref alone.
		stale := &pendingCall{node: "b", ch: make(chan callResult, 1)}
		n.pending[8] = stale
		n.dispatch("b", nil, &grpcprocv1.Envelope{Kind: grpcprocv1.Kind_KIND_REPLY, ToIncarnation: 0, Ref: 8, Status: grpcprocv1.Status_STATUS_OK})
		if len(stale.ch) != 0 || n.pending[8] != stale {
			t.Fatal("a reply to an earlier incarnation answered a call")
		}
		delete(n.pending, 8)
		// Down for a process that does not exist, and for a ref it never held.
		n.dispatch("b", nil, &grpcprocv1.Envelope{Kind: grpcprocv1.Kind_KIND_DOWN, FromIncarnation: 1, FromId: 1, ToIncarnation: 1, ToId: 1, Ref: 1})
		p := &proc{n: n, pid: PID{Node: "a", Incarnation: 1, ID: 5}, mbox: newQueue[item](true)}
		n.procs[5] = p
		n.dispatch("b", nil, &grpcprocv1.Envelope{Kind: grpcprocv1.Kind_KIND_DOWN, FromIncarnation: 1, FromId: 1, ToIncarnation: p.pid.Incarnation, ToId: p.pid.ID, Ref: 1})
		if p.mbox.len() != 0 {
			t.Fatal("unknown ref must not deliver")
		}
		// Delivery to a process whose mailbox is already closed is a dead letter,
		// and a call gets noproc.
		p.accept = func(proto.Message) bool { return true }
		p.mbox.close()
		n.deliver(p.pid, "", item{from: me, body: &testpb.Ping{}}, nil)
		n.deliver(p.pid, "", item{from: me, body: &testpb.Ping{}, ref: 9}, pc.ch)
		if r := <-pc.ch; !errors.Is(r.err, ErrNoProc) {
			t.Fatalf("%v", r.err)
		}
		// A watcher cannot be added to an exited process.
		p.exited = true
		if p.addWatcher(Ref{}, me) {
			t.Fatal("addWatcher on exited")
		}
		delete(n.procs, 5)
	})
}

func TestProtoHelpers(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		if typeName(nil) != "" {
			t.Fatal("nil helpers")
		}
		if _, err := decodeBody(&grpcprocv1.Envelope{}); err == nil {
			t.Fatal("decode without a body")
		}
		// A body whose type is known but whose bytes are not that type.
		if _, err := decodeBody(&grpcprocv1.Envelope{BodyType: "grpcproc.test.v1.Ping", Body: []byte{0xff}}); err == nil {
			t.Fatal("decoded garbage")
		}
		if first(metadata.MD{}, "k") != "" {
			t.Fatal("first")
		}
	})
}
