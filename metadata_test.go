package grpcproc_test

import (
	"testing"
	"testing/synctest"
	"time"

	"google.golang.org/protobuf/proto"

	"github.com/floatdrop/grpcproc"
	"github.com/floatdrop/grpcproc/grpcproctest"
	"github.com/floatdrop/grpcproc/internal/testpb"
)

func TestMetadataMerge(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx := grpcproc.WithMetadata(t.Context(), grpcproc.Metadata{"a": "1", "b": "1"})
		ctx = grpcproc.WithMetadata(ctx, grpcproc.Metadata{"b": "2"})
		md := grpcproc.MetadataFrom(ctx)
		if md["a"] != "1" || md["b"] != "2" || grpcproc.MetadataFrom(t.Context()) != nil {
			t.Fatalf("%v", md)
		}
		m := grpcproc.Msg[proto.Message]{Metadata: md}
		withMD, cancel := m.Context(t.Context())
		defer cancel()
		if grpcproc.MetadataFrom(withMD)["b"] != "2" {
			t.Fatal("Msg.Context")
		}
		if _, ok := withMD.Deadline(); ok {
			t.Fatal("a message that is not a call has no deadline")
		}
		plain, cancel := (grpcproc.Msg[proto.Message]{}).Context(ctx)
		defer cancel()
		if plain != ctx {
			t.Fatal("Msg.Context without metadata or deadline must return parent")
		}
	})
}

func TestMetadataPropagates(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		c := grpcproctest.New(t, "a", "b")
		a, b := c.Node("a"), c.Node("b")
		col, ch := collector(t, b)
		if err := col.Send(grpcproc.WithMetadata(t.Context(), grpcproc.Metadata{"trace": "abc"}), a, &testpb.Ping{}); err != nil {
			t.Fatal(err)
		}
		if m := recv(t, ch); m.Metadata["trace"] != "abc" {
			t.Fatalf("metadata %v", m.Metadata)
		}
	})
}

func TestInheritanceEndsWithHandling(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		c := grpcproctest.New(t, "a")
		a := c.Node("a")
		sink, got := collector(t, a)
		// After a ReceiveTimeout, nothing is being handled: sends inherit nothing.
		p, _ := a.Spawn[*testpb.Ping](func(p *grpcproc.Process[*testpb.Ping]) error {
			if _, err := p.Receive(); err != nil {
				return err
			}
			_ = sink.Send(p.Context(), p, &testpb.Ping{N: 1}) // inherits
			if _, err := p.ReceiveTimeout(time.Millisecond); err == nil {
				t.Error("expected timeout")
			}
			return sink.Send(p.Context(), p, &testpb.Ping{N: 2}) // inherits nothing
		})
		_ = p.Send(grpcproc.WithMetadata(t.Context(), grpcproc.Metadata{"tenant": "acme"}), a, &testpb.Ping{})
		if m := recv(t, got); m.Metadata["tenant"] != "acme" {
			t.Fatalf("1: %v", m.Metadata)
		}
		if m := recv(t, got); len(m.Metadata) != 0 {
			t.Fatalf("2: %v", m.Metadata)
		}
	})
}

func TestCallMergesContextMetadata(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		c := grpcproctest.New(t, "a")
		a := c.Node("a")
		probe, got := answering(t, a)
		// A call from a process carries what it inherited and what ctx adds.
		p, _ := a.Spawn[*testpb.Ping](func(p *grpcproc.Process[*testpb.Ping]) error {
			if _, err := p.Receive(); err != nil {
				return err
			}
			ctx := grpcproc.WithMetadata(t.Context(), grpcproc.Metadata{"extra": "1"})
			_, err := probe.Call[*testpb.Pong](ctx, p, &testpb.Ping{})
			return err
		})
		_ = p.Send(grpcproc.WithMetadata(t.Context(), grpcproc.Metadata{"tenant": "acme"}), a, &testpb.Ping{})
		if md := recv(t, got).Metadata; md["tenant"] != "acme" || md["extra"] != "1" {
			t.Fatalf("got %v", md)
		}
	})
}
