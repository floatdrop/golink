// Package grpcproc benchmarks grpcproc the way the others are benchmarked; see
// ../README.md.
package grpcproc

import (
	"context"
	"net"
	"sync/atomic"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/floatdrop/grpcproc"
	"github.com/floatdrop/grpcproc/benchmarks/internal/shared"
)

func glNode(b *testing.B, name string, peers grpcproc.StaticResolver) (*grpcproc.Node, string) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		b.Fatal(err)
	}
	srv := grpc.NewServer()
	n, err := grpcproc.NewNode(grpcproc.Config{
		Name: name, Advertise: ln.Addr().String(), Resolver: peers,
		DialOptions: []grpc.DialOption{grpc.WithTransportCredentials(insecure.NewCredentials())},
	})
	if err != nil {
		b.Fatal(err)
	}
	n.Register(srv)
	go func() { _ = srv.Serve(ln) }()
	b.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = n.Stop(ctx)
		srv.Stop()
	})
	return n, ln.Addr().String()
}

func glPair(b *testing.B) (a, z *grpcproc.Node) {
	peers := grpcproc.StaticResolver{}
	a, addrA := glNode(b, "a", peers)
	z, addrZ := glNode(b, "z", peers)
	peers["a"], peers["z"] = addrA, addrZ
	return a, z
}

func glLocal(b *testing.B) *grpcproc.Node {
	n, _ := glNode(b, "solo", grpcproc.StaticResolver{})
	return n
}

func glEcho(b *testing.B, n *grpcproc.Node) grpcproc.Addr[*shared.Msg] {
	e, err := n.Spawn(func(p *grpcproc.Process[*shared.Msg]) error {
		for {
			m, err := p.Receive()
			if err != nil {
				return err
			}
			_ = m.Reply(m.Body, nil)
		}
	})
	if err != nil {
		b.Fatal(err)
	}
	return e
}

func glSend(b *testing.B, from, to *grpcproc.Node) {
	var got atomic.Int64
	want := int64(b.N)
	done := make(chan struct{})
	sink, _ := to.Spawn(func(p *grpcproc.Process[*shared.Msg]) error {
		for {
			if _, err := p.Receive(); err != nil {
				return err
			}
			if got.Add(1) == want {
				close(done)
			}
		}
	})
	echo := glEcho(b, to)
	if _, err := from.Call[*shared.Msg](b.Context(), echo, &shared.Msg{Value: 1}); err != nil { // warm up
		b.Fatal(err)
	}
	m := &shared.Msg{Value: 1}
	ctx := b.Context()
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		_ = from.Send(ctx, sink, m)
	}
	<-done
}

func glCall(b *testing.B, from, to *grpcproc.Node, parallel bool) {
	echo := glEcho(b, to)
	ctx := b.Context()
	if _, err := from.Call[*shared.Msg](ctx, echo, &shared.Msg{Value: 1}); err != nil {
		b.Fatal(err)
	}
	m := &shared.Msg{Value: 1}
	b.ReportAllocs()
	b.ResetTimer()
	if parallel {
		b.RunParallel(func(pb *testing.PB) {
			for pb.Next() {
				if _, err := from.Call[*shared.Msg](ctx, echo, m); err != nil {
					b.Fatal(err)
				}
			}
		})
		return
	}
	for range b.N {
		if _, err := from.Call[*shared.Msg](ctx, echo, m); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkLocalSend(b *testing.B)    { n := glLocal(b); glSend(b, n, n) }
func BenchmarkLocalRequest(b *testing.B) { n := glLocal(b); glCall(b, n, n, false) }
func BenchmarkRemoteSend(b *testing.B)   { a, z := glPair(b); glSend(b, a, z) }
func BenchmarkRemoteRequest(b *testing.B) {
	a, z := glPair(b)
	glCall(b, a, z, false)
}
func BenchmarkRemoteRequestParallel(b *testing.B) {
	a, z := glPair(b)
	glCall(b, a, z, true)
}
