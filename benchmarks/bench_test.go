// Package benchmarks compares golink with github.com/anthdm/hollywood on the
// same machine, with the same message (wrapperspb.Int64Value), remote over TCP
// on loopback for both. See README.md for how to read it.
package benchmarks

import (
	"context"
	"net"
	"sync/atomic"
	"testing"
	"time"

	"github.com/anthdm/hollywood/actor"
	"github.com/anthdm/hollywood/remote"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/types/known/wrapperspb"

	"github.com/floatdrop/golink"
)

type msg = wrapperspb.Int64Value

func freeAddr(b *testing.B) string {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		b.Fatal(err)
	}
	defer func() { _ = ln.Close() }()
	return ln.Addr().String()
}

// ---------------- hollywood ----------------

type hwSink struct {
	want, got int64
	done      chan struct{}
}

func (s *hwSink) Receive(c *actor.Context) {
	if _, ok := c.Message().(*msg); ok {
		s.got++
		if s.got == s.want {
			close(s.done)
		}
	}
}

type hwEcho struct{}

func (hwEcho) Receive(c *actor.Context) {
	if m, ok := c.Message().(*msg); ok {
		c.Respond(m)
	}
}

func hwLocal(b *testing.B) *actor.Engine {
	e, err := actor.NewEngine(actor.NewEngineConfig())
	if err != nil {
		b.Fatal(err)
	}
	return e
}

func hwPair(b *testing.B) (a, z *actor.Engine) {
	mk := func() *actor.Engine {
		r := remote.New(freeAddr(b), remote.NewConfig())
		e, err := actor.NewEngine(actor.NewEngineConfig().WithRemote(r))
		if err != nil {
			b.Fatal(err)
		}
		b.Cleanup(func() { r.Stop().Wait() })
		return e
	}
	return mk(), mk()
}

func hwSend(b *testing.B, from, to *actor.Engine) {
	s := &hwSink{want: int64(b.N), done: make(chan struct{})}
	pid := to.Spawn(func() actor.Receiver { return s }, "sink")
	echo := to.Spawn(func() actor.Receiver { return hwEcho{} }, "echo")
	if _, err := from.Request(echo, &msg{Value: 1}, 5*time.Second).Result(); err != nil { // warm up
		b.Fatal(err)
	}
	m := &msg{Value: 1}
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		from.Send(pid, m)
	}
	<-s.done
}

func hwRequest(b *testing.B, from, to *actor.Engine, parallel bool) {
	echo := to.Spawn(func() actor.Receiver { return hwEcho{} }, "echo")
	if _, err := from.Request(echo, &msg{Value: 1}, 5*time.Second).Result(); err != nil {
		b.Fatal(err)
	}
	m := &msg{Value: 1}
	b.ReportAllocs()
	b.ResetTimer()
	if parallel {
		b.RunParallel(func(pb *testing.PB) {
			for pb.Next() {
				if _, err := from.Request(echo, m, 5*time.Second).Result(); err != nil {
					b.Fatal(err)
				}
			}
		})
		return
	}
	for range b.N {
		if _, err := from.Request(echo, m, 5*time.Second).Result(); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkHollywood_LocalSend(b *testing.B) { e := hwLocal(b); hwSend(b, e, e) }
func BenchmarkHollywood_LocalRequest(b *testing.B) {
	e := hwLocal(b)
	hwRequest(b, e, e, false)
}
func BenchmarkHollywood_RemoteSend(b *testing.B) { a, z := hwPair(b); hwSend(b, a, z) }
func BenchmarkHollywood_RemoteRequest(b *testing.B) {
	a, z := hwPair(b)
	hwRequest(b, a, z, false)
}
func BenchmarkHollywood_RemoteRequestParallel(b *testing.B) {
	a, z := hwPair(b)
	hwRequest(b, a, z, true)
}

// ---------------- golink ----------------

func glNode(b *testing.B, name string, peers golink.StaticResolver) (*golink.Node, string) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		b.Fatal(err)
	}
	srv := grpc.NewServer()
	n, err := golink.NewNode(golink.Config{
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

func glPair(b *testing.B) (a, z *golink.Node) {
	peers := golink.StaticResolver{}
	a, addrA := glNode(b, "a", peers)
	z, addrZ := glNode(b, "z", peers)
	peers["a"], peers["z"] = addrA, addrZ
	return a, z
}

func glLocal(b *testing.B) *golink.Node {
	n, _ := glNode(b, "solo", golink.StaticResolver{})
	return n
}

func glEcho(b *testing.B, n *golink.Node) golink.Addr[*msg] {
	e, err := golink.Spawn(n, func(p *golink.Process[*msg]) error {
		for {
			m, err := p.Receive()
			if err != nil {
				return err
			}
			_ = p.Reply(m, m.Body, nil)
		}
	})
	if err != nil {
		b.Fatal(err)
	}
	return e
}

func glSend(b *testing.B, from, to *golink.Node) {
	var got atomic.Int64
	want := int64(b.N)
	done := make(chan struct{})
	sink, _ := golink.Spawn(to, func(p *golink.Process[*msg]) error {
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
	if _, err := from.Call[*msg](b.Context(), echo, &msg{Value: 1}); err != nil { // warm up
		b.Fatal(err)
	}
	m := &msg{Value: 1}
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		_ = from.Send(sink, m)
	}
	<-done
}

func glCall(b *testing.B, from, to *golink.Node, parallel bool) {
	echo := glEcho(b, to)
	ctx := b.Context()
	if _, err := from.Call[*msg](ctx, echo, &msg{Value: 1}); err != nil {
		b.Fatal(err)
	}
	m := &msg{Value: 1}
	b.ReportAllocs()
	b.ResetTimer()
	if parallel {
		b.RunParallel(func(pb *testing.PB) {
			for pb.Next() {
				if _, err := from.Call[*msg](ctx, echo, m); err != nil {
					b.Fatal(err)
				}
			}
		})
		return
	}
	for range b.N {
		if _, err := from.Call[*msg](ctx, echo, m); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkGolink_LocalSend(b *testing.B)    { n := glLocal(b); glSend(b, n, n) }
func BenchmarkGolink_LocalRequest(b *testing.B) { n := glLocal(b); glCall(b, n, n, false) }
func BenchmarkGolink_RemoteSend(b *testing.B)   { a, z := glPair(b); glSend(b, a, z) }
func BenchmarkGolink_RemoteRequest(b *testing.B) {
	a, z := glPair(b)
	glCall(b, a, z, false)
}
func BenchmarkGolink_RemoteRequestParallel(b *testing.B) {
	a, z := glPair(b)
	glCall(b, a, z, true)
}
