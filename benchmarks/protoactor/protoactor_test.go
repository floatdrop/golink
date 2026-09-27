// Package protoactor benchmarks github.com/asynkron/protoactor-go; see
// ../README.md.
package protoactor

import (
	"log/slog"
	"testing"
	"time"

	pactor "github.com/asynkron/protoactor-go/actor"
	premote "github.com/asynkron/protoactor-go/remote"

	"github.com/floatdrop/grpcproc/benchmarks/internal/shared"
)

type paSink struct {
	want, got int64
	done      chan struct{}
}

func (s *paSink) Receive(c pactor.Context) {
	if _, ok := c.Message().(*shared.Msg); ok {
		s.got++
		if s.got == s.want {
			close(s.done)
		}
	}
}

type paEcho struct{}

func (paEcho) Receive(c pactor.Context) {
	if m, ok := c.Message().(*shared.Msg); ok {
		c.Respond(m)
	}
}

func paLocal(b *testing.B) *pactor.ActorSystem {
	sys := pactor.NewActorSystem(pactor.WithLoggerFactory(func(*pactor.ActorSystem) *slog.Logger {
		return slog.New(slog.DiscardHandler)
	}))
	b.Cleanup(sys.Shutdown)
	return sys
}

func paPair(b *testing.B) (a, z *pactor.ActorSystem) {
	mk := func() *pactor.ActorSystem {
		sys := paLocal(b)
		r := premote.NewRemote(sys, premote.Configure("127.0.0.1", 0))
		r.Start()
		b.Cleanup(func() { r.Shutdown(false) })
		return sys
	}
	return mk(), mk()
}

func paSpawn(b *testing.B, sys *pactor.ActorSystem, a pactor.Actor) *pactor.PID {
	return sys.Root.Spawn(pactor.PropsFromProducer(func() pactor.Actor { return a }))
}

func paSend(b *testing.B, from, to *pactor.ActorSystem) {
	s := &paSink{want: int64(b.N), done: make(chan struct{})}
	sink := paSpawn(b, to, s)
	echo := paSpawn(b, to, paEcho{})
	if _, err := from.Root.RequestFuture(echo, &shared.Msg{Value: 1}, 5*time.Second).Result(); err != nil { // warm up
		b.Fatal(err)
	}
	m := &shared.Msg{Value: 1}
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		from.Root.Send(sink, m)
	}
	<-s.done
}

func paRequest(b *testing.B, from, to *pactor.ActorSystem, parallel bool) {
	echo := paSpawn(b, to, paEcho{})
	if _, err := from.Root.RequestFuture(echo, &shared.Msg{Value: 1}, 5*time.Second).Result(); err != nil {
		b.Fatal(err)
	}
	m := &shared.Msg{Value: 1}
	b.ReportAllocs()
	b.ResetTimer()
	if parallel {
		b.RunParallel(func(pb *testing.PB) {
			for pb.Next() {
				if _, err := from.Root.RequestFuture(echo, m, 5*time.Second).Result(); err != nil {
					b.Fatal(err)
				}
			}
		})
		return
	}
	for range b.N {
		if _, err := from.Root.RequestFuture(echo, m, 5*time.Second).Result(); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkLocalSend(b *testing.B) { s := paLocal(b); paSend(b, s, s) }
func BenchmarkLocalRequest(b *testing.B) {
	s := paLocal(b)
	paRequest(b, s, s, false)
}
func BenchmarkRemoteSend(b *testing.B) { a, z := paPair(b); paSend(b, a, z) }
func BenchmarkRemoteRequest(b *testing.B) {
	a, z := paPair(b)
	paRequest(b, a, z, false)
}
func BenchmarkRemoteRequestParallel(b *testing.B) {
	a, z := paPair(b)
	paRequest(b, a, z, true)
}
