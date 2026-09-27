// Package hollywood benchmarks github.com/anthdm/hollywood; see ../README.md.
package hollywood

import (
	"testing"
	"time"

	"github.com/anthdm/hollywood/actor"
	"github.com/anthdm/hollywood/remote"

	"github.com/floatdrop/grpcproc/benchmarks/internal/shared"
)

type hwSink struct {
	want, got int64
	done      chan struct{}
}

func (s *hwSink) Receive(c *actor.Context) {
	if _, ok := c.Message().(*shared.Msg); ok {
		s.got++
		if s.got == s.want {
			close(s.done)
		}
	}
}

type hwEcho struct{}

func (hwEcho) Receive(c *actor.Context) {
	if m, ok := c.Message().(*shared.Msg); ok {
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
		r := remote.New(shared.FreeAddr(b), remote.NewConfig())
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
	if _, err := from.Request(echo, &shared.Msg{Value: 1}, 5*time.Second).Result(); err != nil { // warm up
		b.Fatal(err)
	}
	m := &shared.Msg{Value: 1}
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		from.Send(pid, m)
	}
	<-s.done
}

func hwRequest(b *testing.B, from, to *actor.Engine, parallel bool) {
	echo := to.Spawn(func() actor.Receiver { return hwEcho{} }, "echo")
	if _, err := from.Request(echo, &shared.Msg{Value: 1}, 5*time.Second).Result(); err != nil {
		b.Fatal(err)
	}
	m := &shared.Msg{Value: 1}
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

func BenchmarkLocalSend(b *testing.B) { e := hwLocal(b); hwSend(b, e, e) }
func BenchmarkLocalRequest(b *testing.B) {
	e := hwLocal(b)
	hwRequest(b, e, e, false)
}
func BenchmarkRemoteSend(b *testing.B) { a, z := hwPair(b); hwSend(b, a, z) }
func BenchmarkRemoteRequest(b *testing.B) {
	a, z := hwPair(b)
	hwRequest(b, a, z, false)
}
func BenchmarkRemoteRequestParallel(b *testing.B) {
	a, z := hwPair(b)
	hwRequest(b, a, z, true)
}
