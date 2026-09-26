// Package goakt benchmarks github.com/tochemey/goakt/v4; see ../README.md.
package goakt

import (
	"context"
	"testing"
	"time"

	"github.com/tochemey/goakt/v4/actor"
	"github.com/tochemey/goakt/v4/log"
	goremote "github.com/tochemey/goakt/v4/remote"

	"github.com/floatdrop/golink/benchmarks/internal/shared"
)

type sink struct {
	want, got int64
	done      chan struct{}
}

func (*sink) PreStart(*actor.Context) error { return nil }
func (*sink) PostStop(*actor.Context) error { return nil }

func (s *sink) Receive(ctx *actor.ReceiveContext) {
	switch ctx.Message().(type) {
	case *shared.Msg:
		s.got++
		if s.got == s.want {
			close(s.done)
		}
	default:
		ctx.Unhandled()
	}
}

type echo struct{}

func (*echo) PreStart(*actor.Context) error { return nil }
func (*echo) PostStop(*actor.Context) error { return nil }

func (*echo) Receive(ctx *actor.ReceiveContext) {
	switch m := ctx.Message().(type) {
	case *shared.Msg:
		ctx.Response(m)
	default:
		ctx.Unhandled()
	}
}

// system starts an actor system; with remoting, it also returns its port.
func system(b *testing.B, name string, remoting bool) (actor.ActorSystem, int) {
	b.Helper()
	opts := []actor.Option{actor.WithLogger(log.DiscardLogger)}
	port := 0
	if remoting {
		port = shared.FreePort(b)
		opts = append(opts, actor.WithRemote(goremote.NewConfig("127.0.0.1", port)))
	}
	sys, err := actor.NewActorSystem(name, opts...)
	if err != nil {
		b.Fatal(err)
	}
	if err := sys.Start(context.Background()); err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { _ = sys.Stop(context.Background()) })
	return sys, port
}

func spawn(b *testing.B, sys actor.ActorSystem, name string, a actor.Actor) *actor.PID {
	b.Helper()
	pid, err := sys.Spawn(context.Background(), name, a)
	if err != nil {
		b.Fatal(err)
	}
	return pid
}

// targets spawns a sink and an echo on the receiving side and returns how
// the sending side addresses them: the PIDs themselves when local, remote
// PIDs looked up the public way when not.
func targets(b *testing.B, remoting bool, s *sink) (sinkPID, echoPID *actor.PID) {
	b.Helper()
	if !remoting {
		sys, _ := system(b, "local", false)
		return spawn(b, sys, "sink", s), spawn(b, sys, "echo", &echo{})
	}
	from, _ := system(b, "from", true)
	to, port := system(b, "to", true)
	spawn(b, to, "sink", s)
	spawn(b, to, "echo", &echo{})
	client := spawn(b, from, "client", &echo{})
	ctx := context.Background()
	var err error
	if sinkPID, err = client.RemoteLookup(ctx, "127.0.0.1", port, "sink"); err != nil {
		b.Fatal(err)
	}
	if echoPID, err = client.RemoteLookup(ctx, "127.0.0.1", port, "echo"); err != nil {
		b.Fatal(err)
	}
	return sinkPID, echoPID
}

func send(b *testing.B, remoting bool) {
	s := &sink{want: int64(b.N), done: make(chan struct{})}
	to, e := targets(b, remoting, s)
	ctx := context.Background()
	if _, err := actor.Ask(ctx, e, &shared.Msg{Value: 1}, 5*time.Second); err != nil { // warm up
		b.Fatal(err)
	}
	m := &shared.Msg{Value: 1}
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		if err := actor.Tell(ctx, to, m); err != nil {
			b.Fatal(err)
		}
	}
	<-s.done
}

func request(b *testing.B, remoting, parallel bool) {
	_, e := targets(b, remoting, &sink{done: make(chan struct{})})
	ctx := context.Background()
	if _, err := actor.Ask(ctx, e, &shared.Msg{Value: 1}, 5*time.Second); err != nil {
		b.Fatal(err)
	}
	m := &shared.Msg{Value: 1}
	b.ReportAllocs()
	b.ResetTimer()
	if parallel {
		b.RunParallel(func(pb *testing.PB) {
			for pb.Next() {
				if _, err := actor.Ask(ctx, e, m, 5*time.Second); err != nil {
					b.Fatal(err)
				}
			}
		})
		return
	}
	for range b.N {
		if _, err := actor.Ask(ctx, e, m, 5*time.Second); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkLocalSend(b *testing.B)             { send(b, false) }
func BenchmarkLocalRequest(b *testing.B)          { request(b, false, false) }
func BenchmarkRemoteSend(b *testing.B)            { send(b, true) }
func BenchmarkRemoteRequest(b *testing.B)         { request(b, true, false) }
func BenchmarkRemoteRequestParallel(b *testing.B) { request(b, true, true) }
