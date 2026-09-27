// Package ergo benchmarks ergo.services/ergo; see ../README.md.
package ergo

import (
	"net"
	"strconv"
	"testing"

	"ergo.services/ergo"
	"ergo.services/ergo/act"
	"ergo.services/ergo/gen"
	"ergo.services/ergo/net/registrar"

	"github.com/floatdrop/grpcproc/benchmarks/internal/shared"
)

// Msg is what Ergo sends in place of shared.Msg: its network registers
// struct values, not pointers, so a protobuf cannot go remote. It carries
// the same int64.
type Msg struct{ Value int64 }

type sink struct {
	act.Actor
	want, got int64
	done      chan struct{}
}

func (s *sink) HandleMessage(_ gen.PID, message any) error {
	if _, ok := message.(Msg); ok {
		s.got++
		if s.got == s.want {
			close(s.done)
		}
	}
	return nil
}

type echo struct{ act.Actor }

func (*echo) HandleCall(_ gen.PID, _ gen.Ref, request any) (any, error) {
	return request, nil
}

// start starts a node, stopped when the benchmark ends.
func start(b *testing.B, name gen.Atom, opts gen.NodeOptions) gen.Node {
	b.Helper()
	opts.Log.Level = gen.LogLevelDisabled
	opts.Log.DefaultLogger.Disable = true
	n, err := ergo.StartNode(name, opts)
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(n.Stop)
	return n
}

func local(b *testing.B) gen.Node {
	var opts gen.NodeOptions
	opts.Network.Mode = gen.NetworkModeDisabled
	return start(b, "solo@127.0.0.1", opts)
}

// pair starts two nodes that find each other through Ergo's embedded
// registrar, the default, on a port of their own rather than the shared
// 4499. Software keepalive is off: with it, a timer Reset its flusher
// relies on is now and then lost under parallel calls, and their batch
// waits past the call timeout (see ../README.md). It sends nothing while
// messages flow.
func pair(b *testing.B) (a, z gen.Node) {
	reg := registrarPort(b)
	mk := func(name gen.Atom) gen.Node {
		var opts gen.NodeOptions
		opts.Network.Cookie = "bench"
		opts.Network.Flags = gen.DefaultNetworkFlags
		opts.Network.Flags.EnableSoftwareKeepAlive = 0
		opts.Network.Registrar = registrar.Create(registrar.Options{Port: reg})
		opts.Network.Acceptors = []gen.AcceptorOptions{{
			Host: "127.0.0.1", Port: uint16(shared.FreePort(b)), PortRange: 1,
		}}
		n := start(b, name, opts)
		if err := n.Network().RegisterType(Msg{}); err != nil {
			b.Fatal(err)
		}
		return n
	}
	return mk("a@127.0.0.1"), mk("z@127.0.0.1")
}

// registrarPort returns a port free for both TCP and UDP. The embedded
// registrar takes both, and when it cannot, every node becomes a client of
// a server nobody runs, and fails to start.
func registrarPort(b *testing.B) uint16 {
	b.Helper()
	for range 100 {
		ln, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			b.Fatal(err)
		}
		port := ln.Addr().(*net.TCPAddr).Port
		pc, err := net.ListenPacket("udp", ":"+strconv.Itoa(port))
		_ = ln.Close()
		if err == nil {
			_ = pc.Close()
			return uint16(port)
		}
	}
	b.Fatal("no port free for both TCP and UDP")
	return 0
}

func spawn(b *testing.B, n gen.Node, name gen.Atom, f gen.ProcessFactory) gen.PID {
	b.Helper()
	pid, err := n.SpawnRegister(name, f, gen.ProcessOptions{})
	if err != nil {
		b.Fatal(err)
	}
	return pid
}

func newEcho() gen.ProcessBehavior { return &echo{} }

func send(b *testing.B, from, to gen.Node) {
	s := &sink{want: int64(b.N), done: make(chan struct{})}
	pid := spawn(b, to, "sink", func() gen.ProcessBehavior { return s })
	e := spawn(b, to, "echo", newEcho)
	if _, err := from.Call(e, Msg{Value: 1}); err != nil { // warm up
		b.Fatal(err)
	}
	m := Msg{Value: 1}
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		if err := from.Send(pid, m); err != nil {
			b.Fatal(err)
		}
	}
	<-s.done
}

func request(b *testing.B, from, to gen.Node, parallel bool) {
	e := spawn(b, to, "echo", newEcho)
	if _, err := from.Call(e, Msg{Value: 1}); err != nil {
		b.Fatal(err)
	}
	m := Msg{Value: 1}
	b.ReportAllocs()
	b.ResetTimer()
	if parallel {
		b.RunParallel(func(pb *testing.PB) {
			for pb.Next() {
				if _, err := from.Call(e, m); err != nil {
					b.Fatal(err)
				}
			}
		})
		return
	}
	for range b.N {
		if _, err := from.Call(e, m); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkLocalSend(b *testing.B)    { n := local(b); send(b, n, n) }
func BenchmarkLocalRequest(b *testing.B) { n := local(b); request(b, n, n, false) }
func BenchmarkRemoteSend(b *testing.B)   { a, z := pair(b); send(b, a, z) }
func BenchmarkRemoteRequest(b *testing.B) {
	a, z := pair(b)
	request(b, a, z, false)
}
func BenchmarkRemoteRequestParallel(b *testing.B) {
	a, z := pair(b)
	request(b, a, z, true)
}
