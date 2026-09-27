// Package testcluster builds the cluster the tool tests look at.
package testcluster

import (
	"context"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"

	"github.com/floatdrop/grpcproc"
	"github.com/floatdrop/grpcproc/actor"
	"github.com/floatdrop/grpcproc/grpcproctest"
	"github.com/floatdrop/grpcproc/inspect"
	"github.com/floatdrop/grpcproc/internal/testpb"
	inspectv1 "github.com/floatdrop/grpcproc/proto/grpcproc/inspect/v1"
)

// Fixture is what Start leaves running on node "a".
type Fixture struct {
	C        *grpcproctest.Cluster
	Sup      grpcproc.PID // a supervisor with children "w1" and "w2"
	Stuck    grpcproc.PID // "stuck": busy in a handler, 3 messages waiting
	Talker   grpcproc.PID // "talker": publishes state=ready through WithInspect
	Echo     grpcproc.PID // "echo" on node "b"
	Release  func()       // unblocks "stuck"
	Resolver grpcproc.Resolver
}

func worker(p *grpcproc.Process[*testpb.Ping]) error {
	for {
		if _, err := p.Receive(); err != nil {
			return err
		}
	}
}

// Start runs nodes a and b (and any others named) with Inspectors that
// forward to each other, and links a to b.
func Start(t *testing.T, more ...string) *Fixture {
	t.Helper()
	var c *grpcproctest.Cluster
	var dialer *inspect.Dialer
	c = grpcproctest.NewWith(t, []grpcproctest.Option{grpcproctest.WithServices(func(n *grpcproc.Node, s *grpc.Server) {
		inspect.New(n, inspect.WithPeers(func(ctx context.Context, node string) (inspectv1.InspectorClient, error) {
			return dialer.Peer(ctx, node)
		})).Register(s)
	})}, append([]string{"a", "b"}, more...)...)
	dialer = inspect.NewDialer(c.Resolver(), c.DialOptions()...)
	t.Cleanup(func() { _ = dialer.Close() })
	a, b := c.Node("a"), c.Node("b")

	f := &Fixture{C: c, Resolver: c.Resolver()}
	var err error
	f.Sup, err = actor.Supervise(a, actor.Spec{Children: []actor.ChildSpec{
		actor.ChildFunc("w1", worker), actor.ChildFunc("w2", worker),
	}}, grpcproc.WithName("sup"))
	if err != nil {
		t.Fatal(err)
	}
	release := make(chan struct{})
	f.Release = func() {
		select {
		case <-release:
		default:
			close(release)
		}
	}
	t.Cleanup(f.Release)
	stuck, _ := grpcproc.Spawn[*testpb.Ping](a, func(p *grpcproc.Process[*testpb.Ping]) error {
		if _, err := p.Receive(); err != nil {
			return err
		}
		<-release
		return worker(p)
	}, grpcproc.WithName("stuck"), grpcproc.WithLabel("stuck"))
	f.Stuck = stuck.PID()
	for range 4 {
		_ = a.Send(stuck, &testpb.Ping{})
	}
	talker, _ := grpcproc.Spawn[proto.Message](a, func(p *grpcproc.Process[proto.Message]) error {
		for {
			if _, err := p.Receive(); err != nil {
				return err
			}
		}
	}, grpcproc.WithName("talker"), grpcproc.WithInspect(func() map[string]string { return map[string]string{"state": "ready"} }))
	f.Talker = talker.PID()
	echo, _ := grpcproc.Spawn[*testpb.Ping](b, func(p *grpcproc.Process[*testpb.Ping]) error {
		for {
			m, err := p.Receive()
			if err != nil {
				return err
			}
			if m.IsCall() {
				_ = p.Reply(m, &testpb.Pong{}, nil)
			}
		}
	}, grpcproc.WithName("echo"))
	f.Echo = echo.PID()
	if _, err := a.Call[*testpb.Pong](t.Context(), echo, &testpb.Ping{}); err != nil {
		t.Fatal(err)
	}
	time.Sleep(20 * time.Millisecond) // the stuck process takes its first message
	return f
}
