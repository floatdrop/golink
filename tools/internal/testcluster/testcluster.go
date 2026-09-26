// Package testcluster builds the cluster the tool tests look at.
package testcluster

import (
	"context"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"

	"github.com/floatdrop/golink"
	"github.com/floatdrop/golink/actor"
	"github.com/floatdrop/golink/golinktest"
	"github.com/floatdrop/golink/inspect"
	"github.com/floatdrop/golink/internal/testpb"
	inspectv1 "github.com/floatdrop/golink/proto/golink/inspect/v1"
)

// Fixture is what Start leaves running on node "a".
type Fixture struct {
	C        *golinktest.Cluster
	Sup      golink.PID // a supervisor with children "w1" and "w2"
	Stuck    golink.PID // "stuck": busy in a handler, 3 messages waiting
	Talker   golink.PID // "talker": publishes state=ready through WithInspect
	Echo     golink.PID // "echo" on node "b"
	Release  func()     // unblocks "stuck"
	Resolver golink.Resolver
}

func worker(p *golink.Process[*testpb.Ping]) error {
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
	var c *golinktest.Cluster
	var dialer *inspect.Dialer
	c = golinktest.NewWith(t, []golinktest.Option{golinktest.WithServices(func(n *golink.Node, s *grpc.Server) {
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
	}}, golink.WithName("sup"))
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
	stuck, _ := golink.Spawn[*testpb.Ping](a, func(p *golink.Process[*testpb.Ping]) error {
		if _, err := p.Receive(); err != nil {
			return err
		}
		<-release
		return worker(p)
	}, golink.WithName("stuck"), golink.WithLabel("stuck"))
	f.Stuck = stuck.PID()
	for range 4 {
		_ = a.Send(stuck, &testpb.Ping{})
	}
	talker, _ := golink.Spawn[proto.Message](a, func(p *golink.Process[proto.Message]) error {
		for {
			if _, err := p.Receive(); err != nil {
				return err
			}
		}
	}, golink.WithName("talker"), golink.WithInspect(func() map[string]string { return map[string]string{"state": "ready"} }))
	f.Talker = talker.PID()
	echo, _ := golink.Spawn[*testpb.Ping](b, func(p *golink.Process[*testpb.Ping]) error {
		for {
			m, err := p.Receive()
			if err != nil {
				return err
			}
			if m.IsCall() {
				_ = p.Reply(m, &testpb.Pong{}, nil)
			}
		}
	}, golink.WithName("echo"))
	f.Echo = echo.PID()
	if _, err := a.Call[*testpb.Pong](t.Context(), echo, &testpb.Ping{}); err != nil {
		t.Fatal(err)
	}
	time.Sleep(20 * time.Millisecond) // the stuck process takes its first message
	return f
}
