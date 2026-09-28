package grpcproc_test

import (
	"testing"
	"testing/synctest"

	"google.golang.org/grpc"
	"google.golang.org/grpc/health"
	"google.golang.org/grpc/health/grpc_health_v1"

	"github.com/floatdrop/grpcproc"
	"github.com/floatdrop/grpcproc/grpcproctest"
)

// Dial reaches another service on a peer's server, as the node reaches the
// peer: through its resolver and dial options.
func TestDialReachesAPeersOtherServices(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		c := grpcproctest.NewWith(t, []grpcproctest.Option{grpcproctest.WithServices(func(_ *grpcproc.Node, s *grpc.Server) {
			grpc_health_v1.RegisterHealthServer(s, health.NewServer())
		})}, "a", "b")
		cc, err := c.Node("a").Dial(t.Context(), "b")
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = cc.Close() }()
		r, err := grpc_health_v1.NewHealthClient(cc).Check(t.Context(), &grpc_health_v1.HealthCheckRequest{})
		if err != nil || r.GetStatus() != grpc_health_v1.HealthCheckResponse_SERVING {
			t.Fatalf("%v %v", r, err)
		}
	})
}

func TestDialAnUnknownPeer(t *testing.T) {
	n, err := grpcproc.NewNode(grpcproc.Config{Name: "a", Resolver: grpcproc.StaticResolver{}})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := n.Dial(t.Context(), "nowhere"); err == nil {
		t.Fatal("a peer the resolver does not know: want an error")
	}
}

func TestMembershipIsTheConfigs(t *testing.T) {
	m := &fakeMembership{}
	with, err := grpcproc.NewNode(grpcproc.Config{Name: "a", Resolver: grpcproc.StaticResolver{}, Membership: m})
	if err != nil {
		t.Fatal(err)
	}
	without, err := grpcproc.NewNode(grpcproc.Config{Name: "b", Resolver: grpcproc.StaticResolver{}})
	if err != nil {
		t.Fatal(err)
	}
	if with.Membership() != m || without.Membership() != nil {
		t.Fatalf("%v %v", with.Membership(), without.Membership())
	}
}
