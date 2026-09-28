package inspect_test

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"
	"testing/synctest"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/stats"
	"google.golang.org/grpc/status"

	"github.com/floatdrop/grpcproc"
	"github.com/floatdrop/grpcproc/grpcproctest"
	"github.com/floatdrop/grpcproc/inspect"
	inspectv1 "github.com/floatdrop/grpcproc/proto/grpcproc/inspect/v1"
)

// connCounter counts the transport connections a client opens.
type connCounter struct{ n atomic.Int32 }

func (*connCounter) TagRPC(ctx context.Context, _ *stats.RPCTagInfo) context.Context   { return ctx }
func (*connCounter) HandleRPC(context.Context, stats.RPCStats)                         {}
func (*connCounter) TagConn(ctx context.Context, _ *stats.ConnTagInfo) context.Context { return ctx }
func (c *connCounter) HandleConn(_ context.Context, s stats.ConnStats) {
	if _, ok := s.(*stats.ConnBegin); ok {
		c.n.Add(1)
	}
}

// resolverCluster runs nodes a and b with plain Inspectors, and returns a
// server on a that reaches b through resolve, which wraps the cluster's,
// and the count of connections it opens.
func resolverCluster(t *testing.T, resolve func(ctx context.Context, node string, cluster grpcproc.Resolver) (string, error)) (*inspect.Server, *connCounter) {
	t.Helper()
	c := grpcproctest.NewWith(t, []grpcproctest.Option{grpcproctest.WithServices(func(n *grpcproc.Node, s *grpc.Server) {
		inspect.New(n).Register(s)
	})}, "a", "b")
	r := grpcproc.ResolverFunc(func(ctx context.Context, node string) (string, error) { return resolve(ctx, node, c.Resolver()) })
	conns := &connCounter{}
	srv := inspect.New(c.Node("a"), inspect.WithResolver(r, append(c.DialOptions(), grpc.WithStatsHandler(conns))...))
	t.Cleanup(func() { _ = srv.Close() })
	return srv, conns
}

// getB asks srv about node b, and fails unless b is the one that answers.
func getB(ctx context.Context, srv *inspect.Server) error {
	resp, err := srv.GetNode(ctx, &inspectv1.GetNodeRequest{Node: "b"})
	if err == nil && resp.GetNode().GetId().GetName() != "b" {
		return fmt.Errorf("answered by %v", resp.GetNode().GetId())
	}
	return err
}

func TestWithResolver(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var resolves atomic.Int32
		srv, _ := resolverCluster(t, func(ctx context.Context, node string, cluster grpcproc.Resolver) (string, error) {
			resolves.Add(1)
			return cluster.Resolve(ctx, node)
		})
		for range 2 {
			if err := getB(t.Context(), srv); err != nil {
				t.Fatal(err)
			}
		}
		if n := resolves.Load(); n != 1 {
			t.Fatalf("%d resolves: the connection was not kept", n)
		}
		// Close releases it, twice is fine, and forwarding fails from then on.
		if err := errors.Join(srv.Close(), srv.Close()); err != nil {
			t.Fatal(err)
		}
		if err := getB(t.Context(), srv); status.Code(err) != codes.Unavailable {
			t.Fatalf("after Close: %v", err)
		}
		c := grpcproctest.New(t, "c").Node("c")
		if err := errors.Join(inspect.New(c).Close(), inspect.New(c, inspect.WithPeers(nil)).Close()); err != nil {
			t.Fatal("a server that opened nothing closes cleanly:", err)
		}
	})
}

// With no option, a server forwards through its node's own Dial.
func TestForwardsThroughTheNodeByDefault(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		c := grpcproctest.NewWith(t, []grpcproctest.Option{grpcproctest.WithServices(func(n *grpcproc.Node, s *grpc.Server) {
			inspect.New(n).Register(s)
		})}, "a", "b")
		srv := inspect.New(c.Node("a"))
		defer func() { _ = srv.Close() }()
		if err := getB(t.Context(), srv); err != nil {
			t.Fatal(err)
		}
	})
}

func TestWithResolverErrors(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		// A node the resolver does not know, and a target gRPC cannot dial.
		a := grpcproctest.New(t, "a").Node("a")
		srv := inspect.New(a, inspect.WithResolver(grpcproc.StaticResolver{"bad": "\x7f://not a target"}))
		defer func() { _ = srv.Close() }()
		for _, node := range []string{"unknown", "bad"} {
			if _, err := srv.GetNode(t.Context(), &inspectv1.GetNodeRequest{Node: node}); status.Code(err) != codes.Unavailable {
				t.Fatalf("%s: %v", node, err)
			}
		}
	})
}

func TestWithResolverRaces(t *testing.T) {
	// Resolves wait until released, so requests can be caught mid-dial.
	gate := func() (chan struct{}, chan struct{}, func(context.Context, string, grpcproc.Resolver) (string, error)) {
		entered, release := make(chan struct{}, 2), make(chan struct{})
		return entered, release, func(ctx context.Context, node string, cluster grpcproc.Resolver) (string, error) {
			entered <- struct{}{}
			select {
			case <-release:
			case <-ctx.Done():
				return "", ctx.Err()
			}
			return cluster.Resolve(ctx, node)
		}
	}
	t.Run("two first requests share one connection", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			entered, release, resolve := gate()
			srv, conns := resolverCluster(t, resolve)
			errs := make(chan error, 2)
			for range 2 {
				go func() { errs <- getB(t.Context(), srv) }()
			}
			<-entered
			<-entered
			close(release)
			for range 2 {
				if err := <-errs; err != nil {
					t.Fatal(err)
				}
			}
			if n := conns.n.Load(); n != 1 {
				t.Fatalf("%d connections, want one shared", n)
			}
		})
	})
	t.Run("Close while dialing", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			entered, release, resolve := gate()
			srv, _ := resolverCluster(t, resolve)
			errs := make(chan error, 1)
			go func() { errs <- getB(t.Context(), srv) }()
			<-entered
			if err := srv.Close(); err != nil {
				t.Fatal(err)
			}
			close(release)
			if err := <-errs; status.Code(err) != codes.Unavailable {
				t.Fatalf("got %v", err)
			}
		})
	})
}
