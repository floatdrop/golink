package inspect_test

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/stats"
	"google.golang.org/grpc/status"

	"github.com/floatdrop/grpcproc"
	"github.com/floatdrop/grpcproc/grpcproctest"
	"github.com/floatdrop/grpcproc/inspect"
	"github.com/floatdrop/grpcproc/internal/testpb"
	inspectv1 "github.com/floatdrop/grpcproc/proto/grpcproc/inspect/v1"
)

// fakeWatch is a server stream whose Send fails.
type fakeWatch struct {
	grpc.ServerStream
	ctx context.Context
}

func (f *fakeWatch) Context() context.Context            { return f.ctx }
func (f *fakeWatch) Send(*inspectv1.WatchResponse) error { return errors.New("client gone") }
func (f *fakeWatch) SetHeader(metadata.MD) error         { return nil }
func (f *fakeWatch) SendHeader(metadata.MD) error        { return nil }
func (f *fakeWatch) SetTrailer(metadata.MD)              {}

// fakePeer is an Inspector client whose Watch fails to open (err), or opens
// a stream that fails on the first Recv (recvErr), or yields one event.
type fakePeer struct {
	inspectv1.InspectorClient
	err, recvErr error
}

func (f *fakePeer) Watch(context.Context, *inspectv1.WatchRequest, ...grpc.CallOption) (grpc.ServerStreamingClient[inspectv1.WatchResponse], error) {
	if f.err != nil {
		return nil, f.err
	}
	return &oneEvent{err: f.recvErr}, nil
}

type oneEvent struct {
	grpc.ClientStream
	err error
}

func (o *oneEvent) Recv() (*inspectv1.WatchResponse, error) {
	if o.err != nil {
		return nil, o.err
	}
	return &inspectv1.WatchResponse{Event: &inspectv1.Event{}}, nil
}

func TestWatchSendFailures(t *testing.T) {
	c := grpcproctest.New(t, "a")
	n := c.Node("a")
	stream := &fakeWatch{ctx: t.Context()}

	// Local: the first event cannot be sent to the client.
	srv := inspect.New(n)
	done := make(chan error, 1)
	go func() { done <- srv.Watch(&inspectv1.WatchRequest{}, stream) }()
	for {
		_, _ = n.Spawn(func(p *grpcproc.Process[*testpb.Ping]) error { return nil })
		select {
		case err := <-done:
			if err == nil || err.Error() != "client gone" {
				t.Fatalf("local: %v", err)
			}
			goto forwarded
		default:
		}
	}
forwarded:
	// Forwarded: the peer's stream cannot be opened, or its event cannot be relayed.
	upstreamErr := errors.New("peer refused")
	srv = inspect.New(n, inspect.WithPeers(func(context.Context, string) (inspectv1.InspectorClient, error) {
		return &fakePeer{err: upstreamErr}, nil
	}))
	if err := srv.Watch(&inspectv1.WatchRequest{Node: "b"}, stream); err == nil || !strings.Contains(err.Error(), "node b: peer refused") {
		t.Fatalf("upstream: %v", err)
	}
	srv = inspect.New(n, inspect.WithPeers(func(context.Context, string) (inspectv1.InspectorClient, error) {
		return &fakePeer{}, nil
	}))
	if err := srv.Watch(&inspectv1.WatchRequest{Node: "b"}, stream); err == nil || err.Error() != "client gone" {
		t.Fatalf("relay: %v", err)
	}
	// The peer's stream ends: its error is returned as is.
	recvErr := errors.New("peer stream ended")
	srv = inspect.New(n, inspect.WithPeers(func(context.Context, string) (inspectv1.InspectorClient, error) {
		return &fakePeer{recvErr: recvErr}, nil
	}))
	if err := srv.Watch(&inspectv1.WatchRequest{Node: "b"}, stream); err == nil || !strings.Contains(err.Error(), "node b: peer stream ended") {
		t.Fatalf("peer end: %v", err)
	}
}

func TestWatchEndsWhenClientCancels(t *testing.T) {
	c := grpcproctest.New(t, "a")
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	// A cancelled client is a clean end, not an error.
	if err := inspect.New(c.Node("a")).Watch(&inspectv1.WatchRequest{}, &fakeWatch{ctx: ctx}); err != nil {
		t.Fatalf("got %v", err)
	}
}

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

func getB(ctx context.Context, srv *inspect.Server) error {
	resp, err := srv.GetNode(ctx, &inspectv1.GetNodeRequest{Node: "b"})
	if err == nil && resp.GetNode().GetId().GetName() != "b" {
		return fmt.Errorf("answered by %v", resp.GetNode().GetId())
	}
	return err
}

func TestWithResolver(t *testing.T) {
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
	if err := inspect.New(grpcproctest.New(t, "c").Node("c")).Close(); err != nil {
		t.Fatal("a server that opened nothing closes cleanly:", err)
	}
}

func TestWithResolverErrors(t *testing.T) {
	// A node the resolver does not know, and a target gRPC cannot dial.
	a := grpcproctest.New(t, "a").Node("a")
	srv := inspect.New(a, inspect.WithResolver(grpcproc.StaticResolver{"bad": "\x7f://not a target"}))
	defer func() { _ = srv.Close() }()
	for _, node := range []string{"unknown", "bad"} {
		if _, err := srv.GetNode(t.Context(), &inspectv1.GetNodeRequest{Node: node}); status.Code(err) != codes.Unavailable {
			t.Fatalf("%s: %v", node, err)
		}
	}
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
	t.Run("Close while dialing", func(t *testing.T) {
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
}

func TestListProcessesForwarded(t *testing.T) {
	c := cluster(t, nil, "a", "b")
	_, _ = c.Node("b").Spawn(func(p *grpcproc.Process[*testpb.Ping]) error { _, err := p.Receive(); return err }, grpcproc.WithLabel("remote"))
	resp, err := client(c, "a").ListProcesses(t.Context(), &inspectv1.ListProcessesRequest{Node: "b", Label: "remote"})
	if err != nil || len(resp.GetProcesses()) != 1 || resp.GetProcesses()[0].GetPid().GetNode() != "b" {
		t.Fatalf("%v %v", resp, err)
	}
}

// okWatch is a server stream that accepts every event.
type okWatch struct{ fakeWatch }

func (*okWatch) Send(*inspectv1.WatchResponse) error { return nil }

func TestWatchEndsWithUnavailableWhenNodeStops(t *testing.T) {
	// A node on its own, not behind a gRPC server that would cancel the
	// stream first: only the node stopping can end this Watch.
	n, err := grpcproc.NewNode(grpcproc.Config{Name: "solo", Resolver: grpcproc.StaticResolver{}})
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- inspect.New(n).Watch(&inspectv1.WatchRequest{}, &okWatch{fakeWatch{ctx: t.Context()}}) }()
	time.Sleep(20 * time.Millisecond)
	_ = n.Stop(t.Context())
	if err := <-done; status.Code(err) != codes.Unavailable {
		t.Fatalf("got %v", err)
	}
}
