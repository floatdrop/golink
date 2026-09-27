package inspect_test

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
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
		_, _ = grpcproc.Spawn(n, func(p *grpcproc.Process[*testpb.Ping]) error { return nil })
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

func TestDialerErrors(t *testing.T) {
	d := inspect.NewDialer(grpcproc.StaticResolver{"bad": "\x7f://not a target"})
	defer func() { _ = d.Close() }()
	if _, err := d.Peer(t.Context(), "unknown"); err == nil {
		t.Fatal("resolver error expected")
	}
	if _, err := d.Peer(t.Context(), "bad"); err == nil {
		t.Fatal("dial error expected")
	}
}

func TestListProcessesForwarded(t *testing.T) {
	c := cluster(t, nil, "a", "b")
	_, _ = grpcproc.Spawn(c.Node("b"), func(p *grpcproc.Process[*testpb.Ping]) error { _, err := p.Receive(); return err }, grpcproc.WithLabel("remote"))
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
