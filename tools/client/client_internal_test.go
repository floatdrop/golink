package client

import (
	"cmp"
	"context"
	"errors"
	"io"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"

	inspectv1 "github.com/floatdrop/grpcproc/proto/grpcproc/inspect/v1"
)

// fake answers GetNode for "a" (linked to "b", and failing to dial "c"
// and "d") and fails everything else, or hands out a scripted event stream
// that then ends with end (errFake by default).
type fake struct {
	inspectv1.InspectorClient
	events []*inspectv1.Event
	end    error
}

var errFake = errors.New("unavailable")

func (f *fake) GetNode(_ context.Context, req *inspectv1.GetNodeRequest, _ ...grpc.CallOption) (*inspectv1.GetNodeResponse, error) {
	switch req.GetNode() {
	case "":
		return &inspectv1.GetNodeResponse{Node: &inspectv1.NodeInfo{
			Id: &inspectv1.NodeID{Name: "a", Incarnation: 1},
			Links: []*inspectv1.Link{
				{Peer: &inspectv1.NodeID{Name: "b"}, Outbound: true, State: inspectv1.LinkState_LINK_STATE_UP, Queued: 3},
				{Peer: &inspectv1.NodeID{Name: "c"}, Outbound: true, State: inspectv1.LinkState_LINK_STATE_DOWN, LastError: "refused",
					RetryAt: timestamppb.New(time.Now().Add(time.Hour))},
				{Peer: &inspectv1.NodeID{Name: "d"}, Outbound: true, State: inspectv1.LinkState_LINK_STATE_DOWN, LastError: "refused",
					RetryAt: timestamppb.New(time.Now().Add(-time.Second))},
				// b again, as a down link: it is asked once, as a live peer.
				{Peer: &inspectv1.NodeID{Name: "b"}, Outbound: true, State: inspectv1.LinkState_LINK_STATE_DOWN},
			},
		}}, nil
	}
	return nil, errFake
}

func (f *fake) ListProcesses(context.Context, *inspectv1.ListProcessesRequest, ...grpc.CallOption) (*inspectv1.ListProcessesResponse, error) {
	return nil, errFake
}

func (f *fake) Watch(context.Context, *inspectv1.WatchRequest, ...grpc.CallOption) (grpc.ServerStreamingClient[inspectv1.WatchResponse], error) {
	if f.events == nil {
		return nil, errFake
	}
	return &stream{events: f.events, end: cmp.Or(f.end, errFake)}, nil
}

type stream struct {
	grpc.ClientStream
	events []*inspectv1.Event
	end    error
}

func (s *stream) Recv() (*inspectv1.WatchResponse, error) {
	if len(s.events) == 0 {
		return nil, s.end
	}
	e := s.events[0]
	s.events = s.events[1:]
	return &inspectv1.WatchResponse{Event: e}, nil
}

func TestFailuresAndOddities(t *testing.T) {
	c := &Client{rpc: &fake{}, now: time.Now}
	ctx := t.Context()
	// A node that never started has no uptime; a peer that cannot be
	// reached is listed with why.
	nodes, err := c.Cluster(ctx)
	if err != nil || len(nodes) != 4 || nodes[0].Uptime != "" || nodes[0].Links[0].Age != "" || nodes[1].Name != "b" || nodes[1].Error == "" ||
		nodes[2].Name != "c" || nodes[2].Error == "" || nodes[3].Name != "d" || nodes[3].Error == "" {
		t.Fatalf("%+v %v", nodes, err)
	}
	// An outbound link's queue, and when a down one is dialed again: not
	// at all once that time has passed.
	if up, down, due := nodes[0].Links[0], nodes[0].Links[1], nodes[0].Links[2]; up.Queued != 3 || up.RetryIn != "" || down.State != "down" || down.RetryIn == "" || due.RetryIn != "" {
		t.Fatalf("%+v %+v %+v", up, down, due)
	}
	if _, err := c.Node(ctx, "b"); !errors.Is(err, errFake) {
		t.Fatal(err)
	}
	if _, err := c.Processes(ctx, "", Filter{}); !errors.Is(err, errFake) {
		t.Fatal(err)
	}
	if err := c.Watch(ctx, "", func(EventView) bool { return true }); !errors.Is(err, errFake) {
		t.Fatal(err)
	}
	// A stream that breaks is an error; a link-up event names its peer.
	c.rpc = &fake{events: []*inspectv1.Event{{Time: timestamppb.Now(), Kind: &inspectv1.Event_LinkUp{LinkUp: &inspectv1.NodeID{Name: "b", Incarnation: 2}}}}}
	var got []EventView
	if err := c.Watch(ctx, "", func(e EventView) bool { got = append(got, e); return true }); !errors.Is(err, errFake) {
		t.Fatal(err)
	}
	if len(got) != 1 || got[0].Kind != "link-up" || got[0].Peer != "b#2" {
		t.Fatalf("%+v", got)
	}
	// A stream the Inspector ends cleanly is a clean end; a watch that
	// cannot open because ctx is done, too.
	c.rpc = &fake{events: []*inspectv1.Event{}, end: io.EOF}
	if err := c.Watch(ctx, "", func(EventView) bool { return true }); err != nil {
		t.Fatal(err)
	}
	done, cancel := context.WithCancel(ctx)
	cancel()
	c.rpc = &fake{}
	if err := c.Watch(done, "", func(EventView) bool { return true }); err != nil {
		t.Fatal(err)
	}
	// A cluster whose first node fails is an error.
	c.rpc = unreachable{}
	if _, err := c.Cluster(ctx); !errors.Is(err, errFake) {
		t.Fatal(err)
	}
}

func TestDeadlinePassed(t *testing.T) {
	passed, cancelPassed := context.WithDeadline(t.Context(), time.Now().Add(-time.Second))
	defer cancelPassed()
	later, cancelLater := context.WithTimeout(t.Context(), time.Hour)
	defer cancelLater()
	deadline := status.Error(codes.DeadlineExceeded, "deadline")
	for i, tc := range []struct {
		ctx  context.Context
		err  error
		want bool
	}{
		{passed, deadline, true},       // gRPC reported our deadline first
		{later, deadline, false},       // not our deadline: it has not passed
		{t.Context(), deadline, false}, // no deadline of ours
		{passed, status.Error(codes.Unavailable, "gone"), false},
		{passed, io.EOF, false},
	} {
		if got := deadlinePassed(tc.ctx, tc.err); got != tc.want {
			t.Errorf("case %d (%v): got %v", i, tc.err, got)
		}
	}
}
