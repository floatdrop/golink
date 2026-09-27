package client

import (
	"context"
	"errors"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/timestamppb"

	inspectv1 "github.com/floatdrop/grpcproc/proto/grpcproc/inspect/v1"
)

// fake answers GetNode for "a" (linked to "b", which fails) and fails
// everything else, or hands out a scripted event stream.
type fake struct {
	inspectv1.InspectorClient
	events []*inspectv1.Event
}

var errFake = errors.New("unavailable")

func (f *fake) GetNode(_ context.Context, req *inspectv1.GetNodeRequest, _ ...grpc.CallOption) (*inspectv1.GetNodeResponse, error) {
	switch req.GetNode() {
	case "":
		return &inspectv1.GetNodeResponse{Node: &inspectv1.NodeInfo{
			Id:    &inspectv1.NodeID{Name: "a", Incarnation: 1},
			Links: []*inspectv1.Link{{Peer: &inspectv1.NodeID{Name: "b"}, Outbound: true, State: inspectv1.LinkState_LINK_STATE_UP}},
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
	return &stream{events: f.events}, nil
}

type stream struct {
	grpc.ClientStream
	events []*inspectv1.Event
}

func (s *stream) Recv() (*inspectv1.WatchResponse, error) {
	if len(s.events) == 0 {
		return nil, errFake
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
	if err != nil || len(nodes) != 2 || nodes[0].Uptime != "" || nodes[0].Links[0].Age != "" || nodes[1].Error == "" {
		t.Fatalf("%+v %v", nodes, err)
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
	// A cluster whose first node fails is an error.
	c.rpc = &failing{}
	if _, err := c.Cluster(ctx); !errors.Is(err, errFake) {
		t.Fatal(err)
	}
}

type failing struct{ inspectv1.InspectorClient }

func (failing) GetNode(context.Context, *inspectv1.GetNodeRequest, ...grpc.CallOption) (*inspectv1.GetNodeResponse, error) {
	return nil, errFake
}
