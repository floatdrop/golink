package client

import (
	"context"
	"strings"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	inspectv1 "github.com/floatdrop/grpcproc/proto/grpcproc/inspect/v1"
)

// electionFake is a cluster of five nodes, each answering differently for
// its elector: a and b lead in two terms, c runs none, d cannot be asked,
// e is too busy to answer, and f cannot be reached at all.
type electionFake struct {
	inspectv1.InspectorClient
}

func (electionFake) GetNode(_ context.Context, req *inspectv1.GetNodeRequest, _ ...grpc.CallOption) (*inspectv1.GetNodeResponse, error) {
	name := req.GetNode()
	if name == "f" {
		return nil, status.Error(codes.Unavailable, "no route to f")
	}
	info := &inspectv1.NodeInfo{Id: &inspectv1.NodeID{Name: name}}
	if name == "" {
		info.Id.Name = "a"
		for _, peer := range []string{"b", "c", "d", "e", "f"} {
			info.Links = append(info.Links, &inspectv1.Link{Peer: &inspectv1.NodeID{Name: peer}, Outbound: true, State: inspectv1.LinkState_LINK_STATE_UP})
		}
	}
	return &inspectv1.GetNodeResponse{Node: info}, nil
}

func (electionFake) GetProcess(_ context.Context, req *inspectv1.GetProcessRequest, _ ...grpc.CallOption) (*inspectv1.GetProcessResponse, error) {
	switch req.GetNode() {
	case "a":
		return &inspectv1.GetProcessResponse{Inspect: map[string]string{"role": "leader", "term": "3", "leader": "a"}}, nil
	case "b":
		return &inspectv1.GetProcessResponse{Inspect: map[string]string{"role": "leader", "term": "5", "leader": "b"}}, nil
	case "c":
		return nil, status.Error(codes.NotFound, "no process named leader/x")
	case "d":
		return nil, status.Error(codes.PermissionDenied, "not for you")
	}
	return &inspectv1.GetProcessResponse{InspectError: "busy for 3s"}, nil
}

func TestElectionAsEachNodeAnswers(t *testing.T) {
	c := &Client{rpc: electionFake{}, now: time.Now}
	views, err := c.Election(t.Context(), "x")
	if err != nil {
		t.Fatal(err)
	}
	var got []string
	for _, v := range views {
		got = append(got, v.Node+":"+v.Role+":"+v.Error)
	}
	want := "a:leader:; b:leader:; d::not for you; e::busy for 3s; f::rpc error: code = Unavailable desc = no route to f"
	if strings.Join(got, "; ") != want {
		t.Errorf("got  %s\nwant %s", strings.Join(got, "; "), want)
	}
	if lead := Leading(views); lead != "b" {
		t.Errorf("leading: %s, the leader of the higher term is b", lead)
	}
	if lead := Leading(views[2:]); lead != "" {
		t.Errorf("leading: %q with no leader", lead)
	}
}

// unreachable is an Inspector that answers nothing.
type unreachable struct{ inspectv1.InspectorClient }

func (unreachable) GetNode(context.Context, *inspectv1.GetNodeRequest, ...grpc.CallOption) (*inspectv1.GetNodeResponse, error) {
	return nil, errFake
}

func TestNothingToAskAboutAnElection(t *testing.T) {
	c := &Client{rpc: unreachable{}, now: time.Now}
	if _, err := c.Election(t.Context(), "x"); err == nil {
		t.Error("an election on a node that cannot be asked")
	}
	if _, err := c.MoveLeader(t.Context(), "x", ""); err == nil {
		t.Error("moved a leader nobody knows")
	}
}

// noLeader is a node whose elector knows no leader.
type noLeader struct{ electionFake }

func (noLeader) GetNode(_ context.Context, _ *inspectv1.GetNodeRequest, _ ...grpc.CallOption) (*inspectv1.GetNodeResponse, error) {
	return &inspectv1.GetNodeResponse{Node: &inspectv1.NodeInfo{Id: &inspectv1.NodeID{Name: "e"}}}, nil
}

func (noLeader) GetProcess(context.Context, *inspectv1.GetProcessRequest, ...grpc.CallOption) (*inspectv1.GetProcessResponse, error) {
	return &inspectv1.GetProcessResponse{Inspect: map[string]string{"role": "follower", "term": "7"}}, nil
}

func TestCordonWithNoLeader(t *testing.T) {
	c := &Client{rpc: noLeader{}, now: time.Now}
	if _, err := c.Cordon(t.Context(), "x", "e", false); err == nil || !strings.Contains(err.Error(), `no node leads "x" now`) {
		t.Errorf("cordon with no leader: %v", err)
	}
}
