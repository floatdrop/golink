package client

import (
	"context"
	"fmt"
	"slices"
	"strconv"
	"strings"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"

	"github.com/floatdrop/grpcproc/leader"
	leaderv1 "github.com/floatdrop/grpcproc/leader/proto/grpcproc/leader/v1"
	inspectv1 "github.com/floatdrop/grpcproc/proto/grpcproc/inspect/v1"
)

// ElectorView is one node's part in a grpcproc/leader election, as its
// elector says: comparing the nodes is how a split view shows.
type ElectorView struct {
	Node        string   `json:"node"`
	Role        string   `json:"role,omitempty" jsonschema:"leader, candidate, follower, or unclustered (its view is too small to elect)"`
	Term        uint64   `json:"term,omitzero"`
	Leader      string   `json:"leader,omitempty" jsonschema:"the node it follows; empty while it knows none"`
	VotedFor    string   `json:"voted_for,omitempty"`
	View        []string `json:"view,omitempty" jsonschema:"the nodes whose majority elects"`
	Quorum      int      `json:"quorum,omitzero"`
	State       string   `json:"state,omitempty" jsonschema:"the version of the replicated state it holds: the term of the leader that made it, then its sequence"`
	Cordoned    []string `json:"cordoned,omitempty" jsonschema:"nodes that may not lead"`
	Unreachable []string `json:"unreachable,omitempty" jsonschema:"nodes of its view it has not heard from since it lost them"`
	Singleton   string   `json:"singleton,omitempty" jsonschema:"the singleton: its pid on the leader, or none, starting, stopping"`
	Backoff     string   `json:"backoff,omitempty" jsonschema:"how long it holds off campaigning: after its singleton failed, or after it handed over"`
	Error       string   `json:"error,omitempty" jsonschema:"why the node could not be asked"`
}

// Election describes the election called cluster on every node reachable
// from this one that takes part in it, in the order Cluster finds them. A
// node that runs no elector for it is left out; one that cannot be asked is
// listed with its Error.
func (c *Client) Election(ctx context.Context, cluster string) ([]ElectorView, error) {
	nodes, err := c.Cluster(ctx)
	if err != nil {
		return nil, err
	}
	out := c.election(ctx, nodes, cluster)
	if len(out) == 0 {
		return nil, fmt.Errorf("no node runs an election called %q", cluster)
	}
	return out, nil
}

// ElectionView is one grpcproc/leader election, as each node that takes
// part sees it.
type ElectionView struct {
	Cluster  string        `json:"cluster"`
	Leading  string        `json:"leading,omitempty" jsonschema:"the leader of the highest term any node knows; empty while none leads"`
	Electors []ElectorView `json:"electors"`
}

// Elections finds every grpcproc/leader election that a node reachable from
// this one takes part in, by the names its electors are registered under,
// and describes each as Election does, in order of name. An election whose
// electors all exit while it is asked is listed with none.
func (c *Client) Elections(ctx context.Context) ([]ElectionView, error) {
	nodes, err := c.Cluster(ctx)
	if err != nil {
		return nil, err
	}
	prefix := leader.ElectorName("")
	var names []string
	for _, n := range nodes {
		if n.Error != "" {
			continue
		}
		ps, err := c.Processes(ctx, n.Name, Filter{Name: prefix})
		if err != nil {
			return nil, err
		}
		for _, p := range ps {
			// The filter matches a substring: "myleader/x" is not an elector.
			if cluster, ok := strings.CutPrefix(p.Name, prefix); ok && !slices.Contains(names, cluster) {
				names = append(names, cluster)
			}
		}
	}
	slices.Sort(names)
	out := make([]ElectionView, 0, len(names))
	for _, name := range names {
		views := c.election(ctx, nodes, name)
		out = append(out, ElectionView{Cluster: name, Leading: Leading(views), Electors: views})
	}
	return out, nil
}

// election asks each of nodes for its elector of cluster.
func (c *Client) election(ctx context.Context, nodes []NodeView, cluster string) []ElectorView {
	out := []ElectorView{}
	for _, n := range nodes {
		if n.Error != "" {
			out = append(out, ElectorView{Node: n.Name, Error: n.Error})
			continue
		}
		resp, err := c.rpc.GetProcess(ctx, &inspectv1.GetProcessRequest{Node: n.Name, Target: electorOf(cluster), Inspect: true})
		switch {
		case status.Code(err) == codes.NotFound:
		case err != nil:
			out = append(out, ElectorView{Node: n.Name, Error: status.Convert(err).Message()})
		case resp.GetInspectError() != "":
			out = append(out, ElectorView{Node: n.Name, Error: resp.GetInspectError()})
		default:
			out = append(out, electorView(n.Name, resp.GetInspect()))
		}
	}
	return out
}

// Leading is the node views say leads: the leader of the highest term, or
// empty while none does.
func Leading(views []ElectorView) string {
	var lead string
	var term uint64
	for _, v := range views {
		if v.Role == leader.Leader.String() && v.Term >= term {
			lead, term = v.Node, v.Term
		}
	}
	return lead
}

// MoveLeader asks the leader of cluster to hand over: to the node to, or,
// with to empty, to the follower with the latest state. It returns the node
// that led, once that node has told its successor to campaign.
func (c *Client) MoveLeader(ctx context.Context, cluster, to string) (string, error) {
	return c.toLeader(ctx, cluster, &leaderv1.Resign{To: to})
}

// Cordon keeps node from leading cluster, or, with off, lets it lead again.
// It returns the node that led, once a majority holds the change; if node
// was that leader, it is handing over.
func (c *Client) Cordon(ctx context.Context, cluster, node string, off bool) (string, error) {
	return c.toLeader(ctx, cluster, &leaderv1.Cordon{Node: node, Off: off})
}

// toLeader calls the elector of cluster's leader with m.
func (c *Client) toLeader(ctx context.Context, cluster string, m proto.Message) (string, error) {
	views, err := c.Election(ctx, cluster)
	if err != nil {
		return "", err
	}
	lead := Leading(views)
	if lead == "" {
		return "", fmt.Errorf("no node leads %q now: try again once one does", cluster)
	}
	body, err := anypb.New(m)
	if err != nil {
		return "", err
	}
	_, err = c.rpc.Call(ctx, &inspectv1.CallRequest{Node: lead, Target: electorOf(cluster), Body: body})
	return lead, err
}

func electorOf(cluster string) *inspectv1.Target {
	return &inspectv1.Target{Kind: &inspectv1.Target_Name{Name: leader.ElectorName(cluster)}}
}

// electorView reads what an elector publishes about itself.
func electorView(node string, m map[string]string) ElectorView {
	term, _ := strconv.ParseUint(m["term"], 10, 64)
	quorum, _ := strconv.Atoi(m["quorum"])
	return ElectorView{
		Node: node, Role: m["role"], Term: term, Leader: m["leader"], VotedFor: m["voted_for"],
		View: list(m["view"]), Quorum: quorum, State: m["state"], Cordoned: list(m["cordoned"]),
		Unreachable: list(m["unreachable"]), Singleton: m["singleton"], Backoff: m["backoff"],
	}
}

func list(s string) []string {
	if s == "" {
		return nil
	}
	return strings.Split(s, ",")
}
