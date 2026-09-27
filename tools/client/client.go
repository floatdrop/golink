// Package client is a typed wrapper over grpcproc's Inspector, shared by
// grpcprocctl and its MCP server: flat, readable views of nodes, processes and
// events, target parsing, and a walk over every node of a cluster.
package client

import (
	"cmp"
	"context"
	"fmt"
	"log/slog"
	"slices"
	"strconv"
	"strings"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/durationpb"

	"github.com/floatdrop/grpcproc"
	"github.com/floatdrop/grpcproc/inspect"
	inspectv1 "github.com/floatdrop/grpcproc/proto/grpcproc/inspect/v1"
	grpcprocv1 "github.com/floatdrop/grpcproc/proto/grpcproc/v1"
)

// Client talks to one Inspector; requests about other nodes are forwarded
// by it.
type Client struct {
	rpc inspectv1.InspectorClient
	now func() time.Time
}

// New wraps a connection to a node serving grpcproc.inspect.v1.Inspector.
func New(cc grpc.ClientConnInterface) *Client {
	return &Client{rpc: inspectv1.NewInspectorClient(cc), now: time.Now}
}

// ParseTarget reads a process reference: a PID as grpcproc prints it,
// "<node.incarnation.id>", or a registered name. A name is looked up on node.
func ParseTarget(s string) (*inspectv1.Target, error) {
	if inner, ok := strings.CutPrefix(s, "<"); ok {
		inner, ok = strings.CutSuffix(inner, ">")
		if !ok {
			return nil, fmt.Errorf("bad pid %q: want <node.incarnation.id>", s)
		}
		// Node names may contain dots; incarnations and ids do not.
		rest, id, ok1 := strings.CutLast(inner, ".")
		node, inc, ok2 := strings.CutLast(rest, ".")
		if !ok1 || !ok2 {
			return nil, fmt.Errorf("bad pid %q: want <node.incarnation.id>", s)
		}
		pid := &grpcprocv1.PID{Node: node}
		var err1, err2 error
		pid.Incarnation, err1 = strconv.ParseUint(inc, 10, 64)
		pid.Id, err2 = strconv.ParseUint(id, 10, 64)
		if err1 != nil || err2 != nil || pid.Node == "" {
			return nil, fmt.Errorf("bad pid %q: want <node.incarnation.id>", s)
		}
		return &inspectv1.Target{Kind: &inspectv1.Target_Pid{Pid: pid}}, nil
	}
	if s == "" {
		return nil, fmt.Errorf("a process is required: <node.incarnation.id> or a name")
	}
	return &inspectv1.Target{Kind: &inspectv1.Target_Name{Name: s}}, nil
}

// ParseLevel reads a log level: debug, info, warn, error, or a number.
func ParseLevel(s string) (slog.Level, error) {
	var l slog.Level
	if err := l.UnmarshalText([]byte(s)); err == nil {
		return l, nil
	}
	n, err := strconv.Atoi(s)
	if err != nil {
		return 0, fmt.Errorf("bad level %q: want debug, info, warn, error or a number", s)
	}
	return slog.Level(n), nil
}

// Node describes one node; "" is the node serving the Inspector.
func (c *Client) Node(ctx context.Context, node string) (NodeView, error) {
	resp, err := c.rpc.GetNode(ctx, &inspectv1.GetNodeRequest{Node: node})
	if err != nil {
		return NodeView{}, err
	}
	return c.nodeView(inspect.NodeInfo(resp.GetNode())), nil
}

// Cluster describes every node reachable from this one by following links,
// in the order found. A node that cannot be reached is reported with Error.
func (c *Client) Cluster(ctx context.Context) ([]NodeView, error) {
	first, err := c.Node(ctx, "")
	if err != nil {
		return nil, err
	}
	out := []NodeView{first}
	seen := map[string]bool{first.Name: true}
	for i := 0; i < len(out); i++ {
		for _, l := range out[i].Links {
			if seen[l.Peer] {
				continue
			}
			seen[l.Peer] = true
			v, err := c.Node(ctx, l.Peer)
			if err != nil {
				v = NodeView{Name: l.Peer, Error: err.Error()}
			}
			out = append(out, v)
		}
	}
	return out, nil
}

// Filter narrows Processes; zero fields match everything.
type Filter struct {
	Name       string // substring of a registered name
	Label      string
	State      string // idle, running, waiting-reply, exiting
	MinMailbox int
}

// Processes lists the processes of node that match f, ordered by PID.
func (c *Client) Processes(ctx context.Context, node string, f Filter) ([]ProcessView, error) {
	req := &inspectv1.ListProcessesRequest{Node: node, Name: f.Name, Label: f.Label, MinMailbox: uint32(max(f.MinMailbox, 0))}
	if f.State != "" {
		s, err := parseState(f.State)
		if err != nil {
			return nil, err
		}
		req.State = s
	}
	resp, err := c.rpc.ListProcesses(ctx, req)
	if err != nil {
		return nil, err
	}
	out := make([]ProcessView, 0, len(resp.GetProcesses()))
	for _, p := range resp.GetProcesses() {
		out = append(out, c.processView(inspect.ProcessInfo(p)))
	}
	return out, nil
}

func parseState(s string) (inspectv1.ProcessState, error) {
	for st := grpcproc.StateIdle; st <= grpcproc.StateExiting; st++ {
		if st.String() == s {
			return inspectv1.ProcessState(st + 1), nil
		}
	}
	return 0, fmt.Errorf("bad state %q: want idle, running, waiting-reply or exiting", s)
}

// Process describes one process; with ask, also what it publishes about
// itself, waiting up to wait for a busy process to answer.
func (c *Client) Process(ctx context.Context, node, target string, ask bool, wait time.Duration) (ProcessView, error) {
	t, err := ParseTarget(target)
	if err != nil {
		return ProcessView{}, err
	}
	req := &inspectv1.GetProcessRequest{Node: node, Target: t, Inspect: ask}
	if wait > 0 {
		req.InspectTimeout = durationpb.New(wait)
	}
	resp, err := c.rpc.GetProcess(ctx, req)
	if err != nil {
		return ProcessView{}, err
	}
	v := c.processView(inspect.ProcessInfo(resp.GetProcess()))
	v.Inspect, v.InspectError = resp.GetInspect(), resp.GetInspectError()
	return v, nil
}

// Exit asks a process to exit with reason ("killed" when empty).
func (c *Client) Exit(ctx context.Context, node, target, reason string) error {
	t, err := ParseTarget(target)
	if err != nil {
		return err
	}
	_, err = c.rpc.Exit(ctx, &inspectv1.ExitRequest{Node: node, Target: t, Reason: reason})
	return err
}

// SetLogLevel sets a process's log threshold.
func (c *Client) SetLogLevel(ctx context.Context, node, target, level string) error {
	t, err := ParseTarget(target)
	if err != nil {
		return err
	}
	l, err := ParseLevel(level)
	if err != nil {
		return err
	}
	_, err = c.rpc.SetLogLevel(ctx, &inspectv1.SetLogLevelRequest{Node: node, Target: t, Level: int32(l)})
	return err
}

// Watch streams node's events to fn until ctx is done or fn returns false.
// It returns nil when either ends it.
func (c *Client) Watch(ctx context.Context, node string, fn func(EventView) bool) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	stream, err := c.rpc.Watch(ctx, &inspectv1.WatchRequest{Node: node})
	if err != nil {
		return err
	}
	for {
		resp, err := stream.Recv()
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			return err
		}
		if !fn(c.eventView(inspect.Event(resp.GetEvent()))) {
			return nil
		}
	}
}

// SortProcesses orders ps by pid (as listed), or by mailbox, received or
// sent, largest first.
func SortProcesses(ps []ProcessView, by string) error {
	var key func(ProcessView) uint64
	switch by {
	case "", "pid":
		return nil
	case "mailbox":
		key = func(p ProcessView) uint64 { return uint64(p.Mailbox) }
	case "received":
		key = func(p ProcessView) uint64 { return p.Received }
	case "sent":
		key = func(p ProcessView) uint64 { return p.Sent }
	default:
		return fmt.Errorf("bad sort %q: want pid, mailbox, received or sent", by)
	}
	slices.SortStableFunc(ps, func(a, b ProcessView) int { return cmp.Compare(key(b), key(a)) })
	return nil
}
