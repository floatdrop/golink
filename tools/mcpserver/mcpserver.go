// Package mcpserver offers grpcproc's Inspector to AI agents as MCP tools, so
// an agent can investigate a symptom ("orders are slow") by looking at the
// cluster itself: which processes have backlogs, what they say about
// themselves, what is exiting and why.
package mcpserver

import (
	"cmp"
	"context"
	"fmt"
	"slices"
	"time"

	"github.com/modelcontextprotocol/go-sdk/mcp"

	"github.com/floatdrop/grpcproc/tools/client"
)

// Options configures New.
type Options struct {
	// AllowWrites also offers exit_process and set_log_level.
	AllowWrites bool
	// Version is reported to the client.
	Version string
	// Timeout bounds each Inspector request. Default 5s.
	Timeout time.Duration
}

const instructions = `These tools inspect a grpcproc cluster: Go processes (goroutines with a mailbox) that message each other across nodes over gRPC, Erlang style.

- A process has a PID, written <node.incarnation.id>, and may have a registered name. Its label (the message type by default) groups processes of one kind.
- Its mailbox holds messages waiting to be handled. A deep mailbox, or a large oldest_wait, is a backlog: the process cannot keep up, or is stuck in a handler (state running for a long time), or waits on a call (state waiting-reply).
- get_process with inspect returns what the process publishes about itself (its state machine's state, counters); inspect_error "busy" means it is inside a handler right now.
- Supervisors (label supervisor) restart children; their inspect lists each child and its restarts.
- watch_events shows spawns, exits with reasons, links going up and down, and dead letters (messages that found no process, or the wrong type).

Start with cluster_nodes, then list_processes sorted by mailbox to find backlogs, then get_process on the suspects.`

// New returns an MCP server over c.
func New(c *client.Client, o Options) *mcp.Server {
	o.Timeout = cmp.Or(o.Timeout, 5*time.Second)
	s := mcp.NewServer(&mcp.Implementation{Name: "grpcproc", Version: cmp.Or(o.Version, "dev")}, &mcp.ServerOptions{Instructions: instructions})
	t := tools{c: c, timeout: o.Timeout}
	readOnly := &mcp.ToolAnnotations{ReadOnlyHint: true}
	mcp.AddTool(s, &mcp.Tool{Name: "cluster_nodes", Description: "Every node reachable from the one serving the Inspector: counters, links, and which could not be reached.", Annotations: readOnly}, t.clusterNodes)
	mcp.AddTool(s, &mcp.Tool{Name: "node_info", Description: "One node: process counts, dead letters, and each link with its traffic and last error.", Annotations: readOnly}, t.nodeInfo)
	mcp.AddTool(s, &mcp.Tool{Name: "list_processes", Description: "Processes of a node, filtered and sorted. Sort by mailbox to find backlogs.", Annotations: readOnly}, t.listProcesses)
	mcp.AddTool(s, &mcp.Tool{Name: "get_process", Description: "One process by pid or name, with what it says about itself.", Annotations: readOnly}, t.getProcess)
	mcp.AddTool(s, &mcp.Tool{Name: "watch_events", Description: "Collect a node's events for a few seconds: spawns, exits with reasons, links up and down, dead letters.", Annotations: readOnly}, t.watchEvents)
	if o.AllowWrites {
		mcp.AddTool(s, &mcp.Tool{Name: "exit_process", Description: "Ask a process to exit. Its supervisor, if any, may restart it.", Annotations: &mcp.ToolAnnotations{DestructiveHint: new(true)}}, t.exitProcess)
		mcp.AddTool(s, &mcp.Tool{Name: "set_log_level", Description: "Set one process's log level, to see more from it without restarting anything."}, t.setLogLevel)
	}
	return s
}

type tools struct {
	c       *client.Client
	timeout time.Duration
}

func (t tools) ctx(ctx context.Context) (context.Context, context.CancelFunc) {
	return context.WithTimeout(ctx, t.timeout)
}

type nodesOut struct {
	Nodes []client.NodeView `json:"nodes"`
}

func (t tools) clusterNodes(ctx context.Context, _ *mcp.CallToolRequest, _ struct{}) (*mcp.CallToolResult, nodesOut, error) {
	ctx, cancel := t.ctx(ctx)
	defer cancel()
	nodes, err := t.c.Cluster(ctx)
	return nil, nodesOut{Nodes: nodes}, err
}

type nodeIn struct {
	Node string `json:"node,omitempty" jsonschema:"node name; empty for the node serving the Inspector"`
}

func (t tools) nodeInfo(ctx context.Context, _ *mcp.CallToolRequest, in nodeIn) (*mcp.CallToolResult, client.NodeView, error) {
	ctx, cancel := t.ctx(ctx)
	defer cancel()
	n, err := t.c.Node(ctx, in.Node)
	return nil, n, err
}

type listIn struct {
	Node       string `json:"node,omitempty" jsonschema:"node name; empty for the node serving the Inspector"`
	Name       string `json:"name,omitempty" jsonschema:"only processes with a registered name containing this"`
	Label      string `json:"label,omitempty" jsonschema:"only processes with this label"`
	State      string `json:"state,omitempty" jsonschema:"only processes in this state: idle, running, waiting-reply, exiting"`
	MinMailbox int    `json:"min_mailbox,omitempty" jsonschema:"only processes with at least this many waiting messages"`
	Sort       string `json:"sort,omitempty" jsonschema:"pid (default), mailbox, received or sent; largest first"`
	Limit      int    `json:"limit,omitempty" jsonschema:"at most this many; default 100"`
}

type listOut struct {
	Processes []client.ProcessView `json:"processes"`
	Total     int                  `json:"total" jsonschema:"how many matched before the limit"`
}

func (t tools) listProcesses(ctx context.Context, _ *mcp.CallToolRequest, in listIn) (*mcp.CallToolResult, listOut, error) {
	ctx, cancel := t.ctx(ctx)
	defer cancel()
	ps, err := t.c.Processes(ctx, in.Node, client.Filter{Name: in.Name, Label: in.Label, State: in.State, MinMailbox: in.MinMailbox})
	if err != nil {
		return nil, listOut{}, err
	}
	if err := client.SortProcesses(ps, in.Sort); err != nil {
		return nil, listOut{}, err
	}
	out := listOut{Processes: ps, Total: len(ps)}
	if limit := cmp.Or(in.Limit, 100); len(ps) > limit {
		out.Processes = ps[:limit]
	}
	return nil, out, nil
}

type processIn struct {
	Node       string `json:"node,omitempty" jsonschema:"node the name is registered on; not needed for a pid"`
	Process    string `json:"process" jsonschema:"a pid, <node.incarnation.id>, or a registered name"`
	NoInspect  bool   `json:"no_inspect,omitempty" jsonschema:"skip asking the process about itself"`
	WaitMillis int    `json:"wait_ms,omitempty" jsonschema:"how long a busy process has to answer; default 1000"`
}

func (t tools) getProcess(ctx context.Context, _ *mcp.CallToolRequest, in processIn) (*mcp.CallToolResult, client.ProcessView, error) {
	ctx, cancel := t.ctx(ctx)
	defer cancel()
	p, err := t.c.Process(ctx, in.Node, in.Process, !in.NoInspect, time.Duration(cmp.Or(in.WaitMillis, 1000))*time.Millisecond)
	return nil, p, err
}

type watchIn struct {
	Node      string   `json:"node,omitempty" jsonschema:"node to watch; empty for the node serving the Inspector"`
	Seconds   int      `json:"seconds,omitempty" jsonschema:"how long to collect, 1 to 60; default 5"`
	MaxEvents int      `json:"max_events,omitempty" jsonschema:"stop after this many; default 100"`
	Kinds     []string `json:"kinds,omitempty" jsonschema:"only these kinds: spawn, exit, link-up, link-down, dead-letter"`
}

type watchOut struct {
	Events    []client.EventView `json:"events"`
	Truncated bool               `json:"truncated,omitempty" jsonschema:"max_events was reached before the time was up"`
}

func (t tools) watchEvents(ctx context.Context, _ *mcp.CallToolRequest, in watchIn) (*mcp.CallToolResult, watchOut, error) {
	seconds := min(max(cmp.Or(in.Seconds, 5), 1), 60)
	limit := cmp.Or(in.MaxEvents, 100)
	ctx, cancel := context.WithTimeout(ctx, time.Duration(seconds)*time.Second)
	defer cancel()
	out := watchOut{Events: []client.EventView{}}
	err := t.c.Watch(ctx, in.Node, func(e client.EventView) bool {
		if len(in.Kinds) > 0 && !slices.Contains(in.Kinds, e.Kind) {
			return true
		}
		out.Events = append(out.Events, e)
		out.Truncated = len(out.Events) >= limit
		return !out.Truncated
	})
	return nil, out, err
}

type exitIn struct {
	Node    string `json:"node,omitempty" jsonschema:"node the name is registered on; not needed for a pid"`
	Process string `json:"process" jsonschema:"a pid, <node.incarnation.id>, or a registered name"`
	Reason  string `json:"reason,omitempty" jsonschema:"exit reason; default killed"`
}

type done struct {
	Result string `json:"result"`
}

func (t tools) exitProcess(ctx context.Context, _ *mcp.CallToolRequest, in exitIn) (*mcp.CallToolResult, done, error) {
	ctx, cancel := t.ctx(ctx)
	defer cancel()
	reason := cmp.Or(in.Reason, "killed")
	if err := t.c.Exit(ctx, in.Node, in.Process, reason); err != nil {
		return nil, done{}, err
	}
	return nil, done{Result: fmt.Sprintf("asked %s to exit with reason %q", in.Process, reason)}, nil
}

type levelIn struct {
	Node    string `json:"node,omitempty" jsonschema:"node the name is registered on; not needed for a pid"`
	Process string `json:"process" jsonschema:"a pid, <node.incarnation.id>, or a registered name"`
	Level   string `json:"level" jsonschema:"debug, info, warn, error, or a number"`
}

func (t tools) setLogLevel(ctx context.Context, _ *mcp.CallToolRequest, in levelIn) (*mcp.CallToolResult, done, error) {
	ctx, cancel := t.ctx(ctx)
	defer cancel()
	if err := t.c.SetLogLevel(ctx, in.Node, in.Process, in.Level); err != nil {
		return nil, done{}, err
	}
	return nil, done{Result: fmt.Sprintf("log level of %s set to %s", in.Process, in.Level)}, nil
}
