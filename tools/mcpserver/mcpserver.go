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
	// AllowWrites also offers exit_process, set_log_level, move_leader,
	// cordon_node, uncordon_node, enable_cron_job, disable_cron_job and
	// remove_cron_job.
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
- watch_events shows spawns, exits with reasons, links going up and down, and dead letters (messages that found no process or the wrong type, or that a broken link never delivered).
- A grpcproc/leader election (a cluster name) has an elector process on each node that takes part, registered as leader/<cluster>; election shows what each believes: its role, term, the leader it follows, the nodes cordoned (kept from leading) and those it cannot reach. Nodes that name different leaders, or a node with an old term, point at a partition.
- A grpcproc/cron process runs jobs on crontab schedules; cron_jobs lists each, when it runs next and last ran, how many runs are going, and why its last failed run failed. Each run is a process of its own, labelled cron:<job>.
- node_info and cluster_nodes list each node's links. queued on an out link is what waits to be written to that peer: a growing queue means the peer or the network cannot keep up. A down out link with retry_in means dials to that peer failed, and sends to it fail at once until then; such a peer shows incarnation 0, and cluster_nodes lists it with an error if it cannot be reached.

Start with cluster_nodes, then list_processes sorted by mailbox to find backlogs, then get_process on the suspects.`

// New returns an MCP server over c.
func New(c *client.Client, o Options) *mcp.Server {
	o.Timeout = cmp.Or(max(o.Timeout, 0), 5*time.Second)
	s := mcp.NewServer(&mcp.Implementation{Name: "grpcproc", Version: cmp.Or(o.Version, "dev")}, &mcp.ServerOptions{Instructions: instructions})
	// A handler's panic fails its request, not the server: nothing in the
	// SDK recovers, and an agent can send anything.
	s.AddReceivingMiddleware(func(next mcp.MethodHandler) mcp.MethodHandler {
		return func(ctx context.Context, method string, req mcp.Request) (res mcp.Result, err error) {
			defer func() {
				if r := recover(); r != nil {
					err = fmt.Errorf("%s: internal error: %v", method, r)
				}
			}()
			return next(ctx, method, req)
		}
	})
	t := tools{c: c, timeout: o.Timeout}
	readOnly := &mcp.ToolAnnotations{ReadOnlyHint: true}
	mcp.AddTool(s, &mcp.Tool{Name: "cluster_nodes", Description: "Every node reachable from the one serving the Inspector: counters, links, and which could not be reached.", Annotations: readOnly}, t.clusterNodes)
	mcp.AddTool(s, &mcp.Tool{Name: "node_info", Description: "One node: process counts, dead letters, and each link with its traffic, queue, and last error.", Annotations: readOnly}, t.nodeInfo)
	mcp.AddTool(s, &mcp.Tool{Name: "list_processes", Description: "Processes of a node, filtered and sorted. Sort by mailbox to find backlogs.", Annotations: readOnly}, t.listProcesses)
	mcp.AddTool(s, &mcp.Tool{Name: "get_process", Description: "One process by pid or name, with what it says about itself.", Annotations: readOnly}, t.getProcess)
	mcp.AddTool(s, &mcp.Tool{Name: "watch_events", Description: "Collect a node's events for a few seconds: spawns, exits with reasons, links up and down, dead letters.", Annotations: readOnly}, t.watchEvents)
	mcp.AddTool(s, &mcp.Tool{Name: "cron_jobs", Description: "The grpcproc/cron processes of a node, or of every node, and their jobs: schedule, next and last run, runs going, last failure.", Annotations: readOnly}, t.cronJobs)
	mcp.AddTool(s, &mcp.Tool{Name: "election", Description: "A grpcproc/leader election, as each node that takes part sees it: role, term, leader, view, cordoned nodes, unreachable nodes.", Annotations: readOnly}, t.election)
	if o.AllowWrites {
		mcp.AddTool(s, &mcp.Tool{Name: "move_leader", Description: "Have the leader of an election hand over, to a given node or to the follower with the latest state. Its singleton stops, and starts on the new leader.", Annotations: &mcp.ToolAnnotations{DestructiveHint: new(true)}}, t.moveLeader)
		mcp.AddTool(s, &mcp.Tool{Name: "cordon_node", Description: "Keep a node from leading an election, until uncordon_node: to work on its host. If it leads, it hands over. It still votes.", Annotations: &mcp.ToolAnnotations{DestructiveHint: new(true)}}, t.cordonNode)
		mcp.AddTool(s, &mcp.Tool{Name: "uncordon_node", Description: "Let a cordoned node lead an election again."}, t.uncordonNode)
		mcp.AddTool(s, &mcp.Tool{Name: "enable_cron_job", Description: "Run a disabled cron job on its schedule again, from the next minute; what it missed is not caught up."}, t.cronJob("enable"))
		mcp.AddTool(s, &mcp.Tool{Name: "disable_cron_job", Description: "Keep a cron job but run it no more until enabled. Runs already going go on."}, t.cronJob("disable"))
		mcp.AddTool(s, &mcp.Tool{Name: "remove_cron_job", Description: "Remove a job from a cron process until it restarts from its spec. Runs already going go on.", Annotations: &mcp.ToolAnnotations{DestructiveHint: new(true)}}, t.cronJob("remove"))
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
	Limit      uint   `json:"limit,omitempty" jsonschema:"at most this many; default 100"`
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
	if limit := cmp.Or(in.Limit, 100); uint(len(ps)) > limit {
		out.Processes = ps[:limit]
	}
	return nil, out, nil
}

type processIn struct {
	Node       string `json:"node,omitempty" jsonschema:"node the name is registered on; not needed for a pid"`
	Process    string `json:"process" jsonschema:"a pid, <node.incarnation.id>, or a registered name"`
	NoInspect  bool   `json:"no_inspect,omitempty" jsonschema:"skip asking the process about itself"`
	WaitMillis uint   `json:"wait_ms,omitempty" jsonschema:"how long a busy process has to answer, up to 60000; default 1000"`
}

func (t tools) getProcess(ctx context.Context, _ *mcp.CallToolRequest, in processIn) (*mcp.CallToolResult, client.ProcessView, error) {
	wait := time.Duration(min(cmp.Or(in.WaitMillis, 1000), 60_000)) * time.Millisecond
	// The request waits for the process, then for the answer.
	ctx, cancel := context.WithTimeout(ctx, t.timeout+wait)
	defer cancel()
	p, err := t.c.Process(ctx, in.Node, in.Process, !in.NoInspect, wait)
	return nil, p, err
}

type watchIn struct {
	Node      string   `json:"node,omitempty" jsonschema:"node to watch; empty for the node serving the Inspector"`
	Seconds   int      `json:"seconds,omitempty" jsonschema:"how long to collect, 1 to 60; default 5"`
	MaxEvents uint     `json:"max_events,omitempty" jsonschema:"stop after this many; default 100"`
	Kinds     []string `json:"kinds,omitempty" jsonschema:"only these kinds: spawn, exit, link-up, link-down, dead-letter"`
}

type watchOut struct {
	Events    []client.EventView `json:"events"`
	Truncated bool               `json:"truncated,omitempty" jsonschema:"max_events was reached before the time was up"`
}

func (t tools) watchEvents(ctx context.Context, _ *mcp.CallToolRequest, in watchIn) (*mcp.CallToolResult, watchOut, error) {
	kinds, err := client.ParseKinds(in.Kinds)
	if err != nil {
		return nil, watchOut{}, err
	}
	seconds := min(max(cmp.Or(in.Seconds, 5), 1), 60)
	limit := cmp.Or(in.MaxEvents, 100)
	ctx, cancel := context.WithTimeout(ctx, time.Duration(seconds)*time.Second)
	defer cancel()
	out := watchOut{Events: []client.EventView{}}
	err = t.c.Watch(ctx, in.Node, func(e client.EventView) bool {
		if len(kinds) > 0 && !slices.Contains(kinds, e.Kind) {
			return true
		}
		out.Events = append(out.Events, e)
		out.Truncated = uint(len(out.Events)) >= limit
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
	l, err := client.ParseLevel(in.Level)
	if err != nil {
		return nil, done{}, err
	}
	ctx, cancel := t.ctx(ctx)
	defer cancel()
	if err := t.c.SetLogLevel(ctx, in.Node, in.Process, l); err != nil {
		return nil, done{}, err
	}
	return nil, done{Result: fmt.Sprintf("log level of %s set to %s", in.Process, l)}, nil
}

type electionIn struct {
	Cluster string `json:"cluster" jsonschema:"the election's name, its Spec.Cluster"`
}

type electionOut struct {
	Electors []client.ElectorView `json:"electors"`
	Leader   string               `json:"leader,omitempty" jsonschema:"the node that says it leads, in the highest term; empty while none does"`
}

func (t tools) election(ctx context.Context, _ *mcp.CallToolRequest, in electionIn) (*mcp.CallToolResult, electionOut, error) {
	ctx, cancel := t.ctx(ctx)
	defer cancel()
	views, err := t.c.Election(ctx, in.Cluster)
	return nil, electionOut{Electors: views, Leader: client.Leading(views)}, err
}

type moveIn struct {
	Cluster string `json:"cluster" jsonschema:"the election's name"`
	To      string `json:"to,omitempty" jsonschema:"the node to hand over to; empty for the follower with the latest state"`
}

func (t tools) moveLeader(ctx context.Context, _ *mcp.CallToolRequest, in moveIn) (*mcp.CallToolResult, done, error) {
	ctx, cancel := t.ctx(ctx)
	defer cancel()
	was, err := t.c.MoveLeader(ctx, in.Cluster, in.To)
	if err != nil {
		return nil, done{}, err
	}
	return nil, done{Result: fmt.Sprintf("%s handed over to %s; election shows the new leader once it is elected", was, cmp.Or(in.To, "the follower with the latest state"))}, nil
}

type cordonIn struct {
	Cluster string `json:"cluster" jsonschema:"the election's name"`
	Node    string `json:"node" jsonschema:"the node"`
}

func (t tools) cordonNode(ctx context.Context, _ *mcp.CallToolRequest, in cordonIn) (*mcp.CallToolResult, done, error) {
	return t.cordon(ctx, in, false)
}

func (t tools) uncordonNode(ctx context.Context, _ *mcp.CallToolRequest, in cordonIn) (*mcp.CallToolResult, done, error) {
	return t.cordon(ctx, in, true)
}

func (t tools) cordon(ctx context.Context, in cordonIn, off bool) (*mcp.CallToolResult, done, error) {
	ctx, cancel := t.ctx(ctx)
	defer cancel()
	if _, err := t.c.Cordon(ctx, in.Cluster, in.Node, off); err != nil {
		return nil, done{}, err
	}
	if off {
		return nil, done{Result: in.Node + " may lead " + in.Cluster + " again"}, nil
	}
	return nil, done{Result: in.Node + " may not lead " + in.Cluster + "; if it led, it is handing over"}, nil
}

type cronIn struct {
	Node    string `json:"node,omitempty" jsonschema:"node to look at; empty for every node"`
	Process string `json:"process,omitempty" jsonschema:"one cron process, a pid or a registered name (looked up on node); empty for every cron process"`
}

type cronOut struct {
	Crons []client.CronView `json:"crons"`
}

func (t tools) cronJobs(ctx context.Context, _ *mcp.CallToolRequest, in cronIn) (*mcp.CallToolResult, cronOut, error) {
	ctx, cancel := t.ctx(ctx)
	defer cancel()
	if in.Process != "" {
		c, err := t.c.Cron(ctx, in.Node, in.Process)
		return nil, cronOut{Crons: []client.CronView{c}}, err
	}
	crons, err := t.c.Crons(ctx, in.Node)
	return nil, cronOut{Crons: crons}, err
}

type cronJobIn struct {
	Node    string `json:"node,omitempty" jsonschema:"node the name is registered on; not needed for a pid"`
	Process string `json:"process" jsonschema:"the cron process: a pid, <node.incarnation.id>, or a registered name"`
	Job     string `json:"job" jsonschema:"the job's name"`
}

// cronJob is the tool that does op to a job.
func (t tools) cronJob(op string) mcp.ToolHandlerFor[cronJobIn, done] {
	return func(ctx context.Context, _ *mcp.CallToolRequest, in cronJobIn) (*mcp.CallToolResult, done, error) {
		ctx, cancel := t.ctx(ctx)
		defer cancel()
		if err := t.c.CronJob(ctx, in.Node, in.Process, op, in.Job); err != nil {
			return nil, done{}, err
		}
		return nil, done{Result: fmt.Sprintf("%sd job %s of %s", op, in.Job, in.Process)}, nil
	}
}
