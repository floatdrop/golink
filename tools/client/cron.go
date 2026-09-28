package client

import (
	"cmp"
	"context"
	"fmt"
	"maps"
	"slices"
	"strconv"
	"strings"

	"google.golang.org/protobuf/types/known/anypb"

	cronv1 "github.com/floatdrop/grpcproc/cron/proto/grpcproc/cron/v1"
	inspectv1 "github.com/floatdrop/grpcproc/proto/grpcproc/inspect/v1"
)

// cronType is the type of a grpcproc/cron process's messages, as a process
// snapshot names it: how a cron process is told from the others, whatever
// its label.
const cronType = "*cronv1.Control"

// CronView is a grpcproc/cron process and its jobs.
type CronView struct {
	Node string `json:"node"`
	// Process is the cron process's registered name, or its pid.
	Process string        `json:"process"`
	PID     string        `json:"pid"`
	Jobs    []CronJobView `json:"jobs"`
	Error   string        `json:"error,omitempty" jsonschema:"why its jobs could not be read: it is busy, or gone"`
}

// CronJobView is one job of a cron process, as the process publishes it.
type CronJobView struct {
	Name     string `json:"name"`
	Spec     string `json:"spec" jsonschema:"when it runs, in crontab syntax"`
	Location string `json:"location" jsonschema:"the time zone its spec is read in"`
	Next     string `json:"next,omitempty" jsonschema:"when it runs next, in its zone"`
	Last     string `json:"last,omitempty" jsonschema:"the minute its last run was due, in its zone"`
	Running  int    `json:"running,omitzero" jsonschema:"its runs going now"`
	Disabled bool   `json:"disabled,omitzero" jsonschema:"kept, but not run, until enabled"`
	Failure  string `json:"failure,omitempty" jsonschema:"why its last failed run failed: the error it returned, a panic, timeout or replaced"`
}

// Crons describes the cron processes of node, or of every node reachable
// from this one when node is empty, in the order Cluster finds the nodes
// and by PID on each. A node that cannot be asked is left out; so is one
// with no cron process.
func (c *Client) Crons(ctx context.Context, node string) ([]CronView, error) {
	nodes := []string{node}
	if node == "" {
		all, err := c.Cluster(ctx)
		if err != nil {
			return nil, err
		}
		nodes = nodes[:0]
		for _, n := range all {
			if n.Error == "" {
				nodes = append(nodes, n.Name)
			}
		}
	}
	out := []CronView{}
	for _, n := range nodes {
		ps, err := c.Processes(ctx, n, Filter{})
		if err != nil {
			return nil, err
		}
		for _, p := range ps {
			if p.Type == cronType {
				out = append(out, c.cron(ctx, n, p.PID))
			}
		}
	}
	return out, nil
}

// Cron describes one cron process, found as Process finds one.
func (c *Client) Cron(ctx context.Context, node, target string) (CronView, error) {
	p, err := c.Process(ctx, node, target, true, 0)
	if err != nil {
		return CronView{}, err
	}
	return cronView(p), nil
}

// cron describes the cron process pid on node, or says why it cannot.
func (c *Client) cron(ctx context.Context, node, pid string) CronView {
	p, err := c.Process(ctx, node, pid, true, 0)
	if err != nil {
		return CronView{Node: node, Process: pid, PID: pid, Jobs: []CronJobView{}, Error: err.Error()}
	}
	return cronView(p)
}

func cronView(p ProcessView) CronView {
	pid, _ := parseTarget(p.PID) // as the Inspector wrote it
	v := CronView{Node: pid.GetPid().GetNode(), Process: cmp.Or(p.Name, p.PID), PID: p.PID, Jobs: []CronJobView{}, Error: p.InspectError}
	for _, name := range slices.Sorted(maps.Keys(p.Inspect)) {
		v.Jobs = append(v.Jobs, cronJobView(name, p.Inspect[name]))
	}
	return v
}

// cronJobView reads the line a cron process publishes for a job:
// "<spec> <zone>", then, each after ", " and only if there is one to say,
// "disabled" or "next <time>", "last <time>", "running <n>" and, last,
// "failed: <reason>", which may hold ", " itself. A spec never does.
func cronJobView(name, line string) CronJobView {
	head, rest, _ := strings.Cut(line, ", ")
	spec, zone, _ := strings.CutLast(head, " ")
	v := CronJobView{Name: name, Spec: spec, Location: zone}
	for rest != "" {
		if reason, ok := strings.CutPrefix(rest, "failed: "); ok {
			v.Failure = reason
			break
		}
		var part string
		part, rest, _ = strings.Cut(rest, ", ")
		switch word, value, _ := strings.Cut(part, " "); word {
		case "disabled":
			v.Disabled = true
		case "next":
			v.Next = value
		case "last":
			v.Last = value
		case "running":
			v.Running, _ = strconv.Atoi(value)
		}
	}
	return v
}

// CronJob changes the job called job of a cron process, found as Process
// finds one: enable, disable or remove it. The process answers once it has.
func (c *Client) CronJob(ctx context.Context, node, target, op, job string) error {
	t, err := parseTarget(target)
	if err != nil {
		return err
	}
	var ctl cronv1.Control
	switch op {
	case "enable":
		ctl.Op = &cronv1.Control_Enable{Enable: job}
	case "disable":
		ctl.Op = &cronv1.Control_Disable{Disable: job}
	case "remove":
		ctl.Op = &cronv1.Control_Remove{Remove: job}
	default:
		return fmt.Errorf("bad cron op %q: want enable, disable or remove", op)
	}
	body, err := anypb.New(&ctl)
	if err != nil {
		return err
	}
	_, err = c.rpc.Call(ctx, &inspectv1.CallRequest{Node: node, Target: t, Body: body})
	return err
}
