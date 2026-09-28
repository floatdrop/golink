package mcpserver_test

import (
	"encoding/json/v2"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/modelcontextprotocol/go-sdk/mcp"
	"google.golang.org/protobuf/proto"

	"github.com/floatdrop/grpcproc"
	"github.com/floatdrop/grpcproc/tools/client"
	"github.com/floatdrop/grpcproc/tools/internal/testcluster"
	"github.com/floatdrop/grpcproc/tools/mcpserver"
)

func connect(t *testing.T, f *testcluster.Fixture, o mcpserver.Options) *mcp.ClientSession {
	t.Helper()
	serverT, clientT := mcp.NewInMemoryTransports()
	s := mcpserver.New(client.New(f.C.Conn("a")), o)
	ss, err := s.Connect(t.Context(), serverT, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = ss.Close() })
	cs, err := mcp.NewClient(&mcp.Implementation{Name: "test"}, nil).Connect(t.Context(), clientT, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = cs.Close() })
	return cs
}

// call runs a tool and decodes its structured result into out. It returns
// the error text when the tool failed.
func call(t *testing.T, cs *mcp.ClientSession, name string, args map[string]any, out any) string {
	t.Helper()
	res, err := cs.CallTool(t.Context(), &mcp.CallToolParams{Name: name, Arguments: args})
	if err != nil {
		t.Fatalf("%s: %v", name, err)
	}
	if res.IsError {
		var texts []string
		for _, c := range res.Content {
			if tc, ok := c.(*mcp.TextContent); ok {
				texts = append(texts, tc.Text)
			}
		}
		return strings.Join(texts, " ")
	}
	raw, err := json.Marshal(res.StructuredContent)
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(raw, out); err != nil {
		t.Fatalf("%s: %v in %s", name, err, raw)
	}
	return ""
}

func toolNames(t *testing.T, cs *mcp.ClientSession) []string {
	t.Helper()
	res, err := cs.ListTools(t.Context(), nil)
	if err != nil {
		t.Fatal(err)
	}
	var names []string
	for _, tool := range res.Tools {
		names = append(names, tool.Name)
	}
	slices.Sort(names)
	return names
}

func TestToolsOnOffer(t *testing.T) {
	f := testcluster.Start(t)
	read := []string{"cluster_nodes", "cron_jobs", "election", "get_process", "list_processes", "node_info", "watch_events"}
	if got := toolNames(t, connect(t, f, mcpserver.Options{})); !slices.Equal(got, read) {
		t.Fatalf("read-only: %v", got)
	}
	all := append(slices.Clone(read), "cordon_node", "disable_cron_job", "enable_cron_job", "exit_process", "move_leader", "remove_cron_job", "set_log_level", "uncordon_node")
	slices.Sort(all)
	cs := connect(t, f, mcpserver.Options{AllowWrites: true, Version: "v1"})
	if got := toolNames(t, cs); !slices.Equal(got, all) {
		t.Fatalf("with writes: %v", got)
	}
	if init := cs.InitializeResult(); init.ServerInfo.Name != "grpcproc" || init.ServerInfo.Version != "v1" || !strings.Contains(init.Instructions, "mailbox") {
		t.Fatalf("%+v", init)
	}
}

func TestReadTools(t *testing.T) {
	f := testcluster.Start(t)
	cs := connect(t, f, mcpserver.Options{})

	var nodes struct {
		Nodes []client.NodeView `json:"nodes"`
	}
	if msg := call(t, cs, "cluster_nodes", nil, &nodes); msg != "" || len(nodes.Nodes) != 2 {
		t.Fatalf("%v %s", nodes, msg)
	}
	var n client.NodeView
	if msg := call(t, cs, "node_info", map[string]any{"node": "b"}, &n); msg != "" || n.Name != "b" {
		t.Fatalf("%v %s", n, msg)
	}
	if msg := call(t, cs, "node_info", map[string]any{"node": "nowhere"}, &n); !strings.Contains(msg, "node nowhere") {
		t.Fatalf("unknown node: %q", msg)
	}

	var list struct {
		Processes []client.ProcessView `json:"processes"`
		Total     int                  `json:"total"`
	}
	if msg := call(t, cs, "list_processes", map[string]any{"sort": "mailbox", "limit": 1}, &list); msg != "" || len(list.Processes) != 1 || list.Processes[0].Name != "stuck" || list.Total < 5 {
		t.Fatalf("%+v %s", list, msg)
	}
	// A negative limit is refused by the schema; it used to slice out of
	// range and crash the server.
	if msg := call(t, cs, "list_processes", map[string]any{"limit": -1}, &list); !strings.Contains(msg, "minimum") {
		t.Fatalf("got %q", msg)
	}
	if msg := call(t, cs, "list_processes", map[string]any{"label": "supervisor"}, &list); msg != "" || list.Total != 1 {
		t.Fatalf("%+v %s", list, msg)
	}
	for _, bad := range []map[string]any{{"sort": "age"}, {"state": "sleeping"}} {
		if msg := call(t, cs, "list_processes", bad, &list); msg == "" {
			t.Errorf("accepted %v", bad)
		}
	}

	var p client.ProcessView
	if msg := call(t, cs, "get_process", map[string]any{"process": "talker"}, &p); msg != "" || p.Inspect["state"] != "ready" {
		t.Fatalf("%+v %s", p, msg)
	}
	if msg := call(t, cs, "get_process", map[string]any{"process": "stuck", "wait_ms": 10}, &p); msg != "" || !strings.Contains(p.InspectError, "busy") || p.Mailbox != 3 {
		t.Fatalf("%+v %s", p, msg)
	}
	p = client.ProcessView{} // decoding merges into what is there
	if msg := call(t, cs, "get_process", map[string]any{"process": f.Echo.String(), "no_inspect": true}, &p); msg != "" || p.Inspect != nil {
		t.Fatalf("%+v %s", p, msg)
	}
	if msg := call(t, cs, "get_process", map[string]any{"process": "nobody"}, &p); msg == "" {
		t.Fatal("found nobody")
	}
}

func TestWatchEvents(t *testing.T) {
	f := testcluster.Start(t)
	cs := connect(t, f, mcpserver.Options{})
	stop := make(chan struct{})
	defer close(stop)
	go func() {
		for {
			select {
			case <-stop:
				return
			case <-time.After(10 * time.Millisecond):
				_, _ = f.C.Node("a").Spawn(func(*grpcproc.Process[proto.Message]) error { return nil })
			}
		}
	}()
	var out struct {
		Events    []client.EventView `json:"events"`
		Truncated bool               `json:"truncated"`
	}
	if msg := call(t, cs, "watch_events", map[string]any{"seconds": 5, "max_events": 2, "kinds": []string{"exit"}}, &out); msg != "" || len(out.Events) != 2 || !out.Truncated || out.Events[0].Kind != "exit" {
		t.Fatalf("%+v %s", out, msg)
	}
	if msg := call(t, cs, "watch_events", map[string]any{"max_events": -1}, &out); !strings.Contains(msg, "minimum") {
		t.Fatalf("got %q", msg)
	}
	if msg := call(t, cs, "watch_events", map[string]any{"kinds": []string{"exits"}}, &out); !strings.Contains(msg, "bad event kind") {
		t.Fatalf("got %q", msg)
	}
	// Defaults: every kind, at most 100 events.
	out.Events, out.Truncated = nil, false
	if msg := call(t, cs, "watch_events", map[string]any{"seconds": 1}, &out); msg != "" || len(out.Events) < 2 || len(out.Events) > 100 {
		t.Fatalf("%+v %s", out, msg)
	}
	if msg := call(t, cs, "watch_events", map[string]any{"node": "nowhere", "seconds": 1}, &out); msg == "" {
		t.Fatal("watched an unknown node")
	}
}

func TestWriteTools(t *testing.T) {
	f := testcluster.Start(t)
	cs := connect(t, f, mcpserver.Options{AllowWrites: true})
	var done struct {
		Result string `json:"result"`
	}
	if msg := call(t, cs, "set_log_level", map[string]any{"process": "talker", "level": "debug"}, &done); msg != "" || !strings.Contains(done.Result, "DEBUG") {
		t.Fatalf("%+v %s", done, msg)
	}
	if msg := call(t, cs, "set_log_level", map[string]any{"process": "<bad", "level": "debug"}, &done); msg == "" {
		t.Fatal("accepted a bad pid")
	}
	if msg := call(t, cs, "set_log_level", map[string]any{"process": "talker", "level": "loud"}, &done); msg == "" {
		t.Fatal("accepted loud")
	}
	if msg := call(t, cs, "exit_process", map[string]any{"process": "talker"}, &done); msg != "" || !strings.Contains(done.Result, `"killed"`) {
		t.Fatalf("%+v %s", done, msg)
	}
	if msg := call(t, cs, "exit_process", map[string]any{"process": "<bad"}, &done); msg == "" {
		t.Fatal("accepted a bad pid")
	}
}

func TestLeaderTools(t *testing.T) {
	f := testcluster.Start(t, "c")
	f.Elect(t, "sched", "a", "b", "c")
	cs := connect(t, f, mcpserver.Options{AllowWrites: true})
	var election struct {
		Electors []client.ElectorView `json:"electors"`
		Leader   string               `json:"leader"`
	}
	if msg := call(t, cs, "election", map[string]any{"cluster": "sched"}, &election); msg != "" || len(election.Electors) != 3 || election.Leader == "" {
		t.Fatalf("%+v %s", election, msg)
	}
	if msg := call(t, cs, "election", map[string]any{"cluster": "nothing"}, &election); !strings.Contains(msg, "no node runs an election") {
		t.Fatalf("an election nobody runs: %s", msg)
	}
	lead := election.Leader
	next := "b"
	if lead == "b" {
		next = "c"
	}
	var done struct {
		Result string `json:"result"`
	}
	if msg := call(t, cs, "move_leader", map[string]any{"cluster": "sched", "to": next}, &done); msg != "" || !strings.Contains(done.Result, lead+" handed over to "+next) {
		t.Fatalf("%+v %s", done, msg)
	}
	f.Leading(t, "sched", "a", func(l string) bool { return l == next })
	if msg := call(t, cs, "move_leader", map[string]any{"cluster": "sched", "to": "zzz"}, &done); !strings.Contains(msg, "zzz is not in the view") {
		t.Fatalf("moved to a stranger: %s", msg)
	}
	if msg := call(t, cs, "cordon_node", map[string]any{"cluster": "sched", "node": "a"}, &done); msg != "" || !strings.Contains(done.Result, "a may not lead sched") {
		t.Fatalf("%+v %s", done, msg)
	}
	if msg := call(t, cs, "uncordon_node", map[string]any{"cluster": "sched", "node": "a"}, &done); msg != "" || !strings.Contains(done.Result, "a may lead sched again") {
		t.Fatalf("%+v %s", done, msg)
	}
	if msg := call(t, cs, "cordon_node", map[string]any{"cluster": "nothing", "node": "a"}, &done); !strings.Contains(msg, "no node runs an election") {
		t.Fatalf("cordoned in an election nobody runs: %s", msg)
	}
}

func TestCronTools(t *testing.T) {
	f := testcluster.Start(t)
	f.Cron(t, "a", "cron")
	pid := f.Cron(t, "b", "billing")
	cs := connect(t, f, mcpserver.Options{AllowWrites: true})
	var out struct {
		Crons []client.CronView `json:"crons"`
	}
	if msg := call(t, cs, "cron_jobs", nil, &out); msg != "" || len(out.Crons) != 2 {
		t.Fatalf("%+v %s", out, msg)
	}
	if msg := call(t, cs, "cron_jobs", map[string]any{"node": "b", "process": "billing"}, &out); msg != "" || len(out.Crons) != 1 || out.Crons[0].PID != pid.String() {
		t.Fatalf("%+v %s", out, msg)
	}
	if msg := call(t, cs, "cron_jobs", map[string]any{"process": "nobody"}, &out); !strings.Contains(msg, "no process") {
		t.Fatalf("a cron nobody runs: %s", msg)
	}
	var done struct {
		Result string `json:"result"`
	}
	for _, tc := range [][2]string{{"enable_cron_job", "enabled"}, {"disable_cron_job", "disabled"}, {"remove_cron_job", "removed"}} {
		if msg := call(t, cs, tc[0], map[string]any{"process": pid.String(), "job": "yearly"}, &done); msg != "" || done.Result != tc[1]+" job yearly of "+pid.String() {
			t.Errorf("%s: %+v %s", tc[0], done, msg)
		}
	}
	if msg := call(t, cs, "remove_cron_job", map[string]any{"process": pid.String(), "job": "yearly"}, &done); !strings.Contains(msg, "no such job") {
		t.Errorf("removed twice: %s", msg)
	}
}

// A wait longer than the request timeout still gets its answer: the request
// is given the wait on top.
func TestLongWaitForABusyProcess(t *testing.T) {
	f := testcluster.Start(t)
	cs := connect(t, f, mcpserver.Options{Timeout: 200 * time.Millisecond})
	var p client.ProcessView
	if msg := call(t, cs, "get_process", map[string]any{"process": "stuck", "wait_ms": 400}, &p); msg != "" || !strings.Contains(p.InspectError, "busy") {
		t.Fatalf("%+v %s", p, msg)
	}
}

// A handler that panics fails its request, and the server carries on.
func TestPanickingHandlerFailsItsRequest(t *testing.T) {
	serverT, clientT := mcp.NewInMemoryTransports()
	ss, err := mcpserver.New(&client.Client{}, mcpserver.Options{}).Connect(t.Context(), serverT, nil) // no connection: every call panics
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = ss.Close() })
	cs, err := mcp.NewClient(&mcp.Implementation{Name: "test"}, nil).Connect(t.Context(), clientT, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = cs.Close() })
	for range 2 {
		if _, err := cs.CallTool(t.Context(), &mcp.CallToolParams{Name: "cluster_nodes"}); err == nil || !strings.Contains(err.Error(), "internal error") {
			t.Fatalf("got %v", err)
		}
	}
}
