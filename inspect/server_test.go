package inspect_test

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math"
	"runtime"
	"strings"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/durationpb"

	"github.com/floatdrop/grpcproc"
	"github.com/floatdrop/grpcproc/grpcproctest"
	"github.com/floatdrop/grpcproc/inspect"
	"github.com/floatdrop/grpcproc/internal/testpb"
	inspectv1 "github.com/floatdrop/grpcproc/proto/grpcproc/inspect/v1"
	grpcprocv1 "github.com/floatdrop/grpcproc/proto/grpcproc/v1"
)

// cluster starts nodes that each serve an Inspector able to forward to the
// others, over the cluster's own connections to them.
func cluster(t *testing.T, opts []inspect.Option, names ...string) *grpcproctest.Cluster {
	t.Helper()
	conns := map[string]*grpc.ClientConn{}
	peers := func(_ context.Context, node string) (inspectv1.InspectorClient, error) {
		cc, ok := conns[node]
		if !ok {
			return nil, fmt.Errorf("no node %q", node)
		}
		return inspectv1.NewInspectorClient(cc), nil
	}
	c := grpcproctest.NewWith(t, []grpcproctest.Option{grpcproctest.WithServices(func(n *grpcproc.Node, s *grpc.Server) {
		inspect.New(n, append([]inspect.Option{inspect.WithPeers(peers)}, opts...)...).Register(s)
	})}, names...)
	for _, name := range names {
		conns[name] = c.Conn(name) // here, not on a handler's goroutine: Conn may fail the test
	}
	return c
}

func client(c *grpcproctest.Cluster, node string) inspectv1.InspectorClient {
	return inspectv1.NewInspectorClient(c.Conn(node))
}

func pidPB(p grpcproc.PID) *grpcprocv1.PID {
	return &grpcprocv1.PID{Node: p.Node, Incarnation: p.Incarnation, Id: p.ID}
}

func byPID(p grpcproc.PID) *inspectv1.Target {
	return &inspectv1.Target{Kind: &inspectv1.Target_Pid{Pid: pidPB(p)}}
}

func byName(n string) *inspectv1.Target {
	return &inspectv1.Target{Kind: &inspectv1.Target_Name{Name: n}}
}

func code(t *testing.T, err error, want codes.Code) {
	t.Helper()
	if status.Code(err) != want {
		t.Fatalf("want %v, got %v", want, err)
	}
}

// worker counts handled pings, publishes the count, and blocks on N == 7
// until release is closed.
func worker(release <-chan struct{}) (func(*grpcproc.Process[*testpb.Ping]) error, grpcproc.SpawnOption) {
	handled := 0
	fn := func(p *grpcproc.Process[*testpb.Ping]) error {
		for {
			m, err := p.Receive()
			if err != nil {
				return err
			}
			handled++
			if m.Body.GetN() == 7 {
				<-release
			}
		}
	}
	return fn, grpcproc.WithInspect(func() map[string]string { return map[string]string{"handled": string(rune('0' + handled))} })
}

func TestGetNodeLocalAndForwarded(t *testing.T) {
	c := cluster(t, nil, "a", "b")
	a := client(c, "a")
	resp, err := a.GetNode(t.Context(), &inspectv1.GetNodeRequest{})
	if err != nil {
		t.Fatal(err)
	}
	info := inspect.NodeInfo(resp.GetNode())
	if info.ID != c.Node("a").ID() || info.StartedAt.IsZero() {
		t.Fatalf("%+v", info)
	}
	// Asking a about b forwards to b's Inspector.
	e, _ := c.Node("b").Spawn(func(p *grpcproc.Process[*testpb.Ping]) error { _, err := p.Receive(); return err })
	if _, err := c.Node("a").Call[*testpb.Pong](t.Context(), e, &testpb.Ping{}); err == nil {
		t.Fatal("expected no reply")
	}
	resp, err = a.GetNode(t.Context(), &inspectv1.GetNodeRequest{Node: "b"})
	if err != nil {
		t.Fatal(err)
	}
	info = inspect.NodeInfo(resp.GetNode())
	if info.ID.Name != "b" || len(info.Links) != 2 || info.Links[0].Peer.Name != "a" || info.Links[0].EstablishedAt.IsZero() || info.Links[0].State != grpcproc.LinkUp {
		t.Fatalf("%+v", info)
	}
}

func TestListProcessesFilters(t *testing.T) {
	c := cluster(t, nil, "a")
	n := c.Node("a")
	release := make(chan struct{})
	defer close(release)
	fn, insp := worker(release)
	busy, _ := n.Spawn(fn, grpcproc.WithName("orders-busy"), grpcproc.WithLabel("order"), insp)
	fn2, insp2 := worker(release)
	_, _ = n.Spawn(fn2, grpcproc.WithName("orders-idle"), grpcproc.WithLabel("order"), insp2)
	fn3, insp3 := worker(release)
	_, _ = n.Spawn(fn3, grpcproc.WithName("billing"), grpcproc.WithLabel("bill"), insp3)
	fn4, _ := worker(release)
	_, _ = n.Spawn(fn4, grpcproc.WithLabel("misc")) // no name: matches only an empty name filter
	_ = n.Send(t.Context(), busy, &testpb.Ping{N: 7})
	_ = n.Send(t.Context(), busy, &testpb.Ping{N: 1})
	_ = n.Send(t.Context(), busy, &testpb.Ping{N: 1})
	time.Sleep(30 * time.Millisecond)

	a := client(c, "a")
	count := func(req *inspectv1.ListProcessesRequest) int {
		t.Helper()
		resp, err := a.ListProcesses(t.Context(), req)
		if err != nil {
			t.Fatal(err)
		}
		return len(resp.GetProcesses())
	}
	cases := []struct {
		name string
		req  *inspectv1.ListProcessesRequest
		want int
	}{
		{"all", &inspectv1.ListProcessesRequest{}, 4},
		{"label", &inspectv1.ListProcessesRequest{Label: "order"}, 2},
		{"name substring", &inspectv1.ListProcessesRequest{Name: "orders"}, 2},
		{"no name match", &inspectv1.ListProcessesRequest{Name: "zzz"}, 0},
		{"state", &inspectv1.ListProcessesRequest{State: inspectv1.ProcessState_PROCESS_STATE_RUNNING}, 1},
		{"backlog", &inspectv1.ListProcessesRequest{MinMailbox: 2}, 1},
		{"combined", &inspectv1.ListProcessesRequest{Label: "bill", MinMailbox: 1}, 0},
	}
	for _, tc := range cases {
		if got := count(tc.req); got != tc.want {
			t.Errorf("%s: got %d, want %d", tc.name, got, tc.want)
		}
	}
	resp, _ := a.ListProcesses(t.Context(), &inspectv1.ListProcessesRequest{MinMailbox: 2})
	p := inspect.ProcessInfo(resp.GetProcesses()[0])
	if p.PID != busy.PID() || p.Mailbox.Depth != 2 || p.Mailbox.OldestAge <= 0 || p.State != grpcproc.StateRunning || p.Label != "order" || p.Name != "orders-busy" {
		t.Fatalf("%+v", p)
	}
}

func TestGetProcess(t *testing.T) {
	c := cluster(t, nil, "a", "b")
	b := c.Node("b")
	release := make(chan struct{})
	fn, insp := worker(release)
	parents := make(chan *grpcproc.Process[proto.Message], 1)
	_, _ = b.Spawn(func(p *grpcproc.Process[proto.Message]) error {
		parents <- p
		_, err := p.Receive()
		return err
	})
	spawner := <-parents
	parent := spawner.PID()
	pid, _ := spawner.Spawn(fn, grpcproc.WithName("w"), insp)
	_ = b.Send(t.Context(), pid, &testpb.Ping{N: 1})
	time.Sleep(20 * time.Millisecond)

	a := client(c, "a")
	// By name on another node, with self-inspection.
	resp, err := a.GetProcess(t.Context(), &inspectv1.GetProcessRequest{Node: "b", Target: byName("w"), Inspect: true})
	if err != nil {
		t.Fatal(err)
	}
	if resp.GetInspect()["handled"] != "1" || resp.GetInspectError() != "" || inspect.ProcessInfo(resp.GetProcess()).Parent != parent {
		t.Fatalf("%+v", resp)
	}
	// By PID: the node is taken from the PID.
	resp, err = a.GetProcess(t.Context(), &inspectv1.GetProcessRequest{Target: byPID(pid.PID())})
	if err != nil || resp.GetInspect() != nil {
		t.Fatalf("%v %v", resp, err)
	}
	// Busy: the snapshot is still returned, with why inspect is empty.
	_ = b.Send(t.Context(), pid, &testpb.Ping{N: 7})
	time.Sleep(20 * time.Millisecond)
	resp, err = a.GetProcess(t.Context(), &inspectv1.GetProcessRequest{Target: byPID(pid.PID()), Inspect: true, InspectTimeout: durationpb.New(30 * time.Millisecond)})
	if err != nil || !strings.Contains(resp.GetInspectError(), "busy") || resp.GetProcess() == nil {
		t.Fatalf("%v %v", resp, err)
	}
	close(release)

	_, err = a.GetProcess(t.Context(), &inspectv1.GetProcessRequest{Node: "b", Target: byName("nope")})
	code(t, err, codes.NotFound)
	if !strings.Contains(status.Convert(err).Message(), "node b:") {
		t.Fatalf("forwarded error does not name the node: %v", err)
	}
	_, err = a.GetProcess(t.Context(), &inspectv1.GetProcessRequest{Target: byPID(grpcproc.PID{Node: "b", Incarnation: b.ID().Incarnation, ID: 999})})
	code(t, err, codes.NotFound)
	_, err = a.GetProcess(t.Context(), &inspectv1.GetProcessRequest{})
	code(t, err, codes.InvalidArgument)
}

func TestWrites(t *testing.T) {
	c := cluster(t, nil, "a", "b")
	a, b := client(c, "a"), c.Node("b")
	w, _ := c.Node("a").Spawn[proto.Message](func(p *grpcproc.Process[proto.Message]) error {
		_, err := p.Receive()
		return err
	})
	col := make(chan *testpb.Ping, 1)
	target, _ := b.Spawn[*testpb.Ping](func(p *grpcproc.Process[*testpb.Ping]) error {
		for {
			m, err := p.Receive()
			if err != nil {
				return err
			}
			col <- m.Body
		}
	}, grpcproc.WithName("t"))
	_ = w

	// SetLogLevel through the inspector of another node.
	if _, err := a.SetLogLevel(t.Context(), &inspectv1.SetLogLevelRequest{Node: "b", Target: byName("t"), Level: int32(slog.LevelDebug)}); err != nil {
		t.Fatal(err)
	}
	if info, _ := b.Process(target.PID()); info.LogLevel != slog.LevelDebug {
		t.Fatalf("%+v", info)
	}
	_, err := a.SetLogLevel(t.Context(), &inspectv1.SetLogLevelRequest{Target: byPID(grpcproc.PID{Node: "b", Incarnation: b.ID().Incarnation, ID: 999})})
	code(t, err, codes.NotFound)
	_, err = a.SetLogLevel(t.Context(), &inspectv1.SetLogLevelRequest{Node: "b", Target: byName("nope")})
	code(t, err, codes.NotFound)

	// Send an Any to a named process.
	body, _ := anypb.New(&testpb.Ping{N: 5})
	if _, err := a.Send(t.Context(), &inspectv1.SendRequest{Node: "b", Target: byName("t"), Body: body}); err != nil {
		t.Fatal(err)
	}
	if got := <-col; got.GetN() != 5 {
		t.Fatalf("%v", got)
	}
	_, err = a.Send(t.Context(), &inspectv1.SendRequest{Node: "b", Target: byName("t")})
	code(t, err, codes.InvalidArgument)
	_, err = a.Send(t.Context(), &inspectv1.SendRequest{Node: "b", Target: byName("t"), Body: &anypb.Any{TypeUrl: "type.googleapis.com/no.Such"}})
	code(t, err, codes.InvalidArgument)
	_, err = a.Send(t.Context(), &inspectv1.SendRequest{Node: "b", Body: body})
	code(t, err, codes.InvalidArgument)
	// A PID on a node this one cannot reach: the send fails at the server.
	_, err = a.Send(t.Context(), &inspectv1.SendRequest{Node: "a", Target: byPID(grpcproc.PID{Node: "nowhere"}), Body: body})
	code(t, err, codes.Unavailable)

	// Exit by name, default reason; a watcher on a sees it.
	watchDone := make(chan grpcproc.Down, 1)
	_, _ = c.Node("a").Spawn[proto.Message](func(p *grpcproc.Process[proto.Message]) error {
		p.Monitor(target)
		for {
			m, err := p.Receive()
			if err != nil {
				return err
			}
			if m.Down != nil {
				watchDone <- *m.Down
				return nil
			}
		}
	})
	time.Sleep(30 * time.Millisecond)
	if _, err := a.Exit(t.Context(), &inspectv1.ExitRequest{Node: "b", Target: byName("t")}); err != nil {
		t.Fatal(err)
	}
	select {
	case d := <-watchDone:
		if d.Reason != grpcproc.ReasonKilled {
			t.Fatalf("%+v", d)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("no Down")
	}
	_, err = a.Exit(t.Context(), &inspectv1.ExitRequest{Node: "b"})
	code(t, err, codes.InvalidArgument)
}

func TestReadOnly(t *testing.T) {
	c := cluster(t, []inspect.Option{inspect.ReadOnly()}, "a")
	a := client(c, "a")
	_, err := a.SetLogLevel(t.Context(), &inspectv1.SetLogLevelRequest{Target: byName("x")})
	code(t, err, codes.PermissionDenied)
	_, err = a.Send(t.Context(), &inspectv1.SendRequest{Target: byName("x")})
	code(t, err, codes.PermissionDenied)
	_, err = a.Exit(t.Context(), &inspectv1.ExitRequest{Target: byName("x")})
	code(t, err, codes.PermissionDenied)
	if _, err := a.GetNode(t.Context(), &inspectv1.GetNodeRequest{}); err != nil {
		t.Fatal("reads stay allowed:", err)
	}
}

func TestExitThatCannotRoute(t *testing.T) {
	// Naming this node with a PID on another routes the exit there; a node
	// that cannot be reached is Unavailable, not silently dropped.
	c := cluster(t, nil, "a")
	_, err := client(c, "a").Exit(t.Context(), &inspectv1.ExitRequest{
		Node:   "a",
		Target: byPID(grpcproc.PID{Node: "nowhere", Incarnation: 1, ID: 1}),
	})
	code(t, err, codes.Unavailable)
}

func TestRoutingErrors(t *testing.T) {
	// No way to reach other nodes: another node is FailedPrecondition, for
	// every method.
	c := grpcproctest.NewWith(t, []grpcproctest.Option{grpcproctest.WithServices(func(n *grpcproc.Node, s *grpc.Server) {
		inspect.New(n).Register(s)
	})}, "a")
	a := client(c, "a")
	ctx := t.Context()
	errs := []error{
		func() error { _, err := a.GetNode(ctx, &inspectv1.GetNodeRequest{Node: "b"}); return err }(),
		func() error { _, err := a.ListProcesses(ctx, &inspectv1.ListProcessesRequest{Node: "b"}); return err }(),
		func() error { _, err := a.GetProcess(ctx, &inspectv1.GetProcessRequest{Node: "b"}); return err }(),
		func() error { _, err := a.SetLogLevel(ctx, &inspectv1.SetLogLevelRequest{Node: "b"}); return err }(),
		func() error { _, err := a.Send(ctx, &inspectv1.SendRequest{Node: "b"}); return err }(),
		func() error { _, err := a.Exit(ctx, &inspectv1.ExitRequest{Node: "b"}); return err }(),
		func() error {
			s, err := a.Watch(ctx, &inspectv1.WatchRequest{Node: "b"})
			if err != nil {
				return err
			}
			_, err = s.Recv()
			return err
		}(),
	}
	for i, err := range errs {
		if status.Code(err) != codes.FailedPrecondition {
			t.Errorf("method %d: %v", i, err)
		}
	}
	// Peers that cannot be reached: Unavailable.
	c2 := grpcproctest.NewWith(t, []grpcproctest.Option{grpcproctest.WithServices(func(n *grpcproc.Node, s *grpc.Server) {
		inspect.New(n, inspect.WithPeers(func(context.Context, string) (inspectv1.InspectorClient, error) {
			return nil, errors.New("no route")
		})).Register(s)
	})}, "x")
	_, err := client(c2, "x").GetNode(ctx, &inspectv1.GetNodeRequest{Node: "y"})
	code(t, err, codes.Unavailable)
}

func TestWatchLocalAndForwarded(t *testing.T) {
	c := cluster(t, nil, "a", "b")
	a := client(c, "a")
	for _, node := range []string{"", "b"} {
		t.Run("node="+node, func(t *testing.T) {
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			stream, err := a.Watch(ctx, &inspectv1.WatchRequest{Node: node, Buffer: 16})
			if err != nil {
				t.Fatal(err)
			}
			target := c.Node("a")
			if node != "" {
				target = c.Node(node)
			}
			// The subscription is set up asynchronously: keep spawning until one is seen.
			seen := make(chan grpcproc.Event, 16)
			go func() {
				for {
					resp, err := stream.Recv()
					if err != nil {
						close(seen)
						return
					}
					seen <- inspect.Event(resp.GetEvent())
				}
			}()
			deadline := time.After(5 * time.Second)
			for {
				e, _ := target.Spawn(func(p *grpcproc.Process[*testpb.Ping]) error { return nil })
				select {
				case ev := <-seen:
					if ev.Kind == grpcproc.EventSpawn && ev.Process.PID.Node == target.Name() {
						_ = e
						return
					}
				case <-time.After(20 * time.Millisecond):
				case <-deadline:
					t.Fatal("no spawn event")
				}
			}
		})
	}
}

func TestWatchEndsWhenNodeStops(t *testing.T) {
	c := cluster(t, nil, "a")
	stream, err := client(c, "a").Watch(t.Context(), &inspectv1.WatchRequest{})
	if err != nil {
		t.Fatal(err)
	}
	// The server subscribes asynchronously: keep spawning until an event
	// proves the subscription is live, then stop the node.
	live := make(chan struct{})
	go func() {
		for {
			select {
			case <-live:
				return
			case <-time.After(10 * time.Millisecond):
				_, _ = c.Node("a").Spawn(func(p *grpcproc.Process[*testpb.Ping]) error { return nil })
			}
		}
	}()
	if _, err := stream.Recv(); err != nil {
		t.Fatal(err)
	}
	close(live)
	go c.Stop("a")
	for {
		if _, err := stream.Recv(); err != nil {
			if status.Code(err) != codes.Unavailable {
				t.Fatalf("got %v", err)
			}
			return
		}
	}
}

func TestEventConversions(t *testing.T) {
	now := time.Now()
	pid := grpcproc.PID{Node: "a", Incarnation: 1, ID: 2}
	info := grpcproc.ProcessInfo{PID: pid, Label: "l", StartedAt: now, LogLevel: slog.LevelWarn}
	events := []grpcproc.Event{
		{Kind: grpcproc.EventSpawn, Process: info},
		{Kind: grpcproc.EventExit, Process: info, Reason: "boom"},
		{Kind: grpcproc.EventLinkUp, Peer: grpcproc.NodeID{Name: "b", Incarnation: 3}},
		{Kind: grpcproc.EventLinkDown, Peer: grpcproc.NodeID{Name: "b"}, Err: "eof"},
		{Kind: grpcproc.EventDeadLetter, From: pid, To: pid, Type: "x.Y", Reason: "type"},
	}
	for _, ev := range events {
		ev.Time, ev.Missed = now, 3
		got := inspect.Event(inspect.EventToProto(ev))
		if got.Kind != ev.Kind || got.Reason != ev.Reason || got.Peer != ev.Peer || got.Err != ev.Err ||
			got.From != ev.From || got.Type != ev.Type || got.Missed != 3 || !got.Time.Equal(now) ||
			got.Process.PID != ev.Process.PID || got.Process.LogLevel != ev.Process.LogLevel {
			t.Errorf("%v: got %+v", ev.Kind, got)
		}
	}
	// A wire state no state has, unspecified or from a newer node, becomes
	// 255 rather than wrap around into a real one.
	for _, wire := range []int32{0, 257, -1} {
		p := inspect.ProcessInfo(&inspectv1.ProcessInfo{State: inspectv1.ProcessState(wire)})
		n := inspect.NodeInfo(&inspectv1.NodeInfo{Links: []*inspectv1.Link{{State: inspectv1.LinkState(wire)}}})
		if p.State != 255 || n.Links[0].State != 255 {
			t.Errorf("wire state %d: process %v, link %v", wire, p.State, n.Links[0].State)
		}
	}
	if p := inspect.ProcessInfo(&inspectv1.ProcessInfo{State: inspectv1.ProcessState_PROCESS_STATE_EXITING}); p.State != grpcproc.StateExiting {
		t.Errorf("exiting became %v", p.State)
	}
	// Zero times stay zero across the wire, and set ones cross it.
	if n := inspect.NodeInfo(inspect.NodeInfoToProto(grpcproc.NodeInfo{Links: []grpcproc.LinkInfo{{}}})); !n.StartedAt.IsZero() || !n.Links[0].EstablishedAt.IsZero() || !n.Links[0].RetryAt.IsZero() {
		t.Fatalf("%+v", n)
	}
	down := grpcproc.LinkInfo{Peer: grpcproc.NodeID{Name: "b"}, Outbound: true, State: grpcproc.LinkDown, LastError: "refused", RetryAt: now, Queued: 5}
	if n := inspect.NodeInfo(inspect.NodeInfoToProto(grpcproc.NodeInfo{Links: []grpcproc.LinkInfo{down}})); !n.Links[0].RetryAt.Equal(now) || n.Links[0].State != grpcproc.LinkDown || n.Links[0].Queued != 5 {
		t.Fatalf("%+v", n.Links[0])
	}
}

// The client picks a watch's buffer, and the server allocates it: it is
// capped, and a relayed watch is capped before it reaches a peer, which may
// predate the cap. Before, the largest request allocated 1.8 TB.
func TestWatchCapsItsBuffer(t *testing.T) {
	for _, node := range []string{"", "b"} {
		t.Run("node="+node, func(t *testing.T) {
			c := cluster(t, nil, "a", "b")
			watched := c.Node("a")
			if node != "" {
				watched = c.Node(node)
			}
			var before, after runtime.MemStats
			runtime.ReadMemStats(&before)
			stream, err := client(c, "a").Watch(t.Context(), &inspectv1.WatchRequest{Node: node, Buffer: math.MaxUint32})
			if err != nil {
				t.Fatal(err)
			}
			got := make(chan error, 1)
			go func() {
				_, err := stream.Recv()
				got <- err
			}()
			for deadline := time.After(5 * time.Second); ; {
				_, _ = watched.Spawn(func(p *grpcproc.Process[*testpb.Ping]) error { return nil })
				select {
				case err := <-got:
					if err != nil {
						t.Fatal(err)
					}
					runtime.ReadMemStats(&after)
					if grew := after.TotalAlloc - before.TotalAlloc; grew > 16<<20 {
						t.Fatalf("the watch allocated %d MB", grew>>20)
					}
					return
				case <-deadline:
					t.Fatal("no event")
				case <-time.After(10 * time.Millisecond):
				}
			}
		})
	}
}
