package inspect_test

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math"
	"runtime"
	"strings"
	"testing"
	"testing/synctest"
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

func byPID(p grpcproc.PID) *inspectv1.Target {
	return &inspectv1.Target{Kind: &inspectv1.Target_Pid{Pid: p.Proto()}}
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
	synctest.Test(t, func(t *testing.T) {
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
		if _, err := e.Call[*testpb.Pong](t.Context(), c.Node("a"), &testpb.Ping{}); err == nil {
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
	})
}

func TestListProcessesFilters(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
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
		_ = busy.Send(t.Context(), n, &testpb.Ping{N: 7})
		_ = busy.Send(t.Context(), n, &testpb.Ping{N: 1})
		_ = busy.Send(t.Context(), n, &testpb.Ping{N: 1})
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
	})
}

func TestListProcessesForwarded(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		c := cluster(t, nil, "a", "b")
		_, _ = c.Node("b").Spawn(func(p *grpcproc.Process[*testpb.Ping]) error { _, err := p.Receive(); return err }, grpcproc.WithLabel("remote"))
		resp, err := client(c, "a").ListProcesses(t.Context(), &inspectv1.ListProcessesRequest{Node: "b", Label: "remote"})
		if err != nil || len(resp.GetProcesses()) != 1 || resp.GetProcesses()[0].GetPid().GetNode() != "b" {
			t.Fatalf("%v %v", resp, err)
		}
	})
}

// The Inspector reports an inspect function that panicked, which ended its
// process, as the process's inspect error.
func TestGetProcessWhoseInspectPanics(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		c := cluster(t, nil, "a")
		addr, err := c.Node("a").Spawn(func(p *grpcproc.Process[proto.Message]) error {
			_, err := p.Receive()
			return err
		}, grpcproc.WithInspect(func() map[string]string { panic("inspect") }))
		if err != nil {
			t.Fatal(err)
		}
		resp, err := client(c, "a").GetProcess(t.Context(), &inspectv1.GetProcessRequest{Target: byPID(addr.PID()), Inspect: true})
		if err != nil || !strings.Contains(resp.GetInspectError(), "inspect function panicked") {
			t.Fatalf("%v %v", resp, err)
		}
	})
}

func TestGetProcess(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
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
		_ = pid.Send(t.Context(), b, &testpb.Ping{N: 1})
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
		_ = pid.Send(t.Context(), b, &testpb.Ping{N: 7})
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
	})
}

func TestWrites(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		c := cluster(t, nil, "a", "b")
		a, b := client(c, "a"), c.Node("b")
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
	})
}

// answering is a process that answers a Ping by its N: a Pong, an error,
// an answer that cannot be encoded, or no answer at all.
func answering(p *grpcproc.Process[*testpb.Ping]) error {
	for {
		m, err := p.Receive()
		if err != nil {
			return err
		}
		switch m.Body.GetN() {
		case 1:
			_ = m.Reply(&testpb.Pong{N: 1}, nil)
		case 2:
			_ = m.Reply(nil, errors.New("refused"))
		case 4:
			_ = m.Reply(&testpb.Reserved{Id: "\xff"}, nil)
		}
	}
}

func TestCall(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		c := cluster(t, nil, "a", "b")
		a := client(c, "a")
		for _, n := range []string{"a", "b"} {
			if _, err := c.Node(n).Spawn(answering, grpcproc.WithName("answers")); err != nil {
				t.Fatal(err)
			}
		}
		call := func(ctx context.Context, node string, target *inspectv1.Target, body proto.Message) (*inspectv1.CallResponse, error) {
			t.Helper()
			var b *anypb.Any
			if body != nil {
				b, _ = anypb.New(body)
			}
			return a.Call(ctx, &inspectv1.CallRequest{Node: node, Target: target, Body: b})
		}

		// Answered here, and on another node through its Inspector.
		for _, node := range []string{"a", "b"} {
			resp, err := call(t.Context(), node, byName("answers"), &testpb.Ping{N: 1})
			if err != nil {
				t.Fatal(err)
			}
			pong := &testpb.Pong{}
			if err := resp.GetBody().UnmarshalTo(pong); err != nil || pong.GetN() != 1 {
				t.Fatalf("%s: %v, %v", node, pong, err)
			}
		}

		// An answer that is an error keeps its text, as Unknown.
		_, err := call(t.Context(), "a", byName("answers"), &testpb.Ping{N: 2})
		code(t, err, codes.Unknown)
		if msg := status.Convert(err).Message(); msg != "refused" {
			t.Errorf("message %q", msg)
		}
		_, err = call(t.Context(), "b", byName("answers"), &testpb.Ping{N: 2})
		if msg := status.Convert(err).Message(); msg != "inspect: node b: refused" {
			t.Errorf("forwarded, message %q", msg)
		}

		_, err = call(t.Context(), "a", byName("answers"), &testpb.Ping{N: 4})
		code(t, err, codes.Internal)

		ctx, cancel := context.WithTimeout(t.Context(), 50*time.Millisecond)
		defer cancel()
		_, err = call(ctx, "a", byName("answers"), &testpb.Ping{N: 5})
		code(t, err, codes.DeadlineExceeded)

		_, err = call(t.Context(), "a", byName("nobody"), &testpb.Ping{N: 1})
		code(t, err, codes.NotFound)
		_, err = call(t.Context(), "a", byName("answers"), &testpb.Pong{N: 1})
		code(t, err, codes.InvalidArgument)
		_, err = call(t.Context(), "a", byName("answers"), nil)
		code(t, err, codes.InvalidArgument)
		_, err = a.Call(t.Context(), &inspectv1.CallRequest{Target: byName("answers"), Body: &anypb.Any{TypeUrl: "type.googleapis.com/no.Such"}})
		code(t, err, codes.InvalidArgument)
		_, err = call(t.Context(), "a", nil, &testpb.Ping{N: 1})
		code(t, err, codes.InvalidArgument)
		_, err = call(t.Context(), "a", byPID(grpcproc.PID{Node: "nowhere"}), &testpb.Ping{N: 1})
		code(t, err, codes.Unavailable)
	})
}

func TestReadOnly(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		c := cluster(t, []inspect.Option{inspect.ReadOnly()}, "a")
		a := client(c, "a")
		_, err := a.SetLogLevel(t.Context(), &inspectv1.SetLogLevelRequest{Target: byName("x")})
		code(t, err, codes.PermissionDenied)
		_, err = a.Send(t.Context(), &inspectv1.SendRequest{Target: byName("x")})
		code(t, err, codes.PermissionDenied)
		_, err = a.Call(t.Context(), &inspectv1.CallRequest{Target: byName("x")})
		code(t, err, codes.PermissionDenied)
		_, err = a.Exit(t.Context(), &inspectv1.ExitRequest{Target: byName("x")})
		code(t, err, codes.PermissionDenied)
		if _, err := a.GetNode(t.Context(), &inspectv1.GetNodeRequest{}); err != nil {
			t.Fatal("reads stay allowed:", err)
		}
	})
}

func TestExitThatCannotRoute(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		// Naming this node with a PID on another routes the exit there; a node
		// that cannot be reached is Unavailable, not silently dropped.
		c := cluster(t, nil, "a")
		_, err := client(c, "a").Exit(t.Context(), &inspectv1.ExitRequest{
			Node:   "a",
			Target: byPID(grpcproc.PID{Node: "nowhere", Incarnation: 1, ID: 1}),
		})
		code(t, err, codes.Unavailable)
	})
}

func TestRoutingErrors(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		// Forwarding off: another node is FailedPrecondition, for every
		// method.
		c := grpcproctest.NewWith(t, []grpcproctest.Option{grpcproctest.WithServices(func(n *grpcproc.Node, s *grpc.Server) {
			inspect.New(n, inspect.WithPeers(nil)).Register(s)
		})}, "a")
		a := client(c, "a")
		ctx := t.Context()
		errs := []error{
			func() error { _, err := a.GetNode(ctx, &inspectv1.GetNodeRequest{Node: "b"}); return err }(),
			func() error { _, err := a.ListProcesses(ctx, &inspectv1.ListProcessesRequest{Node: "b"}); return err }(),
			func() error { _, err := a.GetProcess(ctx, &inspectv1.GetProcessRequest{Node: "b"}); return err }(),
			func() error { _, err := a.SetLogLevel(ctx, &inspectv1.SetLogLevelRequest{Node: "b"}); return err }(),
			func() error { _, err := a.Send(ctx, &inspectv1.SendRequest{Node: "b"}); return err }(),
			func() error { _, err := a.Call(ctx, &inspectv1.CallRequest{Node: "b"}); return err }(),
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
	})
}

func TestWatchLocalAndForwarded(t *testing.T) {
	for _, node := range []string{"", "b"} {
		t.Run("node="+node, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				c := cluster(t, nil, "a", "b")
				a := client(c, "a")
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
					_, _ = target.Spawn(func(p *grpcproc.Process[*testpb.Ping]) error { return nil })
					select {
					case ev := <-seen:
						if ev.Kind == grpcproc.EventSpawn && ev.Process.PID.Node == target.Name() {
							return
						}
					case <-time.After(20 * time.Millisecond):
					case <-deadline:
						t.Fatal("no spawn event")
					}
				}
			})
		})
	}
}

func TestWatchEndsWhenNodeStops(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
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
	})
}

// fakeWatch is a server stream whose Send fails.
type fakeWatch struct {
	grpc.ServerStream
	ctx context.Context
}

func (f *fakeWatch) Context() context.Context            { return f.ctx }
func (f *fakeWatch) Send(*inspectv1.WatchResponse) error { return errors.New("client gone") }

// fakePeer is an Inspector client whose Watch fails to open (err), or opens
// a stream that fails on the first Recv (recvErr), or yields one event.
type fakePeer struct {
	inspectv1.InspectorClient
	err, recvErr error
}

func (f *fakePeer) Watch(context.Context, *inspectv1.WatchRequest, ...grpc.CallOption) (grpc.ServerStreamingClient[inspectv1.WatchResponse], error) {
	if f.err != nil {
		return nil, f.err
	}
	return &oneEvent{err: f.recvErr}, nil
}

// oneEvent is a Watch client stream whose Recv fails with err, if set, or
// yields an event.
type oneEvent struct {
	grpc.ClientStream
	err error
}

func (o *oneEvent) Recv() (*inspectv1.WatchResponse, error) {
	if o.err != nil {
		return nil, o.err
	}
	return &inspectv1.WatchResponse{Event: &inspectv1.Event{}}, nil
}

func TestWatchSendFailures(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		c := grpcproctest.New(t, "a")
		n := c.Node("a")
		stream := &fakeWatch{ctx: t.Context()}

		// Local: the first event cannot be sent to the client.
		srv := inspect.New(n)
		done := make(chan error, 1)
		go func() { done <- srv.Watch(&inspectv1.WatchRequest{}, stream) }()
		for {
			_, _ = n.Spawn(func(p *grpcproc.Process[*testpb.Ping]) error { return nil })
			select {
			case err := <-done:
				if err == nil || err.Error() != "client gone" {
					t.Fatalf("local: %v", err)
				}
				goto forwarded
			default:
			}
		}
	forwarded:
		// Forwarded: the peer's stream cannot be opened, or its event cannot be relayed.
		upstreamErr := errors.New("peer refused")
		srv = inspect.New(n, inspect.WithPeers(func(context.Context, string) (inspectv1.InspectorClient, error) {
			return &fakePeer{err: upstreamErr}, nil
		}))
		if err := srv.Watch(&inspectv1.WatchRequest{Node: "b"}, stream); err == nil || !strings.Contains(err.Error(), "node b: peer refused") {
			t.Fatalf("upstream: %v", err)
		}
		srv = inspect.New(n, inspect.WithPeers(func(context.Context, string) (inspectv1.InspectorClient, error) {
			return &fakePeer{}, nil
		}))
		if err := srv.Watch(&inspectv1.WatchRequest{Node: "b"}, stream); err == nil || err.Error() != "client gone" {
			t.Fatalf("relay: %v", err)
		}
		// The peer's stream ends: its error is returned as is.
		recvErr := errors.New("peer stream ended")
		srv = inspect.New(n, inspect.WithPeers(func(context.Context, string) (inspectv1.InspectorClient, error) {
			return &fakePeer{recvErr: recvErr}, nil
		}))
		if err := srv.Watch(&inspectv1.WatchRequest{Node: "b"}, stream); err == nil || !strings.Contains(err.Error(), "node b: peer stream ended") {
			t.Fatalf("peer end: %v", err)
		}
		// The peer ends it cleanly (EOF): so does the relay.
		srv = inspect.New(n, inspect.WithPeers(func(context.Context, string) (inspectv1.InspectorClient, error) {
			return &fakePeer{recvErr: io.EOF}, nil
		}))
		if err := srv.Watch(&inspectv1.WatchRequest{Node: "b"}, stream); err != nil {
			t.Fatalf("clean peer end: %v", err)
		}
	})
}

func TestWatchEndsWhenClientCancels(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		c := grpcproctest.New(t, "a")
		ctx, cancel := context.WithCancel(t.Context())
		cancel()
		// A cancelled client is a clean end, not an error.
		if err := inspect.New(c.Node("a")).Watch(&inspectv1.WatchRequest{}, &fakeWatch{ctx: ctx}); err != nil {
			t.Fatalf("got %v", err)
		}
	})
}

// okWatch is a server stream that accepts every event.
type okWatch struct{ fakeWatch }

func (*okWatch) Send(*inspectv1.WatchResponse) error { return nil }

func TestWatchEndsWithUnavailableWhenNodeStops(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		// A node on its own, not behind a gRPC server that would cancel the
		// stream first: only the node stopping can end this Watch.
		n, err := grpcproc.NewNode(grpcproc.Config{Name: "solo", Resolver: grpcproc.StaticResolver{}})
		if err != nil {
			t.Fatal(err)
		}
		done := make(chan error, 1)
		go func() { done <- inspect.New(n).Watch(&inspectv1.WatchRequest{}, &okWatch{fakeWatch{ctx: t.Context()}}) }()
		time.Sleep(20 * time.Millisecond)
		_ = n.Stop(t.Context())
		if err := <-done; status.Code(err) != codes.Unavailable {
			t.Fatalf("got %v", err)
		}
	})
}

// The client picks a watch's buffer, and the server allocates it: it is
// capped, and a relayed watch is capped before it reaches a peer, which may
// predate the cap. Before, the largest request allocated 1.8 TB.
func TestWatchCapsItsBuffer(t *testing.T) {
	for _, node := range []string{"", "b"} {
		t.Run("node="+node, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
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
		})
	}
}

// An inspect timeout of 0 or less is the default, as an absent one is: a
// negative one used to expire before the process could answer.
func TestNegativeInspectTimeout(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		c := cluster(t, nil, "a")
		addr, err := c.Node("a").Spawn(func(p *grpcproc.Process[proto.Message]) error {
			_, err := p.Receive()
			return err
		}, grpcproc.WithInspect(func() map[string]string { return map[string]string{"state": "idle"} }))
		if err != nil {
			t.Fatal(err)
		}
		for range 20 {
			resp, err := client(c, "a").GetProcess(t.Context(), &inspectv1.GetProcessRequest{
				Target: byPID(addr.PID()), Inspect: true, InspectTimeout: durationpb.New(-time.Second),
			})
			if err != nil || resp.GetInspect()["state"] != "idle" {
				t.Fatalf("%v %v", resp, err)
			}
		}
	})
}
