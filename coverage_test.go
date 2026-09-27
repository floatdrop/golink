package grpcproc_test

import (
	"bytes"
	"context"
	"errors"
	"io"
	"log/slog"
	"net"
	"strings"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
	"google.golang.org/protobuf/proto"

	"github.com/floatdrop/grpcproc"
	"github.com/floatdrop/grpcproc/grpcproctest"
	"github.com/floatdrop/grpcproc/internal/testpb"
	grpcprocv1 "github.com/floatdrop/grpcproc/proto/grpcproc/v1"
)

func TestAddrForms(t *testing.T) {
	pid := grpcproc.PID{Node: "n", Incarnation: 3, ID: 7}
	byPID := grpcproc.AddrOf[*testpb.Ping](pid)
	if byPID.PID() != pid || byPID.Node() != "n" || byPID.Name() != "" || byPID.String() != "<n.3.7>" {
		t.Fatalf("%v %v", byPID, byPID.PID())
	}
	byName := grpcproc.AddrOf[*testpb.Ping](grpcproc.Name{Node: "n", Name: "svc"})
	if byName.Name() != "svc" || byName.Node() != "n" || byName.String() != "{svc@n}" || !byName.PID().IsZero() == false && byName.PID().Node != "n" {
		t.Fatalf("%v", byName)
	}
	if !(grpcproc.PID{}).IsZero() || pid.IsZero() {
		t.Fatal("IsZero")
	}
	if (grpcproc.Ref{Node: "n", ID: 4}).String() != "#n.4" || (grpcproc.NodeID{Name: "n", Incarnation: 2}).String() != "n#2" {
		t.Fatal("String")
	}
	if grpcproc.StateIdle.String() != "idle" || grpcproc.StateExiting.String() != "exiting" || grpcproc.LinkUp.String() != "up" {
		t.Fatal("state strings")
	}
	if (&grpcproc.RemoteError{Msg: "x"}).Error() != "x" || (&grpcproc.ExitError{Reason: "r"}).Error() != "grpcproc: exit: r" {
		t.Fatal("error strings")
	}
	le := &grpcproc.LinkError{Peer: "b", Err: io.EOF}
	if !errors.Is(le, io.EOF) || !errors.Is(le, grpcproc.ErrNoConnection) || !strings.Contains(le.Error(), "link to b") {
		t.Fatal("LinkError")
	}
}

func TestMetadataMerge(t *testing.T) {
	ctx := grpcproc.WithMetadata(t.Context(), grpcproc.Metadata{"a": "1", "b": "1"})
	ctx = grpcproc.WithMetadata(ctx, grpcproc.Metadata{"b": "2"})
	md := grpcproc.MetadataFrom(ctx)
	if md["a"] != "1" || md["b"] != "2" || grpcproc.MetadataFrom(t.Context()) != nil {
		t.Fatalf("%v", md)
	}
	m := grpcproc.Msg[proto.Message]{Metadata: md}
	if grpcproc.MetadataFrom(m.Context(t.Context()))["b"] != "2" {
		t.Fatal("Msg.Context")
	}
	if (grpcproc.Msg[proto.Message]{}).Context(ctx) != ctx {
		t.Fatal("Msg.Context without metadata must return parent")
	}
}

func TestNopHooks(t *testing.T) {
	var h grpcproc.Hooks = grpcproc.NopHooks{}
	h.OnSpawn(grpcproc.ProcessInfo{})
	h.OnExit(grpcproc.ProcessInfo{}, "")
	if md, d := h.OnSend(grpcproc.SendInfo{}, grpcproc.Metadata{"k": "v"}); md["k"] != "v" || d != nil {
		t.Fatal("OnSend must pass metadata through")
	}
	if md, d := h.OnReceive(grpcproc.ReceiveInfo{}, grpcproc.Metadata{"k": "v"}); md["k"] != "v" || d != nil {
		t.Fatal("OnReceive must pass metadata through")
	}
	h.OnDeadLetter(grpcproc.PID{}, grpcproc.PID{}, nil, "")
	h.OnLinkUp(grpcproc.NodeID{})
	h.OnLinkDown(grpcproc.NodeID{}, nil)
}

func TestNewNodeValidation(t *testing.T) {
	if _, err := grpcproc.NewNode(grpcproc.Config{}); err == nil {
		t.Fatal("name required")
	}
	if _, err := grpcproc.NewNode(grpcproc.Config{Name: "a"}); err == nil {
		t.Fatal("resolver required")
	}
	n, err := grpcproc.NewNode(grpcproc.Config{Name: "a", Resolver: grpcproc.StaticResolver{}})
	if err != nil {
		t.Fatal(err)
	}
	if n.Name() != "a" || n.ID().Incarnation == 0 || n.PID().Node != "a" || !n.Info().StartedAt.IsZero() {
		t.Fatalf("defaults: %+v", n.Info())
	}
	// Unknown peer: the resolver refuses, the error is a connection error.
	if err := n.SendTo(t.Context(), grpcproc.Named[*testpb.Ping]("nowhere", "x"), &testpb.Ping{}); !errors.Is(err, grpcproc.ErrNoConnection) {
		t.Fatalf("got %v", err)
	}
	if err := n.SendTo(t.Context(), grpcproc.PID{}, &testpb.Ping{}); err == nil {
		t.Fatal("empty node must be an error")
	}
	if err := n.Stop(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := n.Stop(context.Background()); err != nil {
		t.Fatal("second stop")
	}
	if _, err := n.Spawn(echo); !errors.Is(err, grpcproc.ErrNodeStopped) {
		t.Fatalf("spawn after stop: %v", err)
	}
	if err := n.SendTo(t.Context(), grpcproc.Named[*testpb.Ping]("b", "x"), &testpb.Ping{}); !errors.Is(err, grpcproc.ErrNodeStopped) {
		t.Fatalf("send after stop: %v", err)
	}
}

func TestStopTimesOut(t *testing.T) {
	n, _ := grpcproc.NewNode(grpcproc.Config{Name: "a", Resolver: grpcproc.StaticResolver{}})
	block := make(chan struct{})
	_, _ = n.Spawn[*testpb.Ping](func(p *grpcproc.Process[*testpb.Ping]) error { <-block; return nil })
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if err := n.Stop(ctx); err == nil || !strings.Contains(err.Error(), "stop") {
		t.Fatalf("got %v", err)
	}
	close(block)
}

func TestProcessAccessorsAndNames(t *testing.T) {
	var buf bytes.Buffer
	log := slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelInfo}))
	c := grpcproctest.NewWith(t, []grpcproctest.Option{grpcproctest.WithLogger(log)}, "a")
	a := c.Node("a")
	ready := make(chan *grpcproc.Process[*testpb.Ping], 1)
	addr, _ := a.Spawn[*testpb.Ping](func(p *grpcproc.Process[*testpb.Ping]) error {
		ready <- p
		_, err := p.Receive()
		return err
	}, grpcproc.WithName("one"))
	p := <-ready
	if p.PID() != addr.PID() || p.Node() != a || p.Addr() != addr || p.Context().Err() != nil {
		t.Fatal("accessors")
	}
	if pid, ok := a.Whereis("one"); !ok || pid != addr.PID() {
		t.Fatal("whereis")
	}
	info, _ := a.Process(addr.PID())
	if !info.Parent.IsZero() || info.Name != "one" || info.LogLevel != slog.LevelInfo {
		t.Fatalf("%+v", info)
	}
	// Per-process log level: the node's handler is at Info, so debug is dropped
	// by default; a process can be made more verbose, or quieter, on its own.
	p.Log().Debug("hidden")
	if err := a.SetLogLevel(addr.PID(), slog.LevelDebug); err != nil {
		t.Fatal(err)
	}
	p.Log().WithGroup("g").Debug("shown", "k", "v")
	if info, _ = a.Process(addr.PID()); info.LogLevel != slog.LevelDebug {
		t.Fatalf("%+v", info)
	}
	if err := a.SetLogLevel(addr.PID(), slog.LevelError); err != nil {
		t.Fatal(err)
	}
	p.Log().Info("dropped by process threshold")
	out := buf.String()
	if strings.Contains(out, "hidden") || !strings.Contains(out, "shown") || strings.Contains(out, "dropped") {
		t.Fatalf("log output:\n%s", out)
	}
	if err := a.SetLogLevel(grpcproc.PID{Node: "a"}, slog.LevelDebug); !errors.Is(err, grpcproc.ErrNoProc) {
		t.Fatal(err)
	}
	if _, ok := a.Process(grpcproc.PID{Node: "zzz"}); ok {
		t.Fatal("foreign pid")
	}
	if _, err := a.Inspect(ctx(t), grpcproc.PID{Node: "a"}); !errors.Is(err, grpcproc.ErrNoProc) {
		t.Fatal(err)
	}
	// No WithInspect: an empty answer, but an answer.
	if m, err := a.Inspect(ctx(t), addr.PID()); err != nil || m != nil {
		t.Fatalf("%v %v", m, err)
	}
	// The exit event is published once the process is gone and its name free.
	events := a.Subscribe(t.Context(), 16)
	if err := a.Exit(t.Context(), addr, grpcproc.ReasonKilled); err != nil {
		t.Fatal(err)
	}
	for e := range events {
		if e.Kind == grpcproc.EventExit && e.Process.PID == addr.PID() {
			break
		}
	}
	if _, ok := a.Whereis("one"); ok {
		t.Fatal("a name outlived its process")
	}
	if _, err := a.Inspect(ctx(t), addr.PID()); !errors.Is(err, grpcproc.ErrNoProc) {
		t.Fatal(err)
	}
	// Monitor from an exited process is a no-op that still returns a ref.
	if r := p.Monitor(addr); r.ID == 0 {
		t.Fatal("ref")
	}
}

func TestReceiveTimeoutAndExitError(t *testing.T) {
	c := grpcproctest.New(t, "a")
	a := c.Node("a")
	w, ch := watcher(t, a)
	results := make(chan error, 2)
	addr, _ := a.Spawn[*testpb.Ping](func(p *grpcproc.Process[*testpb.Ping]) error {
		_, err := p.ReceiveTimeout(10 * time.Millisecond)
		results <- err
		_, err = p.ReceiveTimeout(time.Minute)
		results <- err
		return err
	})
	w.Monitor(addr)
	if err := <-results; !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("timeout: %v", err)
	}
	time.Sleep(10 * time.Millisecond)
	_ = a.Exit(t.Context(), addr, "bye")
	if err := <-results; err == nil {
		t.Fatal("want exit error")
	} else if ee, ok := errors.AsType[*grpcproc.ExitError](err); !ok || ee.Reason != "bye" {
		t.Fatalf("exit: %v", err)
	}
	if m := recv(t, ch); m.Down == nil || m.Down.Reason != "bye" {
		t.Fatalf("%+v", m.Down)
	}
	// A process may also return an ExitError itself.
	addr2, _ := a.Spawn[*testpb.Ping](func(p *grpcproc.Process[*testpb.Ping]) error {
		return &grpcproc.ExitError{Reason: "custom"}
	})
	w.Monitor(addr2)
	if m := recv(t, ch); m.Down == nil || m.Down.Reason != "custom" && m.Down.Reason != grpcproc.ReasonNoProc {
		t.Fatalf("%+v", m.Down)
	}
}

func TestProcessSendReplyAndMonitorVariants(t *testing.T) {
	c := grpcproctest.New(t, "a", "b")
	a, b := c.Node("a"), c.Node("b")
	col, ch := collector(t, b)
	e, _ := b.Spawn(echo, grpcproc.WithName("echo"))
	done := make(chan struct{})
	_, _ = a.Spawn[proto.Message](func(p *grpcproc.Process[proto.Message]) error {
		defer close(done)
		if err := p.Send(col, proto.Message(&testpb.Ping{N: 1})); err != nil {
			return err
		}
		// CallTo from a process, to an untyped target.
		if r, err := p.CallTo[*testpb.Pong](t.Context(), grpcproc.Name{Node: "b", Name: "echo"}, &testpb.Ping{N: 1}); err != nil || r.N != 2 {
			t.Errorf("CallTo: %v %v", r, err)
		}
		// Reply to something that is not a call.
		if err := p.Reply(grpcproc.Msg[proto.Message]{}, nil, nil); !errors.Is(err, grpcproc.ErrNotCall) {
			t.Errorf("got %v", err)
		}
		// Monitor by name and demonitor it: the by-name branch.
		ref := p.Monitor(grpcproc.Name{Node: "b", Name: "echo"})
		p.Demonitor(ref)
		p.Demonitor(ref) // unknown ref: no-op
		// Monitoring through a node that cannot be reached is an immediate noconnection.
		ref = p.Monitor(grpcproc.Named[*testpb.Ping]("nowhere", "x"))
		m, err := p.Receive()
		if err != nil {
			return err
		}
		if m.Down == nil || m.Down.Ref != ref || m.Down.Reason != grpcproc.ReasonNoConnection {
			t.Errorf("got %+v", m.Down)
		}
		// Exit a remote process by name, from a process.
		return p.Exit(grpcproc.Name{Node: "b", Name: "echo"}, grpcproc.ReasonKilled)
	})
	if m := recv(t, ch); len(m.Metadata) != 0 || !proto.Equal(m.Body, &testpb.Ping{N: 1}) {
		t.Fatalf("%+v", m)
	}
	<-done
	// The echo was told to exit.
	deadline := time.Now().Add(time.Second)
	for {
		if _, ok := b.Process(e.PID()); !ok {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("echo still alive")
		}
		time.Sleep(5 * time.Millisecond)
	}
	// A process exiting while it monitors others (by name and by pid) cleans up.
	e2, _ := b.Spawn(echo, grpcproc.WithName("echo2"))
	_, _ = a.Spawn[proto.Message](func(p *grpcproc.Process[proto.Message]) error {
		p.Monitor(e2)
		p.Monitor(grpcproc.Name{Node: "b", Name: "echo2"})
		return nil
	})
	time.Sleep(50 * time.Millisecond)
	if info, _ := b.Process(e2.PID()); info.Watchers != 0 {
		t.Fatalf("watchers not cleaned: %+v", info)
	}
}

func TestEncodeErrors(t *testing.T) {
	// Invalid UTF-8 in a proto3 string cannot be marshalled.
	c := grpcproctest.New(t, "a", "b")
	a, b := c.Node("a"), c.Node("b")
	bad := &testpb.Reserve{Id: "\xff"}
	col, _ := collector(t, b)
	if err := a.SendTo(t.Context(), col, bad); err == nil || !strings.Contains(err.Error(), "encode") {
		t.Fatalf("send: %v", err)
	}
	if _, err := a.CallTo[*testpb.Pong](ctx(t), col, bad); err == nil || !strings.Contains(err.Error(), "encode") {
		t.Fatalf("call: %v", err)
	}
	// A reply that cannot be encoded is reported to the replier.
	replyErr := make(chan error, 1)
	svc, _ := b.Spawn[*testpb.Ping](func(p *grpcproc.Process[*testpb.Ping]) error {
		m, err := p.Receive()
		if err != nil {
			return err
		}
		replyErr <- p.Reply(m, bad, nil)
		return nil
	})
	go func() { _, _ = a.CallTo[*testpb.Pong](ctx(t), svc, &testpb.Ping{}) }()
	if err := <-replyErr; err == nil || !strings.Contains(err.Error(), "encode") {
		t.Fatalf("reply: %v", err)
	}
}

func TestInspectWhileBacklogged(t *testing.T) {
	c := grpcproctest.New(t, "a")
	a := c.Node("a")
	release := make(chan struct{})
	entered := make(chan struct{}, 1)
	handled := 0
	e, _ := a.Spawn[*testpb.Ping](func(p *grpcproc.Process[*testpb.Ping]) error {
		for {
			m, err := p.Receive()
			if err != nil {
				return err
			}
			handled++
			if m.Body.N == 7 {
				entered <- struct{}{}
				<-release
			}
		}
	}, grpcproc.WithInspect(func() map[string]string { return map[string]string{"n": "x"} }))
	_ = a.SendTo(t.Context(), e, &testpb.Ping{N: 7})
	<-entered
	_ = a.SendTo(t.Context(), e, &testpb.Ping{N: 1}) // queued behind the busy handler
	got := make(chan error, 1)
	go func() { _, err := a.Inspect(ctx(t), e.PID()); got <- err }()
	time.Sleep(20 * time.Millisecond)
	close(release) // the pending inspect is served before the queued message
	if err := <-got; err != nil {
		t.Fatal(err)
	}
	// The inspect function itself blocking: the caller's ctx bounds the wait.
	blockInspect := make(chan struct{})
	e2, _ := a.Spawn[*testpb.Ping](func(p *grpcproc.Process[*testpb.Ping]) error {
		_, err := p.Receive()
		return err
	}, grpcproc.WithInspect(func() map[string]string { <-blockInspect; return nil }))
	short, cancel := context.WithTimeout(t.Context(), 30*time.Millisecond)
	defer cancel()
	if _, err := a.Inspect(short, e2.PID()); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("got %v", err)
	}
	close(blockInspect)
}

// fakePeer is a grpcproc.v1.Node server that misbehaves in a chosen way.
type fakePeer struct {
	grpcprocv1.UnimplementedNodeServer
	hello *grpcprocv1.Frame
	err   error
	hang  bool
}

func (f *fakePeer) Link(stream grpc.BidiStreamingServer[grpcprocv1.Frame, grpcprocv1.Frame]) error {
	if f.err != nil {
		return f.err
	}
	if f.hang {
		<-stream.Context().Done()
		return nil
	}
	if err := stream.Send(f.hello); err != nil {
		return err
	}
	for {
		if _, err := stream.Recv(); err != nil {
			return nil
		}
	}
}

func nodeAgainst(t *testing.T, peer *fakePeer, dialTimeout time.Duration) *grpcproc.Node {
	t.Helper()
	ln := bufconn.Listen(1 << 20)
	srv := grpc.NewServer()
	grpcprocv1.RegisterNodeServer(srv, peer)
	go func() { _ = srv.Serve(ln) }()
	t.Cleanup(srv.Stop)
	n, err := grpcproc.NewNode(grpcproc.Config{
		Name:     "a",
		Resolver: grpcproc.StaticResolver{"b": "passthrough:///b"},
		DialOptions: []grpc.DialOption{
			grpc.WithTransportCredentials(insecure.NewCredentials()),
			grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) { return ln.DialContext(ctx) }),
		},
		DialTimeout: dialTimeout,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = n.Stop(context.Background()) })
	return n
}

func TestHandshakeFailures(t *testing.T) {
	frame := func(envs ...*grpcprocv1.Envelope) *grpcprocv1.Frame { return &grpcprocv1.Frame{Envelopes: envs} }
	hello := func(node string, version uint32) *grpcprocv1.Frame {
		return frame(&grpcprocv1.Envelope{Kind: grpcprocv1.Kind_KIND_HELLO, Hello: &grpcprocv1.Hello{Node: node, Incarnation: 1, Version: version}})
	}
	cases := []struct {
		name string
		peer *fakePeer
		want string
	}{
		{"server error", &fakePeer{err: status.Error(codes.PermissionDenied, "no")}, "PermissionDenied"},
		{"not a hello", &fakePeer{hello: frame(&grpcprocv1.Envelope{Kind: grpcprocv1.Kind_KIND_EXIT})}, "expected Hello"},
		{"empty frame", &fakePeer{hello: frame()}, "expected Hello"},
		{"wrong version", &fakePeer{hello: hello("b", 99)}, "protocol 99"},
		{"wrong node", &fakePeer{hello: hello("c", 1)}, `reached "c"`},
		{"no hello", &fakePeer{hang: true}, "deadline"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			n := nodeAgainst(t, tc.peer, 200*time.Millisecond)
			err := n.SendTo(t.Context(), grpcproc.Named[*testpb.Ping]("b", "x"), &testpb.Ping{})
			if !errors.Is(err, grpcproc.ErrNoConnection) || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("got %v", err)
			}
		})
	}
}

func TestInboundRejections(t *testing.T) {
	c := grpcproctest.New(t, "a")
	// Reach a's server directly with hand-made metadata.
	ln := bufconn.Listen(1 << 20)
	srv := grpc.NewServer()
	c.Node("a").Register(srv)
	go func() { _ = srv.Serve(ln) }()
	t.Cleanup(srv.Stop)
	cc, err := grpc.NewClient("passthrough:///a",
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) { return ln.DialContext(ctx) }))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = cc.Close() })
	client := grpcprocv1.NewNodeClient(cc)
	open := func(md ...string) error {
		c, cancel := context.WithTimeout(t.Context(), time.Second)
		if len(md) > 0 {
			c = metadataCtx(c, md...)
		}
		stream, err := client.Link(c)
		if err != nil {
			cancel()
			return err
		}
		_, err = stream.Recv()
		if err != nil {
			cancel()
			return err
		}
		t.Cleanup(cancel) // keep the stream open for the rest of the test
		return nil
	}
	if err := open(); status.Code(err) != codes.FailedPrecondition { // no version at all
		t.Fatalf("no metadata: %v", err)
	}
	// A node speaking another protocol version is refused, saying which.
	if err := open("grpcproc-version", "9", "grpcproc-node", "other"); status.Code(err) != codes.FailedPrecondition || !strings.Contains(err.Error(), "protocol version 9") {
		t.Fatalf("protocol 9: %v", err)
	}
	if err := open("grpcproc-version", "1"); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("no name: %v", err)
	}
	if err := open("grpcproc-version", "1", "grpcproc-node", "a"); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("own name: %v", err)
	}
	if err := open("grpcproc-version", "1", "grpcproc-node", "z", "grpcproc-incarnation", "5"); err != nil {
		t.Fatalf("good hello: %v", err)
	}
	// A second link from the same peer replaces the first, which is still open.
	if err := open("grpcproc-version", "1", "grpcproc-node", "z", "grpcproc-incarnation", "6"); err != nil {
		t.Fatalf("replacement: %v", err)
	}
	deadline := time.Now().Add(time.Second)
	for {
		links := c.Node("a").Info().Links
		if len(links) == 1 && links[0].Peer.Incarnation == 6 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("links %+v", links)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func TestCopyLocal(t *testing.T) {
	n, err := grpcproc.NewNode(grpcproc.Config{Name: "a", Resolver: grpcproc.StaticResolver{}, CopyLocal: true})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = n.Stop(context.Background()) })
	e, _ := n.Spawn(echo)
	msg := &testpb.Ping{N: 1}
	if err := n.SendTo(t.Context(), e, msg); err != nil {
		t.Fatal(err)
	}
	msg.N = -100 // would crash the echo if the pointer were shared
	req := &testpb.Ping{N: 1}
	r, err := n.CallTo[*testpb.Pong](ctx(t), e, req)
	if err != nil || r.N != 2 {
		t.Fatalf("%v %v", r, err)
	}
	if _, ok := n.Process(e.PID()); !ok {
		t.Fatal("echo crashed on a mutated message")
	}
}

func TestCallFailsWhenCalleeExitsMidCall(t *testing.T) {
	c := grpcproctest.New(t, "a", "b")
	a, b := c.Node("a"), c.Node("b")
	// The callee takes the request and exits without replying.
	quitter, _ := b.Spawn[*testpb.Ping](func(p *grpcproc.Process[*testpb.Ping]) error {
		_, err := p.Receive()
		return err
	})
	if _, err := a.CallTo[*testpb.Pong](ctx(t), quitter, &testpb.Ping{}); !errors.Is(err, grpcproc.ErrNoProc) {
		t.Fatalf("got %v", err)
	}
}

func TestOrderingOfSnapshots(t *testing.T) {
	c := grpcproctest.New(t, "a", "b", "c")
	a := c.Node("a")
	e1, _ := a.Spawn(echo)
	e2, _ := a.Spawn(echo)
	if all := a.Processes(); len(all) != 2 || all[0].PID != e1.PID() || all[1].PID != e2.PID() {
		t.Fatalf("%+v", all)
	}
	for _, peer := range []string{"c", "b"} {
		e, _ := c.Node(peer).Spawn(echo)
		if _, err := a.CallTo[*testpb.Pong](ctx(t), e, &testpb.Ping{N: 1}); err != nil {
			t.Fatal(err)
		}
	}
	links := a.Info().Links
	if len(links) != 4 || links[0].Peer.Name != "b" || !links[0].Outbound || links[1].Outbound || links[2].Peer.Name != "c" {
		t.Fatalf("%+v", links)
	}
	if peers := a.Peers(); len(peers) != 2 || peers[0] != "b" {
		t.Fatalf("%v", peers)
	}
	// A local monitor, demonitored.
	w, ch := watcher(t, a)
	ref := w.Monitor(e1)
	if info, _ := a.Process(e1.PID()); info.Watchers != 1 {
		t.Fatalf("%+v", info)
	}
	w.Demonitor(ref)
	if info, _ := a.Process(e1.PID()); info.Watchers != 0 {
		t.Fatalf("%+v", info)
	}
	_ = a.SendTo(t.Context(), e1, &testpb.Ping{N: 0})
	select {
	case m := <-ch:
		t.Fatalf("unexpected %+v", m)
	case <-time.After(100 * time.Millisecond):
	}
}

func TestAuthorize(t *testing.T) {
	ln := bufconn.Listen(1 << 20)
	srv := grpc.NewServer()
	n, err := grpcproc.NewNode(grpcproc.Config{
		Name:     "b",
		Resolver: grpcproc.StaticResolver{},
		Authorize: func(_ context.Context, peer grpcproc.NodeID) error {
			if peer.Name != "trusted" {
				return errors.New("unknown peer " + peer.Name)
			}
			return nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	n.Register(srv)
	go func() { _ = srv.Serve(ln) }()
	t.Cleanup(srv.Stop)
	t.Cleanup(func() { _ = n.Stop(context.Background()) })
	for _, name := range []string{"a", "trusted"} {
		a, err := grpcproc.NewNode(grpcproc.Config{
			Name:     name,
			Resolver: grpcproc.StaticResolver{"b": "passthrough:///b"},
			DialOptions: []grpc.DialOption{
				grpc.WithTransportCredentials(insecure.NewCredentials()),
				grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) { return ln.DialContext(ctx) }),
			},
		})
		if err != nil {
			t.Fatal(err)
		}
		err = a.SendTo(t.Context(), grpcproc.Named[*testpb.Ping]("b", "x"), &testpb.Ping{})
		if name == "a" && (err == nil || !strings.Contains(err.Error(), "unknown peer a")) {
			t.Fatalf("a: %v", err)
		}
		if name == "trusted" && err != nil {
			t.Fatalf("trusted: %v", err)
		}
		_ = a.Stop(context.Background())
	}
}

func TestLinkStatsAfterKill(t *testing.T) {
	c := grpcproctest.New(t, "a", "b")
	a := c.Node("a")
	e, _ := c.Node("b").Spawn(echo)
	if _, err := a.CallTo[*testpb.Pong](ctx(t), e, &testpb.Ping{N: 1}); err != nil {
		t.Fatal(err)
	}
	if links := a.Info().Links; len(links) != 2 || links[0].State != grpcproc.LinkUp || links[0].Messages == 0 {
		t.Fatalf("%+v", links)
	}
	c.Kill("b")
	if links := a.Info().Links; len(links) != 0 {
		t.Fatalf("links after kill: %+v", links)
	}
}
