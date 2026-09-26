package golink_test

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

	"github.com/floatdrop/golink"
	"github.com/floatdrop/golink/golinktest"
	"github.com/floatdrop/golink/internal/testpb"
	golinkv1 "github.com/floatdrop/golink/proto/golink/v1"
)

func TestAddrForms(t *testing.T) {
	pid := golink.PID{Node: "n", Incarnation: 3, ID: 7}
	byPID := golink.AddrOf[*testpb.Ping](pid)
	if byPID.PID() != pid || byPID.Node() != "n" || byPID.Name() != "" || byPID.String() != "<n.3.7>" {
		t.Fatalf("%v %v", byPID, byPID.PID())
	}
	byName := golink.AddrOf[*testpb.Ping](golink.Name{Node: "n", Name: "svc"})
	if byName.Name() != "svc" || byName.Node() != "n" || byName.String() != "{svc@n}" || !byName.PID().IsZero() == false && byName.PID().Node != "n" {
		t.Fatalf("%v", byName)
	}
	if !(golink.PID{}).IsZero() || pid.IsZero() {
		t.Fatal("IsZero")
	}
	if (golink.Ref{Node: "n", ID: 4}).String() != "#n.4" || (golink.NodeID{Name: "n", Incarnation: 2}).String() != "n#2" {
		t.Fatal("String")
	}
	if golink.StateIdle.String() != "idle" || golink.StateExiting.String() != "exiting" || golink.LinkUp.String() != "up" {
		t.Fatal("state strings")
	}
	if (&golink.RemoteError{Msg: "x"}).Error() != "x" || (&golink.ExitError{Reason: "r"}).Error() != "golink: exit: r" {
		t.Fatal("error strings")
	}
	le := &golink.LinkError{Peer: "b", Err: io.EOF}
	if !errors.Is(le, io.EOF) || !errors.Is(le, golink.ErrNoConnection) || !strings.Contains(le.Error(), "link to b") {
		t.Fatal("LinkError")
	}
}

func TestMetadataMerge(t *testing.T) {
	ctx := golink.WithMetadata(t.Context(), golink.Metadata{"a": "1", "b": "1"})
	ctx = golink.WithMetadata(ctx, golink.Metadata{"b": "2"})
	md := golink.MetadataFrom(ctx)
	if md["a"] != "1" || md["b"] != "2" || golink.MetadataFrom(t.Context()) != nil {
		t.Fatalf("%v", md)
	}
	m := golink.Msg[proto.Message]{Metadata: md}
	if golink.MetadataFrom(m.Context(t.Context()))["b"] != "2" {
		t.Fatal("Msg.Context")
	}
	if (golink.Msg[proto.Message]{}).Context(ctx) != ctx {
		t.Fatal("Msg.Context without metadata must return parent")
	}
}

func TestNopHooks(t *testing.T) {
	var h golink.Hooks = golink.NopHooks{}
	h.OnSpawn(golink.ProcessInfo{})
	h.OnExit(golink.ProcessInfo{}, "")
	if md, d := h.OnSend(golink.SendInfo{}, golink.Metadata{"k": "v"}); md["k"] != "v" || d != nil {
		t.Fatal("OnSend must pass metadata through")
	}
	if md, d := h.OnReceive(golink.ReceiveInfo{}, golink.Metadata{"k": "v"}); md["k"] != "v" || d != nil {
		t.Fatal("OnReceive must pass metadata through")
	}
	h.OnDeadLetter(golink.PID{}, golink.PID{}, nil, "")
	h.OnLinkUp(golink.NodeID{})
	h.OnLinkDown(golink.NodeID{}, nil)
}

func TestNewNodeValidation(t *testing.T) {
	if _, err := golink.NewNode(golink.Config{}); err == nil {
		t.Fatal("name required")
	}
	if _, err := golink.NewNode(golink.Config{Name: "a"}); err == nil {
		t.Fatal("resolver required")
	}
	n, err := golink.NewNode(golink.Config{Name: "a", Resolver: golink.StaticResolver{}})
	if err != nil {
		t.Fatal(err)
	}
	if n.Name() != "a" || n.ID().Incarnation == 0 || n.PID().Node != "a" || !n.Info().StartedAt.IsZero() {
		t.Fatalf("defaults: %+v", n.Info())
	}
	// Unknown peer: the resolver refuses, the error is a connection error.
	if err := n.SendTo(golink.Named[*testpb.Ping]("nowhere", "x"), &testpb.Ping{}); !errors.Is(err, golink.ErrNoConnection) {
		t.Fatalf("got %v", err)
	}
	if err := n.SendTo(golink.PID{}, &testpb.Ping{}); err == nil {
		t.Fatal("empty node must be an error")
	}
	if err := n.Stop(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := n.Stop(context.Background()); err != nil {
		t.Fatal("second stop")
	}
	if _, err := golink.Spawn(n, echo); !errors.Is(err, golink.ErrNodeStopped) {
		t.Fatalf("spawn after stop: %v", err)
	}
	if err := n.SendTo(golink.Named[*testpb.Ping]("b", "x"), &testpb.Ping{}); !errors.Is(err, golink.ErrNodeStopped) {
		t.Fatalf("send after stop: %v", err)
	}
}

func TestStopTimesOut(t *testing.T) {
	n, _ := golink.NewNode(golink.Config{Name: "a", Resolver: golink.StaticResolver{}})
	block := make(chan struct{})
	_, _ = golink.Spawn[*testpb.Ping](n, func(p *golink.Process[*testpb.Ping]) error { <-block; return nil })
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if err := n.Stop(ctx); err == nil || !strings.Contains(err.Error(), "stop") {
		t.Fatalf("got %v", err)
	}
	close(block)
}

func TestProcessAccessorsAndRegistry(t *testing.T) {
	var buf bytes.Buffer
	log := slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelInfo}))
	c := golinktest.NewWith(t, []golinktest.Option{golinktest.WithLogger(log)}, "a")
	a := c.Node("a")
	parent := golink.PID{Node: "a", Incarnation: a.ID().Incarnation, ID: 99}
	ready := make(chan *golink.Process[*testpb.Ping], 1)
	addr, _ := golink.Spawn[*testpb.Ping](a, func(p *golink.Process[*testpb.Ping]) error {
		ready <- p
		_, err := p.Receive()
		return err
	}, golink.WithParent(parent))
	p := <-ready
	if p.PID() != addr.PID() || p.Node() != a || p.Addr() != addr || p.Context().Err() != nil {
		t.Fatal("accessors")
	}
	if err := p.Register("one"); err != nil {
		t.Fatal(err)
	}
	if err := p.Register("one"); !errors.Is(err, golink.ErrNameTaken) {
		t.Fatal(err)
	}
	info, _ := a.Process(addr.PID())
	if info.Parent != parent || len(info.Names) != 1 || info.LogLevel != slog.LevelInfo {
		t.Fatalf("%+v", info)
	}
	a.Unregister("one")
	a.Unregister("one") // idempotent
	if _, ok := a.Whereis("one"); ok {
		t.Fatal("unregister")
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
	if err := a.SetLogLevel(golink.PID{Node: "a"}, slog.LevelDebug); !errors.Is(err, golink.ErrNoProc) {
		t.Fatal(err)
	}
	if _, ok := a.Process(golink.PID{Node: "zzz"}); ok {
		t.Fatal("foreign pid")
	}
	if _, err := a.Inspect(ctx(t), golink.PID{Node: "a"}); !errors.Is(err, golink.ErrNoProc) {
		t.Fatal(err)
	}
	// No WithInspect: an empty answer, but an answer.
	if m, err := a.Inspect(ctx(t), addr.PID()); err != nil || m != nil {
		t.Fatalf("%v %v", m, err)
	}
	if err := a.Exit(addr, golink.ReasonKilled); err != nil {
		t.Fatal(err)
	}
	<-p.Context().Done()
	time.Sleep(20 * time.Millisecond)
	if err := p.Register("late"); !errors.Is(err, golink.ErrNoProc) {
		t.Fatal(err)
	}
	if _, err := a.Inspect(ctx(t), addr.PID()); !errors.Is(err, golink.ErrNoProc) {
		t.Fatal(err)
	}
	// Monitor from an exited process is a no-op that still returns a ref.
	if r := p.Monitor(addr); r.ID == 0 {
		t.Fatal("ref")
	}
}

func TestReceiveTimeoutAndExitError(t *testing.T) {
	c := golinktest.New(t, "a")
	a := c.Node("a")
	w, ch := watcher(t, a)
	results := make(chan error, 2)
	addr, _ := golink.Spawn[*testpb.Ping](a, func(p *golink.Process[*testpb.Ping]) error {
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
	_ = a.Exit(addr, "bye")
	if err := <-results; err == nil {
		t.Fatal("want exit error")
	} else if ee, ok := errors.AsType[*golink.ExitError](err); !ok || ee.Reason != "bye" {
		t.Fatalf("exit: %v", err)
	}
	if m := recv(t, ch); m.Down == nil || m.Down.Reason != "bye" {
		t.Fatalf("%+v", m.Down)
	}
	// A process may also return an ExitError itself.
	addr2, _ := golink.Spawn[*testpb.Ping](a, func(p *golink.Process[*testpb.Ping]) error {
		return &golink.ExitError{Reason: "custom"}
	})
	w.Monitor(addr2)
	if m := recv(t, ch); m.Down == nil || m.Down.Reason != "custom" && m.Down.Reason != golink.ReasonNoProc {
		t.Fatalf("%+v", m.Down)
	}
}

func TestSendContextReplyAndMonitorVariants(t *testing.T) {
	c := golinktest.New(t, "a", "b")
	a, b := c.Node("a"), c.Node("b")
	col, ch := collector(t, b)
	e, _ := golink.Spawn(b, echo, golink.WithName("echo"))
	done := make(chan struct{})
	_, _ = golink.Spawn[proto.Message](a, func(p *golink.Process[proto.Message]) error {
		defer close(done)
		md := golink.WithMetadata(t.Context(), golink.Metadata{"k": "v"})
		if err := p.SendContext(md, col, proto.Message(&testpb.Ping{N: 1})); err != nil {
			return err
		}
		// CallTo from a process, to an untyped target.
		if r, err := p.CallTo[*testpb.Pong](t.Context(), golink.Name{Node: "b", Name: "echo"}, &testpb.Ping{N: 1}); err != nil || r.N != 2 {
			t.Errorf("CallTo: %v %v", r, err)
		}
		// Reply to something that is not a call.
		if err := p.Reply(golink.Msg[proto.Message]{}, nil, nil); !errors.Is(err, golink.ErrNotCall) {
			t.Errorf("got %v", err)
		}
		// Monitor by name and demonitor it: the by-name branch.
		ref := p.Monitor(golink.Name{Node: "b", Name: "echo"})
		p.Demonitor(ref)
		p.Demonitor(ref) // unknown ref: no-op
		// Monitoring through a node that cannot be reached is an immediate noconnection.
		ref = p.Monitor(golink.Named[*testpb.Ping]("nowhere", "x"))
		m, err := p.Receive()
		if err != nil {
			return err
		}
		if m.Down == nil || m.Down.Ref != ref || m.Down.Reason != golink.ReasonNoConnection {
			t.Errorf("got %+v", m.Down)
		}
		// Exit a remote process by name, from a process.
		return p.Exit(golink.Name{Node: "b", Name: "echo"}, golink.ReasonKilled)
	})
	if m := recv(t, ch); m.Metadata["k"] != "v" {
		t.Fatalf("%v", m.Metadata)
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
	e2, _ := golink.Spawn(b, echo, golink.WithName("echo2"))
	_, _ = golink.Spawn[proto.Message](a, func(p *golink.Process[proto.Message]) error {
		p.Monitor(e2)
		p.Monitor(golink.Name{Node: "b", Name: "echo2"})
		return nil
	})
	time.Sleep(50 * time.Millisecond)
	if info, _ := b.Process(e2.PID()); info.Watchers != 0 {
		t.Fatalf("watchers not cleaned: %+v", info)
	}
}

func TestEncodeErrors(t *testing.T) {
	// Invalid UTF-8 in a proto3 string cannot be marshalled.
	c := golinktest.New(t, "a", "b")
	a, b := c.Node("a"), c.Node("b")
	bad := &testpb.Reserve{Id: "\xff"}
	col, _ := collector(t, b)
	if err := a.SendTo(col, bad); err == nil || !strings.Contains(err.Error(), "encode") {
		t.Fatalf("send: %v", err)
	}
	if _, err := a.CallTo[*testpb.Pong](ctx(t), col, bad); err == nil || !strings.Contains(err.Error(), "encode") {
		t.Fatalf("call: %v", err)
	}
	// A reply that cannot be encoded is reported to the replier.
	replyErr := make(chan error, 1)
	svc, _ := golink.Spawn[*testpb.Ping](b, func(p *golink.Process[*testpb.Ping]) error {
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
	c := golinktest.New(t, "a")
	a := c.Node("a")
	release := make(chan struct{})
	entered := make(chan struct{}, 1)
	handled := 0
	e, _ := golink.Spawn[*testpb.Ping](a, func(p *golink.Process[*testpb.Ping]) error {
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
	}, golink.WithInspect(func() map[string]string { return map[string]string{"n": "x"} }))
	_ = a.SendTo(e, &testpb.Ping{N: 7})
	<-entered
	_ = a.SendTo(e, &testpb.Ping{N: 1}) // queued behind the busy handler
	got := make(chan error, 1)
	go func() { _, err := a.Inspect(ctx(t), e.PID()); got <- err }()
	time.Sleep(20 * time.Millisecond)
	close(release) // the pending inspect is served before the queued message
	if err := <-got; err != nil {
		t.Fatal(err)
	}
	// The inspect function itself blocking: the caller's ctx bounds the wait.
	blockInspect := make(chan struct{})
	e2, _ := golink.Spawn[*testpb.Ping](a, func(p *golink.Process[*testpb.Ping]) error {
		_, err := p.Receive()
		return err
	}, golink.WithInspect(func() map[string]string { <-blockInspect; return nil }))
	short, cancel := context.WithTimeout(t.Context(), 30*time.Millisecond)
	defer cancel()
	if _, err := a.Inspect(short, e2.PID()); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("got %v", err)
	}
	close(blockInspect)
}

// fakePeer is a golink.v1.Node server that misbehaves in a chosen way.
type fakePeer struct {
	golinkv1.UnimplementedNodeServer
	hello *golinkv1.Frame
	err   error
	hang  bool
}

func (f *fakePeer) Link(stream grpc.BidiStreamingServer[golinkv1.Frame, golinkv1.Frame]) error {
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

func nodeAgainst(t *testing.T, peer *fakePeer, dialTimeout time.Duration) *golink.Node {
	t.Helper()
	ln := bufconn.Listen(1 << 20)
	srv := grpc.NewServer()
	golinkv1.RegisterNodeServer(srv, peer)
	go func() { _ = srv.Serve(ln) }()
	t.Cleanup(srv.Stop)
	n, err := golink.NewNode(golink.Config{
		Name:     "a",
		Resolver: golink.StaticResolver{"b": "passthrough:///b"},
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
	frame := func(envs ...*golinkv1.Envelope) *golinkv1.Frame { return &golinkv1.Frame{Envelopes: envs} }
	hello := func(node string, version uint32) *golinkv1.Frame {
		return frame(&golinkv1.Envelope{Kind: golinkv1.Kind_KIND_HELLO, Hello: &golinkv1.Hello{Node: node, Incarnation: 1, Version: version}})
	}
	cases := []struct {
		name string
		peer *fakePeer
		want string
	}{
		{"server error", &fakePeer{err: status.Error(codes.PermissionDenied, "no")}, "PermissionDenied"},
		{"not a hello", &fakePeer{hello: frame(&golinkv1.Envelope{Kind: golinkv1.Kind_KIND_EXIT})}, "expected Hello"},
		{"empty frame", &fakePeer{hello: frame()}, "expected Hello"},
		{"wrong version", &fakePeer{hello: hello("b", 99)}, "protocol 99"},
		{"wrong node", &fakePeer{hello: hello("c", 1)}, `reached "c"`},
		{"no hello", &fakePeer{hang: true}, "deadline"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			n := nodeAgainst(t, tc.peer, 200*time.Millisecond)
			err := n.SendTo(golink.Named[*testpb.Ping]("b", "x"), &testpb.Ping{})
			if !errors.Is(err, golink.ErrNoConnection) || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("got %v", err)
			}
		})
	}
}

func TestInboundRejections(t *testing.T) {
	c := golinktest.New(t, "a")
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
	client := golinkv1.NewNodeClient(cc)
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
	if err := open("golink-version", "9", "golink-node", "other"); status.Code(err) != codes.FailedPrecondition || !strings.Contains(err.Error(), "protocol version 9") {
		t.Fatalf("protocol 9: %v", err)
	}
	if err := open("golink-version", "1"); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("no name: %v", err)
	}
	if err := open("golink-version", "1", "golink-node", "a"); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("own name: %v", err)
	}
	if err := open("golink-version", "1", "golink-node", "z", "golink-incarnation", "5"); err != nil {
		t.Fatalf("good hello: %v", err)
	}
	// A second link from the same peer replaces the first, which is still open.
	if err := open("golink-version", "1", "golink-node", "z", "golink-incarnation", "6"); err != nil {
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
	n, err := golink.NewNode(golink.Config{Name: "a", Resolver: golink.StaticResolver{}, CopyLocal: true})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = n.Stop(context.Background()) })
	e, _ := golink.Spawn(n, echo)
	msg := &testpb.Ping{N: 1}
	if err := n.SendTo(e, msg); err != nil {
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
	c := golinktest.New(t, "a", "b")
	a, b := c.Node("a"), c.Node("b")
	// The callee takes the request and exits without replying.
	quitter, _ := golink.Spawn[*testpb.Ping](b, func(p *golink.Process[*testpb.Ping]) error {
		_, err := p.Receive()
		return err
	})
	if _, err := a.CallTo[*testpb.Pong](ctx(t), quitter, &testpb.Ping{}); !errors.Is(err, golink.ErrNoProc) {
		t.Fatalf("got %v", err)
	}
}

func TestOrderingOfSnapshots(t *testing.T) {
	c := golinktest.New(t, "a", "b", "c")
	a := c.Node("a")
	e1, _ := golink.Spawn(a, echo)
	e2, _ := golink.Spawn(a, echo)
	if all := a.Processes(); len(all) != 2 || all[0].PID != e1.PID() || all[1].PID != e2.PID() {
		t.Fatalf("%+v", all)
	}
	for _, peer := range []string{"c", "b"} {
		e, _ := golink.Spawn(c.Node(peer), echo)
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
	_ = a.SendTo(e1, &testpb.Ping{N: 0})
	select {
	case m := <-ch:
		t.Fatalf("unexpected %+v", m)
	case <-time.After(100 * time.Millisecond):
	}
}

func TestAuthorize(t *testing.T) {
	ln := bufconn.Listen(1 << 20)
	srv := grpc.NewServer()
	n, err := golink.NewNode(golink.Config{
		Name:     "b",
		Resolver: golink.StaticResolver{},
		Authorize: func(_ context.Context, peer golink.NodeID) error {
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
		a, err := golink.NewNode(golink.Config{
			Name:     name,
			Resolver: golink.StaticResolver{"b": "passthrough:///b"},
			DialOptions: []grpc.DialOption{
				grpc.WithTransportCredentials(insecure.NewCredentials()),
				grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) { return ln.DialContext(ctx) }),
			},
		})
		if err != nil {
			t.Fatal(err)
		}
		err = a.SendTo(golink.Named[*testpb.Ping]("b", "x"), &testpb.Ping{})
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
	c := golinktest.New(t, "a", "b")
	a := c.Node("a")
	e, _ := golink.Spawn(c.Node("b"), echo)
	if _, err := a.CallTo[*testpb.Pong](ctx(t), e, &testpb.Ping{N: 1}); err != nil {
		t.Fatal(err)
	}
	if links := a.Info().Links; len(links) != 2 || links[0].State != golink.LinkUp || links[0].Messages == 0 {
		t.Fatalf("%+v", links)
	}
	c.Kill("b")
	if links := a.Info().Links; len(links) != 0 {
		t.Fatalf("links after kill: %+v", links)
	}
}
