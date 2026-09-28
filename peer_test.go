package grpcproc_test

import (
	"context"
	"errors"
	"net"
	"strings"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/health"
	"google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
	"google.golang.org/protobuf/proto"

	"github.com/floatdrop/grpcproc"
	"github.com/floatdrop/grpcproc/grpcproctest"
	"github.com/floatdrop/grpcproc/internal/testpb"
	grpcprocv1 "github.com/floatdrop/grpcproc/proto/grpcproc/v1"
)

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
		{"no hello", &fakePeer{hang: true}, "no Hello within DialTimeout (200ms): context canceled"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				n := nodeAgainst(t, tc.peer, 200*time.Millisecond)
				err := n.SendTo(t.Context(), grpcproc.Named[*testpb.Ping]("b", "x"), &testpb.Ping{})
				if !errors.Is(err, grpcproc.ErrNoConnection) || !strings.Contains(err.Error(), tc.want) {
					t.Fatalf("got %v", err)
				}
			})
		})
	}
}

// A dial that gets no answer ends at DialTimeout, for a node's send and a
// process's alike: not at gRPC's 20s connect timeout, and not never.
func TestDialTimeoutBoundsTheDial(t *testing.T) {
	tcp, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = tcp.Close() })
	unserved := bufconn.Listen(1 << 20)
	t.Cleanup(func() { _ = unserved.Close() })
	cases := []struct {
		name string
		dial func(ctx context.Context, _ string) (net.Conn, error)
		opts []grpc.DialOption
		want string
	}{{
		// The connect never completes: nothing accepts it.
		name: "no connect",
		dial: func(ctx context.Context, _ string) (net.Conn, error) { return unserved.DialContext(ctx) },
		want: "no connection within DialTimeout (100ms): context canceled",
	}, {
		// The kernel accepts the connection, and nothing ever speaks on it:
		// a frozen process.
		name: "silent peer",
		dial: func(ctx context.Context, _ string) (net.Conn, error) {
			return (&net.Dialer{}).DialContext(ctx, "tcp", tcp.Addr().String())
		},
		want: "no connection within DialTimeout (100ms)",
	}, {
		// WaitForReady waits through failed connects; the message keeps why
		// they failed.
		name: "wait for ready",
		dial: func(context.Context, string) (net.Conn, error) {
			return nil, errors.New("certificate signed by unknown authority")
		},
		opts: []grpc.DialOption{grpc.WithDefaultCallOptions(grpc.WaitForReady(true))},
		want: "no connection within DialTimeout (100ms): latest balancer error",
	}}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			n, err := grpcproc.NewNode(grpcproc.Config{
				Name:     "a",
				Resolver: grpcproc.StaticResolver{"b": "passthrough:///b"},
				DialOptions: append([]grpc.DialOption{
					grpc.WithTransportCredentials(insecure.NewCredentials()),
					grpc.WithContextDialer(tc.dial),
				}, tc.opts...),
				DialTimeout: 100 * time.Millisecond,
				DialBackoff: -1, // the process's send dials too
			})
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = n.Stop(context.Background()) })
			to := grpcproc.Named[*testpb.Ping]("b", "x")
			check := func(who string, start time.Time, err error) {
				t.Helper()
				if !errors.Is(err, grpcproc.ErrNoConnection) || !strings.Contains(err.Error(), tc.want) {
					t.Fatalf("%s: got %v", who, err)
				}
				if tc.opts != nil && !strings.Contains(err.Error(), "certificate signed by unknown authority") {
					t.Fatalf("%s: the cause is lost: %v", who, err)
				}
				if d := time.Since(start); d > 5*time.Second {
					t.Fatalf("%s: the dial took %v", who, d)
				}
			}

			start := time.Now()
			check("node", start, n.SendTo(t.Context(), to, &testpb.Ping{}))

			type result struct {
				start time.Time
				err   error
			}
			sent := make(chan result, 1)
			if _, err := n.Spawn(func(p *grpcproc.Process[proto.Message]) error {
				start := time.Now()
				sent <- result{start, p.SendTo(to, &testpb.Ping{})}
				return nil
			}); err != nil {
				t.Fatal(err)
			}
			select {
			case r := <-sent:
				check("process", r.start, r.err)
			case <-time.After(10 * time.Second):
				t.Fatal("process: the dial did not end")
			}
		})
	}
}

func TestContextBoundsTheDial(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		// Node a resolves its peers only once released, so its first dial hangs.
		release := make(chan struct{})
		var resolves atomic.Int32
		var c *grpcproctest.Cluster
		c = grpcproctest.NewWith(t, []grpcproctest.Option{grpcproctest.WithConfig(func(name string, cfg *grpcproc.Config) {
			if name == "a" {
				cfg.Resolver = grpcproc.ResolverFunc(func(ctx context.Context, node string) (string, error) {
					resolves.Add(1)
					select {
					case <-release:
					case <-ctx.Done():
						return "", ctx.Err()
					}
					return c.Resolver().Resolve(ctx, node)
				})
			}
		})}, "a", "b")
		a := c.Node("a")
		sink, got := collector(t, c.Node("b"))
		ping := func(ctx context.Context, n int64) error { return sink.Send(ctx, a, &testpb.Ping{N: n}) }

		// A sender whose ctx is already done does not start a dial.
		done, cancel := context.WithCancel(t.Context())
		cancel()
		if err := ping(done, 0); !errors.Is(err, context.Canceled) || resolves.Load() != 0 {
			t.Fatalf("got %v after %d resolves", err, resolves.Load())
		}
		if err := a.Exit(done, sink, grpcproc.ReasonKilled); !errors.Is(err, context.Canceled) || resolves.Load() != 0 {
			t.Fatalf("exit: got %v after %d resolves", err, resolves.Load())
		}
		// One whose ctx ends stops waiting for the dial, and its message is not sent...
		short, cancelShort := context.WithTimeout(t.Context(), 20*time.Millisecond)
		defer cancelShort()
		if err := ping(short, 1); !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("got %v", err)
		}
		// ...while the same dial goes on for those still waiting.
		sent := make(chan error, 1)
		go func() { sent <- ping(t.Context(), 2) }()
		close(release)
		if err := <-sent; err != nil {
			t.Fatal(err)
		}
		if err := ping(t.Context(), 3); err != nil {
			t.Fatal(err)
		}
		for _, want := range []int64{2, 3} {
			if m := recv(t, got); m.Body.(*testpb.Ping).GetN() != want {
				t.Fatalf("got %+v, want %d", m, want)
			}
		}
		if n := resolves.Load(); n != 1 {
			t.Fatalf("%d dials, want one shared", n)
		}
	})
}

func TestInboundRejections(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
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
				c = metadata.AppendToOutgoingContext(c, md...)
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
	})
}

func TestAuthorize(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
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
	})
}

// A call's LinkError is Unsent when the message never left the node: the
// peer could not be reached. When the link broke under a call waiting for
// its reply, the peer may have handled it, and the error says nothing more.
func TestLinkErrorUnsent(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		c := grpcproctest.New(t, "a", "b")
		a := c.Node("a")
		called := make(chan struct{})
		if _, err := c.Node("b").Spawn(func(p *grpcproc.Process[*testpb.Ping]) error {
			if _, err := p.Receive(); err != nil {
				return err
			}
			close(called)
			_, err := p.Receive() // never answers
			return err
		}, grpcproc.WithName("silent")); err != nil {
			t.Fatal(err)
		}
		silent := grpcproc.Named[*testpb.Ping]("b", "silent")

		failed := make(chan error, 1)
		go func() {
			_, err := silent.Call[*testpb.Ping](t.Context(), a, &testpb.Ping{})
			failed <- err
		}()
		<-called
		c.Partition("a", "b")
		le, ok := errors.AsType[*grpcproc.LinkError](<-failed)
		if !ok || le.Unsent || !errors.Is(le, grpcproc.ErrNoConnection) {
			t.Fatalf("a call the link broke under: %v", le)
		}

		_, err := silent.Call[*testpb.Ping](t.Context(), a, &testpb.Ping{})
		le, ok = errors.AsType[*grpcproc.LinkError](err)
		if !ok || !le.Unsent || !errors.Is(err, grpcproc.ErrNoConnection) {
			t.Fatalf("a call that never left: %v", err)
		}
	})
}

func TestPartitionAndHeal(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		c := grpcproctest.New(t, "a", "b")
		a, b := c.Node("a"), c.Node("b")
		w, ch := watcher(t, a)
		e, _ := b.Spawn(echo, grpcproc.WithName("echo"))
		w.Monitor(e)
		if _, err := e.Call[*testpb.Pong](ctx(t), w, &testpb.Ping{N: 1}); err != nil {
			t.Fatal(err)
		}
		c.Partition("a", "b")
		if m := recv(t, ch); m.Down == nil || m.Down.Reason != grpcproc.ReasonNoConnection {
			t.Fatalf("got %+v", m.Down)
		}
		if _, err := a.CallTo[*testpb.Pong](ctx(t), e, &testpb.Ping{N: 1}); !errors.Is(err, grpcproc.ErrNoConnection) {
			t.Fatalf("partitioned call: %v", err)
		}
		c.Heal("a", "b")
		if r, err := a.CallTo[*testpb.Pong](ctx(t), e, &testpb.Ping{N: 1}); err != nil || r.N != 2 {
			t.Fatalf("after heal: %v %v", r, err)
		}
		// The process on b survived the partition; only the link was lost.
		if _, ok := b.Process(e.PID()); !ok {
			t.Fatal("process gone")
		}
		info := a.Info()
		if len(info.Links) == 0 || info.Links[0].Reconnects != 1 {
			t.Fatalf("links %+v", info.Links)
		}
	})
}

func TestKillAndRestart(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		h := &countingHooks{}
		c := grpcproctest.NewWith(t, []grpcproctest.Option{grpcproctest.WithHooks(h)}, "a", "b")
		a, b := c.Node("a"), c.Node("b")
		w, ch := watcher(t, a)

		silent, _ := b.Spawn[proto.Message](func(p *grpcproc.Process[proto.Message]) error {
			for {
				if _, err := p.Receive(); err != nil {
					return err
				}
			}
		}, grpcproc.WithName("silent"))
		w.Monitor(silent)
		callErr := make(chan error, 1)
		go func() {
			_, err := a.CallTo[*testpb.Pong](t.Context(), silent, &testpb.Ping{})
			callErr <- err
		}()
		time.Sleep(100 * time.Millisecond)

		c.Kill("b")

		select {
		case err := <-callErr:
			if !errors.Is(err, grpcproc.ErrNoConnection) {
				t.Fatalf("want ErrNoConnection, got %v", err)
			}
		case <-time.After(5 * time.Second):
			t.Fatal("pending call not failed on node down")
		}
		if m := recv(t, ch); m.Down == nil || m.Down.PID != silent.PID() || m.Down.Reason != grpcproc.ReasonNoConnection {
			t.Fatalf("got %+v", m.Down)
		}
		if err := a.SendTo(t.Context(), silent, &testpb.Ping{}); !errors.Is(err, grpcproc.ErrNoConnection) {
			t.Fatalf("send to dead node: %v", err)
		}

		b2 := c.Restart("b")
		if b2.ID().Incarnation == b.ID().Incarnation {
			t.Fatal("incarnation did not change")
		}
		e, _ := b2.Spawn(echo, grpcproc.WithName("echo"))
		if r, err := a.CallTo[*testpb.Pong](ctx(t), grpcproc.Named[*testpb.Ping]("b", "echo"), &testpb.Ping{N: 5}); err != nil || r.N != 6 {
			t.Fatalf("after restart: %v %v", r, err)
		}
		// The old PID names a process of the old incarnation: noproc, never a
		// delivery to whatever now has that id.
		w.Monitor(silent)
		if m := recv(t, ch); m.Down == nil || m.Down.Reason != grpcproc.ReasonNoProc {
			t.Fatalf("stale pid: %+v", m.Down)
		}
		_ = e
		if h.linkDowns.Load() == 0 {
			t.Fatal("OnLinkDown not called")
		}
	})
}

func TestLinkStatsAfterKill(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
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
	})
}

// Dial reaches another service on a peer's server, as the node reaches the
// peer: through its resolver and dial options.
func TestDialReachesAPeersOtherServices(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		c := grpcproctest.NewWith(t, []grpcproctest.Option{grpcproctest.WithServices(func(_ *grpcproc.Node, s *grpc.Server) {
			grpc_health_v1.RegisterHealthServer(s, health.NewServer())
		})}, "a", "b")
		cc, err := c.Node("a").Dial(t.Context(), "b")
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = cc.Close() }()
		r, err := grpc_health_v1.NewHealthClient(cc).Check(t.Context(), &grpc_health_v1.HealthCheckRequest{})
		if err != nil || r.GetStatus() != grpc_health_v1.HealthCheckResponse_SERVING {
			t.Fatalf("%v %v", r, err)
		}
	})
}

func TestDialAnUnknownPeer(t *testing.T) {
	n, err := grpcproc.NewNode(grpcproc.Config{Name: "a", Resolver: grpcproc.StaticResolver{}})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := n.Dial(t.Context(), "nowhere"); err == nil {
		t.Fatal("a peer the resolver does not know: want an error")
	}
}
