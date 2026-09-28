package grpcproc

import (
	"context"
	"errors"
	"net"
	"strconv"
	"testing"
	"testing/synctest"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/test/bufconn"
	"google.golang.org/protobuf/proto"

	"github.com/floatdrop/grpcproc/internal/testpb"
	grpcprocv1 "github.com/floatdrop/grpcproc/proto/grpcproc/v1"
)

// A peer is another program, and may send anything: whatever frame it
// sends, a node must not panic or stall, and must go on serving its
// processes. The fuzz targets below check that at the two doors a peer's
// bytes come in by: dispatch, which acts on each envelope, and serveLink,
// which reads the handshake and the stream around them. Both run in a
// synctest bubble, so the node's timers cost no time and a goroutine it
// leaves behind fails the run.

// fuzzEcho answers a call on a Ping with a Pong, and a Ping sent to it
// with nothing.
func fuzzEcho(p *Process[*testpb.Ping]) error {
	for {
		m, err := p.Receive()
		if err != nil {
			return err
		}
		if m.IsCall() {
			_ = m.Reply(&testpb.Pong{N: m.Body.GetN() + 1}, nil)
		}
	}
}

// fuzzSink takes anything, and traps exits, so that an Exited is a message.
func fuzzSink(p *Process[proto.Message]) error {
	p.SetTrapExit(true)
	for {
		if _, err := p.Receive(); err != nil {
			return err
		}
	}
}

// fuzzNode is node a of incarnation 1, with what a peer's envelopes can
// reach by the ids the fuzzer is likely to try: an echo (1, "echo"), a
// sink that takes anything (2, "sink"), and calls to peer b waiting on
// refs 1 to 3.
func fuzzNode(t *testing.T) (*Node, []*pendingCall) {
	t.Helper()
	n, err := NewNode(Config{Name: "a", Resolver: StaticResolver{}, Incarnation: 1})
	if err != nil {
		t.Fatal(err)
	}
	// Stopped however the run ends, so that a failed check leaves no
	// process behind for the bubble to report instead.
	t.Cleanup(func() { _ = n.Stop(context.Background()) })
	if _, err := n.Spawn(fuzzEcho, WithName("echo")); err != nil {
		t.Fatal(err)
	}
	if _, err := n.Spawn(fuzzSink, WithName("sink")); err != nil {
		t.Fatal(err)
	}
	pending := make([]*pendingCall, 3)
	n.pendingMu.Lock()
	for i := range pending {
		pending[i] = &pendingCall{node: "b", ch: make(chan callResult, 1)}
		n.pending[uint64(i+1)] = pending[i]
	}
	n.pendingMu.Unlock()
	return n, pending
}

// healthy checks that n still serves: a process spawned now answers a call,
// and Stop ends every process in time.
func healthy(t *testing.T, n *Node) {
	t.Helper()
	e, err := n.Spawn(fuzzEcho)
	if err != nil {
		t.Fatalf("spawn after the frame: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	if r, err := n.Call[*testpb.Pong](ctx, e, &testpb.Ping{N: 1}); err != nil || r.GetN() != 2 {
		t.Fatalf("call after the frame: %v %v", r, err)
	}
	if err := n.Stop(ctx); err != nil {
		t.Fatalf("stop after the frame: %v", err)
	}
}

func body(m proto.Message) (string, []byte) {
	b, err := proto.Marshal(m)
	if err != nil {
		panic(err)
	}
	return string(proto.MessageName(m)), b
}

// fuzzSeeds are frames a peer could send, one per kind of envelope, aimed
// at what fuzzNode has, and a few that are wrong on purpose.
func fuzzSeeds() []*grpcprocv1.Frame {
	pingType, ping := body(&testpb.Ping{N: 1})
	pongType, pong := body(&testpb.Pong{N: 2})
	env := func(kind grpcprocv1.Kind, toID uint64, name string, ref uint64) *grpcprocv1.Envelope {
		return &grpcprocv1.Envelope{Kind: kind, FromIncarnation: 1, FromId: 7, ToIncarnation: 1, ToId: toID, ToName: name, Ref: ref}
	}
	with := func(e *grpcprocv1.Envelope, typ string, b []byte) *grpcprocv1.Envelope {
		e.BodyType, e.Body = typ, b
		return e
	}
	frame := func(envs ...*grpcprocv1.Envelope) *grpcprocv1.Frame { return &grpcprocv1.Frame{Envelopes: envs} }
	return []*grpcprocv1.Frame{
		frame(with(env(grpcprocv1.Kind_KIND_SEND, 0, "echo", 0), pingType, ping)),
		frame(with(env(grpcprocv1.Kind_KIND_SEND, 1, "", 0), pongType, pong)),
		frame(with(env(grpcprocv1.Kind_KIND_SEND, 2, "", 0), pongType, pong)),
		frame(with(env(grpcprocv1.Kind_KIND_CALL, 0, "echo", 11), pingType, ping)),
		frame(with(&grpcprocv1.Envelope{Kind: grpcprocv1.Kind_KIND_CALL, FromIncarnation: 1, FromId: 7, ToIncarnation: 1, ToId: 1, Ref: 12, TimeoutNanos: int64(time.Second)}, pingType, ping)),
		frame(with(env(grpcprocv1.Kind_KIND_CALL, 9, "", 13), "no.such.Type", []byte{1})),
		frame(with(env(grpcprocv1.Kind_KIND_REPLY, 0, "", 1), pongType, pong)),
		frame(&grpcprocv1.Envelope{Kind: grpcprocv1.Kind_KIND_REPLY, ToIncarnation: 1, Ref: 2, Status: grpcprocv1.Status_STATUS_ERROR, Reason: "boom"}),
		frame(&grpcprocv1.Envelope{Kind: grpcprocv1.Kind_KIND_REPLY, ToIncarnation: 1, Ref: 3, Status: grpcprocv1.Status_STATUS_TYPE}),
		frame(env(grpcprocv1.Kind_KIND_MONITOR, 1, "", 21), env(grpcprocv1.Kind_KIND_DEMONITOR, 1, "", 21)),
		frame(env(grpcprocv1.Kind_KIND_MONITOR, 0, "sink", 22), env(grpcprocv1.Kind_KIND_EXIT, 0, "sink", 0)),
		frame(env(grpcprocv1.Kind_KIND_MONITOR, 99, "", 23)),
		frame(&grpcprocv1.Envelope{Kind: grpcprocv1.Kind_KIND_DOWN, FromIncarnation: 1, FromId: 7, ToIncarnation: 1, ToId: 2, Ref: 1, Reason: "normal"}),
		frame(&grpcprocv1.Envelope{Kind: grpcprocv1.Kind_KIND_EXIT, ToIncarnation: 1, ToId: 1, Reason: "fuzz"}),
		frame(&grpcprocv1.Envelope{Kind: grpcprocv1.Kind_KIND_HELLO, Hello: &grpcprocv1.Hello{Node: "b", Incarnation: 1, Version: protoVersion}}),
		frame(&grpcprocv1.Envelope{Kind: 99}, &grpcprocv1.Envelope{}),
	}
}

func FuzzDispatch(f *testing.F) {
	for _, fr := range fuzzSeeds() {
		b, err := proto.Marshal(fr)
		if err != nil {
			f.Fatal(err)
		}
		f.Add(b)
	}
	f.Fuzz(func(t *testing.T, data []byte) {
		var fr grpcprocv1.Frame
		if proto.Unmarshal(data, &fr) != nil {
			return // not a frame: the stream's decoder refuses it before dispatch
		}
		synctest.Test(t, func(t *testing.T) {
			n, pending := fuzzNode(t)
			for _, env := range fr.GetEnvelopes() {
				n.dispatch("b", env)
			}
			synctest.Wait()
			// A pending call is answered once at most, and with a result a
			// caller can use: a reply's body, or an error.
			for i, pc := range pending {
				select {
				case r := <-pc.ch:
					if r.err == nil && r.body == nil && !answeredEmpty(&fr, uint64(i+1)) {
						t.Fatalf("call %d answered with neither a body nor an error", i+1)
					}
					if len(pc.ch) != 0 {
						t.Fatalf("call %d answered twice", i+1)
					}
				default:
				}
			}
			healthy(t, n)
		})
	})
}

// answeredEmpty reports whether fr holds an OK reply with no body for ref:
// a call may be answered with nothing, and its caller then gets nil.
func answeredEmpty(fr *grpcprocv1.Frame, ref uint64) bool {
	for _, env := range fr.GetEnvelopes() {
		if env.GetKind() == grpcprocv1.Kind_KIND_REPLY && env.GetRef() == ref && env.GetBodyType() == "" {
			return true
		}
	}
	return false
}

func FuzzServeLink(f *testing.F) {
	for _, fr := range fuzzSeeds() {
		b, err := proto.Marshal(fr)
		if err != nil {
			f.Fatal(err)
		}
		f.Add("b", "1", strconv.Itoa(protoVersion), b, false)
	}
	f.Add("", "", "", []byte{}, true)
	f.Add("a", "1", strconv.Itoa(protoVersion), []byte{}, false)
	f.Add("b", "x", "2", []byte{0xff}, true)
	f.Fuzz(func(t *testing.T, name, incarnation, version string, data []byte, cancelStream bool) {
		var fr grpcprocv1.Frame
		if proto.Unmarshal(data, &fr) != nil {
			return
		}
		synctest.Test(t, func(t *testing.T) {
			n, _ := fuzzNode(t)
			ln := bufconn.Listen(1 << 20)
			srv := grpc.NewServer()
			n.Register(srv)
			go func() { _ = srv.Serve(ln) }()
			defer srv.Stop()
			cc, err := grpc.NewClient("passthrough:///a",
				grpc.WithTransportCredentials(insecure.NewCredentials()),
				grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) { return ln.DialContext(ctx) }))
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = cc.Close() }()

			ctx, cancel := context.WithCancel(metadata.AppendToOutgoingContext(context.Background(),
				mdNode, name, mdIncarnation, incarnation, mdVersion, version))
			defer cancel()
			stream, err := grpcprocv1.NewNodeClient(cc).Link(ctx)
			if err != nil {
				// gRPC would not send it: header values of non-printable
				// bytes, say. It never reaches a node that runs gRPC either.
				return
			}
			_ = stream.Send(&fr)
			if cancelStream {
				cancel()
			} else {
				_ = stream.CloseSend()
			}
			// Read until the node ends the stream: it refused the peer, or
			// saw the end of what the peer sent.
			for {
				if _, err := stream.Recv(); err != nil {
					if errors.Is(err, context.Canceled) && !cancelStream {
						t.Fatalf("stream ended with %v", err)
					}
					break
				}
			}
			synctest.Wait()
			healthy(t, n)
		})
	})
}
