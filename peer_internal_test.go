package grpcproc

import (
	"context"
	"errors"
	"io"
	"math"
	"net"
	"strconv"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/test/bufconn"

	"github.com/floatdrop/grpcproc/internal/testpb"
	grpcprocv1 "github.com/floatdrop/grpcproc/proto/grpcproc/v1"
)

func TestLostStaleAndSendClosed(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		n := newTestNode(t, "a")
		// Links no longer registered are just closed.
		out := testOutLink(t, n, NodeID{Name: "b"})
		in := &inLink{peer: NodeID{Name: "b"}, done: make(chan struct{})}
		pc := &pendingCall{node: "b", ch: make(chan callResult, 1)}
		n.pendingMu.Lock()
		n.pending[5] = pc // b is not declared down for them
		n.pendingMu.Unlock()
		n.inLost(in, nil)
		if err := out.send(nil); err != nil {
			t.Fatal(err)
		}
		out.close(io.EOF)
		if le, ok := errors.AsType[*LinkError](out.send(nil)); !ok || !le.Unsent || !errors.Is(le, ErrNoConnection) {
			t.Fatalf("send on closed: %v", le)
		}
		n.outLost(out, io.EOF)
		if len(pc.ch) != 0 {
			t.Fatal("losing links that were no longer registered failed a call to their peer")
		}
		if n.Disconnect("nobody") {
			t.Fatal("disconnect unknown must be false")
		}
		// getOut after stop.
		_ = n.Stop(context.Background())
		if _, err := n.getOut(t.Context(), "b"); !errors.Is(err, ErrNodeStopped) {
			t.Fatal(err)
		}
	})
}

func TestLinkHandlerBranches(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		n := newTestNode(t, "a")
		ctx := metadata.NewIncomingContext(t.Context(), metadata.Pairs(mdNode, "b", mdIncarnation, "2", mdVersion, strconv.Itoa(protoVersion)))
		// Hello cannot be sent.
		fs := &fakeStream{ctx: ctx, sendErr: io.ErrClosedPipe, recv: make(chan *grpcprocv1.Frame)}
		if err := n.serveLink(fs); !errors.Is(err, io.ErrClosedPipe) {
			t.Fatalf("%v", err)
		}
		// Closed from this side while the peer is still sending.
		fs = &fakeStream{ctx: ctx, recvErr: io.EOF, recv: make(chan *grpcprocv1.Frame)}
		done := make(chan error, 1)
		go func() { done <- n.serveLink(fs) }()
		time.Sleep(20 * time.Millisecond)
		if !n.Disconnect("b") {
			t.Fatal("no inbound link registered")
		}
		if err := <-done; err != nil {
			t.Fatal(err)
		}
		fs.end()
		// A node that has stopped refuses links.
		_ = n.Stop(context.Background())
		fs = &fakeStream{ctx: ctx, recv: make(chan *grpcprocv1.Frame)}
		if err := n.serveLink(fs); err == nil {
			t.Fatal("link after stop")
		}
	})
}

func TestRecvGoroutineStopsWhenClosed(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		n := newTestNode(t, "a")
		ctx := metadata.NewIncomingContext(t.Context(), metadata.Pairs(mdNode, "b", mdIncarnation, "2", mdVersion, strconv.Itoa(protoVersion)))
		fs := &fakeStream{ctx: ctx, recvErr: io.EOF, recv: make(chan *grpcprocv1.Frame)}
		done := make(chan error, 1)
		go func() { done <- n.serveLink(fs) }()
		time.Sleep(20 * time.Millisecond)
		n.Disconnect("b")
		<-done
		// The handler is gone; an envelope arriving now finds nobody to hand it to.
		fs.recv <- &grpcprocv1.Frame{}
		fs.end()
	})
}

// A frame arriving after the link closed is not dispatched.
func TestNoDispatchAfterClose(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		n := newTestNode(t, "a")
		l := &inLink{peer: NodeID{Name: "b"}, done: make(chan struct{})}
		l.close()
		if l.deliver(n, &grpcprocv1.Frame{Envelopes: []*grpcprocv1.Envelope{{}}}) {
			t.Fatal("dispatched after close")
		}
	})
}

// A stream whose Send fails takes the link down. The frame it was writing
// may have gone out: its messages are dead letters and its call fails with
// the peer, as possibly handled. The rest of the batch never went: its call
// fails as unsent, whether the link was still open or had closed meanwhile.
func TestOutboundWriteFailure(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		for _, closedFirst := range []bool{false, true} {
			n := newTestNode(t, "a")
			entered, release := make(chan struct{}), make(chan struct{})
			l := testOutLink(t, n, NodeID{Name: "b"})
			l.stream = &fakeClientStream{sendErr: io.ErrClosedPipe, onSend: func() {
				close(entered)
				<-release
				if closedFirst {
					l.close(io.EOF)
				}
			}}
			n.mu.Lock()
			n.out["b"] = l
			n.mu.Unlock()
			written, unwritten := &pendingCall{node: "b", ch: make(chan callResult, 1)}, &pendingCall{node: "b", ch: make(chan callResult, 1)}
			n.pendingMu.Lock()
			n.pending[9], n.pending[10] = written, unwritten
			n.pendingMu.Unlock()
			// The first call fills a frame on its own, so the second waits for
			// the next.
			_ = l.send(&grpcprocv1.Envelope{Kind: grpcprocv1.Kind_KIND_CALL, Ref: 9, Body: make([]byte, 2*maxFrame)})
			_ = l.send(&grpcprocv1.Envelope{Kind: grpcprocv1.Kind_KIND_CALL, Ref: 10})
			go l.writeLoop()
			<-entered
			if q := l.info().Queued; q != 2 {
				t.Fatalf("queued while writing: %d", q)
			}
			close(release)
			<-l.done
			le, ok := errors.AsType[*LinkError]((<-unwritten.ch).err)
			if !ok || !le.Unsent {
				t.Fatalf("closed first %v: the call never written: %v", closedFirst, le)
			}
			if n.deadLetters.Load() != 2 {
				t.Fatalf("dead letters %d", n.deadLetters.Load())
			}
			if !closedFirst {
				le, ok := errors.AsType[*LinkError]((<-written.ch).err)
				if !ok || le.Unsent {
					t.Fatalf("the call being written: %v", le)
				}
				n.mu.Lock()
				_, still := n.out["b"]
				n.mu.Unlock()
				if still {
					t.Fatal("link still registered")
				}
			}
			// finish on a link that is already gone returns at once.
			l.shutdown()
			l.finish(t.Context())
		}
	})
}

func TestFinishArms(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		mk := func() *outLink {
			return &outLink{peer: NodeID{Name: "b"}, q: newQueue[*grpcprocv1.Envelope](false), done: make(chan struct{}),
				cancel: func() {}}
		}
		// Caller's ctx expires before the peer ends the stream.
		l := mk()
		l.q.notify <- struct{}{} // notify already pending: the non-blocking push takes the default arm
		expired, cancel := context.WithCancel(t.Context())
		cancel()
		l.once.Do(func() {}) // neutralise close: these arms are about finish's waits
		l.shutdown()
		if !l.finish(expired) {
			t.Fatal("an expired ctx did not cut the flush")
		}
		// The link closes first: the peer ended the stream, or it broke.
		l = mk()
		l.once.Do(func() {})
		close(l.done)
		l.shutdown()
		if l.finish(t.Context()) {
			t.Fatal("cut a flush that ended")
		}
	})
}

func TestOutboundLostWhileInboundAlive(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		n := newTestNode(t, "a")
		out := &outLink{peer: NodeID{Name: "b"}, q: newQueue[*grpcprocv1.Envelope](false), done: make(chan struct{}),
			cancel: func() {}}
		out.once.Do(func() {})
		in := &inLink{peer: NodeID{Name: "b"}, done: make(chan struct{})}
		n.out["b"], n.in["b"] = out, in
		pc := &pendingCall{node: "b", ch: make(chan callResult, 1)}
		n.pending[1] = pc
		n.outLost(out, io.EOF)
		if _, ok := n.in["b"]; !ok {
			t.Fatal("inbound link must survive an outbound failure")
		}
		if _, ok := n.out["b"]; ok {
			t.Fatal("outbound link must be dropped")
		}
		select {
		case <-pc.ch:
			t.Fatal("pending call failed while the peer may still answer")
		default:
		}
		n.inLost(in, io.EOF)
		if r := <-pc.ch; !errors.Is(r.err, ErrNoConnection) {
			t.Fatal(r.err)
		}
	})
}

// What a closing link never wrote: its messages are dead letters, and its
// calls fail at once as unsent. The queue depth counts what waits.
func TestLostEnvelopes(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		n := newTestNode(t, "a")
		pc := &pendingCall{node: "b", ch: make(chan callResult, 1)}
		n.pendingMu.Lock()
		n.pending[7] = pc
		n.pendingMu.Unlock()
		events := n.Subscribe(t.Context(), 8)
		l := testOutLink(t, n, NodeID{Name: "b"})
		send := wire(grpcprocv1.Kind_KIND_SEND, PID{Node: "a", Incarnation: 1, ID: 3}, PID{Node: "b", Incarnation: 2, ID: 4}, "")
		if err := encodeBody(send, &grpcprocv1.Hello{Node: "x"}); err != nil {
			t.Fatal(err)
		}
		call := wire(grpcprocv1.Kind_KIND_CALL, PID{Node: "a", Incarnation: 1, ID: 3}, PID{Node: "b"}, "stock")
		call.Ref = 7
		for _, env := range []*grpcprocv1.Envelope{send, call, wire(grpcprocv1.Kind_KIND_MONITOR, PID{}, PID{}, "")} {
			_ = l.send(env)
		}
		if q := l.info().Queued; q != 3 {
			t.Fatalf("queued %d", q)
		}

		l.close(nil)
		if le, ok := errors.AsType[*LinkError]((<-pc.ch).err); !ok || !le.Unsent || !errors.Is(le, ErrNoConnection) {
			t.Fatalf("call: %v", le)
		}
		if got := n.deadLetters.Load(); got != 2 {
			t.Fatalf("dead letters %d", got)
		}
		e := <-events
		if e.Kind != EventDeadLetter || e.Reason != ReasonNoConnection || e.From != (PID{Node: "a", Incarnation: 1, ID: 3}) ||
			e.To != (PID{Node: "b", Incarnation: 2, ID: 4}) || e.Type != "grpcproc.v1.Hello" {
			t.Fatalf("%+v", e)
		}
		// A call already ended is left alone.
		n.failCall(7, io.EOF)
	})
}

// A full link refuses messages and calls to its peer at once, as unsent;
// what the peer waits for, and what cannot be refused, is queued regardless,
// and counted. Once the writer has written, there is room again.
func TestLinkBound(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		n, err := NewNode(Config{Name: "a", Resolver: StaticResolver{}, Incarnation: 1, MaxQueued: 2})
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = n.Stop(context.Background()) })
		l := queuedLink(t, n, NodeID{Name: "b", Incarnation: 2})
		type ping = grpcprocv1.Hello
		from, to := PID{Node: "a", Incarnation: 1, ID: 1}, PID{Node: "b", Incarnation: 2, ID: 1}
		peer := Addr[*ping]{pid: to}
		for range 2 {
			if err := peer.Send(t.Context(), n, &ping{}); err != nil {
				t.Fatal(err)
			}
		}
		busy := func(what string, err error) {
			t.Helper()
			le, ok := errors.AsType[*LinkError](err)
			if !ok || !le.Unsent || le.Peer != "b" || !errors.Is(err, ErrLinkBusy) || !errors.Is(err, ErrNoConnection) {
				t.Fatalf("%s to a full link: %v", what, err)
			}
		}
		busy("a send", peer.Send(t.Context(), n, &ping{}))
		_, err = peer.Call[*ping](t.Context(), n, &ping{})
		busy("a call", err)

		for _, err := range []error{
			n.reply(from, to, 1, answerTo{}, &ping{}, grpcprocv1.Status_STATUS_OK, "", false),
			n.down(from, to, 2, ReasonNormal, false),
			n.monitor(from, to, 3),
			n.demonitor(from, to, 3),
			n.exit(t.Context(), from, to, ReasonKilled),
		} {
			if err != nil {
				t.Fatal(err)
			}
		}
		if q := l.info().Queued; q != 7 {
			t.Fatalf("queued %d", q)
		}

		l.stream = &fakeClientStream{}
		go l.writeLoop()
		synctest.Wait()
		if q := l.info().Queued; q != 0 {
			t.Fatalf("queued %d once written", q)
		}
		if err := peer.Send(t.Context(), n, &ping{}); err != nil {
			t.Fatal(err)
		}
	})
}

func TestFrameSplitting(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		env := func(body, md int) *grpcprocv1.Envelope {
			e := &grpcprocv1.Envelope{Kind: grpcprocv1.Kind_KIND_SEND, Body: make([]byte, body)}
			if md > 0 {
				e.Metadata = map[string]string{"k": string(make([]byte, md))}
			}
			return e
		}
		// Small envelopes all fit; their bodies are counted.
		if n, body := frameOf([]*grpcprocv1.Envelope{env(10, 0), env(20, 5), {Kind: grpcprocv1.Kind_KIND_MONITOR}}); n != 3 || body != 30 {
			t.Fatalf("%d %d", n, body)
		}
		// A frame stops before it would pass maxFrame, metadata included.
		half := maxFrame/2 - 100
		if n, _ := frameOf([]*grpcprocv1.Envelope{env(half, 0), env(half, 0), env(10, 0)}); n != 2 {
			t.Fatalf("split at %d", n)
		}
		if n, _ := frameOf([]*grpcprocv1.Envelope{env(half, 0), env(10, half+200)}); n != 1 {
			t.Fatalf("metadata ignored: %d", n)
		}
		// An envelope larger than a frame still goes, alone.
		if n, body := frameOf([]*grpcprocv1.Envelope{env(2*maxFrame, 0), env(1, 0)}); n != 1 || body != 2*maxFrame {
			t.Fatalf("%d %d", n, body)
		}
		if bodySize(&grpcprocv1.Envelope{Kind: grpcprocv1.Kind_KIND_REPLY, Body: []byte("ab")}) != 2 {
			t.Fatal("reply body not counted")
		}
	})
}

// bufconnPeer starts a node behind an in-memory listener and returns the
// dial option that reaches it.
func bufconnPeer(t *testing.T, name string) (*Node, grpc.DialOption) {
	t.Helper()
	ln := bufconn.Listen(1 << 20)
	srv := grpc.NewServer()
	n, err := NewNode(Config{Name: name, Resolver: StaticResolver{}, Incarnation: 1})
	if err != nil {
		t.Fatal(err)
	}
	n.Register(srv)
	go func() { _ = srv.Serve(ln) }()
	t.Cleanup(func() { _ = n.Stop(context.Background()); srv.Stop() })
	return n, grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) { return ln.DialContext(ctx) })
}

func TestDialSharingAndStopWhileDialing(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		_, dial := bufconnPeer(t, "b")
		entered := make(chan struct{}, 2)
		release := make(chan struct{})
		n, err := NewNode(Config{
			Name: "a", Incarnation: 1,
			Resolver: ResolverFunc(func(context.Context, string) (string, error) {
				entered <- struct{}{}
				<-release
				return "passthrough:///b", nil
			}),
			DialOptions: []grpc.DialOption{grpc.WithTransportCredentials(insecure.NewCredentials()), dial},
		})
		if err != nil {
			t.Fatal(err)
		}
		// Two concurrent sends share one dial.
		errs := make(chan error, 2)
		go func() { errs <- n.SendTo(t.Context(), Named[*testpb.Ping]("b", "x"), &testpb.Ping{}) }()
		<-entered
		go func() { errs <- n.SendTo(t.Context(), Named[*testpb.Ping]("b", "x"), &testpb.Ping{}) }()
		time.Sleep(20 * time.Millisecond)
		close(release)
		for range 2 {
			if err := <-errs; err != nil {
				t.Fatal(err)
			}
		}
		if len(n.dials) != 1 || n.dials["b"] != 1 {
			t.Fatalf("dials %v", n.dials)
		}
		// Stop while a dial is in flight: the link is discarded.
		n.Disconnect("b")
		release = make(chan struct{})
		go func() { errs <- n.SendTo(t.Context(), Named[*testpb.Ping]("b", "x"), &testpb.Ping{}) }()
		<-entered
		// Stop waits for the dial; let it complete only once the node is stopped.
		go func() {
			for {
				n.mu.Lock()
				stopped := n.stopped
				n.mu.Unlock()
				if stopped {
					close(release)
					return
				}
				time.Sleep(time.Millisecond)
			}
		}()
		if err := n.Stop(context.Background()); err != nil {
			t.Fatal(err)
		}
		if err := <-errs; !errors.Is(err, ErrNodeStopped) {
			t.Fatalf("got %v", err)
		}
	})
}

func TestDialBadTarget(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		n, err := NewNode(Config{Name: "a", Incarnation: 1,
			Resolver:    StaticResolver{"b": "\x7f://not a target"},
			DialOptions: []grpc.DialOption{grpc.WithTransportCredentials(insecure.NewCredentials())}})
		if err != nil {
			t.Fatal(err)
		}
		if err := n.SendTo(t.Context(), Named[*testpb.Ping]("b", "x"), &testpb.Ping{}); !errors.Is(err, ErrNoConnection) {
			t.Fatalf("got %v", err)
		}
	})
}

// The wait doubles from a 32nd of DialBackoff to DialBackoff, jittered by up
// to a fifth; it does not overflow near the largest Duration; and an entry
// whose wait ended more than DialBackoff ago is forgotten.
func TestFailedDialBackoff(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		n := newTestNode(t, "a")
		boom := errors.New("boom")
		// locked runs fn with n.mu held, as the node does.
		locked := func(fn func()) {
			n.mu.Lock()
			defer n.mu.Unlock()
			fn()
		}
		fail := func() (r *redial) {
			locked(func() {
				n.failedDial("b", boom, "boom", 0)
				r = n.backoff["b"]
			})
			return r
		}

		locked(func() { n.cfg.DialBackoff = 320 * time.Millisecond })
		for i, want := range []time.Duration{10, 20, 40, 80, 160, 320, 320} {
			want *= time.Millisecond
			before := time.Now()
			r := fail()
			if r.wait != want || r.at.Before(before.Add(want*4/5)) || r.at.After(time.Now().Add(want)) {
				t.Fatalf("failure %d: wait %v, at in %v; want %v", i, r.wait, time.Until(r.at), want)
			}
		}

		locked(func() {
			n.cfg.DialBackoff = math.MaxInt64
			delete(n.backoff, "b")
		})
		var r *redial
		for range 40 {
			r = fail()
		}
		if r.wait != math.MaxInt64 || !r.at.After(time.Now()) {
			t.Fatalf("wait %v, at %v", r.wait, r.at)
		}

		locked(func() {
			n.cfg.DialBackoff = time.Second
			n.backoff["c"] = &redial{at: time.Now().Add(-2 * time.Second), err: boom, why: "boom"}
		})
		for _, l := range n.Info().Links {
			if l.Peer.Name == "c" {
				t.Fatalf("stale backoff listed: %+v", l)
			}
		}
		var kept bool
		locked(func() { _, kept = n.backoff["c"] })
		if kept {
			t.Fatal("stale backoff kept")
		}
	})
}

// errHook is a ctx whose Err runs f first: getOut asks it between its shared
// look for a link and its exclusive one.
type errHook struct {
	context.Context
	f func()
}

func (c errHook) Err() error { c.f(); return nil }

// getOut looks again under the exclusive lock: a link may have come up, or
// the node stopped, since its shared look. A dial then would be a second link.
func TestGetOutLooksAgainBeforeDialing(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		n := newTestNode(t, "a")
		unlink := func() { n.mu.Lock(); delete(n.out, "b"); n.mu.Unlock() }
		t.Cleanup(unlink) // before the node's Stop, which would flush the stand-in link
		l := &outLink{peer: NodeID{Name: "b"}}
		up := errHook{t.Context(), func() { n.mu.Lock(); n.out["b"] = l; n.mu.Unlock() }}
		if got, err := n.getOut(up, "b"); got != l || err != nil {
			t.Fatalf("got %p, %v", got, err)
		}
		unlink()
		stopped := errHook{t.Context(), func() { n.mu.Lock(); n.stopped = true; n.mu.Unlock() }}
		if _, err := n.getOut(stopped, "b"); !errors.Is(err, ErrNodeStopped) {
			t.Fatalf("got %v", err)
		}
	})
}

// Stop starts every outbound link's shutdown before it waits for any: a peer
// that never ends its stream holds its own link to the end of Stop's ctx, not
// the others' flush. Here no peer ever ends its stream, and every link is
// still told to half-close at once.
func TestStopFlushesEveryLinkAtOnce(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		n := newTestNode(t, "a")
		start := time.Now()
		var mu sync.Mutex
		closedSend := map[string]time.Duration{}
		var links []*outLink
		for _, peer := range []string{"b", "c", "d"} {
			l := testOutLink(t, n, NodeID{Name: peer})
			l.stream = &fakeClientStream{onCloseSend: func() {
				mu.Lock()
				closedSend[peer] = time.Since(start)
				mu.Unlock()
			}}
			n.mu.Lock()
			n.out[peer] = l
			n.mu.Unlock()
			links = append(links, l)
			go l.writeLoop()
		}
		ctx, cancel := context.WithTimeout(t.Context(), 300*time.Millisecond)
		defer cancel()
		// No stream ends, so the flush runs to ctx, and Stop says so.
		if err := n.Stop(ctx); !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("stop: %v", err)
		}
		mu.Lock()
		defer mu.Unlock()
		for _, peer := range []string{"b", "c", "d"} {
			if at, ok := closedSend[peer]; !ok || at > 150*time.Millisecond {
				t.Errorf("%s: half-closed at %v (%v)", peer, at, ok)
			}
		}
		for _, l := range links {
			select {
			case <-l.done:
			default:
				t.Errorf("%s: not closed", l.peer.Name)
			}
		}
	})
}

type recordingRegistrar struct{ withdrawCtxErr chan error }

func (r recordingRegistrar) Register(context.Context, Member) (func(context.Context) error, error) {
	return func(ctx context.Context) error { r.withdrawCtxErr <- ctx.Err(); return nil }, nil
}

// Stop's withdraw runs with time of its own when the flush used Stop's ctx
// up: a peer that never ends its stream must not leave the node published.
func TestStopWithdrawsAfterALongFlush(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		r := recordingRegistrar{withdrawCtxErr: make(chan error, 1)}
		n, err := NewNode(Config{Name: "a", Resolver: StaticResolver{}, Registrar: r})
		if err != nil {
			t.Fatal(err)
		}
		if err := n.Start(t.Context()); err != nil {
			t.Fatal(err)
		}
		l := testOutLink(t, n, NodeID{Name: "b"})
		l.stream = &fakeClientStream{}
		n.mu.Lock()
		n.out["b"] = l // its peer never ends the stream
		n.mu.Unlock()
		go l.writeLoop()
		ctx, cancel := context.WithTimeout(t.Context(), 50*time.Millisecond)
		defer cancel()
		_ = n.Stop(ctx)
		if err := <-r.withdrawCtxErr; err != nil {
			t.Fatalf("withdraw ran with a ctx already done: %v", err)
		}
	})
}
