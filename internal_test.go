package grpcproc

import (
	"context"
	"errors"
	"io"
	"math"
	"net"
	"runtime"
	"slices"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/test/bufconn"
	"google.golang.org/protobuf/proto"

	"github.com/floatdrop/grpcproc/internal/testpb"
	grpcprocv1 "github.com/floatdrop/grpcproc/proto/grpcproc/v1"
)

func TestQueue(t *testing.T) {
	q := newQueue[int](true)
	if _, ok := q.tryPop(); ok || q.oldestStamp() != 0 || q.takenAt.Load() != 0 {
		t.Fatal("empty")
	}
	before := time.Now().UnixNano()
	for i := 1; i <= 3; i++ {
		q.push(i)
	}
	// Before the consumer takes a batch, the oldest is when the first came.
	first := q.oldestStamp()
	if q.len() != 3 || first < before || first > time.Now().UnixNano() {
		t.Fatalf("len %d oldest %d", q.len(), first)
	}
	if v, _ := q.tryPop(); v != 1 || q.oldestStamp() != first || q.takenAt.Load() < first {
		t.Fatalf("pop %d oldest %d", v, q.oldestStamp())
	}
	q.push(4) // lands in the producers' buffer while the consumer holds 2, 3
	for want := 2; want <= 4; want++ {
		if v, ok := q.tryPop(); !ok || v != want {
			t.Fatalf("pop %d: %d %v", want, v, ok)
		}
	}
	if q.len() != 0 || q.oldestStamp() != 0 {
		t.Fatal("drained queue reports items")
	}
	// Buffers are reused: a queue that is kept up with stops allocating.
	if n := testing.AllocsPerRun(100, func() { q.push(1); q.tryPop() }); n != 0 {
		t.Fatalf("%v allocs per push and pop", n)
	}
	// What the consumer swapped in but did not pop, and what producers
	// queued since, are both returned once the queue closes.
	q.push(5)
	q.push(6)
	q.tryPop()
	q.push(7)
	rest := append(q.taken(), q.close()...)
	if len(rest) != 2 || rest[0] != 6 || rest[1] != 7 || q.len() != 0 {
		t.Fatalf("rest %v, len %d", rest, q.len())
	}
	if q.push(8) {
		t.Fatal("push after close")
	}
	// drain hands over batches.
	d := newQueue[int](false)
	d.push(1)
	d.push(2)
	if b := d.drain(); len(b) != 2 || d.len() != 0 {
		t.Fatalf("batch %v", b)
	}
	d.push(3)
	if b := d.drain(); len(b) != 1 || b[0] != 3 {
		t.Fatalf("batch %v", b)
	}
}

func TestProtoHelpers(t *testing.T) {
	if typeName(nil) != "" {
		t.Fatal("nil helpers")
	}
	if _, err := decodeBody(&grpcprocv1.Envelope{}); err == nil {
		t.Fatal("decode without a body")
	}
	// A body whose type is known but whose bytes are not that type.
	if _, err := decodeBody(&grpcprocv1.Envelope{BodyType: "grpcproc.test.v1.Ping", Body: []byte{0xff}}); err == nil {
		t.Fatal("decoded garbage")
	}
	if first(metadata.MD{}, "k") != "" {
		t.Fatal("first")
	}
	var s linkStats
	s.fail(errors.New("boom"))
	var li LinkInfo
	s.fill(&li)
	if li.State != LinkDown || li.LastError != "boom" {
		t.Fatalf("%+v", li)
	}
}

func newTestNode(t *testing.T, name string) *Node {
	t.Helper()
	n, err := NewNode(Config{Name: name, Resolver: StaticResolver{}, Incarnation: 1})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = n.Stop(context.Background()) })
	return n
}

func TestDispatchMalformed(t *testing.T) {
	n := newTestNode(t, "a")
	me := n.PID()
	unknown := func(kind grpcprocv1.Kind, ref uint64) *grpcprocv1.Envelope {
		return &grpcprocv1.Envelope{Kind: kind, FromIncarnation: 1, FromId: 1, ToIncarnation: 1, ToId: 1, Ref: ref,
			BodyType: "no.such.Type", Body: []byte{1}}
	}
	n.dispatch("b", unknown(grpcprocv1.Kind_KIND_SEND, 0))
	n.dispatch("b", unknown(grpcprocv1.Kind_KIND_CALL, 1))
	if n.deadLetters.Load() != 1 {
		t.Fatalf("dead letters %d", n.deadLetters.Load())
	}
	// A reply with an undecodable body fails the pending call with ErrType;
	// one nobody waits for is dropped.
	pc := &pendingCall{node: "b", ch: make(chan callResult, 1)}
	n.pending[7] = pc
	n.dispatch("b", unknown(grpcprocv1.Kind_KIND_REPLY, 7))
	if r := <-pc.ch; !errors.Is(r.err, ErrType) {
		t.Fatalf("%v", r.err)
	}
	n.dispatch("b", &grpcprocv1.Envelope{Kind: grpcprocv1.Kind_KIND_REPLY, ToIncarnation: 1, Ref: 8, Status: grpcprocv1.Status_STATUS_OK})
	// A reply to an earlier incarnation of this node leaves a call of this
	// one with the same ref alone.
	stale := &pendingCall{node: "b", ch: make(chan callResult, 1)}
	n.pending[8] = stale
	n.dispatch("b", &grpcprocv1.Envelope{Kind: grpcprocv1.Kind_KIND_REPLY, ToIncarnation: 0, Ref: 8, Status: grpcprocv1.Status_STATUS_OK})
	if len(stale.ch) != 0 || n.pending[8] != stale {
		t.Fatal("a reply to an earlier incarnation answered a call")
	}
	delete(n.pending, 8)
	// Down for a process that does not exist, and for a ref it never held.
	n.dispatch("b", &grpcprocv1.Envelope{Kind: grpcprocv1.Kind_KIND_DOWN, FromIncarnation: 1, FromId: 1, ToIncarnation: 1, ToId: 1, Ref: 1})
	p := &proc{n: n, pid: PID{Node: "a", Incarnation: 1, ID: 5}, mbox: newQueue[item](true)}
	n.procs[5] = p
	n.dispatch("b", &grpcprocv1.Envelope{Kind: grpcprocv1.Kind_KIND_DOWN, FromIncarnation: 1, FromId: 1, ToIncarnation: p.pid.Incarnation, ToId: p.pid.ID, Ref: 1})
	if p.mbox.len() != 0 {
		t.Fatal("unknown ref must not deliver")
	}
	// Delivery to a process whose mailbox is already closed is a dead letter,
	// and a call gets noproc.
	p.accept = func(proto.Message) bool { return true }
	p.mbox.close()
	n.deliver(me, p.pid, "", &testpb.Ping{}, nil, 0)
	n.pending[9] = pc
	n.deliver(me, p.pid, "", &testpb.Ping{}, nil, 9)
	if r := <-pc.ch; !errors.Is(r.err, ErrNoProc) {
		t.Fatalf("%v", r.err)
	}
	// A watcher cannot be added to an exited process.
	p.exited = true
	if p.addWatcher(Ref{}, me) {
		t.Fatal("addWatcher on exited")
	}
	delete(n.procs, 5)
}

func TestConnLostStaleAndSendClosed(t *testing.T) {
	n := newTestNode(t, "a")
	// Stale links are just closed.
	out := &outLink{node: n, peer: NodeID{Name: "b"}, cc: testConn(t), q: newQueue[*grpcprocv1.Envelope](false), done: make(chan struct{}), drained: make(chan struct{}), recvDone: make(chan struct{}), cancel: func() {}}
	in := &inLink{peer: NodeID{Name: "b"}, closed: make(chan struct{})}
	n.connLost("b", nil, in, nil, false)
	if err := out.send(nil); err != nil {
		t.Fatal(err)
	}
	out.close(io.EOF)
	if le, ok := errors.AsType[*LinkError](out.send(nil)); !ok || !le.Unsent || !errors.Is(le, ErrNoConnection) {
		t.Fatalf("send on closed: %v", le)
	}
	n.connLost("b", out, nil, io.EOF, false)
	n.connLost("b", out, in, io.EOF, true)
	if n.Disconnect("nobody") {
		t.Fatal("disconnect unknown must be false")
	}
	// getOut after stop.
	_ = n.Stop(context.Background())
	if _, err := n.getOut(t.Context(), "b"); !errors.Is(err, ErrNodeStopped) {
		t.Fatal(err)
	}
}

// fakeStream is a server stream whose Send fails or whose Recv ends at will.
type fakeStream struct {
	grpc.ServerStream
	ctx     context.Context
	sendErr error
	recvErr error
	recv    chan *grpcprocv1.Frame
}

func (f *fakeStream) Context() context.Context { return f.ctx }
func (f *fakeStream) Send(*grpcprocv1.Frame) error {
	return f.sendErr
}
func (f *fakeStream) Recv() (*grpcprocv1.Frame, error) {
	if env, ok := <-f.recv; ok {
		return env, nil
	}
	return nil, f.recvErr
}

func TestLinkHandlerBranches(t *testing.T) {
	n := newTestNode(t, "a")
	ctx := metadata.NewIncomingContext(t.Context(), metadata.Pairs(mdNode, "b", mdIncarnation, "2", mdVersion, "1"))
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
	close(fs.recv)
	// A node that has stopped refuses links.
	_ = n.Stop(context.Background())
	fs = &fakeStream{ctx: ctx, recv: make(chan *grpcprocv1.Frame)}
	if err := n.serveLink(fs); err == nil {
		t.Fatal("link after stop")
	}
}

// testConn is a client connection that is never dialed, for links built by
// hand.
func testConn(t *testing.T) *grpc.ClientConn {
	t.Helper()
	cc, err := grpc.NewClient("passthrough:///x", grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	return cc
}

// A stream whose Send fails takes the link down. The frame it was writing
// may have gone out: its messages are dead letters and its call fails with
// the peer, as possibly handled. The rest of the batch never went: its call
// fails as unsent, whether the link was still open or had closed meanwhile.
func TestOutboundWriteFailure(t *testing.T) {
	for _, closedFirst := range []bool{false, true} {
		n := newTestNode(t, "a")
		entered, release := make(chan struct{}), make(chan struct{})
		l := &outLink{
			node: n, peer: NodeID{Name: "b"}, cc: testConn(t), q: newQueue[*grpcprocv1.Envelope](false), done: make(chan struct{}),
			drained: make(chan struct{}), recvDone: make(chan struct{}), cancel: func() {},
		}
		l.stream = &fakeClientStream{sendErr: io.ErrClosedPipe, onSend: func() {
			close(entered)
			<-release
			if closedFirst {
				l.close(io.EOF)
			}
		}}
		n.mu.Lock()
		n.out["b"] = l
		written, unwritten := &pendingCall{node: "b", ch: make(chan callResult, 1)}, &pendingCall{node: "b", ch: make(chan callResult, 1)}
		n.pending[9], n.pending[10] = written, unwritten
		n.mu.Unlock()
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
		// shutdown on a link that is already gone returns at once.
		l.shutdown(t.Context())
	}
}

type fakeClientStream struct {
	grpc.ClientStream
	sendErr error
	onSend  func()
}

func (f *fakeClientStream) Send(*grpcprocv1.Frame) error {
	if f.onSend != nil {
		f.onSend()
	}
	return f.sendErr
}
func (f *fakeClientStream) Recv() (*grpcprocv1.Frame, error) { select {} }
func (f *fakeClientStream) CloseSend() error                 { return nil }

func TestShutdownArms(t *testing.T) {
	mk := func() *outLink {
		return &outLink{peer: NodeID{Name: "b"}, q: newQueue[*grpcprocv1.Envelope](false), done: make(chan struct{}),
			drained: make(chan struct{}), recvDone: make(chan struct{}), cancel: func() {}}
	}
	// Caller's ctx expires before anything drains.
	l := mk()
	l.q.notify <- struct{}{} // notify already pending: the non-blocking push takes the default arm
	expired, cancel := context.WithCancel(t.Context())
	cancel()
	l.once.Do(func() {}) // neutralise close: these arms are about shutdown's waits
	l.shutdown(expired)
	// Drained, then the link dies before the peer ends the stream.
	l = mk()
	l.once.Do(func() {})
	close(l.drained)
	close(l.done)
	l.shutdown(t.Context())
	// Drained, peer never answers, ctx expires.
	l = mk()
	l.once.Do(func() {})
	close(l.drained)
	l.shutdown(expired)
}

func TestInspectNowOnExited(t *testing.T) {
	ctx, cancel := context.WithCancelCause(t.Context())
	cancel(nil)
	p := &proc{ctx: ctx, sys: make(chan inspectReq), started: time.Now()}
	if _, err := p.inspectNow(t.Context()); !errors.Is(err, ErrNoProc) {
		t.Fatal(err)
	}
}

func TestOutboundLostWhileInboundAlive(t *testing.T) {
	n := newTestNode(t, "a")
	out := &outLink{peer: NodeID{Name: "b"}, q: newQueue[*grpcprocv1.Envelope](false), done: make(chan struct{}),
		drained: make(chan struct{}), recvDone: make(chan struct{}), cancel: func() {}}
	out.once.Do(func() {})
	in := &inLink{peer: NodeID{Name: "b"}, closed: make(chan struct{})}
	n.out["b"], n.in["b"] = out, in
	pc := &pendingCall{node: "b", ch: make(chan callResult, 1)}
	n.pending[1] = pc
	n.connLost("b", out, nil, io.EOF, false)
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
	n.connLost("b", nil, in, io.EOF, false)
	if r := <-pc.ch; !errors.Is(r.err, ErrNoConnection) {
		t.Fatal(r.err)
	}
}

func TestRecvGoroutineStopsWhenClosed(t *testing.T) {
	n := newTestNode(t, "a")
	ctx := metadata.NewIncomingContext(t.Context(), metadata.Pairs(mdNode, "b", mdIncarnation, "2", mdVersion, "1"))
	fs := &fakeStream{ctx: ctx, recvErr: io.EOF, recv: make(chan *grpcprocv1.Frame)}
	done := make(chan error, 1)
	go func() { done <- n.serveLink(fs) }()
	time.Sleep(20 * time.Millisecond)
	n.Disconnect("b")
	<-done
	// The handler is gone; an envelope arriving now finds nobody to hand it to.
	fs.recv <- &grpcprocv1.Frame{}
	close(fs.recv)
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
}

func TestDialBadTarget(t *testing.T) {
	n, err := NewNode(Config{Name: "a", Incarnation: 1,
		Resolver:    StaticResolver{"b": "\x7f://not a target"},
		DialOptions: []grpc.DialOption{grpc.WithTransportCredentials(insecure.NewCredentials())}})
	if err != nil {
		t.Fatal(err)
	}
	if err := n.SendTo(t.Context(), Named[*testpb.Ping]("b", "x"), &testpb.Ping{}); !errors.Is(err, ErrNoConnection) {
		t.Fatalf("got %v", err)
	}
}

func TestSubscribersEdges(t *testing.T) {
	var s subscribers
	sub := &subscriber{ch: make(chan Event, 1)}
	s.remove(sub) // nothing registered yet
	s.add(sub)
	sub.close()
	sub.close()        // idempotent
	s.publish(Event{}) // delivering to a closed subscriber is a no-op
	if len(sub.ch) != 0 {
		t.Fatal("delivered to a closed subscriber")
	}
}

func TestFrameSplitting(t *testing.T) {
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
}

// A frame arriving after the link closed is not dispatched.
func TestNoDispatchAfterClose(t *testing.T) {
	n := newTestNode(t, "a")
	l := &inLink{peer: NodeID{Name: "b"}, closed: make(chan struct{})}
	l.close()
	if l.deliver(n, &grpcprocv1.Frame{Envelopes: []*grpcprocv1.Envelope{{}}}) {
		t.Fatal("dispatched after close")
	}
}

// The wait doubles from a 32nd of DialBackoff to DialBackoff, jittered by up
// to a fifth; it does not overflow near the largest Duration; and an entry
// whose wait ended more than DialBackoff ago is forgotten.
func TestFailedDialBackoff(t *testing.T) {
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
			n.failedDial("b", boom, "boom")
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
}

// What a closing link never wrote: its messages are dead letters, and its
// calls fail at once as unsent. The queue depth counts what waits.
func TestLostEnvelopes(t *testing.T) {
	n := newTestNode(t, "a")
	pc := &pendingCall{node: "b", ch: make(chan callResult, 1)}
	n.mu.Lock()
	n.pending[7] = pc
	n.mu.Unlock()
	events := n.Subscribe(t.Context(), 8)
	l := &outLink{node: n, peer: NodeID{Name: "b"}, cc: testConn(t), q: newQueue[*grpcprocv1.Envelope](false), done: make(chan struct{}),
		drained: make(chan struct{}), recvDone: make(chan struct{}), cancel: func() {}}
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
}

// putBack returns items to the front of an open queue, and refuses a closed
// one.
func TestQueuePutBack(t *testing.T) {
	q := newQueue[int](false)
	q.push(3)
	if !q.putBack([]int{1, 2}) || q.len() != 3 {
		t.Fatal("put back refused")
	}
	if got := q.drain(); !slices.Equal(got, []int{1, 2, 3}) {
		t.Fatalf("got %v", got)
	}
	q.close()
	if q.putBack([]int{4}) {
		t.Fatal("put back into a closed queue")
	}
}

// A subscription lets go of its subscriber and buffer as soon as it ends,
// whichever ends it: its ctx, before the node stops, or the node, while a
// long-lived ctx goes on.
func TestSubscribeLetsGoWhenItEnds(t *testing.T) {
	for _, nodeFirst := range []bool{false, true} {
		n := newTestNode(t, "a")
		ctx, cancel := context.WithCancel(context.Background())
		events := n.Subscribe(ctx, 16)
		released := make(chan struct{})
		last := func() *subscriber { subs := *n.subs.list.Load(); return subs[len(subs)-1] }
		runtime.AddCleanup(last(), func(struct{}) { close(released) }, struct{}{})
		if nodeFirst {
			_ = n.Stop(context.Background())
		} else {
			cancel()
		}
		for range events {
		}
		for deadline := time.Now().Add(5 * time.Second); ; {
			runtime.GC()
			select {
			case <-released:
			case <-time.After(10 * time.Millisecond):
				if time.Now().After(deadline) {
					t.Fatalf("node first %v: the subscriber is still referenced", nodeFirst)
				}
				continue
			}
			break
		}
		cancel()
	}
}

// Demonitor sends one DEMONITOR, addressed as the monitor was: by PID or by
// name. It used to send a second, addressed by an empty name.
func TestDemonitorSendsOneEnvelope(t *testing.T) {
	n := newTestNode(t, "a")
	l := &outLink{node: n, peer: NodeID{Name: "b"}, cc: testConn(t), q: newQueue[*grpcprocv1.Envelope](false), done: make(chan struct{}),
		drained: make(chan struct{}), recvDone: make(chan struct{}), cancel: func() {}}
	n.mu.Lock()
	n.out["b"] = l // no writer: what is sent stays queued
	n.mu.Unlock()
	t.Cleanup(func() { // before Stop, which would wait for it to drain
		n.mu.Lock()
		delete(n.out, "b")
		n.mu.Unlock()
		l.close(nil)
	})
	targets := []Target{PID{Node: "b", Incarnation: 1, ID: 5}, Name{Node: "b", Name: "x"}}
	done := make(chan struct{})
	if _, err := n.Spawn(func(p *Process[*grpcprocv1.Hello]) error {
		for _, target := range targets {
			p.Demonitor(p.Monitor(target))
		}
		close(done)
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	<-done
	sent := l.q.drain()
	if len(sent) != 4 {
		t.Fatalf("sent %d envelopes: %v", len(sent), sent)
	}
	for i, target := range targets {
		m, d := sent[2*i], sent[2*i+1]
		if m.GetKind() != grpcprocv1.Kind_KIND_MONITOR || d.GetKind() != grpcprocv1.Kind_KIND_DEMONITOR ||
			d.GetToIncarnation() != m.GetToIncarnation() || d.GetToId() != m.GetToId() ||
			d.GetToName() != m.GetToName() || d.GetRef() != m.GetRef() {
			t.Errorf("%v: monitor %v, demonitor %v", target, m, d)
		}
	}
}
