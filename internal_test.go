package golink

import (
	"context"
	"errors"
	"io"
	"net"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/test/bufconn"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"

	"github.com/floatdrop/golink/internal/testpb"
	golinkv1 "github.com/floatdrop/golink/proto/golink/v1"
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
	if !pidFrom(nil).IsZero() || typeName(nil) != "" {
		t.Fatal("nil helpers")
	}
	if _, err := decode(nil); err == nil {
		t.Fatal("decode nil")
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
	bogus := &anypb.Any{TypeUrl: "type.googleapis.com/no.such.Type", Value: []byte{1}}
	to := pidTo(PID{Node: "a", Incarnation: 1, ID: 1})
	from := pidTo(PID{Node: "b", Incarnation: 1, ID: 1})
	n.dispatch(&golinkv1.Envelope{Kind: &golinkv1.Envelope_Send{Send: &golinkv1.Send{From: from, To: to, Body: bogus}}})
	n.dispatch(&golinkv1.Envelope{Kind: &golinkv1.Envelope_Call{Call: &golinkv1.Call{From: from, To: to, Ref: 1, Body: bogus}}})
	if n.deadLetters.Load() != 1 {
		t.Fatalf("dead letters %d", n.deadLetters.Load())
	}
	// A reply with an undecodable body fails the pending call with ErrType;
	// one nobody waits for is dropped.
	pc := &pendingCall{node: "b", ch: make(chan callResult, 1)}
	n.pending[7] = pc
	n.dispatch(&golinkv1.Envelope{Kind: &golinkv1.Envelope_Reply{Reply: &golinkv1.Reply{To: pidTo(me), Ref: 7, Status: golinkv1.Status_STATUS_OK, Body: bogus}}})
	if r := <-pc.ch; !errors.Is(r.err, ErrType) {
		t.Fatalf("%v", r.err)
	}
	n.dispatch(&golinkv1.Envelope{Kind: &golinkv1.Envelope_Reply{Reply: &golinkv1.Reply{To: pidTo(me), Ref: 8, Status: golinkv1.Status_STATUS_OK}}})
	// Down for a process that does not exist, and for a ref it never held.
	n.dispatch(&golinkv1.Envelope{Kind: &golinkv1.Envelope_Down{Down: &golinkv1.Down{From: from, To: to, Ref: 1}}})
	p := &proc{n: n, pid: PID{Node: "a", Incarnation: 1, ID: 5}, mbox: newQueue[item](true)}
	n.procs[5] = p
	n.dispatch(&golinkv1.Envelope{Kind: &golinkv1.Envelope_Down{Down: &golinkv1.Down{From: from, To: pidTo(p.pid), Ref: 1}}})
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
	out := &outLink{peer: NodeID{Name: "b"}, q: newQueue[*golinkv1.Envelope](false), done: make(chan struct{}), drained: make(chan struct{}), recvDone: make(chan struct{}), cancel: func() {}}
	out.cc = nil
	in := &inLink{peer: NodeID{Name: "b"}, closed: make(chan struct{})}
	n.connLost("b", nil, in, nil, false)
	if err := out.send(nil); err != nil {
		t.Fatal(err)
	}
	out.closeNoConn(io.EOF)
	if err := out.send(nil); !errors.Is(err, ErrNoConnection) {
		t.Fatalf("send on closed: %v", err)
	}
	n.connLost("b", out, nil, io.EOF, false)
	n.connLost("b", out, in, io.EOF, true)
	if n.Disconnect("nobody") {
		t.Fatal("disconnect unknown must be false")
	}
	// getOut after stop.
	_ = n.Stop(context.Background())
	if _, err := n.getOut("b"); !errors.Is(err, ErrNodeStopped) {
		t.Fatal(err)
	}
}

// closeNoConn is close without a ClientConn to close (tests build outLinks by hand).
func (l *outLink) closeNoConn(err error) {
	l.once.Do(func() {
		l.fail(err)
		close(l.done)
		l.q.close()
		l.cancel()
	})
}

// fakeStream is a server stream whose Send fails or whose Recv ends at will.
type fakeStream struct {
	grpc.ServerStream
	ctx     context.Context
	sendErr error
	recvErr error
	recv    chan *golinkv1.Envelope
}

func (f *fakeStream) Context() context.Context { return f.ctx }
func (f *fakeStream) Send(*golinkv1.Envelope) error {
	return f.sendErr
}
func (f *fakeStream) Recv() (*golinkv1.Envelope, error) {
	if env, ok := <-f.recv; ok {
		return env, nil
	}
	return nil, f.recvErr
}

func TestLinkHandlerBranches(t *testing.T) {
	n := newTestNode(t, "a")
	ctx := metadata.NewIncomingContext(t.Context(), metadata.Pairs(mdNode, "b", mdIncarnation, "2", mdVersion, "1"))
	// Hello cannot be sent.
	fs := &fakeStream{ctx: ctx, sendErr: io.ErrClosedPipe, recv: make(chan *golinkv1.Envelope)}
	if err := n.Link(fs); !errors.Is(err, io.ErrClosedPipe) {
		t.Fatalf("%v", err)
	}
	// Closed from this side while the peer is still sending.
	fs = &fakeStream{ctx: ctx, recvErr: io.EOF, recv: make(chan *golinkv1.Envelope)}
	done := make(chan error, 1)
	go func() { done <- n.Link(fs) }()
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
	fs = &fakeStream{ctx: ctx, recv: make(chan *golinkv1.Envelope)}
	if err := n.Link(fs); err == nil {
		t.Fatal("link after stop")
	}
}

func TestOutboundWriteFailure(t *testing.T) {
	// A stream whose Send fails takes the link down.
	n := newTestNode(t, "a")
	cc, err := grpc.NewClient("passthrough:///x", grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	l := &outLink{
		peer: NodeID{Name: "b"}, cc: cc, q: newQueue[*golinkv1.Envelope](false), done: make(chan struct{}),
		drained: make(chan struct{}), recvDone: make(chan struct{}), cancel: func() {},
		stream: &fakeClientStream{sendErr: io.ErrClosedPipe},
	}
	n.out["b"] = l
	go l.writeLoop(n)
	_ = l.send(&golinkv1.Envelope{})
	select {
	case <-l.done:
	case <-time.After(time.Second):
		t.Fatal("link not closed")
	}
	if _, ok := n.out["b"]; ok {
		t.Fatal("link still registered")
	}
	// shutdown on a link that is already gone returns at once.
	l.shutdown(t.Context())
}

type fakeClientStream struct {
	grpc.ClientStream
	sendErr error
}

func (f *fakeClientStream) Send(*golinkv1.Envelope) error     { return f.sendErr }
func (f *fakeClientStream) Recv() (*golinkv1.Envelope, error) { select {} }
func (f *fakeClientStream) CloseSend() error                  { return nil }

func TestShutdownArms(t *testing.T) {
	mk := func() *outLink {
		return &outLink{peer: NodeID{Name: "b"}, q: newQueue[*golinkv1.Envelope](false), done: make(chan struct{}),
			drained: make(chan struct{}), recvDone: make(chan struct{}), cancel: func() {}}
	}
	// Caller's ctx expires before anything drains.
	l := mk()
	l.q.notify <- struct{}{} // notify already pending: the non-blocking push takes the default arm
	expired, cancel := context.WithCancel(t.Context())
	cancel()
	l.once.Do(func() {}) // neutralise close (no ClientConn to close)
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
	out := &outLink{peer: NodeID{Name: "b"}, q: newQueue[*golinkv1.Envelope](false), done: make(chan struct{}),
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
	fs := &fakeStream{ctx: ctx, recvErr: io.EOF, recv: make(chan *golinkv1.Envelope)}
	done := make(chan error, 1)
	go func() { done <- n.Link(fs) }()
	time.Sleep(20 * time.Millisecond)
	n.Disconnect("b")
	<-done
	// The handler is gone; an envelope arriving now finds nobody to hand it to.
	fs.recv <- &golinkv1.Envelope{}
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
	go func() { errs <- n.SendTo(Named[*testpb.Ping]("b", "x"), &testpb.Ping{}) }()
	<-entered
	go func() { errs <- n.SendTo(Named[*testpb.Ping]("b", "x"), &testpb.Ping{}) }()
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
	go func() { errs <- n.SendTo(Named[*testpb.Ping]("b", "x"), &testpb.Ping{}) }()
	<-entered
	_ = n.Stop(context.Background())
	close(release)
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
	if err := n.SendTo(Named[*testpb.Ping]("b", "x"), &testpb.Ping{}); !errors.Is(err, ErrNoConnection) {
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
