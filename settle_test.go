package grpcproc

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/proto"

	grpcprocv1 "github.com/floatdrop/grpcproc/proto/grpcproc/v1"
)

// holdHooks holds the first dead letter until released. Closing an outbound
// link with an envelope queued dead-letters it inside linksLost: after the
// links were taken, before the peer is declared down.
type holdHooks struct {
	NopHooks
	once             sync.Once
	entered, release chan struct{}
}

func (h *holdHooks) OnDeadLetter(PID, PID, proto.Message, string) {
	h.once.Do(func() { close(h.entered); <-h.release })
}

type settleEnv struct {
	n *Node
	h *holdHooks
	p *proc
}

// newSettleEnv is node a with process 5, and a hook that holds its first
// dead letter.
func newSettleEnv(t *testing.T) settleEnv {
	t.Helper()
	h := &holdHooks{entered: make(chan struct{}), release: make(chan struct{})}
	n, err := NewNode(Config{Name: "a", Resolver: StaticResolver{}, Incarnation: 1, Hooks: h})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		select {
		case <-h.release:
		default:
			close(h.release)
		}
	})
	p := &proc{n: n, pid: PID{Node: "a", Incarnation: 1, ID: 5}, mbox: newQueue[item](true)}
	n.mu.Lock()
	n.procs[5] = p
	n.mu.Unlock()
	return settleEnv{n, h, p}
}

// links gives node a an outbound link to b's incarnation inc, with one
// message queued, whose dead letter the hook holds, and an inbound one.
func (e settleEnv) links(t *testing.T, inc uint64) *inLink {
	t.Helper()
	out := &outLink{node: e.n, peer: NodeID{Name: "b", Incarnation: inc}, cc: testConn(t),
		q: newQueue[*grpcprocv1.Envelope](false), done: make(chan struct{}), cancel: func() {}}
	env := wire(grpcprocv1.Kind_KIND_SEND, e.p.pid, PID{Node: "b", Incarnation: inc, ID: 9}, "")
	if err := encodeBody(env, &grpcprocv1.Hello{}); err != nil {
		t.Fatal(err)
	}
	out.q.push(env)
	in := &inLink{peer: NodeID{Name: "b", Incarnation: inc}, done: make(chan struct{})}
	e.n.mu.Lock()
	e.n.out["b"], e.n.in["b"] = out, in
	e.n.mu.Unlock()
	return in
}

// stream is a new stream from b's incarnation inc, served by node a.
func (e settleEnv) stream(t *testing.T, inc string) (*fakeStream, chan error) {
	return serveStream(t, e.n, inc)
}

// serveStream serves a new stream from b's incarnation inc on n. A panic is
// recovered, as a recovery interceptor would.
func serveStream(t *testing.T, n *Node, inc string) (*fakeStream, chan error) {
	ctx := metadata.NewIncomingContext(t.Context(), metadata.Pairs(mdNode, "b", mdIncarnation, inc, mdVersion, "1"))
	fs := &fakeStream{ctx: ctx, recvErr: io.EOF, recv: make(chan *grpcprocv1.Frame)}
	served := make(chan error, 1)
	go func() {
		defer func() {
			if r := recover(); r != nil {
				served <- fmt.Errorf("panic: %v", r)
			}
		}()
		served <- n.serveLink(fs)
	}()
	return fs, served
}

// monitor has b's process 7 monitor process 5 over fs; the channel is closed
// once that was dispatched.
func (e settleEnv) monitor(fs *fakeStream, inc uint64) (Ref, chan struct{}) {
	mon := wire(grpcprocv1.Kind_KIND_MONITOR, PID{Node: "b", Incarnation: inc, ID: 7}, e.p.pid, "")
	mon.Ref = 42
	dispatched := make(chan struct{})
	go func() {
		fs.recv <- &grpcprocv1.Frame{Envelopes: []*grpcprocv1.Envelope{mon}}
		fs.recv <- &grpcprocv1.Frame{} // taken: the monitor was dispatched
		close(dispatched)
	}()
	return Ref{Node: "b", ID: 42}, dispatched
}

func (e settleEnv) watched(r Ref) bool {
	e.p.mu.Lock()
	defer e.p.mu.Unlock()
	_, ok := e.p.watchers[r]
	return ok
}

// A new session of b that begins while the old one is being torn down waits
// for b to be declared down: otherwise that would drop the monitors set over
// it, and b's Downs would come after its new messages.
func TestNewSessionWaitsForTheOldOnesDown(t *testing.T) {
	for _, tc := range []struct {
		name string
		end  func(e settleEnv, old *inLink)
	}{
		{"Disconnect", func(e settleEnv, _ *inLink) { e.n.Disconnect("b") }},
		{"inbound link ends", func(e settleEnv, old *inLink) { e.n.inLost(old, io.EOF) }},
		{"b restarted", func(e settleEnv, _ *inLink) {
			e.n.memberEvent(MemberEvent{Member: Member{Name: "b", Incarnation: 3}, Up: true})
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			e := newSettleEnv(t)
			old := e.links(t, 2)
			ended := make(chan struct{})
			go func() { tc.end(e, old); close(ended) }()
			<-e.h.entered
			fs, _ := e.stream(t, "3")
			r, dispatched := e.monitor(fs, 3)
			select {
			case <-dispatched:
				t.Error("the new session was dispatched while the old one was being torn down")
			case <-time.After(50 * time.Millisecond):
			}
			close(e.h.release)
			<-ended
			<-dispatched
			if !e.watched(r) {
				t.Error("declaring the old session down dropped a monitor set over the new one")
			}
		})
	}
}

// Two new streams at once: one replaces the old link, the other replaces
// that one, each in turn, and the link events say so in order.
func TestNewStreamsReplaceInTurn(t *testing.T) {
	e := newSettleEnv(t)
	events := e.n.Subscribe(t.Context(), 32)
	e.links(t, 2)
	_, served1 := e.stream(t, "3")
	<-e.h.entered
	_, served2 := e.stream(t, "3")
	close(e.h.release)
	var got []string
	for len(got) < 4 {
		select {
		case ev := <-events:
			if ev.Kind == EventLinkUp || ev.Kind == EventLinkDown {
				got = append(got, ev.Kind.String()+" "+ev.Peer.String())
			}
		case <-time.After(5 * time.Second):
			t.Fatalf("link events %q", got)
		}
	}
	want := []string{"link-down b#2", "link-up b#3", "link-down b#3", "link-up b#3"}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("link events %q, want %q", got, want)
		}
	}
	select { // the one replaced returns; the other is up
	case <-served1:
	case <-served2:
	case <-time.After(5 * time.Second):
		t.Fatal("neither stream was replaced")
	}
	e.n.mu.Lock()
	up := e.n.in["b"]
	e.n.mu.Unlock()
	if up == nil {
		t.Fatal("no link from b")
	}
}

// A frame the old link is still dispatching, whose answer cannot be routed
// back (dials to b back off), cuts that link, not the new one.
func TestOldDispatchDoesNotCutTheNewLink(t *testing.T) {
	e := newSettleEnv(t)
	e.n.mu.Lock()
	e.n.backoff["b"] = &redial{at: time.Now().Add(time.Hour), wait: time.Second, err: errors.New("boom"), why: "boom"}
	e.n.dialing["b"] = &dialOp{done: make(chan struct{})}
	e.n.mu.Unlock()
	fs1, served1 := e.stream(t, "2")
	// A call to a process that does not exist: dispatch dead-letters it (the
	// hook holds it, under the old link's mu), then answers noproc.
	call := wire(grpcprocv1.Kind_KIND_CALL, PID{Node: "b", Incarnation: 2, ID: 7}, PID{Node: "a", Incarnation: 1, ID: 99}, "")
	call.Ref = 1
	if err := encodeBody(call, &grpcprocv1.Hello{}); err != nil {
		t.Fatal(err)
	}
	fs1.recv <- &grpcprocv1.Frame{Envelopes: []*grpcprocv1.Envelope{call}}
	<-e.h.entered
	e.n.mu.Lock()
	first := e.n.in["b"]
	e.n.mu.Unlock()
	_, served2 := e.stream(t, "2")
	for { // the new stream has taken the old link
		e.n.mu.Lock()
		cur := e.n.in["b"]
		e.n.mu.Unlock()
		if cur != first {
			break
		}
		time.Sleep(time.Millisecond)
	}
	close(e.h.release)
	<-served1 // the old link closed: its dispatch, and any cut, are done
	var second *inLink
	for deadline := time.Now().Add(5 * time.Second); second == nil || second == first; time.Sleep(time.Millisecond) {
		if time.Now().After(deadline) {
			t.Fatal("the new link never went in")
		}
		e.n.mu.Lock()
		second = e.n.in["b"]
		e.n.mu.Unlock()
	}
	select {
	case <-second.done:
		t.Fatalf("the new link was cut by the old one's answer: %v", <-served2)
	default:
	}
}

// A new stream from b ends the old session first: process 5's Down for b's
// process is queued before anything the new stream carries.
func TestReplacedLinkDownsFirst(t *testing.T) {
	e := newSettleEnv(t)
	close(e.h.release)
	r := PID{Node: "b", Incarnation: 2, ID: 7}
	e.p.accept = func(proto.Message) bool { return true }
	e.p.mu.Lock()
	e.p.monitors = map[Ref]monitorTarget{{Node: "a", ID: 1}: {pid: r}}
	e.p.mu.Unlock()
	e.n.mu.Lock()
	e.n.in["b"] = &inLink{peer: NodeID{Name: "b", Incarnation: 2}, done: make(chan struct{})}
	e.n.mu.Unlock()
	fs, _ := e.stream(t, "2")
	send := wire(grpcprocv1.Kind_KIND_SEND, r, e.p.pid, "")
	if err := encodeBody(send, &grpcprocv1.Hello{}); err != nil {
		t.Fatal(err)
	}
	fs.recv <- &grpcprocv1.Frame{Envelopes: []*grpcprocv1.Envelope{send}}
	fs.recv <- &grpcprocv1.Frame{}
	if first, ok := e.p.mbox.tryPop(); !ok || first.down == nil {
		t.Fatalf("first item %+v", first)
	}
	if second, ok := e.p.mbox.tryPop(); !ok || second.body == nil {
		t.Fatalf("second item %+v", second)
	}
}

// A dial that fails with answers queued cuts the links they answer, not a
// link that has replaced them since: b restarted, and its new session is
// owed nothing.
func TestFailedDialCutsOnlyTheLinksItAnswers(t *testing.T) {
	entered, release := make(chan struct{}), make(chan struct{})
	n, err := NewNode(Config{Name: "a", Incarnation: 1, Resolver: ResolverFunc(func(context.Context, string) (string, error) {
		close(entered)
		<-release
		return "", errors.New("unreachable")
	})})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = n.Stop(context.Background()) })
	fs1, served1 := serveStream(t, n, "2")
	call := wire(grpcprocv1.Kind_KIND_CALL, PID{Node: "b", Incarnation: 2, ID: 7}, PID{Node: "a", Incarnation: 1, ID: 99}, "")
	call.Ref = 1
	if err := encodeBody(call, &grpcprocv1.Hello{}); err != nil {
		t.Fatal(err)
	}
	fs1.recv <- &grpcprocv1.Frame{Envelopes: []*grpcprocv1.Envelope{call}}
	fs1.recv <- &grpcprocv1.Frame{} // dispatched: its noproc waits on a dial
	<-entered
	n.mu.Lock()
	old, d := n.in["b"], n.dialing["b"]
	n.mu.Unlock()
	serveStream(t, n, "3") // b restarted
	<-served1
	var cur *inLink
	for cur == nil || cur == old {
		time.Sleep(time.Millisecond)
		n.mu.Lock()
		cur = n.in["b"]
		n.mu.Unlock()
	}
	close(release)
	<-d.done
	select {
	case <-cur.done:
		t.Fatal("the failed dial cut b's new link")
	default:
	}
}

type panicUp struct{ NopHooks }

func (panicUp) OnLinkUp(NodeID) { panic("hook") }

// A panic in OnLinkUp, recovered by an interceptor, leaves no change to b's
// links under way: later streams from b are not held up for ever.
func TestLinkUpPanicSettles(t *testing.T) {
	n, err := NewNode(Config{Name: "a", Incarnation: 1, Resolver: StaticResolver{}, Hooks: panicUp{}})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = n.Stop(context.Background()) })
	_, served := serveStream(t, n, "2")
	if err := <-served; err == nil {
		t.Fatal("the hook did not panic")
	}
	n.mu.Lock()
	defer n.mu.Unlock()
	if s := n.settling["b"]; s != 0 {
		t.Fatalf("b's links still settling: %d", s)
	}
}
