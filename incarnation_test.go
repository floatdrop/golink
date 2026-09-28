package grpcproc_test

import (
	"context"
	"errors"
	"fmt"
	"net"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"
	"google.golang.org/protobuf/proto"

	"github.com/floatdrop/grpcproc"
	"github.com/floatdrop/grpcproc/internal/testpb"
)

// twins is an in-memory network on which two nodes may share a name: an
// instance that was replaced and still runs, and the one that replaced it.
// A name resolves to whichever instance route last pointed it at.
type twins struct {
	t      *testing.T
	mu     sync.Mutex
	lns    map[string]*bufconn.Listener // by address
	routes map[string]string            // name -> address
}

func newTwins(t *testing.T) *twins {
	return &twins{t: t, lns: map[string]*bufconn.Listener{}, routes: map[string]string{}}
}

// start runs incarnation inc of node name, and routes the name to it.
func (w *twins) start(name string, inc uint64, configure ...func(*grpcproc.Config)) *grpcproc.Node {
	w.t.Helper()
	addr := fmt.Sprintf("%s-%d", name, inc) // not name#inc: # starts a URL's fragment
	ln := bufconn.Listen(1 << 20)
	cfg := grpcproc.Config{
		Name: name, Incarnation: inc,
		Resolver: grpcproc.ResolverFunc(func(_ context.Context, peer string) (string, error) {
			w.mu.Lock()
			defer w.mu.Unlock()
			return "passthrough:///" + w.routes[peer], nil
		}),
		DialOptions: []grpc.DialOption{
			grpc.WithTransportCredentials(insecure.NewCredentials()),
			grpc.WithContextDialer(func(ctx context.Context, addr string) (net.Conn, error) {
				w.mu.Lock()
				ln := w.lns[addr]
				w.mu.Unlock()
				if ln == nil {
					return nil, errors.New("no such address: " + addr)
				}
				return ln.DialContext(ctx)
			}),
		},
		DialTimeout: 2 * time.Second,
		DialBackoff: -1,
	}
	for _, fn := range configure {
		fn(&cfg)
	}
	n, err := grpcproc.NewNode(cfg)
	if err != nil {
		w.t.Fatal(err)
	}
	srv := grpc.NewServer()
	n.Register(srv)
	go func() { _ = srv.Serve(ln) }()
	if err := n.Start(context.Background()); err != nil {
		w.t.Fatal(err)
	}
	w.t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = n.Stop(ctx)
		srv.Stop()
	})
	w.mu.Lock()
	w.lns[addr] = ln
	w.mu.Unlock()
	w.route(n)
	return n
}

// route points n's name at n.
func (w *twins) route(n *grpcproc.Node) {
	w.mu.Lock()
	w.routes[n.Name()] = fmt.Sprintf("%s-%d", n.Name(), n.ID().Incarnation)
	w.mu.Unlock()
}

// linksOf lists n's links as peer#incarnation, outbound ones marked with >.
func linksOf(n *grpcproc.Node) []string {
	var out []string
	for _, l := range n.Info().Links {
		s := l.Peer.String()
		if l.Outbound {
			s = ">" + s
		}
		out = append(out, s)
	}
	return out
}

func sameLinks(t *testing.T, n *grpcproc.Node, want ...string) {
	t.Helper()
	got := linksOf(n)
	if strings.Join(got, " ") != strings.Join(want, " ") {
		t.Fatalf("%s's links: %v, want %v", n.Name(), got, want)
	}
}

func isStale(err error, old, seen string) bool {
	return errors.Is(err, grpcproc.ErrNoConnection) && strings.Contains(err.Error(), old+" is an old incarnation: a has seen "+seen)
}

// nextLinkEvent returns the next link event on ch.
func nextLinkEvent(t *testing.T, ch <-chan grpcproc.Event) grpcproc.Event {
	t.Helper()
	deadline := time.After(5 * time.Second)
	for {
		select {
		case ev := <-ch:
			if ev.Kind == grpcproc.EventLinkUp || ev.Kind == grpcproc.EventLinkDown {
				return ev
			}
		case <-deadline:
			t.Fatal("no link event")
		}
	}
}

func noMore[M proto.Message](t *testing.T, ch <-chan grpcproc.Msg[M]) {
	t.Helper()
	select {
	case m := <-ch:
		t.Fatalf("unexpected %+v", m)
	case <-time.After(50 * time.Millisecond):
	}
}

// b was replaced by incarnation 2 while incarnation 1 still runs. The old
// one's links are refused: it cannot cut a off from the current b, and its
// messages do not arrive.
func TestOldIncarnationIsRefused(t *testing.T) {
	w := newTwins(t)
	a := w.start("a", 1)
	old := w.start("b", 1)
	b := w.start("b", 2)
	svc := mustEcho(t, b)
	watch, downs := watcher(t, a)
	watch.Monitor(svc)
	sink, got := collector(t, a)
	// a and the current b are linked both ways.
	if err := b.SendTo(t.Context(), sink, &testpb.Ping{N: 2}); err != nil {
		t.Fatal(err)
	}
	recv(t, got)
	if _, err := a.Call[*testpb.Pong](ctx(t), svc, &testpb.Ping{N: 1}); err != nil {
		t.Fatal(err)
	}
	sameLinks(t, a, ">b#2", "b#2")

	events := a.Subscribe(t.Context(), 16)
	err := old.SendTo(t.Context(), sink, &testpb.Ping{N: 1})
	if !isStale(err, "b#1", "b#2") {
		t.Fatalf("the old b's send: %v", err)
	}
	var le *grpcproc.LinkError
	if !errors.As(err, &le) || !le.Unsent {
		t.Fatalf("not reported unsent: %v", err)
	}
	if _, err := a.Call[*testpb.Pong](ctx(t), svc, &testpb.Ping{N: 1}); err != nil {
		t.Fatalf("a calls the current b: %v", err)
	}
	noMore(t, got)
	noMore(t, downs)
	select {
	case e := <-events:
		t.Fatalf("a's links changed: %+v", e)
	default:
	}
	sameLinks(t, a, ">b#2", "b#2")
}

// A dial that reaches an old incarnation, through an address that still
// points at it, fails, and the links with the current one stay.
func TestDialReachingAnOldIncarnationFails(t *testing.T) {
	w := newTwins(t)
	a := w.start("a", 1)
	old := w.start("b", 1)
	b := w.start("b", 2)
	sink, got := collector(t, a)
	if err := b.SendTo(t.Context(), sink, &testpb.Ping{}); err != nil {
		t.Fatal(err)
	}
	recv(t, got)
	w.route(old)
	err := a.SendTo(t.Context(), grpcproc.Named[*testpb.Ping]("b", "x"), &testpb.Ping{})
	if !isStale(err, "b#1", "b#2") {
		t.Fatalf("got %v", err)
	}
	sameLinks(t, a, "b#2")
	// The refused dial made no link: the first one a makes is not a
	// reconnect.
	w.route(b)
	if err := a.SendTo(t.Context(), grpcproc.Named[*testpb.Ping]("b", "x"), &testpb.Ping{}); err != nil {
		t.Fatal(err)
	}
	for _, l := range a.Info().Links {
		if l.Outbound && l.Reconnects != 0 {
			t.Fatalf("%+v", l)
		}
	}
}

// flush returns once a has handled the events sent to m before it: the
// second of two events is taken only once the one before them is handled.
func flush(m *fakeMembership) {
	for range 2 {
		m.events <- grpcproc.MemberEvent{Member: grpcproc.Member{Name: "zzz"}}
	}
}

// The fence lifts once the newest incarnation is gone: when Membership
// reports it left, or on Disconnect. An older one is then the only one
// running, and links.
func TestFenceLiftsWhenTheNewestIsGone(t *testing.T) {
	for _, tc := range []struct {
		name string
		lift func(a *grpcproc.Node, m *fakeMembership)
	}{
		{"Membership", func(_ *grpcproc.Node, m *fakeMembership) {
			m.events <- grpcproc.MemberEvent{Member: grpcproc.Member{Name: "b", Incarnation: 2}}
		}},
		{"Disconnect", func(a *grpcproc.Node, _ *fakeMembership) { a.Disconnect("b") }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			w := newTwins(t)
			m := &fakeMembership{events: make(chan grpcproc.MemberEvent)}
			a := w.start("a", 1, func(cfg *grpcproc.Config) { cfg.Membership = m })
			old := w.start("b", 1)
			b := w.start("b", 2)
			sink, got := collector(t, a)
			if err := b.SendTo(t.Context(), sink, &testpb.Ping{}); err != nil {
				t.Fatal(err)
			}
			recv(t, got)
			if err := b.Stop(ctx(t)); err != nil {
				t.Fatal(err)
			}
			waitNoPeer(t, a, "b")
			// The old b leaving lifts nothing.
			m.events <- grpcproc.MemberEvent{Member: grpcproc.Member{Name: "b", Incarnation: 1}}
			flush(m)
			if err := old.SendTo(t.Context(), sink, &testpb.Ping{}); !isStale(err, "b#1", "b#2") {
				t.Fatalf("got %v", err)
			}
			tc.lift(a, m)
			flush(m)
			if err := old.SendTo(t.Context(), sink, &testpb.Ping{}); err != nil {
				t.Fatal(err)
			}
			if msg := recv(t, got); msg.From.Incarnation != 1 {
				t.Fatalf("from %v", msg.From)
			}
		})
	}
}

// oldB is node a linked with b's incarnation 1, which runs svc, and which a
// is about to see replaced by incarnation 2.
type oldB struct {
	w      *twins
	a, old *grpcproc.Node
	svc    grpcproc.Addr[*testpb.Ping]
	sink   grpcproc.Addr[proto.Message]
	got    <-chan grpcproc.Msg[proto.Message]
}

// linksIn has n send to a, which it reaches over a link of its own.
func (e oldB) linksIn(t *testing.T, n *grpcproc.Node) {
	t.Helper()
	if err := n.SendTo(t.Context(), e.sink, &testpb.Ping{}); err != nil {
		t.Fatal(err)
	}
	recv(t, e.got)
}

// A newer incarnation takes the place of the older one: a's links with the
// old one go before the new one's come in, monitors across them fire, and
// the old one cannot link again.
func TestNewIncarnationDropsTheOldOnesLinks(t *testing.T) {
	for _, tc := range []struct {
		name   string
		linked func(e oldB, t *testing.T)                   // links a with the old b
		meet   func(e oldB, t *testing.T, b *grpcproc.Node) // links a with the new one
		want   string                                       // a's links then
	}{{
		name:   "both ways, then the new one links in",
		linked: func(e oldB, t *testing.T) { e.linksIn(t, e.old); e.linksOut(t) },
		meet:   oldB.linksIn,
		want:   "b#2",
	}, {
		name:   "out only, then the new one links in",
		linked: oldB.linksOut,
		meet:   oldB.linksIn,
		want:   "b#2",
	}, {
		name:   "in only, then a dials the new one",
		linked: func(e oldB, t *testing.T) { e.linksIn(t, e.old) },
		meet: func(e oldB, t *testing.T, _ *grpcproc.Node) {
			if err := e.a.SendTo(t.Context(), grpcproc.Named[*testpb.Ping]("b", "x"), &testpb.Ping{}); err != nil {
				t.Fatal(err)
			}
		},
		want: ">b#2",
	}} {
		t.Run(tc.name, func(t *testing.T) {
			w := newTwins(t)
			e := oldB{w: w, a: w.start("a", 1), old: w.start("b", 1)}
			e.svc = mustEcho(t, e.old)
			e.sink, e.got = collector(t, e.a)
			watch, downs := watcher(t, e.a)
			tc.linked(e, t)
			outbound := slices.Contains(linksOf(e.a), ">b#1")
			if outbound {
				watch.Monitor(e.svc)
				waitWatchers(t, e.old, e.svc.PID(), 1)
			}
			events := e.a.Subscribe(t.Context(), 16)
			tc.meet(e, t, w.start("b", 2))
			if ev := nextLinkEvent(t, events); ev.Kind != grpcproc.EventLinkDown || ev.Peer.Incarnation != 1 || ev.Err != "restarted as incarnation 2" {
				t.Fatalf("got %+v", ev)
			}
			if ev := nextLinkEvent(t, events); ev.Kind != grpcproc.EventLinkUp || ev.Peer.Incarnation != 2 {
				t.Fatalf("got %+v", ev)
			}
			if outbound {
				if d := recv(t, downs).Down; d == nil || d.Reason != grpcproc.ReasonNoConnection {
					t.Fatalf("got %+v", d)
				}
			}
			sameLinks(t, e.a, tc.want)
			waitNoPeer(t, e.old, "a")
			if err := e.old.SendTo(t.Context(), e.sink, &testpb.Ping{}); !isStale(err, "b#1", "b#2") {
				t.Fatalf("the old b's send: %v", err)
			}
		})
	}
}

// linksOut has a reach the old b over a link of its own. Nothing answers:
// the old b needs no link back.
func (e oldB) linksOut(t *testing.T) {
	t.Helper()
	if err := e.a.SendTo(t.Context(), grpcproc.Named[*testpb.Ping]("b", "x"), &testpb.Ping{}); err != nil {
		t.Fatal(err)
	}
}
