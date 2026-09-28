package pubsub_test

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"testing/synctest"
	"time"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/emptypb"

	"github.com/floatdrop/grpcproc"
	"github.com/floatdrop/grpcproc/grpcproctest"
	"github.com/floatdrop/grpcproc/internal/testpb"
	pubsubv1 "github.com/floatdrop/grpcproc/proto/grpcproc/pubsub/v1"
	"github.com/floatdrop/grpcproc/pubsub"
)

type Pings = pubsub.Topic[*testpb.Ping]

func ctx(t testing.TB) context.Context {
	c, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	t.Cleanup(cancel)
	return c
}

func within[T any](t *testing.T, ch <-chan T) T {
	t.Helper()
	select {
	case v := <-ch:
		return v
	case <-time.After(5 * time.Second):
		t.Fatal("timed out")
		panic("unreachable")
	}
}

// eventually polls cond until it holds, for up to 5s.
func eventually(t *testing.T, what string, cond func() bool) {
	t.Helper()
	for deadline := time.Now().Add(5 * time.Second); !cond(); {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// listener is a process subscribed to a topic. It reports what reaches it on
// got: "N" for an event (" k=V" after it when its metadata has k), "down
// REASON" for the end of the subscription. Any other message cancels it.
type listener struct {
	addr grpcproc.Addr[proto.Message]
	sub  pubsub.Subscription
	got  chan string
}

func listen(t *testing.T, n *grpcproc.Node, topic Pings) *listener {
	t.Helper()
	l := &listener{got: make(chan string, 64)}
	ready := make(chan error, 1)
	addr, err := n.Spawn(func(p *grpcproc.Process[proto.Message]) error {
		s, err := topic.Subscribe(ctx(t), p)
		l.sub = s
		ready <- err
		if err != nil {
			return nil
		}
		for {
			m, err := p.Receive()
			if err != nil {
				return err
			}
			switch {
			case m.Down != nil:
				l.got <- "down " + m.Down.Reason
			case m.From == s.From:
				got := fmt.Sprint(m.Body.(*testpb.Ping).GetN())
				if v, ok := m.Metadata["k"]; ok {
					got += " k=" + v
				}
				l.got <- got
			default:
				s.Cancel(p)
				l.got <- "cancelled"
			}
		}
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := within(t, ready); err != nil {
		t.Fatal(err)
	}
	l.addr = addr
	return l
}

func (l *listener) expect(t *testing.T, want ...string) {
	t.Helper()
	for _, w := range want {
		if got := within(t, l.got); got != w {
			t.Fatalf("got %q, want %q", got, w)
		}
	}
}

// quiet checks that nothing more reaches l for a while.
func (l *listener) quiet(t *testing.T) {
	t.Helper()
	select {
	case got := <-l.got:
		t.Fatalf("got %q, want nothing", got)
	case <-time.After(50 * time.Millisecond):
	}
}

func (l *listener) cancel(t *testing.T, n *grpcproc.Node) {
	t.Helper()
	if err := n.SendTo(ctx(t), l.addr, &testpb.Pong{}); err != nil {
		t.Fatal(err)
	}
	l.expect(t, "cancelled")
}

func publish(t *testing.T, from grpcproc.Caller, topic Pings, ns ...int64) {
	t.Helper()
	for _, n := range ns {
		if err := topic.Publish(ctx(t), from, &testpb.Ping{N: n}); err != nil {
			t.Fatal(err)
		}
	}
}

func inspect(t *testing.T, n *grpcproc.Node, pid grpcproc.PID, key string) string {
	t.Helper()
	info, err := n.Inspect(ctx(t), pid)
	if err != nil {
		t.Fatal(err)
	}
	return info[key]
}

func TestSubscriberGetsWhatTheTopicKeptThenWhatFollows(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		a := grpcproctest.New(t, "a").Node("a")
		topic, err := pubsub.Spawn[*testpb.Ping](a, pubsub.Config{Buffer: 2})
		if err != nil {
			t.Fatal(err)
		}
		publish(t, a, topic, 1, 2, 3)

		l := listen(t, a, topic)
		l.expect(t, "2", "3")
		publish(t, a, topic, 4)
		l.expect(t, "4")
		l.quiet(t)
	})
}

func TestEventsKeepTheirMetadata(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		a := grpcproctest.New(t, "a").Node("a")
		topic, err := pubsub.Spawn[*testpb.Ping](a, pubsub.Config{Buffer: 1})
		if err != nil {
			t.Fatal(err)
		}
		with := func(v string) context.Context { return grpcproc.WithMetadata(ctx(t), grpcproc.Metadata{"k": v}) }
		if err := topic.Publish(with("kept"), a, &testpb.Ping{N: 1}); err != nil {
			t.Fatal(err)
		}
		l := listen(t, a, topic)
		if err := topic.Publish(with("live"), a, &testpb.Ping{N: 2}); err != nil {
			t.Fatal(err)
		}
		l.expect(t, "1 k=kept", "2 k=live")
	})
}

func TestOwnedTopicEndsWithItsOwner(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		a := grpcproctest.New(t, "a").Node("a")
		topics := make(chan Pings, 1)
		owner, err := a.Spawn(func(p *grpcproc.Process[proto.Message]) error {
			topic, err := pubsub.SpawnOwned[*testpb.Ping](p, pubsub.Config{}, grpcproc.WithName("pings"))
			if err != nil {
				return err
			}
			topics <- topic
			for {
				if _, err := p.Receive(); err != nil {
					return err
				}
			}
		})
		if err != nil {
			t.Fatal(err)
		}
		topic := within(t, topics)
		l := listen(t, a, pubsub.Named[*testpb.Ping]("a", "pings"))
		publish(t, a, topic, 1)
		l.expect(t, "1")

		if err := a.Exit(ctx(t), owner, "closing"); err != nil {
			t.Fatal(err)
		}
		l.expect(t, "down closing")
	})
}

func TestNotifyTellsWhenTheFirstSubscriberComesAndTheLastLeaves(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		a := grpcproctest.New(t, "a").Node("a")
		topics, demand := make(chan Pings, 1), make(chan bool, 8)
		_, err := a.Spawn(func(p *grpcproc.Process[proto.Message]) error {
			topic, err := pubsub.SpawnOwned[*testpb.Ping](p, pubsub.Config{Notify: p.PID()})
			if err != nil {
				return err
			}
			topics <- topic
			for {
				m, err := p.Receive()
				if err != nil {
					return err
				}
				if d, ok := m.Body.(*pubsubv1.Demand); ok && m.From == topic.Addr().PID() {
					demand <- d.GetSubscribed()
				}
			}
		})
		if err != nil {
			t.Fatal(err)
		}
		topic := within(t, topics)
		noDemand := func() {
			t.Helper()
			select {
			case d := <-demand:
				t.Fatalf("demand %v, want none", d)
			case <-time.After(50 * time.Millisecond):
			}
		}

		l1 := listen(t, a, topic)
		if !within(t, demand) {
			t.Fatal("first subscriber: want demand")
		}
		l2 := listen(t, a, topic)
		noDemand()
		if err := a.Exit(ctx(t), l1.addr, "bye"); err != nil { // leaving by exiting
			t.Fatal(err)
		}
		noDemand()
		l2.cancel(t, a) // leaving by cancelling
		if within(t, demand) {
			t.Fatal("last subscriber gone: want no demand")
		}
		listen(t, a, topic)
		if !within(t, demand) {
			t.Fatal("a subscriber again: want demand")
		}
	})
}

// Subscribers of a topic on another node share a relay on theirs: the
// topic has one subscriber for the node, and sends each event there once.
func TestRemoteSubscribersShareARelay(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		c := grpcproctest.New(t, "a", "b")
		a, b := c.Node("a"), c.Node("b")
		topic, err := pubsub.Spawn[*testpb.Ping](a, pubsub.Config{Buffer: 3}, grpcproc.WithName("pings"))
		if err != nil {
			t.Fatal(err)
		}
		publish(t, a, topic, 1, 2, 3, 4)

		remote := pubsub.Named[*testpb.Ping]("a", "pings")
		l1 := listen(t, b, remote)
		l1.expect(t, "2", "3", "4")
		l2 := listen(t, b, remote) // what the relay kept
		l2.expect(t, "2", "3", "4")
		local := listen(t, a, topic)
		local.expect(t, "2", "3", "4")

		publish(t, b, remote, 5) // from the other node, as well
		for _, l := range []*listener{l1, l2, local} {
			l.expect(t, "5")
			l.quiet(t)
		}

		relay, ok := b.Whereis("pubsub:{pings@a}")
		if !ok {
			t.Fatal("no relay on b")
		}
		if l1.sub.From != relay || l2.sub.From != relay || local.sub.From != topic.Addr().PID() {
			t.Fatalf("events from %v, %v, %v; want the relay %v, and the topic %v", l1.sub.From, l2.sub.From, local.sub.From, relay, topic.Addr().PID())
		}
		if got := inspect(t, a, topic.Addr().PID(), "subscribers"); got != "2" {
			t.Fatalf("topic subscribers %s, want 2: the relay and the local one", got)
		}
		if got := inspect(t, b, relay, "subscribers"); got != "2" {
			t.Fatalf("relay subscribers %s, want 2", got)
		}

		// The relay ends with the last of its subscribers, and the topic forgets it.
		l1.cancel(t, b)
		l2.cancel(t, b)
		eventually(t, "the relay to end", func() bool { _, ok := b.Whereis("pubsub:{pings@a}"); return !ok })
		eventually(t, "the topic to forget the relay", func() bool { return inspect(t, a, topic.Addr().PID(), "subscribers") == "1" })
	})
}

func TestRemoteSubscribersSeeTheTopicEnd(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		c := grpcproctest.New(t, "a", "b")
		a, b := c.Node("a"), c.Node("b")
		topic, err := pubsub.Spawn[*testpb.Ping](a, pubsub.Config{}, grpcproc.WithName("pings"))
		if err != nil {
			t.Fatal(err)
		}
		l := listen(t, b, pubsub.Named[*testpb.Ping]("a", "pings"))
		if err := a.Exit(ctx(t), topic.Addr(), "bye"); err != nil {
			t.Fatal(err)
		}
		l.expect(t, "down bye")
	})
}

func TestRemoteSubscribersSeeTheTopicsNodeGo(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		c := grpcproctest.New(t, "a", "b")
		a, b := c.Node("a"), c.Node("b")
		if _, err := pubsub.Spawn[*testpb.Ping](a, pubsub.Config{}, grpcproc.WithName("pings")); err != nil {
			t.Fatal(err)
		}
		l := listen(t, b, pubsub.Named[*testpb.Ping]("a", "pings"))
		c.Partition("a", "b")
		l.expect(t, "down "+grpcproc.ReasonNoConnection)
	})
}

// subscribeFrom subscribes a new process of n to topic, and returns what
// Subscribe did.
func subscribeFrom[M, E proto.Message](t *testing.T, n *grpcproc.Node, topic pubsub.Topic[E]) error {
	t.Helper()
	done := make(chan error, 1)
	if _, err := n.Spawn(func(p *grpcproc.Process[M]) error {
		_, err := topic.Subscribe(ctx(t), p)
		done <- err
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	return within(t, done)
}

func TestSubscribeErrors(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		c := grpcproctest.New(t, "a", "b")
		a, b := c.Node("a"), c.Node("b")
		topic, err := pubsub.Spawn[*testpb.Ping](a, pubsub.Config{}, grpcproc.WithName("pings"))
		if err != nil {
			t.Fatal(err)
		}
		nope := pubsub.Named[*testpb.Ping]("a", "nope")
		pongs := pubsub.Named[*testpb.Pong]("a", "pings")
		for _, tc := range []struct {
			name string
			err  error
			want error
		}{
			{"no topic", subscribeFrom[proto.Message](t, a, nope), grpcproc.ErrNoProc},
			{"no topic, through a relay", subscribeFrom[proto.Message](t, b, nope), grpcproc.ErrNoProc},
			{"a mailbox of another type", subscribeFrom[*testpb.Pong](t, a, topic), grpcproc.ErrType},
			{"a topic of another type", subscribeFrom[proto.Message](t, a, pongs), grpcproc.ErrType},
			{"a topic of another type, through a relay", subscribeFrom[proto.Message](t, b, pongs), grpcproc.ErrType},
		} {
			if !errors.Is(tc.err, tc.want) {
				t.Errorf("%s: %v, want %v", tc.name, tc.err, tc.want)
			}
		}
		eventually(t, "the relays to end", func() bool {
			_, nope := b.Whereis("pubsub:{nope@a}")
			_, pings := b.Whereis("pubsub:{pings@a}")
			return !nope && !pings
		})
		eventually(t, "the topic to forget who subscribed to the wrong type", func() bool {
			return inspect(t, a, topic.Addr().PID(), "subscribers") == "0"
		})
	})
}

// A subscriber whose ctx ends before the topic answers tells the topic to
// forget it, in case the topic takes the request later.
func TestSubscribeThatGivesUpUnsubscribes(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		a := grpcproctest.New(t, "a").Node("a")
		got := make(chan proto.Message, 2)
		if _, err := a.Spawn(func(p *grpcproc.Process[proto.Message]) error {
			for { // a topic too slow to answer
				m, err := p.Receive()
				if err != nil {
					return err
				}
				got <- m.Body
			}
		}, grpcproc.WithName("slow")); err != nil {
			t.Fatal(err)
		}
		done := make(chan error, 1)
		if _, err := a.Spawn(func(p *grpcproc.Process[proto.Message]) error {
			c, cancel := context.WithTimeout(t.Context(), 20*time.Millisecond)
			defer cancel()
			_, err := pubsub.Named[*testpb.Ping]("a", "slow").Subscribe(c, p)
			done <- err
			return nil
		}); err != nil {
			t.Fatal(err)
		}
		if err := within(t, done); !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("Subscribe: %v, want DeadlineExceeded", err)
		}
		if _, ok := within(t, got).(*pubsubv1.Subscribe); !ok {
			t.Fatal("want the Subscribe first")
		}
		if _, ok := within(t, got).(*pubsubv1.Unsubscribe); !ok {
			t.Fatal("want an Unsubscribe after it")
		}
	})
}

func TestTopicTakesOnlyItsEvents(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		a := grpcproctest.New(t, "a").Node("a")
		topic, err := pubsub.Spawn[*testpb.Ping](a, pubsub.Config{})
		if err != nil {
			t.Fatal(err)
		}
		l := listen(t, a, topic)
		if err := a.SendTo(ctx(t), topic.Addr(), &testpb.Pong{N: 1}); err != nil {
			t.Fatal(err)
		}
		if _, err := a.CallTo[*emptypb.Empty](ctx(t), topic.Addr(), &testpb.Pong{N: 2}); err == nil {
			t.Fatal("a call with another type: want an error")
		}
		if _, err := a.CallTo[*emptypb.Empty](ctx(t), topic.Addr(), &testpb.Ping{N: 3}); err != nil {
			t.Fatalf("an event published with a call: %v", err)
		}
		l.expect(t, "3")
		l.quiet(t)
	})
}

func TestTopicOfAddressesATopicByPID(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		a := grpcproctest.New(t, "a").Node("a")
		spawned, err := pubsub.Spawn[*testpb.Ping](a, pubsub.Config{})
		if err != nil {
			t.Fatal(err)
		}
		topic := pubsub.TopicOf[*testpb.Ping](spawned.Addr().PID())
		l := listen(t, a, topic)
		publish(t, a, topic, 1)
		l.expect(t, "1")
	})
}

// A topic whose events are of an interface type takes any message of it,
// and so do its subscribers.
func TestTopicOfAnyMessage(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		a := grpcproctest.New(t, "a").Node("a")
		topic, err := pubsub.Spawn[proto.Message](a, pubsub.Config{Buffer: 1})
		if err != nil {
			t.Fatal(err)
		}
		if err := topic.Publish(ctx(t), a, &testpb.Pong{N: 1}); err != nil {
			t.Fatal(err)
		}
		got := make(chan string, 2)
		if _, err := a.Spawn(func(p *grpcproc.Process[proto.Message]) error {
			if _, err := topic.Subscribe(ctx(t), p); err != nil {
				return err
			}
			for {
				m, err := p.Receive()
				if err != nil {
					return err
				}
				got <- string(proto.MessageName(m.Body).Name())
			}
		}); err != nil {
			t.Fatal(err)
		}
		if name := within(t, got); name != "Pong" {
			t.Fatalf("kept: %s, want Pong", name)
		}
		if err := topic.Publish(ctx(t), a, &testpb.Ping{N: 2}); err != nil {
			t.Fatal(err)
		}
		if name := within(t, got); name != "Ping" {
			t.Fatalf("published: %s, want Ping", name)
		}
	})
}

// A Subscribe that is not a call, and an Unsubscribe from a process that
// did not subscribe, change nothing.
func TestTopicIgnoresStrayRequests(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		a := grpcproctest.New(t, "a").Node("a")
		topic, err := pubsub.Spawn[*testpb.Ping](a, pubsub.Config{})
		if err != nil {
			t.Fatal(err)
		}
		l := listen(t, a, topic)
		for _, m := range []proto.Message{&pubsubv1.Subscribe{}, &pubsubv1.Unsubscribe{}} {
			if err := a.SendTo(ctx(t), topic.Addr(), m); err != nil {
				t.Fatal(err)
			}
		}
		if got := inspect(t, a, topic.Addr().PID(), "subscribers"); got != "1" {
			t.Fatalf("subscribers %s, want 1", got)
		}
		publish(t, a, topic, 1)
		l.expect(t, "1")
	})
}

// slowTopic stands in for a topic registered as name on n. It reports each
// Subscribe it is called with on held, then answers it with what it is
// given on answer: an error, or nil for Subscribed.
func slowTopic(t *testing.T, n *grpcproc.Node, name string) (held <-chan struct{}, answer chan<- error) {
	t.Helper()
	h, ans := make(chan struct{}, 4), make(chan error, 4)
	if _, err := n.Spawn(func(p *grpcproc.Process[proto.Message]) error {
		for {
			m, err := p.Receive()
			if err != nil {
				return err
			}
			if _, ok := m.Body.(*pubsubv1.Subscribe); !ok || !m.IsCall() {
				continue
			}
			h <- struct{}{}
			select {
			case err := <-ans:
				_ = m.Reply(&pubsubv1.Subscribed{From: p.PID().Proto()}, err)
			case <-p.Context().Done():
				return nil
			}
		}
	}, grpcproc.WithName(name)); err != nil {
		t.Fatal(err)
	}
	return h, ans
}

// A relay subscribes to its topic within the time its first subscriber has:
// when the subscriber gives up, so does the relay, and it ends.
func TestRelayGivesUpWithItsFirstSubscriber(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		c := grpcproctest.New(t, "a", "b")
		b := c.Node("b")
		slowTopic(t, c.Node("a"), "slow") // never answers
		done := make(chan error, 1)
		if _, err := b.Spawn(func(p *grpcproc.Process[proto.Message]) error {
			c, cancel := context.WithTimeout(t.Context(), 50*time.Millisecond)
			defer cancel()
			_, err := pubsub.Named[*testpb.Ping]("a", "slow").Subscribe(c, p)
			done <- err
			return nil
		}); err != nil {
			t.Fatal(err)
		}
		if err := within(t, done); !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("Subscribe: %v, want DeadlineExceeded", err)
		}
		eventually(t, "the relay to end", func() bool { _, ok := b.Whereis("pubsub:{slow@a}"); return !ok })
	})
}

// A relay that fails to subscribe to its topic answers its first subscriber
// with the error and ends. Subscribers queued behind the first start
// another relay, which tries again.
func TestSubscribersQueuedOnAFailedRelayStartAnother(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		c := grpcproctest.New(t, "a", "b")
		b := c.Node("b")
		held, answer := slowTopic(t, c.Node("a"), "flaky")
		topic := pubsub.Named[*testpb.Ping]("a", "flaky")

		type result struct {
			sub pubsub.Subscription
			err error
		}
		subscribe := func() <-chan result {
			done := make(chan result, 1)
			if _, err := b.Spawn(func(p *grpcproc.Process[proto.Message]) error {
				s, err := topic.Subscribe(t.Context(), p) // no deadline: the relay waits as long as it takes
				done <- result{s, err}
				return nil
			}); err != nil {
				t.Fatal(err)
			}
			return done
		}

		first := subscribe()
		within(t, held) // the relay waits on the topic
		relay, ok := b.Whereis("pubsub:{flaky@a}")
		if !ok {
			t.Fatal("no relay")
		}
		second := subscribe()
		eventually(t, "the second Subscribe to queue on the relay", func() bool {
			info, ok := b.Process(relay)
			return ok && info.Mailbox.Depth == 1
		})

		answer <- errors.New("not yet")
		r := within(t, first)
		if re, ok := errors.AsType[*grpcproc.RemoteError](r.err); !ok || re.Msg != "not yet" {
			t.Fatalf("first: %v, want the topic's error", r.err)
		}
		within(t, held) // the second's relay asks again
		answer <- nil
		r = within(t, second)
		if r.err != nil {
			t.Fatalf("second: %v", r.err)
		}
		if r.sub.From == relay || r.sub.From.Node != "b" {
			t.Fatalf("second subscribed through %v, want a new relay on b", r.sub.From)
		}
	})
}

// A subscriber to a topic of another node starts a relay, which a node that
// is stopping does not.
func TestSubscribeOnAStoppingNode(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		c := grpcproctest.New(t, "a", "b")
		b := c.Node("b")
		release, done := make(chan struct{}), make(chan error, 1)
		if _, err := b.Spawn(func(p *grpcproc.Process[proto.Message]) error {
			<-release
			_, err := pubsub.Named[*testpb.Ping]("a", "pings").Subscribe(ctx(t), p)
			done <- err
			return nil
		}); err != nil {
			t.Fatal(err)
		}
		stopped := make(chan error, 1)
		go func() { stopped <- b.Stop(ctx(t)) }()
		eventually(t, "b to stop spawning", func() bool {
			_, err := b.Spawn(func(*grpcproc.Process[proto.Message]) error { return nil })
			return errors.Is(err, grpcproc.ErrNodeStopped)
		})
		close(release)
		if err := within(t, done); !errors.Is(err, grpcproc.ErrNodeStopped) {
			t.Fatalf("Subscribe: %v, want ErrNodeStopped", err)
		}
		if err := within(t, stopped); err != nil {
			t.Fatal(err)
		}
	})
}
