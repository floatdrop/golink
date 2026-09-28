// Package pubsub is publish/subscribe on grpcproc processes. A topic is a
// process: it keeps its subscribers and the last few events, and sends each
// event it is given to every subscriber, on any node. It uses only
// grpcproc's public API, so it can be ignored or replaced.
//
// A topic owned by the process that publishes to it ends when that process
// does, and its subscribers receive the reason in a Down:
//
//	placed, err := pubsub.SpawnOwned[*orderspb.Placed](p, pubsub.Config{Buffer: 100}, grpcproc.WithName("placed"))
//	…
//	err = placed.Publish(ctx, p, &orderspb.Placed{Id: id})
//
// A process on any node subscribes by the topic's name:
//
//	placed := pubsub.Named[*orderspb.Placed]("orders", "placed")
//	sub, err := placed.Subscribe(ctx, p)
//	for {
//		m, err := p.Receive()
//		…	// an event is a message from sub.From; a Down with sub.Ref, the end of them
//	}
//
// A process subscribes to a topic of another node through a relay on its own
// node, which the first such subscriber starts and the last one's leaving
// ends. The topic sends each event once to each node, however many
// subscribers the node has.
//
// Events are delivered as grpcproc delivers any message: at most once, and
// in order from one sender, so in the order the topic received them.
package pubsub

import (
	"context"
	"errors"
	"fmt"
	"reflect"

	"google.golang.org/protobuf/proto"

	"github.com/floatdrop/grpcproc"
	pubsubv1 "github.com/floatdrop/grpcproc/proto/grpcproc/pubsub/v1"
	grpcprocv1 "github.com/floatdrop/grpcproc/proto/grpcproc/v1"
)

// Config configures a topic.
type Config struct {
	// Buffer is how many of the last events the topic keeps. A new
	// subscriber receives them before any event published after it
	// subscribed. Zero keeps none.
	Buffer int
	// Notify, when set, is sent a *pubsubv1.Demand when the first
	// subscriber comes and when the last one leaves: for a producer that
	// works only while someone listens.
	Notify grpcproc.Target
}

// Topic is the address of a topic whose events are E.
type Topic[E proto.Message] struct{ addr grpcproc.Addr[proto.Message] }

// Named addresses the topic registered as name on node.
func Named[E proto.Message](node, name string) Topic[E] {
	return Topic[E]{grpcproc.Named[proto.Message](node, name)}
}

// TopicOf addresses a topic by its PID or its Name.
func TopicOf[E proto.Message](t grpcproc.Target) Topic[E] {
	return Topic[E]{grpcproc.AddrOf[proto.Message](t)}
}

// Addr returns the address of the topic's process, to monitor it, link to
// it, or ask it to exit.
func (t Topic[E]) Addr() grpcproc.Addr[proto.Message] { return t.addr }

func (t Topic[E]) String() string { return t.addr.String() }

// Publish sends e to the topic, which sends it on to every subscriber. from
// is the Node or a Process, as for grpcproc.Addr.Send; the subscribers
// receive e with the metadata it was published with.
func (t Topic[E]) Publish(ctx context.Context, from grpcproc.Caller, e E) error {
	return t.addr.Send(ctx, from, e)
}

// Subscription is a process's subscription to a topic.
type Subscription struct {
	// From is the process the events come from: the topic, or, for a topic
	// of another node, the relay on the subscriber's node.
	From grpcproc.PID
	// Ref is the subscriber's monitor of From. The Down with Ref means no
	// more events come: its reason is the topic's exit reason, or
	// noconnection when the topic's node became unreachable.
	Ref grpcproc.Ref
}

// Subscribe subscribes p to t. When it returns, the events t keeps are in
// p's mailbox, and those published after them follow, as messages from
// s.From. p's mailbox must accept E. A second Subscribe to the same topic is
// the same subscription.
//
// ctx bounds the whole of it: for a topic of another node, the relay's own
// subscription to the topic too, when p is the relay's first subscriber. If
// ctx ends first, Subscribe tells the topic to forget p, in case it got the
// request all the same; events it sent before that may still reach p.
func (t Topic[E]) Subscribe[M proto.Message](ctx context.Context, p *grpcproc.Process[M]) (Subscription, error) {
	if !reflect.TypeFor[E]().AssignableTo(reflect.TypeFor[M]()) {
		return Subscription{}, fmt.Errorf("pubsub: a process of %s cannot receive %s: %w", reflect.TypeFor[M](), reflect.TypeFor[E](), grpcproc.ErrType)
	}
	relayed := t.addr.Node() != p.Node().Name()
	for {
		to := grpcproc.Target(t.addr)
		if relayed {
			relay, err := relayFor(p.Node(), t.addr)
			if err != nil {
				return Subscription{}, err
			}
			to = relay
		}
		s, r, err := subscribe(ctx, p, to)
		if relayed && errors.Is(err, grpcproc.ErrNoProc) && ctx.Err() == nil {
			continue // the relay ended before it took the call: start another
		}
		if err != nil {
			return Subscription{}, fmt.Errorf("pubsub: subscribe to %s: %w", t, fromRelay(err))
		}
		if want := typeName[E](); want != "" && r.GetType() != "" && r.GetType() != want {
			s.Cancel(p)
			return Subscription{}, fmt.Errorf("pubsub: %s publishes %s, not %s: %w", t, r.GetType(), want, grpcproc.ErrType)
		}
		return s, nil
	}
}

// Cancel ends the subscription: p stops monitoring s.From and tells it to
// send no more. Events already in p's mailbox stay there.
func (s Subscription) Cancel[M proto.Message](p *grpcproc.Process[M]) {
	p.Demonitor(s.Ref)
	_ = p.SendTo(s.From, &pubsubv1.Unsubscribe{})
}

// subscribe calls to, a topic or a relay, and monitors it.
//
// On one node, the monitor follows the answer and is placed by PID, at
// once: a topic that exits in between is a Down all the same, after the
// events it sent, and a call that fails leaves no Down behind. Across nodes,
// which only a relay's call to its topic crosses, a monitor placed after the
// answer could reach a topic that had exited meanwhile and report noproc in
// place of its reason; so there the monitor goes first, on the same link as
// the call. A failed call then leaves a Down in the relay's mailbox, but the
// relay ends.
func subscribe[M proto.Message](ctx context.Context, p *grpcproc.Process[M], to grpcproc.Target) (Subscription, *pubsubv1.Subscribed, error) {
	var ref grpcproc.Ref
	remote := grpcproc.AddrOf[proto.Message](to).Node() != p.Node().Name()
	if remote {
		ref = p.Monitor(to)
	}
	r, err := p.CallTo[*pubsubv1.Subscribed](ctx, to, &pubsubv1.Subscribe{})
	if err != nil {
		if remote {
			p.Demonitor(ref)
		}
		if ctx.Err() != nil {
			_ = p.SendTo(to, &pubsubv1.Unsubscribe{})
		}
		return Subscription{}, nil, err
	}
	from := pidFrom(r.GetFrom())
	if !remote {
		ref = p.Monitor(from)
	}
	return Subscription{From: from, Ref: ref}, r, nil
}

// A reply carries no sentinel error, only its text: a relay that answers
// with the error of its own call to the topic answers with its text, and
// fromRelay turns that back into the error.
var relayErrs = []error{grpcproc.ErrNoProc, grpcproc.ErrNoConnection, grpcproc.ErrType, context.DeadlineExceeded}

func toRelay(err error) error {
	for _, e := range relayErrs {
		if errors.Is(err, e) {
			return e
		}
	}
	return err
}

func fromRelay(err error) error {
	if re, ok := errors.AsType[*grpcproc.RemoteError](err); ok {
		for _, e := range relayErrs {
			if re.Msg == e.Error() {
				return e
			}
		}
	}
	return err
}

// typeName is the full name of E, or "" when E is an interface.
func typeName[E proto.Message]() string {
	if reflect.TypeFor[E]().Kind() == reflect.Interface {
		return ""
	}
	var e E
	return string(e.ProtoReflect().Descriptor().FullName())
}

func pidTo(p grpcproc.PID) *grpcprocv1.PID {
	return &grpcprocv1.PID{Node: p.Node, Incarnation: p.Incarnation, Id: p.ID}
}

func pidFrom(p *grpcprocv1.PID) grpcproc.PID {
	return grpcproc.PID{Node: p.GetNode(), Incarnation: p.GetIncarnation(), ID: p.GetId()}
}
