package pubsub

import (
	"errors"
	"slices"
	"strconv"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/emptypb"

	"github.com/floatdrop/grpcproc"
	pubsubv1 "github.com/floatdrop/grpcproc/proto/grpcproc/pubsub/v1"
)

// Spawn starts a topic of E on n. It lasts until it is asked to exit or n
// stops: a bus for the whole node, whoever publishes to it.
func Spawn[E proto.Message](n *grpcproc.Node, cfg Config, opts ...grpcproc.SpawnOption) (Topic[E], error) {
	t := newTopic[E](cfg)
	a, err := n.Spawn(t.run, t.options(opts)...)
	return Topic[E]{a}, err
}

// SpawnOwned starts a topic of E owned by p: linked to p, it ends when p
// does, with p's reason, which its subscribers receive in their Down.
func SpawnOwned[E, M proto.Message](p *grpcproc.Process[M], cfg Config, opts ...grpcproc.SpawnOption) (Topic[E], error) {
	t := newTopic[E](cfg)
	a, err := p.Spawn(t.run, t.options(append(slices.Clip(opts), grpcproc.LinkParent()))...)
	return Topic[E]{a}, err
}

// relayFor finds the relay on n for the topic at of, or starts one. It is named
// after the topic's address, so every subscriber on n that addresses the
// topic the same way finds the same relay.
func relayFor(n *grpcproc.Node, of grpcproc.Addr[proto.Message]) (grpcproc.PID, error) {
	name := "pubsub:" + of.String()
	for {
		if pid, ok := n.Whereis(name); ok {
			return pid, nil
		}
		t := &topic{label: "pubsub.relay", up: &upstream{addr: of}}
		t.isEvent = func(m grpcproc.Msg[proto.Message]) bool { return m.From == t.up.sub.From }
		a, err := n.Spawn(t.run, t.options([]grpcproc.SpawnOption{grpcproc.WithName(name)})...)
		if !errors.Is(err, grpcproc.ErrNameTaken) {
			return a.PID(), err
		}
		// Another subscriber started it first: find it.
	}
}

// topic is the state of a topic's process, or of a relay's: a relay is a
// topic whose events are those of its upstream topic, on another node.
type topic struct {
	cfg     Config
	label   string
	typ     string // the full name of the events' type, or ""
	isEvent func(grpcproc.Msg[proto.Message]) bool
	up      *upstream // a relay's

	p         *grpcproc.Process[proto.Message]
	subs      map[grpcproc.PID]grpcproc.Ref // subscriber → the topic's monitor of it
	kept      []kept                        // the last cfg.Buffer events: a ring, oldest at head
	head      int
	published uint64
}

type upstream struct {
	addr grpcproc.Addr[proto.Message]
	sub  Subscription // the relay's; zero until its first subscriber comes
}

type kept struct {
	body proto.Message
	md   grpcproc.Metadata
}

// errDone ends a topic's process normally.
var errDone = errors.New("pubsub: done")

func newTopic[E proto.Message](cfg Config) *topic {
	cfg.Buffer = max(cfg.Buffer, 0)
	return &topic{
		cfg:     cfg,
		label:   "pubsub.topic",
		typ:     typeName[E](),
		isEvent: func(m grpcproc.Msg[proto.Message]) bool { _, ok := m.Body.(E); return ok },
	}
}

// options puts the topic's label before opts, which can replace it, and its
// inspection after them.
func (t *topic) options(opts []grpcproc.SpawnOption) []grpcproc.SpawnOption {
	return append(append([]grpcproc.SpawnOption{grpcproc.WithLabel(t.label)}, opts...), grpcproc.WithInspect(t.inspect))
}

func (t *topic) run(p *grpcproc.Process[proto.Message]) error {
	t.p, t.subs = p, map[grpcproc.PID]grpcproc.Ref{}
	for {
		m, err := p.Receive()
		if err != nil {
			return err
		}
		if err := t.handle(m); err != nil {
			if errors.Is(err, errDone) {
				return nil
			}
			return err
		}
	}
}

func (t *topic) handle(m grpcproc.Msg[proto.Message]) error {
	switch {
	case m.Down != nil:
		if t.up != nil && m.Down.Ref == t.up.sub.Ref {
			// The topic ended, or its node is gone: so does the relay, for
			// the same reason, which its subscribers see.
			return &grpcproc.ExitError{Reason: m.Down.Reason}
		}
		if ref, ok := t.subs[m.Down.PID]; ok && ref == m.Down.Ref {
			return t.drop(m.Down.PID)
		}
		return nil
	}
	// A topic traps no exits: no Exited reaches it, and the exit of the
	// process that owns it ends it in Receive.
	switch m.Body.(type) {
	case *pubsubv1.Subscribe:
		return t.subscribe(m)
	case *pubsubv1.Unsubscribe:
		if ref, ok := t.subs[m.From]; ok {
			t.p.Demonitor(ref)
			return t.drop(m.From)
		}
		return nil
	}
	if !t.isEvent(m) {
		if m.IsCall() {
			_ = m.Reply(nil, grpcproc.ErrType)
		} else {
			t.p.Log().Warn("pubsub: dropped a message that is not an event of this topic", "from", m.From, "type", proto.MessageName(m.Body))
		}
		return nil
	}
	t.publish(m)
	return nil
}

func (t *topic) subscribe(m grpcproc.Msg[proto.Message]) error {
	if !m.IsCall() {
		t.p.Log().Warn("pubsub: Subscribe must be a call", "from", m.From)
		return nil
	}
	if t.up != nil && t.up.sub.From.IsZero() {
		// The first subscriber waits for this, within its call's deadline,
		// which m.Context ends at: the relay gives up when it does.
		ctx, cancel := m.Context(t.p.Context())
		defer cancel()
		s, r, err := subscribe(ctx, t.p, t.up.addr)
		if err != nil {
			_ = m.Reply(nil, toRelay(err))
			// With no subscriber, the relay ends: the calls that were
			// queued behind this one fail with ErrNoProc, and Subscribe
			// starts another relay for them, which tries again.
			return errDone
		}
		t.up.sub, t.cfg.Buffer, t.typ = s, int(r.GetBuffer()), r.GetType()
	}
	if _, ok := t.subs[m.From]; !ok {
		t.subs[m.From] = t.p.Monitor(m.From)
		for i := range t.kept {
			k := t.kept[(t.head+i)%len(t.kept)]
			ctx := grpcproc.WithMetadata(t.p.Context(), k.md)
			_ = grpcproc.AddrOf[proto.Message](m.From).Send(ctx, t.p, k.body)
		}
		if len(t.subs) == 1 && t.cfg.Notify != nil {
			_ = t.p.SendTo(t.cfg.Notify, &pubsubv1.Demand{Subscribed: true})
		}
	}
	_ = m.Reply(&pubsubv1.Subscribed{From: pidTo(t.p.PID()), Buffer: uint32(t.cfg.Buffer), Type: t.typ}, nil)
	return nil
}

// drop forgets a subscriber. A relay with none left ends: the topic sees it
// go, and forgets it in turn.
func (t *topic) drop(pid grpcproc.PID) error {
	delete(t.subs, pid)
	if len(t.subs) > 0 {
		return nil
	}
	if t.cfg.Notify != nil {
		_ = t.p.SendTo(t.cfg.Notify, &pubsubv1.Demand{Subscribed: false})
	}
	if t.up != nil {
		return errDone
	}
	return nil
}

// publish sends an event to every subscriber, with the metadata it came
// with, which the topic's sends inherit while it handles it.
func (t *topic) publish(m grpcproc.Msg[proto.Message]) {
	t.published++
	if t.cfg.Buffer > 0 {
		if len(t.kept) < t.cfg.Buffer {
			t.kept = append(t.kept, kept{m.Body, m.Metadata})
		} else {
			t.kept[t.head] = kept{m.Body, m.Metadata}
			t.head = (t.head + 1) % len(t.kept)
		}
	}
	for pid := range t.subs {
		_ = t.p.SendTo(pid, m.Body)
	}
	if m.IsCall() {
		_ = m.Reply(&emptypb.Empty{}, nil)
	}
}

func (t *topic) inspect() map[string]string {
	info := map[string]string{
		"subscribers": strconv.Itoa(len(t.subs)),
		"kept":        strconv.Itoa(len(t.kept)),
		"published":   strconv.FormatUint(t.published, 10),
	}
	if t.up != nil {
		info["upstream"] = t.up.addr.String()
	}
	return info
}
