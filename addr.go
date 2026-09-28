package grpcproc

import (
	"context"
	"maps"

	"google.golang.org/protobuf/proto"
)

// Addr is a typed address: a PID or a Name plus the message type M the
// process behind it accepts. The type is checked at compile time for local
// and remote sends alike; it is verified again on delivery, because types do
// not cross the wire.
type Addr[M proto.Message] struct {
	pid  PID
	name string
}

// AddrOf types an untyped target. Nothing checks that the process behind it
// really accepts M until a message is delivered.
func AddrOf[M proto.Message](t Target) Addr[M] {
	pid, name := t.target()
	return Addr[M]{pid: pid, name: name}
}

// Named addresses the process registered as name on node.
func Named[M proto.Message](node, name string) Addr[M] {
	return Addr[M]{pid: PID{Node: node}, name: name}
}

// PID returns the address's PID; it is zero but for Node when the address is a name.
func (a Addr[M]) PID() PID { return a.pid }

// Node returns the node the address points at.
func (a Addr[M]) Node() string { return a.pid.Node }

// Name returns the registered name, or "" for a PID address.
func (a Addr[M]) Name() string { return a.name }

func (a Addr[M]) String() string {
	if a.name != "" {
		return Name{Node: a.pid.Node, Name: a.name}.String()
	}
	return a.pid.String()
}

func (a Addr[M]) target() (PID, string) { return a.pid, a.name }

// Call sends req to the address and waits for the reply, typed as R:
//
//	resp, err := addr.Call[*orderspb.Reserved](ctx, from, &orderspb.Order{…})
//
// from is who calls: the Node, or a Process from inside its handler, whose
// call carries the metadata of the message it is handling with ctx's merged
// over it. Replies do not pass through the mailbox, so calling from inside a
// process never reorders its messages. The callee sees ctx's deadline as the
// call's (Msg.Deadline).
//
// A reply of another type is ErrType; a handler error is a *RemoteError; a
// callee that is gone, or exits before answering, is ErrNoProc; a peer that
// cannot be reached is a *LinkError, whose Unsent says that req never left.
// If ctx ends first, Call returns its error, and req may have been handled;
// a ctx already done sends nothing.
//
// It lets a contract package write its protocol once, as a method per
// operation on its own address type, which names R so callers do not:
//
//	type StockAddr struct{ grpcproc.Addr[*Command] }
//
//	func (s StockAddr) Reserve(ctx context.Context, from grpcproc.Caller, r *Reserve) (*Reserved, error) {
//		return s.Call[*Reserved](ctx, from, &Command{Op: &Command_Reserve{Reserve: r}})
//	}
//
// and then, from a node or from inside a process alike:
//
//	reserved, err := stock.Reserve(ctx, p, &Reserve{Sku: "apple", Qty: 2})
func (a Addr[M]) Call[R proto.Message](ctx context.Context, from Caller, req M) (R, error) {
	o := from.origin(ctx)
	return typed[R](o.n.doCall(ctx, o.from, o.p, a.dest(), req, o.md))
}

// Send delivers m to the address, local or remote, from the Node or a
// Process, and returns once m is queued. From a process, m carries the
// metadata of the message it is handling with ctx's merged over it; from the
// node, ctx's. ctx bounds only the wait for a connection to a peer the node
// has no link to yet, and while dials to the peer are failing it does not
// wait at all (see Config.DialBackoff). Sending to a process that does not
// exist is not an error (it is a dead letter); an error means m could not be
// encoded, the node could not be reached, or ctx ended first.
func (a Addr[M]) Send(ctx context.Context, from Caller, m M) error {
	o := from.origin(ctx)
	return o.n.send(ctx, o.from, o.p, a.dest(), m, o.md)
}

// Caller is who sends or calls through Addr.Call and Addr.Send: a *Node, or
// a *Process from inside its handler. Only this package implements it.
type Caller interface {
	origin(ctx context.Context) origin
}

// origin is what the send path needs of a sender: its node, the PID the
// message is from, the process when one sends it, and the metadata to carry.
type origin struct {
	n    *Node
	from PID
	p    *proc
	md   Metadata
}

func (n *Node) origin(ctx context.Context) origin {
	return origin{n: n, from: n.PID(), md: MetadataFrom(ctx)}
}

func (p *proc) origin(ctx context.Context) origin {
	return origin{n: p.n, from: p.pid, p: p, md: p.outgoing(MetadataFrom(ctx))}
}

// dest is a target as the send path carries it: a plain value, where a
// Target would be boxed on the heap for every message.
type dest struct {
	pid  PID
	name string
}

func (a Addr[M]) dest() dest { return dest(a) }

func destOf(t Target) dest {
	pid, name := t.target()
	return dest{pid, name}
}

// typed asserts a reply to the type the caller asked for. A reply of another
// type is ErrType, the same error a caller gets when the callee rejects the
// request's type.
func typed[R proto.Message](resp proto.Message, err error) (R, error) {
	var zero R
	if err != nil {
		return zero, err
	}
	r, ok := resp.(R)
	if !ok {
		return zero, ErrType
	}
	return r, nil
}

type mdKey struct{}

// WithMetadata returns a context carrying md, merged over any metadata already
// in ctx. Every send and call made with ctx propagates it with the message,
// merged, from a process, over what the process inherited.
func WithMetadata(ctx context.Context, md Metadata) context.Context {
	if old := MetadataFrom(ctx); len(old) > 0 {
		merged := maps.Clone(old)
		maps.Copy(merged, md)
		md = merged
	}
	return context.WithValue(ctx, mdKey{}, md)
}

// MetadataFrom returns the metadata carried by ctx, or nil.
func MetadataFrom(ctx context.Context) Metadata {
	md, _ := ctx.Value(mdKey{}).(Metadata)
	return md
}
