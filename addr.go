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
// in ctx. Node.Send, Node.SendTo and every Call propagate it with the message.
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
