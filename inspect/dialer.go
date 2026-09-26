package inspect

import (
	"context"
	"errors"
	"sync"

	"google.golang.org/grpc"

	"github.com/floatdrop/golink"
	inspectv1 "github.com/floatdrop/golink/proto/golink/inspect/v1"
)

// Dialer reaches other nodes' Inspectors through the same resolver the node
// uses, keeping one connection per peer. Its Peer method is a PeerFunc.
type Dialer struct {
	resolver golink.Resolver
	opts     []grpc.DialOption

	mu    sync.Mutex
	conns map[string]*grpc.ClientConn
}

// NewDialer returns a Dialer that resolves node names with r and dials with opts.
func NewDialer(r golink.Resolver, opts ...grpc.DialOption) *Dialer {
	return &Dialer{resolver: r, opts: opts, conns: map[string]*grpc.ClientConn{}}
}

// Peer returns an Inspector client for node.
func (d *Dialer) Peer(ctx context.Context, node string) (inspectv1.InspectorClient, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if cc := d.conns[node]; cc != nil {
		return inspectv1.NewInspectorClient(cc), nil
	}
	addr, err := d.resolver.Resolve(ctx, node)
	if err != nil {
		return nil, err
	}
	cc, err := grpc.NewClient(addr, d.opts...)
	if err != nil {
		return nil, err
	}
	d.conns[node] = cc
	return inspectv1.NewInspectorClient(cc), nil
}

// Close closes every connection the Dialer opened.
func (d *Dialer) Close() error {
	d.mu.Lock()
	defer d.mu.Unlock()
	var errs []error
	for node, cc := range d.conns {
		errs = append(errs, cc.Close())
		delete(d.conns, node)
	}
	return errors.Join(errs...)
}
