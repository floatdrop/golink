package inspect

import (
	"context"
	"errors"
	"sync"

	"google.golang.org/grpc"

	"github.com/floatdrop/grpcproc"
	inspectv1 "github.com/floatdrop/grpcproc/proto/grpcproc/inspect/v1"
)

// dialer reaches other nodes' Inspectors, keeping one connection per peer:
// by default through the node's own Dial, or as WithResolver says. Its peer
// method is a PeerFunc.
type dialer struct {
	dial func(ctx context.Context, node string) (*grpc.ClientConn, error)

	mu     sync.Mutex
	conns  map[string]*grpc.ClientConn
	closed bool
}

var errClosed = errors.New("inspect: server closed")

func newDialer(dial func(context.Context, string) (*grpc.ClientConn, error)) *dialer {
	return &dialer{dial: dial, conns: map[string]*grpc.ClientConn{}}
}

// resolving dials through r with opts, for WithResolver.
func resolving(r grpcproc.Resolver, opts []grpc.DialOption) func(context.Context, string) (*grpc.ClientConn, error) {
	return func(ctx context.Context, node string) (*grpc.ClientConn, error) {
		addr, err := r.Resolve(ctx, node)
		if err != nil {
			return nil, err
		}
		return grpc.NewClient(addr, opts...)
	}
}

// peer returns an Inspector client for node. It resolves and dials without
// the lock, so a slow resolver holds up only first requests to a node.
func (d *dialer) peer(ctx context.Context, node string) (inspectv1.InspectorClient, error) {
	d.mu.Lock()
	cc, closed := d.conns[node], d.closed
	d.mu.Unlock()
	switch {
	case closed:
		return nil, errClosed
	case cc != nil:
		return inspectv1.NewInspectorClient(cc), nil
	}
	cc, err := d.dial(ctx, node)
	if err != nil {
		return nil, err
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	switch {
	case d.closed:
		_ = cc.Close()
		return nil, errClosed
	case d.conns[node] != nil:
		_ = cc.Close() // a concurrent first request got there first
		cc = d.conns[node]
	default:
		d.conns[node] = cc
	}
	return inspectv1.NewInspectorClient(cc), nil
}

// closeAll closes every connection the dialer opened; it opens no more.
func (d *dialer) closeAll() error {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.closed = true
	var errs []error
	for node, cc := range d.conns {
		errs = append(errs, cc.Close())
		delete(d.conns, node)
	}
	return errors.Join(errs...)
}
