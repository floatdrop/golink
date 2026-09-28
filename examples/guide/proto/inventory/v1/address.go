package inventoryv1

import (
	"context"

	"github.com/floatdrop/grpcproc"
)

const (
	// Service is the inventory service's name in a deployment's placement.
	Service = "inventory"
	// StockName is the name its stock process is registered under.
	StockName = "stock"
)

// StockAddr addresses the stock process. Its methods are the protocol: a
// reservation is a call, a release is a send, and the Command that carries
// each is wrapped here, once. A node or a process calls them alike.
type StockAddr struct{ grpcproc.Addr[*Command] }

// Stock addresses the stock process on node, the node that runs the
// inventory service. The name it is registered under is part of the
// contract, as much as the messages it accepts.
func Stock(node string) StockAddr {
	return StockAddr{Addr: grpcproc.Named[*Command](node, StockName)}
}

// Reserve takes items from the stock. A refusal is part of the answer; an
// error means the stock failed, or did not answer.
func (s StockAddr) Reserve(ctx context.Context, from grpcproc.Caller, r *Reserve) (*Reserved, error) {
	return s.Call[*Reserved](ctx, from, &Command{Op: &Command_Reserve{Reserve: r}})
}

// Release gives back what a reservation took. Nobody waits for it.
func (s StockAddr) Release(ctx context.Context, from grpcproc.Caller, r *Release) error {
	return s.Send(ctx, from, &Command{Op: &Command_Release{Release: r}})
}
