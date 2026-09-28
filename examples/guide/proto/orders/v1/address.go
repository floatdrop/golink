package ordersv1

import (
	"context"

	"github.com/floatdrop/grpcproc"
)

const (
	// Service is the orders service's name in a deployment's placement.
	Service = "orders"
	// DeskName is the name its desk process is registered under.
	DeskName = "desk"
)

// DeskAddr addresses the desk process; Place is its protocol.
type DeskAddr struct{ grpcproc.Addr[*Place] }

// Desk addresses the desk process on node, the node that runs the orders
// service.
func Desk(node string) DeskAddr {
	return DeskAddr{Addr: grpcproc.Named[*Place](node, DeskName)}
}

// Place takes an order. A refusal is part of the answer; an error means no
// answer: the desk could not reach a service it depends on, or not in time.
func (d DeskAddr) Place(ctx context.Context, from grpcproc.Caller, p *Place) (*Placed, error) {
	return d.Call[*Placed](ctx, from, p)
}
