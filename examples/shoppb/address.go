package shoppb

import (
	"context"

	"github.com/floatdrop/grpcproc"
)

// StockAddr addresses a process whose mailbox holds Stock. Its methods are
// the protocol: a reservation is a call answered with Reserved, a restock is
// a send, and the Stock around each is wrapped here, once. A node or a
// process calls them alike.
type StockAddr struct{ grpcproc.Addr[*Stock] }

// Reserve asks for items and waits for what is left.
func (s StockAddr) Reserve(ctx context.Context, from grpcproc.Caller, r *Reserve) (*Reserved, error) {
	return s.Call[*Reserved](ctx, from, &Stock{Op: &Stock_Reserve{Reserve: r}})
}

// Restock adds items. Nobody waits for it.
func (s StockAddr) Restock(ctx context.Context, from grpcproc.Caller, r *Restock) error {
	return s.Send(ctx, from, &Stock{Op: &Stock_Restock{Restock: r}})
}
