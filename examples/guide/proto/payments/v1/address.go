package paymentsv1

import (
	"context"

	"github.com/floatdrop/grpcproc"
)

const (
	// Service is the payments service's name in a deployment's placement.
	Service = "payments"
	// CashierName is the name its cashier process is registered under.
	CashierName = "cashier"
)

// CashierAddr addresses the cashier process; Charge is its protocol.
type CashierAddr struct{ grpcproc.Addr[*Charge] }

// Cashier addresses the cashier process on node, the node that runs the
// payments service.
func Cashier(node string) CashierAddr {
	return CashierAddr{Addr: grpcproc.Named[*Charge](node, CashierName)}
}

// Charge takes an amount from a card. A declined card is part of the
// answer; an error means the card may or may not have been charged.
func (c CashierAddr) Charge(ctx context.Context, from grpcproc.Caller, ch *Charge) (*Charged, error) {
	return c.Call[*Charged](ctx, from, ch)
}
