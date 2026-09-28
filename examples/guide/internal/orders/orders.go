// Package orders is the service that takes orders. Its desk reserves the
// items with the inventory service, charges for them with the payments
// service, and gives them back when the cashier declines. It reaches both
// through their contracts and the configuration's placement: whether they
// run in this program or on other nodes is not the desk's business.
package orders

import (
	"context"
	"fmt"
	"time"
	"uuid"

	"golang.yandex/di"
	"google.golang.org/protobuf/proto"

	"github.com/floatdrop/grpcproc"
	"github.com/floatdrop/grpcproc/actor"
	"github.com/floatdrop/grpcproc/examples/guide/internal/platform"
	inventoryv1 "github.com/floatdrop/grpcproc/examples/guide/proto/inventory/v1"
	ordersv1 "github.com/floatdrop/grpcproc/examples/guide/proto/orders/v1"
	paymentsv1 "github.com/floatdrop/grpcproc/examples/guide/proto/payments/v1"
)

// Prices are what each sku costs, in cents.
type Prices map[string]int64

// desk is the actor. It only answers calls, and handles one order at a
// time: while it waits on the inventory and the payments, the next order
// waits in its mailbox.
type desk struct {
	actor.CallsOnly[*ordersv1.Place]
	stock   inventoryv1.StockAddr
	cashier paymentsv1.CashierAddr
	prices  Prices
}

func (d *desk) HandleCall(p *grpcproc.Process[*ordersv1.Place], m grpcproc.Msg[*ordersv1.Place]) (proto.Message, error) {
	place := m.Body
	price, ok := d.prices[place.Sku]
	if !ok {
		return &ordersv1.Placed{Refused: fmt.Sprintf("no such item %q", place.Sku)}, nil
	}
	order := uuid.New().String()
	ctx, cancel := context.WithTimeout(p.Context(), 5*time.Second)
	defer cancel()

	// A no is part of each answer. An error is no answer, or a failure:
	// what happened is not known, so it is left as it is, logged for
	// whoever reconciles orders, and the call fails.
	reserved, err := d.stock.Reserve(ctx, p, &inventoryv1.Reserve{Order: order, Sku: place.Sku, Qty: place.Qty})
	if err != nil {
		p.Log().Error("reservation unsettled", "order", order, "err", err)
		return nil, err
	}
	if reserved.Refused != "" {
		return &ordersv1.Placed{Refused: reserved.Refused}, nil
	}
	charged, err := d.cashier.Charge(ctx, p, &paymentsv1.Charge{
		Order: order, Card: place.Card, Amount: price * place.Qty,
	})
	if err != nil {
		// The card may have been charged: the items stay reserved.
		p.Log().Error("charge unsettled", "order", order, "err", err)
		return nil, err
	}
	if charged.Declined != "" {
		// Nothing was taken, so give the items back. A send, since
		// nothing here needs to wait for it.
		release := &inventoryv1.Release{Order: order, Sku: place.Sku, Qty: place.Qty}
		if err := d.stock.Release(ctx, p, release); err != nil {
			p.Log().Error("release failed", "order", order, "err", err)
		}
		return &ordersv1.Placed{Order: order, Refused: charged.Declined}, nil
	}
	return &ordersv1.Placed{Order: order, Receipt: charged.Receipt, Left: reserved.Left}, nil
}

// tree places the other services' processes once, from the configuration,
// and every desk the supervisor starts gets the same addresses.
func tree(cfg platform.Config, prices Prices) actor.ChildSpec {
	stock := inventoryv1.Stock(cfg.Where(inventoryv1.Service))
	cashier := paymentsv1.Cashier(cfg.Where(paymentsv1.Service))
	return actor.ChildSupervisor(ordersv1.Service, actor.Spec{
		Children: []actor.ChildSpec{
			actor.Child(ordersv1.DeskName, func() *desk {
				return &desk{stock: stock, cashier: cashier, prices: prices}
			}),
		},
	})
}

// Module registers the prices and adds the tree to the node's root.
func Module(s *di.Scope) {
	s.Value(Prices{"apple": 120, "pear": 150})
	s.Wire[actor.ChildSpec](tree).Group()
}
