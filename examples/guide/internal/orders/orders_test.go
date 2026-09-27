package orders_test

import (
	"log/slog"
	"testing"
	"time"

	"golang.yandex/di"

	"github.com/floatdrop/grpcproc"
	"github.com/floatdrop/grpcproc/examples/guide/internal/orders"
	"github.com/floatdrop/grpcproc/examples/guide/internal/platform"
	inventoryv1 "github.com/floatdrop/grpcproc/examples/guide/proto/inventory/v1"
	ordersv1 "github.com/floatdrop/grpcproc/examples/guide/proto/orders/v1"
	paymentsv1 "github.com/floatdrop/grpcproc/examples/guide/proto/payments/v1"
)

// The desk alone. Its collaborators are addresses, so a test puts fakes
// behind them: processes registered under the names the contracts give, on
// the node the placement points at, which with none is this one.
func desk(t *testing.T, cashier func(*grpcproc.Process[*paymentsv1.Charge]) error) (*grpcproc.Node, <-chan *inventoryv1.Release) {
	t.Helper()
	s := di.Test(t)
	platform.Compose(s, platform.Config{Node: "shop", Listen: "127.0.0.1:0"}, slog.New(slog.DiscardHandler), orders.Module)
	if err := s.Start(t.Context()); err != nil {
		t.Fatal(err)
	}
	n := s.Get[*grpcproc.Node]()
	released := make(chan *inventoryv1.Release, 1)
	stock := func(p *grpcproc.Process[*inventoryv1.Command]) error {
		for {
			m, err := p.Receive()
			if err != nil {
				return err
			}
			if m.IsCall() {
				_ = p.Reply(m, &inventoryv1.Reserved{Left: 8}, nil)
				continue
			}
			released <- m.Body.GetRelease()
		}
	}
	if _, err := n.Spawn(stock, grpcproc.WithName(inventoryv1.StockName)); err != nil {
		t.Fatal(err)
	}
	if _, err := n.Spawn(cashier, grpcproc.WithName(paymentsv1.CashierName)); err != nil {
		t.Fatal(err)
	}
	return n, released
}

func place(t *testing.T, n *grpcproc.Node) (*ordersv1.Placed, error) {
	return n.Call[*ordersv1.Placed](t.Context(), ordersv1.Desk("shop"), &ordersv1.Place{Sku: "apple", Qty: 2, Card: "4242"})
}

func TestADeclinedCardGivesTheItemsBack(t *testing.T) {
	n, released := desk(t, func(p *grpcproc.Process[*paymentsv1.Charge]) error {
		for {
			m, err := p.Receive()
			if err != nil {
				return err
			}
			_ = p.Reply(m, &paymentsv1.Charged{Declined: "declined"}, nil)
		}
	})
	if placed, err := place(t, n); err != nil || placed.Refused != "declined" {
		t.Fatalf("%v %v", placed, err)
	}
	select {
	case r := <-released:
		if r.GetSku() != "apple" || r.GetQty() != 2 {
			t.Fatalf("released %v", r)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("nothing given back")
	}
}

// A cashier that takes the charge and answers nothing: the card may have
// been charged, so the desk fails the order and keeps the items reserved.
func TestAChargeWithNoAnswerKeepsTheItems(t *testing.T) {
	n, released := desk(t, func(p *grpcproc.Process[*paymentsv1.Charge]) error {
		_, err := p.Receive()
		return err // exits without a reply: the caller learns it is gone
	})
	if placed, err := place(t, n); err == nil {
		t.Fatalf("placed %v", placed)
	}
	select {
	case r := <-released:
		t.Fatalf("released %v", r)
	case <-time.After(100 * time.Millisecond):
	}
}
