package main

import (
	"context"
	"testing"
	"testing/synctest"

	"github.com/floatdrop/grpcproc"
	"github.com/floatdrop/grpcproc/actor"
	"github.com/floatdrop/grpcproc/examples/shoppb"
)

// A reservation with nothing in stock waits for a restock. Inside a
// synctest bubble the test can say so without a timeout: synctest.Wait
// returns once every process waits, so a call not answered by then is
// parked, not slow.
func TestReservationWaitsForRestock(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		node, err := grpcproc.NewNode(grpcproc.Config{Name: "test", Resolver: grpcproc.StaticResolver{}})
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = node.Stop(context.Background()) })
		addr, err := node.Spawn(actor.Run(&Inventory{ledger: &Ledger{}}))
		if err != nil {
			t.Fatal(err)
		}
		inventory := shoppb.StockAddr{Addr: addr}

		reserved := make(chan *shoppb.Reserved, 1)
		go func() {
			r, err := inventory.Reserve(t.Context(), node, &shoppb.Reserve{Sku: "apple", Qty: 2})
			if err != nil {
				t.Error(err)
				return
			}
			reserved <- r
		}()
		synctest.Wait()
		if len(reserved) != 0 {
			t.Fatal("a reservation was answered with nothing in stock")
		}

		if err := inventory.Restock(t.Context(), node, &shoppb.Restock{Sku: "apple", Qty: 5}); err != nil {
			t.Fatal(err)
		}
		synctest.Wait()
		if r := <-reserved; r.Left != 3 {
			t.Fatalf("left %d after the restock, want 3", r.Left)
		}
	})
}
