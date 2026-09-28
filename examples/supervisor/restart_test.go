package main

import (
	"context"
	"testing"
	"testing/synctest"
	"time"

	"github.com/floatdrop/grpcproc"
	"github.com/floatdrop/grpcproc/actor"
	"github.com/floatdrop/grpcproc/examples/shoppb"
)

// The tree restarts the inventory 3 times a minute. Inside a synctest
// bubble a minute is virtual: time.Sleep spends it at once, and
// synctest.Wait returns once the restart it caused has settled.
func TestRestartsThreeTimesAMinute(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		node, err := grpcproc.NewNode(grpcproc.Config{Name: "shop", Resolver: grpcproc.StaticResolver{}})
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = node.Stop(context.Background()) })
		sup, err := actor.Supervise(node, tree(&Ledger{left: map[string]int64{}}))
		if err != nil {
			t.Fatal(err)
		}
		inventory := shoppb.StockAddr{Addr: grpcproc.Named[*shoppb.Stock]("shop", "inventory")}
		crash := func() { // a restock of zero is a bug, and ends the inventory
			_ = inventory.Restock(t.Context(), node, &shoppb.Restock{Sku: "apple", Qty: 0})
			synctest.Wait()
		}
		supervising := func() bool { _, ok := node.Process(sup); return ok }

		// A crash a minute never makes two restarts in one.
		for range 5 {
			crash()
			time.Sleep(time.Minute)
		}
		if !supervising() {
			t.Fatal("the supervisor gave up on a crash a minute")
		}

		// Four in a row are one restart too many.
		for range 4 {
			crash()
		}
		if supervising() {
			t.Fatal("the supervisor outlived four crashes in a minute")
		}
	})
}
