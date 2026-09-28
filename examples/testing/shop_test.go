package shop

import (
	"errors"
	"testing"

	"github.com/floatdrop/grpcproc"
	"github.com/floatdrop/grpcproc/examples/shoppb"
	"github.com/floatdrop/grpcproc/grpcproctest"
)

func TestReserveAcrossNodes(t *testing.T) {
	// Two nodes over in-memory gRPC connections, stopped when the test ends.
	c := grpcproctest.New(t, "shop", "warehouse")
	shop := c.Node("shop")
	if _, err := c.Node("warehouse").Spawn(Stock(map[string]int64{"apple": 3}), grpcproc.WithName("stock")); err != nil {
		t.Fatal(err)
	}
	stock := grpcproc.Named[*shoppb.Reserve]("warehouse", "stock")
	apple := &shoppb.Reserve{Sku: "apple", Qty: 1}

	if r, err := stock.Call[*shoppb.Reserved](t.Context(), shop, apple); err != nil || r.Left != 2 {
		t.Fatal(r, err)
	}

	// A partition fails calls, and fires monitors with Down{noconnection},
	// until it heals.
	c.Partition("shop", "warehouse")
	if _, err := stock.Call[*shoppb.Reserved](t.Context(), shop, apple); !errors.Is(err, grpcproc.ErrNoConnection) {
		t.Fatal(err)
	}
	c.Heal("shop", "warehouse")
	if r, err := stock.Call[*shoppb.Reserved](t.Context(), shop, apple); err != nil || r.Left != 1 {
		t.Fatal(r, err)
	}

	// A crash, and a restart: same node name, new incarnation, and none of
	// the processes the old one ran.
	c.Kill("warehouse")
	c.Restart("warehouse")
	if _, err := stock.Call[*shoppb.Reserved](t.Context(), shop, apple); !errors.Is(err, grpcproc.ErrNoProc) {
		t.Fatal(err)
	}
}
