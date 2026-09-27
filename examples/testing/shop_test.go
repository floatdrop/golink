package shop

import (
	"errors"
	"testing"

	"github.com/floatdrop/golink"
	"github.com/floatdrop/golink/examples/shoppb"
	"github.com/floatdrop/golink/golinktest"
)

func TestReserveAcrossNodes(t *testing.T) {
	// Two nodes over in-memory gRPC connections, stopped when the test ends.
	c := golinktest.New(t, "shop", "warehouse")
	shop := c.Node("shop")
	if _, err := golink.Spawn(c.Node("warehouse"), Stock(map[string]int64{"apple": 3}), golink.WithName("stock")); err != nil {
		t.Fatal(err)
	}
	stock := golink.Named[*shoppb.Reserve]("warehouse", "stock")
	apple := &shoppb.Reserve{Sku: "apple", Qty: 1}

	if r, err := shop.Call[*shoppb.Reserved](t.Context(), stock, apple); err != nil || r.Left != 2 {
		t.Fatal(r, err)
	}

	// A partition fails calls, and fires monitors with Down{noconnection},
	// until it heals.
	c.Partition("shop", "warehouse")
	if _, err := shop.Call[*shoppb.Reserved](t.Context(), stock, apple); !errors.Is(err, golink.ErrNoConnection) {
		t.Fatal(err)
	}
	c.Heal("shop", "warehouse")
	if r, err := shop.Call[*shoppb.Reserved](t.Context(), stock, apple); err != nil || r.Left != 1 {
		t.Fatal(r, err)
	}

	// A crash, and a restart: same node name, new incarnation, and none of
	// the processes the old one ran.
	c.Kill("warehouse")
	c.Restart("warehouse")
	if _, err := shop.Call[*shoppb.Reserved](t.Context(), stock, apple); !errors.Is(err, golink.ErrNoProc) {
		t.Fatal(err)
	}
}
