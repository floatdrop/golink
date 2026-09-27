package inventoryv1

import "github.com/floatdrop/grpcproc"

const (
	// Service is the inventory service's name in a deployment's placement.
	Service = "inventory"
	// StockName is the name its stock process is registered under.
	StockName = "stock"
)

// Stock addresses the stock process on node, the node that runs the
// inventory service. The name it is registered under is part of the
// contract, as much as the messages it accepts.
func Stock(node string) grpcproc.Addr[*Command] {
	return grpcproc.Named[*Command](node, StockName)
}
