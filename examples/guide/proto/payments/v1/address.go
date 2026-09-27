package paymentsv1

import "github.com/floatdrop/grpcproc"

const (
	// Service is the payments service's name in a deployment's placement.
	Service = "payments"
	// CashierName is the name its cashier process is registered under.
	CashierName = "cashier"
)

// Cashier addresses the cashier process on node, the node that runs the
// payments service.
func Cashier(node string) grpcproc.Addr[*Charge] {
	return grpcproc.Named[*Charge](node, CashierName)
}
