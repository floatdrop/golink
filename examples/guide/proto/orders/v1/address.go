package ordersv1

import "github.com/floatdrop/grpcproc"

const (
	// Service is the orders service's name in a deployment's placement.
	Service = "orders"
	// DeskName is the name its desk process is registered under.
	DeskName = "desk"
)

// Desk addresses the desk process on node, the node that runs the orders
// service.
func Desk(node string) grpcproc.Addr[*Place] {
	return grpcproc.Named[*Place](node, DeskName)
}
