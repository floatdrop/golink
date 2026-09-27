// Command local runs the whole shop as one program, the way a developer
// runs it: one node, every service on it. It composes the same modules that
// cmd/front, cmd/warehouse and cmd/billing split between three nodes.
package main

import (
	"github.com/floatdrop/grpcproc/examples/guide/internal/inventory"
	"github.com/floatdrop/grpcproc/examples/guide/internal/orders"
	"github.com/floatdrop/grpcproc/examples/guide/internal/payments"
	"github.com/floatdrop/grpcproc/examples/guide/internal/platform"
	"github.com/floatdrop/grpcproc/examples/guide/internal/web"
)

func main() {
	platform.Run(inventory.Module, payments.Module, orders.Module, web.Module)
}
