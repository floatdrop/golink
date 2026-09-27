// Command front is the node that faces customers: the web front and the
// orders desk. Its configuration places the inventory and the payments on
// the other two nodes.
package main

import (
	"github.com/floatdrop/grpcproc/examples/guide/internal/orders"
	"github.com/floatdrop/grpcproc/examples/guide/internal/platform"
	"github.com/floatdrop/grpcproc/examples/guide/internal/web"
)

func main() { platform.Run(orders.Module, web.Module) }
