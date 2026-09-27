// Command warehouse is the node that keeps the stock.
package main

import (
	"github.com/floatdrop/grpcproc/examples/guide/internal/inventory"
	"github.com/floatdrop/grpcproc/examples/guide/internal/platform"
)

func main() { platform.Run(inventory.Module) }
