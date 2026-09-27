// Command billing is the node that takes payments.
package main

import (
	"github.com/floatdrop/grpcproc/examples/guide/internal/payments"
	"github.com/floatdrop/grpcproc/examples/guide/internal/platform"
)

func main() { platform.Run(payments.Module) }
