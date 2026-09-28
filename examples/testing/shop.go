// Package shop is code under test: the stock process of the quick start.
package shop

import (
	"fmt"

	"github.com/floatdrop/grpcproc"
	"github.com/floatdrop/grpcproc/examples/shoppb"
)

// Stock serves reservations from left.
func Stock(left map[string]int64) func(*grpcproc.Process[*shoppb.Reserve]) error {
	return func(p *grpcproc.Process[*shoppb.Reserve]) error {
		for {
			m, err := p.Receive()
			if err != nil {
				return err
			}
			if left[m.Body.Sku] < m.Body.Qty {
				_ = m.Reply(nil, fmt.Errorf("only %d %s left", left[m.Body.Sku], m.Body.Sku))
				continue
			}
			left[m.Body.Sku] -= m.Body.Qty
			_ = m.Reply(&shoppb.Reserved{Sku: m.Body.Sku, Left: left[m.Body.Sku]}, nil)
		}
	}
}
