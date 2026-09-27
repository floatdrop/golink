// Package shop is code under test: the stock process of the quick start.
package shop

import (
	"fmt"

	"github.com/floatdrop/golink"
	"github.com/floatdrop/golink/examples/shoppb"
)

// Stock serves reservations from left.
func Stock(left map[string]int64) func(*golink.Process[*shoppb.Reserve]) error {
	return func(p *golink.Process[*shoppb.Reserve]) error {
		for {
			m, err := p.Receive()
			if err != nil {
				return err
			}
			if left[m.Body.Sku] < m.Body.Qty {
				_ = p.Reply(m, nil, fmt.Errorf("only %d %s left", left[m.Body.Sku], m.Body.Sku))
				continue
			}
			left[m.Body.Sku] -= m.Body.Qty
			_ = p.Reply(m, &shoppb.Reserved{Sku: m.Body.Sku, Left: left[m.Body.Sku]}, nil)
		}
	}
}
