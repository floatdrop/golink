// Package payments is the service that takes payments. Its cashier answers
// charges through a Gateway, the payment provider's client, which the
// container builds. The package exports its Module and the Gateway.
package payments

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync/atomic"

	"golang.yandex/di"
	"google.golang.org/protobuf/proto"

	"github.com/floatdrop/grpcproc"
	"github.com/floatdrop/grpcproc/actor"
	paymentsv1 "github.com/floatdrop/grpcproc/examples/guide/proto/payments/v1"
)

// Gateway charges cards. A card the provider refuses is ErrDeclined; any
// other error is a failure, after which the card may or may not have been
// charged.
type Gateway interface {
	Charge(ctx context.Context, card string, amount int64) (receipt string, err error)
}

var ErrDeclined = errors.New("card declined")

// sandbox is the provider's test mode: it declines cards that start with
// 4000 and charges every other.
type sandbox struct{ receipts atomic.Int64 }

func newSandbox() *sandbox { return &sandbox{} }

func (s *sandbox) Charge(_ context.Context, card string, amount int64) (string, error) {
	if strings.HasPrefix(card, "4000") {
		return "", ErrDeclined
	}
	return fmt.Sprintf("receipt-%d-%d", s.receipts.Add(1), amount), nil
}

// cashier only answers calls. CallsOnly is its HandleMessage, which logs and
// drops anything sent to it without one.
type cashier struct {
	actor.CallsOnly[*paymentsv1.Charge]
	gateway Gateway
}

func (c *cashier) HandleCall(p *grpcproc.Process[*paymentsv1.Charge], m grpcproc.Msg[*paymentsv1.Charge]) (proto.Message, error) {
	receipt, err := c.gateway.Charge(p.Context(), m.Body.Card, m.Body.Amount)
	if errors.Is(err, ErrDeclined) {
		return &paymentsv1.Charged{Declined: err.Error()}, nil
	}
	if err != nil {
		return nil, err
	}
	p.Log().Info("charged", "order", m.Body.Order, "amount", m.Body.Amount)
	return &paymentsv1.Charged{Receipt: receipt}, nil
}

func tree(g Gateway) actor.ChildSpec {
	return actor.ChildSupervisor(paymentsv1.Service, actor.Spec{
		Children: []actor.ChildSpec{
			actor.Child(paymentsv1.CashierName, func() *cashier { return &cashier{gateway: g} }),
		},
	})
}

// Module registers the gateway and adds the tree to the node's root.
func Module(s *di.Scope) {
	s.Wire[Gateway](newSandbox)
	s.Wire[actor.ChildSpec](tree).Group()
}
