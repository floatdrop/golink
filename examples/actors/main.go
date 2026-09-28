// Actors: a struct with its dependencies and a method per kind of message,
// instead of a receive loop.
package main

import (
	"context"
	"errors"
	"fmt"
	"log"

	"google.golang.org/protobuf/proto"

	"github.com/floatdrop/grpcproc"
	"github.com/floatdrop/grpcproc/actor"
	"github.com/floatdrop/grpcproc/examples/shoppb"
)

// Ledger records reservations. It stands for whatever the actor depends on:
// a repository, a client, a config, built by a constructor or a DI container.
type Ledger struct{}

func (Ledger) Record(r *shoppb.Reserve) { fmt.Println("ledger:", r.Qty, r.Sku) }

// Inventory is an actor whose mailbox holds *shoppb.Stock. It implements
// actor.Handler, and whichever of the optional interfaces it needs:
// CallHandler, DownHandler, Initializer, Terminator.
type Inventory struct {
	ledger  *Ledger
	left    map[string]int64
	waiting []grpcproc.Msg[*shoppb.Stock] // reservations parked until a restock
}

// Init runs on the actor's goroutine before the first message.
func (i *Inventory) Init(*grpcproc.Process[*shoppb.Stock]) error {
	i.left = map[string]int64{}
	return nil
}

// HandleCall gets what was sent with Call. The returned message, or error,
// is the reply, and the actor carries on either way.
func (i *Inventory) HandleCall(_ *grpcproc.Process[*shoppb.Stock], m grpcproc.Msg[*shoppb.Stock]) (proto.Message, error) {
	r := m.Body.GetReserve()
	switch {
	case r == nil:
		return nil, errors.New("only reservations are calls")
	case r.Qty <= 0:
		return nil, fmt.Errorf("cannot reserve %d", r.Qty)
	case i.left[r.Sku] < r.Qty:
		// Not enough yet: keep the call and answer it from HandleMessage,
		// when stock arrives. The caller just waits.
		i.waiting = append(i.waiting, m)
		return nil, actor.ErrNoReply
	}
	return i.reserve(r), nil
}

// HandleMessage gets what was sent with Send; nobody waits for an answer.
// An error ends the actor, with the error as its exit reason, and
// actor.ErrStop ends it normally.
func (i *Inventory) HandleMessage(_ *grpcproc.Process[*shoppb.Stock], m grpcproc.Msg[*shoppb.Stock]) error {
	r := m.Body.GetRestock()
	if r == nil {
		return errors.New("a reservation must be a call")
	}
	i.left[r.Sku] += r.Qty
	parked := i.waiting
	i.waiting = nil
	for _, c := range parked {
		if res := c.Body.GetReserve(); i.left[res.Sku] >= res.Qty {
			_ = c.Reply(i.reserve(res), nil)
		} else {
			i.waiting = append(i.waiting, c)
		}
	}
	return nil
}

// Terminate runs however the actor ends. Calls it never answered fail with
// grpcproc.ErrNoProc on their own.
func (*Inventory) Terminate(_ *grpcproc.Process[*shoppb.Stock], err error) {
	fmt.Println("inventory stopped:", err)
}

func (i *Inventory) reserve(r *shoppb.Reserve) *shoppb.Reserved {
	i.left[r.Sku] -= r.Qty
	i.ledger.Record(r)
	return &shoppb.Reserved{Sku: r.Sku, Left: i.left[r.Sku]}
}

func main() {
	ctx := context.Background()
	node, err := grpcproc.NewNode(grpcproc.Config{Name: "shop", Resolver: grpcproc.StaticResolver{}})
	if err != nil {
		log.Fatal(err)
	}
	defer func() { _ = node.Stop(ctx) }()

	inventory, err := node.Spawn(actor.Run(&Inventory{ledger: &Ledger{}}), grpcproc.WithName("inventory"))
	if err != nil {
		log.Fatal(err)
	}

	// Nothing is in stock, so this call is parked in the actor until the
	// restock below arrives.
	reserved := make(chan *shoppb.Reserved)
	go func() {
		r, err := node.Call[*shoppb.Reserved](ctx, inventory, reserve("apple", 2))
		if err != nil {
			log.Fatal(err)
		}
		reserved <- r
	}()
	if err := node.Send(ctx, inventory, restock("apple", 5)); err != nil {
		log.Fatal(err)
	}
	fmt.Println("reserved, left:", (<-reserved).Left)

	// A handler's error goes back to the caller; the actor carries on.
	_, err = node.Call[*shoppb.Reserved](ctx, inventory, reserve("apple", -1))
	fmt.Println("reserve failed:", err)
	r, err := node.Call[*shoppb.Reserved](ctx, inventory, reserve("apple", 1))
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("reserved, left:", r.Left)

	// A reservation sent without waiting for the answer breaks the
	// protocol: HandleMessage fails, and the actor exits. The call behind
	// it in the mailbox is never handled.
	if err := node.Send(ctx, inventory, reserve("apple", 1)); err != nil {
		log.Fatal(err)
	}
	_, err = node.Call[*shoppb.Reserved](ctx, inventory, reserve("apple", 1))
	fmt.Println("then:", err)
}

func reserve(sku string, qty int64) *shoppb.Stock {
	return &shoppb.Stock{Op: &shoppb.Stock_Reserve{Reserve: &shoppb.Reserve{Sku: sku, Qty: qty}}}
}

func restock(sku string, qty int64) *shoppb.Stock {
	return &shoppb.Stock{Op: &shoppb.Stock_Restock{Restock: &shoppb.Restock{Sku: sku, Qty: qty}}}
}
