// Supervisors: an actor that crashes is started again, from a clean state,
// under the same name.
package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"maps"
	"sync"
	"time"

	"google.golang.org/protobuf/proto"

	"github.com/floatdrop/grpcproc"
	"github.com/floatdrop/grpcproc/actor"
	"github.com/floatdrop/grpcproc/examples/shoppb"
)

// Ledger stands for a database. What must outlive a crash lives outside the
// actor, and the actor loads it when it starts.
type Ledger struct {
	mu   sync.Mutex
	left map[string]int64
}

func (l *Ledger) Load() map[string]int64 {
	l.mu.Lock()
	defer l.mu.Unlock()
	return maps.Clone(l.left)
}

func (l *Ledger) Save(sku string, left int64) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.left[sku] = left
}

// Inventory is the supervised actor.
type Inventory struct {
	ledger *Ledger
	left   map[string]int64
}

func (i *Inventory) Init(*grpcproc.Process[*shoppb.Stock]) error {
	i.left = i.ledger.Load()
	return nil
}

func (i *Inventory) HandleMessage(_ *grpcproc.Process[*shoppb.Stock], m grpcproc.Msg[*shoppb.Stock]) error {
	r := m.Body.GetRestock()
	if r.GetQty() <= 0 {
		// A bad message, or a bug: the error ends the actor with it as the
		// reason, and the supervisor starts a new one.
		return fmt.Errorf("bad restock of %d %s", r.GetQty(), r.GetSku())
	}
	i.left[r.Sku] += r.Qty
	i.ledger.Save(r.Sku, i.left[r.Sku])
	return nil
}

func (i *Inventory) HandleCall(_ *grpcproc.Process[*shoppb.Stock], m grpcproc.Msg[*shoppb.Stock]) (proto.Message, error) {
	r := m.Body.GetReserve()
	if i.left[r.GetSku()] < r.GetQty() {
		return nil, errors.New("not enough stock")
	}
	i.left[r.Sku] -= r.Qty
	i.ledger.Save(r.Sku, i.left[r.Sku])
	return &shoppb.Reserved{Sku: r.Sku, Left: i.left[r.Sku]}, nil
}

func main() {
	ctx := context.Background()
	node, err := grpcproc.NewNode(grpcproc.Config{Name: "shop", Resolver: grpcproc.StaticResolver{}})
	if err != nil {
		log.Fatal(err)
	}
	defer func() { _ = node.Stop(ctx) }()

	ledger := &Ledger{left: map[string]int64{}}
	sup, err := actor.Supervise(node, actor.Spec{
		// Restart only the child that exited. OneForAll restarts every
		// child; RestForOne, the child and those started after it.
		Strategy: actor.OneForOne,
		// More restarts than this and the supervisor gives up: it exits
		// with "max restarts", and its own supervisor, if any, restarts it.
		MaxRestarts: 3,
		Within:      time.Minute,
		Children: []actor.ChildSpec{
			// A child is registered under its name. Child builds a new
			// handler for every start, so a restart never sees the state
			// that crashed. ChildFunc runs a plain process function, and
			// ChildSupervisor nests another Spec.
			actor.Child("inventory", func() *Inventory { return &Inventory{ledger: ledger} }),
		},
	}, grpcproc.WithName("supervisor"))
	if err != nil {
		log.Fatal(err)
	}

	// The name reaches whichever process currently runs the child.
	inventory := grpcproc.Named[*shoppb.Stock]("shop", "inventory")
	if err := node.Send(ctx, inventory, restock("apple", 5)); err != nil {
		log.Fatal(err)
	}
	r, err := node.Call[*shoppb.Reserved](ctx, inventory, reserve("apple", 2))
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("reserved, left:", r.Left)

	// Crash it, and follow what the supervisor does through node events.
	events := node.Subscribe(ctx, 16)
	if err := node.Send(ctx, inventory, restock("apple", 0)); err != nil {
		log.Fatal(err)
	}
	for e := range events {
		if e.Process.Name != "inventory" {
			continue
		}
		if e.Kind == grpcproc.EventExit {
			fmt.Println("inventory exited:", e.Reason)
		}
		if e.Kind == grpcproc.EventSpawn {
			fmt.Println("inventory started again")
			break
		}
	}

	// Same name, new process, state loaded back from the ledger.
	r, err = node.Call[*shoppb.Reserved](ctx, inventory, reserve("apple", 1))
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("reserved, left:", r.Left)

	// The supervisor publishes its state through WithInspect, which is what
	// the Inspector and grpcprocctl show.
	state, err := node.Inspect(ctx, sup)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("supervisor restarts:", state["restarts"])
}

func reserve(sku string, qty int64) *shoppb.Stock {
	return &shoppb.Stock{Op: &shoppb.Stock_Reserve{Reserve: &shoppb.Reserve{Sku: sku, Qty: qty}}}
}

func restock(sku string, qty int64) *shoppb.Stock {
	return &shoppb.Stock{Op: &shoppb.Stock_Restock{Restock: &shoppb.Restock{Sku: sku, Qty: qty}}}
}
