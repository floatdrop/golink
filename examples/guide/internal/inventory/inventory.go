// Package inventory is the service that keeps stock. Its process, stock,
// reserves items and takes them back; the levels live in a Store, so a
// crash of the process loses none of them. The package exports its Module
// and the Store it depends on, and nothing else.
package inventory

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"sync"

	"golang.yandex/di"
	"google.golang.org/protobuf/proto"

	"github.com/floatdrop/grpcproc"
	"github.com/floatdrop/grpcproc/actor"
	inventoryv1 "github.com/floatdrop/grpcproc/examples/guide/proto/inventory/v1"
)

// Store keeps the stock levels outside the process that serves them, so a
// restart loads them back. The guide keeps them in memory; a real Store is
// a database, which the container builds, starts and stops the same way.
type Store interface {
	Levels(ctx context.Context) (map[string]int64, error)
	Save(ctx context.Context, sku string, left int64) error
}

type memory struct {
	mu     sync.Mutex
	levels map[string]int64
}

func newMemory() *memory { return &memory{levels: map[string]int64{"apple": 10, "pear": 4}} }

func (m *memory) Levels(context.Context) (map[string]int64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return maps.Clone(m.levels), nil
}

func (m *memory) Save(_ context.Context, sku string, left int64) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.levels[sku] = left
	return nil
}

// stock is the actor. The supervisor builds a new one for every start of
// the process, so a restart begins from the Store, not from what crashed.
type stock struct {
	store Store
	left  map[string]int64
}

// Init runs before the first message: the levels come from the Store.
func (s *stock) Init(p *grpcproc.Process[*inventoryv1.Command]) (err error) {
	s.left, err = s.store.Levels(p.Context())
	return err
}

// HandleCall reserves. A refusal is part of the answer; an error, the
// caller's to see, means the stock failed. Either way the process carries
// on.
func (s *stock) HandleCall(p *grpcproc.Process[*inventoryv1.Command], m grpcproc.Msg[*inventoryv1.Command]) (proto.Message, error) {
	r := m.Body.GetReserve()
	switch {
	case r == nil:
		return nil, errors.New("inventory: only a reservation is a call")
	case r.Qty <= 0:
		return &inventoryv1.Reserved{Refused: fmt.Sprintf("cannot reserve %d", r.Qty)}, nil
	case s.left[r.Sku] < r.Qty:
		return &inventoryv1.Reserved{Refused: fmt.Sprintf("%d %s left, %d asked for", s.left[r.Sku], r.Sku, r.Qty)}, nil
	}
	if err := s.set(p.Context(), r.Sku, s.left[r.Sku]-r.Qty); err != nil {
		return nil, err
	}
	return &inventoryv1.Reserved{Left: s.left[r.Sku]}, nil
}

// HandleMessage takes back what a reservation took. It is a send: nobody
// waits for it. A reservation sent without a call is the sender's bug, and
// is logged and dropped: bad input from another service is no reason to
// crash. An error from the store is, and ends the process; its supervisor
// starts a new one, which loads the levels back.
func (s *stock) HandleMessage(p *grpcproc.Process[*inventoryv1.Command], m grpcproc.Msg[*inventoryv1.Command]) error {
	r := m.Body.GetRelease()
	if r == nil {
		p.Log().Warn("inventory: a reservation must be a call", "from", m.From)
		return nil
	}
	return s.set(p.Context(), r.Sku, s.left[r.Sku]+r.Qty)
}

func (s *stock) set(ctx context.Context, sku string, left int64) error {
	if err := s.store.Save(ctx, sku, left); err != nil {
		return err
	}
	s.left[sku] = left
	return nil
}

// tree is the service's supervision tree. The actor's dependencies come
// from the container; Child builds a fresh actor around them at every start.
func tree(store Store) actor.ChildSpec {
	return actor.ChildSupervisor(inventoryv1.Service, actor.Spec{
		Children: []actor.ChildSpec{
			actor.Child(inventoryv1.StockName, func() *stock { return &stock{store: store} }),
		},
	})
}

// Module registers the store and adds the tree to the node's root.
func Module(s *di.Scope) {
	s.Wire[Store](newMemory)
	s.Wire[actor.ChildSpec](tree).Group()
}
