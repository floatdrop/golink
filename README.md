# grpcproc

Erlang-style processes for Go, on the gRPC server you already run.

A process is a goroutine with a mailbox and a cluster-wide PID. `Send`,
`Call`, `Monitor` and `Exit` work the same whether the target is in this
binary or on another node; the node-to-node traffic is one gRPC stream per
direction, registered on your `*grpc.Server` next to your other services.

- **A library, not a framework.** You bring the gRPC server, credentials,
  discovery, `slog` and lifecycle. grpcproc never opens a listener, reads env
  vars, installs globals or starts a goroutine outside `Start`/`Stop`.
- **Typed mailboxes.** The message type lives on the address, so a send to
  `Addr[*shoppb.Reserve]` is checked by the compiler, locally and remotely.
  Instantiate with `proto.Message` for an untyped process.
- **Erlang semantics.** Per-sender ordering, `Down` in order with messages,
  incarnation in the PID so a restart never resurrects a reference,
  unbounded mailboxes so a slow process cannot stall the link.
- **Inspectable.** Every process and link keeps counters you can snapshot;
  a process can publish what it currently believes through `WithInspect`;
  one `Hooks` interface taps everything for metrics, tracing and dead letters.
- **Core depends on `grpc` and `protobuf` only.**

```sh
go get github.com/floatdrop/grpcproc
```

Requires **Go 1.27** (generic methods).

## Quick start

Two nodes, a typed process on one, called and monitored from the other:

[embedmd]:# (examples/quickstart/main.go go)
```go
// Quick start: two nodes, a typed process on one, called and monitored from
// the other. Each node is normally its own service; here both run in one
// binary, on loopback.
package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/proto"

	"github.com/floatdrop/grpcproc"
	"github.com/floatdrop/grpcproc/examples/shoppb"
)

func main() {
	ctx := context.Background()

	// Where each node's gRPC server listens: a static address book here,
	// grpcproc/etcd in a real cluster.
	warehouseLis := listen()
	shopLis := listen()
	peers := grpcproc.StaticResolver{
		"warehouse": warehouseLis.Addr().String(),
		"shop":      shopLis.Addr().String(),
	}

	warehouse, stopWarehouse := serve(ctx, "warehouse", warehouseLis, peers)
	defer stopWarehouse()
	shop, stopShop := serve(ctx, "shop", shopLis, peers)
	defer stopShop()

	// A process on warehouse. Its mailbox holds *shoppb.Reserve and
	// nothing else; it answers each call with *shoppb.Reserved.
	_, err := warehouse.Spawn(func(p *grpcproc.Process[*shoppb.Reserve]) error {
		left := map[string]int64{"apple": 3}
		for {
			m, err := p.Receive()
			if err != nil {
				return err // asked to exit, or the node is stopping
			}
			if left[m.Body.Sku] < m.Body.Qty {
				_ = p.Reply(m, nil, fmt.Errorf("only %d %s left", left[m.Body.Sku], m.Body.Sku))
				continue
			}
			left[m.Body.Sku] -= m.Body.Qty
			_ = p.Reply(m, &shoppb.Reserved{Sku: m.Body.Sku, Left: left[m.Body.Sku]}, nil)
		}
	}, grpcproc.WithName("stock"))
	if err != nil {
		log.Fatal(err)
	}

	// From shop, the process is a node name and a process name. The address
	// carries the mailbox type, so the compiler checks what is sent to it.
	stock := grpcproc.Named[*shoppb.Reserve]("warehouse", "stock")
	for range 2 {
		r, err := shop.Call[*shoppb.Reserved](ctx, stock, &shoppb.Reserve{Sku: "apple", Qty: 2})
		if err != nil {
			fmt.Println("reserve failed:", err) // the handler's error, as a *grpcproc.RemoteError
			continue
		}
		fmt.Println("reserved, left:", r.Left)
	}

	// A process on shop monitors stock, then asks it to exit. The Down
	// arrives with the reason, as it would for a crash or a lost node. It
	// expects no messages, so its mailbox is untyped: proto.Message.
	done := make(chan grpcproc.Down)
	_, err = shop.Spawn(func(p *grpcproc.Process[proto.Message]) error {
		p.Monitor(stock)
		if err := p.Exit(stock, "closing"); err != nil {
			return err
		}
		for {
			m, err := p.Receive()
			if err != nil {
				return err
			}
			if m.Down != nil {
				done <- *m.Down
				return nil
			}
		}
	})
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("stock exited:", (<-done).Reason)

	_, err = shop.Call[*shoppb.Reserved](ctx, stock, &shoppb.Reserve{Sku: "apple", Qty: 1})
	fmt.Println("no such process:", errors.Is(err, grpcproc.ErrNoProc))
}

func listen() net.Listener {
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		log.Fatal(err)
	}
	return lis
}

// serve runs a node on its own gRPC server, the one the service already has
// for its other APIs.
func serve(ctx context.Context, name string, lis net.Listener, peers grpcproc.Resolver) (*grpcproc.Node, func()) {
	node, err := grpcproc.NewNode(grpcproc.Config{
		Name:        name,
		Resolver:    peers,
		DialOptions: []grpc.DialOption{grpc.WithTransportCredentials(insecure.NewCredentials())},
	})
	if err != nil {
		log.Fatal(err)
	}
	srv := grpc.NewServer()
	node.Register(srv) // grpcproc.v1.Node, next to the service's own
	go func() { _ = srv.Serve(lis) }()
	if err := node.Start(ctx); err != nil {
		log.Fatal(err)
	}
	return node, func() {
		if err := node.Stop(ctx); err != nil {
			log.Println(err)
		}
		srv.GracefulStop()
	}
}
```

It prints:

[embedmd]:# (examples/quickstart/output.txt)
```txt
reserved, left: 1
reserve failed: only 1 apple left
stock exited: closing
no such process: true
```

## Actors and supervisors

`grpcproc/actor` adds structure on top of processes, using only the public
API. An actor is a plain struct holding its dependencies, which is what a
constructor or a DI container builds. Instead of a receive loop it has a
method per kind of message; all but `HandleMessage` are optional. An actor
that only answers calls embeds `actor.CallsOnly[M]` in place of it, which
logs and drops anything sent to it without a call:

| Method | Runs for | Its result |
| --- | --- | --- |
| `Init` | once, before the first message | an error ends the actor |
| `HandleMessage` | a message sent with `Send` | an error ends the actor with it as the reason; `actor.ErrStop` ends it normally |
| `HandleCall` | a message sent with `Call` | the reply, a message or an error, and the actor carries on; `actor.ErrNoReply` answers later with `p.Reply` |
| `HandleDown` | a `Down` from a monitor | as for `HandleMessage` |
| `Terminate` | once, when the actor ends, however it ends | none: the exit reason is already decided |

This one keeps stock. A reservation it cannot serve yet is parked and
answered when a restock arrives:

[embedmd]:# (examples/actors/main.go go)
```go
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
func (i *Inventory) HandleMessage(p *grpcproc.Process[*shoppb.Stock], m grpcproc.Msg[*shoppb.Stock]) error {
	r := m.Body.GetRestock()
	if r == nil {
		return errors.New("a reservation must be a call")
	}
	i.left[r.Sku] += r.Qty
	parked := i.waiting
	i.waiting = nil
	for _, c := range parked {
		if res := c.Body.GetReserve(); i.left[res.Sku] >= res.Qty {
			_ = p.Reply(c, i.reserve(res), nil)
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
```

It prints:

[embedmd]:# (examples/actors/output.txt)
```txt
ledger: 2 apple
reserved, left: 3
reserve failed: cannot reserve -1
ledger: 1 apple
reserved, left: 2
inventory stopped: a reservation must be a call
then: grpcproc: no such process
```

A supervisor starts its children in order, monitors them, and restarts
those that exit. Its `Strategy` says which: `OneForOne` only the child that
exited, `OneForAll` every child, `RestForOne` the child and those started
after it. A child's `Restart` says when: `Permanent` always, `Transient`
after an abnormal exit only, `Temporary` never. A supervisor that restarts
more than `MaxRestarts` times `Within` its window gives up and exits with
`max restarts`; its own supervisor, a `ChildSupervisor`, then restarts it,
which is how failure moves up the tree.

Here the inventory crashes on a bad message and comes back under the same
name, with its state loaded from the ledger:

[embedmd]:# (examples/supervisor/main.go go)
```go
// Supervisors: an actor that crashes is started again, from a clean state,
// under the same name.
package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"maps"
	"slices"
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
		if !slices.Contains(e.Process.Names, "inventory") {
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
```

It prints:

[embedmd]:# (examples/supervisor/output.txt)
```txt
reserved, left: 3
inventory exited: bad restock of 0 apple
inventory started again
reserved, left: 2
supervisor restarts: 1/3 in 1m0s
```

Children are monitored from before they run (`p.SpawnMonitor`), so no exit
is missed. A supervisor publishes its children and restart counts through
`WithInspect`, so the Inspector shows the supervision tree's state.

## What a process sees

```go
ref := p.Monitor(stock)                  // Down{Ref: ref} when it exits or its node is unreachable
err := p.Send(stock, &shoppb.Reserve{…}) // compile-time typed
r, err := p.Call[*shoppb.Reserved](ctx, stock, &shoppb.Reserve{…})
err = p.SendTo(m.From, &shoppb.Restock{…}) // untyped: a PID from a message
a, err := p.CallTo[*shoppb.Reserved](ctx, m.From, &shoppb.Reserve{…})

// Node has the same four, and each takes a ctx: its metadata goes with the
// message. It bounds a whole Call, but for Send only the first connection.
```

| Call | Returns |
| --- | --- |
| `p.Receive()` | `Msg[M]{From, Body, Down, Metadata}`; error when asked to exit |
| `p.ReceiveTimeout(d)` | the same, or `context.DeadlineExceeded` |
| `p.Send(to Addr[N], m N)` / `p.SendTo(Target, proto.Message)` | typed / untyped asynchronous send |
| `p.Call[R](ctx, to Addr[N], req N)` / `p.CallTo[R](ctx, Target, proto.Message)` | the reply as `R`, `*RemoteError`, `ErrNoProc` (also when the callee exits before answering), `ErrType` or `ErrNoConnection` |
| `p.Reply(m, resp, err)` | answers a call; may be deferred to another goroutine |
| `p.Monitor(target)` / `p.Demonitor(ref)` | a `Down` with the ref when the target exits: `normal`, the returned error, `panic: …`, `killed`, `noproc`, `noconnection`, `shutdown` |
| `p.Exit(target, reason)` | asks another process to exit; its `Receive` returns an `*ExitError` |
| `p.Log()` | `*slog.Logger` with pid and label; threshold settable at runtime |
| `p.SendAfter(d, to, m)` | a `*Timer`; cancelled if the process exits first; carries the metadata of when it was scheduled |
| `p.Spawn[N](fn, …)` / `p.SpawnMonitor[N](fn, …)` | a child on the same node, with `p` as its parent; the second monitored before it runs |

Exit reasons and the type check on delivery are the whole error model: a
message of the wrong type is a dead letter with reason `type` (and `ErrType`
to a caller), never a panic in the process.

## Observability

```go
node.Processes()            // []ProcessInfo: state, mailbox depth and oldest age, sent/received,
                            // calls in flight, last message type, watchers, monitors, log level
node.Process(pid)
node.Info()                 // NodeInfo: incarnation, uptime, counts, per-link messages/bytes/reconnects
node.Inspect(ctx, pid)      // the map the process's WithInspect returns, produced on its own goroutine
node.SetLogLevel(pid, slog.LevelDebug)
```

`WithInspect` runs between two messages, inside `Receive`, so it reads the
process's state without a lock. A process busy in a handler answers when it
next receives; one that never does reports `busy for 12s`, which is the
diagnosis. With a state machine that is one line:

```go
grpcproc.WithInspect(func() map[string]string { return map[string]string{"state": rec.state.String()} })
```

`node.Subscribe(ctx, buffer)` streams what happens on the node: spawns,
exits with their reasons, links up and down, dead letters. It never blocks
the node; a subscriber that falls behind loses events and is told how many
in the next one's `Missed`.

`Config.Hooks` is the synchronous tap for everything else: `OnSpawn`,
`OnExit`, `OnSend`, `OnReceive`, `OnDeadLetter`, `OnLinkUp`, `OnLinkDown`.
`OnSend` and `OnReceive` return the metadata to use and a `Done` that closes
what they started, which is all a tracer needs. A process's sends inherit the
metadata of the message it is handling, so trace chains form without
threading a context through handlers. `grpcproc.JoinHooks` combines several.

[`grpcproc/otel`](otel/README.md) implements them with OpenTelemetry: spans
for every send, call and handled message, chained across nodes, and metrics
by process label (throughput, mailbox wait and depth, handling and call
latency, exits by reason class, dead letters, link traffic).

## Inspector

`grpcproc/inspect` serves all of the above over gRPC, on the same server:

```go
node.Register(grpcServer)
insp := inspect.New(node, inspect.WithResolver(resolver, dialOptions...)) // reaches other nodes' Inspectors
insp.Register(grpcServer)
defer insp.Close()
```

`grpcproc.inspect.v1.Inspector` has `GetNode`, `ListProcesses` (filter by
name, label, state, mailbox depth), `GetProcess` (snapshot plus what the
process publishes through `WithInspect`), `SetLogLevel`, `Send`, `Exit` and
`Watch`. Every request names a node; one that is not this node is forwarded
to that node's Inspector, so one endpoint inspects the whole cluster.
`inspect.ReadOnly()` refuses the three writes; anything finer is the job of
the interceptors that guard your other services.

[`grpcprocctl`](tools/README.md) is its command line, and serves it to AI
agents over MCP:

```sh
grpcprocctl --plaintext ps --sort mailbox        # who is falling behind
grpcprocctl --plaintext inspect ledger-writer    # what it believes, or that it is busy
grpcprocctl --plaintext dot --cluster | dot -Tsvg -o processes.svg
claude mcp add grpcproc -- grpcprocctl --plaintext --addr 10.0.0.5:9000 mcp
```

It is a plain gRPC service, so `grpcurl` works on it too.

## Discovery

`Config.Resolver` is all a node needs to reach peers; `grpcproc.StaticResolver`
is a map. `Config.Registrar` publishes the node on `Start` and withdraws it
on `Stop`, and `Config.Membership` is the cluster's view of who is alive: a
peer that leaves, or comes back as a new incarnation, has its links dropped,
so monitors fire and pending calls fail even when its connection never
closed. [`grpcproc/etcd`](etcd/README.md) implements all three on etcd leases.

## Testing a cluster

`grpcproctest` runs nodes over in-memory connections, so a multi-node scenario
is a plain `go test`:

[embedmd]:# (examples/testing/shop_test.go go)
```go
package shop

import (
	"errors"
	"testing"

	"github.com/floatdrop/grpcproc"
	"github.com/floatdrop/grpcproc/examples/shoppb"
	"github.com/floatdrop/grpcproc/grpcproctest"
)

func TestReserveAcrossNodes(t *testing.T) {
	// Two nodes over in-memory gRPC connections, stopped when the test ends.
	c := grpcproctest.New(t, "shop", "warehouse")
	shop := c.Node("shop")
	if _, err := c.Node("warehouse").Spawn(Stock(map[string]int64{"apple": 3}), grpcproc.WithName("stock")); err != nil {
		t.Fatal(err)
	}
	stock := grpcproc.Named[*shoppb.Reserve]("warehouse", "stock")
	apple := &shoppb.Reserve{Sku: "apple", Qty: 1}

	if r, err := shop.Call[*shoppb.Reserved](t.Context(), stock, apple); err != nil || r.Left != 2 {
		t.Fatal(r, err)
	}

	// A partition fails calls, and fires monitors with Down{noconnection},
	// until it heals.
	c.Partition("shop", "warehouse")
	if _, err := shop.Call[*shoppb.Reserved](t.Context(), stock, apple); !errors.Is(err, grpcproc.ErrNoConnection) {
		t.Fatal(err)
	}
	c.Heal("shop", "warehouse")
	if r, err := shop.Call[*shoppb.Reserved](t.Context(), stock, apple); err != nil || r.Left != 1 {
		t.Fatal(r, err)
	}

	// A crash, and a restart: same node name, new incarnation, and none of
	// the processes the old one ran.
	c.Kill("warehouse")
	c.Restart("warehouse")
	if _, err := shop.Call[*shoppb.Reserved](t.Context(), stock, apple); !errors.Is(err, grpcproc.ErrNoProc) {
		t.Fatal(err)
	}
}
```

`c.Stop(name)` stops a node gracefully, so watchers get `Down{shutdown}`.
`grpcproctest.WithServices` registers extra services (an Inspector) on every
node, and `c.Conn(name)` dials one.

## Examples

The programs above are in [examples](examples), a module of its own. CI
runs each one and compares what it prints with its `output.txt`, and checks
that this README embeds the current code, with
[embedmd](https://github.com/campoy/embedmd). After editing an example:

```sh
cd examples && go test ./quickstart ./actors ./supervisor -update
cd .. && gofmt -w examples && go run github.com/campoy/embedmd@v1.0.0 -w README.md
```

## Design

[docs/DESIGN.md](docs/DESIGN.md) has the wire protocol, the reasons behind
the choices (and what was rejected), what the observability surface is copied
from, and what is left: releases, a cluster-wide name registry.

## Performance

Against [GoAkt](https://github.com/tochemey/goakt),
[Hollywood](https://github.com/anthdm/hollywood) and
[Proto.Actor](https://github.com/asynkron/protoactor-go) on the same machine
(Apple M3 Max), remote over TCP on loopback for all; see
[benchmarks](benchmarks/README.md) for the method and how to read it:

| | grpcproc | GoAkt | Hollywood | Proto.Actor |
| --- | --- | --- | --- | --- |
| Local send | 99 ns | 96 ns | 58 ns | 191 ns |
| Local request | 738 ns | 565 ns | 2265 ns | 2354 ns |
| Remote send | 368 ns | 514 ns | 205 ns | 315 ns |
| Remote request | 44.4 µs | 34.5 µs | 36.4 µs | 58.6 µs |
| Remote request, 14 in parallel | 7.5 µs | 10.2 µs | 4.9 µs | 7.1 µs |

A mailbox that keeps up, and a local send, allocate nothing.

## License

MIT, see [LICENSE](LICENSE).
