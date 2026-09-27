# <img src="site/public/favicon.svg" alt="" height="28"> grpcproc

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
- **Erlang semantics.** Per-sender ordering, `Down` in order with messages,
  incarnation in the PID so a restart never resurrects a reference,
  unbounded mailboxes so a slow process cannot stall the link.
- **Inspectable.** Every process and link keeps counters; a process can
  publish what it currently believes; one `Hooks` interface taps everything
  for metrics, tracing and dead letters.
- **Core depends on `grpc` and `protobuf` only.**

```sh
go get github.com/floatdrop/grpcproc
```

Requires Go 1.27 (generic methods). The full reference is on
[pkg.go.dev](https://pkg.go.dev/github.com/floatdrop/grpcproc).
**[The guide](https://floatdrop.github.io/grpcproc/)** is one application,
read top to bottom: a shop whose services are actors wired with
[golang.yandex/di](https://github.com/yandex/di), run as one program or as
three nodes from the same code.

## Quick start

A process is a function over a typed mailbox:

[embedmd]:# (examples/quickstart/main.go go /\/\/ inventory is a process/ /\n}\n/)
```go
// inventory is a process whose mailbox holds *shoppb.Reserve and nothing
// else; every message is a call, answered with *shoppb.Reserved or with an
// error.
func inventory(p *grpcproc.Process[*shoppb.Reserve]) error {
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
}
```

`warehouse` and `shop` are two nodes, built as [API](#api) shows. The
process runs on one under a name, and the other reaches it by that name:

[embedmd]:# (examples/quickstart/main.go go /\tif _, err := warehouse.Spawn/ /reserved, left:", r.Left\)\n\t}/)
```go
	if _, err := warehouse.Spawn(inventory, grpcproc.WithName("stock")); err != nil {
		log.Fatal(err)
	}

	// From shop, it is a node name and a process name. The address carries
	// the mailbox type, so the compiler checks what is sent to it.
	stock := grpcproc.Named[*shoppb.Reserve]("warehouse", "stock")
	for range 2 {
		r, err := shop.Call[*shoppb.Reserved](ctx, stock, &shoppb.Reserve{Sku: "apple", Qty: 2})
		if err != nil {
			fmt.Println("reserve failed:", err) // the handler's error, as a *grpcproc.RemoteError
			continue
		}
		fmt.Println("reserved, left:", r.Left)
	}
```

A monitor across nodes works as one within a node does. The watcher's
mailbox is untyped, `proto.Message`, since it expects nothing but the `Down`:

[embedmd]:# (examples/quickstart/main.go go /\t\/\/ A process on shop monitors/ /fmt.Println\("stock exited:", <-exited\)/)
```go
	// A process on shop monitors stock, then asks it to exit. The Down
	// arrives with the reason, as it would for a crash or a lost node.
	exited := make(chan string)
	_, err := shop.Spawn(func(p *grpcproc.Process[proto.Message]) error {
		p.Monitor(stock)
		if err := p.Exit(stock, "closing"); err != nil {
			return err
		}
		m, err := p.Receive() // the Down: nothing else is sent to this process
		if err != nil {
			return err
		}
		exited <- m.Down.Reason
		return nil
	})
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("stock exited:", <-exited)
```

[examples/quickstart](examples/quickstart/main.go) is the whole program:
each node on its own gRPC server, and a last call to the gone process,
which fails with `ErrNoProc`. It prints:

[embedmd]:# (examples/quickstart/output.txt)
```txt
reserved, left: 1
reserve failed: only 1 apple left
stock exited: closing
no such process: true
```

## API

A node is a value you construct, mount on your gRPC server, start and stop.
`Config` needs a `Name` and a `Resolver`; `DialOptions` carry the
credentials and keepalive for the links it opens:

```go
node, err := grpcproc.NewNode(grpcproc.Config{Name: "shop", Resolver: peers, DialOptions: opts})
node.Register(grpcServer) // grpcproc.v1.Node, next to your services; before it serves
err = node.Start(ctx)     // publishes the node; processes spawned before it run already
err = node.Stop(ctx)      // "shutdown" to every process, waits for them (ctx bounds the wait), closes the links
```

### Node

| | |
| --- | --- |
| `Spawn[M](fn func(*Process[M]) error, opts ...SpawnOption) (Addr[M], error)` | Runs `fn` as a process until it returns. `M` may be a concrete type, an interface, or `proto.Message` for an untyped process. |
| `Send[N](ctx, to Addr[N], m N) error` | Queues `m` and returns. No such process is a dead letter, not an error. |
| `SendTo(ctx, to Target, m proto.Message) error` | The same to an untyped target; the type is checked on delivery. |
| `Call[R, N](ctx, to Addr[N], req N) (R, error)` | Sends and waits for the reply as `R`. |
| `CallTo[R](ctx, to Target, req proto.Message) (R, error)` | The same to an untyped target. |
| `Exit(ctx, to Target, reason string) error` | Asks a process anywhere to exit. |
| `Whereis(name string) (PID, bool)` | A name registered on this node. |
| `Processes()`, `Process(pid)`, `Info()`, `Peers()` | Snapshots: every local process, one, the node with its links, the peers with a live link. |
| `Inspect(ctx, pid) (map[string]string, error)` | What a local process publishes through `WithInspect`. |
| `Subscribe(ctx, buffer int) <-chan Event` | Spawns, exits with their reasons, links up and down, dead letters. |
| `SetLogLevel(pid, level slog.Level) error` | The threshold of a local process's `Log()`. |
| `Disconnect(peer string) bool` | Drops the links with a peer, as a partition would. |
| `ID()`, `Name()`, `PID()` | The node's name and incarnation; the name alone; the pseudo-process (ID 0) its own sends come from. |

The `ctx` of a `Call` bounds the whole call; for `Send`, `SendTo` and
`Exit` it bounds only the wait for a first connection to a peer. Sends and
calls carry the `Metadata` in that ctx with the message.

### Process[M]

| | |
| --- | --- |
| `Receive() (Msg[M], error)` | Blocks for a message or a `Down`. The error is an `*ExitError` after `Exit`, `context.Canceled` when the node stops. |
| `ReceiveTimeout(d) (Msg[M], error)` | The same, or `context.DeadlineExceeded`. |
| `Reply(m Msg[M], resp proto.Message, err error) error` | Answers a call. It may run later, from any goroutine. |
| `Send[N](to Addr[N], m N) error`, `SendTo(to Target, m proto.Message) error` | As on `Node`, with the process as the sender and the metadata of the message it is handling. |
| `Call[R, N](ctx, to Addr[N], req N) (R, error)`, `CallTo[R](ctx, to Target, req proto.Message) (R, error)` | As on `Node`. Replies bypass the mailbox, so a call from a process never reorders its messages. |
| `SendAfter[N](d, to Addr[N], m N) *Timer` | A send after `d`, as this process, cancelled if it exits first; `Timer.Stop` cancels it too. |
| `Monitor(target Target) Ref`, `Demonitor(ref)` | A `Down` with the ref when the target exits or its node is unreachable; at once, with `noproc`, for a process that does not exist. |
| `Exit(to Target, reason string) error` | Asks another process to exit. |
| `Spawn[N](fn, opts...) (Addr[N], error)`, `SpawnMonitor[N](fn, opts...) (Addr[N], Ref, error)` | A child on the same node, with this process recorded as its parent, for inspection: neither exits with the other. The second is monitored before it runs, so no exit is missed. |
| `Context() context.Context` | Cancelled when the process is asked to exit or the node stops; after `Exit`, `context.Cause` is the `*ExitError`. |
| `Log() *slog.Logger` | With pid and label attached. |
| `Addr()`, `PID()`, `Node()` | The process's typed address, its PID, its node. |

### Addresses and messages

| | |
| --- | --- |
| `PID{Node, Incarnation, ID}` | A process anywhere: comparable, and travels inside messages. |
| `Name{Node, Name}` | A process by the name it registered. |
| `Addr[M]` | A PID or a Name plus the message type. `Spawn` returns one, `Named[M](node, name)` makes one, `AddrOf[M](target)` types a target. |
| `Target` | What a message can go to: a `PID`, a `Name` or any `Addr`. |
| `Msg[M]{From, Body, Down, Metadata}` | What `Receive` returns: a `Body` or a `Down`, never both. `IsCall()` says whether a `Reply` is awaited; `Context(parent)` carries the metadata into a ctx. |
| `Down{Ref, PID, Name, Reason}` | A monitored process exited, or its node is unreachable. |
| `Metadata` | `map[string]string` carried with every message, untouched: trace context, tenant. `WithMetadata(ctx, md)` sets it for `Node` sends and every `Call`, `MetadataFrom(ctx)` reads it back. |

### Spawn options

| | |
| --- | --- |
| `WithName(name)` | Registers the process under `name` on its node until it exits; `ErrNameTaken` while another holds it. |
| `WithLabel(label)` | The low-cardinality key metrics aggregate by. Defaults to the type of `M`. |
| `WithInspect(fn func() map[string]string)` | What `Node.Inspect` and the Inspector show. `fn` runs on the process's goroutine, between two messages, so it reads the process's state without a lock. |

### Errors and exit reasons

| | |
| --- | --- |
| `ErrNoProc` | No such process, or a callee that exited before answering. |
| `ErrNoConnection` | The peer cannot be reached. It comes as a `*LinkError` wrapping the transport error, which `errors.Is` matches. |
| `ErrType` | The process does not accept this message type, or the reply is not an `R`. |
| `*RemoteError` | The error a `Call` handler returned, carried back as text. |
| `ErrNameTaken`, `ErrNodeStopped`, `ErrNotCall` | A `Spawn` under a held name; the node has stopped; a `Reply` to a message nobody waits on. |

A process exits with a reason, a string: `normal` for a `nil` return, the
error's text otherwise, `panic: …`, or what `Exit` asked for. The node stops
its processes with `shutdown`; the Inspector's `Exit` defaults to `killed`.
A `Down` alone carries `noproc`, for a process that does not exist, and
`noconnection`, for a node that cannot be reached. Reasons and the type
check on delivery are the whole error model: a message of the wrong type is
a dead letter with reason `type` (and `ErrType` to a caller), never a panic
in the process.

### Config

| | |
| --- | --- |
| `Name`, `Resolver` | Required: the node's name, and what turns a peer's name into an address. `StaticResolver` is a map. |
| `DialOptions` | For every outbound link: credentials, keepalive, interceptors. Keepalive is what turns a silent partition into a link error, so set it. |
| `Advertise`, `Registrar`, `Membership` | Where peers dial this node, what publishes it, and the cluster's view of who is alive; see [Discovery](#discovery). |
| `Authorize func(ctx, peer NodeID) error` | Runs for every inbound link, with the peer's transport credentials in `ctx`. |
| `Hooks`, `Logger` | The observability tap, and the logger (`slog.Default()`). |
| `Incarnation`, `DialTimeout`, `CopyLocal` | This start of the node (the time, by default); how long a dial may take (5s); whether local messages are cloned rather than shared (off). |

## Actors and supervisors

`grpcproc/actor` is optional structure on top of processes, built on the
public API only. An actor is a struct holding its dependencies, which is what
a constructor or a DI container builds, with a method per kind of message
instead of a receive loop. All but `HandleMessage` are optional:

| Method | Runs for | Its result |
| --- | --- | --- |
| `Init` | once, before the first message | an error ends the actor |
| `HandleMessage` | a message sent with `Send` | an error ends the actor with it as the reason; `actor.ErrStop` ends it normally |
| `HandleCall` | a message sent with `Call` | the reply, a message or an error, and the actor carries on; `actor.ErrNoReply` answers later with `p.Reply` |
| `HandleDown` | a `Down` from a monitor | as for `HandleMessage` |
| `Terminate` | once, when the actor ends, however it ends | none: the exit reason is already decided |

`actor.Run(h)` turns an actor into a process function for `Spawn`. An actor
that only answers calls embeds `actor.CallsOnly[M]` in place of
`HandleMessage`: a plain send to it is logged and dropped.

This inventory records reservations in a `Ledger`, its dependency, parks
one it cannot serve yet, and answers it from `HandleMessage` when a restock
arrives:

[embedmd]:# (examples/actors/main.go go /\/\/ HandleCall gets what was sent/ /\n}\n/)
```go
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
```

[examples/actors](examples/actors/main.go) spawns it with
`node.Spawn(actor.Run(&Inventory{…}), grpcproc.WithName("inventory"))`,
then breaks its protocol with a reservation that is sent rather than
called, which ends it. It prints:

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

A supervisor starts its children in order, monitors them from before they
run, and restarts those that exit. `Strategy` says which: `OneForOne` only
the child that exited, `OneForAll` every child, `RestForOne` the child and
those started after it. A child's `Restart` says when: `Permanent` always,
`Transient` after an abnormal exit only, `Temporary` never. More than
`MaxRestarts` restarts `Within` the window (3 in 5s by default) and the
supervisor exits with `max restarts`; its own supervisor, a
`ChildSupervisor`, then restarts it, which is how failure moves up the tree.
A child told to exit has `Shutdown` (5s) to do so.

`actor.Child` builds the handler afresh at every start, so a restart never
sees the state that crashed; `ChildFunc` runs a plain process function. What
must outlive a crash lives outside the actor, here in a `Ledger` that `Init`
loads:

[embedmd]:# (examples/supervisor/main.go go /\/\/ tree is the supervision tree/ /\n}\n/)
```go
// tree is the supervision tree: one child, restarted alone when it exits,
// up to 3 times a minute.
func tree(ledger *Ledger) actor.Spec {
	return actor.Spec{
		Strategy:    actor.OneForOne,
		MaxRestarts: 3,
		Within:      time.Minute,
		Children: []actor.ChildSpec{
			actor.Child("inventory", func() *Inventory { return &Inventory{ledger: ledger} }),
		},
	}
}
```

`actor.Supervise(node, tree(ledger), grpcproc.WithName("supervisor"))`
starts it. In [examples/supervisor](examples/supervisor/main.go) the
inventory crashes on a bad message and comes back under the same name, with
its state loaded from the ledger; the supervisor reports its restarts
through `WithInspect`. It prints:

[embedmd]:# (examples/supervisor/output.txt)
```txt
reserved, left: 3
inventory exited: bad restock of 0 apple
inventory started again
reserved, left: 2
supervisor restarts: 1/3 in 1m0s
```

## Observability

A `ProcessInfo` has the state, mailbox depth and oldest wait, messages sent
and received, calls in flight, last message type, watchers, monitors and log
level; a `LinkInfo` its messages, bytes and reconnects. `node.Inspect` adds
what the process says about itself: a process busy in a handler answers when
it next receives, and one that never does reports `busy for 12s`, which is
the diagnosis. With a state machine that is one line:

```go
grpcproc.WithInspect(func() map[string]string { return map[string]string{"state": rec.state.String()} })
```

`node.Subscribe` never blocks the node: a subscriber that falls behind
loses events and is told how many in the next one's `Missed`.

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
name, label, state, mailbox depth), `GetProcess` (the snapshot plus what the
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

`grpcproctest` runs nodes over in-memory connections, so a multi-node
scenario, partition and crash included, is a plain `go test`:

[embedmd]:# (examples/testing/shop_test.go go /func TestReserveAcrossNodes/ $)
```go
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

The code above is from [examples](examples), a module of its own: whole
programs, of which this README shows the parts that matter. CI runs each
one, compares what it prints with its `output.txt`, and checks that the
README embeds the current code, with
[embedmd](https://github.com/campoy/embedmd). After editing an example:

```sh
cd examples && go test ./quickstart ./actors ./supervisor -update
cd .. && gofmt -w examples && go run github.com/campoy/embedmd@v1.0.0 -w README.md
```

[`examples/guide`](examples/guide) is the application the guide shows, from
[`site/`](site). Its tests run it in both deployments and pin the output the
page prints; `go test ./guide -update` refreshes it.

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
