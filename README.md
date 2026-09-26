# golink

Erlang-style processes for Go, on the gRPC server you already run.

A process is a goroutine with a mailbox and a cluster-wide PID. `Send`,
`Call`, `Monitor` and `Exit` work the same whether the target is in this
binary or on another node; the node-to-node traffic is one gRPC stream per
direction, registered on your `*grpc.Server` next to your other services.

- **A library, not a framework.** You bring the gRPC server, credentials,
  discovery, `slog` and lifecycle. golink never opens a listener, reads env
  vars, installs globals or starts a goroutine outside `Start`/`Stop`.
- **Typed mailboxes.** The message type lives on the address, so a send to
  `Addr[*orderspb.Order]` is checked by the compiler, locally and remotely.
  Instantiate with `proto.Message` for an untyped process.
- **Erlang semantics.** Per-sender ordering, `Down` in order with messages,
  incarnation in the PID so a restart never resurrects a reference,
  unbounded mailboxes so a slow process cannot stall the link.
- **Inspectable.** Every process and link keeps counters you can snapshot;
  a process can publish what it currently believes through `WithInspect`;
  one `Hooks` interface taps everything for metrics, tracing and dead letters.
- **Core depends on `grpc` and `protobuf` only.**

```sh
go get github.com/floatdrop/golink
```

Requires **Go 1.27** (generic methods).

## Quick start

```go
node, err := golink.NewNode(golink.Config{
    Name:        "orders-1",
    Resolver:    golink.StaticResolver{"billing-1": "10.0.0.7:9000"},
    DialOptions: []grpc.DialOption{grpc.WithTransportCredentials(creds)},
})
node.Register(grpcServer) // mounts golink.v1.Node on your server
node.Start(ctx)
defer node.Stop(ctx)

// A typed process: it receives *orderspb.Order and nothing else.
addr, _ := golink.Spawn(node, func(p *golink.Process[*orderspb.Order]) error {
    for {
        m, err := p.Receive()
        if err != nil {
            return err // Exit, or the node stopping
        }
        if m.Down != nil {
            p.Log().Warn("ledger gone", "reason", m.Down.Reason)
            continue
        }
        switch k := m.Body.Kind.(type) {
        case *orderspb.Order_Reserve:
            p.Reply(m, &orderspb.Reserved{Id: k.Reserve.Id}, nil)
        }
    }
}, golink.WithName("orders"), golink.WithLabel("order"))

// From anywhere in the cluster; the address carries the type.
ledger := golink.Named[*ledgerpb.Entry]("billing-1", "ledger")
resp, err := node.Call[*orderspb.Reserved](ctx, addr, &orderspb.Order{…})
```

Inside a process:

```go
ref := p.Monitor(ledger)                       // Down{Ref: ref} when it exits or its node is unreachable
err := p.Send(ledger, &ledgerpb.Entry{…})       // compile-time typed
r, err := p.Call[*ledgerpb.Posted](ctx, ledger, &ledgerpb.Entry{…})
err = p.SendTo(m.From, &orderspb.Ack{})         // untyped: a PID from a message
a, err := p.CallTo[*orderspb.Ack](ctx, m.From, &orderspb.Ping{})

// Node has the same four: Send, SendTo, Call, CallTo.
```

## Actors and supervisors

`golink/actor` adds structure on top of processes, using only the public API.
An actor is a plain struct with its dependencies, which is what a DI
container builds:

```go
type Orders struct{ repo *Repo }

func (o *Orders) HandleMessage(p *golink.Process[*orderspb.Order], m golink.Msg[*orderspb.Order]) error { … }
func (o *Orders) HandleCall(p *golink.Process[*orderspb.Order], m golink.Msg[*orderspb.Order]) (proto.Message, error) { … }
// optional: HandleDown, Init, Terminate

addr, err := actor.Spawn[*orderspb.Order](node, &Orders{repo: repo})
```

An error from `HandleCall` goes back to the caller and the actor carries on;
from `HandleMessage` it ends the actor with that reason. `actor.ErrStop` ends
it normally, `actor.ErrNoReply` defers a call's answer to a later `p.Reply`.

Supervisors restart what fails, one-for-one, one-for-all or rest-for-one,
within a restart intensity; a supervisor that gives up exits with
`max restarts`, and its own supervisor restarts it:

```go
sup, err := actor.Supervise(node, actor.Spec{
    Strategy: actor.OneForOne,
    Children: []actor.ChildSpec{
        actor.Child[*orderspb.Order]("orders", func() *Orders { return &Orders{repo: repo} }),
        actor.ChildFunc("mailer", mailer).WithRestart(actor.Transient),
        actor.ChildSupervisor("billing", billingSpec),
    },
})
```

Each child is registered under its name, so `golink.Named` keeps reaching it
across restarts, and `Child` builds a fresh handler on every start. Children
are monitored from before they run (`p.SpawnMonitor`), so no exit is missed.
A supervisor publishes its children and restart counts through
`WithInspect`, so the Inspector shows the supervision tree's state.

## What a process sees

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
| `p.SpawnMonitor[N](fn, …)` | a child on the same node, monitored before it runs |

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
golink.WithInspect(func() map[string]string { return map[string]string{"state": rec.state.String()} })
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
threading a context through handlers. `golink.JoinHooks` combines several.

[`golink/otel`](otel/README.md) implements them with OpenTelemetry: spans
for every send, call and handled message, chained across nodes, and metrics
by process label (throughput, mailbox wait and depth, handling and call
latency, exits by reason class, dead letters, link traffic).

## Inspector

`golink/inspect` serves all of the above over gRPC, on the same server:

```go
node.Register(grpcServer)
dialer := inspect.NewDialer(resolver, dialOptions...) // reach other nodes' Inspectors
inspect.New(node, inspect.WithPeers(dialer.Peer)).Register(grpcServer)
```

`golink.inspect.v1.Inspector` has `GetNode`, `ListProcesses` (filter by
name, label, state, mailbox depth), `GetProcess` (snapshot plus what the
process publishes through `WithInspect`), `SetLogLevel`, `Send`, `Exit` and
`Watch`. Every request names a node; one that is not this node is forwarded
to that node's Inspector, so one endpoint inspects the whole cluster.
`inspect.ReadOnly()` refuses the three writes; anything finer is the job of
the interceptors that guard your other services.

[`golinkctl`](tools/README.md) is its command line, and serves it to AI
agents over MCP:

```sh
golinkctl --plaintext ps --sort mailbox        # who is falling behind
golinkctl --plaintext inspect ledger-writer    # what it believes, or that it is busy
golinkctl --plaintext dot --cluster | dot -Tsvg -o processes.svg
claude mcp add golink -- golinkctl --plaintext --addr 10.0.0.5:9000 mcp
```

It is a plain gRPC service, so `grpcurl` works on it too.

## Discovery

`Config.Resolver` is all a node needs to reach peers; `golink.StaticResolver`
is a map. `Config.Registrar` publishes the node on `Start` and withdraws it
on `Stop`, and `Config.Membership` is the cluster's view of who is alive: a
peer that leaves, or comes back as a new incarnation, has its links dropped,
so monitors fire and pending calls fail even when its connection never
closed. [`golink/etcd`](etcd/README.md) implements all three on etcd leases.

## Testing a cluster

`golinktest` runs nodes over in-memory connections, so a multi-node scenario
is a plain `go test`:

```go
c := golinktest.New(t, "a", "b")
a, b := c.Node("a"), c.Node("b")
c.Partition("a", "b") // monitors fire Down{noconnection}; calls fail
c.Heal("a", "b")
c.Kill("b")           // as a crash: no shutdown notice
c.Restart("b")        // same name, new incarnation
```

`golinktest.WithServices` registers extra services (an Inspector) on every
node, and `c.Conn(name)` dials one.

## Design

[docs/DESIGN.md](docs/DESIGN.md) has the wire protocol, the reasons behind
the choices (and what was rejected), what the observability surface is copied
from, and what is left: releases, a cluster-wide name registry.

## Performance

Against [Hollywood](https://github.com/anthdm/hollywood) on the same machine
(Apple M3 Max), remote over TCP on loopback for both; see
[benchmarks](benchmarks/README.md) for the method and what is left of the
gap:

| | Hollywood | golink |
| --- | --- | --- |
| Local send | 57 ns | 110 ns |
| Local request | 2311 ns | 753 ns |
| Remote send | 208 ns | 354 ns |
| Remote request | 37.3 µs | 42.2 µs |
| Remote request, 14 in parallel | 4.9 µs | 7.8 µs |

A mailbox that keeps up, and a local send, allocate nothing.

## License

MIT, see [LICENSE](LICENSE).
