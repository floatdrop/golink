# golink — design and roadmap

`golink` gives goroutines Erlang-style network transparency on top of the gRPC
server a service already runs. A process is addressed by a PID or a name; the
same `Send`, `Call`, `Monitor` and `Exit` work whether the target is in this
binary or on another node.

It is a library, not a framework, in the same sense that `fsm` and `di` are:

- **The caller owns the infrastructure.** `golink` never opens a listener, reads
  env vars, installs globals or starts a goroutine outside `Start`/`Stop`. The
  application brings its `*grpc.Server`, credentials, discovery, `slog` and
  lifecycle; `golink` registers one gRPC service on that server.
- **Errors at construction, none in the hot path.** Bad config fails in
  `NewNode`; a local `Send` allocates nothing and never panics.
- **Introspection is a first-class API**, in stable order, so a test can assert
  on it and a tool can render it.
- **The core depends on `grpc` and `protobuf` only.** Everything that would pull
  in another dependency (etcd, OpenTelemetry) is a separate Go module that
  plugs into an interface the core defines.

This document lists what to build, in what order, and what was looked at in
other frameworks to decide it (ergo.services, Proto.Actor, GoAkt, Hollywood,
go-actor, Erlang/OTP).

## The shape at a glance

```go
node, err := golink.NewNode(golink.Config{
    Name:        "orders-1",
    Advertise:   "10.0.0.5:9000",           // where *your* gRPC server listens
    Resolver:    golinketcd.New(cli, "/golink"), // optional; static map by default
    DialOptions: []grpc.DialOption{grpc.WithTransportCredentials(creds)},
    Logger:      slog.Default(),
    Hooks:       golinkotel.Hooks(meter, tracer), // optional taps, see Observability
})
node.Register(grpcServer) // mounts golink.v1.Mesh (+ golink.v1.Inspector if enabled)
node.Start(ctx)
defer node.Stop(ctx)

// A typed process: it receives *orderspb.OrderMsg (a oneof) and nothing else.
addr, _ := golink.Spawn[*orderspb.OrderMsg](node, func(p *golink.Process[*orderspb.OrderMsg]) error {
    for {
        m, err := p.Receive()
        if err != nil { return err }              // Exit, node stop, …
        if m.Down != nil { /* a monitored process is gone */ continue }
        switch k := m.Body.Kind.(type) {
        case *orderspb.OrderMsg_Reserve: p.Reply(m, &orderspb.Reserved{}, nil)
        }
    }
}, golink.WithName("reservations"), golink.WithLabel("order"))

ref := p.Monitor(golink.Name{Node: "billing-2", Name: "ledger"})
p.Send(addr, &orderspb.OrderMsg{Kind: &orderspb.OrderMsg_Reserve{}})     // compile-time typed
resp, err := golink.Call[*orderspb.Reserved](ctx, node, addr, &orderspb.OrderMsg{}) // reply typed by R
```

## Package layout

```
golink/                    core: Node, Process, PID, Send/Call/Monitor/Exit, links, inspection API
golink/proto/golink/v1       wire protocol (.proto + generated code)
golink/golinktest            in-memory clusters over bufconn: Cluster, Partition, Kill
golink/inspect             golink.v1.Inspector gRPC service + Go client (optional to register)
golink/actor               optional helpers: handler loop, supervisor, timers
golink/etcd     (nested module)   Resolver + Registrar + Membership on etcd leases
golink/otel     (nested module)   Hooks implementation: OTel metrics + trace propagation
cmd/golinkctl   (later)    CLI over golink.v1.Inspector
```

`golinktest` ships with the first release: the best argument for network
transparency is that a three-node scenario, including a node dying, runs in a
plain `go test` with no sockets and no etcd.

## Core (v0.1)

### Identity

```go
type PID  struct { Node string; Incarnation uint64; ID uint64 }
type Name struct { Node, Name string }
type Ref  struct { Node string; ID uint64 }   // monitor reference
```

`Incarnation` is a per-start nonce (or the etcd lease ID when the registrar
supplies one). A PID from before a node restart is a different process: it gets
`Down{noproc}`, never a message delivered to a stranger. Erlang has creation
numbers for the same reason; Proto.Actor and GoAkt do not, and both have issues
about stale references after restarts.

Names are per node. A cluster-wide registry is a later, etcd-backed feature;
Erlang's `global` is the model and it is deliberately separate from local
registration.

### Wire protocol

One gRPC service, one method:

```proto
service Node { rpc Link(stream Envelope) returns (stream Envelope); }

message Envelope {
  oneof kind { Send send = 1; Call call = 2; Reply reply = 3;
               Monitor monitor = 4; Demonitor demonitor = 5; Down down = 6;
               Exit exit = 7; }
  map<string,string> metadata = 15;   // trace context, deadlines, tenant …
}
message Send { PID from = 1; PID to = 2; string to_name = 3; google.protobuf.Any body = 4; }
```

- **One link per (node → node) direction**, opened by the sender as a client
  stream. Everything between two nodes — messages, calls, replies, `Down` —
  travels on it in order, which is what gives Erlang's guarantee that a
  process's last message arrives before its `Down`. Two client streams (one
  each way) avoid the simultaneous-dial race a single bidirectional link has.
- **The inbound link ending is the node-down signal.** The two directions are
  independent streams, so an envelope the peer sent can still be in flight on
  the inbound link when the outbound one fails. A node is declared down only
  once the inbound link from it has ended (everything it sent has then been
  dispatched, in order), or when there is no inbound link at all; an outbound
  failure alone drops that link and the next send dials again. Then every
  monitor that crossed the link fires `Down{noconnection}` and every pending
  call fails with `ErrNoConnection`. gRPC keepalive on both sides (client
  `keepalive.ClientParameters`, server `keepalive.ServerParameters`) is what
  turns a silent partition into a stream error in seconds; the library does
  not set it, the application's gRPC configuration does.
- **A graceful `Stop` flushes before it closes.** Processes exit first and
  their `Down{shutdown}` envelopes are queued, each outbound link is
  half-closed and waited on until the peer ends it, and only then are
  connections closed. The inbound handler never selects on the stream's own
  context: a peer's cancel is observed through `Recv`, after every envelope
  that preceded it.
- **Node identity travels in the stream's metadata** (`name`, `incarnation`,
  protocol version). An `Authorize(peer credentials.AuthInfo, claimed NodeInfo) error`
  hook lets mTLS deployments refuse a node whose certificate does not match the
  name it claims.
- **Bodies are `proto.Message` as `anypb.Any`.** Generated types register
  themselves, so there is no `Register()` step. Local sends pass the pointer
  without copying (fast; "do not mutate after send"); `WithCopyLocal()` clones
  for teams that want strict isolation.
- **Calls ride the same link**, matched by a call id, never as separate unary
  RPCs, so they cannot overtake or be overtaken by messages from the same sender.

### Typed processes

The type of a process's messages lives on its **address**, so it survives the
network boundary and every send is checked by the compiler:

```go
type Addr[M proto.Message] struct { /* PID or Name, plus the phantom type M */ }

func Spawn[M proto.Message](n *Node, fn func(*Process[M]) error, opts ...SpawnOption) (Addr[M], error)
func Named[M proto.Message](node, name string) Addr[M]   // remote by name; checked on delivery
func (a Addr[M]) PID() PID

func (p *Process[M]) Receive() (Msg[M], error)
func (p *Process[M]) ReceiveTimeout(d time.Duration) (Msg[M], error)
func (p *Process[M]) Send[N proto.Message](to Addr[N], m N) error
func (p *Process[M]) Call[N, R proto.Message](ctx, to Addr[N], req N) (R, error)
func (p *Process[M]) Reply(m Msg[M], resp proto.Message, err error) error   // may be deferred
func (p *Process[M]) Monitor(to Target) Ref
func (p *Process[M]) Demonitor(ref Ref)
func (p *Process[M]) Exit(to Target, reason string) error
func (p *Process[M]) Register(name string) error
func (p *Process[M]) Log() *slog.Logger        // pid, name, label attrs attached
func (p *Process[M]) Context() context.Context // cancelled on Exit / node stop

type Msg[M proto.Message] struct {
    From     PID
    Body     M        // zero when Down is set
    Down     *Down    // set when a monitored process is gone
    Metadata Metadata
}
func (m Msg[M]) IsCall() bool
func (m Msg[M]) Context() context.Context      // sender's deadline and metadata
```

`M` is whichever of these fits:

| `M` | Send site | Use |
|---|---|---|
| a generated oneof wrapper, `*orderspb.OrderMsg` | `p.Send(a, &orderspb.OrderMsg{Kind: …})` | one contract per process, visible in the `.proto` |
| your own marker interface, `interface{ proto.Message; orderMsg() }` | `p.Send(a, &orderspb.Reserve{})` | several generated types, no wrapper, one extra method each |
| `proto.Message` | anything | the untyped process |

The untyped process is the typed one instantiated with the interface, so
there is one implementation. Types do not cross the wire: a remote sender can
address `Named[*A]` at a process that is `Process[*B]`. The receiving node
checks `decoded.(M)` on delivery, and a mismatch is a dead letter with reason
`type`, counted and passed to `Hooks.OnDeadLetter`, never a panic inside the
process. Local sends are checked at compile time and skip the assertion.

`Down` arrives through the same `Receive`, in order with messages, so the
guarantee "a process's last message is seen before its `Down`" holds for typed
processes too. Mailboxes are unbounded so that delivery never blocks a link
(the Erlang choice; a bounded mailbox in one process would stall every other
process behind it on the shared stream). Backpressure is an application
concern; the mailbox depth is visible (see Observability) so it can be one.

Exit reasons: `normal`, `noproc`, `noconnection`, `shutdown`, `killed`, a
panic (`panic: …` with the stack logged), or the error string the function
returned.

### Discovery interfaces

```go
type Resolver interface { Resolve(ctx, node string) (addr string, err error) }
type Registrar interface { Register(ctx, self NodeInfo) error }       // keeps alive until ctx is done
type Membership interface { Watch(ctx) <-chan NodeEvent }             // authoritative up/down
```

The core ships a static resolver (a map, or `name@host:port` names). The etcd
module implements all three on leases. `Membership` is how "the lease expired"
becomes a cluster-wide verdict that a node is dead, on top of the fast but
local link-loss suspicion — the same split Erlang has between `net_kernel`
tick and an external registry.

### golinktest

```go
c := golinktest.New(t, "a", "b", "c")     // three nodes over bufconn
c.Partition("a", "b")                    // links between a and b break, monitors fire
c.Heal("a", "b")
c.Kill("b")                              // node gone; incarnation changes on Restart
c.Restart("b")
```

## Observability (v0.1 core surface, v0.2 tools)

This is the part taken from ergo. Its Observer, REST API and MCP server are
thin clients of a `system` application every node runs, which answers: what
processes exist, what is in their mailboxes, what does each process say about
itself, and what is happening on the wire. `golink` builds that answering surface
into the core and leaves the clients (UI, CLI, MCP) as separate, optional
programs. The rules:

1. **Counters are always on and cheap.** Atomics on the process and the link,
   read by snapshot. No sampling, no configuration.
2. **Nothing in the core imports a metrics or tracing library.** A `Hooks`
   interface is the single tap; `golink/otel` implements it.
3. **The Go API is the source of truth.** The gRPC `Inspector` service and any
   tool built on it expose exactly what `node.Info()` and `node.Processes()`
   return, nothing more.

### Process snapshot

What ergo's process table shows (identification, messaging, lifecycle) and
GoAkt's `pid.Metric()` returns, as one struct:

```go
type ProcessInfo struct {
    PID        PID
    Names      []string
    Label      string            // WithLabel; the low-cardinality key for metrics
    Parent     PID
    State      ProcessState      // Idle | Running | WaitingReply | Exiting
    StartedAt  time.Time
    Mailbox    MailboxInfo       // Depth, OldestAge (latency), Peak
    Received   uint64            // messages taken from the mailbox
    Sent       uint64
    CallsInFlight uint32
    LastMessage string           // proto full name of the last body handled
    Monitors   int               // held by this process
    Watchers   int               // processes monitoring this one
    Wakeups    uint64            // Receive returns
    LogLevel   slog.Level
}

func (n *Node) Processes() []ProcessInfo          // ordered by PID.ID
func (n *Node) Process(pid PID) (ProcessInfo, bool)
func (n *Node) Info() NodeInfo                   // name, incarnation, uptime, counts, Links []LinkInfo
```

`LinkInfo` per peer: state, established at, reconnects, messages and bytes in
and out, last error — ergo's network charts are drawn from exactly these.

### Self-inspection

Ergo's `HandleInspect(from, item...) map[string]string` is the single most
useful debugging feature it has: a process publishes what it currently
believes. In `golink`:

```go
node.Spawn(fn, golink.WithInspect(func() map[string]string {
    return map[string]string{"state": rec.state.String(), "pending": strconv.Itoa(rec.pendingUploads)}
}))
```

The function runs **on the process's own goroutine**, inside `Receive`, so it
reads state without a lock: an inspect request is delivered as a system item
that `Receive` serves before returning the next message. A process that is
busy in a handler answers when it next calls `Receive`; one that never does
reports `busy for 12s`, which is itself the diagnosis. This is how ergo's
Urgent queue behaves, without a second queue in the API.

With `fsm` this is one line: the inspect map is the machine's state and the
last transition, and `fsm.OnTransition` can log through `p.Log()`. A process
whose behaviour is an `fsm` therefore shows up in a tool as
`state=stopped, last=active --stop--> stopped`, which is what an operator wants
to see first.

### Hooks: the single tap

```go
type Hooks interface {
    OnSpawn(ProcessInfo)
    OnExit(ProcessInfo, reason string)
    OnSend(from, to PID, body proto.Message, md Metadata)      // local and remote
    OnReceive(pid PID, body proto.Message, waited time.Duration)
    OnDeadLetter(from, to PID, body proto.Message, reason string)
    OnLinkUp(peer NodeInfo)
    OnLinkDown(peer NodeInfo, err error)
}
```

Nil by default; a nil check per message when unset. This is the same shape as
`grpc.StatsHandler`, and it is what every observability feature in other
frameworks reduces to:

| Feature | ergo | GoAkt | golink |
|---|---|---|---|
| Metrics | Observer charts | `WithMetrics()` OTel gauges | `golink/otel` on `Hooks` + `Processes()` snapshots, one series per **label** by default (GoAkt's per-actor default is a cardinality trap it later added a switch for) |
| Dead letters | log | dead-letter actor + event | `OnDeadLetter` + counter in `NodeInfo` |
| System events | `gen.CoreEvent` | event stream | `OnSpawn/OnExit/OnLinkUp/OnLinkDown`; `Node.Events()` channel is a thin subscriber over the same hooks |
| Tracing | Sent / Delivered / Processed observations, trace id in the message | eBPF sidecar | `Envelope.metadata` carries W3C trace context; `golink/otel` injects in `OnSend`, extracts in `OnReceive`, and opens a span per `Call`. Same three points as ergo. Sampling stays the tracer's job |
| Logging | loggers as processes, per-process level | — | `p.Log()` is `slog` with pid/name/label; per-process level via a `slog.Handler` wrapper the node owns, settable at runtime |

### Inspector service (`golink/inspect`, v0.2)

A second gRPC service registered on the same server, optional:

```proto
service Inspector {
  rpc Node(NodeRequest) returns (NodeInfo);
  rpc ListProcesses(ListRequest) returns (ListResponse);    // filter by name/label/state
  rpc GetProcess(PID) returns (ProcessDetail);               // ProcessInfo + Inspect map
  rpc SetLogLevel(SetLogLevelRequest) returns (Empty);
  rpc Send(SendRequest) returns (Empty);                     // Any body, from a tool
  rpc Exit(ExitRequest) returns (Empty);
  rpc Watch(WatchRequest) returns (stream Event);            // spawn/exit/link events
}
```

Because it is gRPC, one node's inspector can answer for a peer by dialing the
peer's inspector through the resolver — ergo's "run Observer on one node and
inspect the whole cluster" for free. Access control is the application's
(interceptors, mTLS), as for any of its other services.

What sits on top, later and outside the core: `golinkctl ps / top / inspect /
send`, a `Node.DOT()` that draws processes and monitor edges across nodes
(cheap, and consistent with `fsm.DOT`), and an MCP server exposing the same
methods for an agent — ergo's MCP experience is convincing, and it is a
half-day of work once the gRPC service exists.

Goroutine dumps and heap profiles are `net/http/pprof`; `golink` does not
duplicate them.

## Helpers (`golink/actor`, v0.2)

Optional, built only on the public core API so users can ignore or replace them:

- `actor.Run(h Handler)`: the handler loop from the prototype — `HandleMessage`
  / `HandleCall`, `ErrStop`, optional `Init`/`Terminate`, panics become exit
  reasons.
- `actor.Supervise(node, spec)`: one-for-one and one-for-all restart with
  intensity limits, over `Monitor` and `Spawn` only. Restart count shows in
  `ProcessInfo` via the parent relationship, not a special field.
- `p.SendAfter(d, to, msg) (Cancel)`: timers; ergo and Erlang both have them and
  a `time.AfterFunc` that sends is fifteen lines.

## Later

- `golink/etcd`: `Resolver`, `Registrar`, `Membership`, and a **global name
  registry** (`golink.Global{"ledger"}` resolving through etcd with a lease as
  fencing token).
- Cross-node pub/sub with a replay buffer (ergo's events). Useful; not core.
- Delivery beyond at-most-once, in order per sender. Explicitly out of scope;
  build it above `golink`, as OTP does.
- Virtual actors / placement, persistence, remote spawn. Out of scope.

## What was rejected, and why

| Idea | Seen in | Why not |
|---|---|---|
| Own TCP protocol | ergo, GoAkt, Hollywood (dRPC) | The whole point is to reuse the gRPC server, TLS, interceptors and tooling the service already has |
| One monitor = one stream | — | Loses message-before-Down ordering, costs a goroutine per monitor |
| Priority mailbox queues | ergo (4 queues) | Inspection runs inside `Receive` instead; `Down` must stay in order with messages |
| Bounded mailboxes | GoAkt | A full mailbox would stall the shared link for everyone |
| Metrics per PID by default | GoAkt | Cardinality; label is the key, PID is available on request |
| Embedded web UI | ergo Observer | A UI is a client; the core exposes the gRPC surface it would need |
| gob / custom codec | first prototype | protobuf is already the service's contract; `Any` needs no registration |

## Order of work

1. `proto/golink/v1`, core `Node`/`Process`, links, static resolver, `ProcessInfo`
   counters, `Hooks`, `WithInspect`, `golinktest`. Tests: ordering, monitors with
   every reason, node down, restart with new incarnation, bad peer identity.
2. `golink/inspect` service and Go client; `golink/actor` helpers.
3. `golink/otel` (metrics + trace propagation), `golink/etcd`.
4. `golinkctl`, `DOT`, MCP server.

Coverage target and style follow `fsm` and `di`: 100 % on the core,
race-detected, examples compiled in CI, `DESIGN.md` kept current.
