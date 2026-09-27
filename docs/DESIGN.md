# grpcproc — design and roadmap

`grpcproc` gives goroutines Erlang-style network transparency on top of the gRPC
server a service already runs. A process is addressed by a PID or a name; the
same `Send`, `Call`, `Monitor` and `Exit` work whether the target is in this
binary or on another node.

It is a library, not a framework, in the same sense that `fsm` and `di` are:

- **The caller owns the infrastructure.** `grpcproc` never opens a listener, reads
  env vars, installs globals or starts a goroutine outside `Start`/`Stop`. The
  application brings its `*grpc.Server`, credentials, discovery, `slog` and
  lifecycle; `grpcproc` registers one gRPC service on that server.
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
node, err := grpcproc.NewNode(grpcproc.Config{
    Name:        "orders-1",
    Advertise:   "10.0.0.5:9000",           // where *your* gRPC server listens
    Resolver:    grpcprocetcd.New(cli, "/grpcproc"), // optional; static map by default
    DialOptions: []grpc.DialOption{grpc.WithTransportCredentials(creds)},
    Logger:      slog.Default(),
    Hooks:       otelHooks, // grpcprocotel.New(): optional, see Observability
})
node.Register(grpcServer) // mounts grpcproc.v1.Node
node.Start(ctx)
defer node.Stop(ctx)

// A typed process: it receives *orderspb.OrderMsg (a oneof) and nothing else.
addr, _ := node.Spawn[*orderspb.OrderMsg](func(p *grpcproc.Process[*orderspb.OrderMsg]) error {
    for {
        m, err := p.Receive()
        if err != nil { return err }              // Exit, node stop, …
        if m.Down != nil { /* a monitored process is gone */ continue }
        switch k := m.Body.Kind.(type) {
        case *orderspb.OrderMsg_Reserve: p.Reply(m, &orderspb.Reserved{}, nil)
        }
    }
}, grpcproc.WithName("reservations"), grpcproc.WithLabel("order"))

ref := p.Monitor(grpcproc.Name{Node: "billing-2", Name: "ledger"})
p.Send(addr, &orderspb.OrderMsg{Kind: &orderspb.OrderMsg_Reserve{}})     // compile-time typed
resp, err := node.Call[*orderspb.Reserved](ctx, addr, &orderspb.OrderMsg{}) // reply typed by R
```

## Package layout

```
grpcproc/                    core: Node, Process, PID, Send/Call/Monitor/Exit, links, inspection API
grpcproc/proto/grpcproc/v1       wire protocol (.proto + generated code)
grpcproc/grpcproctest            in-memory clusters over bufconn: Cluster, Partition, Kill
grpcproc/inspect             grpcproc.v1.Inspector gRPC service + Go client (optional to register)
grpcproc/actor               optional helpers: handler loop, supervisor, timers
grpcproc/etcd     (nested module)   Resolver + Registrar + Membership on etcd leases
grpcproc/otel     (nested module)   Hooks implementation: OTel metrics + trace propagation
grpcproc/tools    (nested module)   grpcprocctl: CLI, Graphviz and MCP server over the Inspector
```

`grpcproctest` ships with the first release: the best argument for network
transparency is that a three-node scenario, including a node dying, runs in a
plain `go test` with no sockets and no etcd.

## Core (v0.1)

### Identity

```go
type PID  struct { Node string; Incarnation uint64; ID uint64 }
type Name struct { Node, Name string }
type Ref  struct { Node string; ID uint64 }   // monitor reference
```

`Incarnation` is a per-start nonce (`Config.Incarnation`, by default the
start time in nanoseconds). A PID from before a node restart is a different process: it gets
`Down{noproc}`, never a message delivered to a stranger. Erlang has creation
numbers for the same reason; Proto.Actor and GoAkt do not, and both have issues
about stale references after restarts.

Names are per node. A cluster-wide registry is a later, etcd-backed feature;
Erlang's `global` is the model and it is deliberately separate from local
registration.

### Wire protocol

One gRPC service, one method:

```proto
service Node { rpc Link(stream Frame) returns (stream Frame); }

message Frame { repeated Envelope envelopes = 1; }   // everything queued since the last write

message Envelope {                                   // one flat message, decoded cheaply
  Kind kind = 1;                                     // send, call, reply, monitor, demonitor, down, exit, hello
  uint64 from_incarnation = 2; uint64 from_id = 3;   // on the node that opened the link
  uint64 to_incarnation = 4;   uint64 to_id = 5;     // on the node that accepted it
  string to_name = 6;
  uint64 ref = 7; Status status = 8; string reason = 9;
  string body_type = 10; bytes body = 11;            // the message's full name and encoding
  Hello hello = 12;
  map<string,string> metadata = 15;                  // trace context, deadlines, tenant …
}
```

- **A frame per write.** A link's writer sends everything queued since its
  last write as one gRPC message, split at about 1 MiB (well under gRPC's
  default 4 MiB limit), so under load many envelopes share the per-message
  cost of gRPC; at low load a frame holds one envelope and nothing waits.
- **No node names per envelope.** Every envelope on a link goes from a
  process of the node that opened it to one of the node that accepted it,
  so PIDs travel as incarnation and id; the reader fills the node names in
  from the link.
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
  that preceded it. The goroutine reading a link dispatches each frame
  itself; a per-link lock held while dispatching and while closing means
  nothing from a closed link is dispatched after its `Down{noconnection}`.
- **Node identity travels in the stream's metadata** (`name`, `incarnation`,
  protocol version). An `Authorize(peer credentials.AuthInfo, claimed NodeInfo) error`
  hook lets mTLS deployments refuse a node whose certificate does not match the
  name it claims.
- **Bodies are `proto.Message`, sent as their type's full name and their
  encoding.** Generated types register themselves, so there is no
  `Register()` step. Local sends pass the pointer
  without copying (fast; "do not mutate after send"); `WithCopyLocal()` clones
  for teams that want strict isolation.
- **Calls ride the same link**, matched by a call id, never as separate unary
  RPCs, so they cannot overtake or be overtaken by messages from the same sender.

### Typed processes

The type of a process's messages lives on its **address**, so it survives the
network boundary and every send is checked by the compiler:

```go
type Addr[M proto.Message] struct { /* PID or Name, plus the phantom type M */ }

func (n *Node) Spawn[M proto.Message](fn func(*Process[M]) error, opts ...SpawnOption) (Addr[M], error)
func Named[M proto.Message](node, name string) Addr[M]   // remote by name; checked on delivery
func (a Addr[M]) PID() PID

func (p *Process[M]) Receive() (Msg[M], error)
func (p *Process[M]) ReceiveTimeout(d time.Duration) (Msg[M], error)
func (p *Process[M]) Send[N proto.Message](to Addr[N], m N) error
func (p *Process[M]) Call[R, N proto.Message](ctx, to Addr[N], req N) (R, error)
func (p *Process[M]) CallTo[R proto.Message](ctx, to Target, req proto.Message) (R, error)
func (p *Process[M]) SendTo(to Target, m proto.Message) error
// Node has the same Send / SendTo / Call / CallTo, with the node as sender;
// its Send and SendTo take a ctx too, for metadata and the dial.
func (p *Process[M]) Reply(m Msg[M], resp proto.Message, err error) error   // may be deferred
func (p *Process[M]) Monitor(to Target) Ref
func (p *Process[M]) Demonitor(ref Ref)
func (p *Process[M]) Exit(to Target, reason string) error
func (p *Process[M]) Log() *slog.Logger        // pid and label attrs attached
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
| your own marker interface, `OrderMsg interface{ proto.Message; orderMsg() }` | `p.Send[OrderMsg](a, &orderspb.Reserve{})` | several generated types, no wrapper, one extra method each |
| `proto.Message` | `p.SendTo(a, anything)` | the untyped process |

With an interface `M`, the send names it: Go infers `N` from the address and
from the message alike, and they differ. That is why the examples use a
oneof.

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
type Member struct { Name string; Incarnation uint64; Addr string }
type Registrar interface {
    Register(ctx, self Member) (withdraw func(context.Context) error, err error)
}
type Membership interface { Watch(ctx) (<-chan MemberEvent, error) } // {Member, Up}
```

The core ships a static resolver (a map). `grpcproc/etcd` implements all three
on leases. `Start` watches `Membership` and then registers; `Stop` withdraws
last, after the node's processes have exited and their `Down{shutdown}`
notices have been flushed to peers, so peers see "shutdown" and not
"noconnection".

`Membership` is how "the lease expired" becomes a cluster-wide verdict on
top of the fast but local link-loss signal, the same split Erlang has
between the `net_kernel` tick and an external registry. A `down` for the
incarnation a node has links to, or an `up` for a newer incarnation, drops
those links: monitors across them fire `Down{noconnection}` and pending calls
fail with the cause ("left the cluster", "restarted as incarnation N"). It
catches a crashed peer behind a half-open connection even when keepalive is
not configured.

### grpcproctest

```go
c := grpcproctest.New(t, "a", "b", "c")     // three nodes over bufconn
c.Partition("a", "b")                    // links between a and b break, monitors fire
c.Heal("a", "b")
c.Kill("b")                              // node gone; incarnation changes on Restart
c.Restart("b")
```

## Observability (v0.1 core surface, v0.2 tools)

This is the part taken from ergo. Its Observer, REST API and MCP server are
thin clients of a `system` application every node runs, which answers: what
processes exist, what is in their mailboxes, what does each process say about
itself, and what is happening on the wire. `grpcproc` builds that answering surface
into the core and leaves the clients (UI, CLI, MCP) as separate, optional
programs. The rules:

1. **Counters are always on and cheap.** Atomics on the process and the link,
   read by snapshot. No sampling, no configuration.
2. **Nothing in the core imports a metrics or tracing library.** A `Hooks`
   interface is the single tap; `grpcproc/otel` implements it.
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
believes. In `grpcproc`:

```go
node.Spawn(fn, grpcproc.WithInspect(func() map[string]string {
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
    OnSend(SendInfo, Metadata) (Metadata, Done)       // before a send or call leaves
    OnReceive(ReceiveInfo, Metadata) (Metadata, Done) // when a process takes a message or Down
    OnDeadLetter(from, to PID, body proto.Message, reason string)
    OnLinkUp(NodeID)
    OnLinkDown(NodeID, error)
}
type Done func(err error)
```

`OnSend` and `OnReceive` return the metadata to use from then on and a
`Done` that closes what they started: a send once handed to delivery, a call
once it returns, the handling of a message at the process's next `Receive`
or its exit. A process remembers the metadata of the message it is handling
(after `OnReceive`), and its own sends and calls inherit it; a call's ctx
can add to it. That is ergo's "the outgoing message inherits the trace", and
it is what lets a tracer build causal chains without the
application threading a context through every handler: `OnReceive` stamps
the consumer span, and everything the handler sends is its child.
`JoinHooks` combines several; metadata threads through them in order and
`Done`s run in reverse.

Nil by default; a nil check per message when unset. This is the same shape as
`grpc.StatsHandler`, and it is what every observability feature in other
frameworks reduces to:

| Feature | ergo | GoAkt | grpcproc |
|---|---|---|---|
| Metrics | Observer charts | `WithMetrics()` OTel gauges | `grpcproc/otel` on `Hooks` + `Processes()` snapshots, one series per **label** by default (GoAkt's per-actor default is a cardinality trap it later added a switch for) |
| Dead letters | log | dead-letter actor + event | `OnDeadLetter` + counter in `NodeInfo` |
| System events | `gen.CoreEvent` | event stream | `OnSpawn/OnExit/OnLinkUp/OnLinkDown`; `Node.Events()` channel is a thin subscriber over the same hooks |
| Tracing | Sent / Delivered / Processed observations, trace id in the message | eBPF sidecar | `Envelope.metadata` carries W3C trace context. `grpcproc/otel` opens a producer (send) or client (call) span in `OnSend` and a consumer/server span covering the handling in `OnReceive`; a process's sends inherit the handling span, so chains form without threading a context. Sampling stays the tracer's job |
| Logging | loggers as processes, per-process level | — | `p.Log()` is `slog` with pid/name/label; per-process level via a `slog.Handler` wrapper the node owns, settable at runtime |

### Events

`node.Subscribe(ctx, buffer) <-chan Event` streams spawns, exits (with the
reason), links up and down, and dead letters. Publishing never blocks the
node: a full subscriber loses the event and the next one it receives carries
`Missed`, the count lost in between. With no subscriber, the cost is one
atomic load at each point that would publish. The list of subscribers is
copy-on-write, so publishing takes no lock but the per-subscriber one that
guards against a concurrent close.

### Inspector service (`grpcproc/inspect`, done)

A second gRPC service registered on the same server, optional:

```proto
service Inspector {
  rpc GetNode(GetNodeRequest) returns (GetNodeResponse);
  rpc ListProcesses(ListProcessesRequest) returns (ListProcessesResponse); // name, label, state, min mailbox
  rpc GetProcess(GetProcessRequest) returns (GetProcessResponse);          // ProcessInfo + WithInspect map
  rpc SetLogLevel(SetLogLevelRequest) returns (SetLogLevelResponse);
  rpc Send(SendRequest) returns (SendResponse);                            // Any body, from a tool
  rpc Exit(ExitRequest) returns (ExitResponse);
  rpc Watch(WatchRequest) returns (stream WatchResponse);                  // Node.Subscribe over the wire
}
```

Every request names a node. One that is not this node is forwarded to that
node's Inspector, which `inspect.WithResolver` dials over the node's own
resolver and dial options, one connection per peer (`WithPeers` takes any
other `PeerFunc`). That is ergo's "run
Observer on one node and inspect the whole cluster". A process targeted by
PID routes to the PID's node when the request names none. `GetProcess` with
`inspect: true` returns the snapshot even when the process is too busy to
answer, with `inspect_error` saying so. Access control is the application's
(interceptors, mTLS), as for any of its other services.

What sits on top, later and outside the core: `grpcprocctl ps / top / inspect /
send`, a `Node.DOT()` that draws processes and monitor edges across nodes
(cheap, and consistent with `fsm.DOT`), and an MCP server exposing the same
methods for an agent — ergo's MCP experience is convincing, and it is a
half-day of work once the gRPC service exists.

Goroutine dumps and heap profiles are `net/http/pprof`; `grpcproc` does not
duplicate them.

## Helpers (`grpcproc/actor`, done)

Optional, built only on the public core API, so users can ignore or replace
them. Two primitives went into the core because they need process internals:

- `p.SendAfter(d, to, m) *Timer`: owned by the process and cancelled when it
  exits. The message carries the metadata the process held when it was
  scheduled, so a timer set while handling one request is not attributed to
  whichever request is being handled when it fires. For that, inheritance
  moved from the send internals to the public `Send`/`Call` methods.
- `p.Spawn[N](fn, …)` / `p.SpawnMonitor[N](fn, …)`: a child recorded with
  `p` as its parent, for inspection; the second is Erlang's `spawn_monitor`.
  The monitor exists before the child runs, so a child that exits at once is
  reported with its real reason instead of `noproc`. A supervisor that
  monitored after spawning would misread a transient child's instant normal
  exit as abnormal and restart it in a loop.

`grpcproc/actor`:

- `actor.Run(h)`, spawned as `n.Spawn(actor.Run(h))`: the handler loop.
  `Handler[M]` has `HandleMessage`; `CallHandler`, `DownHandler`,
  `Initializer`, `Terminator` are optional interfaces, found by type assertion
  once. A call-only actor embeds `CallsOnly[M]`, whose `HandleMessage` logs
  and drops what is sent without a call: gen_server's default `handle_info`,
  so that a stray sender cannot crash the actor. It is not a dead letter,
  because the message was delivered. An error from `HandleCall` is the
  reply and the actor carries on; from `HandleMessage` it is the exit
  reason. `ErrStop` ends normally (replying first, from a call);
  `ErrNoReply` defers the answer. `Terminate` also runs on a panic, which then
  continues so grpcproc reports it.
- `actor.Supervise(n, Spec)`: one-for-one, one-for-all, rest-for-one;
  permanent, transient, temporary children; restart intensity (`MaxRestarts`
  within `Within`, default 3 in 5s). Giving up is exit reason
  `max restarts`, abnormal, so a parent supervisor restarts the child
  supervisor: escalation. Children are registered under their names, so
  addresses survive restarts; `Child` takes a factory so each start gets a
  fresh handler. Only children that were running come back with their group:
  a transient child that finished stays finished. Stopping a child is
  Demonitor, Exit, then waiting on node events (not the mailbox, which is
  closed once the supervisor itself has been told to exit), bounded by
  `Spec.Shutdown`; grpcproc cannot kill a goroutine that ignores Exit, so such
  a child is left behind and logged. A restart that cannot start counts
  against the intensity, so it ends rather than loops. The supervisor's state
  is published through `WithInspect`.

## Later

- A **global name registry** (`grpcproc.Global{"ledger"}` resolving through
  etcd, with a lease as fencing token), on top of `grpcproc/etcd`.
- Cross-node pub/sub with a replay buffer (ergo's events). Useful; not core.
- Delivery beyond at-most-once, in order per sender. Explicitly out of scope;
  build it above `grpcproc`, as OTP does.
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

1. ~~`proto/grpcproc/v1`, core `Node`/`Process`, links, static resolver, `ProcessInfo`
   counters, `Hooks`, `WithInspect`, `grpcproctest`~~ (done). Tests: ordering, monitors with
   every reason, node down, restart with new incarnation, bad peer identity.
2. ~~`grpcproc/inspect` service and Go client~~ (done, with `Node.Subscribe`);
   ~~`grpcproc/actor` helpers~~ (done, with `SendAfter` and `SpawnMonitor`).
3. ~~`grpcproc/otel` (metrics + trace propagation)~~ (done: see otel/README.md),
   ~~`grpcproc/etcd`~~ (done: see etcd/README.md).
4. ~~`grpcprocctl`, `DOT`, MCP server~~ (done: `grpcproc/tools`, see tools/README.md).

Coverage target and style follow `fsm` and `di`: 100 % on the core,
race-detected, examples compiled in CI, `DESIGN.md` kept current.
