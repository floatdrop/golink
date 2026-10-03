# grpcproc — design

`grpcproc` gives goroutines Erlang-style network transparency on top of the gRPC
server a service already runs. A process is addressed by a PID or a name; the
same `Send`, `Call`, `Monitor`, `Link` and `Exit` work whether the target is
in this binary or on another node.

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

This document describes how grpcproc works and why, what was looked at in
other frameworks to decide it (ergo.services, Proto.Actor, GoAkt, Hollywood,
go-actor, Erlang/OTP, Akka, Orleans), what was rejected, and what is still
open.

## The shape at a glance

```go
node, err := grpcproc.NewNode(grpcproc.Config{
    Name:        "orders-1",
    Advertise:   "10.0.0.5:9000",           // where *your* gRPC server listens
    Resolver:    grpcprocetcd.New(cli, "/grpcproc"), // required; grpcproc.StaticResolver for a fixed map
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
        switch m.Body.Kind.(type) {
        case *orderspb.OrderMsg_Reserve: m.Reply(&orderspb.Reserved{}, nil)
        }
    }
}, grpcproc.WithName("reservations"), grpcproc.WithLabel("order"))

// From any node: the node itself, or a process (inside its function), sends.
addr.Send(ctx, node, &orderspb.OrderMsg{Kind: &orderspb.OrderMsg_Reserve{}}) // compile-time typed
resp, err := addr.Call[*orderspb.Reserved](ctx, node, &orderspb.OrderMsg{})  // reply typed by R

// Inside a process: watch something on another node.
ref := p.Monitor(grpcproc.Name{Node: "billing-2", Name: "ledger"})
```

## Package layout

```
grpcproc/                    core: Node, Process, PID, Send/Call/Monitor/Link/Exit, node links, inspection API
grpcproc/proto/grpcproc/v1       wire protocol (.proto + generated code); inspect/v1, actor/v1, pubsub/v1 beside it
grpcproc/grpcproctest            in-memory clusters over bufconn: Cluster, Partition, Kill
grpcproc/inspect             grpcproc.inspect.v1.Inspector gRPC service (optional to register); its Go client is tools/client
grpcproc/actor               optional helpers: handler loop, supervisor
grpcproc/pubsub              optional: topics with a replay buffer, relayed once per node
grpcproc/cron                optional: jobs on crontab schedules, every run a process
grpcproc/leader              optional: leader election, and a singleton that runs on the leader with its state
grpcproc/etcd     (nested module)   Resolver + Registrar + Membership on etcd leases
grpcproc/otel     (nested module)   Hooks implementation: OTel metrics + trace propagation
grpcproc/tools    (nested module)   grpcprocctl over the Inspector: CLI, Graphviz, MCP server, web UI
grpcproc/examples (nested module)   runnable examples and the shop the site's tutorial builds
grpcproc/benchmarks (nested module) grpcproc against GoAkt, Hollywood, Proto.Actor and Ergo
site/                            the documentation site
```

`grpcproctest` is part of the core module: the best argument for network
transparency is that a three-node scenario, including a node dying, runs in a
plain `go test` with no sockets and no etcd.

## Core

### Identity

```go
type PID  struct { Node string; Incarnation uint64; ID uint64 }
type Name struct { Node, Name string }
type Ref  struct { Node string; ID uint64 }   // monitor reference
```

`Incarnation` is a per-start number that grows with each start
(`Config.Incarnation`, by default the start time in nanoseconds). A PID from before a node restart is a different process: it gets
`Down{noproc}`, never a message delivered to a stranger. Erlang has creation
numbers for the same reason; Proto.Actor and GoAkt do not, and both have issues
about stale references after restarts.

Incarnations also fence links. Two processes can claim one node name at
once: an instance that was replaced but still runs, after a partition
heals, or two deploys given one name. If a link from either replaced the
other's, each would take its peers down for the other, and the two would
knock each other off their links for as long as both ran. So a node
remembers, per peer name, the newest incarnation it has seen, over a link in
either direction or from `Membership`, and refuses one older than that: an
inbound link before its `Hello`, so the old instance's dial fails and backs
off, and a dial that reaches one, through an address that still points at
it. A newer incarnation's links replace the older one's, which go first,
with "restarted as incarnation N". The node forgets the newest when
`Membership` reports it gone, when an older incarnation is the only one
left, or on `Disconnect`, and then lets an older one in. The rule needs
incarnations that grow with each start; the default, the start time, does
as long as clocks agree to within the time between two starts, and a random
one would be refused whenever it came out lower than the last. It fences grpcproc traffic only: an old instance
can still write to a database, which needs fencing of its own.

Names are per node. Names of the whole installation are global names (see
Global names), claimed by a process and kept in a store, deliberately
separate from local registration, as Erlang's `global` is.

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
  int64 timeout_nanos = 13;                          // a call's time left, not its deadline
  map<string,string> metadata = 15;                  // trace context, tenant …
  bool watch = 16; uint64 watched_id = 17;           // a call's watch, and what its reply placed it on
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
  call fails with `ErrNoConnection`: the peer may have handled it. A new
  inbound stream from the peer waits until then, so the old session's
  `Down`s come before anything the new one carries. Calls
  still queued on the broken link, never written, fail at once as `Unsent`
  instead, and their messages, like those in a frame being written, become
  dead letters. Only a `LinkError` whose `Unsent` is set is known safe to
  retry: the message never left this node. A call that ends with its ctx may
  have been handled too, and so may one still waiting when its node stops,
  which fails with `ErrNodeStopped`.
  `Unsent` is the field, not `Sent`, so that a `LinkError` built without it
  claims nothing. gRPC keepalive on both sides (client
  `keepalive.ClientParameters`, server `keepalive.ServerParameters`) is what
  turns a silent partition into a stream error in seconds; the library does
  not set it, the application's gRPC configuration does.
- **Both ends count the sessions that ended.** Declaring a peer down ends
  the session with it: its watches on this node's processes are dropped,
  this node's monitors of its processes fire. The peer learns of it when its
  links from this node end, which they then do, except when it had none, or
  it opened one that this node had ended by the time it came up: a dial
  under way when this node declared it down, whose Hello was sent before.
  The peer would then send a `Down` on a link this node no longer reads,
  or wait for one from a watch this node no longer holds, for good. So each
  node counts, per peer, the sessions that ended, and says how many on each
  link: the dialer in its metadata, the server in its `Hello`, the larger
  of its count and the dialer's. A node that hears a larger count than its
  own ends the session it thought was current and takes the count, without
  counting one more, so that the two agree; a dialer that hears a smaller
  one dialed in a session that has ended since, and drops the link, without
  backing off (the dial did not fail). `Node.Disconnect` drops a dial under
  way outright. A new incarnation counts from 0. `LinkInfo.Sessions` shows
  the count: one that keeps growing is a peer whose links keep breaking.
- **A failed dial backs off.** `Config.DialTimeout` (5s by default) bounds a
  dial, and after one fails, everything routed to that peer fails at once
  with `ErrNoConnection` for a while, rather than each wait out a dial of its
  own: a process sending to a dead or hung node would otherwise stall for
  `DialTimeout` per send. The wait starts at a 32nd of `Config.DialBackoff`
  (5s by default) and doubles to it, with jitter. Then one send dials again
  while the others keep failing, so a hung peer holds one sender at a time:
  a half-open breaker. `Membership` reporting the peer up (not as an older
  incarnation) ends the wait, and so does `Node.Disconnect`. A link the peer opens lets the next send dial at once but
  keeps the doubling: it shows the peer is up, not that this node can reach
  it, and in a one-way partition every reply would otherwise wait out a dial
  again. A link that ends within `DialTimeout` of coming up counts as a
  dial that failed, after the wait its own dial came after, so the doubling
  goes on: a path that lets a dial through and then breaks, a proxy cutting
  streams, a peer cutting each link for the replies it cannot route back,
  backs off as one that cannot be dialed does, rather than redial at every
  send. `LinkInfo` shows the peer as a down outbound link with its
  `RetryAt`. Retries are not part of it, since delivery stays at-most-once.
- **A link's queue can be bounded.** It is unbounded by default, as Erlang's
  distribution buffer is. `Config.MaxQueued` and `Config.MaxQueuedBytes`
  bound what a link holds, the envelopes queued or being written and the
  bytes of their bodies, which is what `LinkInfo.Queued` and `QueuedBytes`
  count. A message or a call to a peer whose link is full fails at once, as a `LinkError` whose
  `Err` is `ErrLinkBusy` and whose `Unsent` is set, and the sender decides:
  drop it, send it again later, or shed the load behind it. Each peer has a
  link of its own, so this is not the bounded mailbox that was rejected: a
  peer that cannot keep up refuses what is sent to it, and holds up no one
  else. Erlang suspends a sender on a busy distribution port (`+zdbbl`);
  grpcproc fails the send instead, since a sender that blocks on a peer can
  deadlock with it. Replies, `Down`s, monitors and exits are queued
  regardless, and count: a refused reply or `Down` would have to cut the
  peer's link (below), and the others are few. Past the link, gRPC's flow
  control bounds what the transport holds.
- **A reply or a `Down` that cannot be routed cuts the peer's link.** The peer
  waits for those over its own link to this node, which stays up when this
  node cannot reach it back, so it would never learn that one was lost: a
  monitor that never fires. Ending the peer's link, with a status that says
  this node cannot reach it back, makes it see this node as unreachable, as
  Erlang's single connection would: its calls fail and its monitors fire with
  `noconnection`.
- **A graceful `Stop` flushes before it closes.** Processes exit first and
  their `Down{shutdown}` envelopes are queued; then every outbound link
  flushes and half-closes at once, each is waited on until its peer ends the
  stream or Stop's ctx ends, and only then is its connection closed. A peer
  that never ends its stream holds its own link to the deadline, not the
  others. The inbound handler never selects on the stream's own
  context: a peer's cancel is observed through `Recv`, after every envelope
  that preceded it. The goroutine reading a link dispatches each frame
  itself; a per-link lock held while dispatching and while closing means
  nothing from a closed link is dispatched after its `Down{noconnection}`.
  So dispatch never waits for a dial: the answers it makes itself (no such
  process, wrong type) to a peer this node has no link to yet are queued on
  the dial, and written first, in order, once it is up.
- **Node identity travels in the stream's metadata** (`name`, `incarnation`,
  protocol version). `Config.Admit(ctx, peer NodeID) (Policy, error)`, with
  the peer's transport credentials in ctx (`grpc/peer`), lets mTLS
  deployments refuse a node whose certificate does not match the name it
  claims, and says what an admitted one may ask (see Admission).
- **Connections to one peer can have options of their own.**
  `Config.DialOptionsFor(peer)` adds to `DialOptions` for that peer's link
  and for `Node.Dial`, after them, so that they win: the credentials of
  another installation, whose nodes present certificates of another CA.
- **Bodies are `proto.Message`, sent as their type's full name and their
  encoding.** Generated types register themselves, so there is no
  `Register()` step. Local sends pass the pointer
  without copying (fast; "do not mutate after send"); `Config.CopyLocal` clones
  for teams that want strict isolation.
- **Calls ride the same link**, matched by a call id, never as separate unary
  RPCs, so they cannot overtake or be overtaken by messages from the same sender.
- **A call carries its caller's deadline**, as gRPC's `grpc-timeout` does: the
  time left when it is written, not the deadline, so that clocks need not
  agree, counted again from when it arrives, which makes the callee's
  deadline the caller's or a little later. A local call passes the deadline
  itself. The callee sees it as `Msg.Deadline`, and `Msg.Context` ends then,
  so work for a caller that gave up, a deferred reply's above all, can stop.
  It is not applied to the callee's own sends and calls, as metadata is:
  a callee may have to finish what it started for a caller that stopped
  waiting (the shop's desk takes an order whose client hung up), so it
  passes the message's context on when it wants the bound. Cancellation does
  not travel, only the deadline.

### Typed processes

The type of a process's messages lives on its **address**, so it survives the
network boundary and every send is checked by the compiler:

```go
type Addr[M proto.Message] struct { /* PID or Name, plus the phantom type M */ }

func (n *Node) Spawn[M proto.Message](fn func(*Process[M]) error, opts ...SpawnOption) (Addr[M], error)
func Named[M proto.Message](node, name string) Addr[M]   // remote by name; checked on delivery
func AddrOf[M proto.Message](t Target) Addr[M]           // a PID or a Name, typed
func (a Addr[M]) PID() PID
func (a Addr[M]) Node() string
func (a Addr[M]) Name() string
func (a Addr[M]) Call[R proto.Message](ctx, from Caller, req M) (R, error) // from: a *Node or a *Process
func (a Addr[M]) CallMonitor[R proto.Message](ctx, from Caller, req M) (R, Ref, error) // from a process; see Remote spawn
func (a Addr[M]) CallLink[R proto.Message](ctx, from Caller, req M) (R, error)
func (a Addr[M]) Send(ctx, from Caller, m M) error

func (p *Process[M]) PID() PID
func (p *Process[M]) Addr() Addr[M]
func (p *Process[M]) Node() *Node
func (p *Process[M]) Receive() (Msg[M], error)
func (p *Process[M]) ReceiveTimeout(d time.Duration) (Msg[M], error)
func (p *Process[M]) CallTo[R proto.Message](ctx, to Target, req proto.Message) (R, error)
func (p *Process[M]) SendTo(to Target, m proto.Message) error
func (p *Process[M]) SendAfter[N proto.Message](d time.Duration, to Addr[N], m N) *Timer
// Node has SendTo / CallTo / Exit too, with the node as sender; its SendTo
// and Exit take a ctx, for the dial (SendTo also carries its metadata).
func (p *Process[M]) Monitor(to Target) Ref
func (p *Process[M]) Demonitor(ref Ref)
func (p *Process[M]) Link(to Target)           // one way: to's exit ends p
func (p *Process[M]) Unlink(to Target)
func (p *Process[M]) SetTrapExit(trap bool)    // links' exits arrive as messages
func (p *Process[M]) TrapExit() bool
func (p *Process[M]) Parent() PID              // the spawning process, if any
func (p *Process[M]) Exit(to Target, reason string) error
func (p *Process[M]) Log() *slog.Logger        // pid and label attrs attached
func (p *Process[M]) Context() context.Context // cancelled on Exit, a link's exit, node stop

type Msg[M proto.Message] struct {
    From     PID
    Body     M        // zero when Down or Exited is set
    Down     *Down    // set when a monitored process is gone
    Exited   *Exited  // set when a linked process is gone, if p traps exits
    Metadata Metadata
}
func (m Msg[M]) IsCall() bool
func (m Msg[M]) Reply(resp proto.Message, err error) error // may be deferred, from any goroutine
func (m Msg[M]) Deadline() (time.Time, bool)   // when a call's caller stops waiting
func (m Msg[M]) Context(parent context.Context) (context.Context, context.CancelFunc) // metadata, ending then
func (m Msg[M]) Watch(pid PID) error            // a CallMonitor's watch, on a process that runs
```

`M` is whichever of these fits:

| `M` | Send site | Use |
|---|---|---|
| a generated oneof wrapper, `*orderspb.OrderMsg` | `a.Send(ctx, p, &orderspb.OrderMsg{Kind: …})` | one contract per process, visible in the `.proto` |
| your own marker interface, `OrderMsg interface{ proto.Message; orderMsg() }` | `a.Send(ctx, p, &orderspb.Reserve{})` | several generated types, no wrapper, one extra method each |
| `proto.Message` | `p.SendTo(a, anything)` | the untyped process |

A typed send or call goes through the address, whose `M` fixes the type: a
message of any type that implements it goes as it is. The Node and the
Process have only the untyped `SendTo` and `CallTo`, for a PID or a `Msg`'s
`From`; typed methods of their own, which were a second way to write every
send, went, so that there is one.

`Addr.Call` and `Addr.Send` take the sender as an argument, a `Caller`: the
`*Node` or a `*Process`. A contract package uses them to write its protocol
as methods on an address type of its own, so the reply type of each
operation, and the oneof around its request, are named once, in the package,
instead of at every call site:

```go
type StockAddr struct{ grpcproc.Addr[*Command] }

func (s StockAddr) Reserve(ctx context.Context, from grpcproc.Caller, r *Reserve) (*Reserved, error) {
    return s.Call[*Reserved](ctx, from, &Command{Op: &Command_Reserve{Reserve: r}})
}

reserved, err := stock.Reserve(ctx, p, &Reserve{Sku: "apple", Qty: 2}) // p, or a node
```

`Caller` has one unexported method, because an interface cannot declare a
generic one: a `*Node` and a `*Process` could share no `Call[R]`, so the
address calls, and takes the sender. The
sender is an argument, not bound into the address, because who sends matters:
a process's call carries the metadata of the message it is handling and
shows the process waiting on a reply, and an actor keeps its addresses in
fields but has its process only inside a handler.

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
concern; the mailbox depth, and each link's queue, are visible (see
Observability) so it can be one. A link's queue is its peer's alone, so it
can be bounded as well (`Config.MaxQueued`, under Wire protocol).

Exit reasons: `normal`, `noproc`, `noconnection`, `shutdown`, `killed`, a
panic (`panic: …` with the stack logged), or the error string the function
returned.

### Links

A link is one way, as in ergo: after `p.Link(b)`, `b`'s exit ends `p`, with
`b`'s reason, whatever it is, `normal` included; `p`'s exit leaves `b` alone,
and `b` links to `p` for the other way. A link says "this process cannot go
on without that one", so a normal end of the target ends the linker too:
a child linked to a supervisor that was stopped cleanly must still go. The
linker takes the reason as it is, so a transient child whose dependency
ended `normal` or `shutdown` ended normally too, and is not restarted.

- **On the wire, a link is a monitor.** Only the linker's node tells the two
  apart: when the `Down` arrives, a monitor's is queued as a message, a
  link's ends the process (its context is cancelled with an `*ExitError`,
  as `Exit` does). So everything monitors get, links get: `noproc` for a
  process that does not exist, `noconnection` when its node cannot be
  reached or the connection breaks, stale incarnations, ordering behind the
  target's last messages. A peer on an older version needs nothing new.
- **Trapping exits.** `p.SetTrapExit(true)` turns links' exits into messages,
  `Msg{Exited: &Exited{PID, Name, Reason}}`, in order with the rest. A
  request to exit, `Exit` from a process or `Node.Exit`, is never trapped: a
  goroutine cannot be killed, so `Exit` is the one request a process cannot
  refuse by configuration. That is where grpcproc parts from Erlang and
  ergo, whose explicit exit signals trap like a link's.
- **Spawning linked.** `LinkParent()` links the child to its parent and
  `LinkChild()` the parent to the child, both inside the critical section
  that admits the child, as `SpawnMonitor` does its monitor, so neither
  side's exit is missed however soon it comes. Both together are Erlang's
  `spawn_link`. A process of another node links to a child as `LinkChild`
  does with `CallLink` (see Remote spawn).
- **The parent's exit ends an actor even when it traps exits**, as it ends a
  gen_server and an ergo actor. That rule is in `actor.Run`, not the core: a
  raw process that traps exits decides for itself.

Two-way links, Erlang's, were the alternative. They need Erlang's rule that
a non-trapping process ignores a `normal` exit, or a helper that finishes
would take its parent with it, and so a child whose supervisor stops
cleanly is not stopped by the link. They also need both nodes to agree:
Erlang's link protocol gained unlink ids and acknowledgements in OTP 23 to
settle races between link, unlink and crossing exits. A one-way link is a
monitor, which each side already handles alone.

### Admission

```go
type Op uint8 // OpSend, OpCall, OpMonitor (a link travels as one), OpExit
type Policy func(op Op, name string) bool

Admit func(ctx context.Context, peer NodeID) (Policy, error) // in Config
func Export(names ...string) Policy
```

An admitted peer may otherwise ask anything of any process, `Exit`
included, which a process cannot trap: right for the nodes of one
installation, wrong for a partner's or a tenant's. So `Admit` both refuses
a link and, when it admits one, returns what the peer may ask over it.

- **One hook, once per link.** Whether a peer may link and what it may do
  come from the same evidence, its certificate or its name, so they are one
  decision, made where the link is accepted; two hooks would read the
  certificate twice and could disagree. The `Policy` is kept on the inbound
  link and judges every envelope the peer sends on it; a nil one, as a nil
  `Admit`, is a nil check per envelope and nothing else.
- **By the name of the process.** A request by name is judged by that name,
  one by PID by the name the process was spawned with, "" if none. A name
  is what a node offers others; a PID is not a capability, since
  incarnations and ids can be guessed. `Export(names…)` is the usual
  policy: those names, by name or PID, for sends, calls and monitors, and
  no exits.
- **Refused is not there.** What a policy refuses is answered as for a
  process that does not exist: a call fails with `ErrNoProc`, a monitor or
  a link gets `Down{noproc}`, and a message or an exit is dropped. The peer
  learns nothing of what this node runs that it may not reach, and nothing
  on the wire changed, so a peer of any version gets the answers it knows.
  This node counts a dead letter with reason `denied` (`ReasonDenied`), for
  `OnDeadLetter` and the events, so a wrong export list shows in
  `grpcprocctl watch`.
- **Answers always pass.** Replies and `Down`s answer what this node asked,
  and a demonitor takes back what the peer placed; none is judged. A
  process here can call and monitor any process of the peer, whatever the
  peer may reach here.
- **Only the links.** The Inspector is a second gRPC service, guarded by the
  server's interceptors and `inspect.ReadOnly()`, not by a node's `Policy`.

### Discovery interfaces

```go
type Resolver interface { Resolve(ctx, node string) (addr string, err error) }
type Member struct { Name string; Incarnation uint64; Addr string; Metadata map[string]string }
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
not configured. An `up` for an older incarnation than a node has seen is
ignored, as a link from it would be refused.

**Member metadata.** `Config.Metadata` describes one start of a node: the
application's version, its zone, whatever placement or a rolling deploy
decides by. grpcproc reads none of it. `Start` registers it with the
node's `Member`, so a `Membership` carries it to every node, and
`Node.Members()` is what the node's `Membership` reports up, each member
with its metadata: the newest incarnation of each name, this node
included, as current as the events. It is the input for choosing where a
child goes (the newest version, the caller's zone) without a lookup of its
own; `NodeInfo.Metadata` and the Inspector show a node's own, so a tool
sees a rollout's progress node by node.

- **Fixed for the node's life.** A version or a zone does not change while
  a process runs, and metadata that did would need publishing again and
  ordering against the links. A change is a new start, whose newer
  incarnation replaces the old member everywhere, as it already does.
- **Not on the links.** It reaches peers through `Membership`, not the
  `Hello`: a node needs the metadata of nodes it has no link to, to choose
  among them, and without a `Membership` the Inspector still shows each
  node's own. The wire is unchanged.
- **Not identity.** A node claims its metadata; nothing checks it. `Admit`
  decides by what transport credentials prove, not by what a peer says it
  is.

### grpcproctest

```go
c := grpcproctest.New(t, "a", "b", "c")     // three nodes over bufconn
c.Partition("a", "b")                    // links between a and b break, monitors fire
c.Heal("a", "b")
c.Kill("b")                              // node gone; incarnation changes on Restart
c.Restart("b")
```

Its nodes dial again at once after a failed dial (`DialBackoff` is negative),
so the send right after `Heal` or `Restart` reaches the peer.

Nodes and clusters run inside a `testing/synctest` bubble as they are, and
tests in the core, `actor`, `grpcproctest`, `pubsub`, `cron` and `leader` hold
them to it. A process
waits on channels the bubble sees, so `synctest.Wait` returns once every
process waits, and timers (`SendAfter`, `ReceiveTimeout`, deadlines, a
supervisor's restart window and `Shutdown`) keep the bubble's fake clock;
`grpcproctest`'s bufconn connections are channels too. That is the whole of
deterministic testing: no clock interface in `Config`, no mock node. One
default had to change for it: a bubble's clock stands still until every
goroutine waits, so the default incarnation, the start time, is now one
more than the last this program picked when the clock has not moved, and
a node made again under the same name still gets a newer one.

## Observability

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
3. **The Go API is the source of truth.** The gRPC `Inspector` service is the
   node's public API over the wire (`Info`, `Processes`, `Subscribe`,
   `SendTo`, `CallTo`, `Exit`, a process's log level), and tools use it and
   nothing else: what `grpcprocctl` shows of a supervisor, a cron process or
   an elector, it gets by calling that process, as any other caller would.

### Process snapshot

What ergo's process table shows (identification, messaging, lifecycle) and
GoAkt's `pid.Metric()` returns, as one struct:

```go
type ProcessInfo struct {
    PID        PID
    Name       string
    Label      string            // WithLabel; the low-cardinality key for metrics
    Type       string            // the Go type of M, for display
    Parent     PID               // zero for Node.Spawn
    State      ProcessState      // Idle | Running | WaitingReply | Exiting
    StartedAt  time.Time
    Mailbox    MailboxInfo       // Depth, OldestAge (latency), Peak
    Received   uint64            // messages taken from the mailbox
    Sent       uint64
    CallsInFlight uint32
    LastMessage string           // proto full name of the last body handled
    Monitors   int               // held by this process
    Links      int               // process links held by this process
    Watchers   int               // processes monitoring or linked to this one
    Wakeups    uint64            // Receive returns
    LogLevel   slog.Level
    TrapExit   bool
}

func (n *Node) Processes() []ProcessInfo          // ordered by PID.ID
func (n *Node) Process(pid PID) (ProcessInfo, bool)
func (n *Node) Info() NodeInfo                   // name, incarnation, metadata, uptime, counts, Links []LinkInfo
```

`LinkInfo` per peer: state, established at, reconnects, sessions ended,
messages and bytes in and out, envelopes waiting to be written, last error,
and when a peer whose dials fail is dialed again. Ergo's network charts are drawn from exactly
these.

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
| Metrics | Observer charts | `WithMetrics()` OTel gauges | `grpcproc/otel` on `Hooks` + `Processes()` snapshots, one series per **label** and never per PID (GoAkt's per-actor default is a cardinality trap it later added a switch for); a PID's own numbers are in `Processes()` and the Inspector |
| Dead letters | log | dead-letter actor + event | `OnDeadLetter` + counter in `NodeInfo` |
| System events | `gen.CoreEvent` | event stream | `OnSpawn/OnExit/OnLinkUp/OnLinkDown`; `Node.Subscribe(ctx, buffer)` is a channel of the same events |
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

### Inspector service (`grpcproc/inspect`)

A second gRPC service registered on the same server, optional:

```proto
service Inspector {
  rpc GetNode(GetNodeRequest) returns (GetNodeResponse);
  rpc ListProcesses(ListProcessesRequest) returns (ListProcessesResponse); // name, label, state, min mailbox
  rpc GetProcess(GetProcessRequest) returns (GetProcessResponse);          // ProcessInfo + WithInspect map
  rpc SetLogLevel(SetLogLevelRequest) returns (SetLogLevelResponse);
  rpc Send(SendRequest) returns (SendResponse);                            // Any body, from a tool
  rpc Call(CallRequest) returns (CallResponse);                            // Node.CallTo from a tool; an error answer is Unknown
  rpc Exit(ExitRequest) returns (ExitResponse);
  rpc Watch(WatchRequest) returns (stream WatchResponse);                  // Node.Subscribe over the wire
}
```

Every request names a node. One that is not this node is forwarded to that
node's Inspector, which the server dials as the node does its peers,
`Node.Dial`, over the node's own resolver and dial options, one connection
per peer; `inspect.WithResolver` takes other ones, `WithPeers` any other
`PeerFunc`, and `WithPeers(nil)` none. That is ergo's "run
Observer on one node and inspect the whole cluster". A process targeted by
PID routes to the PID's node when the request names none. `GetProcess` with
`inspect: true` returns the snapshot even when the process is too busy to
answer, with `inspect_error` saying so. Access control is the application's
(interceptors, mTLS), as for any of its other services; `inspect.ReadOnly()`
refuses `SetLogLevel`, `Send`, `Call` and `Exit` with `PermissionDenied`, for
an Inspector that should only be looked at.

What sits on top, outside the core, is `grpcproc/tools`, one binary,
`grpcprocctl`, with three faces over the same Go client (`tools/client`):

- **A CLI**: nodes and their links, `ps`, `inspect`, `watch`, `exit`,
  `loglevel`; `leader` and `cron`, which read and operate those modules'
  processes by calling them; and `dot`, which draws processes and who started
  whom across nodes. The command list is in `tools/README.md`.
- **An MCP server** (`grpcprocctl mcp`) for an AI agent, ergo's most
  convincing tool: the CLI's reads as tools, and writes (exit, log level,
  moving and cordoning a leader, cron jobs) only with `--allow-writes`.
- **A web UI** (`grpcprocctl web`), after ergo's Observer: a page embedded in
  the binary, served on the operator's machine, that shows the cluster live
  through one node's Inspector, read-only unless `--allow-writes`. It is a
  client like the other two; no node serves a page.

Goroutine dumps and heap profiles are `net/http/pprof`; `grpcproc` does not
duplicate them.

## Helpers (`grpcproc/actor`)

Optional, built only on the public core API, so users can ignore or replace
them. Two primitives went into the core because they need process internals:

- `p.SendAfter(d, to, m) *Timer`: owned by the process and cancelled when it
  exits. The message carries the metadata the process held when it was
  scheduled, so a timer set while handling one request is not attributed to
  whichever request is being handled when it fires. For that, inheritance
  moved from the send internals to the public `Send`/`Call` methods.
- `p.Spawn[N](fn, …)` / `p.SpawnMonitor[N](fn, …)`: a child recorded with
  `p` as its parent, linked to it only with `LinkParent` or `LinkChild`; the
  second is Erlang's `spawn_monitor`.
  The monitor exists before the child runs, so a child that exits at once is
  reported with its real reason instead of `noproc`. A supervisor that
  monitored after spawning would misread a transient child's instant normal
  exit as abnormal and restart it in a loop.
- `Addr.CallMonitor` / `Addr.CallLink`, `WatchedBy(m)` and `m.Watch(pid)`:
  the same for a child of another node, which the call's callee spawns (see
  Remote spawn). The monitor is placed where the child is admitted, which
  only the core can reach.

`grpcproc/actor`:

- `actor.Run(h)`, spawned as `n.Spawn(actor.Run(h))`: the handler loop.
  `Handler[M]` has `HandleMessage`; `CallHandler`, `DownHandler`,
  `ExitedHandler`, `Initializer`, `Terminator` are optional interfaces, found by type assertion
  once. A call-only actor embeds `CallsOnly[M]`, whose `HandleMessage` logs
  and drops what is sent without a call: gen_server's default `handle_info`,
  so that a stray sender cannot crash the actor. It is not a dead letter,
  because the message was delivered. An error from `HandleCall` is the
  reply and the actor carries on; from `HandleMessage` it is the exit
  reason. `ErrStop` ends normally (replying first, from a call);
  `ErrNoReply` defers the answer. `Terminate` also runs on a panic, which then
  continues so grpcproc reports it.
- `actor.Supervise(n, Spec)`: one-for-one, one-for-all, rest-for-one;
  children monitored from before they run, and linked to the supervisor
  (`LinkParent`), a safeguard beside the orderly stop it makes when it ends;
  permanent, transient, temporary children; restart intensity (`MaxRestarts`
  within `Within`, default 3 in 5s). Giving up is exit reason
  `max restarts`, abnormal, so a parent supervisor restarts the child
  supervisor: escalation. Children are registered under their names, so
  addresses survive restarts, or anonymous, with no name; `Child` takes a
  factory so each start gets a fresh handler. Only children that were
  running come back with their group: a transient child that finished stays
  finished. Stopping a child is Demonitor, Exit, then waiting on node events
  (not the mailbox, which is closed once the supervisor itself has been told
  to exit), and looking again every 50ms, since events can be dropped. A
  worker's wait is bounded by `Spec.Shutdown`, or its own; a child
  supervisor's is not (`Infinity`, OTP's default for supervisors), since its
  subtree stops in order.
- **Nothing starts under a name an old process still holds.** OTP kills a
  child that outlives its shutdown, so its restart always finds the name
  free; grpcproc cannot kill a goroutine. Starting anyway fails on the name
  at once, and every retry does too, so one stuck child used to burn the
  whole restart intensity in a millisecond and end the tree to its root.
  Instead the supervisor logs the child, monitors it, and a restart that
  reaches it waits for its `Down`, starting the children after it only then,
  in order, while the supervisor goes on handling its mailbox. Its own end
  waits for such processes too, since whatever restarts it will start the
  same names. An anonymous child holds no name and is waited for by nobody.
  A child that never exits holds its restart for good: the alternative was
  to end the tree, which cannot free the name either. A restart that cannot
  start for another reason counts against the intensity, so it ends rather
  than loops. The supervisor's state is published through `WithInspect`,
  and answered as data to `actor.Children`, OTP's `which_children`, from
  any node.
- `actor.StartChildFrom` asks a supervisor on any node for a child that
  one of its `Spec.Factories` builds from an argument, monitored or linked
  from before it runs with `WithMonitor` or `WithLink` (see Remote spawn).
- `actor.StartChild` adds a child to a running supervisor, and
  `actor.StopChild` stops one for good. A spec holds Go functions, so
  `StartChild` registers it in the actor package and calls the supervisor
  with its id (`grpcproc.actor.v1.Control`): only a supervisor on the same
  node can start it, and the supervisor refuses a caller of another node.
  Both take who asks as a `grpcproc.Caller`, the node or a process, so a
  process asks as itself, with its metadata, and the supervisor as a
  `Target`, a PID or a name. `StopChild` carries a PID and works across nodes. A
  child `StartChild` added is forgotten once it ends for good, so a pool of
  anonymous workers does not grow the supervisor. There is no
  `simple_one_for_one`: a pool is a `OneForOne` supervisor whose children
  `StartChild` adds. `StartChild` is refused while a restart waits, since
  the new child would start before the ones owed a start.
- **`StartChild` of a name that runs answers with that child.** It starts
  nothing and returns the running child's PID with `ErrAlreadyStarted`, as
  OTP's `{error, {already_started, Pid}}` does. Code that starts a child per
  key and keeps a map of them goes stale at the first restart, which the
  supervisor makes without telling it; with this it keeps no map. A child
  that has exited, but whose `Down` the supervisor has yet to handle, has
  freed its name already, since a process frees its name before its `Down`
  goes. So the call is held until the supervisor has handled a `Down`, and
  answered afresh: with the child's restart, or, once the supervisor has
  forgotten it, with a start of the spec.
- **A supervisor that waits long for a child answers calls with
  `ErrBusy`.** It waits outside `Receive`, and so do the supervisors above
  it, each waiting for its subtree; a child that calls its supervisor while
  it exits, from a `Terminate` say, would hold the whole chain until its call
  gave up, or for good. So while it waits it takes what is queued, to handle
  after, and once the wait passes 100ms it answers the calls among it with
  `ErrBusy`: a quick stop is invisible to callers, a stuck one holds none of
  them. Hooks see what it takes as received then, not when it is handled.
- Significant children and `Spec.AutoShutdown` (`AnySignificant`,
  `AllSignificant`; `NoAutoShutdown` by default) are OTP's: a supervisor ends itself, with `shutdown`,
  when its significant children end by themselves for good; one it stops
  does not count. A permanent child cannot be significant.

## Remote spawn

Locally, `SpawnMonitor` places the monitor in the critical section that
admits the child. Across nodes an application could only place a child, by
a call to something on the other node, and monitor it after the answer: a
child that exited in between was `noproc`, the answer given for a process
that never was, for a wrong node, and for a node that restarted since.
The caller could not tell a child that failed from one that was never
there, which is when the reason matters: a placement that failed for a bad
codec is retried elsewhere, a name that does not exist is not. A monitor is
placed by its watcher, so the node starting the child could not place the
caller's, and a proxy process watching on the caller's behalf would cost a
process per child, could itself die, and would not produce a `Down` of the
caller's.

Erlang had the same gap until OTP 23: `spawn_opt(Node, …, [monitor])` was
`badarg`, since a remote `spawn_link` made its link atomically and monitors
had no equivalent. `spawn_request/5` settled it, an asynchronous request
taking `monitor` and `link` and answered with a `spawn_reply` message.
grpcproc has no such asymmetry to reconcile: on the wire a link is a monitor,
so remote spawn with a monitor and with a link are one feature.

- **A call is the request.** The core has no factories and no spawn
  service: whatever answers a call on the target node spawns, a supervisor,
  or an application's own placement actor, where capacity and admission
  belong. The caller asks for a watch with `Addr.CallMonitor` or
  `Addr.CallLink`, which sets `watch` on the call; its ref is the call's, a
  ref of the caller's node like any monitor's. The callee spawns with
  `WatchedBy(m)`, which places the watcher on the child inside the critical
  section that admits it, before it runs, as `SpawnMonitor` places its own,
  and records it on the open call; or places it on a process that runs,
  with `m.Watch(pid)`. The reply names the process, `watched_id`, which the
  caller's node turns into a monitor, or a link, of it.
- **A Down that comes before the answer waits for it.** The child can exit
  before the answer is sent, so its `Down` can come first on the same link.
  The caller's node keeps it with the pending watch, and delivers it once
  the answer is taken, after it: in order with what the child sent, and with
  the real reason. So is a link that breaks between the answer and the
  caller's taking it: the watch worked, and its `Down` is `noconnection`.
- **Only an answer that is not an error brings the watch.** The callee
  takes its watcher back when it answers with an error, exits with the call
  open, or its node stops; the caller drops a pending watch when the call
  fails, its ctx ends, or the answer is of a type it cannot take, and a
  `Down` that came meanwhile with it. A spawn that failed and a child that
  started and exited are then told apart: the first is an error, the second
  a `Down`. A callee that places no watch answers with none, and the monitor
  is `Down` at once with `noproc`, as a monitor of nothing is.
- **Only a process can watch.** `CallMonitor` from the `Node` fails before
  it sends: nothing receives a `Down` for a node.
- **The call is at most once, and synchronous.** A caller whose answer is
  lost, a link that broke or a ctx that ended, holds no watch, and the child
  may have started; its watcher then stays on the child until it exits,
  when its `Down` is dropped. `spawn_request`'s asynchrony answers this in
  Erlang, at the cost of a message to match; here a named child is found
  again: `StartChildFrom` of its name answers `ErrAlreadyStarted` with its
  PID and places the watch on it, as `StartChild` answers with the child
  that runs. An anonymous child has no such handle, and runs until it ends,
  listed by `actor.Children`. Remote spawn is not named-only: a pool of
  anonymous workers asked for from another node is as useful as one asked
  for locally, and the lost answer is the same one a `StartChild` whose ctx
  ends leaves.
- **Factories are a supervisor's.** A spec holds Go functions, so
  `StartChild` works on the supervisor's node only. `Spec.Factories` names
  what a supervisor can be asked for from anywhere: a `ChildFactory[A]` turns
  an argument of type `A`, which travels as an `Any`, into a `ChildSpec` on
  the supervisor's node, closing over its dependencies there. Declared in
  the `Spec`, what a supervisor can be asked to start is visible where what
  it starts is, and outlasts restarts of either node as the `Spec` does,
  with an argument type per name. The factory is where admission goes: its
  error is `StartChildFrom`'s, as it is, so `errors.Is` matches it on the
  caller's node.
- **The monitor is of a process, not of the child's place.** A restart the
  supervisor makes is a new process, which a monitor from before does not
  follow: its `Down` says the old one ended, and `StartChildFrom` of the
  name monitors the new one.

## Pub/sub (`grpcproc/pubsub`)

Built on the public core API only, as `actor` is. A topic is a process: it
keeps its subscribers and its last `Buffer` events, and sends each event it
is given to every subscriber. Each part of it is something the core already
has:

| Feature | grpcproc/pubsub |
|---|---|
| Owning a topic | a topic process under a name; holding its address is the right to publish |
| Publishing | `Topic.Publish`: a send to the topic, which sends on; in order per sender, so per topic |
| Subscribing | `Topic.Subscribe` monitors; a subscriber that wants to exit with the topic links to it |
| Replay | the topic sends what it kept, then answers: a local call's reply does not pass through the mailbox, and a remote one shares the link with the sends, so nothing is missed or seen twice |
| Demand | `Config.Notify` gets a `Demand` on the first subscriber and after the last; the topic monitors its subscribers |
| A topic that ends with its producer | `SpawnOwned` links the topic to its owner; its reason reaches subscribers in their Down |
| A topic for the whole node | `Spawn` on the node; a restart loses subscribers, who see a Down and subscribe again, as with `pg` |
| The node's own events | `Node.Subscribe`, and the Inspector's `Watch` for other nodes |

- **A relay per node.** A topic sending to each remote subscriber would
  encode and send an event once per subscriber. So `Subscribe` to another
  node's topic goes through a relay on the subscriber's node,
  `pubsub:{name@node}`, which the first subscriber starts: a topic whose
  events come from the upstream topic, holding its buffer. The upstream
  topic sends each event once per node. The relay ends with its last
  subscriber, and with the upstream topic, for its reason, which its
  subscribers see.
- **The relay waits as long as its first subscriber.** It subscribes to its
  topic while that subscriber's call waits on it, within the call's
  deadline, which the call carries (`Msg.Context`): the relay gives up when
  the subscriber does, rather than after a timeout of its own. A relay that
  fails ends, and the subscribers queued behind the first start another.
- **Where the monitor goes.** A subscriber on the topic's node monitors by
  PID after the answer, so a failed call leaves no Down behind. The relay
  monitors first, on the same link as its call: a monitor placed after the
  answer could reach a topic that exited meanwhile, and say noproc in place
  of the reason.
- A reply carries an error's text, not the error, and `errors.Is` matches
  a `RemoteError` by its text: the relay answers with the sentinel its own
  call failed with, not the error that wraps it, so that it is that sentinel
  on the subscriber's side too.

## Cron and leader election

Both are built on the public API alone, as `actor` is, and neither imports
the other. They were nested modules, to be versioned apart from the core,
until v0.6.0; needing nothing the core does not, they are its packages now,
released with it. They compose through `actor.ChildSpec`: a cron process is the
leader's singleton, and its state the singleton's.

### `grpcproc/cron`

- **A process, not a service of the node.** The application starts it
  (`cron.Start`, or `cron.Child` under a supervisor), so it has a PID, a
  lifecycle and an inspect map, and the core starts no goroutine of its own.
- **Every run is a process**, spawned with `SpawnMonitor` and `LinkParent`.
  Its exit reason is its result (an error, a panic, `timeout`, `replaced`),
  so failures need no separate channel; the cron process knows which runs
  still go, so a run due while the last one goes can be allowed, skipped or
  replaced; a `Timeout` is an `Exit`, and the run's `Deadline`, which its
  context ends at and its calls carry; and runs end with the cron process.
- **The wall clock, once a minute.** The process wakes at the start of each
  minute by the wall clock and starts what is due, in each job's own
  `Location` (UTC by default, so nodes agree). A minute clocks skip does not
  exist; one they repeat runs the first time only (`Schedule.Next` finds the
  first pass even where `time.Date` returns the second). A clock stepped back
  runs nothing twice.
- **Missed runs** are skipped, unless a job has a `StartingDeadline`: then
  the latest one missed within it starts, once. The process reports each
  job's last run as a `cronv1.State`; one started from it catches up from
  there, and checks the minute it starts in at once, for the run the process
  before it did not start.
- **Standard crontab**, Vixie's day rule, plus `L`, `5L` and `5#2` from
  Quartz; the standard macros. `AddJob` takes Go functions, so it works on
  the cron process's own node only, through the registry `actor.StartChild`
  uses; enable, disable and remove work from any node. They are methods of
  a `Crontab`, the cron process's address, which `Start` returns and
  `Named` finds, and take who asks as a `grpcproc.Caller`, as pubsub's
  `Topic` does.

### `grpcproc/leader`

- **Leadership is a process's lifetime.** The user's code is a singleton, an
  `actor.ChildSpec`, which the elector starts on a win and tells to exit
  (`demoted`) on a loss. No handler asks whether it leads, and a demoted
  singleton's context is cancelled, which stops its I/O. Election messages
  could not reach a typed `Process[M]` anyway: they would be dead letters
  with reason `type`. The singleton is `Temporary`: one that exits, or cannot
  start (or `Confirm` refuses), ends its node's leadership with a backoff that
  doubles each time, so a failing singleton moves on rather than loops.
- **Raft's election without the log**: terms, votes, heartbeats; a leader
  steps down after two election timeouts without a majority's acks
  (check-quorum); followers that heard from a leader lately ignore vote
  requests (stickiness); and pre-votes, so a node cut off from the others
  does not raise its term and depose the leader when it is back.
- **One relay process per peer.** A remote send or monitor waits for the
  dial (up to `DialTimeout`), and a heartbeat loop cannot. The elector sends
  locally to relays, which send on and monitor the peer's elector: a hung
  peer holds its relay only. The monitor's reasons say what happened: a
  graceful exit or `noproc` removes the peer from a dynamic view at once and
  lets followers of a leader that left campaign without waiting; a
  `noconnection` leaves it counted (a ghost). Followers do not talk to each
  other, so a node greets the peers it lost every `GhostTTL` to hear back.
- **Static voters or a dynamic view.** A fixed `Voters` set cannot elect two
  leaders in a partition. Without it the view is `Peers`, `Membership`, and
  (without `Membership`) whoever talks: available, but two nodes with
  different views can each count a majority, and `MinClusterSize` bounds how
  small a view elects.
- **State from one leader to the next.** `Lease.Checkpoint` replicates the
  singleton's state with the heartbeats and returns once a majority holds
  it; a voter refuses a candidate whose state version (term, sequence) is
  older than its own, Raft's election rule on a one-entry log, so every
  acknowledged checkpoint reaches the next leader. A new leader stamps the
  state with its term, as Raft commits a no-op. `Resign` hands over:
  it stops the singleton, whose `Terminate` may still save, waits for the
  most recent follower to hold the leader's state, and sends it `TimeoutNow`.
- **Maintenance.** `Transfer` moves leadership to a named follower, as
  `Resign` hands over. `Cordon` keeps a node from leading: the cordoned set
  is part of the replicated state, so it outlasts the node's restarts and
  the leader's; a cordoned node does not campaign, voters refuse it, and a
  cordoned leader hands over. A cordon that would leave no node of the view
  to lead is refused. Both go to whichever node leads. A voter that refuses
  a candidate for its older state sends its own with the refusal, and a
  refused pre-vote carries the voter's term: a restarted node, which holds
  nothing, catches up without a leader to send it anything, which is what
  keeps a cluster whose other nodes are all cordoned from deadlocking.
- **Calling the leader.** `leader.Call` finds the leader through the
  caller's node's elector and calls the singleton by name, following
  leadership as it moves. It asks again only when the call cannot have
  reached it: no leader known, `ErrNoProc` from the node named, or a
  `LinkError` whose `Unsent` is set. A call that left may have been handled,
  and delivery stays at most once, so that one is the caller's to repeat.
- **In memory, or in a `Store`.** Without a `Store`, terms, votes and state
  are not persisted: losing a majority at once loses them. A restarted
  elector does not vote for two election timeouts, the time a leader elected
  with a vote it may have cast before needs to be heard from. With one, the
  elector keeps Raft's persistent state, `{term, votedFor, State}`, the log
  being the one entry. It saves in three places, and nowhere else: before a
  send to a peer (a vote, an ack, a RequestVote, a heartbeat carrying a new
  state), before `commit` counts the leader's own copy, and before the
  singleton gets a lease, whose term must not come round again. It saves only
  what changed since, by term, vote and state version, so a heartbeat costs no
  write, and elections and checkpoints cost one each. A `Save` that fails
  stops anything else from leaving, and the elector exits with it; its
  supervisor starts it from what the `Store` holds. `leader.File` is
  write-sync-rename-sync-directory, through a fixed `path.tmp`, so crashes
  leave no temporary files to pile up. A Store without the syncs would be
  worse than none: after a machine crash it could come back with an older
  vote, which the node would trust, skip the quiet period, and vote twice in
  a term. Saves run in the elector's loop and hold up its heartbeats (a
  leader may write in parallel with sending, as Raft allows, but at one save
  per election or checkpoint that is not worth it yet): on slow disks,
  `ElectionTimeout` goes up. `Lease.Term` is the fencing token for external
  resources; `Confirm` makes leadership wait for an external lock.

## Global names (core built; the etcd store next)

A process's name is its node's: `Named[M]("warehouse", "stock")` reaches it
only if the caller knows the node. A global name belongs to the
installation, and whichever process holds it, wherever it runs, is what
`Global{"ledger"}` reaches. It is for a service that moves, restarted by a
supervisor on another node or placed by `StartChildFrom`, and for many of
them: one coordinator per tenant or per order, where a `leader` election
per name would cost a Raft group each, or one room of a teleconference
per name, placed on the SFU node with the most room. `grpcproc/etcd` knows
nodes, not processes, and an application that writes the keys itself gets
the parts below wrong, the claim's lifetime first.

```go
// core: the target, the claim, and what a store does
type Global struct{ Name string } // a Target: whoever holds Name in this installation
func (p *Process[M]) Claim(ctx context.Context, name string, opts ...ClaimOption) (*Claim, error) // *TakenError{Name, Holder}
func WaitForName() ClaimOption   // wait for a name that is held to be free, and claim it then
func KeepOnLoss() ClaimOption    // keep the holder running when the claim is lost, and claim again
func (c *Claim) Revision() int64 // the fencing token; a new one after a claim made again
func (c *Claim) Held() bool      // false while a KeepOnLoss claim is lost
func (c *Claim) Release(ctx context.Context) error

type Names interface { // Config.Names; Node.Names() for components that resolve
    Watch(ctx context.Context) error                             // from Start, before Registrar; returns once every name is known
    Lookup(name string) (PID, bool)                              // must not block: the implementation keeps what it watches
    Resolve(ctx context.Context, name string) (PID, bool, error) // asks the store itself, when Lookup's lag matters
    Claim(ctx context.Context, name string, holder PID, opts ClaimOptions, notify func(ClaimEvent)) (NameClaim, error)
}
type NameClaim interface{ Revision() int64; Release(ctx context.Context) error }
type ClaimEvent struct{ Kind ClaimEventKind; Revision int64 } // ClaimLost, ClaimRegained, ClaimConflict
type NameLister interface{ List(prefix string, limit int) []GlobalName } // optional: a store that keeps every name

// grpcproctest: an in-memory store every test cluster's nodes share
c.Names(); c.CutNames("b"); c.RestoreNames("b")

// grpcproc/etcd (next, once a core with Names is released): *Cluster is a Names
```

- **The core keeps the claims; a store keeps the names.** What a claim
  means, that it lives as long as its process, that losing it ends the
  holder or, with `KeepOnLoss`, does not, that a conflict ends the old
  holder, is the core's, in `Process.Claim`, and the same whatever keeps
  the names. A store does storage and says when it can no longer vouch for
  a claim: `ClaimLost`, then for a `KeepOnLoss` claim `ClaimRegained` or
  `ClaimConflict`. So an application's room process claims its name the
  same way in a `grpcproctest` cluster and on etcd, and the rules are
  tested once, in the core, against the in-memory store, which a test cuts
  off from a node (`CutNames`) as a partition from etcd would.
- **Resolved on the sender's node, to a PID.** `Send`, `Call`, `Monitor`,
  `Link` and `Exit` to a `Global`, and an `AddrOf[M](Global{…})`, look the
  name up in `Config.Names` and go to the PID it gives, as if addressed by
  it. Nothing on the wire changes, and every API that takes a `Target`, an
  actor's supervisor or a pubsub topic say, takes a global name too.
  `Lookup` must not block, since `Process.SendTo` has no ctx: the etcd
  implementation lists `<prefix>/names/` and watches it, as `Membership`
  does `<prefix>/nodes/`, and `Start` waits for the list. A name no one
  holds is a process that does not exist: a call fails with `ErrNoProc`, a
  monitor gets `Down{noproc}` with the name in `Down.Name`, a message is a
  dead letter.
- **Stale for a moment, never wrong for long.** A lookup reads this node's
  copy, which lags etcd by a watch event. A send in that gap reaches the old
  holder's PID: `noproc` once it has exited, `noconnection` if its node is
  gone, both of which delivery at most once already allows. A monitor
  follows the process it found, not the name: its `Down` says that holder
  ended, and the caller looks again, as a monitor of a restarted child
  does. Where the lag matters, `Resolve` reads etcd itself: before starting
  what may already exist, a room whose first join came to another node a
  moment ago, and after losing a race to start it.
- **A claim lives as long as its process.** `Claim` is a compare-and-swap
  that creates `<prefix>/names/<name>`, holding the PID, under the lease of
  the holder's node's registration, and fails with a `*TakenError` holding
  the holder's PID if it exists, as `StartChild` answers with
  `ErrAlreadyStarted`, so a caller that lost the race reaches the winner.
  Only on the claimer's node, though: an error crosses the wire as its text
  (`errors.Is` matches `ErrTaken` by it), and the PID does not survive. A
  room started on an SFU node through `StartChildFrom` from a signaling
  node that loses its claim fails the call with `ErrTaken`, and the
  signaling node then asks `Resolve` for the winner, which etcd knows by
  then, rather than its own copy, which may not. The core releases a
  process's claims when it exits, in the background, before its watchers
  hear of the exit, so that a store out of reach does not hold up its
  `Down`s, and `Stop` waits for the releases before it withdraws the node.
  A watcher may therefore see the `Down` a moment before the name is free:
  a claim then fails with `ErrTaken` naming the old holder, whose call
  answers `ErrNoProc`, and the claimer tries again, or waits with
  `WaitForName`. The etcd store releases by comparing the key's create
  revision, so that a newer claim of the name is never deleted, and a node
  that dies takes its claims with its lease, at the same time as its peers
  drop their links to it. `ProcessInfo.Globals` lists what a process
  holds.
- **Losing the claim ends the holder, by default.** When the node's lease
  is lost, etcd out of reach longer than the TTL, its keys go, and another
  node may claim the name. The holder must have stopped by then, so the
  per-node process exits every holder of the node, with reason `name lost`,
  once the TTL has passed since the last keepalive that succeeded, counted
  from when it was sent: etcd counts from when it arrived, which is later,
  so the holder stops first, as long as the two clocks run at the same
  rate. The store decides when (`ClaimLost`), and the core ends the
  holder. That is `leader`'s rule, leadership as a process's lifetime: a
  holder does not ask whether it still holds the name, and its context ends
  with it. `name lost` is abnormal, so a supervisor restarts it, and its
  claim at start then waits for etcd, or reaches the new holder. For what
  clocks cannot promise, `Revision` is the fencing token: create revisions
  grow, so a write fenced by it refuses a holder that lost the name.
- **Uniqueness by default, availability by choice.** In a partition, the
  side that cannot reach etcd cannot claim, and by default its holders end
  when their leases do; the other side claims the names again. With a
  lease there is no clash to resolve, at the price of a quiet minority, as
  with Akka's lease-majority downing: right for a ledger, whose writes two
  holders would corrupt. It is wrong for a teleconference room. An SFU node
  that loses etcd still carries the room's media to clients that still
  reach it, and ending its rooms ends every call on it, every call
  everywhere if etcd itself is down for longer than the TTL, to avoid a
  duplicate that costs far less. So a claim made with `KeepOnLoss` keeps
  its holder running when the lease is lost: `Held` turns false, and the
  store claims the name again once etcd is back, all of a node's at once
  rather than each holder trying on its own, and the claim holds it with a
  new `Revision`. If another process claimed it meanwhile, a room that the
  other side started for a join that could not reach this one, the holder
  etcd has wins, since every other node already routes to it, and the one
  claiming again exits with reason `name conflict`, as Horde sends the
  loser `name_conflict` and Erlang's `global` resolves a clash with
  `random_exit_name`. Between the two, two rooms carry one name: the
  application chooses that by the option, for a process whose duplicate is
  a nuisance and whose absence is an outage.
- **Waiting for a name: a standby.** The same binary on five nodes, each
  with a `billing-gateway` process, of which one may be active: one
  connection to an outside API, one consumer of a queue in order. With
  `WaitForName()`, `Claim` does not fail with `ErrTaken` but blocks, until ctx
  is done, for the name to be free, and claims it then: the holder exited,
  or its node's lease ended. That is etcd's election, and the light
  alternative to `leader` for a singleton with no state to hand over: etcd
  elects, not a Raft group of the nodes, and the standby takes over within
  a TTL and a watch event, without a supervisor elsewhere to restart
  anything. What it waits on is the key, deleted, and not the holder's
  `Down`: `Down{noconnection}` says this node lost its link, not that the
  holder is gone, and a standby that claims again on it fails the
  compare-and-swap, as it should, and spins. Standbys race for the
  compare-and-swap when the key goes, and one wins; no queue orders them,
  since with a few standbys the race costs a few failed writes and which
  one wins does not matter. A process per name that waits is fine for a
  few singletons; per-key processes, rooms and orders, are started when
  needed and do not wait.
- **A directory, not placement.** A global name says where a process is,
  not where it should go. The node with the most room is decided by load,
  which changes by the second and is not `Member.Metadata`, fixed for a
  node's life: each node publishing its load on a pubsub topic, or asking
  two nodes at random and taking the lighter (the power of two choices).
  Placement by load is also why a room wants a directory and not a hash of
  its id over `Membership`, keyed entities' way (see Open work), which
  decides the node itself and cannot weigh load.
- **Scale.** One key per name, under one lease per node, so leases do not
  grow with names. Writes are a claim and a release per holder's life: a
  hundred thousand rooms lasting half an hour on average is about 110 a
  second, and an SFU node that dies with ten thousand rooms is ten
  thousand claims again as their clients reconnect, which etcd clears in
  seconds; Kubernetes keeps more objects than that in one etcd. Every node
  keeps a copy, some ten to twenty megabytes for a hundred thousand names,
  and receives every claim and release: fine for hundreds of thousands of
  names over tens of nodes. Past that, `Lookup` would ask etcd on a miss,
  or a node would watch only the names it routes to. The initial list is
  paged, and etcd needs automatic compaction, since every claim and
  release is a revision.
- **One installation.** Names do not cross installations: a registry
  spanning two needs agreement over a network neither side controls (see
  What was rejected). A `Policy` judges a request by the process's local
  name, so a process that another installation reaches by its global name
  has a local one to export.
- **Tests.** Every `grpcproctest` cluster's nodes share an in-memory
  `Names`, so a test of a moving service needs no etcd, and `CutNames` and
  `RestoreNames` play a node's partition from the store: its claims lost,
  its `KeepOnLoss` holders running on, then held again or in conflict.

- **In the tools, as far as the node can tell.** A node with `Names`
  resolves any one name, so `grpcprocctl names room:42` answers wherever
  the room is, through the Inspector's `LookupName` of that node. Listing them
  needs a `Names` that keeps them all, as the etcd one and `grpcproctest`'s
  do, and not one that would ask etcd on a miss past hundreds of thousands
  of names: so listing is the optional `NameLister`, and the Inspector's
  `ListNames` (a prefix, a limit, since there may be a hundred thousand
  rooms) answers `Unimplemented` for a node whose `Names` cannot list, and
  `grpcprocctl names` says so rather than show an empty list. Where it can
  list, `names` lists them by prefix. `grpcprocctl inspect` shows the
  global names a process holds whatever the store, from
  `ProcessInfo.Globals`, since the core keeps the claims; the MCP server
  and the web UI show the same. A node without `Names` has none to show,
  and its Inspector answers `FailedPrecondition`. The Inspector's RPCs are
  in the core; `grpcprocctl` follows with the etcd store.

## What was rejected, and why

| Idea | Seen in | Why not |
|---|---|---|
| Own TCP protocol | ergo, GoAkt, Hollywood (dRPC) | The whole point is to reuse the gRPC server, TLS, interceptors and tooling the service already has. Akka went the same way, deprecating its ClusterClient for gRPC. Should one stream per direction show head-of-line blocking, Partisan's case for channels, gRPC answers it: more `Link` streams to a peer, split by sender, which keeps each sender's order |
| One monitor = one stream | — | Loses message-before-Down ordering, costs a goroutine per monitor |
| Priority mailbox queues | ergo (4 queues), GoAkt | Inspection runs inside `Receive` instead; `Down` must stay in order with messages; and `Exit` cancels the process's context rather than wait in its mailbox, so no signal is stuck behind a backlog |
| Two-way links | Erlang/OTP | A one-way link is a monitor on the wire and needs no agreement between nodes; see Links. Erlang needed unlink ids and acknowledgements (OTP 23) to settle the races two-way links have |
| Mailboxes that block when full | GoAkt | A full mailbox would stall the shared link for everyone; a link's queue is per peer, and can be bounded (`Config.MaxQueued`). A bound that refuses instead is open work, below |
| Metrics per PID | GoAkt | Cardinality; the label is the key, and a PID's own numbers are in `Processes()` and the Inspector |
| A UI served by every node | ergo Observer | A UI is a client: `grpcprocctl web` is one, over the Inspector, and a node serves nothing but gRPC |
| gob / custom codec | first prototype | protobuf is already the service's contract; a body travels as its full name and bytes, and generated types register themselves |
| Delivery beyond at-most-once in the core | Akka Reliable Delivery, GoAkt | Every send would pay for a store and acknowledgements most do not need, and both Akka and GoAkt made it a layer one opts into. Here it is a module, open work below; the core's `LinkError.Unsent` tells it what is safe to send again |
| Sagas that live in memory | ergo `gen.Saga` (v2, gone in v3), Elixir's Sage | A crashed coordinator leaves steps done and nothing to undo them. Every saga framework that calls itself production-ready (Akka's workflows, Dapr Workflow, Commanded) has a coordinator that outlives a crash, and steps that are idempotent for it. A recipe and a durable module are open work, below |
| One membership across installations | Orleans multi-cluster (removed in 3.2), Akka ClusterClient (deprecated in 2.6) | Two installations are operated apart: a registry or a singleton spanning both needs agreement over a network neither side controls, and the systems that tried went back to an explicit boundary: grpcproc's is `Admit` and `DialOptionsFor` (see Admission) |

## Open work

What production use asks for next, roughly in order. The core gets hooks and
interfaces only; whatever needs a dependency is a nested module, as etcd and
OpenTelemetry are.

- **Federation between installations.** Two installations can talk: a
  resolver that answers the other's node names, `DialOptionsFor` with its
  credentials, and an `Admit` that gives its nodes an `Export` policy (see
  Admission). What is left is convenience and naming: node names must be
  unique across both. A federation module would add qualified names
  (`installation/node`) and tables of what each installation exports, as
  NATS accounts export services and Temporal's Nexus endpoints list their
  callers; a supervisor never links across. Only
  if one side can dial out and not in (a customer's VPC, a factory floor)
  does the link need turning around: a bridge with a gRPC service of its own,
  so the core keeps one link per direction.
- **Sagas.** The shop's order desk is one already, in memory: it reserves,
  charges, releases the stock on a decline, and logs an error from the bank
  as unsettled, since the card may have been charged. A guide first: an
  idempotency key per step, the saga's id and the step's, in the request or
  the metadata; a step sent again only when it cannot have left
  (`ErrNoProc`, `LinkError.Unsent`), the rule `leader.Call` follows; each
  compensation registered before its step, so a step that timed out but
  happened is undone too; a pivot step, after which recovery retries forward
  instead of compensating; and reconciliation for what stays unknown. Then a
  coordinator that needs nothing new: a leader's singleton whose state is
  the sagas in flight, saved with `Lease.Checkpoint` after each step, with
  `Lease.Term` passed to participants as a fence. That holds tens or
  hundreds in flight, since the whole state travels with the heartbeats.
  Then, for volume, a `grpcproc/saga` module: each step's result saved behind
  a `Store` (create, append with compare-and-swap, claim with a lease and an
  epoch, list what is in flight), as DBOS checkpoints steps, rather than
  Temporal's replay of a history, whose determinism rules forbid the
  `select` and goroutines processes are written with. The leader takes over
  the sagas of a node that left. Steps' timeouts need durable timers (below).
- **Global names on etcd**: the core and `grpcproctest` have them (see
  Global names); the etcd store, `grpcprocctl names` and the MCP and web
  views follow a release of the core.
- **Process groups**, Erlang's `pg` and Akka's Receptionist: the live
  members of a group, found and watched. Pub/sub topics already monitor their
  subscribers through a relay per node, so it is a thin module.
- **Durable one-shot timers**: "send this to that name at that time", kept
  in the checkpointed state of a cron process that runs as the leader's
  singleton, so it outlasts the node that set it, as Dapr's reminders outlast
  an actor.
- **More discovery**: a DNS SRV resolver, standard library only, so in the
  core, and a Kubernetes one, a nested module for client-go.
- **Member metadata, the rest of it** (see Discovery interfaces): the etcd
  record carrying it, `grpcprocctl nodes` showing it, both once a core with
  it is released; then `leader` preferring new versions to lead, as Akka's
  `app-version` keeps new work on new nodes, and `Cordon` keeping old ones
  from leading during a rollout.
- **An operations guide**: keepalive values, static `Voters` or etcd
  `Membership` for an election, the two election timeouts in which two nodes
  can each believe they lead and `Lease.Term` against it, and the order of a
  shutdown (`Resign`, stop supervisors, `Stop`, withdraw). Akka does that
  order itself on SIGTERM; grpcproc does not own signals.
- **Delivery beyond at-most-once**, as a module beside sagas, on the same
  `Store`: idempotency keys, sending again what `Unsent` says never left, and
  an outbox fenced by an epoch, so a writer on a node that left cannot commit,
  as GoAkt's durable queue is. The core stays at most once.
- **A mailbox bound that refuses** rather than blocks: optional, per process.
  A local send fails at once, a remote call is answered busy, a remote send
  is a dead letter with reason `mailbox full`, and `Down`s and exits always
  get in. Depth is visible already; this is for a process that cannot be
  trusted to shed load itself.
- **Keyed entities**: a consistent hash over `Membership` picks the node,
  `StartChildFrom` starts the entity there or answers with the one that runs
  (`ErrAlreadyStarted`), and an entity idle for a while stops. Orleans' grains
  and Akka's sharding, without the strongly consistent directory that made
  Orleans' multi-cluster mode admit duplicates; state only through the saga
  module's `Store`.
- **A protoc plugin** that writes contract address types like `StockAddr`
  from a service definition, as Proto.Actor generates its grains' clients.

Coverage and style follow `fsm` and `di`: CI requires 100 % coverage of every
library package, the core's and the nested modules', race-detected; CI runs
every example and checks what it prints; and `DESIGN.md` is kept current.
