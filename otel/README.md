# grpcproc/otel

OpenTelemetry for [grpcproc](https://floatdrop.github.io/grpcproc/guides/observability/): a span for every send, call and
handled message, chained across processes and nodes, and metrics keyed by
process label. A separate module, so grpcproc itself does not depend on
OpenTelemetry.

```sh
go get github.com/floatdrop/grpcproc/otel
```

```go
h, err := grpcprocotel.New() // global meter/tracer providers and propagator; see Options
node, err := grpcproc.NewNode(grpcproc.Config{…, Hooks: grpcproc.JoinHooks(h, myHooks)})
reg, err := h.Observe(node) // gauges read from node snapshots at each collection
defer reg.Unregister()
```

## Traces

| Where | Span | Kind | Parent |
| --- | --- | --- | --- |
| `Send` | `send <message type>` | producer | the context the sender carries |
| `Call` | `call <message type>` | client | the context the sender carries |
| a process takes a message | `process <message type>` | consumer (server for a call) | the sender's span |
| a process takes a `Down` | `process grpcproc.Down` | consumer | — |
| a process that traps exits takes an `Exited` | `process grpcproc.Exited` | consumer | — |

A handling span lasts until the process calls `Receive` again, or exits (with
an error status if abnormally). Everything the process sends meanwhile is
its child, because grpcproc hands a process's sends the metadata of the
message it is handling, and these hooks put the span there. No context has
to be threaded through handlers for the chain to form.

For a handler's own spans (a database call), take the handling span from the
message, in the message's context, which ends when its caller stops waiting:

```go
ctx, cancel := m.Context(p.Context())
defer cancel()
ctx, span := tracer.Start(h.Extract(ctx, m.Metadata), "load order")
```

Spans carry `messaging.system=grpcproc`, `messaging.operation.type`,
`messaging.destination.name`, `grpcproc.message.type`, `grpcproc.label`,
`grpcproc.pid`; failed sends and calls carry `error.type`.

## Metrics

No metric carries a PID: the process label (`grpcproc.WithLabel`, default the
message type) is the unit, and every other attribute is bounded.

| Name | Type | Attributes |
| --- | --- | --- |
| `grpcproc.messages.sent` | counter | `grpcproc.label` (sender), `grpcproc.call`, `grpcproc.remote` |
| `grpcproc.messages.received` | counter | `grpcproc.label` |
| `grpcproc.mailbox.wait` | histogram, s | `grpcproc.label` |
| `grpcproc.process.duration` | histogram, s | `grpcproc.label` — time from taking a message to the next `Receive` |
| `grpcproc.call.duration` | histogram, s | `grpcproc.label`, `grpcproc.remote`, `error.type` on failure |
| `grpcproc.processes.spawned` | counter | `grpcproc.label` |
| `grpcproc.processes.exited` | counter | `grpcproc.label`, `grpcproc.reason` (`normal`, `shutdown`, `killed`, `noproc`, `noconnection`, `type`; `max restarts` from actor, `timeout` and `replaced` from cron, `demoted` from leader; `panic`, `error`) |
| `grpcproc.dead_letters` | counter | `grpcproc.reason`, `grpcproc.message.type` |
| `grpcproc.links.up`, `grpcproc.links.down` | counter | `grpcproc.peer` |
| `grpcproc.processes` | gauge (Observe) | `grpcproc.label` |
| `grpcproc.mailbox.depth` | gauge (Observe) | `grpcproc.label`, summed |
| `grpcproc.mailbox.oldest` | gauge (Observe), s | `grpcproc.label`, maximum |
| `grpcproc.link.messages`, `grpcproc.link.bytes` | counter (Observe) | `grpcproc.peer`, `grpcproc.direction` |

`error.type` is one of `noproc`, `type`, `busy` (the link to the peer was
full: `Config.MaxQueued`), `noconnection`, `timeout`, `canceled`, `remote`
(the handler returned an error), `other`.
