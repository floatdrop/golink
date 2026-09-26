# golink/otel

OpenTelemetry for [golink](../README.md): a span for every send, call and
handled message, chained across processes and nodes, and metrics keyed by
process label. A separate module, so golink itself does not depend on
OpenTelemetry.

```sh
go get github.com/floatdrop/golink/otel
```

```go
h, err := golinkotel.New() // global meter/tracer providers and propagator; see Options
node, err := golink.NewNode(golink.Config{…, Hooks: golink.JoinHooks(h, myHooks)})
reg, err := h.Observe(node) // gauges read from node snapshots at each collection
defer reg.Unregister()
```

## Traces

| Where | Span | Kind | Parent |
| --- | --- | --- | --- |
| `Send` | `send <message type>` | producer | the context the sender carries |
| `Call` | `call <message type>` | client | the context the sender carries |
| a process takes a message | `process <message type>` | consumer (server for a call) | the sender's span |
| a process takes a `Down` | `process golink.Down` | consumer | — |

A handling span lasts until the process calls `Receive` again, or exits (with
an error status if abnormally). Everything the process sends meanwhile is
its child, because golink hands a process's sends the metadata of the
message it is handling, and these hooks put the span there. No context has
to be threaded through handlers for the chain to form.

For a handler's own spans (a database call), take the handling span from the
message:

```go
ctx := h.Extract(p.Context(), m.Metadata)
ctx, span := tracer.Start(ctx, "load order")
```

Spans carry `messaging.system=golink`, `messaging.operation.type`,
`messaging.destination.name`, `golink.message.type`, `golink.label`,
`golink.pid`; failed sends and calls carry `error.type`.

## Metrics

No metric carries a PID: the process label (`golink.WithLabel`, default the
message type) is the unit, and every other attribute is bounded.

| Name | Type | Attributes |
| --- | --- | --- |
| `golink.messages.sent` | counter | `golink.label` (sender), `golink.call`, `golink.remote` |
| `golink.messages.received` | counter | `golink.label` |
| `golink.mailbox.wait` | histogram, s | `golink.label` |
| `golink.process.duration` | histogram, s | `golink.label` — time from taking a message to the next `Receive` |
| `golink.call.duration` | histogram, s | `golink.label`, `golink.remote`, `error.type` on failure |
| `golink.processes.spawned` | counter | `golink.label` |
| `golink.processes.exited` | counter | `golink.label`, `golink.reason` (`normal`, `shutdown`, `killed`, `noproc`, `noconnection`, `type`, `panic`, `error`) |
| `golink.dead_letters` | counter | `golink.reason`, `golink.message.type` |
| `golink.links.up`, `golink.links.down` | counter | `golink.peer` |
| `golink.processes` | gauge (Observe) | `golink.label` |
| `golink.mailbox.depth` | gauge (Observe) | `golink.label`, summed |
| `golink.mailbox.oldest` | gauge (Observe), s | `golink.label`, maximum |
| `golink.link.messages`, `golink.link.bytes` | counter (Observe) | `golink.peer`, `golink.direction` |

`error.type` is one of `noproc`, `type`, `noconnection`, `timeout`,
`canceled`, `remote` (the handler returned an error), `other`.
