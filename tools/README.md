# grpcproc/tools

`grpcprocctl`: inspect and operate grpcproc nodes through their
[Inspector](../README.md#inspector), from a terminal or, as an MCP server,
from an AI agent. A separate module, so grpcproc itself carries no CLI or MCP
dependencies.

```sh
go install github.com/floatdrop/grpcproc/tools/cmd/grpcprocctl@latest
```

The node must serve the Inspector next to grpcproc:

```go
node.Register(grpcServer)
insp := inspect.New(node, inspect.WithResolver(resolver, dialOptions...))
insp.Register(grpcServer)
defer insp.Close()
```

## In a terminal

```sh
export GRPCPROC_ADDR=10.0.0.5:9000   # or --addr; TLS by default, --plaintext without
grpcprocctl --plaintext ps --sort mailbox
```

```
PID                  NAME           LABEL        STATE    MAILBOX  OLDEST  RECEIVED  SENT  LAST MESSAGE    UPTIME
<orders-1.1718.4>    ledger-writer  ledger       running  41       1.111s  1         0     ledger.v1.Post  1.112s
<orders-1.1718.1>    orders-sup     supervisor   idle     0                0         0                     1.112s
<orders-1.1718.2>    reservations   reservation  idle     0                0         0                     1.112s
<orders-1.1718.3>    payments       payment      idle     0                0         0                     1.112s
<orders-1.1718.5>    bank-session   session      idle     0                0         0                     1.112s
```

`ledger-writer` has been running one message for a second while 41 wait.
Ask it what it believes, and it cannot answer, because it is busy:

```sh
grpcprocctl --plaintext inspect --wait 50ms ledger-writer
```

```
state:            running
mailbox:          41 (peak 42, oldest 1.136s)
last message:     ledger.v1.Post
inspect:          grpcproc: inspect <orders-1.1718.4>: busy for 1.187s: context deadline exceeded
```

A process that is free answers with whatever it publishes through
`grpcproc.WithInspect`; a supervisor lists its children:

```
name:                  orders-sup
label:                 supervisor
monitors:              2
  child.payments:      <orders-1.1718.3> permanent restarts=0
  child.reservations:  <orders-1.1718.2> permanent restarts=0
  restarts:            0/3 in 5s
  strategy:            one_for_one
```

| Command | |
| --- | --- |
| `node [name]` | counters and links of a node |
| `nodes` | every node reachable from this one, following links |
| `ps` | processes: `--node`, `--name`, `--label`, `--state`, `--min-mailbox`, `--sort pid\|mailbox\|received\|sent`, `--limit` |
| `inspect <pid\|name>` | one process, with what it says about itself: `--node`, `--wait` |
| `watch` | stream spawns, exits, links, dead letters: `--node`, `--kind`, `--count` |
| `exit <pid\|name> [reason]` | ask a process to exit |
| `loglevel <pid\|name> <level>` | change one process's log level |
| `dot` | Graphviz of processes and who started whom: `--node`, `--cluster` |
| `mcp` | serve these as MCP tools over stdio: `--allow-writes` |

`grpcprocctl --version` prints the version it was installed at, which is also
what its MCP server reports.

A pid is written as grpcproc prints it, `<node.incarnation.id>`; a name is
looked up on `--node`, by default the node serving the Inspector. Any
command takes `--json` before it for the same data as JSON (the same shapes
the MCP tools return). Connection flags follow grpcurl: `--plaintext`,
`--cacert`, `--cert` and `--key` for mutual TLS, `--servername`.

```sh
grpcprocctl --plaintext dot --cluster | dot -Tsvg -o processes.svg
```

draws each node as a cluster, each supervisor bold, an edge from each
process to those it started, and any process with waiting messages in red.

## For an AI agent

```sh
claude mcp add grpcproc -- grpcprocctl --plaintext --addr 10.0.0.5:9000 mcp
```

The server explains grpcproc to the agent (pids, labels, what a deep mailbox
or a busy process means) and offers:

| Tool | |
| --- | --- |
| `cluster_nodes` | every reachable node, and which could not be reached |
| `node_info` | one node: counts, dead letters, link traffic and errors |
| `list_processes` | filter and sort, e.g. by mailbox to find backlogs |
| `get_process` | one process, with what it says about itself |
| `watch_events` | collect events for a few seconds |
| `exit_process`, `set_log_level` | only with `--allow-writes` |

So "orders are slow since the deploy" becomes: list processes by mailbox,
find `ledger-writer` with 41 waiting, inspect it, see it busy on one message
for a second, watch events for exits and dead letters.
