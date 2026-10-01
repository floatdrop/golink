# grpcproc/tools

`grpcprocctl`: inspect and operate grpcproc nodes through their
[Inspector](https://floatdrop.github.io/grpcproc/guides/inspector/), from a terminal or, as an MCP server,
from an AI agent. A separate module, so grpcproc itself carries no CLI or MCP
dependencies. The site has a page for each:
[grpcprocctl](https://floatdrop.github.io/grpcproc/guides/grpcprocctl/) and
[an AI agent over MCP](https://floatdrop.github.io/grpcproc/guides/mcp/).

```sh
go install github.com/floatdrop/grpcproc/tools/cmd/grpcprocctl@latest
```

The node must serve the Inspector next to grpcproc:

```go
node.Register(grpcServer)
insp := inspect.New(node) // forwards to other nodes' Inspectors, as the node reaches them
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
| `names [name]` | [global names](https://floatdrop.github.io/grpcproc/concepts/addressing/#global): who holds one, or a list: `--node`, `--prefix`, `--limit` |
| `watch` | stream spawns, exits, links, dead letters: `--node`, `--kind`, `--count` |
| `exit <pid\|name> [reason]` | ask a process to exit |
| `loglevel <pid\|name> <level>` | change one process's log level |
| `leader [status] <cluster>` | a [grpcproc/leader](../leader/README.md) election, as each node that takes part sees it |
| `leader move <cluster>` | hand leadership over: `--to` a node, by default the follower with the latest state |
| `leader cordon <cluster> <node>` | keep a node from leading, to work on its host; if it leads, it hands over |
| `leader uncordon <cluster> <node>` | let it lead again |
| `cron [list] [<pid\|name>]` | [grpcproc/cron](../cron/README.md) processes and their jobs: `--node`, every node by default |
| `cron enable\|disable\|remove <pid\|name> <job>` | change a job of a cron process: `--node` |
| `dot` | Graphviz of processes and who started whom: `--node`, `--cluster` |
| `mcp` | serve these as MCP tools over stdio: `--allow-writes` |
| `web` | serve a web UI that shows the cluster live: `--listen`, `--allow-writes` |

`grpcprocctl --version` prints the version it was installed at, which is also
what its MCP server reports.

A pid is written as grpcproc prints it, `<node.incarnation.id>`; a name is
looked up on `--node`, by default the node serving the Inspector.
`--json` before `node`, `nodes`, `ps`, `inspect`, `names`, `watch`, `leader` or `cron` prints the same
data as JSON: one indented value, or for `watch` one compact event per line,
so `grpcprocctl --json watch | jq` sees events as they happen. The objects
are those the MCP tools return, which wrap lists in an object of their own.
Connection flags follow grpcurl: `--plaintext`, `--cacert`, `--cert` and
`--key` for mutual TLS, `--servername`.

```sh
grpcprocctl --plaintext dot --cluster | dot -Tsvg -o processes.svg
```

draws each node as a cluster, each supervisor bold, an edge from each
process to those it started, and any process with waiting messages in red.

### Leader elections

```sh
grpcprocctl --plaintext leader cordon sched b   # b led: it hands over
```

```txt
NODE  ROLE      TERM  LEADER  VIEW   STATE  CORDONED  UNREACHABLE  SINGLETON  BACKOFF  ERROR
a     leader    2     a       a,b,c  2.3    b                      <a.1.11>
b     follower  2     a       a,b,c  2.3    b                      none       200ms
c     follower  2     a       a,b,c  2.3    b                      none
```

Each row is what one node's elector believes, so nodes that name different
leaders, or lag a term behind, show a partition. `move`, `cordon` and
`uncordon` go to whichever node leads, through the Inspector's `Call`, and
print the table once every node agrees, or as it stands when `--timeout`
runs out, saying so. A cordon is kept in the state the leader replicates:
the node stays out of the running across its own restarts until `uncordon`.

### Cron jobs

```sh
grpcprocctl --plaintext cron disable --node b billing yearly
```

```txt
NODE  CRON     JOB     SPEC        ZONE  NEXT                  LAST  RUNNING  LAST FAILURE  ERROR
b     billing  leap    0 0 29 2 *  UTC   2028-02-29T00:00:00Z        0
b     billing  paused  0 * * * *   UTC   disabled                    0
b     billing  yearly  @yearly     UTC   disabled                    0
```

`cron` finds every cron process, on one node or all of them, by the type of
message it takes, whatever it is named or labelled, and lists its jobs as it
publishes them: when each runs next and last ran, how many runs are going,
why the last one that failed did. `enable`, `disable` and `remove` go to the
cron process through the Inspector's `Call`, then list it again. Its runs are
processes of their own: `grpcprocctl ps --label cron:yearly` shows those
going.

## In a browser

```sh
grpcprocctl --plaintext web          # http://localhost:9911
```

serves a page, in the spirit of Erlang's observer, that reads the cluster
through the same Inspector and refreshes every second:

<picture>
  <source media="(prefers-color-scheme: dark)" srcset="../site/public/screenshots/grpcprocctl-web-processes-dark.webp">
  <img alt="grpcprocctl web: the Processes view sorted by mailbox, with ledger, whose mailbox climbs, open beside it" src="../site/public/screenshots/grpcprocctl-web-processes-light.webp">
</picture>

- **Cluster**: every node, a map of the links between them with messages
  per second on each, dead letters and queues.
- **Node**: its counters and links, with the last minute charted.
- **Processes**: the table `ps` prints, with messages in and out per
  second; the scope (name, label, state, mailbox) goes to the node, the
  search and the order stay in the page.
- **Supervision**: each process under the one that started it, coloured by
  state, mailbox or activity.
- **Events**: spawns, exits with reasons, links and dead letters, streamed
  as they happen.
- **Cron** and **Elections**: every grpcproc/cron job and grpcproc/leader
  election reachable from here, found by what they run and how their
  electors are named.

A process opens beside any of these: its mailbox and messages per second
charted, what it says about itself (asked for, or asked again on an
interval, since asking takes it a turn), and what it started. The address
says what is open, so a view can be shared.

<picture>
  <source media="(prefers-color-scheme: dark)" srcset="../site/public/screenshots/grpcprocctl-web-cluster-dark.webp">
  <img alt="grpcprocctl web: the Cluster view, three nodes and the messages per second on each link" src="../site/public/screenshots/grpcprocctl-web-cluster-light.webp">
</picture>

The page is read-only unless started with `--allow-writes`, which adds
exiting a process, setting its log level, enabling, disabling and removing
cron jobs, and moving or cordoning a leader, each confirmed first. It
listens on localhost by default and then answers only to a localhost Host,
so a page elsewhere cannot reach it through the browser; a change must come
from the page itself. On any other address, put it behind something that
authenticates. The API behind the page returns the objects `--json`
prints: `/api/nodes`, `/api/node`, `/api/processes`, `/api/process`,
`/api/crons`, `/api/elections`, and `/api/events` as server-sent events.

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
| `election` | a leader election, as each node sees it |
| `cron_jobs` | cron processes and their jobs, on a node or all of them |
| `global_names` | who holds a global name, or every name with a prefix |
| `exit_process`, `set_log_level`, `move_leader`, `cordon_node`, `uncordon_node`, `enable_cron_job`, `disable_cron_job`, `remove_cron_job` | only with `--allow-writes` |

So "orders are slow since the deploy" becomes: list processes by mailbox,
find `ledger-writer` with 41 waiting, inspect it, see it busy on one message
for a second, watch events for exits and dead letters.
