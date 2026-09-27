# benchmarks

Separate module, so grpcproc itself does not depend on what it is compared
with. It measures the working tree (`replace ../`).

grpcproc against [GoAkt](https://github.com/tochemey/goakt) v4.5.6,
[Hollywood](https://github.com/anthdm/hollywood) v1.0.5,
[Proto.Actor](https://github.com/asynkron/protoactor-go) (its development
branch; it has no Go-style release tags) and
[Ergo](https://ergo.services) 3.3.0 (module version v1.999.330), each used
the way it is meant to be:

- the same message, `wrapperspb.Int64Value`, because Hollywood and
  Proto.Actor need protobuf to go remote. Ergo's network registers struct
  values and not pointers, so it sends a struct holding the same `int64`;
- **send** is end-to-end throughput: the timer stops when the receiver has
  counted every message, not when the sender has queued them;
- **request** is one caller waiting for each reply; **parallel** is
  `RunParallel` callers against one echo;
- **remote** is two engines, systems or nodes over real TCP on loopback, the
  connection warmed up before timing. grpcproc's own benchmarks in the root
  module use in-memory connections, which would flatter it here. GoAkt's
  remote actors are found with the public `PID.RemoteLookup` and reached
  through its remoting client, as an application would. Ergo's nodes find
  each other through its embedded registrar, the default, on a port of
  their own rather than the shared 4499.

Ergo runs with its software keepalive off, the one default changed. With it
on, parallel remote requests timed out in 8 runs of 16: each connection's
flusher arms one `time.AfterFunc` timer from its writers (500 ns) and from
its own callback (15 s, the keepalive), and on Go 1.26.0 and 1.27.1 a
`Reset` to 500 ns is now and then lost when more than one P runs. The batch
of calls then sits unsent until another timer runs, at worst the keepalive,
past Ergo's 5 s call timeout. The standard library alone reproduces it. The
keepalive sends nothing while messages flow, so the numbers are still
Ergo's.

Each framework is its own package: Hollywood and Proto.Actor both register a
protobuf file named `actor.proto`, which cannot share a binary, and
separate binaries also keep one framework's globals (Proto.Actor replaces
gRPC's logger) out of another's numbers.

Apple M3 Max, `-count=6`, medians by `benchstat`:

| | grpcproc | GoAkt | Hollywood | Proto.Actor | Ergo |
| --- | --- | --- | --- | --- | --- |
| Local send | 96 ns, 0 allocs | 95 ns (±15%), 0 allocs | **59 ns**, 0 allocs | 200 ns, 0 allocs | 198 ns (±23%), 3 allocs |
| Local request | 761 ns, 2 allocs | **568 ns**, 2 allocs | 2362 ns, 12 allocs | 2451 ns, 10 allocs | 3200 ns, 5 allocs |
| Remote send | 382 ns, 6 allocs | 520 ns (±10%), 7 allocs | **197 ns**, 7 allocs | 340 ns, 9 allocs | 551 ns (±18%), 10 allocs |
| Remote request | 44.8 µs, 52 allocs | **35.9 µs**, 42 allocs | 37.6 µs, 66 allocs | 60.7 µs, 103 allocs | 49.6 µs, 21 allocs |
| Remote request, parallel | 7.5 µs, 27 allocs | 10.1 µs, 40 allocs | **5.0 µs**, 54 allocs | 7.0 µs, 54 allocs | 6.5 µs, 19 allocs |
| Geometric mean | 1.56 µs | 1.59 µs | **1.39 µs** | 2.35 µs | 2.58 µs |

Proto.Actor's local send varies between runs (94 ns in one run of six, 189
to 200 ns in four others), and GoAkt's and Ergo's sends vary within one, as
marked; the others hold within a few percent.

How to read it:

- **The transport decides sequential remote latency.** GoAkt (its own TCP
  protocol) and Hollywood (dRPC) answer a remote request in 36–38 µs; the
  two that speak gRPC take longer, grpcproc 45 µs and Proto.Actor 61 µs.
  gRPC-go's writer adds a goroutine hand-off in each direction. On a real
  network the round trip dwarfs the difference; for grpcproc it is the cost
  of living on the application's gRPC server.
- **Against the other gRPC library**, grpcproc answers a remote request 26%
  sooner than Proto.Actor and trails it by about 40 ns on remote send, where
  Proto.Actor's writer batches up to a thousand envelopes.
- **Locally**, a grpcproc call waits on one channel of its own where
  Hollywood and Proto.Actor create a temporary process per request, which
  puts it three times ahead of them. GoAkt is quicker still, with as many
  allocations per call.
- **Ergo starts a goroutine per hop.** A sleeping process runs on a new
  goroutine each time a message wakes it, and so does each connection's
  read queue and each write flush (a timer callback). A request finds its
  echo asleep every time, so a local one takes 3.2 µs and a remote one
  50 µs, over Ergo's own TCP protocol; a stream of sends keeps the sink
  awake. Its remote calls allocate least of all, 19–21 times and about
  1 KB where the others take 27–103 times and 2.3–4.3 KB, and it is second
  only to Hollywood on parallel requests.
- **Hollywood's sends are fastest** everywhere, with a lighter transport and
  vtprotobuf-generated envelopes. grpcproc also pays, on every message, for
  what its Inspector reports: per-process counters, mailbox ages, and a type
  check on delivery.

```sh
cd benchmarks
go test -run '^$' -bench . -count=6 ./... | sed 's|pkg: .*/benchmarks/|pkg: |' > new.txt
benchstat -table goos,goarch,cpu -col pkg -row .name new.txt
```

## History

The same benchmarks, v0.0.0 (published as golink) against the commits after it:

| | v0.0.0 | now |
| --- | --- | --- |
| Local send | 201 ns, 2 allocs | 93 ns, 0 allocs |
| Local request | 812 ns, 5 allocs | 726 ns, 2 allocs |
| Remote send | 1570 ns, 29 allocs | 374 ns, 6 allocs |
| Remote request | 47.5 µs, 72 allocs | 41.8 µs, 52 allocs |
| Remote request, parallel | 9.6 µs, 63 allocs | 7.1 µs, 28 allocs |

Mailboxes swap batches between producers and the consumer instead of
growing a slice, read the clock once per batch, and share no counter
between sides; links write one gRPC message per batch of envelopes and
dispatch what they read without a hop through a channel; the wire envelope
is one flat message, without node names or an `Any`; and a local call waits
on a channel of its own, with no node-wide table of calls or lock between
callers.
