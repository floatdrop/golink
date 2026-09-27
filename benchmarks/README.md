# benchmarks

Separate module, so grpcproc itself does not depend on what it is compared
with. It measures the working tree (`replace ../`).

grpcproc against [GoAkt](https://github.com/tochemey/goakt) v4.5.6,
[Hollywood](https://github.com/anthdm/hollywood) v1.0.5 and
[Proto.Actor](https://github.com/asynkron/protoactor-go) (its development
branch; it has no Go-style release tags), each used the way it is meant to
be:

- the same message, `wrapperspb.Int64Value`, because the others need
  protobuf to go remote;
- **send** is end-to-end throughput: the timer stops when the receiver has
  counted every message, not when the sender has queued them;
- **request** is one caller waiting for each reply; **parallel** is
  `RunParallel` callers against one echo;
- **remote** is two engines, systems or nodes over real TCP on loopback, the
  connection warmed up before timing. grpcproc's own benchmarks in the root
  module use in-memory connections, which would flatter it here. GoAkt's
  remote actors are found with the public `PID.RemoteLookup` and reached
  through its remoting client, as an application would.

Each framework is its own package: Hollywood and Proto.Actor both register a
protobuf file named `actor.proto`, which cannot share a binary, and
separate binaries also keep one framework's globals (Proto.Actor replaces
gRPC's logger) out of another's numbers.

Apple M3 Max, `-count=6`, medians by `benchstat`:

| | grpcproc | GoAkt | Hollywood | Proto.Actor |
| --- | --- | --- | --- | --- |
| Local send | 93 ns, 0 allocs | 106 ns (±16%), 0 allocs | **58 ns**, 0 allocs | 189 ns, 0 allocs |
| Local request | 726 ns, 2 allocs | **574 ns**, 2 allocs | 2246 ns, 12 allocs | 2321 ns (±13%), 10 allocs |
| Remote send | 374 ns, 6 allocs | 453 ns (±11%), 6 allocs | **211 ns**, 7 allocs | 309 ns, 9 allocs |
| Remote request | 41.8 µs, 52 allocs | **34.7 µs**, 42 allocs | 35.9 µs, 66 allocs | 53.8 µs, 103 allocs |
| Remote request, parallel | 7.1 µs, 28 allocs | 10.1 µs, 40 allocs | **5.1 µs**, 54 allocs | 6.9 µs (±12%), 54 allocs |
| Geometric mean | 1.50 µs | 1.58 µs | **1.38 µs** | 2.19 µs |

Proto.Actor's local send varies between runs (94 ns and 189 ns in two runs
of six), and GoAkt's sends and some of Proto.Actor's requests vary within
one, as marked; the others hold within a few percent.

How to read it:

- **The transport decides sequential remote latency.** GoAkt (its own TCP
  protocol) and Hollywood (dRPC) answer a remote request in 35–36 µs; the
  two that speak gRPC take longer, grpcproc 42 µs and Proto.Actor 54 µs.
  gRPC-go's writer adds a goroutine hand-off in each direction. On a real
  network the round trip dwarfs the difference; for grpcproc it is the cost
  of living on the application's gRPC server.
- **Against the other gRPC library**, grpcproc answers a remote request 22%
  sooner than Proto.Actor and trails it by about 65 ns on remote send, where
  Proto.Actor's writer batches up to a thousand envelopes.
- **Locally**, a grpcproc call waits on one channel of its own where
  Hollywood and Proto.Actor create a temporary process per request, which
  puts it three times ahead of them. GoAkt is quicker still, with as many
  allocations per call.
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
