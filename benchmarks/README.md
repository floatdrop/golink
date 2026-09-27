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
| Local send | 99 ns, 0 allocs | 96 ns, 0 allocs | **58 ns**, 0 allocs | 191 ns, 0 allocs |
| Local request | 738 ns, 3 allocs | **565 ns**, 2 allocs | 2265 ns, 12 allocs | 2354 ns, 10 allocs |
| Remote send | 368 ns, 6 allocs | 514 ns (±22%), 7 allocs | **205 ns**, 7 allocs | 315 ns, 9 allocs |
| Remote request | 44.4 µs, 52 allocs | **34.5 µs**, 42 allocs | 36.4 µs, 66 allocs | 58.6 µs, 103 allocs |
| Remote request, parallel | 7.5 µs, 28 allocs | 10.2 µs, 40 allocs | **4.9 µs**, 54 allocs | 7.1 µs, 54 allocs |
| Geometric mean | 1.55 µs | 1.58 µs | **1.37 µs** | 2.26 µs |

Proto.Actor's local send varies between runs (134 ns to 191 ns in two runs
of six), and so does GoAkt's remote send; the others hold within a few
percent.

How to read it:

- **The transport decides sequential remote latency.** GoAkt (its own TCP
  protocol) and Hollywood (dRPC) answer a remote request in 35–36 µs; the
  two that speak gRPC take longer, grpcproc 44 µs and Proto.Actor 59 µs.
  gRPC-go's writer adds a goroutine hand-off in each direction. On a real
  network the round trip dwarfs the difference; for grpcproc it is the cost
  of living on the application's gRPC server.
- **Against the other gRPC library**, grpcproc answers a remote request 24%
  sooner than Proto.Actor and trails it by about 50 ns on remote send, where
  Proto.Actor's writer batches up to a thousand envelopes.
- **Locally**, a grpcproc call waits on one channel where Hollywood and
  Proto.Actor create a temporary process per request, which puts it three
  times ahead of them. GoAkt is quicker still: grpcproc allocates a pending
  call and its channel per call, which a pool could remove.
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
| Local send | 201 ns, 2 allocs | 110 ns, 0 allocs |
| Local request | 812 ns, 5 allocs | 753 ns, 3 allocs |
| Remote send | 1570 ns, 29 allocs | 354 ns, 6 allocs |
| Remote request | 47.5 µs, 72 allocs | 42.2 µs, 52 allocs |
| Remote request, parallel | 9.6 µs, 63 allocs | 7.8 µs, 28 allocs |

Mailboxes swap batches between producers and the consumer instead of
growing a slice, read the clock once per batch, and share no counter
between sides; links write one gRPC message per batch of envelopes and
dispatch what they read without a hop through a channel; and the wire
envelope is one flat message, without node names or an `Any`.
