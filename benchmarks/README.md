# benchmarks

Separate module, so golink itself does not depend on what it is compared
with. It measures the working tree (`replace ../`).

golink against [Hollywood](https://github.com/anthdm/hollywood) v1.0.5 and
[Proto.Actor](https://github.com/asynkron/protoactor-go) (its development
branch; it has no Go-style release tags), each used the way it is meant to
be:

- the same message, `wrapperspb.Int64Value`, because Hollywood and
  Proto.Actor need protobuf to go remote;
- **send** is end-to-end throughput: the timer stops when the receiver has
  counted every message, not when the sender has queued them;
- **request** is one caller waiting for each reply; **parallel** is
  `RunParallel` callers against one echo;
- **remote** is two engines, systems or nodes over real TCP on loopback, the
  connection warmed up before timing. golink's own benchmarks in the root
  module use in-memory connections, which would flatter it here.

Each framework is its own package: Hollywood and Proto.Actor both register a
protobuf file named `actor.proto`, which cannot share a binary, and
separate binaries also keep one framework's globals (Proto.Actor replaces
gRPC's logger) out of another's numbers.

Apple M3 Max, `-count=6`, medians by `benchstat`:

| | golink | Hollywood | Proto.Actor |
| --- | --- | --- | --- |
| Local send | 96 ns, 0 allocs | **60 ns**, 0 allocs | 134 ns (±56%), 0 allocs |
| Local request | **746 ns**, 3 allocs | 2282 ns, 12 allocs | 2305 ns, 10 allocs |
| Remote send | 366 ns, 6 allocs | **208 ns**, 7 allocs | 301 ns, 9 allocs |
| Remote request | 42.4 µs, 52 allocs | **37.5 µs**, 66 allocs | 57.3 µs, 103 allocs |
| Remote request, parallel | 7.5 µs, 28 allocs | **5.0 µs**, 54 allocs | 6.8 µs, 54 allocs |
| Geometric mean | 1.53 µs | **1.40 µs** | 2.05 µs |

Proto.Actor is the useful control: it speaks gRPC between nodes too, so
what separates it from golink is not the transport. golink answers a remote
request 26% sooner than it and a local one three times sooner (a call waits
on one channel, where both others create a temporary process per request),
and allocates the least everywhere. It still trails Proto.Actor on remote
send, where Proto.Actor's writer batches up to a thousand envelopes, and
Hollywood on every send, where its lighter dRPC transport and
vtprotobuf-generated envelope count. golink also pays, on every message,
for what its Inspector reports: per-process counters, mailbox ages, and a
type check on delivery.

```sh
cd benchmarks
go test -run '^$' -bench . -count=6 ./... | sed 's|pkg: .*/benchmarks/|pkg: |' > new.txt
benchstat -table goos,goarch,cpu -col pkg -row .name new.txt
```

## History

The same benchmarks, golink v0.0.0 against the commits after it:

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
