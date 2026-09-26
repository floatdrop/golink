# benchmarks

Separate module, so golink itself does not depend on what it is compared
with. It measures the working tree (`replace ../`).

golink against [Hollywood](https://github.com/anthdm/hollywood) v1.0.5, each
used the way it is meant to be:

- the same message, `wrapperspb.Int64Value`, because Hollywood needs protobuf
  to go remote;
- **send** is end-to-end throughput: the timer stops when the receiver has
  counted every message, not when the sender has queued them;
- **request** is one caller waiting for each reply; **parallel** is
  `RunParallel` callers against one echo;
- **remote** is two engines or nodes over real TCP on loopback, the
  connection warmed up before timing. golink's own benchmarks in the root
  module use in-memory connections, which would flatter it here.

Apple M3 Max, `-count=6`, medians by `benchstat`:

| | Hollywood | golink | |
| --- | --- | --- | --- |
| Local send | 57 ns, 0 allocs | 110 ns, 0 allocs | 1.9× slower |
| Local request | 2311 ns, 12 allocs | 753 ns, 3 allocs | 3.1× faster |
| Remote send | 208 ns, 7 allocs | 354 ns, 6 allocs | 1.7× slower |
| Remote request | 37.3 µs, 66 allocs | 42.2 µs, 52 allocs | 13% slower |
| Remote request, parallel | 4.9 µs, 54 allocs | 7.8 µs, 28 allocs | 1.6× slower |

What is left of the gap is mostly structural:

- golink keeps per-process counters and mailbox ages for `ProcessInfo`, and
  checks each message's type against the process's on delivery;
- it speaks gRPC, whose writer adds a goroutine hand-off in each direction
  that Hollywood's dRPC transport does not have. That is most of the
  sequential remote request's difference; on a real network the round trip
  dwarfs it;
- the wire envelope is marshalled with protobuf reflection, where Hollywood
  uses vtprotobuf.

```sh
cd benchmarks && go test -run '^$' -bench . -count=6 > new.txt
benchstat new.txt   # or compare two runs: benchstat old.txt new.txt
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
