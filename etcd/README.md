# grpcproc/etcd

Cluster membership for [grpcproc](https://floatdrop.github.io/grpcproc/) on etcd: nodes register under
a lease they keep alive, peers resolve their addresses from it, and when a
lease ends (the node stopped, or stopped answering) every node that watches
the cluster drops its links to it. A separate module, so grpcproc itself does
not depend on the etcd client.

```sh
go get github.com/floatdrop/grpcproc/etcd
```

```go
cluster := grpcprocetcd.New(etcdClient, "/grpcproc/prod") // WithTTL, WithRetry, WithLogger
node, err := grpcproc.NewNode(grpcproc.Config{
    Name:       "orders-1",
    Advertise:  "10.0.0.5:9000", // this node's gRPC server, as peers reach it
    Resolver:   cluster,
    Registrar:  cluster,
    Membership: cluster,
})
node.Start(ctx) // registers; Stop withdraws
```

One `Cluster` value serves every node of a process, and the Inspector's
forwarding too: `inspect.WithResolver(cluster, dialOptions...)`.

## What it does

| grpcproc role | etcd |
| --- | --- |
| `Registrar` | one key per node, `<prefix>/nodes/<name>`, holding `{"name","incarnation","addr"}`, attached to a lease kept alive. If the lease is lost (etcd unreachable longer than the TTL), the node registers again every retry interval until it succeeds, or until it finds a newer incarnation registered. `Stop` revokes the lease, which removes the key at once. |
| `Resolver` | reads the key. |
| `Membership` | lists the keys (every node is reported up), then watches the prefix: a put is a member up, a delete a member down, with the incarnation from the previous value. If the watch breaks (compaction, etcd restarting), it lists again and reports what changed in between. |

A node that registers a name already present replaces it: a restarted node
supersedes its previous incarnation, whose lease may not have expired yet.
Its peers see the new incarnation come up and drop their links to the old
one. An older incarnation never replaces a newer one's record, which a
compare-and-swap settles: `Register` fails with `ErrSuperseded` until the
newer one's lease ends, and an instance that was replaced, and comes back
to etcd after losing its lease, stops registering if it finds a newer
incarnation registered, rather than take the name back. grpcproc peers
that have seen the newer one refuse its links too. Incarnations must
therefore grow with each start: with the default, the start time, the
hosts' clocks must agree.

## Why a lease and not just the links

grpcproc notices a lost link by itself, as fast as gRPC keepalive allows. A
node that dies without closing its connections, behind a half-open TCP
connection or a network partition, is noticed only when keepalive gives up,
or never if keepalive is not configured. Its lease ends after the TTL
regardless: every watching node then drops its links, monitors across them
fire `Down{noconnection}`, and pending calls fail with "left the cluster".
