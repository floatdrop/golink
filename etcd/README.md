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
    Metadata:   map[string]string{"version": buildVersion}, // in its record, for every node's Members
    Resolver:   cluster,
    Registrar:  cluster,
    Membership: cluster,
    Names:      cluster.Names(), // global names: Global targets and Process.Claim
})
node.Start(ctx) // registers; Stop withdraws
```

One `Cluster` value serves every node of a process, and what reaches other
nodes through the node: the Inspector forwards through `node.Dial` with no
option of its own, and a `leader` elector with no `Membership` of its own
follows the node's.

## What it does

| grpcproc role | etcd |
| --- | --- |
| `Registrar` | one key per node, `<prefix>/nodes/<name>`, holding `{"name","incarnation","addr","metadata"}`, attached to a lease kept alive. If the lease is lost (etcd unreachable longer than the TTL), the node registers again every retry interval until it succeeds, or until it finds a newer incarnation registered. `Stop` revokes the lease, which removes the key at once. |
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

## Global names

`cluster.Names()` is the store of the installation's
[global names](https://floatdrop.github.io/grpcproc/concepts/addressing/#global):
one key per name, `<prefix>/names/<name>`, holding its holder's PID, under
the lease of the holder's node, which this `Cluster` must have registered.

| | etcd |
| --- | --- |
| A claim | a transaction that creates the key, if it does not exist; else a `*grpcproc.TakenError` naming the holder |
| A claim that waits (`WaitForName`) | a watch of the key, until it is deleted, then the claim again |
| A release | a delete, if the key's create revision is still the claim's, so that a newer claim is never deleted |
| The fencing token (`Claim.Revision`) | the key's create revision |
| `Lookup`, `List` | one copy per process, listed a page at a time when its first node starts, then watched; a broken watch lists again |
| `Resolve` | a read of the key |

A node's claims are **lost** once its lease has not been kept alive for
three quarters of the TTL: the holders end with `name lost` before etcd can
let the lease end and another node claim their names, as long as the clocks
run at the same rate; `Claim.Revision` fences what they cannot promise.
Claims made with `KeepOnLoss` are made again once the node has registered
again, all at once: a key that outlived the old lease moves to the new one
with its revision, a free name is claimed with a new one, and a name another
process took meanwhile ends the holder with `name conflict`. A node that
dies takes its names with its lease, as its peers drop their links to it.
