// Package grpcprocetcd keeps a grpcproc cluster's membership in etcd. A node
// registers under a lease it keeps alive; peers resolve its address from
// there; and when the lease ends, because the node stopped or because it
// stopped answering, every node that watches the cluster drops its links to
// it, firing Down{noconnection} for monitors across them.
//
//	cluster := grpcprocetcd.New(etcdClient, "/grpcproc/prod")
//	node, err := grpcproc.NewNode(grpcproc.Config{
//		Name:      "orders-1",
//		Advertise: "10.0.0.5:9000",
//		Resolver:  cluster, Registrar: cluster, Membership: cluster,
//	})
//
// Each node is one key, <prefix>/nodes/<name>, holding its name,
// incarnation and address as JSON. A node that registers a name already
// present replaces it: a restarted node supersedes its previous
// incarnation, whose lease has not yet expired. An older incarnation never
// replaces a newer one.
package grpcprocetcd

import (
	"cmp"
	"context"
	"encoding/json/v2"
	"errors"
	"fmt"
	"log/slog"
	"maps"
	"slices"
	"strings"
	"sync/atomic"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"

	"github.com/floatdrop/grpcproc"
)

var (
	// ErrNotRegistered is returned by Resolve for a node absent from etcd.
	ErrNotRegistered = errors.New("grpcprocetcd: node is not registered")
	// ErrSuperseded is returned by Register when a newer incarnation of the
	// node is registered.
	ErrSuperseded = errors.New("grpcprocetcd: a newer incarnation is registered")
)

// Option configures New.
type Option func(*Cluster)

// WithTTL sets the lease TTL, how long a node that stops answering stays
// registered. It is rounded up to whole seconds. Default 10s.
func WithTTL(d time.Duration) Option {
	return func(c *Cluster) { c.ttl = int64((d + time.Second - 1) / time.Second) }
}

// WithRetry sets the pause before registering again after a lost lease, and
// before watching again after a broken watch. Default 1s.
func WithRetry(d time.Duration) Option { return func(c *Cluster) { c.retry = d } }

// WithLogger sets the logger. Default slog.Default().
func WithLogger(l *slog.Logger) Option { return func(c *Cluster) { c.log = l } }

// Cluster implements grpcproc.Resolver, grpcproc.Registrar and grpcproc.Membership
// on etcd. One value serves every node of a process.
type Cluster struct {
	kv      clientv3.KV
	lease   clientv3.Lease
	watcher clientv3.Watcher
	prefix  string
	ttl     int64
	retry   time.Duration
	log     *slog.Logger
}

var (
	_ grpcproc.Resolver   = (*Cluster)(nil)
	_ grpcproc.Registrar  = (*Cluster)(nil)
	_ grpcproc.Membership = (*Cluster)(nil)
)

// New returns a Cluster keeping its keys under prefix.
func New(cli *clientv3.Client, prefix string, opts ...Option) *Cluster {
	return newCluster(cli, cli, cli, prefix, opts...)
}

func newCluster(kv clientv3.KV, lease clientv3.Lease, watcher clientv3.Watcher, prefix string, opts ...Option) *Cluster {
	c := &Cluster{
		kv: kv, lease: lease, watcher: watcher,
		prefix: strings.TrimSuffix(prefix, "/") + "/nodes/",
		ttl:    10,
		retry:  time.Second,
	}
	for _, o := range opts {
		o(c)
	}
	c.log = cmp.Or(c.log, slog.Default())
	return c
}

type record struct {
	Name        string `json:"name"`
	Incarnation uint64 `json:"incarnation"`
	Addr        string `json:"addr,omitempty"`
}

func (c *Cluster) key(name string) string { return c.prefix + name }

func decode(value []byte) (grpcproc.Member, error) {
	var r record
	if err := json.Unmarshal(value, &r); err != nil {
		return grpcproc.Member{}, fmt.Errorf("grpcprocetcd: bad record: %w", err)
	}
	return grpcproc.Member{Name: r.Name, Incarnation: r.Incarnation, Addr: r.Addr}, nil
}

// Resolve returns the address a node registered.
func (c *Cluster) Resolve(ctx context.Context, node string) (string, error) {
	resp, err := c.kv.Get(ctx, c.key(node))
	if err != nil {
		return "", fmt.Errorf("grpcprocetcd: resolve %s: %w", node, err)
	}
	if len(resp.Kvs) == 0 {
		return "", fmt.Errorf("%w: %s", ErrNotRegistered, node)
	}
	m, err := decode(resp.Kvs[0].Value)
	if err != nil {
		return "", err
	}
	if m.Addr == "" {
		return "", fmt.Errorf("grpcprocetcd: %s registered without an address", node)
	}
	return m.Addr, nil
}

// Members lists the registered nodes, ordered by name. Records that cannot
// be decoded are skipped.
func (c *Cluster) Members(ctx context.Context) ([]grpcproc.Member, error) {
	known, _, err := c.list(ctx)
	if err != nil {
		return nil, err
	}
	out := make([]grpcproc.Member, 0, len(known))
	for _, name := range slices.Sorted(maps.Keys(known)) {
		out = append(out, known[name])
	}
	return out, nil
}

func (c *Cluster) list(ctx context.Context) (map[string]grpcproc.Member, int64, error) {
	resp, err := c.kv.Get(ctx, c.prefix, clientv3.WithPrefix())
	if err != nil {
		return nil, 0, fmt.Errorf("grpcprocetcd: list: %w", err)
	}
	known := make(map[string]grpcproc.Member, len(resp.Kvs))
	for _, kv := range resp.Kvs {
		m, err := decode(kv.Value)
		if err != nil {
			c.log.Warn("skipping member", "key", string(kv.Key), "err", err)
			continue
		}
		known[m.Name] = m
	}
	return known, resp.Header.Revision, nil
}

// Register publishes self under a lease and keeps it alive. If the lease is
// lost (etcd was unreachable for longer than the TTL), it registers again,
// every retry interval, until it succeeds or withdraw is called, or until
// it finds a newer incarnation of self registered: an instance that was
// replaced does not take the name back. It fails with ErrSuperseded while
// one is registered, until that one's lease ends; the same incarnation
// replaces its own record. withdraw revokes the lease, which removes the
// key at once.
func (c *Cluster) Register(ctx context.Context, self grpcproc.Member) (func(context.Context) error, error) {
	// Cannot fail: a struct of strings and an integer.
	value, _ := json.Marshal(record{Name: self.Name, Incarnation: self.Incarnation, Addr: self.Addr})
	id, err := c.publish(ctx, self, value)
	if err != nil {
		return nil, err
	}
	var lease atomic.Int64
	lease.Store(int64(id))
	kctx, stop := context.WithCancel(context.Background())
	done := make(chan struct{})
	go c.keep(kctx, self, value, &lease, done)
	return func(ctx context.Context) error {
		stop()
		select {
		case <-done:
		case <-ctx.Done():
			// keep is still cleaning up after etcd went out of reach; the
			// lease expires by itself.
			return fmt.Errorf("grpcprocetcd: withdraw %s: %w", self.Name, ctx.Err())
		}
		id := lease.Load()
		if id == 0 {
			return nil // superseded: nothing is published
		}
		if _, err := c.lease.Revoke(ctx, clientv3.LeaseID(id)); err != nil {
			return fmt.Errorf("grpcprocetcd: withdraw %s: %w", self.Name, err)
		}
		return nil
	}, nil
}

func (c *Cluster) publish(ctx context.Context, self grpcproc.Member, value []byte) (clientv3.LeaseID, error) {
	grant, err := c.lease.Grant(ctx, c.ttl)
	if err != nil {
		return 0, fmt.Errorf("grpcprocetcd: register %s: %w", self.Name, err)
	}
	if err := c.claim(ctx, self, value, grant.ID); err != nil {
		// Bounded, as etcd may be out of reach: the lease expires by itself
		// within the TTL.
		rctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), time.Duration(c.ttl)*time.Second)
		_, _ = c.lease.Revoke(rctx, grant.ID)
		cancel()
		return 0, err
	}
	return grant.ID, nil
}

// claim puts self's record under lease, unless a newer incarnation's is
// there. It compares and swaps, so that a registration that comes in between
// is judged too.
func (c *Cluster) claim(ctx context.Context, self grpcproc.Member, value []byte, lease clientv3.LeaseID) error {
	key := c.key(self.Name)
	for {
		resp, err := c.kv.Get(ctx, key)
		if err != nil {
			return fmt.Errorf("grpcprocetcd: register %s: %w", self.Name, err)
		}
		var rev int64 // an absent key's ModRevision compares as 0
		if len(resp.Kvs) > 0 {
			kv := resp.Kvs[0]
			if m, err := decode(kv.Value); err == nil && m.Incarnation > self.Incarnation {
				return fmt.Errorf("%w: %v, and this node is %v", ErrSuperseded,
					grpcproc.NodeID{Name: self.Name, Incarnation: m.Incarnation}, grpcproc.NodeID{Name: self.Name, Incarnation: self.Incarnation})
			}
			rev = kv.ModRevision
		}
		unchanged := clientv3.Compare(clientv3.ModRevision(key), "=", rev)
		put := clientv3.OpPut(key, string(value), clientv3.WithLease(lease))
		txn, err := c.kv.Txn(ctx).If(unchanged).Then(put).Commit()
		if err != nil {
			return fmt.Errorf("grpcprocetcd: register %s: %w", self.Name, err)
		}
		if txn.Succeeded {
			return nil
		}
	}
}

// keep keeps the lease alive until ctx is done, registering again when it
// is lost, until a newer incarnation of self is registered.
func (c *Cluster) keep(ctx context.Context, self grpcproc.Member, value []byte, lease *atomic.Int64, done chan<- struct{}) {
	defer close(done)
	for {
		alive, err := c.lease.KeepAlive(ctx, clientv3.LeaseID(lease.Load()))
		if err == nil {
			for range alive {
			}
		}
		for {
			if ctx.Err() != nil {
				return
			}
			// The lease is gone, and nothing is published under it: a
			// withdraw while this registers again has nothing to revoke.
			lease.Store(0)
			c.log.Warn("lease lost, registering again", "node", self.Name, "err", err)
			select {
			case <-ctx.Done():
				return
			case <-time.After(c.retry):
			}
			var id clientv3.LeaseID
			if id, err = c.publish(ctx, self, value); err == nil {
				lease.Store(int64(id))
				break
			}
			if errors.Is(err, ErrSuperseded) {
				c.log.Error("replaced by a newer incarnation, not registering again", "node", self.Name, "err", err)
				lease.Store(0)
				return
			}
		}
	}
}

// Watch reports every registered node as up, then follows the prefix:
// a new or replaced key is a member up, a deleted one (a withdrawn or
// expired lease) a member down. If the watch breaks (etcd restarted, its
// history compacted), it lists the members again, every retry interval
// until that works, and reports what changed in between.
func (c *Cluster) Watch(ctx context.Context) (<-chan grpcproc.MemberEvent, error) {
	known, rev, err := c.list(ctx)
	if err != nil {
		return nil, err
	}
	out := make(chan grpcproc.MemberEvent)
	go c.follow(ctx, known, rev, out)
	return out, nil
}

func (c *Cluster) follow(ctx context.Context, known map[string]grpcproc.Member, rev int64, out chan<- grpcproc.MemberEvent) {
	defer close(out)
	emit := func(m grpcproc.Member, up bool) bool {
		select {
		case out <- grpcproc.MemberEvent{Member: m, Up: up}:
			return true
		case <-ctx.Done():
			return false
		}
	}
	for _, name := range slices.Sorted(maps.Keys(known)) {
		if !emit(known[name], true) {
			return
		}
	}
	for {
		for resp := range c.watcher.Watch(ctx, c.prefix, clientv3.WithPrefix(), clientv3.WithRev(rev+1), clientv3.WithPrevKV()) {
			if err := resp.Err(); err != nil {
				c.log.Warn("watch broken, listing members again", "err", err)
				break
			}
			for _, ev := range resp.Events {
				name := strings.TrimPrefix(string(ev.Kv.Key), c.prefix)
				if ev.Type == clientv3.EventTypePut {
					m, err := decode(ev.Kv.Value)
					if err != nil {
						c.log.Warn("skipping member", "key", string(ev.Kv.Key), "err", err)
						continue
					}
					known[name] = m
					if !emit(m, true) {
						return
					}
					continue
				}
				gone := grpcproc.Member{Name: name} // incarnation 0: whichever it was
				if ev.PrevKv != nil {
					if m, err := decode(ev.PrevKv.Value); err == nil {
						gone = m
					}
				}
				delete(known, name)
				if !emit(gone, false) {
					return
				}
			}
		}
		// The watch ended: take a fresh snapshot and report the difference.
		for {
			select {
			case <-ctx.Done():
				return
			case <-time.After(c.retry):
			}
			now, nrev, err := c.list(ctx)
			if err != nil {
				c.log.Warn("listing members failed", "err", err)
				continue
			}
			for _, name := range slices.Sorted(maps.Keys(known)) {
				if m, ok := now[name]; !ok || m.Incarnation != known[name].Incarnation {
					if !emit(known[name], false) {
						return
					}
				}
			}
			for _, name := range slices.Sorted(maps.Keys(now)) {
				if m, ok := known[name]; !ok || m.Incarnation != now[name].Incarnation {
					if !emit(now[name], true) {
						return
					}
				}
			}
			known, rev = now, nrev
			break
		}
	}
}
