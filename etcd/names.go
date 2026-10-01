package grpcprocetcd

import (
	"context"
	"encoding/json/v2"
	"errors"
	"fmt"
	"maps"
	"slices"
	"strings"
	"sync"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"

	"github.com/floatdrop/grpcproc"
)

// ErrNoLease is the error of a claim for a node this Cluster has not
// registered, or whose lease is lost: a claim lives under its node's lease.
var ErrNoLease = errors.New("grpcprocetcd: the node holds no lease")

// pageSize is how many names one Get of the initial list returns.
const pageSize = 1000

// Names is the installation's global names in etcd, a grpcproc.Names for
// Config.Names, from Cluster.Names. Each name is a key, <prefix>/names/<name>,
// holding its holder's PID, under the lease its node registered with: a node
// that dies takes its names with it, as its peers drop their links to it.
//
// Every node of the process shares one copy of the names, listed when the
// first node starts and kept by a watch, which Lookup and List read. A claim
// is a compare-and-swap that creates the key. A claim that waits watches the
// key until it is deleted. The node's claims are lost once its lease has not
// been kept alive for three quarters of the TTL, before etcd can let it end
// and another node claim the names; claims made with KeepOnLoss are made
// again once the node has registered again.
type Names struct {
	c      *Cluster
	prefix string
	page   int64

	mu       sync.RWMutex
	held     map[string]grpcproc.PID // the copy; guarded by mu
	watchers int                     // nodes watching; guarded by mu
	stop     context.CancelFunc      // ends the watch; guarded by mu

	claimsMu sync.Mutex
	claims   map[string]map[*claim]struct{} // by node; guarded by claimsMu
}

var (
	_ grpcproc.Names      = (*Names)(nil)
	_ grpcproc.NameLister = (*Names)(nil)
)

// Names returns the store of global names that shares this Cluster's etcd
// and its nodes' leases. Claims are made for nodes this Cluster registered.
func (c *Cluster) Names() *Names {
	c.namesOnce.Do(func() {
		c.names.Store(&Names{c: c, prefix: c.root + "/names/", page: pageSize, held: map[string]grpcproc.PID{}, claims: map[string]map[*claim]struct{}{}})
	})
	return c.names.Load()
}

type pidRecord struct {
	Node        string `json:"node"`
	Incarnation uint64 `json:"incarnation"`
	ID          uint64 `json:"id"`
}

func encodePID(pid grpcproc.PID) string {
	// Cannot fail: a struct of a string and integers.
	b, _ := json.Marshal(pidRecord{Node: pid.Node, Incarnation: pid.Incarnation, ID: pid.ID})
	return string(b)
}

func decodePID(value []byte) (grpcproc.PID, error) {
	var r pidRecord
	if err := json.Unmarshal(value, &r); err != nil {
		return grpcproc.PID{}, fmt.Errorf("grpcprocetcd: bad name record: %w", err)
	}
	return grpcproc.PID{Node: r.Node, Incarnation: r.Incarnation, ID: r.ID}, nil
}

func (n *Names) key(name string) string { return n.prefix + name }

// Watch lists the names, if no node of this process watches them yet, and
// keeps the copy until the last node's ctx is done.
func (n *Names) Watch(ctx context.Context) error {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.watchers == 0 {
		held, rev, err := n.list(ctx)
		if err != nil {
			return err
		}
		wctx, stop := context.WithCancel(context.Background())
		n.held, n.stop = held, stop
		go n.follow(wctx, rev)
	}
	n.watchers++
	context.AfterFunc(ctx, func() {
		n.mu.Lock()
		defer n.mu.Unlock()
		if n.watchers--; n.watchers == 0 {
			n.stop()
		}
	})
	return nil
}

// list reads every name, a page at a time, all at one revision.
func (n *Names) list(ctx context.Context) (map[string]grpcproc.PID, int64, error) {
	held := map[string]grpcproc.PID{}
	key, end := n.prefix, clientv3.GetPrefixRangeEnd(n.prefix)
	var rev int64
	for {
		opts := []clientv3.OpOption{clientv3.WithRange(end), clientv3.WithLimit(n.page)}
		if rev != 0 {
			opts = append(opts, clientv3.WithRev(rev))
		}
		resp, err := n.c.kv.Get(ctx, key, opts...)
		if err != nil {
			return nil, 0, fmt.Errorf("grpcprocetcd: list names: %w", err)
		}
		if rev == 0 {
			rev = resp.Header.Revision
		}
		for _, kv := range resp.Kvs {
			n.put(held, kv)
		}
		if !resp.More {
			return held, rev, nil
		}
		key = string(resp.Kvs[len(resp.Kvs)-1].Key) + "\x00"
	}
}

// put records kv's name in held, skipping a record it cannot read.
func (n *Names) put(held map[string]grpcproc.PID, kv interface {
	GetKey() []byte
	GetValue() []byte
}) {
	pid, err := decodePID(kv.GetValue())
	if err != nil {
		n.c.log.Warn("skipping name", "key", string(kv.GetKey()), "err", err)
		return
	}
	held[strings.TrimPrefix(string(kv.GetKey()), n.prefix)] = pid
}

// follow keeps the copy, from rev on, until ctx is done. A broken watch
// lists the names again, every retry interval until that works.
func (n *Names) follow(ctx context.Context, rev int64) {
	for {
		for resp := range n.c.watcher.Watch(ctx, n.prefix, clientv3.WithPrefix(), clientv3.WithRev(rev+1)) {
			if err := resp.Err(); err != nil {
				n.c.log.Warn("names watch broken, listing again", "err", err)
				break
			}
			n.mu.Lock()
			for _, ev := range resp.Events {
				if ev.Type == clientv3.EventTypePut {
					n.put(n.held, ev.Kv)
				} else {
					delete(n.held, strings.TrimPrefix(string(ev.Kv.Key), n.prefix))
				}
			}
			n.mu.Unlock()
		}
		// The watch ended: a fresh list replaces the copy, whatever it missed.
		for {
			select {
			case <-ctx.Done():
				return
			case <-time.After(n.c.retry):
			}
			held, nrev, err := n.list(ctx)
			if err != nil {
				n.c.log.Warn("listing names failed", "err", err)
				continue
			}
			n.mu.Lock()
			n.held = held
			n.mu.Unlock()
			rev = nrev
			break
		}
	}
}

// Lookup returns the holder of name, as this process's copy has it.
func (n *Names) Lookup(name string) (grpcproc.PID, bool) {
	n.mu.RLock()
	defer n.mu.RUnlock()
	pid, ok := n.held[name]
	return pid, ok
}

// List returns the names with prefix, ordered by name, at most limit unless
// limit is 0, as this process's copy has them.
func (n *Names) List(prefix string, limit int) []grpcproc.GlobalName {
	n.mu.RLock()
	defer n.mu.RUnlock()
	var out []grpcproc.GlobalName
	for _, name := range slices.Sorted(maps.Keys(n.held)) {
		if strings.HasPrefix(name, prefix) && (limit == 0 || len(out) < limit) {
			out = append(out, grpcproc.GlobalName{Name: name, PID: n.held[name]})
		}
	}
	return out
}

// Resolve reads the holder of name from etcd.
func (n *Names) Resolve(ctx context.Context, name string) (grpcproc.PID, bool, error) {
	resp, err := n.c.kv.Get(ctx, n.key(name))
	if err != nil {
		return grpcproc.PID{}, false, fmt.Errorf("grpcprocetcd: resolve name %s: %w", name, err)
	}
	if len(resp.Kvs) == 0 {
		return grpcproc.PID{}, false, nil
	}
	pid, err := decodePID(resp.Kvs[0].Value)
	if err != nil {
		return grpcproc.PID{}, false, err
	}
	return pid, true, nil
}

// claim is a name a process holds, under its node's lease.
type claim struct {
	n      *Names
	name   string
	holder grpcproc.PID
	keep   bool
	notify func(grpcproc.ClaimEvent)

	// guarded by n.claimsMu
	rev     int64
	pending bool // lost, to be made again: KeepOnLoss
}

func (c *claim) Revision() int64 {
	c.n.claimsMu.Lock()
	defer c.n.claimsMu.Unlock()
	return c.rev
}

// Release deletes the name, if this claim still holds it, comparing the
// key's create revision with the claim's.
func (c *claim) Release(ctx context.Context) error {
	n := c.n
	n.claimsMu.Lock()
	rev, pending := c.rev, c.pending
	delete(n.claims[c.holder.Node], c)
	n.claimsMu.Unlock()
	if pending {
		return nil // lost: its key went with its lease
	}
	key := n.key(c.name)
	if _, err := n.c.kv.Txn(ctx).If(clientv3.Compare(clientv3.CreateRevision(key), "=", rev)).Then(clientv3.OpDelete(key)).Commit(); err != nil {
		return fmt.Errorf("grpcprocetcd: release name %s: %w", c.name, err)
	}
	return nil
}

// Claim creates name for holder under the lease of holder's node, which this
// Cluster must have registered: ErrNoLease otherwise, or while the lease is
// lost. A name that exists fails with a *grpcproc.TakenError naming its
// holder, or, with opts.Wait, is watched until it is deleted, and claimed
// then, until ctx is done.
func (n *Names) Claim(ctx context.Context, name string, holder grpcproc.PID, opts grpcproc.ClaimOptions, notify func(grpcproc.ClaimEvent)) (grpcproc.NameClaim, error) {
	key, value := n.key(name), encodePID(holder)
	for {
		lease := n.c.leaseOf(holder.Node)
		if lease == 0 {
			return nil, fmt.Errorf("%w: claim %s for %v", ErrNoLease, name, holder)
		}
		resp, err := n.c.kv.Txn(ctx).
			If(clientv3.Compare(clientv3.CreateRevision(key), "=", 0)).
			Then(clientv3.OpPut(key, value, clientv3.WithLease(lease))).
			Else(clientv3.OpGet(key)).
			Commit()
		if err != nil {
			return nil, fmt.Errorf("grpcprocetcd: claim name %s: %w", name, err)
		}
		if resp.Succeeded {
			c := &claim{n: n, name: name, holder: holder, keep: opts.KeepOnLoss, notify: notify, rev: resp.Header.Revision}
			n.track(c, lease)
			return c, nil
		}
		// The Else's read, in the same transaction: the key is there.
		kv := resp.Responses[0].GetResponseRange().GetKvs()[0]
		if !opts.Wait {
			taken, _ := decodePID(kv.Value) // unreadable: a zero holder
			return nil, &grpcproc.TakenError{Name: name, Holder: taken}
		}
		if err := n.waitFree(ctx, key, resp.Header.Revision); err != nil {
			return nil, err
		}
	}
}

// waitFree returns once key is deleted after rev, or the watch breaks, for
// the claim to try again; or with ctx's error.
func (n *Names) waitFree(ctx context.Context, key string, rev int64) error {
	wctx, cancel := context.WithCancel(ctx)
	defer cancel()
	for resp := range n.c.watcher.Watch(wctx, key, clientv3.WithRev(rev+1)) {
		for _, ev := range resp.Events {
			if ev.Type == clientv3.EventTypeDelete {
				return nil
			}
		}
		if resp.Err() != nil {
			return nil
		}
	}
	return ctx.Err()
}

// track records c among its node's claims. If the lease it was made under
// was lost meanwhile, so is c, at once.
func (n *Names) track(c *claim, lease clientv3.LeaseID) {
	node := c.holder.Node
	n.claimsMu.Lock()
	lost := n.c.leaseOf(node) != lease
	if !lost || c.keep {
		if n.claims[node] == nil {
			n.claims[node] = map[*claim]struct{}{}
		}
		n.claims[node][c] = struct{}{}
		c.pending = lost
	}
	n.claimsMu.Unlock()
	if lost {
		c.notify(grpcproc.ClaimEvent{Kind: grpcproc.ClaimLost})
	}
}

// lost tells node's claims they are lost. A KeepOnLoss one waits to be made
// again; the others are over.
func (n *Names) lost(node string) {
	n.claimsMu.Lock()
	var told []*claim
	for c := range n.claims[node] {
		if c.pending {
			continue
		}
		told = append(told, c)
		if c.keep {
			c.pending = true
		} else {
			delete(n.claims[node], c)
		}
	}
	n.claimsMu.Unlock()
	for _, c := range told {
		c.notify(grpcproc.ClaimEvent{Kind: grpcproc.ClaimLost})
	}
}

// regained makes node's lost KeepOnLoss claims again under its new lease:
// each holds its name again, or finds it taken and is over. An etcd error is
// tried again every retry interval, while the lease is still node's.
func (n *Names) regained(node string, lease clientv3.LeaseID) {
	n.claimsMu.Lock()
	var pending []*claim
	for c := range n.claims[node] {
		if c.pending {
			pending = append(pending, c)
		}
	}
	n.claimsMu.Unlock()
	for _, c := range pending {
		for n.c.leaseOf(node) == lease {
			ev, err := n.reclaim(c, lease)
			if err == nil {
				n.claimsMu.Lock()
				if ev.Kind == grpcproc.ClaimRegained {
					c.rev, c.pending = ev.Revision, false
				} else {
					delete(n.claims[node], c)
				}
				n.claimsMu.Unlock()
				c.notify(ev)
				break
			}
			n.c.log.Warn("claiming a name again failed", "name", c.name, "err", err)
			time.Sleep(n.c.retry)
		}
	}
}

// reclaim makes c again under lease. A key that survived its old lease, with
// c's holder in it, moves to the new one and keeps its revision.
func (n *Names) reclaim(c *claim, lease clientv3.LeaseID) (grpcproc.ClaimEvent, error) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(n.c.ttl)*time.Second)
	defer cancel()
	key, value := n.key(c.name), encodePID(c.holder)
	resp, err := n.c.kv.Txn(ctx).
		If(clientv3.Compare(clientv3.CreateRevision(key), "=", 0)).
		Then(clientv3.OpPut(key, value, clientv3.WithLease(lease))).
		Else(clientv3.OpGet(key)).
		Commit()
	if err != nil {
		return grpcproc.ClaimEvent{}, err
	}
	if resp.Succeeded {
		return grpcproc.ClaimEvent{Kind: grpcproc.ClaimRegained, Revision: resp.Header.Revision}, nil
	}
	kv := resp.Responses[0].GetResponseRange().GetKvs()[0] // the Else's read: the key is there
	if string(kv.Value) != value {
		return grpcproc.ClaimEvent{Kind: grpcproc.ClaimConflict}, nil
	}
	moved, err := n.c.kv.Txn(ctx).
		If(clientv3.Compare(clientv3.ModRevision(key), "=", kv.ModRevision)).
		Then(clientv3.OpPut(key, value, clientv3.WithLease(lease))).
		Commit()
	if err != nil {
		return grpcproc.ClaimEvent{}, err
	}
	if !moved.Succeeded {
		return grpcproc.ClaimEvent{}, errors.New("grpcprocetcd: the name changed while it was claimed again")
	}
	return grpcproc.ClaimEvent{Kind: grpcproc.ClaimRegained, Revision: kv.CreateRevision}, nil
}
