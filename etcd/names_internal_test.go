package grpcprocetcd

import (
	"context"
	"errors"
	"testing"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"

	"github.com/floatdrop/grpcproc"
)

// events collects what a store tells of a claim.
type events chan grpcproc.ClaimEvent

func (e events) notify(ev grpcproc.ClaimEvent) { e <- ev }

func (e events) next(t *testing.T) grpcproc.ClaimEvent {
	t.Helper()
	select {
	case ev := <-e:
		return ev
	case <-time.After(10 * time.Second):
		t.Fatal("no claim event")
		return grpcproc.ClaimEvent{}
	}
}

// registered is env with node a registered, and its names.
func registered(t *testing.T) (env, *Names) {
	t.Helper()
	e := setup(t)
	if _, err := e.c.Register(t.Context(), grpcproc.Member{Name: "a", Incarnation: 1, Addr: "a:1"}); err != nil {
		t.Fatal(err)
	}
	return e, e.c.Names()
}

func pid(id uint64) grpcproc.PID { return grpcproc.PID{Node: "a", Incarnation: 1, ID: id} }

func claimOK(t *testing.T, n *Names, name string, holder grpcproc.PID, opts grpcproc.ClaimOptions, ev events) *claim {
	t.Helper()
	c, err := n.Claim(t.Context(), name, holder, opts, ev.notify)
	if err != nil {
		t.Fatal(err)
	}
	return c.(*claim)
}

func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	for deadline := time.Now().Add(10 * time.Second); !cond(); time.Sleep(5 * time.Millisecond) {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
	}
}

// The copy is listed a page at a time, skips what it cannot read, follows
// puts and deletes, and lists again when the watch breaks.
func TestNamesCopy(t *testing.T) {
	e, n := registered(t)
	n.page = 1
	for _, name := range []string{"x", "y", "z"} {
		claimOK(t, n, name, pid(1), grpcproc.ClaimOptions{}, make(events, 4))
	}
	_, _ = e.cli.Put(t.Context(), "/x/names/junk", "{not json")
	e.kv.failGets.Store(1)
	if err := n.Watch(t.Context()); !errors.Is(err, errInjected) {
		t.Fatalf("watch: %v", err)
	}
	first := make(chan clientv3.WatchResponse, 1)
	e.watcher.first = first
	ctx, cancel := context.WithCancel(t.Context())
	if err := n.Watch(ctx); err != nil {
		t.Fatal(err)
	}
	ctx2, cancel2 := context.WithCancel(t.Context())
	if err := n.Watch(ctx2); err != nil { // a second node shares the copy
		t.Fatal(err)
	}
	if list := n.List("", 0); len(list) != 3 {
		t.Fatalf("list %v", list)
	}
	if list := n.List("y", 1); len(list) != 1 || list[0].Name != "y" {
		t.Fatalf("list y %v", list)
	}
	// The watch breaks, and the first list after it fails.
	_, _ = e.cli.Delete(t.Context(), "/x/names/x")
	e.kv.failGets.Store(1)
	first <- clientv3.WatchResponse{CompactRevision: 1}
	waitFor(t, "x to go", func() bool { _, ok := n.Lookup("x"); return !ok })
	// The real watch from then on: a delete and a put.
	_, _ = e.cli.Delete(t.Context(), "/x/names/y")
	_, _ = e.cli.Put(t.Context(), "/x/names/w", encodePID(pid(9)))
	waitFor(t, "y to go and w to come", func() bool {
		_, y := n.Lookup("y")
		w, _ := n.Lookup("w")
		return !y && w == pid(9)
	})
	cancel()
	cancel2()
	waitFor(t, "the watch to stop", func() bool {
		n.mu.RLock()
		defer n.mu.RUnlock()
		return n.watchers == 0
	})

	// A watch whose list fails is retried until its ctx is done.
	first = make(chan clientv3.WatchResponse, 1)
	e.watcher.first = first
	ctx3, cancel3 := context.WithCancel(t.Context())
	if err := n.Watch(ctx3); err != nil {
		t.Fatal(err)
	}
	e.kv.failGets.Store(1 << 20)
	first <- clientv3.WatchResponse{CompactRevision: 1}
	time.Sleep(50 * time.Millisecond)
	cancel3()
	time.Sleep(50 * time.Millisecond)
	e.kv.failGets.Store(0)
}

func TestNamesResolve(t *testing.T) {
	e, n := registered(t)
	claimOK(t, n, "x", pid(1), grpcproc.ClaimOptions{}, make(events, 4))
	if got, ok, err := n.Resolve(t.Context(), "x"); err != nil || !ok || got != pid(1) {
		t.Fatalf("x: %v %v %v", got, ok, err)
	}
	if _, ok, err := n.Resolve(t.Context(), "nobody"); err != nil || ok {
		t.Fatalf("nobody: %v %v", ok, err)
	}
	_, _ = e.cli.Put(t.Context(), "/x/names/junk", "{not json")
	if _, _, err := n.Resolve(t.Context(), "junk"); err == nil {
		t.Fatal("junk resolved")
	}
	e.kv.failGets.Store(1)
	if _, _, err := n.Resolve(t.Context(), "x"); !errors.Is(err, errInjected) {
		t.Fatalf("resolve: %v", err)
	}
}

func TestNamesClaim(t *testing.T) {
	e, n := registered(t)
	// Only for a node this Cluster registered.
	if _, err := n.Claim(t.Context(), "x", grpcproc.PID{Node: "b"}, grpcproc.ClaimOptions{}, nil); !errors.Is(err, ErrNoLease) {
		t.Fatalf("claim for b: %v", err)
	}
	e.kv.failTxns(errInjected)
	if _, err := n.Claim(t.Context(), "x", pid(1), grpcproc.ClaimOptions{}, nil); !errors.Is(err, errInjected) {
		t.Fatalf("claim: %v", err)
	}
	e.kv.failTxns(nil)
	c := claimOK(t, n, "x", pid(1), grpcproc.ClaimOptions{}, make(events, 4))
	var taken *grpcproc.TakenError
	if _, err := n.Claim(t.Context(), "x", pid(2), grpcproc.ClaimOptions{}, nil); !errors.As(err, &taken) || taken.Holder != pid(1) {
		t.Fatalf("taken: %v", err)
	}

	// A claim that waits: through a broken watch, then a put, then the
	// delete it waits for.
	first := make(chan clientv3.WatchResponse, 1)
	e.watcher.first = first
	got := make(chan grpcproc.NameClaim, 1)
	go func() {
		c2, err := n.Claim(t.Context(), "x", pid(2), grpcproc.ClaimOptions{Wait: true}, make(events, 4).notify)
		if err != nil {
			t.Error(err)
		}
		got <- c2
	}()
	first <- clientv3.WatchResponse{CompactRevision: 1}
	time.Sleep(50 * time.Millisecond)
	_, _ = e.cli.Put(t.Context(), "/x/names/x", encodePID(pid(1)), clientv3.WithIgnoreLease())
	if err := c.Release(t.Context()); err != nil {
		t.Fatal(err)
	}
	select {
	case c2 := <-got:
		if c2.Revision() <= c.Revision() {
			t.Fatalf("revision %d after %d", c2.Revision(), c.Revision())
		}
	case <-time.After(10 * time.Second):
		t.Fatal("the waiting claim never claimed")
	}

	// A wait ends with its ctx.
	ctx, cancel := context.WithTimeout(t.Context(), 50*time.Millisecond)
	defer cancel()
	if _, err := n.Claim(ctx, "x", pid(3), grpcproc.ClaimOptions{Wait: true}, nil); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("wait: %v", err)
	}
	// A release that fails.
	c3 := claimOK(t, n, "y", pid(4), grpcproc.ClaimOptions{}, make(events, 4))
	e.kv.failTxns(errInjected)
	if err := c3.Release(t.Context()); !errors.Is(err, errInjected) {
		t.Fatalf("release: %v", err)
	}
	e.kv.failTxns(nil)
}

// A claim made under a lease that is lost before it is tracked is lost at
// once: over, or, with KeepOnLoss, waiting to be made again.
func TestNamesClaimLostWhileMade(t *testing.T) {
	e, n := registered(t)
	for _, keep := range []bool{false, true} {
		ev := make(events, 4)
		e.kv.beforeNextTxn(func() { e.c.leases["a"].Store(0) })
		c := claimOK(t, n, "x", pid(1), grpcproc.ClaimOptions{KeepOnLoss: keep}, ev)
		if got := ev.next(t); got.Kind != grpcproc.ClaimLost {
			t.Fatalf("event %v", got)
		}
		n.claimsMu.Lock()
		_, tracked := n.claims["a"][c]
		n.claimsMu.Unlock()
		if tracked != keep || c.pending != keep {
			t.Fatalf("keep=%v: tracked %v, pending %v", keep, tracked, c.pending)
		}
		if err := c.Release(t.Context()); err != nil { // pending: nothing to delete
			t.Fatal(err)
		}
		_, _ = e.cli.Delete(t.Context(), "/x/names/x")
		e.c.leases["a"].Store(int64(leaseOf(t, e, "a")))
	}
}

// leaseOf reads node's lease from its record.
func leaseOf(t *testing.T, e env, node string) clientv3.LeaseID {
	t.Helper()
	resp, err := e.cli.Get(t.Context(), "/x/nodes/"+node)
	if err != nil || len(resp.Kvs) == 0 {
		t.Fatalf("%s: %v", node, err)
	}
	return clientv3.LeaseID(resp.Kvs[0].Lease)
}

// Claims made again: one whose key survived its lease moves to the new one
// and keeps its revision; one whose name was taken ends in conflict; an etcd
// error is tried again.
func TestNamesRegained(t *testing.T) {
	e, n := registered(t)
	lease := e.c.leaseOf("a")
	survivor := make(events, 4)
	s := claimOK(t, n, "s", pid(1), grpcproc.ClaimOptions{KeepOnLoss: true}, survivor)
	strict := make(events, 4)
	claimOK(t, n, "u", pid(3), grpcproc.ClaimOptions{}, strict)
	n.lost("a")
	n.lost("a") // again: the pending one is not told twice
	for _, ev := range []events{survivor, strict} {
		if got := ev.next(t); got.Kind != grpcproc.ClaimLost {
			t.Fatalf("event %v", got)
		}
	}
	before := s.Revision()

	// The first two tries fail, then the move of the survivor's key does
	// once, then it moves.
	e.kv.failTxn.Store(2)
	e.kv.beforeNextTxn(func() { e.kv.failTxn.Store(1) })
	n.regained("a", lease)
	if got := survivor.next(t); got.Kind != grpcproc.ClaimRegained || got.Revision != before {
		t.Fatalf("survivor %v, revision %d", got, before)
	}

	// A name another process took meanwhile ends its claim in conflict.
	taken := make(events, 4)
	claimOK(t, n, "t", pid(2), grpcproc.ClaimOptions{KeepOnLoss: true}, taken)
	n.lost("a")
	survivor.next(t)
	taken.next(t)
	_, _ = e.cli.Put(t.Context(), "/x/names/t", encodePID(pid(99)))
	n.regained("a", lease)
	if got := survivor.next(t); got.Kind != grpcproc.ClaimRegained {
		t.Fatalf("survivor %v", got)
	}
	if got := taken.next(t); got.Kind != grpcproc.ClaimConflict {
		t.Fatalf("taken %v", got)
	}

	// A key that changes between the read and the move is tried again.
	n.lost("a")
	survivor.next(t)
	e.kv.beforeNextTxn(func() {
		e.kv.beforeNextTxn(func() { _, _ = e.cli.Put(context.Background(), "/x/names/s", encodePID(pid(1))) })
	})
	n.regained("a", lease)
	if got := survivor.next(t); got.Kind != grpcproc.ClaimRegained {
		t.Fatalf("survivor %v", got)
	}
	// A lease that changed again: nothing is tried.
	n.lost("a")
	survivor.next(t)
	n.regained("a", lease+1)
	select {
	case got := <-survivor:
		t.Fatalf("event %v", got)
	case <-time.After(20 * time.Millisecond):
	}
}

// A keepalive that fails at once is a lost lease too.
func TestKeepAliveFails(t *testing.T) {
	e, n := registered(t)
	ev := make(events, 4)
	claimOK(t, n, "x", pid(1), grpcproc.ClaimOptions{KeepOnLoss: true}, ev)
	e.lease.failKeeps.Store(1)
	if _, err := e.cli.Revoke(t.Context(), leaseOf(t, e, "a")); err != nil {
		t.Fatal(err)
	}
	if got := ev.next(t); got.Kind != grpcproc.ClaimLost {
		t.Fatalf("event %v", got)
	}
	if got := ev.next(t); got.Kind != grpcproc.ClaimRegained {
		t.Fatalf("event %v", got)
	}
}

// A lease that is not kept alive within three quarters of the TTL is lost
// before etcd ends it: a KeepOnLoss claim whose key outlived it moves to the
// next lease, with its revision.
func TestKeepAliveSilent(t *testing.T) {
	e := setup(t)
	e.c.ttl = 1
	e.lease.silent.Store(true)
	if _, err := e.c.Register(t.Context(), grpcproc.Member{Name: "a", Incarnation: 1, Addr: "a:1"}); err != nil {
		t.Fatal(err)
	}
	ev := make(events, 8)
	c := claimOK(t, e.c.Names(), "x", pid(1), grpcproc.ClaimOptions{KeepOnLoss: true}, ev)
	before := c.Revision()
	if got := ev.next(t); got.Kind != grpcproc.ClaimLost {
		t.Fatalf("event %v", got)
	}
	e.lease.silent.Store(false)
	if got := ev.next(t); got.Kind != grpcproc.ClaimRegained || got.Revision != before {
		t.Fatalf("event %v, revision %d", got, before)
	}
}
