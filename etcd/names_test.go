package grpcprocetcd_test

import (
	"context"
	"errors"
	"log/slog"
	"net"
	"testing"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/floatdrop/grpcproc"
	grpcprocetcd "github.com/floatdrop/grpcproc/etcd"
	"github.com/floatdrop/grpcproc/internal/testpb"
)

type claimed struct {
	c   *grpcproc.Claim
	err error
}

// claimer spawns on n a process that claims name with opts and then answers
// calls; it ends on Ping{N: 0}. Its claim, or the claim's error, comes on
// the channel.
func claimer(t *testing.T, n *grpcproc.Node, name string, opts ...grpcproc.ClaimOption) (grpcproc.Addr[*testpb.Ping], <-chan claimed) {
	t.Helper()
	ch := make(chan claimed, 1)
	addr, err := n.Spawn(func(p *grpcproc.Process[*testpb.Ping]) error {
		c, err := p.Claim(p.Context(), name, opts...)
		ch <- claimed{c, err}
		if err != nil {
			return err
		}
		for {
			m, err := p.Receive()
			if err != nil {
				return err
			}
			if m.Down != nil {
				continue
			}
			if m.Body.GetN() == 0 {
				return nil
			}
			_ = m.Reply(&testpb.Pong{N: m.Body.GetN() + 1}, nil)
		}
	})
	if err != nil {
		t.Fatal(err)
	}
	return addr, ch
}

func wait[T any](t *testing.T, ch <-chan T, what string) T {
	t.Helper()
	select {
	case v := <-ch:
		return v
	case <-time.After(15 * time.Second):
		t.Fatalf("no %s", what)
		panic("unreachable")
	}
}

func claimOf(t *testing.T, ch <-chan claimed) *grpcproc.Claim {
	t.Helper()
	got := wait(t, ch, "claim")
	if got.err != nil {
		t.Fatalf("claim: %v", got.err)
	}
	return got.c
}

func eventually(t *testing.T, what string, cond func() bool) {
	t.Helper()
	for deadline := time.Now().Add(15 * time.Second); !cond(); time.Sleep(10 * time.Millisecond) {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
	}
}

// namesNode is a node whose global names are c's.
func namesNode(t *testing.T, c *grpcprocetcd.Cluster, name string) *grpcproc.Node {
	t.Helper()
	ln := listen(t)
	srv := grpc.NewServer()
	n, err := grpcproc.NewNode(grpcproc.Config{
		Name: name, Advertise: ln.Addr().String(),
		Resolver: c, Registrar: c, Membership: c, Names: c.Names(),
		DialOptions: []grpc.DialOption{grpc.WithTransportCredentials(insecure.NewCredentials())},
		Logger:      slog.New(slog.DiscardHandler),
	})
	if err != nil {
		t.Fatal(err)
	}
	n.Register(srv)
	go func() { _ = srv.Serve(ln) }()
	if err := n.Start(t.Context()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = n.Stop(ctx)
		srv.Stop()
	})
	return n
}

// cluster is one process's view of etcd: two of them are two processes.
func cluster(cli *clientv3.Client, retry time.Duration) *grpcprocetcd.Cluster {
	return grpcprocetcd.New(cli, "/names-test", grpcprocetcd.WithTTL(2*time.Second), grpcprocetcd.WithRetry(retry), grpcprocetcd.WithLogger(slog.New(slog.DiscardHandler)))
}

// revoke ends node's lease in etcd, as etcd would when it stopped hearing
// from the node.
func revoke(t *testing.T, cli *clientv3.Client, node string) {
	t.Helper()
	resp, err := cli.Get(t.Context(), "/names-test/nodes/"+node)
	if err != nil || len(resp.Kvs) == 0 {
		t.Fatalf("%s's record: %v", node, err)
	}
	if _, err := cli.Revoke(t.Context(), clientv3.LeaseID(resp.Kvs[0].Lease)); err != nil {
		t.Fatal(err)
	}
}

// Global names on etcd, end to end: two processes, each a node, each with
// its own view of etcd.
func TestGlobalNames(t *testing.T) {
	cli := startEtcd(t)
	ca, cb := cluster(cli, 20*time.Millisecond), cluster(cli, 20*time.Millisecond)
	a, b := namesNode(t, ca, "a"), namesNode(t, cb, "b")

	ledger, ch := claimer(t, b, "ledger")
	held := claimOf(t, ch)
	room, ch := claimer(t, b, "room:1", grpcproc.KeepOnLoss())
	kept := claimOf(t, ch)

	// a finds both through its copy, and through etcd itself.
	eventually(t, "a's copy", func() bool { _, ok := ca.Names().Lookup("room:1"); return ok })
	g := grpcproc.AddrOf[*testpb.Ping](grpcproc.Global{Name: "ledger"})
	if r, err := g.Call[*testpb.Pong](t.Context(), a, &testpb.Ping{N: 1}); err != nil || r.GetN() != 2 {
		t.Fatalf("call: %v %v", r, err)
	}
	if pid, ok, err := ca.Names().Resolve(t.Context(), "ledger"); err != nil || !ok || pid != ledger.PID() {
		t.Fatalf("resolve: %v %v %v", pid, ok, err)
	}
	if list := ca.Names().List("room:", 0); len(list) != 1 || list[0].PID != room.PID() {
		t.Fatalf("list: %v", list)
	}

	// A held name is taken; a standby waits for it.
	_, ch = claimer(t, a, "ledger")
	var taken *grpcproc.TakenError
	if got := wait(t, ch, "a's claim"); !errors.As(got.err, &taken) || taken.Holder != ledger.PID() {
		t.Fatalf("a's claim: %v", got.err)
	}
	standby, ch := claimer(t, a, "ledger", grpcproc.WaitForName())
	if err := ledger.Send(t.Context(), b, &testpb.Ping{N: 0}); err != nil { // ends it
		t.Fatal(err)
	}
	next := claimOf(t, ch)
	if next.Revision() <= held.Revision() {
		t.Fatalf("revision %d after %d", next.Revision(), held.Revision())
	}
	eventually(t, "the standby to hold ledger", func() bool { pid, _ := cb.Names().Lookup("ledger"); return pid == standby.PID() })

	// b's lease ends: its strict holder would have ended; room:1 runs on,
	// and holds its name again once b has registered again.
	before := kept.Revision()
	revoke(t, cli, "b")
	eventually(t, "room:1 held again", func() bool { return kept.Held() && kept.Revision() > before })
	if pid, ok, _ := cb.Names().Resolve(t.Context(), "room:1"); !ok || pid != room.PID() {
		t.Fatalf("room:1 is %v %v", pid, ok)
	}

	// A release frees the name, and a lost claim's release does nothing.
	if err := next.Release(t.Context()); err != nil {
		t.Fatal(err)
	}
	if _, ok, _ := ca.Names().Resolve(t.Context(), "ledger"); ok {
		t.Fatal("ledger still held")
	}
}

// A strict claim ends its holder when its node's lease is lost, and a
// KeepOnLoss claim whose name another process took meanwhile ends in
// conflict.
func TestNamesLost(t *testing.T) {
	cli := startEtcd(t)
	ca, cb := cluster(cli, 20*time.Millisecond), cluster(cli, time.Second)
	a, b := namesNode(t, ca, "a"), namesNode(t, cb, "b")
	events := b.Subscribe(t.Context(), 256)
	strict, ch := claimer(t, b, "ledger")
	claimOf(t, ch)
	room, ch := claimer(t, b, "room:1", grpcproc.KeepOnLoss())
	claimOf(t, ch)

	// b's lease ends, and b waits a second before it registers again:
	// room:1 is free meanwhile, and a starts it.
	revoke(t, cli, "b")
	eventually(t, "room:1 free", func() bool { _, ok, _ := ca.Names().Resolve(t.Context(), "room:1"); return !ok })
	_, ch = claimer(t, a, "room:1")
	claimOf(t, ch)

	want := map[grpcproc.PID]string{strict.PID(): grpcproc.ReasonNameLost, room.PID(): grpcproc.ReasonNameConflict}
	for len(want) > 0 {
		ev := wait(t, events, "exit")
		if r, ok := want[ev.Process.PID]; ok && ev.Kind == grpcproc.EventExit {
			if ev.Reason != r {
				t.Fatalf("%v ended with %q, want %q", ev.Process.PID, ev.Reason, r)
			}
			delete(want, ev.Process.PID)
		}
	}
}

func listen(t *testing.T) net.Listener {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	return ln
}
