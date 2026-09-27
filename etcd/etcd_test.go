package grpcprocetcd_test

import (
	"context"
	"errors"
	"net"
	"net/url"
	"testing"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/server/v3/embed"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/proto"

	"github.com/floatdrop/grpcproc"
	grpcprocetcd "github.com/floatdrop/grpcproc/etcd"
	"github.com/floatdrop/grpcproc/internal/testpb"
)

func freeURL(t *testing.T) url.URL {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = ln.Close() }()
	return url.URL{Scheme: "http", Host: ln.Addr().String()}
}

// startEtcd runs a single-member etcd in the test process.
func startEtcd(t *testing.T) *clientv3.Client {
	t.Helper()
	cfg := embed.NewConfig()
	cfg.Dir = t.TempDir()
	cfg.LogLevel = "error"
	client, peer := freeURL(t), freeURL(t)
	cfg.ListenClientUrls, cfg.AdvertiseClientUrls = []url.URL{client}, []url.URL{client}
	cfg.ListenPeerUrls, cfg.AdvertisePeerUrls = []url.URL{peer}, []url.URL{peer}
	cfg.InitialCluster = cfg.Name + "=" + peer.String()
	e, err := embed.StartEtcd(cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(e.Close)
	select {
	case <-e.Server.ReadyNotify():
	case <-time.After(20 * time.Second):
		t.Fatal("etcd did not start")
	}
	cli, err := clientv3.New(clientv3.Config{Endpoints: []string{client.String()}, DialTimeout: 5 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = cli.Close() })
	return cli
}

func next(t *testing.T, ch <-chan grpcproc.MemberEvent) grpcproc.MemberEvent {
	t.Helper()
	select {
	case ev := <-ch:
		return ev
	case <-time.After(10 * time.Second):
		t.Fatal("no member event")
		return grpcproc.MemberEvent{}
	}
}

func TestRegisterResolveAndMembers(t *testing.T) {
	cli := startEtcd(t)
	c := grpcprocetcd.New(cli, "/test/", grpcprocetcd.WithTTL(5*time.Second))
	withdraw, err := c.Register(t.Context(), grpcproc.Member{Name: "a", Incarnation: 7, Addr: "10.0.0.1:9000"})
	if err != nil {
		t.Fatal(err)
	}
	if addr, err := c.Resolve(t.Context(), "a"); err != nil || addr != "10.0.0.1:9000" {
		t.Fatalf("%q %v", addr, err)
	}
	if _, err := c.Resolve(t.Context(), "nobody"); !errors.Is(err, grpcprocetcd.ErrNotRegistered) {
		t.Fatalf("got %v", err)
	}
	// A record that is not JSON, and one without an address.
	_, _ = cli.Put(t.Context(), "/test/nodes/garbage", "{not json")
	if _, err := c.Resolve(t.Context(), "garbage"); err == nil {
		t.Fatal("garbage resolved")
	}
	noAddr, err := c.Register(t.Context(), grpcproc.Member{Name: "b", Incarnation: 1})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := c.Resolve(t.Context(), "b"); err == nil {
		t.Fatal("resolved a node without an address")
	}
	members, err := c.Members(t.Context())
	if err != nil || len(members) != 2 || members[0].Name != "a" || members[0].Incarnation != 7 || members[1].Name != "b" {
		t.Fatalf("%+v %v", members, err) // garbage is skipped
	}
	// Withdrawing removes the key at once.
	if err := withdraw(t.Context()); err != nil {
		t.Fatal(err)
	}
	_ = noAddr(t.Context())
	if _, err := c.Resolve(t.Context(), "a"); !errors.Is(err, grpcprocetcd.ErrNotRegistered) {
		t.Fatalf("still registered: %v", err)
	}
	// Withdrawing twice revokes a lease that is gone.
	if err := withdraw(t.Context()); err == nil {
		t.Fatal("second withdraw succeeded")
	}
}

func TestWatch(t *testing.T) {
	cli := startEtcd(t)
	c := grpcprocetcd.New(cli, "/w")
	wa, _ := c.Register(t.Context(), grpcproc.Member{Name: "a", Incarnation: 1, Addr: "a:1"})
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	events, err := c.Watch(ctx)
	if err != nil {
		t.Fatal(err)
	}
	// The snapshot first.
	if ev := next(t, events); !ev.Up || ev.Member != (grpcproc.Member{Name: "a", Incarnation: 1, Addr: "a:1"}) {
		t.Fatalf("%+v", ev)
	}
	wb, _ := c.Register(t.Context(), grpcproc.Member{Name: "b", Incarnation: 1, Addr: "b:1"})
	if ev := next(t, events); !ev.Up || ev.Member.Name != "b" {
		t.Fatalf("%+v", ev)
	}
	// b stops: a down with its incarnation.
	_ = wb(t.Context())
	if ev := next(t, events); ev.Up || ev.Member != (grpcproc.Member{Name: "b", Incarnation: 1, Addr: "b:1"}) {
		t.Fatalf("%+v", ev)
	}
	// a restarts before its old lease expired: the new incarnation replaces it.
	wa2, _ := c.Register(t.Context(), grpcproc.Member{Name: "a", Incarnation: 2, Addr: "a:2"})
	if ev := next(t, events); !ev.Up || ev.Member.Incarnation != 2 {
		t.Fatalf("%+v", ev)
	}
	_ = wa
	// Garbage is skipped; deleting it is a down for whichever incarnation.
	_, _ = cli.Put(t.Context(), "/w/nodes/junk", "{")
	_, _ = cli.Delete(t.Context(), "/w/nodes/junk")
	if ev := next(t, events); ev.Up || ev.Member != (grpcproc.Member{Name: "junk"}) {
		t.Fatalf("%+v", ev)
	}
	_ = wa2(t.Context())
	next(t, events)
	cancel()
	for range events {
	}
}

func TestLostLeaseRegistersAgain(t *testing.T) {
	cli := startEtcd(t)
	c := grpcprocetcd.New(cli, "/l", grpcprocetcd.WithRetry(10*time.Millisecond))
	withdraw, err := c.Register(t.Context(), grpcproc.Member{Name: "a", Incarnation: 1, Addr: "a:1"})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = withdraw(t.Context()) }()
	resp, _ := cli.Get(t.Context(), "/l/nodes/a")
	first := clientv3.LeaseID(resp.Kvs[0].Lease)
	// As if etcd had lost the lease: the key goes, then comes back.
	if _, err := cli.Revoke(t.Context(), first); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(10 * time.Second)
	for {
		resp, _ := cli.Get(t.Context(), "/l/nodes/a")
		if len(resp.Kvs) == 1 && clientv3.LeaseID(resp.Kvs[0].Lease) != first {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("not registered again")
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// node starts a grpcproc node over real TCP, registered in etcd.
func node(t *testing.T, c *grpcprocetcd.Cluster, name string) *grpcproc.Node {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	srv := grpc.NewServer()
	n, err := grpcproc.NewNode(grpcproc.Config{
		Name:        name,
		Advertise:   ln.Addr().String(),
		Resolver:    c,
		Registrar:   c,
		Membership:  c,
		DialOptions: []grpc.DialOption{grpc.WithTransportCredentials(insecure.NewCredentials())},
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

func TestNodesFindAndLoseEachOther(t *testing.T) {
	cli := startEtcd(t)
	c := grpcprocetcd.New(cli, "/grpcproc/test", grpcprocetcd.WithRetry(time.Hour)) // no re-registering in this test
	a, b := node(t, c, "a"), node(t, c, "b")

	// b's process is found through etcd; a monitors it.
	target, _ := b.Spawn[*testpb.Ping](func(p *grpcproc.Process[*testpb.Ping]) error {
		for {
			m, err := p.Receive()
			if err != nil {
				return err
			}
			if m.IsCall() {
				_ = p.Reply(m, &testpb.Pong{N: m.Body.GetN() + 1}, nil)
			}
		}
	}, grpcproc.WithName("echo"))
	downs := make(chan grpcproc.Down, 1)
	ready := make(chan struct{})
	_, _ = a.Spawn[proto.Message](func(p *grpcproc.Process[proto.Message]) error {
		p.Monitor(target)
		close(ready)
		for {
			m, err := p.Receive()
			if err != nil {
				return err
			}
			if m.Down != nil {
				downs <- *m.Down
			}
		}
	})
	<-ready
	if r, err := a.Call[*testpb.Pong](t.Context(), grpcproc.Named[*testpb.Ping]("b", "echo"), &testpb.Ping{N: 1}); err != nil || r.GetN() != 2 {
		t.Fatalf("%v %v", r, err)
	}

	// b goes silent: its lease ends while its TCP connection stays up.
	// Only etcd can tell a, and it does.
	resp, _ := cli.Get(t.Context(), "/grpcproc/test/nodes/b")
	if _, err := cli.Revoke(t.Context(), clientv3.LeaseID(resp.Kvs[0].Lease)); err != nil {
		t.Fatal(err)
	}
	select {
	case d := <-downs:
		if d.PID != target.PID() || d.Reason != grpcproc.ReasonNoConnection {
			t.Fatalf("%+v", d)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("a never learned that b left")
	}
}
