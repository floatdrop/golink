package grpcprocetcd

import (
	"context"
	"errors"
	"log/slog"
	"net"
	"net/url"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/server/v3/embed"

	"github.com/floatdrop/grpcproc"
)

// These tests reach the paths a healthy etcd never takes, through fakes
// wrapped around a real client.

var errInjected = errors.New("injected")

type faultKV struct {
	clientv3.KV
	failGets atomic.Int32 // fail this many Gets
	putErr   error
}

func (f *faultKV) Get(ctx context.Context, key string, opts ...clientv3.OpOption) (*clientv3.GetResponse, error) {
	if f.failGets.Add(-1) >= 0 {
		return nil, errInjected
	}
	return f.KV.Get(ctx, key, opts...)
}

func (f *faultKV) Put(ctx context.Context, key, val string, opts ...clientv3.OpOption) (*clientv3.PutResponse, error) {
	if f.putErr != nil {
		return nil, f.putErr
	}
	return f.KV.Put(ctx, key, val, opts...)
}

type faultLease struct {
	clientv3.Lease
	failGrants atomic.Int32 // fail this many Grants
	failKeeps  atomic.Int32 // fail this many KeepAlives
	revoked    atomic.Int32
}

func (f *faultLease) Grant(ctx context.Context, ttl int64) (*clientv3.LeaseGrantResponse, error) {
	if f.failGrants.Add(-1) >= 0 {
		return nil, errInjected
	}
	return f.Lease.Grant(ctx, ttl)
}

func (f *faultLease) KeepAlive(ctx context.Context, id clientv3.LeaseID) (<-chan *clientv3.LeaseKeepAliveResponse, error) {
	if f.failKeeps.Add(-1) >= 0 {
		return nil, errInjected
	}
	return f.Lease.KeepAlive(ctx, id)
}

func (f *faultLease) Revoke(ctx context.Context, id clientv3.LeaseID) (*clientv3.LeaseRevokeResponse, error) {
	f.revoked.Add(1)
	return f.Lease.Revoke(ctx, id)
}

// scriptedWatcher hands out a channel the test controls for the first
// Watch, then the real watcher.
type scriptedWatcher struct {
	clientv3.Watcher
	mu    sync.Mutex
	first chan clientv3.WatchResponse
}

func (s *scriptedWatcher) Watch(ctx context.Context, key string, opts ...clientv3.OpOption) clientv3.WatchChan {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.first != nil {
		ch := s.first
		s.first = nil
		return ch
	}
	return s.Watcher.Watch(ctx, key, opts...)
}

func freeURL(t *testing.T) url.URL {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = ln.Close() }()
	return url.URL{Scheme: "http", Host: ln.Addr().String()}
}

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
	<-e.Server.ReadyNotify()
	cli, err := clientv3.New(clientv3.Config{Endpoints: []string{client.String()}, DialTimeout: 5 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = cli.Close() })
	return cli
}

type env struct {
	cli     *clientv3.Client
	kv      *faultKV
	lease   *faultLease
	watcher *scriptedWatcher
	c       *Cluster
}

func setup(t *testing.T) env {
	cli := startEtcd(t)
	e := env{cli: cli, kv: &faultKV{KV: cli}, lease: &faultLease{Lease: cli}, watcher: &scriptedWatcher{Watcher: cli}}
	e.c = newCluster(e.kv, e.lease, e.watcher, "/x", WithRetry(5*time.Millisecond), WithLogger(slog.New(slog.DiscardHandler)))
	return e
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

func TestEtcdErrorsSurface(t *testing.T) {
	e := setup(t)
	e.kv.failGets.Store(3)
	if _, err := e.c.Resolve(t.Context(), "a"); !errors.Is(err, errInjected) {
		t.Fatalf("Resolve: %v", err)
	}
	if _, err := e.c.Members(t.Context()); !errors.Is(err, errInjected) {
		t.Fatalf("Members: %v", err)
	}
	if _, err := e.c.Watch(t.Context()); !errors.Is(err, errInjected) {
		t.Fatalf("Watch: %v", err)
	}
	e.lease.failGrants.Store(1)
	if _, err := e.c.Register(t.Context(), grpcproc.Member{Name: "a"}); !errors.Is(err, errInjected) {
		t.Fatalf("Register (grant): %v", err)
	}
	e.kv.putErr = errInjected
	if _, err := e.c.Register(t.Context(), grpcproc.Member{Name: "a"}); !errors.Is(err, errInjected) {
		t.Fatalf("Register (put): %v", err)
	}
	if e.lease.revoked.Load() != 1 {
		t.Fatal("the lease of a failed put was not revoked")
	}
}

func TestKeepRegistersAgainThroughFailures(t *testing.T) {
	e := setup(t)
	// KeepAlive fails at once, and so does the first registration after it.
	e.lease.failKeeps.Store(1)
	e.lease.failGrants.Store(0)
	withdraw, err := e.c.Register(t.Context(), grpcproc.Member{Name: "a", Incarnation: 1, Addr: "a:1"})
	if err != nil {
		t.Fatal(err)
	}
	e.lease.failGrants.Store(1)
	resp, _ := e.cli.Get(t.Context(), "/x/nodes/a")
	first := resp.Kvs[0].Lease
	deadline := time.Now().Add(10 * time.Second)
	for {
		resp, _ := e.cli.Get(t.Context(), "/x/nodes/a")
		if len(resp.Kvs) == 1 && resp.Kvs[0].Lease != first {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("never registered again")
		}
		time.Sleep(5 * time.Millisecond)
	}
	if err := withdraw(t.Context()); err != nil {
		t.Fatal(err)
	}
	// Withdrawn while waiting to register again: it stops waiting.
	e.lease.failKeeps.Store(1)
	e.c.retry = time.Hour
	withdraw, err = e.c.Register(t.Context(), grpcproc.Member{Name: "b", Incarnation: 1, Addr: "b:1"})
	if err != nil {
		t.Fatal(err)
	}
	time.Sleep(20 * time.Millisecond)
	if err := withdraw(t.Context()); err != nil {
		t.Fatal(err)
	}
	// Withdrawn while registering again keeps failing.
	e.c.retry = 5 * time.Millisecond
	e.lease.failKeeps.Store(1)
	withdraw, err = e.c.Register(t.Context(), grpcproc.Member{Name: "c", Incarnation: 1, Addr: "c:1"})
	if err != nil {
		t.Fatal(err)
	}
	e.lease.failGrants.Store(1 << 20)
	time.Sleep(30 * time.Millisecond)
	_ = withdraw(t.Context())
	e.lease.failGrants.Store(0)
}

func TestWatchRecoversFromABrokenWatch(t *testing.T) {
	e := setup(t)
	wa, _ := e.c.Register(t.Context(), grpcproc.Member{Name: "a", Incarnation: 1, Addr: "a:1"})
	wb, _ := e.c.Register(t.Context(), grpcproc.Member{Name: "b", Incarnation: 1, Addr: "b:1"})
	first := make(chan clientv3.WatchResponse)
	e.watcher.first = first
	events, err := e.c.Watch(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	next(t, events)
	next(t, events)
	// While the watch sees nothing: b leaves, a restarts, c joins.
	_ = wb(t.Context())
	_, _ = e.c.Register(t.Context(), grpcproc.Member{Name: "a", Incarnation: 2, Addr: "a:2"})
	_, _ = e.c.Register(t.Context(), grpcproc.Member{Name: "c", Incarnation: 1, Addr: "c:1"})
	_ = wa
	// The watch breaks; the first attempt to list again fails too.
	e.kv.failGets.Store(1)
	first <- clientv3.WatchResponse{CompactRevision: 1}
	want := []grpcproc.MemberEvent{
		{Member: grpcproc.Member{Name: "a", Incarnation: 1, Addr: "a:1"}},
		{Member: grpcproc.Member{Name: "b", Incarnation: 1, Addr: "b:1"}},
		{Member: grpcproc.Member{Name: "a", Incarnation: 2, Addr: "a:2"}, Up: true},
		{Member: grpcproc.Member{Name: "c", Incarnation: 1, Addr: "c:1"}, Up: true},
	}
	for _, w := range want {
		if ev := next(t, events); ev != w {
			t.Fatalf("got %+v, want %+v", ev, w)
		}
	}
	// And it follows the real watch from there.
	_, _ = e.c.Register(t.Context(), grpcproc.Member{Name: "d", Incarnation: 1, Addr: "d:1"})
	if ev := next(t, events); !ev.Up || ev.Member.Name != "d" {
		t.Fatalf("%+v", ev)
	}
}

// Every place follow emits returns when the watcher's context ends while
// nobody reads.
func TestWatchStopsWhenNobodyReads(t *testing.T) {
	stages := []struct {
		name string
		run  func(t *testing.T, e env, ctx context.Context) <-chan grpcproc.MemberEvent
	}{
		{"snapshot", func(t *testing.T, e env, ctx context.Context) <-chan grpcproc.MemberEvent {
			_, _ = e.c.Register(t.Context(), grpcproc.Member{Name: "a", Incarnation: 1})
			ev, _ := e.c.Watch(ctx)
			return ev
		}},
		{"put", func(t *testing.T, e env, ctx context.Context) <-chan grpcproc.MemberEvent {
			ev, _ := e.c.Watch(ctx)
			_, _ = e.c.Register(t.Context(), grpcproc.Member{Name: "a", Incarnation: 1})
			return ev
		}},
		{"delete", func(t *testing.T, e env, ctx context.Context) <-chan grpcproc.MemberEvent {
			w, _ := e.c.Register(t.Context(), grpcproc.Member{Name: "a", Incarnation: 1})
			ev, _ := e.c.Watch(ctx)
			next(t, ev)
			_ = w(t.Context())
			return ev
		}},
		{"relist down", func(t *testing.T, e env, ctx context.Context) <-chan grpcproc.MemberEvent {
			w, _ := e.c.Register(t.Context(), grpcproc.Member{Name: "a", Incarnation: 1})
			first := make(chan clientv3.WatchResponse, 1)
			e.watcher.first = first
			ev, _ := e.c.Watch(ctx)
			next(t, ev)
			_ = w(t.Context())
			first <- clientv3.WatchResponse{CompactRevision: 1}
			return ev
		}},
		{"relist up", func(t *testing.T, e env, ctx context.Context) <-chan grpcproc.MemberEvent {
			first := make(chan clientv3.WatchResponse, 1)
			e.watcher.first = first
			ev, _ := e.c.Watch(ctx)
			_, _ = e.c.Register(t.Context(), grpcproc.Member{Name: "a", Incarnation: 1})
			first <- clientv3.WatchResponse{CompactRevision: 1}
			return ev
		}},
		{"waiting to list again", func(t *testing.T, e env, ctx context.Context) <-chan grpcproc.MemberEvent {
			e.c.retry = time.Hour
			first := make(chan clientv3.WatchResponse, 1)
			e.watcher.first = first
			ev, _ := e.c.Watch(ctx)
			first <- clientv3.WatchResponse{CompactRevision: 1}
			return ev
		}},
	}
	for _, st := range stages {
		t.Run(st.name, func(t *testing.T) {
			e := setup(t)
			ctx, cancel := context.WithCancel(t.Context())
			events := st.run(t, e, ctx)
			time.Sleep(100 * time.Millisecond) // let follow block where the stage leads it
			cancel()
			select {
			case <-drained(events):
			case <-time.After(5 * time.Second):
				t.Fatal("follow did not stop")
			}
		})
	}
}

func drained(ch <-chan grpcproc.MemberEvent) <-chan struct{} {
	done := make(chan struct{})
	go func() {
		for range ch {
		}
		close(done)
	}()
	return done
}
