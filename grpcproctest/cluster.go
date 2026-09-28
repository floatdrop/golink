// Package grpcproctest runs a grpcproc cluster inside one test binary. Nodes talk
// over in-memory gRPC connections (bufconn), so a multi-node scenario,
// including a partition or a node dying, needs no sockets and no registry.
//
// Its nodes dial a peer again at once after a dial to it failed
// (Config.DialBackoff is negative), so the send right after Heal or Restart
// reaches the peer. Set DialBackoff with WithConfig to test the backoff.
package grpcproctest

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"maps"
	"net"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"

	"github.com/floatdrop/grpcproc"
)

// Option configures a Cluster.
type Option func(*Cluster)

// WithHooks installs h on every node.
func WithHooks(h grpcproc.Hooks) Option { return func(c *Cluster) { c.hooks = h } }

// WithLogger sets the logger every node uses. Default: discards.
func WithLogger(l *slog.Logger) Option { return func(c *Cluster) { c.logger = l } }

// WithConfig adjusts each node's Config before the node is created, each
// time it starts (including after Restart): a Registrar, a Membership, hooks
// for one node only. It runs after WithHooks and WithLogger have set theirs,
// so what it sets wins, whatever the order of the options.
func WithConfig(fn func(name string, cfg *grpcproc.Config)) Option {
	return func(c *Cluster) { c.configure = append(c.configure, fn) }
}

// WithServices registers extra gRPC services on every node's server, such as
// an Inspector, each time the node starts (including after Restart).
func WithServices(fn func(n *grpcproc.Node, s *grpc.Server)) Option {
	return func(c *Cluster) { c.services = append(c.services, fn) }
}

// Cluster is a set of nodes and the (simulated) network between them.
type Cluster struct {
	t         testing.TB
	hooks     grpcproc.Hooks
	logger    *slog.Logger
	services  []func(*grpcproc.Node, *grpc.Server)
	configure []func(string, *grpcproc.Config)

	mu    sync.Mutex
	nodes map[string]*member
	cut   map[[2]string]bool // {from, to}: dials refused
	conns map[string]*grpc.ClientConn
	incs  atomic.Uint64
}

type member struct {
	name   string
	ln     *bufconn.Listener
	srv    *grpc.Server
	node   *grpcproc.Node
	dead   bool
	killed bool       // it dials no one: a crashed node sends nothing
	conns  []net.Conn // what it dialed, for Kill to close; guarded by Cluster.mu
}

// New starts one node per name and stops them all when the test ends.
func New(t testing.TB, names ...string) *Cluster {
	return NewWith(t, nil, names...)
}

// NewWith is New with options.
func NewWith(t testing.TB, opts []Option, names ...string) *Cluster {
	t.Helper()
	c := &Cluster{
		t:      t,
		logger: slog.New(slog.DiscardHandler),
		nodes:  map[string]*member{},
		cut:    map[[2]string]bool{},
		conns:  map[string]*grpc.ClientConn{},
	}
	for _, o := range opts {
		o(c)
	}
	for _, name := range names {
		c.start(name)
	}
	t.Cleanup(func() {
		c.mu.Lock()
		members := make([]*member, 0, len(c.nodes))
		for _, m := range c.nodes {
			members = append(members, m)
		}
		conns := slices.Collect(maps.Values(c.conns))
		c.mu.Unlock()
		for _, cc := range conns {
			_ = cc.Close()
		}
		for _, m := range members {
			stopMember(m, 2*time.Second)
		}
	})
	return c
}

// Resolver resolves node names of this cluster, for components that dial
// nodes themselves (an Inspector built with inspect.WithResolver). Use it
// with DialOptions.
func (*Cluster) Resolver() grpcproc.Resolver {
	return grpcproc.ResolverFunc(func(_ context.Context, node string) (string, error) { return "passthrough:///" + node, nil })
}

// DialOptions reach the cluster's in-memory servers from outside any node:
// not subject to Partition, and following a node across Restart.
func (c *Cluster) DialOptions() []grpc.DialOption {
	return []grpc.DialOption{
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithContextDialer(c.dialer("", nil)),
	}
}

// Conn returns a client connection to the gRPC server of node name, for
// calling services registered with WithServices. It is not subject to
// Partition, and it follows the node across Restart.
func (c *Cluster) Conn(name string) *grpc.ClientConn {
	c.t.Helper()
	c.mu.Lock()
	defer c.mu.Unlock()
	if cc := c.conns[name]; cc != nil {
		return cc
	}
	cc, err := grpc.NewClient("passthrough:///"+name, c.DialOptions()...)
	if err != nil {
		c.t.Fatal(err)
	}
	c.conns[name] = cc
	return cc
}

// Node returns the running node called name.
func (c *Cluster) Node(name string) *grpcproc.Node {
	c.t.Helper()
	c.mu.Lock()
	defer c.mu.Unlock()
	m := c.nodes[name]
	if m == nil || m.dead {
		c.t.Fatalf("grpcproctest: no running node %q", name)
	}
	return m.node
}

// Partition cuts the network between a and b in both directions: existing
// links break (monitors fire Down{noconnection}) and new dials are refused
// until Heal.
func (c *Cluster) Partition(a, b string) {
	c.mu.Lock()
	c.cut[[2]string{a, b}] = true
	c.cut[[2]string{b, a}] = true
	// Whether each runs is read under the lock that Stop and Kill write it
	// under.
	ma, mb := c.nodes[a], c.nodes[b]
	upA, upB := ma != nil && !ma.dead, mb != nil && !mb.dead
	c.mu.Unlock()
	if upA {
		ma.node.Disconnect(b)
	}
	if upB {
		mb.node.Disconnect(a)
	}
}

// Heal lets a and b reach each other again.
func (c *Cluster) Heal(a, b string) {
	c.mu.Lock()
	delete(c.cut, [2]string{a, b})
	delete(c.cut, [2]string{b, a})
	c.mu.Unlock()
}

// Kill stops name abruptly, as a crash would: no Down{shutdown} reaches
// anyone; peers see their links break. They have by the time Kill returns,
// monitors fired and calls failed, so the next send to name fails, and after
// Restart reaches the new node (dials are not backed off; see the package
// doc).
func (c *Cluster) Kill(name string) {
	c.t.Helper()
	c.mu.Lock()
	m := c.nodes[name]
	if m == nil || m.dead {
		c.mu.Unlock()
		c.t.Fatalf("grpcproctest: no running node %q", name)
	}
	m.dead, m.killed = true, true
	conns := m.conns
	m.conns = nil
	var peers []*grpcproc.Node
	for _, other := range c.nodes {
		if !other.dead {
			peers = append(peers, other.node)
		}
	}
	c.mu.Unlock()

	// A peer linked to name reports one link-down once it has torn the
	// links down, whoever notices first: its own side, or Disconnect below.
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	var linked []<-chan grpcproc.Event
	for _, peer := range peers {
		events := peer.Subscribe(ctx, 1024)
		if slices.Contains(peer.Peers(), name) {
			linked = append(linked, events)
		}
	}

	// Its connections go first, including one a dial just made that its
	// node has not yet linked over.
	for _, conn := range conns {
		_ = conn.Close()
	}
	for _, peer := range m.node.Peers() {
		m.node.Disconnect(peer)
	}
	stopMember(m, 100*time.Millisecond)
	for _, peer := range peers {
		peer.Disconnect(name)
	}
	for _, events := range linked {
		for e := range events {
			if e.Kind == grpcproc.EventLinkDown && e.Peer.Name == name {
				break
			}
		}
	}
}

// Stop stops name gracefully: watchers of its processes get Down{shutdown},
// or Down{noconnection} if a Partition breaks their link meanwhile.
func (c *Cluster) Stop(name string) {
	c.t.Helper()
	c.mu.Lock()
	m := c.nodes[name]
	if m == nil || m.dead {
		c.mu.Unlock()
		c.t.Fatalf("grpcproctest: no running node %q", name)
	}
	m.dead = true
	c.mu.Unlock()
	stopMember(m, 2*time.Second)
}

// Restart starts a stopped or killed node again, with a new incarnation.
func (c *Cluster) Restart(name string) *grpcproc.Node {
	c.t.Helper()
	c.mu.Lock()
	m := c.nodes[name]
	if m != nil && !m.dead {
		c.mu.Unlock()
		c.t.Fatalf("grpcproctest: node %q is running", name)
	}
	c.mu.Unlock()
	return c.start(name)
}

func (c *Cluster) start(name string) *grpcproc.Node {
	c.t.Helper()
	ln := bufconn.Listen(1 << 20)
	srv := grpc.NewServer()
	// The member exists before its node, so the node's dialer is bound to
	// this start of it, not to whatever runs under its name later.
	m := &member{name: name, ln: ln, srv: srv}
	cfg := grpcproc.Config{
		Name:        name,
		Advertise:   name,
		Incarnation: c.incs.Add(1),
		Resolver:    c.Resolver(),
		DialOptions: []grpc.DialOption{
			grpc.WithTransportCredentials(insecure.NewCredentials()),
			grpc.WithContextDialer(c.dialer(name, m)),
		},
		Logger:      c.logger,
		Hooks:       c.hooks,
		DialTimeout: 2 * time.Second,
		DialBackoff: -1,
	}
	for _, fn := range c.configure {
		fn(name, &cfg)
	}
	node, err := grpcproc.NewNode(cfg)
	if err != nil {
		c.t.Fatal(err)
	}
	node.Register(srv)
	for _, register := range c.services {
		register(node, srv)
	}
	go func() { _ = srv.Serve(ln) }()
	m.node = node
	if err := node.Start(context.Background()); err != nil {
		stopMember(m, 2*time.Second) // it is in no list Cleanup stops
		c.t.Fatal(err)
	}
	c.mu.Lock()
	if cur := c.nodes[name]; cur != nil && !cur.dead {
		// A Restart that ran alongside this one got there first.
		c.mu.Unlock()
		stopMember(m, 2*time.Second)
		c.t.Fatalf("grpcproctest: node %q is running", name)
	}
	c.nodes[name] = m
	c.mu.Unlock()
	return node
}

func stopMember(m *member, timeout time.Duration) {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	_ = m.node.Stop(ctx)
	m.srv.Stop()
	_ = m.ln.Close()
}

// dialer connects src, the node called from (or, with from empty, a client
// outside the cluster), to the node a dial names.
func (c *Cluster) dialer(from string, src *member) func(context.Context, string) (net.Conn, error) {
	return func(ctx context.Context, to string) (net.Conn, error) {
		c.mu.Lock()
		cut := c.cut[[2]string{from, to}]
		m := c.nodes[to]
		killed := src != nil && src.killed
		down := m == nil || m.dead
		c.mu.Unlock()
		switch {
		case killed:
			// Its processes exit as it stops, and would tell their callers
			// and watchers so; a crash tells no one.
			return nil, fmt.Errorf("grpcproctest: %s was killed", from)
		case cut:
			return nil, fmt.Errorf("grpcproctest: %s -> %s is partitioned", from, to)
		case down:
			return nil, errors.New("grpcproctest: connection refused")
		}
		conn, err := m.ln.DialContext(ctx)
		if err != nil || src == nil {
			return conn, err
		}
		c.mu.Lock()
		defer c.mu.Unlock()
		if src.killed { // killed while this dial connected
			_ = conn.Close()
			return nil, fmt.Errorf("grpcproctest: %s was killed", from)
		}
		src.conns = append(src.conns, conn)
		return conn, nil
	}
}
