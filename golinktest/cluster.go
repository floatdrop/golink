// Package golinktest runs a golink cluster inside one test binary. Nodes talk
// over in-memory gRPC connections (bufconn), so a multi-node scenario,
// including a partition or a node dying, needs no sockets and no registry.
package golinktest

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"

	"github.com/floatdrop/golink"
)

// Option configures a Cluster.
type Option func(*Cluster)

// WithHooks installs h on every node.
func WithHooks(h golink.Hooks) Option { return func(c *Cluster) { c.hooks = h } }

// WithLogger sets the logger every node uses. Default: discards.
func WithLogger(l *slog.Logger) Option { return func(c *Cluster) { c.logger = l } }

// Cluster is a set of nodes and the (simulated) network between them.
type Cluster struct {
	t      testing.TB
	hooks  golink.Hooks
	logger *slog.Logger

	mu    sync.Mutex
	nodes map[string]*member
	cut   map[[2]string]bool // {from, to}: dials refused
	incs  atomic.Uint64
}

type member struct {
	name string
	ln   *bufconn.Listener
	srv  *grpc.Server
	node *golink.Node
	dead bool
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
		c.mu.Unlock()
		for _, m := range members {
			c.stop(m, 2*time.Second)
		}
	})
	return c
}

// Node returns the running node called name.
func (c *Cluster) Node(name string) *golink.Node {
	c.t.Helper()
	c.mu.Lock()
	defer c.mu.Unlock()
	m := c.nodes[name]
	if m == nil || m.dead {
		c.t.Fatalf("golinktest: no running node %q", name)
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
	ma, mb := c.nodes[a], c.nodes[b]
	c.mu.Unlock()
	if ma != nil && !ma.dead {
		ma.node.Disconnect(b)
	}
	if mb != nil && !mb.dead {
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
// anyone; peers see their links break.
func (c *Cluster) Kill(name string) {
	c.t.Helper()
	c.mu.Lock()
	m := c.nodes[name]
	if m == nil || m.dead {
		c.mu.Unlock()
		c.t.Fatalf("golinktest: no running node %q", name)
	}
	m.dead = true
	c.mu.Unlock()
	for _, peer := range m.node.Peers() {
		m.node.Disconnect(peer)
	}
	c.stop(m, 100*time.Millisecond)
}

// Stop stops name gracefully: watchers of its processes get Down{shutdown}.
func (c *Cluster) Stop(name string) {
	c.t.Helper()
	c.mu.Lock()
	m := c.nodes[name]
	if m == nil || m.dead {
		c.mu.Unlock()
		c.t.Fatalf("golinktest: no running node %q", name)
	}
	m.dead = true
	c.mu.Unlock()
	c.stop(m, 2*time.Second)
}

// Restart starts a stopped or killed node again, with a new incarnation.
func (c *Cluster) Restart(name string) *golink.Node {
	c.t.Helper()
	c.mu.Lock()
	m := c.nodes[name]
	if m != nil && !m.dead {
		c.mu.Unlock()
		c.t.Fatalf("golinktest: node %q is running", name)
	}
	c.mu.Unlock()
	return c.start(name)
}

func (c *Cluster) start(name string) *golink.Node {
	c.t.Helper()
	ln := bufconn.Listen(1 << 20)
	srv := grpc.NewServer()
	node, err := golink.NewNode(golink.Config{
		Name:        name,
		Advertise:   name,
		Incarnation: c.incs.Add(1),
		Resolver:    golink.ResolverFunc(func(_ context.Context, node string) (string, error) { return "passthrough:///" + node, nil }),
		DialOptions: []grpc.DialOption{
			grpc.WithTransportCredentials(insecure.NewCredentials()),
			grpc.WithContextDialer(c.dialer(name)),
		},
		Logger:      c.logger,
		Hooks:       c.hooks,
		DialTimeout: 2 * time.Second,
	})
	if err != nil {
		c.t.Fatal(err)
	}
	node.Register(srv)
	go func() { _ = srv.Serve(ln) }()
	if err := node.Start(context.Background()); err != nil {
		c.t.Fatal(err)
	}
	m := &member{name: name, ln: ln, srv: srv, node: node}
	c.mu.Lock()
	c.nodes[name] = m
	c.mu.Unlock()
	return node
}

func (c *Cluster) stop(m *member, timeout time.Duration) {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	_ = m.node.Stop(ctx)
	m.srv.Stop()
	_ = m.ln.Close()
}

func (c *Cluster) dialer(from string) func(context.Context, string) (net.Conn, error) {
	return func(ctx context.Context, to string) (net.Conn, error) {
		c.mu.Lock()
		cut := c.cut[[2]string{from, to}]
		m := c.nodes[to]
		c.mu.Unlock()
		switch {
		case cut:
			return nil, fmt.Errorf("golinktest: %s -> %s is partitioned", from, to)
		case m == nil || m.dead:
			return nil, errors.New("golinktest: connection refused")
		}
		return m.ln.DialContext(ctx)
	}
}
