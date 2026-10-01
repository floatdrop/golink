package grpcproctest

import (
	"cmp"
	"context"
	"errors"
	"maps"
	"slices"
	"strings"
	"sync"

	"github.com/floatdrop/grpcproc"
)

// ErrNamesUnreachable is what a node cut off from the names (Cluster.CutNames)
// gets from a claim or a Resolve.
var ErrNamesUnreachable = errors.New("grpcproctest: names out of reach")

// Names is an in-memory store of global names, shared by a Cluster's nodes:
// each node's Config.Names is a view of it (For). It has no lag: a node's
// Lookup reads the store itself. A node can be cut off from it, as from etcd
// (Cluster.CutNames): its claims are lost, the names free for the others,
// and its claims and Resolves fail until it is restored (RestoreNames).
type Names struct {
	mu      sync.Mutex
	held    map[string]*claim
	rev     int64
	freed   chan struct{} // closed, and replaced, whenever a name is freed
	cut     map[string]bool
	pending map[*claim]struct{} // KeepOnLoss claims lost, to make again on restore
}

// NewNames returns an empty store.
func NewNames() *Names {
	return &Names{held: map[string]*claim{}, freed: make(chan struct{}), cut: map[string]bool{}, pending: map[*claim]struct{}{}}
}

type claim struct {
	s      *Names
	name   string
	holder grpcproc.PID
	keep   bool
	notify func(grpcproc.ClaimEvent)
	rev    int64 // guarded by s.mu
	over   bool  // guarded by s.mu
}

func (c *claim) Revision() int64 {
	c.s.mu.Lock()
	defer c.s.mu.Unlock()
	return c.rev
}

func (c *claim) Release(context.Context) error {
	s := c.s
	s.mu.Lock()
	defer s.mu.Unlock()
	c.over = true
	delete(s.pending, c)
	if s.held[c.name] == c {
		s.free(c.name)
	}
	return nil
}

// free deletes name and wakes those that wait for it. Called with s.mu held.
func (s *Names) free(name string) {
	delete(s.held, name)
	close(s.freed)
	s.freed = make(chan struct{})
}

// For is the view of the store a node named node uses as its Config.Names.
func (s *Names) For(node string) grpcproc.Names { return view{s: s, node: node} }

// List returns the names with prefix, ordered by name, at most limit unless
// limit is 0.
func (s *Names) List(prefix string, limit int) []grpcproc.GlobalName {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []grpcproc.GlobalName
	for _, name := range slices.Sorted(maps.Keys(s.held)) {
		if strings.HasPrefix(name, prefix) && (limit == 0 || len(out) < limit) {
			out = append(out, grpcproc.GlobalName{Name: name, PID: s.held[name].holder})
		}
	}
	return out
}

// lookup returns the holder of name.
func (s *Names) lookup(name string) (grpcproc.PID, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if c := s.held[name]; c != nil {
		return c.holder, true
	}
	return grpcproc.PID{}, false
}

// cutOff loses node's claims: each is lost, its name free for the others, a
// KeepOnLoss one kept to make again when node is restored.
func (s *Names) cutOff(node string) {
	s.mu.Lock()
	s.cut[node] = true
	var lost []*claim
	for name, c := range s.held {
		if c.holder.Node == node {
			s.free(name)
			lost = append(lost, c)
			if c.keep {
				s.pending[c] = struct{}{}
			} else {
				c.over = true
			}
		}
	}
	s.mu.Unlock()
	for _, c := range lost {
		c.notify(grpcproc.ClaimEvent{Kind: grpcproc.ClaimLost})
	}
}

// restore lets node reach the store again, and makes its KeepOnLoss claims
// again: each holds its name once more, or, if another process claimed it
// meanwhile, ends in conflict.
func (s *Names) restore(node string) {
	type outcome struct {
		c  *claim
		ev grpcproc.ClaimEvent
	}
	var told []outcome
	s.mu.Lock()
	delete(s.cut, node)
	for c := range s.pending {
		if c.holder.Node != node {
			continue
		}
		delete(s.pending, c)
		if s.held[c.name] != nil {
			c.over = true
			told = append(told, outcome{c, grpcproc.ClaimEvent{Kind: grpcproc.ClaimConflict}})
			continue
		}
		s.rev++
		c.rev = s.rev
		s.held[c.name] = c
		told = append(told, outcome{c, grpcproc.ClaimEvent{Kind: grpcproc.ClaimRegained, Revision: c.rev}})
	}
	s.mu.Unlock()
	for _, o := range told {
		o.c.notify(o.ev)
	}
}

// drop frees the names node's processes held, as a lease that ends with its
// node does.
func (s *Names) drop(node string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for name, c := range s.held {
		if c.holder.Node == node {
			c.over = true
			s.free(name)
		}
	}
	for c := range s.pending {
		if c.holder.Node == node {
			delete(s.pending, c)
		}
	}
}

// view is a node's Config.Names.
type view struct {
	s    *Names
	node string
}

func (view) Watch(context.Context) error { return nil }

func (v view) Lookup(name string) (grpcproc.PID, bool) { return v.s.lookup(name) }

func (v view) List(prefix string, limit int) []grpcproc.GlobalName { return v.s.List(prefix, limit) }

func (v view) Resolve(ctx context.Context, name string) (grpcproc.PID, bool, error) {
	if err := v.reach(ctx); err != nil {
		return grpcproc.PID{}, false, err
	}
	pid, ok := v.s.lookup(name)
	return pid, ok, nil
}

// reach fails if ctx is done or the node is cut off from the store.
func (v view) reach(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	v.s.mu.Lock()
	defer v.s.mu.Unlock()
	if v.s.cut[v.node] {
		return ErrNamesUnreachable
	}
	return nil
}

func (v view) Claim(ctx context.Context, name string, holder grpcproc.PID, opts grpcproc.ClaimOptions, notify func(grpcproc.ClaimEvent)) (grpcproc.NameClaim, error) {
	s := v.s
	for {
		if err := v.reach(ctx); err != nil {
			return nil, err
		}
		s.mu.Lock()
		taken := s.held[name]
		if taken == nil {
			s.rev++
			c := &claim{s: s, name: name, holder: holder, keep: opts.KeepOnLoss, notify: notify, rev: s.rev}
			s.held[name] = c
			s.mu.Unlock()
			return c, nil
		}
		freed := s.freed
		s.mu.Unlock()
		if !opts.Wait {
			return nil, &grpcproc.TakenError{Name: name, Holder: taken.holder}
		}
		select {
		case <-freed:
		case <-ctx.Done():
			return nil, cmp.Or(context.Cause(ctx), ctx.Err())
		}
	}
}
