package grpcproc

import (
	"context"
	"errors"
	"sync"
)

// Global addresses whichever process holds Name among the installation's
// global names (Config.Names), wherever it runs. The sender's node looks the
// name up, and the message goes to the PID it finds, as if addressed by it:
// nothing on the wire changes. A name no one holds, or a node without Names,
// is a process that does not exist: a call fails with ErrNoProc, a monitor or
// a link gets Down{noproc} with Name set to the global name, and a message is
// a dead letter.
//
// The node's copy of the names lags the store by a moment, so a message sent
// just after a holder moved may reach the old one: noproc once it has
// exited, noconnection if its node is gone. Names.Resolve asks the store
// itself, where that matters.
type Global struct{ Name string }

func (g Global) String() string      { return "{" + g.Name + "@global}" }
func (Global) target() (PID, string) { return PID{}, "" }
func (g Global) globalName() string  { return g.Name }

// globalTarget is a Target that may be a global name: a Global, or an Addr
// made from one.
type globalTarget interface{ globalName() string }

// GlobalName is a global name and the process that holds it.
type GlobalName struct {
	Name string
	PID  PID
}

// Names is a store of global names: the names of the installation, each held
// by one process at a time. The core resolves Global targets through it and
// ties each claim to its process; the store keeps the names, and says when it
// can no longer vouch for a claim. grpcproc/etcd keeps them in etcd;
// grpcproctest keeps them in memory.
type Names interface {
	// Watch begins keeping this node's copy of the names, until ctx is done.
	// Start calls it before the Registrar, and it returns once the copy holds
	// every name, so a node that has started finds every name claimed before.
	Watch(ctx context.Context) error
	// Lookup returns the process that holds name, as this node's copy has
	// it. It must not block: a process's SendTo, which resolves through it,
	// takes no ctx.
	Lookup(name string) (PID, bool)
	// Resolve asks the store itself, for when Lookup's lag matters: before
	// starting what another node may have started a moment ago, or after
	// losing a race to start it.
	Resolve(ctx context.Context, name string) (PID, bool, error)
	// Claim makes holder the holder of name, unless another process holds it:
	// then it fails with a *TakenError, or, with opts.Wait, waits until ctx
	// is done for the name to be free. notify, which must not block, is told
	// what becomes of the claim (see ClaimEvent). Process.Claim calls it, and
	// releases the claim when its process exits.
	Claim(ctx context.Context, name string, holder PID, opts ClaimOptions, notify func(ClaimEvent)) (NameClaim, error)
}

// NameLister is a Names that can list its names: one that keeps them all,
// as the etcd and in-memory stores do. The Inspector lists global names only
// through it.
type NameLister interface {
	// List returns the names with prefix, ordered by name, at most limit of
	// them unless limit is 0.
	List(prefix string, limit int) []GlobalName
}

// NameClaim is a claim a Names store made.
type NameClaim interface {
	// Revision is the claim's fencing token: it grows with each claim of a
	// name, so a write fenced by it refuses a holder that lost the name.
	Revision() int64
	// Release gives the name up, if this claim still holds it, and stops a
	// claim made with KeepOnLoss from being made again.
	Release(ctx context.Context) error
}

// ClaimOptions are what Process.Claim passes to Names.Claim.
type ClaimOptions struct {
	Wait       bool // wait for a name that is held to be free (WaitForName)
	KeepOnLoss bool // claim again once the store can, rather than give up (KeepOnLoss)
}

// ClaimEventKind is what became of a claim.
type ClaimEventKind uint8

const (
	// ClaimLost: the store can no longer vouch for the claim (an etcd lease
	// that ended, or will have by the time another node could claim the
	// name). Without KeepOnLoss, the claim is over.
	ClaimLost ClaimEventKind = iota + 1
	// ClaimRegained: a claim made with KeepOnLoss holds the name again, with
	// a new Revision.
	ClaimRegained
	// ClaimConflict: a claim made with KeepOnLoss found the name held by
	// another process when it could claim again. The claim is over.
	ClaimConflict
)

// ClaimEvent is what a Names store tells of a claim it made.
type ClaimEvent struct {
	Kind     ClaimEventKind
	Revision int64 // the new one, for ClaimRegained
}

// ClaimOption configures Process.Claim.
type ClaimOption func(*ClaimOptions)

// WaitForName makes Claim wait, until its ctx is done, for a name that is
// held to be free, and claim it then: a standby, with the same process on
// other nodes, that takes over when the holder exits or its node goes.
func WaitForName() ClaimOption { return func(o *ClaimOptions) { o.Wait = true } }

// KeepOnLoss keeps the holder running when its claim is lost, rather than
// end it with reason ReasonNameLost, and claims the name again once the
// store can. Meanwhile Held is false, and another process may claim the name
// and run beside it; if one did, the holder the store has wins, and this one
// ends with reason ReasonNameConflict. It is for a process whose duplicate is
// a nuisance and whose absence is an outage: a teleconference room, whose
// calls go on while etcd is out of reach.
func KeepOnLoss() ClaimOption { return func(o *ClaimOptions) { o.KeepOnLoss = true } }

// Errors of global names.
var (
	// ErrTaken is the error of a claim of a name another process holds; the
	// error itself is a *TakenError, which says which. Across the wire only
	// its text travels, which errors.Is still matches.
	ErrTaken = errors.New("grpcproc: name taken")
	// ErrNoNames is the error of a claim on a node without Config.Names.
	ErrNoNames = errors.New("grpcproc: no Config.Names")
)

// TakenError is the error of a claim of a name another process holds.
type TakenError struct {
	Name   string
	Holder PID
}

// Error is ErrTaken's text, so that the error matches ErrTaken on another
// node too, where it arrives as its text alone.
func (*TakenError) Error() string { return ErrTaken.Error() }

// Is reports whether target is ErrTaken.
func (*TakenError) Is(target error) bool { return target == ErrTaken }

// Claim is a global name a process holds: from Process.Claim until the
// process exits, Release is called, or the claim is lost.
type Claim struct {
	name string
	p    *proc
	keep bool

	mu       sync.Mutex
	nc       NameClaim
	revision int64
	held     bool
	over     bool // released, lost for good, or its process exited
}

// Name returns the global name.
func (c *Claim) Name() string { return c.name }

// Revision returns the claim's fencing token: a new one each time a claim
// made with KeepOnLoss holds the name again.
func (c *Claim) Revision() int64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.revision
}

// Held reports whether the claim holds its name: false while a claim made
// with KeepOnLoss is lost, and once it is over.
func (c *Claim) Held() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.held && !c.over
}

// Release gives the name up, while the process goes on.
func (c *Claim) Release(ctx context.Context) error {
	if !c.end() {
		return nil
	}
	c.p.mu.Lock()
	delete(c.p.claims, c)
	c.p.mu.Unlock()
	return c.nc.Release(ctx)
}

// end marks the claim over, and reports whether it was not yet.
func (c *Claim) end() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.over {
		return false
	}
	c.over = true
	return true
}

// notified is what the store tells of the claim.
func (c *Claim) notified(ev ClaimEvent) {
	c.mu.Lock()
	switch ev.Kind {
	case ClaimLost:
		c.held = false
	case ClaimRegained:
		c.held, c.revision = true, ev.Revision
	}
	over := c.over
	c.mu.Unlock()
	if over {
		return
	}
	switch {
	case ev.Kind == ClaimLost && !c.keep:
		c.p.cancel(&ExitError{Reason: ReasonNameLost})
	case ev.Kind == ClaimConflict:
		c.p.cancel(&ExitError{Reason: ReasonNameConflict})
	}
}

// Claim makes p the holder of a global name, until p exits or the claim is
// released: the name is p's to all the installation's nodes, whose Global
// targets reach p. A name another process holds fails with a *TakenError,
// which says which (errors.Is matches ErrTaken), unless WaitForName waits
// for it. A node without Config.Names fails with ErrNoNames.
//
// A claim the store loses, an etcd lease that ended, ends p with reason
// ReasonNameLost, before another process can claim the name, unless it was
// made with KeepOnLoss. Its Revision is the fencing token for what clocks
// cannot promise.
func (p *proc) Claim(ctx context.Context, name string, opts ...ClaimOption) (*Claim, error) {
	names := p.n.cfg.Names
	if names == nil {
		return nil, ErrNoNames
	}
	var o ClaimOptions
	for _, opt := range opts {
		opt(&o)
	}
	// A wait for the name ends with the process too.
	ctx, cancel := context.WithCancel(ctx)
	defer context.AfterFunc(p.ctx, cancel)()
	defer cancel()
	c := &Claim{name: name, p: p, keep: o.KeepOnLoss, held: true}
	nc, err := names.Claim(ctx, name, p.pid, o, c.notified)
	if err != nil {
		return nil, err
	}
	c.mu.Lock()
	c.nc = nc
	c.revision = nc.Revision()
	c.mu.Unlock()
	p.mu.Lock()
	if p.exited {
		p.mu.Unlock()
		c.end()
		p.n.release(nc)
		return nil, ErrNoProc
	}
	if p.claims == nil {
		p.claims = map[*Claim]struct{}{}
	}
	p.claims[c] = struct{}{}
	p.mu.Unlock()
	return c, nil
}

// claimNames lists the global names p holds, for ProcessInfo. Called with
// p.mu held.
func (p *proc) claimNames() []string {
	var names []string
	for c := range p.claims {
		names = append(names, c.name)
	}
	return names
}

// release gives up claims of a process that exited, in the background: the
// store may be slow or out of reach, and the process's watchers are not to
// wait for it. Stop waits for them, before it withdraws the node.
func (n *Node) release(claims ...NameClaim) {
	for _, nc := range claims {
		n.releases.Go(func() {
			ctx, cancel := context.WithTimeout(context.Background(), n.cfg.DialTimeout)
			defer cancel()
			if err := nc.Release(ctx); err != nil {
				n.log.Warn("release of a global name failed", "err", err)
			}
		})
	}
}

// resolve turns a global name into what the node sends to: the PID that
// holds it, or nowhere, a PID of this node that no process has, so that
// delivery answers as for a process that does not exist.
func (n *Node) resolve(name string) PID {
	if names := n.cfg.Names; names != nil {
		if pid, ok := names.Lookup(name); ok {
			return pid
		}
	}
	return PID{Node: n.id.Name}
}

// resolveTarget is resolve for a Target, which it returns as a PID, along
// with the global name, or returns as it is.
func (n *Node) resolveTarget(t Target) (Target, string) {
	if g, ok := t.(globalTarget); ok {
		if name := g.globalName(); name != "" {
			return n.resolve(name), name
		}
	}
	return t, ""
}

// resolveDest is resolve for a dest.
func (n *Node) resolveDest(d dest) dest {
	if d.global != "" {
		return dest{pid: n.resolve(d.global)}
	}
	return d
}

// Names returns Config.Names, or nil: for a component that resolves names
// itself, through Resolve.
func (n *Node) Names() Names { return n.cfg.Names }
