package grpcproc

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"maps"
	"math"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/reflect/protoregistry"

	grpcprocv1 "github.com/floatdrop/grpcproc/proto/grpcproc/v1"
)

// Resolver turns a node name into an address grpc can dial. Resolve must
// return once ctx is done: that is what bounds a dial by Config.DialTimeout.
type Resolver interface {
	Resolve(ctx context.Context, node string) (addr string, err error)
}

// Member is one incarnation of a node, as the cluster knows it.
type Member struct {
	Name        string
	Incarnation uint64
	Addr        string // where peers dial it: its Config.Advertise
}

// Registrar publishes a node to the cluster, so that peers resolve it and
// Membership reports it.
type Registrar interface {
	// Register returns once self is published, and keeps it published
	// until withdraw is called. Start calls Register; Stop calls withdraw
	// last, after the node's processes have exited and their Down notices
	// have reached its peers. An error must leave self unpublished: Start
	// may call Register again.
	Register(ctx context.Context, self Member) (withdraw func(context.Context) error, err error)
}

// Membership is the cluster's authoritative view of which nodes are alive.
// Links notice a dead peer by themselves, but only as fast as keepalive
// allows and not at all behind a half-open connection; Membership (an etcd
// lease expiring, say) settles it.
type Membership interface {
	// Watch reports members joining and leaving until ctx is done, then
	// closes the channel.
	Watch(ctx context.Context) (<-chan MemberEvent, error)
}

// MemberEvent is a member joining (Up) or leaving the cluster. A leaving
// member with Incarnation 0 means whichever incarnation it was; a joining
// one, an incarnation not known.
type MemberEvent struct {
	Member Member
	Up     bool
}

// lastIncarnation is the default incarnation this program picked last.
var lastIncarnation atomic.Uint64

// nextIncarnation is the current Unix time in nanoseconds, or one more than
// the incarnation picked last, if the clock has not moved past it: two nodes
// of one name started at the same instant must not share an incarnation.
func nextIncarnation() uint64 {
	for {
		last := lastIncarnation.Load()
		inc := max(uint64(time.Now().UnixNano()), last+1)
		if lastIncarnation.CompareAndSwap(last, inc) {
			return inc
		}
	}
}

// ResolverFunc adapts a function to Resolver.
type ResolverFunc func(ctx context.Context, node string) (string, error)

func (f ResolverFunc) Resolve(ctx context.Context, node string) (string, error) { return f(ctx, node) }

// StaticResolver is a fixed node -> address map.
type StaticResolver map[string]string

func (s StaticResolver) Resolve(_ context.Context, node string) (string, error) {
	if addr, ok := s[node]; ok {
		return addr, nil
	}
	return "", fmt.Errorf("grpcproc: unknown node %q", node)
}

// Config configures a Node. Name and Resolver are required.
type Config struct {
	// Name is the node's logical name; peers address it by this.
	Name string
	// Advertise is the address peers dial to reach this node's gRPC server.
	// It is only informational to the core (NodeInfo, Hello); a Registrar
	// publishes it.
	Advertise string
	// Incarnation distinguishes this start of the node from earlier ones,
	// and must grow with each start: a peer that has seen an incarnation of
	// the node refuses links from older ones, so that an instance that was
	// replaced, and still runs, cannot take the links of its replacement.
	// The peer refuses them until Membership reports the newer one gone, or
	// it calls Disconnect: a random or hashed Incarnation would be refused
	// whenever it came out lower than the last one. Zero picks the current
	// Unix time in nanoseconds, which grows as long as the clocks of the
	// hosts the node starts on agree to within the time between two starts,
	// or one more than the last incarnation picked in this program, if the
	// clock has not moved since: a coarse clock, or a testing/synctest
	// bubble, whose clock stands still until every goroutine in it waits.
	Incarnation uint64
	// Resolver maps peer names to addresses.
	Resolver Resolver
	// Registrar, if set, publishes the node on Start and withdraws it on Stop.
	Registrar Registrar
	// Membership, if set, is watched from Start: a peer that leaves the
	// cluster, or comes back as a newer incarnation, has its links dropped,
	// which fires Down{noconnection} for monitors across them and fails
	// pending calls. An older incarnation reported up is ignored.
	Membership Membership
	// DialOptions are used for every outbound connection: credentials,
	// keepalive, interceptors. Keepalive is what turns a silent partition
	// into a link error; set it.
	DialOptions []grpc.DialOption
	// Authorize, if set, runs for every inbound link with the peer's
	// transport credentials in ctx (grpc/peer) and the identity it claims.
	Authorize func(ctx context.Context, peer NodeID) error
	// Logger defaults to slog.Default().
	Logger *slog.Logger
	// Hooks is the observability tap; nil means none.
	Hooks Hooks
	// CopyLocal clones every locally delivered message, so a sender can keep
	// mutating what it sent. Off by default: the pointer is shared.
	CopyLocal bool
	// DialTimeout bounds a dial: resolving the peer, connecting, and the
	// handshake. A send that waits for a dial waits at most this long, as
	// long as the Resolver and any interceptors in DialOptions honour their
	// ctx. Default 5s.
	DialTimeout time.Duration
	// DialBackoff is the longest a node waits before dialing a peer again
	// once dials to it have failed. Meanwhile everything routed to the peer
	// fails at once with ErrNoConnection rather than wait for a dial that
	// would likely fail too: sends, calls, exits, replies, Downs; a Monitor
	// gets Down{noconnection} at once. The first send after the wait dials
	// again, and the others keep failing while it does. The first wait is a
	// 32nd of DialBackoff, up to a fifth less for jitter. Each further failure
	// doubles it, unless the peer was left alone for longer than DialBackoff,
	// and a dial that succeeds starts over. A link the peer opens lets the
	// next send dial at once, without starting over, since it shows the peer
	// is up but not that this node can reach it. Membership reporting the peer
	// up (not as an older incarnation than this node has seen), or
	// Disconnect, ends the wait. Default 5s; negative dials again at once.
	DialBackoff time.Duration
}

// Node hosts processes and links to peers. It is a plain value the
// application constructs, registers on its gRPC server, starts and stops.
type Node struct {
	cfg   Config
	id    NodeID
	log   *slog.Logger
	hooks Hooks

	ctx    context.Context // parent of every process; cancelled first by Stop
	cancel context.CancelFunc
	dialWG sync.WaitGroup // finishDial goroutines; Stop waits for them

	nextID   atomic.Uint64
	nextRef  atomic.Uint64
	started  atomic.Int64                // unix nanos, 0 before Start succeeds
	startMu  sync.Mutex                  // one Start at a time
	withdraw func(context.Context) error // from Registrar; guarded by mu

	// pending is the calls waiting for an answer from a peer. pendingMu
	// guards it, and nothing is locked while pendingMu is held: every such
	// call takes it twice, and shares it with no one else's state. A call to
	// a process of this node is not in it (see callLocal).
	pendingMu sync.Mutex
	pending   map[uint64]*pendingCall

	// mu guards the fields below, through stopped. A process's lock may be
	// taken inside it (spawn does), never the reverse. Deliveries, and sends
	// over a live link, only read, so they take it shared.
	mu       sync.RWMutex
	procs    map[uint64]*proc
	names    map[string]*proc
	out      map[string]*outLink
	in       map[string]*inLink
	dialing  map[string]*dialOp
	backoff  map[string]*redial // peers whose last dial failed
	dials    map[string]uint64  // per peer, for LinkInfo.Reconnects
	newest   map[string]uint64  // per peer, the newest incarnation seen (see meet)
	stopping bool               // Stop began: no new processes
	stopped  bool               // links closed: no new links
	// settling counts, per peer, the changes to its links that were decided
	// under mu and are still being carried out: a teardown until the peer is
	// declared down, a new inbound link until it is announced. A new inbound
	// link waits for none to be under way (settled, on mu), so nothing it
	// carries overtakes the old session's Downs, and the peer's inbound link
	// events come in order. (A dial is not held up: its link carries nothing
	// in.)
	settling map[string]int
	settled  *sync.Cond
	wg       sync.WaitGroup

	spawned, exited, deadLetters atomic.Uint64
	subs                         subscribers
}

type pendingCall struct {
	node string
	// ch has room for the one answer. Whoever answers first removes the call
	// from n.pending, under n.pendingMu, so no one else can: takePending,
	// nodeDown, Stop.
	ch chan callResult
}

// takePending removes and returns the call waiting on ref, or nil: the
// caller that gets it answers it.
func (n *Node) takePending(ref uint64) *pendingCall {
	n.pendingMu.Lock()
	defer n.pendingMu.Unlock()
	pc := n.pending[ref]
	delete(n.pending, ref)
	return pc
}

type callResult struct {
	body proto.Message
	err  error
}

// outcome is what a caller gets for an answer with status.
func outcome(body proto.Message, status grpcprocv1.Status, errText string) callResult {
	switch status {
	case grpcprocv1.Status_STATUS_OK:
		return callResult{body: body}
	case grpcprocv1.Status_STATUS_NOPROC:
		return callResult{err: ErrNoProc}
	case grpcprocv1.Status_STATUS_TYPE:
		return callResult{err: ErrType}
	default:
		return callResult{body: body, err: &RemoteError{Msg: errText}}
	}
}

// NewNode validates cfg and returns a node that is not yet started.
func NewNode(cfg Config) (*Node, error) {
	if cfg.Name == "" {
		return nil, errors.New("grpcproc: Config.Name is required")
	}
	if cfg.Resolver == nil {
		return nil, errors.New("grpcproc: Config.Resolver is required")
	}
	if cfg.Incarnation == 0 {
		cfg.Incarnation = nextIncarnation()
	}
	cfg.Logger = cmp.Or(cfg.Logger, slog.Default())
	cfg.DialTimeout = cmp.Or(cfg.DialTimeout, 5*time.Second)
	cfg.DialBackoff = cmp.Or(cfg.DialBackoff, 5*time.Second)
	ctx, cancel := context.WithCancel(context.Background())
	n := &Node{
		cfg:      cfg,
		id:       NodeID{Name: cfg.Name, Incarnation: cfg.Incarnation},
		hooks:    cfg.Hooks,
		ctx:      ctx,
		cancel:   cancel,
		procs:    map[uint64]*proc{},
		names:    map[string]*proc{},
		pending:  map[uint64]*pendingCall{},
		out:      map[string]*outLink{},
		in:       map[string]*inLink{},
		dialing:  map[string]*dialOp{},
		backoff:  map[string]*redial{},
		dials:    map[string]uint64{},
		newest:   map[string]uint64{},
		settling: map[string]int{},
	}
	n.settled = sync.NewCond(&n.mu)
	n.log = cfg.Logger.With("node", cfg.Name)
	return n, nil
}

// Register mounts the grpcproc.v1.Node service on s. Call it before the server
// starts serving.
func (n *Node) Register(s grpc.ServiceRegistrar) {
	grpcprocv1.RegisterNodeServer(s, linkServer{n: n})
}

// linkServer is the grpcproc.v1.Node service. It is a type of its own so that
// Node's API does not carry the generated server's methods.
type linkServer struct {
	grpcprocv1.UnimplementedNodeServer
	n *Node
}

// Link serves one inbound link; see serveLink.
func (s linkServer) Link(stream grpc.BidiStreamingServer[grpcprocv1.Frame, grpcprocv1.Frame]) error {
	return s.n.serveLink(stream)
}

// Name is the node's logical name.
func (n *Node) Name() string { return n.id.Name }

// ID is the node's name and incarnation.
func (n *Node) ID() NodeID { return n.id }

// PID is the node's own pseudo-process (ID 0): the sender of messages sent
// from outside any process.
func (n *Node) PID() PID { return PID{Node: n.id.Name, Incarnation: n.id.Incarnation} }

// Start watches Membership and publishes the node with its Registrar, when
// configured. Processes may be spawned before Start and run immediately;
// Start only makes the node known. ctx bounds it. A call after one that
// succeeded does nothing; after one that failed, Start tries again;
// concurrent calls wait for the one in progress; after Stop began, Start
// fails with ErrNodeStopped.
func (n *Node) Start(ctx context.Context) error {
	n.startMu.Lock()
	defer n.startMu.Unlock()
	if n.started.Load() != 0 {
		return nil
	}
	n.mu.Lock()
	stopping := n.stopping
	n.mu.Unlock()
	if stopping {
		return ErrNodeStopped
	}
	now := time.Now()
	// A Start that fails undoes itself: it stops watching and waits for the
	// watcher to finish, so that no event from it lands after a retry.
	watching, unwatch := context.WithCancel(n.ctx)
	watched := make(chan struct{})
	failed := func(err error) error {
		unwatch()
		select {
		case <-watched:
		case <-ctx.Done():
		}
		return err
	}
	if m := n.cfg.Membership; m != nil {
		stop := context.AfterFunc(ctx, unwatch) // ctx bounds the Watch call too
		events, err := m.Watch(watching)
		if !stop() && err == nil {
			err = context.Cause(ctx)
		}
		if err != nil {
			close(watched)
			return failed(fmt.Errorf("grpcproc: membership: %w", err))
		}
		go func() {
			defer close(watched)
			n.watchMembers(events)
		}()
	} else {
		close(watched)
	}
	if r := n.cfg.Registrar; r != nil {
		withdraw, err := r.Register(ctx, Member{Name: n.id.Name, Incarnation: n.id.Incarnation, Addr: n.cfg.Advertise})
		if err != nil {
			return failed(fmt.Errorf("grpcproc: register: %w", err))
		}
		n.mu.Lock()
		stopping := n.stopping
		if !stopping {
			n.withdraw = withdraw
		}
		n.mu.Unlock()
		if stopping {
			// Stop began while this registered: it would not withdraw it.
			return failed(errors.Join(ErrNodeStopped, withdraw(ctx)))
		}
	}
	n.started.Store(now.UnixNano())
	return nil
}

func (n *Node) watchMembers(events <-chan MemberEvent) {
	for ev := range events {
		if ev.Member.Name != n.id.Name {
			n.memberEvent(ev)
		}
	}
}

// memberEvent drops the links to a peer that left, or that came back as a
// newer incarnation: either way, whatever crossed those links is gone. An
// older incarnation reported up is ignored, as a link from it is refused.
// Once the newest incarnation has left, no incarnation is refused.
func (n *Node) memberEvent(ev MemberEvent) {
	peer := NodeID{Name: ev.Member.Name, Incarnation: ev.Member.Incarnation}
	n.mu.Lock()
	var cause error
	switch newest := n.newest[peer.Name]; {
	case !ev.Up:
		// The links with a peer are with its newest incarnation.
		if newest != 0 && (peer.Incarnation == newest || peer.Incarnation == 0) {
			delete(n.newest, peer.Name)
			cause = errors.New("left the cluster")
		}
	case peer.Incarnation == 0: // up, as whichever incarnation it is
		n.forget(peer.Name)
	default:
		var stale error
		if cause, stale = n.meet(peer); stale != nil {
			n.mu.Unlock()
			n.log.Warn("ignored an old incarnation reported up", "peer", peer, "err", stale)
			return
		}
		n.forget(peer.Name) // it is back: dial it at once
	}
	// The links judged are the links dropped: taken in the same critical
	// section, not one that may have replaced them since.
	var out *outLink
	var in *inLink
	if cause != nil {
		out, in = n.takeLinks(peer.Name)
	}
	n.mu.Unlock()
	if out != nil || in != nil {
		n.linksLost(out, in, cause)
	}
}

// Stop asks every process to exit (Receive returns ReasonShutdown), waits for
// them until ctx is done, then closes every link. Watchers of this node's
// processes receive Down{shutdown} while the links are still up: every
// outbound link flushes, and is waited on until its peer ends it, or ctx is
// done. A dial in flight completes within Config.DialTimeout and its link is
// discarded; Stop waits for it too, until ctx is done. If ctx cuts any of
// these waits short, Stop returns its error. The Registrar's withdraw comes
// last, and gets a second of its own if ctx is done by then.
func (n *Node) Stop(ctx context.Context) error {
	n.mu.Lock()
	if n.stopping {
		n.mu.Unlock()
		return nil
	}
	n.stopping = true
	n.mu.Unlock()

	n.cancel()
	done := make(chan struct{})
	go func() { n.wg.Wait(); close(done) }()
	var err error
	select {
	case <-done:
	case <-ctx.Done():
		err = fmt.Errorf("grpcproc: stop: %w", ctx.Err())
	}

	n.mu.Lock()
	n.stopped = true
	dialing := len(n.dialing) > 0
	outs, ins := n.out, n.in
	n.out, n.in = map[string]*outLink{}, map[string]*inLink{}
	n.mu.Unlock()
	// Let the Down{shutdown} envelopes reach their peers before the links go:
	// every link flushes at once, then each is waited for.
	for _, l := range outs {
		l.shutdown()
	}
	flushed := true
	for _, l := range outs {
		if l.finish(ctx) {
			flushed = false
		}
	}
	if !flushed && err == nil {
		err = fmt.Errorf("grpcproc: stop: %w", ctx.Err())
	}
	for _, l := range ins {
		l.close()
	}
	// Calls still waiting on a peer will get no answer, and nothing declares
	// the peers down any more: fail them. They may have been handled.
	n.pendingMu.Lock()
	pending := n.pending
	n.pending = map[uint64]*pendingCall{}
	n.pendingMu.Unlock()
	for _, pc := range pending {
		pc.ch <- callResult{err: ErrNodeStopped}
	}
	// So will local calls still open on processes that outlived the wait.
	n.mu.RLock()
	procs := slices.Collect(maps.Values(n.procs))
	n.mu.RUnlock()
	for _, p := range procs {
		p.failLocalCalls(ErrNodeStopped)
	}
	// A dial that was in flight when the node stopped completes, and
	// finishDial discards its link: wait for it to let go of the node.
	if dialing {
		dialed := make(chan struct{})
		go func() { n.dialWG.Wait(); close(dialed) }()
		select {
		case <-dialed:
		case <-ctx.Done():
			if err == nil {
				err = fmt.Errorf("grpcproc: stop: %w", ctx.Err())
			}
		}
	}
	n.mu.Lock()
	withdraw := n.withdraw
	n.mu.Unlock()
	if withdraw != nil {
		// Withdrawing is what tells the cluster this node is gone; one peer
		// that never ended its stream must not leave it published.
		wctx, cancel := context.WithCancel(ctx)
		if ctx.Err() != nil {
			wctx, cancel = context.WithTimeout(context.WithoutCancel(ctx), time.Second)
		}
		defer cancel()
		if werr := withdraw(wctx); werr != nil {
			err = errors.Join(err, fmt.Errorf("grpcproc: withdraw: %w", werr))
		}
	}
	return err
}

// ---------- registry & inspection ----------

// Whereis resolves a name registered on this node.
func (n *Node) Whereis(name string) (PID, bool) {
	n.mu.RLock()
	defer n.mu.RUnlock()
	if p := n.names[name]; p != nil {
		return p.pid, true
	}
	return PID{}, false
}

// Processes snapshots every local process, ordered by PID.
func (n *Node) Processes() []ProcessInfo {
	n.mu.RLock()
	procs := slices.Collect(maps.Values(n.procs))
	n.mu.RUnlock()
	slices.SortFunc(procs, func(a, b *proc) int { return cmp.Compare(a.pid.ID, b.pid.ID) }) // PIDs never change
	out := make([]ProcessInfo, len(procs))
	for i, p := range procs {
		out[i] = p.info()
	}
	return out
}

// Process snapshots one local process.
func (n *Node) Process(pid PID) (ProcessInfo, bool) {
	p := n.local(pid)
	if p == nil {
		return ProcessInfo{}, false
	}
	return p.info(), true
}

// Inspect asks a local process what it currently believes: the map its
// WithInspect function returns, produced on the process's own goroutine
// between two messages. A process busy in a handler answers when it next
// calls Receive; ctx bounds the wait, and the error then wraps ctx.Err(). A
// process that is gone, or exits before answering (its inspect function
// panicked), is ErrNoProc.
func (n *Node) Inspect(ctx context.Context, pid PID) (map[string]string, error) {
	p := n.local(pid)
	if p == nil {
		return nil, ErrNoProc
	}
	return p.inspectNow(ctx)
}

// SetLogLevel sets the threshold of a local process's Log() at runtime.
func (n *Node) SetLogLevel(pid PID, level slog.Level) error {
	p := n.local(pid)
	if p == nil {
		return ErrNoProc
	}
	p.level.Store(int64(level))
	p.levelSet.Store(true)
	return nil
}

// Info snapshots the node.
func (n *Node) Info() NodeInfo {
	n.mu.Lock()
	info := NodeInfo{
		ID:          n.id,
		Advertise:   n.cfg.Advertise,
		Processes:   len(n.procs),
		Spawned:     n.spawned.Load(),
		Exited:      n.exited.Load(),
		DeadLetters: n.deadLetters.Load(),
	}
	if s := n.started.Load(); s != 0 {
		info.StartedAt = time.Unix(0, s)
	}
	for _, l := range n.out {
		info.Links = append(info.Links, l.info())
	}
	for peer := range n.backoff {
		if r := n.backedOff(peer); r != nil {
			info.Links = append(info.Links, LinkInfo{
				Peer: NodeID{Name: peer}, Outbound: true, State: LinkDown,
				Reconnects: n.dials[peer], LastError: r.why, RetryAt: r.at,
			})
		}
	}
	for _, l := range n.in {
		info.Links = append(info.Links, l.info())
	}
	n.mu.Unlock()
	// Outbound links were collected first, so a stable sort by peer keeps
	// each peer's outbound link ahead of its inbound one.
	slices.SortStableFunc(info.Links, func(a, b LinkInfo) int { return cmp.Compare(a.Peer.Name, b.Peer.Name) })
	return info
}

// Disconnect drops every link with peer, as if the network had, and
// forgets that dials to it failed (Config.DialBackoff) and which
// incarnation of it this node has seen, so that none is refused (see
// Config.Incarnation). Monitors across it fire Down{noconnection} and
// pending calls fail; the next send dials again. It reports whether there
// was a link to drop.
func (n *Node) Disconnect(peer string) bool {
	n.mu.Lock()
	n.forget(peer)
	delete(n.newest, peer)
	out, in := n.takeLinks(peer)
	n.mu.Unlock()
	if out == nil && in == nil {
		return false
	}
	n.linksLost(out, in, errors.New("disconnected"))
	return true
}

// Peers lists nodes with a live link in either direction.
func (n *Node) Peers() []string {
	n.mu.RLock()
	defer n.mu.RUnlock()
	seen := make(map[string]struct{}, len(n.out)+len(n.in))
	for p := range n.out {
		seen[p] = struct{}{}
	}
	for p := range n.in {
		seen[p] = struct{}{}
	}
	return slices.Sorted(maps.Keys(seen))
}

func (n *Node) local(pid PID) *proc {
	if pid.Node != n.id.Name || pid.Incarnation != n.id.Incarnation {
		return nil
	}
	n.mu.RLock()
	defer n.mu.RUnlock()
	return n.procs[pid.ID]
}

// lookup resolves a delivery target on this node. A PID from another
// incarnation resolves to nothing.
func (n *Node) lookup(pid PID, name string) *proc {
	n.mu.RLock()
	defer n.mu.RUnlock()
	if name != "" {
		return n.names[name]
	}
	if pid.Incarnation != n.id.Incarnation {
		return nil
	}
	return n.procs[pid.ID]
}

// ---------- messaging from outside a process ----------

// SendTo delivers m from the node to an untyped target: a PID, a Name, or an
// Addr of another type, as Addr.Send does to a typed one. The target's type
// is checked on delivery only.
func (n *Node) SendTo(ctx context.Context, to Target, m proto.Message) error {
	return n.send(ctx, n.PID(), nil, destOf(to), m, MetadataFrom(ctx))
}

// CallTo calls an untyped target from the node, as Addr.Call does a typed
// one, and types the reply as R.
func (n *Node) CallTo[R proto.Message](ctx context.Context, to Target, req proto.Message) (R, error) {
	return typed[R](n.doCall(ctx, n.PID(), nil, destOf(to), req, MetadataFrom(ctx)))
}

// Exit asks a process anywhere to terminate with reason. As for Send, ctx
// bounds only the wait for a connection to a peer this node has no link to
// yet.
func (n *Node) Exit(ctx context.Context, to Target, reason string) error {
	return n.exit(ctx, n.PID(), to, reason)
}

// ---------- the operations; each has a local and a remote path ----------

// hookSend runs OnSend. md is final: a process's inherited metadata was
// merged in by the method that sent (see proc.outgoing).
func (n *Node) hookSend(from PID, sender *proc, pid PID, name string, body proto.Message, md Metadata, call bool) (Metadata, Done) {
	if n.hooks == nil {
		return md, nil
	}
	s := SendInfo{From: from, To: pid, ToName: name, Body: body, Call: call, Remote: pid.Node != n.id.Name}
	if sender != nil {
		s.FromLabel = sender.label
	}
	return n.hooks.OnSend(s, md)
}

func (n *Node) send(ctx context.Context, from PID, sender *proc, to dest, body proto.Message, md Metadata) (err error) {
	pid, name := to.pid, to.name
	md, done := n.hookSend(from, sender, pid, name, body, md, false)
	if done != nil {
		defer func() { done(err) }()
	}
	if sender != nil {
		sender.sent.Add(1)
	}
	if pid.Node == n.id.Name {
		if n.cfg.CopyLocal {
			body = proto.Clone(body)
		}
		n.deliver(pid, name, item{from: from, body: body, md: md}, nil)
		return nil
	}
	env := wire(grpcprocv1.Kind_KIND_SEND, from, pid, name)
	env.Metadata = md
	if err := encodeBody(env, body); err != nil {
		return err
	}
	return n.route(ctx, pid.Node, env)
}

func (n *Node) doCall(ctx context.Context, from PID, caller *proc, to dest, req proto.Message, md Metadata) (_ proto.Message, err error) {
	pid, name := to.pid, to.name
	md, done := n.hookSend(from, caller, pid, name, req, md, true)
	if done != nil {
		defer func() { done(err) }()
	}
	if err := ctx.Err(); err != nil {
		return nil, err // sends nothing: a ctx error from a call is otherwise ambiguous
	}
	if caller != nil {
		caller.sent.Add(1)
		caller.callsInFlight.Add(1)
		caller.setState(StateWaitingReply)
		defer func() { caller.callsInFlight.Add(-1); caller.setState(StateRunning) }()
	}
	if pid.Node == n.id.Name {
		return n.callLocal(ctx, from, pid, name, req, md)
	}
	return n.callRemote(ctx, from, pid, name, req, md)
}

// callLocal calls a process of this node. Its caller waits on a channel of
// its own, which the call leaves in the process's open calls when it is
// queued, and whoever takes it from there answers it: the process's Reply,
// its exit, or Stop, if the process outlives it. deliver answers a call it
// cannot queue. No one else can, so nothing needs to find the call by its
// ref, and the caller waits on nothing else but its ctx.
func (n *Node) callLocal(ctx context.Context, from, to PID, name string, req proto.Message, md Metadata) (proto.Message, error) {
	if n.cfg.CopyLocal {
		req = proto.Clone(req)
	}
	ch := make(chan callResult, 1)
	it := item{from: from, body: req, md: md, ref: n.nextRef.Add(1)}
	if d, ok := ctx.Deadline(); ok {
		it.deadline = unixNanos(d)
	}
	n.deliver(to, name, it, ch)
	select {
	case r := <-ch:
		return r.body, r.err
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

// callRemote calls a process of a peer, which answers by ref: the call
// waits in n.pending.
func (n *Node) callRemote(ctx context.Context, from, to PID, name string, req proto.Message, md Metadata) (proto.Message, error) {
	ref := n.nextRef.Add(1)
	env := wire(grpcprocv1.Kind_KIND_CALL, from, to, name)
	env.Ref, env.Metadata = ref, md
	if d, ok := ctx.Deadline(); ok {
		// Time left rather than the deadline, so that the peer's clock need
		// not agree with this one's; at least 1ns, which still says "a
		// deadline", for one that has just passed.
		env.TimeoutNanos = max(int64(time.Until(d)), 1)
	}
	if err := encodeBody(env, req); err != nil {
		return nil, err
	}
	pc := &pendingCall{node: to.Node, ch: make(chan callResult, 1)}
	n.pendingMu.Lock()
	n.pending[ref] = pc
	n.pendingMu.Unlock()
	// Whatever answers the call removes it from pending first (see
	// pendingCall), so only a call that ends unanswered does it here, and
	// takes pendingMu again: a route error, ctx, or a panic on the way.
	answered := false
	defer func() {
		if !answered {
			n.pendingMu.Lock()
			delete(n.pending, ref)
			n.pendingMu.Unlock()
		}
	}()
	if err := n.route(ctx, to.Node, env); err != nil {
		return nil, err
	}
	select {
	case r := <-pc.ch:
		answered = true
		return r.body, r.err
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

// reply answers a call: a caller of this node on ch, the channel it waits
// on, one of a peer's by ref. dispatching is set when the answer comes from
// dispatch, on the link the call arrived by; see routeOrCut.
func (n *Node) reply(from, to PID, ref uint64, ch chan<- callResult, body proto.Message, status grpcprocv1.Status, errText string, dispatching bool) error {
	if to.Node == n.id.Name {
		// With no ch, the call was answered already: this is a second Reply,
		// or one after the process's exit, or Stop, answered for it.
		if ch != nil {
			answer(ch, outcome(body, status, errText))
		}
		return nil
	}
	env := wire(grpcprocv1.Kind_KIND_REPLY, from, to, "")
	env.Ref, env.Status, env.Reason = ref, status, errText
	if body != nil {
		if err := encodeBody(env, body); err != nil {
			return err
		}
	}
	return n.routeOrCut(to.Node, env, dispatching)
}

// routeOrCut routes a reply or a Down. The peer waits for those over a link
// of its own, which stays up when this node cannot reach it back, so it would
// never learn that one was lost. When one cannot be routed, that link goes
// too, told why: the peer sees this node as unreachable, its calls fail and
// its monitors fire, as if the connection had broken both ways.
//
// An answer dispatch makes itself (no such process, wrong type) runs on the
// link the peer's frame came by, holding it: closing that link, as Stop and
// Disconnect do, waits for the frame. It must not wait for a dial there. With
// no link to the peer yet, it is queued on the dial, which writes it first,
// in order with the other answers, once the link is up.
func (n *Node) routeOrCut(node string, env *grpcprocv1.Envelope, dispatching bool) error {
	var err error
	if dispatching {
		n.mu.Lock()
		l := n.out[node]
		if l == nil && !n.stopped {
			var d *dialOp
			if d, err = n.dialFor(node); err == nil {
				d.answers = append(d.answers, env)
				// The frame came by n.in[node], unless that is being torn
				// down (see settling): the link to cut if the answer fails.
				if in := n.in[node]; in != nil && !slices.Contains(d.cut, in) {
					d.cut = append(d.cut, in)
				}
				n.mu.Unlock()
				return nil
			}
		}
		n.mu.Unlock()
		if l != nil {
			err = l.send(env)
		}
	} else {
		err = n.route(context.Background(), node, env)
	}
	if le, ok := errors.AsType[*LinkError](err); ok {
		n.mu.Lock()
		in := n.in[node]
		n.mu.Unlock()
		if in != nil {
			in.abort(le.Err)
		}
	}
	return err
}

func (n *Node) monitor(from PID, to Target, ref uint64) error {
	pid, name := to.target()
	if pid.Node == n.id.Name {
		n.deliverMonitor(from, pid, name, ref)
		return nil
	}
	env := wire(grpcprocv1.Kind_KIND_MONITOR, from, pid, name)
	env.Ref = ref
	return n.route(context.Background(), pid.Node, env)
}

func (n *Node) demonitor(from PID, to Target, ref uint64) error {
	pid, name := to.target()
	if pid.Node == n.id.Name {
		n.deliverDemonitor(from, pid, name, ref)
		return nil
	}
	env := wire(grpcprocv1.Kind_KIND_DEMONITOR, from, pid, name)
	env.Ref = ref
	return n.route(context.Background(), pid.Node, env)
}

// down tells a watcher that what it monitors is gone. dispatching is as for
// reply.
func (n *Node) down(from, to PID, ref uint64, reason string, dispatching bool) error {
	if to.Node == n.id.Name {
		n.deliverDown(from, to, ref, reason)
		return nil
	}
	env := wire(grpcprocv1.Kind_KIND_DOWN, from, to, "")
	env.Ref, env.Reason = ref, reason
	return n.routeOrCut(to.Node, env, dispatching)
}

func (n *Node) exit(ctx context.Context, from PID, to Target, reason string) error {
	pid, name := to.target()
	if pid.Node == n.id.Name {
		n.deliverExit(pid, name, reason)
		return nil
	}
	env := wire(grpcprocv1.Kind_KIND_EXIT, from, pid, name)
	env.Reason = reason
	return n.route(ctx, pid.Node, env)
}

// route queues env on the link to node, dialing it if needed. ctx bounds the
// wait for the dial. Sends through an address, the node's SendTo and Exit,
// and calls pass their caller's ctx; everything else passes
// context.Background(): replies, monitors, Downs, a process's SendTo and
// Exit, and timers wait for the dial, which Config.DialTimeout bounds.
// While dials to node are backed off, route fails at once.
func (n *Node) route(ctx context.Context, node string, env *grpcprocv1.Envelope) error {
	if node == "" {
		return errors.New("grpcproc: empty destination node")
	}
	l, err := n.getOut(ctx, node)
	if err != nil {
		return err
	}
	return l.send(env)
}

// ---------- local delivery; also the sink for decoded inbound envelopes ----------

// deliver queues a message, or a call (it.ref set), for a process of this
// node, or answers for it if there is none that takes it. reply is what a
// caller of this node waits on (see callLocal).
func (n *Node) deliver(to PID, name string, it item, reply chan<- callResult) {
	p := n.lookup(to, name)
	if p == nil {
		n.deadLetter(it.from, to, it.body, ReasonNoProc)
		if it.ref != 0 {
			_ = n.reply(to, it.from, it.ref, reply, nil, grpcprocv1.Status_STATUS_NOPROC, "", true)
		}
		return
	}
	if !p.accept(it.body) {
		n.deadLetter(it.from, p.pid, it.body, ReasonType)
		if it.ref != 0 {
			_ = n.reply(p.pid, it.from, it.ref, reply, nil, grpcprocv1.Status_STATUS_TYPE, "", true)
		}
		return
	}
	if n.hooks != nil {
		it.at = time.Now().UnixNano() // for OnReceive's exact wait
	}
	var queued bool
	if it.ref == 0 {
		queued = p.push(it)
	} else {
		queued = p.queueCall(it, reply)
	}
	if !queued {
		n.deadLetter(it.from, p.pid, it.body, ReasonNoProc)
		if it.ref != 0 {
			_ = n.reply(p.pid, it.from, it.ref, reply, nil, grpcprocv1.Status_STATUS_NOPROC, "", true)
		}
	}
}

// answer answers a call of this node's on the channel its caller waits on.
// Only the call's one holder answers it (see callLocal), so the send never
// finds the channel full; were it to, a blocked send would hang an exit or
// a Stop.
func answer(ch chan<- callResult, r callResult) {
	select {
	case ch <- r:
	default:
	}
}

// deliverReply answers a call this node made to a peer.
func (n *Node) deliverReply(ref uint64, body proto.Message, status grpcprocv1.Status, errText string) {
	if pc := n.takePending(ref); pc != nil { // or the caller gave up
		pc.ch <- outcome(body, status, errText)
	}
}

func (n *Node) deliverMonitor(from, to PID, name string, ref uint64) {
	r := Ref{Node: from.Node, ID: ref}
	p := n.lookup(to, name)
	if p == nil || !p.addWatcher(r, from) {
		_ = n.down(to, from, ref, ReasonNoProc, true)
	}
}

func (n *Node) deliverDemonitor(from, to PID, name string, ref uint64) {
	if p := n.lookup(to, name); p != nil {
		p.removeWatcher(Ref{Node: from.Node, ID: ref})
	}
}

func (n *Node) deliverDown(from, to PID, ref uint64, reason string) {
	p := n.lookup(to, "")
	if p == nil {
		return
	}
	r := Ref{Node: n.id.Name, ID: ref}
	switch t, link, ok := p.dropWatch(r); {
	case !ok: // demonitored or unlinked meanwhile
	case link:
		p.exitSignal(from, t.name, reason)
	default:
		p.push(item{from: from, down: &Down{Ref: r, PID: from, Name: t.name, Reason: reason}})
	}
}

func (n *Node) deliverExit(to PID, name, reason string) {
	if p := n.lookup(to, name); p != nil {
		p.cancel(&ExitError{Reason: reason})
	}
}

func (n *Node) deadLetter(from, to PID, body proto.Message, reason string) {
	n.deadLetters.Add(1)
	if n.hooks != nil {
		n.hooks.OnDeadLetter(from, to, body, reason)
	}
	if n.subs.active() {
		n.subs.publish(Event{Kind: EventDeadLetter, From: from, To: to, Type: typeName(body), Reason: reason})
	}
	n.log.Debug("dead letter", "from", from, "to", to, "reason", reason, "type", typeName(body))
}

// dispatch handles an envelope that arrived on the inbound link from peer:
// its sender is a process of peer, its target one of this node.
func (n *Node) dispatch(peer string, env *grpcprocv1.Envelope) {
	from := PID{Node: peer, Incarnation: env.GetFromIncarnation(), ID: env.GetFromId()}
	to := PID{Node: n.id.Name, Incarnation: env.GetToIncarnation(), ID: env.GetToId()}
	name, ref := env.GetToName(), env.GetRef()
	switch env.GetKind() {
	case grpcprocv1.Kind_KIND_SEND:
		body, err := decodeBody(env)
		if err != nil {
			n.log.Warn("undecodable message", "err", err)
			n.deadLetter(from, to, nil, ReasonType)
			return
		}
		n.deliver(to, name, item{from: from, body: body, md: env.GetMetadata()}, nil)
	case grpcprocv1.Kind_KIND_CALL:
		body, err := decodeBody(env)
		if err != nil {
			n.log.Warn("undecodable call", "err", err)
			_ = n.reply(to, from, ref, nil, nil, grpcprocv1.Status_STATUS_TYPE, "", true)
			return
		}
		it := item{from: from, body: body, md: env.GetMetadata(), ref: ref}
		if t := env.GetTimeoutNanos(); t > 0 {
			it.deadline = unixNanos(time.Now().Add(time.Duration(t)))
		}
		n.deliver(to, name, it, nil)
	case grpcprocv1.Kind_KIND_REPLY:
		if to.Incarnation != n.id.Incarnation {
			// A reply to a call of an earlier incarnation of this node,
			// whose refs this one reuses: its caller is gone.
			return
		}
		var body proto.Message
		if env.GetBodyType() != "" {
			var err error
			if body, err = decodeBody(env); err != nil {
				n.deliverReply(ref, nil, grpcprocv1.Status_STATUS_TYPE, "")
				return
			}
		}
		n.deliverReply(ref, body, env.GetStatus(), env.GetReason())
	case grpcprocv1.Kind_KIND_MONITOR:
		n.deliverMonitor(from, to, name, ref)
	case grpcprocv1.Kind_KIND_DEMONITOR:
		n.deliverDemonitor(from, to, name, ref)
	case grpcprocv1.Kind_KIND_DOWN:
		n.deliverDown(from, to, ref, env.GetReason())
	case grpcprocv1.Kind_KIND_EXIT:
		n.deliverExit(to, name, env.GetReason())
	}
}

// nodeDown fails everything that depended on peer: pending calls get
// ErrNoConnection, monitors of its processes fire Down{noconnection}, links
// to them exit their processes with noconnection, and monitors and links its
// processes held on ours are dropped.
func (n *Node) nodeDown(peer string, err error) {
	n.mu.RLock()
	procs := slices.Collect(maps.Values(n.procs))
	n.mu.RUnlock()
	var failed []*pendingCall
	n.pendingMu.Lock()
	for ref, pc := range n.pending {
		if pc.node == peer {
			delete(n.pending, ref)
			failed = append(failed, pc)
		}
	}
	n.pendingMu.Unlock()
	// The cause travels with the error (a transport error, "left the
	// cluster"); LinkError matches ErrNoConnection either way.
	cause := err
	if cause == nil {
		cause = ErrNoConnection
	}
	for _, pc := range failed {
		pc.ch <- callResult{err: &LinkError{Peer: peer, Err: cause}}
	}
	for _, p := range procs {
		downs, exits := p.peerDown(peer)
		for _, d := range downs {
			p.push(item{from: d.PID, down: &d})
		}
		for _, e := range exits {
			p.exitSignal(e.PID, e.Name, e.Reason)
		}
	}
	n.log.Debug("node down", "peer", peer, "err", err)
}

// ---------- wire helpers ----------

// wire starts an envelope from a process of this node to one of the node it
// is sent to; the node names are the link's, not the envelope's.
// unixNanos is a deadline in unix nanoseconds, held to what an int64 can
// say: one past the year 2262 becomes the latest, which is as good as none
// and, unlike an overflow, still in the future.
func unixNanos(t time.Time) int64 {
	if t.After(latest) {
		return math.MaxInt64
	}
	return t.UnixNano()
}

var latest = time.Unix(0, math.MaxInt64)

func wire(kind grpcprocv1.Kind, from, to PID, name string) *grpcprocv1.Envelope {
	return &grpcprocv1.Envelope{
		Kind:            kind,
		FromIncarnation: from.Incarnation, FromId: from.ID,
		ToIncarnation: to.Incarnation, ToId: to.ID, ToName: name,
	}
}

func encodeBody(env *grpcprocv1.Envelope, m proto.Message) error {
	b, err := proto.Marshal(m)
	if err != nil {
		return fmt.Errorf("grpcproc: encode: %w", err)
	}
	env.BodyType, env.Body = typeName(m), b
	return nil
}

func decodeBody(env *grpcprocv1.Envelope) (proto.Message, error) {
	if env.GetBodyType() == "" {
		return nil, errors.New("grpcproc: empty body")
	}
	mt, err := protoregistry.GlobalTypes.FindMessageByName(protoreflect.FullName(env.GetBodyType()))
	if err != nil {
		return nil, err
	}
	m := mt.New().Interface()
	if err := proto.Unmarshal(env.GetBody(), m); err != nil {
		return nil, err
	}
	return m, nil
}

func typeName(m proto.Message) string {
	if m == nil {
		return ""
	}
	return string(m.ProtoReflect().Descriptor().FullName())
}
