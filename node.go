package golink

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"maps"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"

	golinkv1 "github.com/floatdrop/golink/proto/golink/v1"
)

// Resolver turns a node name into an address grpc can dial.
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
	// have reached its peers.
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
// member with Incarnation 0 means whichever incarnation it was.
type MemberEvent struct {
	Member Member
	Up     bool
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
	return "", fmt.Errorf("golink: unknown node %q", node)
}

// Config configures a Node. Name and Resolver are required.
type Config struct {
	// Name is the node's logical name; peers address it by this.
	Name string
	// Advertise is the address peers dial to reach this node's gRPC server.
	// It is only informational to the core (NodeInfo, Hello); a Registrar
	// publishes it.
	Advertise string
	// Incarnation distinguishes this start of the node from earlier ones.
	// Zero picks the current Unix time in nanoseconds.
	Incarnation uint64
	// Resolver maps peer names to addresses.
	Resolver Resolver
	// Registrar, if set, publishes the node on Start and withdraws it on Stop.
	Registrar Registrar
	// Membership, if set, is watched from Start: a peer that leaves the
	// cluster, or comes back as a new incarnation, has its links dropped,
	// which fires Down{noconnection} for monitors across them and fails
	// pending calls.
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
	// DialTimeout bounds the connect + handshake of a link. Default 5s.
	DialTimeout time.Duration
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

	nextID   atomic.Uint64
	nextRef  atomic.Uint64
	started  atomic.Int64                // unix nanos, 0 before Start
	withdraw func(context.Context) error // from Registrar; guarded by mu

	mu       sync.Mutex
	procs    map[uint64]*proc
	names    map[string]*proc
	pending  map[uint64]*pendingCall
	out      map[string]*outLink
	in       map[string]*inLink
	dialing  map[string]*dialOp
	dials    map[string]uint64 // per peer, for LinkInfo.Reconnects
	stopping bool              // Stop began: no new processes
	stopped  bool              // links closed: no new links
	wg       sync.WaitGroup

	spawned, exited, deadLetters atomic.Uint64
	subs                         subscribers

	golinkv1.UnimplementedNodeServer
}

type pendingCall struct {
	node string
	ch   chan callResult
}

type callResult struct {
	body proto.Message
	err  error
}

// NewNode validates cfg and returns a node that is not yet started.
func NewNode(cfg Config) (*Node, error) {
	if cfg.Name == "" {
		return nil, errors.New("golink: Config.Name is required")
	}
	if cfg.Resolver == nil {
		return nil, errors.New("golink: Config.Resolver is required")
	}
	cfg.Incarnation = cmp.Or(cfg.Incarnation, uint64(time.Now().UnixNano()))
	cfg.Logger = cmp.Or(cfg.Logger, slog.Default())
	cfg.DialTimeout = cmp.Or(cfg.DialTimeout, 5*time.Second)
	ctx, cancel := context.WithCancel(context.Background())
	n := &Node{
		cfg:     cfg,
		id:      NodeID{Name: cfg.Name, Incarnation: cfg.Incarnation},
		hooks:   cfg.Hooks,
		ctx:     ctx,
		cancel:  cancel,
		procs:   map[uint64]*proc{},
		names:   map[string]*proc{},
		pending: map[uint64]*pendingCall{},
		out:     map[string]*outLink{},
		in:      map[string]*inLink{},
		dialing: map[string]*dialOp{},
		dials:   map[string]uint64{},
	}
	n.log = cfg.Logger.With("node", cfg.Name)
	return n, nil
}

// Register mounts the golink.v1.Node service on s. Call it before the server
// starts serving.
func (n *Node) Register(s grpc.ServiceRegistrar) { golinkv1.RegisterNodeServer(s, n) }

// Name is the node's logical name.
func (n *Node) Name() string { return n.id.Name }

// ID is the node's name and incarnation.
func (n *Node) ID() NodeID { return n.id }

// PID is the node's own pseudo-process (ID 0): the sender of messages sent
// from outside any process.
func (n *Node) PID() PID { return PID{Node: n.id.Name, Incarnation: n.id.Incarnation} }

// Start watches Membership and publishes the node with its Registrar, when
// configured. Processes may be spawned before Start and run immediately;
// Start only makes the node known. A second call does nothing.
func (n *Node) Start(ctx context.Context) error {
	if !n.started.CompareAndSwap(0, time.Now().UnixNano()) {
		return nil
	}
	if m := n.cfg.Membership; m != nil {
		events, err := m.Watch(n.ctx)
		if err != nil {
			return fmt.Errorf("golink: membership: %w", err)
		}
		go n.watchMembers(events)
	}
	if r := n.cfg.Registrar; r != nil {
		withdraw, err := r.Register(ctx, Member{Name: n.id.Name, Incarnation: n.id.Incarnation, Addr: n.cfg.Advertise})
		if err != nil {
			return fmt.Errorf("golink: register: %w", err)
		}
		n.mu.Lock()
		n.withdraw = withdraw
		n.mu.Unlock()
	}
	return nil
}

func (n *Node) watchMembers(events <-chan MemberEvent) {
	for ev := range events {
		if ev.Member.Name != n.id.Name {
			n.memberEvent(ev)
		}
	}
}

// memberEvent drops the links to a peer that left, or that came back as
// another incarnation: either way, whatever crossed those links is gone.
func (n *Node) memberEvent(ev MemberEvent) {
	name := ev.Member.Name
	n.mu.Lock()
	var linked uint64
	if l := n.out[name]; l != nil {
		linked = l.peer.Incarnation
	} else if l := n.in[name]; l != nil {
		linked = l.peer.Incarnation
	}
	n.mu.Unlock()
	switch {
	case linked == 0:
		return // no link, nothing to settle
	case !ev.Up && (ev.Member.Incarnation == linked || ev.Member.Incarnation == 0):
		n.disconnect(name, errors.New("left the cluster"))
	case ev.Up && ev.Member.Incarnation != linked:
		n.disconnect(name, errors.New("restarted as incarnation "+itoa(ev.Member.Incarnation)))
	}
}

// Stop asks every process to exit (Receive returns ReasonShutdown), waits for
// them until ctx is done, then closes every link. Watchers of this node's
// processes receive Down{shutdown} while the links are still up.
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
		err = fmt.Errorf("golink: stop: %w", ctx.Err())
	}

	n.mu.Lock()
	n.stopped = true
	outs, ins := n.out, n.in
	n.out, n.in = map[string]*outLink{}, map[string]*inLink{}
	n.mu.Unlock()
	// Let the Down{shutdown} envelopes reach their peers before the links go.
	for _, l := range outs {
		l.shutdown(ctx)
	}
	for _, l := range ins {
		l.close()
	}
	n.mu.Lock()
	withdraw := n.withdraw
	n.mu.Unlock()
	if withdraw != nil {
		if werr := withdraw(ctx); werr != nil {
			err = errors.Join(err, fmt.Errorf("golink: withdraw: %w", werr))
		}
	}
	return err
}

// ---------- registry & inspection ----------

// Whereis resolves a name registered on this node.
func (n *Node) Whereis(name string) (PID, bool) {
	n.mu.Lock()
	defer n.mu.Unlock()
	if p := n.names[name]; p != nil {
		return p.pid, true
	}
	return PID{}, false
}

// Unregister removes a name binding on this node.
func (n *Node) Unregister(name string) {
	n.mu.Lock()
	defer n.mu.Unlock()
	if p := n.names[name]; p != nil {
		delete(n.names, name)
		p.dropName(name)
	}
}

// Processes snapshots every local process, ordered by PID.
func (n *Node) Processes() []ProcessInfo {
	n.mu.Lock()
	procs := slices.SortedFunc(maps.Values(n.procs), func(a, b *proc) int { return cmp.Compare(a.pid.ID, b.pid.ID) })
	n.mu.Unlock()
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
// calls Receive; ctx bounds the wait.
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
	for _, l := range n.in {
		info.Links = append(info.Links, l.info())
	}
	n.mu.Unlock()
	// Outbound links were collected first, so a stable sort by peer keeps
	// each peer's outbound link ahead of its inbound one.
	slices.SortStableFunc(info.Links, func(a, b LinkInfo) int { return cmp.Compare(a.Peer.Name, b.Peer.Name) })
	return info
}

// Disconnect drops every link with peer, as if the network had. Monitors
// across it fire Down{noconnection} and pending calls fail; the next send
// dials again. It reports whether there was a link to drop.
func (n *Node) Disconnect(peer string) bool {
	return n.disconnect(peer, errors.New("disconnected"))
}

func (n *Node) disconnect(peer string, cause error) bool {
	n.mu.Lock()
	out, in := n.out[peer], n.in[peer]
	n.mu.Unlock()
	if out == nil && in == nil {
		return false
	}
	n.connLost(peer, out, in, cause, true)
	return true
}

// Peers lists nodes with a live link in either direction.
func (n *Node) Peers() []string {
	n.mu.Lock()
	defer n.mu.Unlock()
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
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.procs[pid.ID]
}

// lookup resolves a delivery target on this node. A PID from another
// incarnation resolves to nothing.
func (n *Node) lookup(pid PID, name string) *proc {
	n.mu.Lock()
	defer n.mu.Unlock()
	if name != "" {
		return n.names[name]
	}
	if pid.Incarnation != n.id.Incarnation {
		return nil
	}
	return n.procs[pid.ID]
}

// ---------- messaging from outside a process ----------

// Send delivers m to a typed address, local or remote, with the node as
// sender. Sending to a process that does not exist is not an error (it is a
// dead letter); an error means m could not be encoded or the node could not
// be reached.
func (n *Node) Send[N proto.Message](to Addr[N], m N) error {
	return n.send(n.PID(), nil, to.dest(), m, nil)
}

// SendContext is Send with the metadata carried by ctx.
func (n *Node) SendContext[N proto.Message](ctx context.Context, to Addr[N], m N) error {
	return n.send(n.PID(), nil, to.dest(), m, MetadataFrom(ctx))
}

// SendTo delivers m to an untyped target: a PID, a Name, or an Addr of
// another type. The target's type is checked on delivery only.
func (n *Node) SendTo(to Target, m proto.Message) error {
	return n.send(n.PID(), nil, destOf(to), m, nil)
}

// Call sends req to a typed address and waits for the reply, typed as R:
//
//	resp, err := node.Call[*orderspb.Reserved](ctx, addr, &orderspb.Order{…})
//
// A reply of another type is ErrType; a handler error is a *RemoteError; a
// callee that is gone, or exits before answering, is ErrNoProc.
func (n *Node) Call[R, N proto.Message](ctx context.Context, to Addr[N], req N) (R, error) {
	return typed[R](n.doCall(ctx, n.PID(), nil, to.dest(), req, MetadataFrom(ctx)))
}

// CallTo is Call to an untyped target.
func (n *Node) CallTo[R proto.Message](ctx context.Context, to Target, req proto.Message) (R, error) {
	return typed[R](n.doCall(ctx, n.PID(), nil, destOf(to), req, MetadataFrom(ctx)))
}

// Exit asks a process anywhere to terminate with reason.
func (n *Node) Exit(to Target, reason string) error { return n.exit(n.PID(), to, reason) }

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

func (n *Node) send(from PID, sender *proc, to dest, body proto.Message, md Metadata) (err error) {
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
		n.deliver(from, pid, name, body, md, 0)
		return nil
	}
	a, err := anypb.New(body)
	if err != nil {
		return fmt.Errorf("golink: encode: %w", err)
	}
	return n.route(pid.Node, &golinkv1.Envelope{
		Kind:     &golinkv1.Envelope_Send{Send: &golinkv1.Send{From: pidTo(from), To: pidTo(pid), ToName: name, Body: a}},
		Metadata: md,
	})
}

func (n *Node) doCall(ctx context.Context, from PID, caller *proc, to dest, req proto.Message, md Metadata) (_ proto.Message, err error) {
	pid, name := to.pid, to.name
	md, done := n.hookSend(from, caller, pid, name, req, md, true)
	if done != nil {
		defer func() { done(err) }()
	}
	ref := n.nextRef.Add(1)
	pc := &pendingCall{node: pid.Node, ch: make(chan callResult, 1)}
	n.mu.Lock()
	n.pending[ref] = pc
	n.mu.Unlock()
	defer func() {
		n.mu.Lock()
		delete(n.pending, ref)
		n.mu.Unlock()
	}()
	if caller != nil {
		caller.sent.Add(1)
		caller.callsInFlight.Add(1)
		caller.setState(StateWaitingReply)
		defer func() { caller.callsInFlight.Add(-1); caller.setState(StateRunning) }()
	}

	if pid.Node == n.id.Name {
		if n.cfg.CopyLocal {
			req = proto.Clone(req)
		}
		n.deliver(from, pid, name, req, md, ref)
	} else {
		a, err := anypb.New(req)
		if err != nil {
			return nil, fmt.Errorf("golink: encode: %w", err)
		}
		err = n.route(pid.Node, &golinkv1.Envelope{
			Kind:     &golinkv1.Envelope_Call{Call: &golinkv1.Call{From: pidTo(from), To: pidTo(pid), ToName: name, Ref: ref, Body: a}},
			Metadata: md,
		})
		if err != nil {
			return nil, err
		}
	}
	select {
	case r := <-pc.ch:
		return r.body, r.err
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (n *Node) reply(from, to PID, ref uint64, body proto.Message, status golinkv1.Status, errText string) error {
	if to.Node == n.id.Name {
		n.deliverReply(ref, body, status, errText)
		return nil
	}
	var a *anypb.Any
	if body != nil {
		var err error
		if a, err = anypb.New(body); err != nil {
			return fmt.Errorf("golink: encode reply: %w", err)
		}
	}
	return n.route(to.Node, &golinkv1.Envelope{Kind: &golinkv1.Envelope_Reply{Reply: &golinkv1.Reply{
		From: pidTo(from), To: pidTo(to), Ref: ref, Status: status, Error: errText, Body: a,
	}}})
}

func (n *Node) monitor(from PID, to Target, ref uint64) error {
	pid, name := to.target()
	if pid.Node == n.id.Name {
		n.deliverMonitor(from, pid, name, ref)
		return nil
	}
	return n.route(pid.Node, &golinkv1.Envelope{Kind: &golinkv1.Envelope_Monitor{Monitor: &golinkv1.Monitor{
		From: pidTo(from), To: pidTo(pid), ToName: name, Ref: ref,
	}}})
}

func (n *Node) demonitor(from PID, to Target, ref uint64) error {
	pid, name := to.target()
	if pid.Node == n.id.Name {
		n.deliverDemonitor(from, pid, name, ref)
		return nil
	}
	return n.route(pid.Node, &golinkv1.Envelope{Kind: &golinkv1.Envelope_Demonitor{Demonitor: &golinkv1.Demonitor{
		From: pidTo(from), To: pidTo(pid), ToName: name, Ref: ref,
	}}})
}

func (n *Node) down(from, to PID, ref uint64, reason string) error {
	if to.Node == n.id.Name {
		n.deliverDown(from, to, ref, reason)
		return nil
	}
	return n.route(to.Node, &golinkv1.Envelope{Kind: &golinkv1.Envelope_Down{Down: &golinkv1.Down{
		From: pidTo(from), To: pidTo(to), Ref: ref, Reason: reason,
	}}})
}

func (n *Node) exit(from PID, to Target, reason string) error {
	pid, name := to.target()
	if pid.Node == n.id.Name {
		n.deliverExit(pid, name, reason)
		return nil
	}
	return n.route(pid.Node, &golinkv1.Envelope{Kind: &golinkv1.Envelope_Exit{Exit: &golinkv1.Exit{
		From: pidTo(from), To: pidTo(pid), ToName: name, Reason: reason,
	}}})
}

// route queues env on the link to node, dialing it if needed.
func (n *Node) route(node string, env *golinkv1.Envelope) error {
	if node == "" {
		return errors.New("golink: empty destination node")
	}
	l, err := n.getOut(node)
	if err != nil {
		return err
	}
	return l.send(env)
}

// ---------- local delivery; also the sink for decoded inbound envelopes ----------

func (n *Node) deliver(from, to PID, name string, body proto.Message, md Metadata, ref uint64) {
	p := n.lookup(to, name)
	if p == nil {
		n.deadLetter(from, to, body, ReasonNoProc)
		if ref != 0 {
			_ = n.reply(to, from, ref, nil, golinkv1.Status_STATUS_NOPROC, "")
		}
		return
	}
	if !p.accept(body) {
		n.deadLetter(from, p.pid, body, ReasonType)
		if ref != 0 {
			_ = n.reply(p.pid, from, ref, nil, golinkv1.Status_STATUS_TYPE, "")
		}
		return
	}
	it := item{from: from, body: body, md: md, ref: ref}
	if n.hooks != nil {
		it.at = time.Now().UnixNano() // for OnReceive's exact wait
	}
	if !p.push(it) {
		n.deadLetter(from, p.pid, body, ReasonNoProc)
		if ref != 0 {
			_ = n.reply(p.pid, from, ref, nil, golinkv1.Status_STATUS_NOPROC, "")
		}
	}
}

func (n *Node) deliverReply(ref uint64, body proto.Message, status golinkv1.Status, errText string) {
	n.mu.Lock()
	pc := n.pending[ref]
	delete(n.pending, ref)
	n.mu.Unlock()
	if pc == nil {
		return // the caller gave up
	}
	switch status {
	case golinkv1.Status_STATUS_OK:
		pc.ch <- callResult{body: body}
	case golinkv1.Status_STATUS_NOPROC:
		pc.ch <- callResult{err: ErrNoProc}
	case golinkv1.Status_STATUS_TYPE:
		pc.ch <- callResult{err: ErrType}
	default:
		pc.ch <- callResult{body: body, err: &RemoteError{Msg: errText}}
	}
}

func (n *Node) deliverMonitor(from, to PID, name string, ref uint64) {
	r := Ref{Node: from.Node, ID: ref}
	p := n.lookup(to, name)
	if p == nil || !p.addWatcher(r, from) {
		_ = n.down(to, from, ref, ReasonNoProc)
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
	if t, ok := p.dropMonitor(r); ok {
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

// dispatch handles an envelope that arrived on an inbound link.
func (n *Node) dispatch(env *golinkv1.Envelope) {
	switch k := env.Kind.(type) {
	case *golinkv1.Envelope_Send:
		body, err := decode(k.Send.Body)
		if err != nil {
			n.log.Warn("undecodable message", "err", err)
			n.deadLetter(pidFrom(k.Send.From), pidFrom(k.Send.To), nil, ReasonType)
			return
		}
		n.deliver(pidFrom(k.Send.From), pidFrom(k.Send.To), k.Send.ToName, body, env.Metadata, 0)
	case *golinkv1.Envelope_Call:
		body, err := decode(k.Call.Body)
		if err != nil {
			n.log.Warn("undecodable call", "err", err)
			_ = n.reply(pidFrom(k.Call.To), pidFrom(k.Call.From), k.Call.Ref, nil, golinkv1.Status_STATUS_TYPE, "")
			return
		}
		n.deliver(pidFrom(k.Call.From), pidFrom(k.Call.To), k.Call.ToName, body, env.Metadata, k.Call.Ref)
	case *golinkv1.Envelope_Reply:
		var body proto.Message
		if k.Reply.Body != nil {
			var err error
			if body, err = decode(k.Reply.Body); err != nil {
				n.deliverReply(k.Reply.Ref, nil, golinkv1.Status_STATUS_TYPE, "")
				return
			}
		}
		n.deliverReply(k.Reply.Ref, body, k.Reply.Status, k.Reply.Error)
	case *golinkv1.Envelope_Monitor:
		n.deliverMonitor(pidFrom(k.Monitor.From), pidFrom(k.Monitor.To), k.Monitor.ToName, k.Monitor.Ref)
	case *golinkv1.Envelope_Demonitor:
		n.deliverDemonitor(pidFrom(k.Demonitor.From), pidFrom(k.Demonitor.To), k.Demonitor.ToName, k.Demonitor.Ref)
	case *golinkv1.Envelope_Down:
		n.deliverDown(pidFrom(k.Down.From), pidFrom(k.Down.To), k.Down.Ref, k.Down.Reason)
	case *golinkv1.Envelope_Exit:
		n.deliverExit(pidFrom(k.Exit.To), k.Exit.ToName, k.Exit.Reason)
	}
}

// nodeDown fails everything that depended on peer: pending calls get
// ErrNoConnection, monitors of its processes fire Down{noconnection}, and
// monitors its processes held on ours are dropped.
func (n *Node) nodeDown(peer string, err error) {
	n.mu.Lock()
	procs := make([]*proc, 0, len(n.procs))
	for _, p := range n.procs {
		procs = append(procs, p)
	}
	var failed []*pendingCall
	for ref, pc := range n.pending {
		if pc.node == peer {
			delete(n.pending, ref)
			failed = append(failed, pc)
		}
	}
	n.mu.Unlock()
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
		for _, d := range p.peerDown(peer) {
			p.push(item{from: d.PID, down: &d})
		}
	}
	n.log.Debug("node down", "peer", peer, "err", err)
}

// ---------- proto helpers ----------

func pidTo(p PID) *golinkv1.PID {
	return &golinkv1.PID{Node: p.Node, Incarnation: p.Incarnation, Id: p.ID}
}

func pidFrom(p *golinkv1.PID) PID {
	if p == nil {
		return PID{}
	}
	return PID{Node: p.Node, Incarnation: p.Incarnation, ID: p.Id}
}

func decode(a *anypb.Any) (proto.Message, error) {
	if a == nil {
		return nil, errors.New("golink: empty body")
	}
	return a.UnmarshalNew()
}

func typeName(m proto.Message) string {
	if m == nil {
		return ""
	}
	return string(m.ProtoReflect().Descriptor().FullName())
}
