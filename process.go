package grpcproc

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"maps"
	"reflect"
	"runtime/debug"
	"sync"
	"sync/atomic"
	"time"

	"google.golang.org/protobuf/proto"

	grpcprocv1 "github.com/floatdrop/grpcproc/proto/grpcproc/v1"
)

// item is what sits in a mailbox: a message, a call, a Down or an Exited.
type item struct {
	from   PID
	body   proto.Message // nil for a Down or an Exited
	down   *Down
	exited *Exited
	md     Metadata
	ref    uint64 // call ref; 0 for a plain message
	// watch is set on a call whose caller asked for a watch, with
	// CallMonitor or CallLink: see WatchedBy.
	watch bool
	at    int64 // unix nanos, when it was queued; only when hooks want the wait
	// deadline is when a call's caller stops waiting for the reply, in unix
	// nanos; 0 for a caller with no deadline, and for a plain message.
	deadline int64
}

type inspectReq struct {
	ch chan map[string]string
}

// proc is the untyped half of a process; Process[M] is a typed view of it.
type proc struct {
	n       *Node
	pid     PID
	name    string // WithName; fixed at spawn
	label   string
	typ     string
	parent  PID
	mbox    *queue[item]
	sys     chan inspectReq
	inspect func() map[string]string
	accept  func(proto.Message) bool

	// current is the metadata of the message being handled, after
	// OnReceive: what the process's own sends inherit. handling ends what
	// OnReceive started. Both are touched only by the process's goroutine,
	// but current is read by sends that user code may make from others.
	current  atomic.Pointer[Metadata]
	handling Done
	ctx      context.Context
	cancel   context.CancelCauseFunc
	started  time.Time
	log      *slog.Logger

	received, sent, wakeups atomic.Uint64
	callsInFlight           atomic.Int32
	state                   atomic.Uint32
	lastMsg                 atomic.Pointer[string]
	level                   atomic.Int64
	levelSet                atomic.Bool
	trapExit                atomic.Bool

	// mu guards what follows. Taken inside Node.mu, never around it; two
	// processes' locks are held at once only under Node.mu held exclusively
	// (see WatchedBy).
	mu       sync.Mutex
	exited   bool
	watchers map[Ref]PID           // who monitors me, or is linked to me
	monitors map[Ref]monitorTarget // whom I monitor
	links    map[Ref]monitorTarget // whom I am linked to: one per target
	awaiting map[Ref]*awaited      // watches my calls asked for, until they are answered
	open     map[openCall]answerTo // calls queued or taken, not yet answered
	timers   map[*Timer]struct{}   // SendAfter timers not yet fired
}

type openCall struct {
	from PID
	ref  uint64
}

// answerTo is how an open call is answered: ch is what a caller of this node
// waits on, nil for a peer's; watched is the process its caller's watch was
// placed on, if it was (see WatchedBy), which the answer names.
type answerTo struct {
	ch      chan<- callResult
	watched PID
}

// awaited is a watch a call of the process asked for, with CallMonitor or
// CallLink, until the call has its answer, which says what it watches.
type awaited struct {
	node string // the callee's, where the watched process runs
	link bool
	// watched is the process the answer names, set just before the answer
	// reaches the caller: the answer itself carries no more than a call's.
	watched PID
	// down is set by a Down that came before the answer: the watched process
	// exited at once, or node became unreachable. It waits for the answer,
	// which says whether the call worked, and so whether it is delivered.
	down   bool
	reason string
}

type monitorTarget struct {
	pid  PID
	name string
}

// target makes a monitorTarget the Target it was placed on: a name, with
// only its node in pid, or a PID.
func (t monitorTarget) target() (PID, string) { return t.pid, t.name }

// SpawnOption configures Node.Spawn, Process.Spawn and Process.SpawnMonitor.
type SpawnOption func(*spawnOpts)

type spawnOpts struct {
	name                  string
	label                 string
	inspect               func() map[string]string
	linkParent, linkChild bool
	watchedBy             *heldCall
}

// WithName registers the process under name on its node before it runs.
// The name is held until the process exits; while another process holds it,
// spawning fails with ErrNameTaken.
func WithName(name string) SpawnOption { return func(o *spawnOpts) { o.name = name } }

// WithLabel tags the process with a low-cardinality label, the key metrics
// aggregate by. Defaults to the type of M.
func WithLabel(label string) SpawnOption { return func(o *spawnOpts) { o.label = label } }

// WithInspect lets the process publish what it currently believes. fn runs
// on the process's own goroutine, inside Receive, so it may read the
// process's state without locking. A panic in fn is the process's own: it
// exits with reason "panic: …", and Inspect returns ErrNoProc. See
// Node.Inspect.
func WithInspect(fn func() map[string]string) SpawnOption {
	return func(o *spawnOpts) { o.inspect = fn }
}

// LinkParent links the child to the process that spawns it, before the child
// runs: when the parent exits, the child does too, with the parent's reason,
// or receives an Exited if it traps exits. See Process.Link. It is for
// Process.Spawn and SpawnMonitor: Node.Spawn, which has no parent, refuses it.
func LinkParent() SpawnOption { return func(o *spawnOpts) { o.linkParent = true } }

// LinkChild links the spawning process to the child, before the child runs:
// when the child exits, the parent does too, with the child's reason, or
// receives an Exited if it traps exits. With LinkParent as well, the two are
// linked both ways, as Erlang's spawn_link does. Node.Spawn refuses it.
func LinkChild() SpawnOption { return func(o *spawnOpts) { o.linkChild = true } }

// WatchedBy places the watch m's caller asked for, a monitor or a link (see
// Addr.CallMonitor and Addr.CallLink), on the child, before it runs, as
// SpawnMonitor places its monitor: however soon the child exits, the caller
// hears of it with the real reason, never noproc. The answer to m names the
// child to the caller, and brings the watch only if it is not an error: a call
// that fails leaves no watch behind. For a caller that asked for none,
// WatchedBy does nothing.
//
// m must be a call a process of the spawning node holds and has not answered
// yet, whose watch is not placed yet (see Msg.Watch); otherwise the spawn
// fails, with ErrNotCall for a message that is not a call.
func WatchedBy[M proto.Message](m Msg[M]) SpawnOption {
	h := m.held()
	return func(o *spawnOpts) {
		if h.c.ref == 0 || h.watch {
			o.watchedBy = &h
		}
	}
}

// heldCall is a call as WatchedBy and Msg.Watch see it: who made it, its ref,
// which is its watch's too, and the process that holds it.
type heldCall struct {
	c     openCall
	watch bool
	taker *proc
}

// check is what makes a call's watch impossible to place on a process of n
// before any lock is taken.
func (h *heldCall) check(n *Node) error {
	switch {
	case h.c.ref == 0:
		return ErrNotCall
	case h.taker.n != n:
		return errHeldElsewhere
	}
	return nil
}

var (
	errNoParent      = errors.New("grpcproc: LinkParent and LinkChild need a parent: spawn with Process.Spawn")
	errHeldElsewhere = errors.New("grpcproc: the call's watch goes on a process of the node that holds it")
	errAnswered      = errors.New("grpcproc: the call is answered already, so its watch cannot be placed")
	errWatchPlaced   = errors.New("grpcproc: the call's watch is placed already")
)

func parentPID(p *proc) PID {
	if p == nil {
		return PID{}
	}
	return p.pid
}

// Process is a goroutine with a mailbox of M and a cluster-wide PID.
//
// Receive and ReceiveTimeout belong to the process's own goroutine. Other
// goroutines may send and call as the process, ask another process to exit,
// and use its PID, Addr, Node, Context and Log; a message's Reply works from
// anywhere. A send from another goroutine inherits the metadata of whatever
// message the process is handling at that moment, and a Call made there
// shows the process as waiting on a reply, so a goroutine that feeds a
// process, one blocked on a read say, sends to it as the node instead:
//
//	_ = p.Addr().Send(ctx, p.Node(), m)
type Process[M proto.Message] struct{ *proc }

// Msg is what Receive returns: a message (Body), a Down or an Exited, one
// of them only.
type Msg[M proto.Message] struct {
	From     PID
	Body     M
	Down     *Down
	Exited   *Exited
	Metadata Metadata
	ref      uint64
	watch    bool  // the caller asked for a watch: see WatchedBy
	taker    *proc // for a call: the process that took it, which holds it until it is answered
	deadline int64 // unix nanos; see Deadline
}

// IsCall reports whether the sender waits for a Reply.
func (m Msg[M]) IsCall() bool { return m.ref != 0 }

// Deadline reports when the caller of a call stops waiting for its Reply:
// the deadline of the context it called with. ok is false for a plain
// message and for a call whose context had no deadline. A remote call
// carries the time its caller had left, so its deadline counts from when it
// reached this node, a little after the caller's own; clocks need not agree.
func (m Msg[M]) Deadline() (deadline time.Time, ok bool) {
	if m.deadline == 0 {
		return time.Time{}, false
	}
	return time.Unix(0, m.deadline), true
}

// Context returns a context for code run on behalf of the message: it
// carries the message's metadata, for a send, a call, or any code taking a
// ctx, and for a call with a Deadline it ends then, when nobody waits for
// the reply any more. Call cancel once that code is done, as with
// context.WithDeadline.
func (m Msg[M]) Context(parent context.Context) (context.Context, context.CancelFunc) {
	ctx := parent
	if len(m.Metadata) > 0 {
		ctx = WithMetadata(parent, m.Metadata)
	}
	if d, ok := m.Deadline(); ok {
		return context.WithDeadline(ctx, d)
	}
	return ctx, func() {}
}

// Reply answers the message, if IsCall is true. The message holds all it
// takes, so a reply can wait: m can be kept, or handed to another goroutine
// or process, and answered from there. A call is answered once: a second
// Reply is dropped, and so is one after the process that received m exited
// (which answers ErrNoProc) or its node's Stop gave up on it
// (ErrNodeStopped). An error answer takes back the watch the caller asked
// for, if it was placed (see WatchedBy).
func (m Msg[M]) Reply(resp proto.Message, err error) error {
	if m.ref == 0 {
		return ErrNotCall
	}
	// The call belongs to the process that received it, whoever answers:
	// whoever takes it from that process's open calls answers it, from that
	// process's node, the way the call came in.
	t, c := m.taker, openCall{m.From, m.ref}
	t.mu.Lock()
	a := t.open[c]
	delete(t.open, c)
	t.mu.Unlock()
	if err != nil {
		return t.n.reply(t.pid, m.From, m.ref, a, resp, grpcprocv1.Status_STATUS_ERROR, err.Error(), false)
	}
	return t.n.reply(t.pid, m.From, m.ref, a, resp, grpcprocv1.Status_STATUS_OK, "", false)
}

// Watch places the watch m's caller asked for, a monitor or a link (see
// Addr.CallMonitor and Addr.CallLink), on pid, a process of this node that
// runs: for a call answered with a process that was running already, as
// WatchedBy places it on one spawned for the call. From then on, the caller
// hears of pid's exit, if the answer to m is not an error. For a caller that
// asked for none, Watch does nothing.
//
// It is ErrNotCall for a message that is not a call, and ErrNoProc when pid
// has exited, or is not of the node that holds m. A call answered already, or
// whose watch is placed already, is an error too.
func (m Msg[M]) Watch(pid PID) error {
	switch {
	case m.ref == 0:
		return ErrNotCall
	case !m.watch:
		return nil
	}
	t := m.taker
	n := t.n
	n.mu.Lock()
	defer n.mu.Unlock()
	w := n.procs[pid.ID]
	if w == nil || w.pid != pid {
		return ErrNoProc
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	if w != t {
		w.mu.Lock()
		defer w.mu.Unlock()
	}
	return t.watchFor(openCall{m.From, m.ref}, w)
}

func (m Msg[M]) held() heldCall {
	return heldCall{c: openCall{m.From, m.ref}, watch: m.watch, taker: m.taker}
}

// Spawn starts fn as a process that accepts messages of type M. The process
// ends when fn returns; nil means exit reason "normal", otherwise the error
// text is the reason. A panic is recovered, logged with its stack, and
// becomes reason "panic: …".
//
// M may be a concrete type (one contract per process), an interface the
// accepted types implement, or proto.Message for an untyped process.
func (n *Node) Spawn[M proto.Message](fn func(*Process[M]) error, opts ...SpawnOption) (Addr[M], error) {
	a, _, err := spawn(n, fn, opts, nil, false)
	return a, err
}

// Spawn starts fn as a process on p's node, like Node.Spawn, with p
// recorded as its parent. By itself that is for inspection: p does not
// monitor the child, and neither exits when the other does, unless
// LinkParent or LinkChild links them. A process that has exited spawns
// nothing: the error is ErrNoProc.
func (p *Process[M]) Spawn[N proto.Message](fn func(*Process[N]) error, opts ...SpawnOption) (Addr[N], error) {
	a, _, err := spawn(p.n, fn, opts, p.proc, false)
	return a, err
}

// SpawnMonitor is Spawn, with the child monitored from p before it runs:
// however soon the child exits, p receives its Down with the real reason,
// never noproc. It is Erlang's spawn_monitor, and what a supervisor needs.
// A process that has exited spawns nothing: the error is ErrNoProc.
func (p *Process[M]) SpawnMonitor[N proto.Message](fn func(*Process[N]) error, opts ...SpawnOption) (Addr[N], Ref, error) {
	return spawn(p.n, fn, opts, p.proc, true)
}

// spawn starts fn on n. parent, when set, is recorded as the child's parent,
// and monitors the child from before it runs if monitor is set; so are the
// links LinkParent and LinkChild ask for, and the watch WatchedBy places.
func spawn[M proto.Message](n *Node, fn func(*Process[M]) error, opts []SpawnOption, parent *proc, monitor bool) (Addr[M], Ref, error) {
	monitor = monitor && parent != nil
	var o spawnOpts
	for _, opt := range opts {
		opt(&o)
	}
	if parent == nil && (o.linkParent || o.linkChild) {
		return Addr[M]{}, Ref{}, errNoParent
	}
	// The process holding the call WatchedBy places a watch for is locked
	// beside the parent, when it is another one.
	var taker *proc
	if w := o.watchedBy; w != nil {
		if err := w.check(n); err != nil {
			return Addr[M]{}, Ref{}, err
		}
		if w.taker != parent {
			taker = w.taker
		}
	}
	typ := typeString[M]()
	if o.label == "" {
		o.label = typ
	}
	id := n.nextID.Add(1)
	ctx, cancel := context.WithCancelCause(n.ctx)
	p := &proc{
		n:       n,
		pid:     PID{Node: n.id.Name, Incarnation: n.id.Incarnation, ID: id},
		name:    o.name,
		label:   o.label,
		typ:     typ,
		parent:  parentPID(parent),
		mbox:    newQueue[item](true),
		sys:     make(chan inspectReq),
		inspect: o.inspect,
		accept:  func(m proto.Message) bool { _, ok := m.(M); return ok },
		ctx:     ctx,
		cancel:  cancel,
		started: time.Now(),
	}
	p.log = slog.New(&levelHandler{h: n.log.Handler(), p: p}).With(
		"pid", p.pid.String(), "label", o.label)

	// One critical section admits the child: under n.mu, with the parent's
	// lock inside it (n.mu first, then a process's lock: the one order), a
	// parent's exit either precedes the spawn, which then fails, or finds the
	// child registered and its monitor and links in place. They exist before
	// the child runs, so no exit on either side is missed. So does the watch
	// of a call's caller, which the call's answer then names.
	n.mu.Lock()
	if parent != nil {
		parent.mu.Lock()
	}
	if taker != nil {
		taker.mu.Lock()
	}
	var err error
	switch {
	case parent != nil && parent.exited:
		err = ErrNoProc // a process that has exited spawns nothing
	case n.stopping:
		err = ErrNodeStopped
	case o.name != "" && n.names[o.name] != nil:
		err = ErrNameTaken
	case o.watchedBy != nil:
		err = o.watchedBy.taker.watchFor(o.watchedBy.c, p) // p runs nowhere yet: it needs no lock
	}
	if taker != nil {
		taker.mu.Unlock()
	}
	var ref Ref
	if err == nil && parent != nil {
		watch := func(watched, watcher *proc, into *map[Ref]monitorTarget) Ref {
			r := Ref{Node: n.id.Name, ID: n.nextRef.Add(1)}
			if *into == nil {
				*into = map[Ref]monitorTarget{}
			}
			(*into)[r] = monitorTarget{pid: watched.pid}
			if watched.watchers == nil {
				watched.watchers = map[Ref]PID{}
			}
			watched.watchers[r] = watcher.pid
			return r
		}
		if monitor {
			ref = watch(p, parent, &parent.monitors)
		}
		if o.linkChild {
			watch(p, parent, &parent.links)
		}
		if o.linkParent {
			watch(parent, p, &p.links)
		}
	}
	if parent != nil {
		parent.mu.Unlock()
	}
	if err != nil {
		n.mu.Unlock()
		cancel(nil)
		return Addr[M]{}, Ref{}, err
	}
	if o.name != "" {
		n.names[o.name] = p
	}
	n.procs[id] = p
	n.wg.Add(1)
	n.mu.Unlock()
	n.spawned.Add(1)
	if n.hooks != nil || n.subs.active() {
		info := p.info()
		if n.hooks != nil {
			n.hooks.OnSpawn(info)
		}
		n.subs.publish(Event{Kind: EventSpawn, Process: info})
	}

	go p.run(func() error { return fn(&Process[M]{p}) })
	return Addr[M]{pid: p.pid}, ref, nil
}

// ---------- Process[M] API ----------

// PID returns the process's PID.
func (p *proc) PID() PID { return p.pid }

// Node returns the node hosting the process.
func (p *proc) Node() *Node { return p.n }

// Addr returns the process's typed address.
func (p *Process[M]) Addr() Addr[M] { return Addr[M]{pid: p.pid} }

// Context is cancelled when the process is asked to exit, a process it is
// linked to exits, or its node stops. context.Cause is then an *ExitError,
// or context.Canceled.
func (p *proc) Context() context.Context { return p.ctx }

// Log returns a logger with the process's pid and label attached. Its
// threshold can be changed at runtime with Node.SetLogLevel.
func (p *proc) Log() *slog.Logger { return p.log }

// Receive blocks until a message, a Down or an Exited arrives, or the process
// is told to exit. The error is then context.Canceled (node stop) or an
// *ExitError: after Exit, or the exit of a process it is linked to.
func (p *Process[M]) Receive() (Msg[M], error) {
	it, err := p.receive(p.ctx)
	return toMsg[M](p.proc, it), err
}

// ReceiveTimeout is Receive with a deadline; context.DeadlineExceeded on timeout.
func (p *Process[M]) ReceiveTimeout(d time.Duration) (Msg[M], error) {
	ctx, cancel := context.WithTimeout(p.ctx, d)
	defer cancel()
	it, err := p.receive(ctx)
	if err != nil && p.ctx.Err() != nil {
		err = context.Cause(p.ctx)
	}
	return toMsg[M](p.proc, it), err
}

func toMsg[M proto.Message](taker *proc, it item) Msg[M] {
	m := Msg[M]{From: it.from, Down: it.down, Exited: it.exited, Metadata: it.md, ref: it.ref, watch: it.watch, deadline: it.deadline}
	if it.ref != 0 {
		m.taker = taker
	}
	if it.body != nil {
		m.Body, _ = it.body.(M) // accept() checked this on delivery
	}
	return m
}

// SendTo delivers msg from the process to an untyped target, such as a Msg's
// From, as Addr.Send does to a typed one, carrying the metadata of the
// message the process is handling. A first send to a node with no link yet
// waits for the dial, up to Config.DialTimeout, unless dials to it are
// failing (Config.DialBackoff). The target's type is checked on delivery
// only.
//
// It takes no ctx, as Exit takes none: a process carries its own, the
// metadata it inherits and the node's DialTimeout. Its own Context would be
// no better, since it ends as the process is asked to exit, when a process
// may still have a send to make. For a ctx of its own, a process sends
// through the address: AddrOf[proto.Message](to).Send(ctx, p, msg).
func (p *proc) SendTo(to Target, msg proto.Message) error {
	return p.n.send(context.Background(), p.pid, p, destOf(to), msg, p.outgoing(nil))
}

// CallTo calls an untyped target, such as a Msg's From, from the process, as
// Addr.Call does a typed one, and types the reply as R.
func (p *Process[M]) CallTo[R proto.Message](ctx context.Context, to Target, req proto.Message) (R, error) {
	return typed[R](p.n.doCall(ctx, p.pid, p.proc, destOf(to), req, p.outgoing(MetadataFrom(ctx)), 0))
}

// SendAfter sends m to a typed address after d, as this process. The timer
// belongs to the process: it is cancelled if the process exits first. The
// message carries the metadata the process holds now, when it is scheduled,
// not whatever it is handling when the timer fires.
func (p *Process[M]) SendAfter[N proto.Message](d time.Duration, to Addr[N], m N) *Timer {
	return p.sendAfter(d, to.dest(), m)
}

// Timer is a message scheduled with SendAfter.
type Timer struct {
	p *proc
	t *time.Timer
}

// Stop cancels the send. It reports whether it did: false if the message
// was already sent, or the process has exited.
func (t *Timer) Stop() bool {
	if t.t == nil {
		return false
	}
	t.p.mu.Lock()
	_, pending := t.p.timers[t]
	delete(t.p.timers, t)
	t.p.mu.Unlock()
	t.t.Stop()
	return pending
}

func (p *proc) sendAfter(d time.Duration, to dest, m proto.Message) *Timer {
	md := p.outgoing(nil)
	tm := &Timer{p: p}
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.exited {
		return tm
	}
	if p.timers == nil {
		p.timers = map[*Timer]struct{}{}
	}
	p.timers[tm] = struct{}{}
	tm.t = time.AfterFunc(d, func() {
		p.mu.Lock()
		_, pending := p.timers[tm]
		delete(p.timers, tm)
		p.mu.Unlock()
		if pending {
			_ = p.n.send(context.Background(), p.pid, p, to, m, md)
		}
	})
	return tm
}

// Exit asks another process, anywhere, to terminate with reason. A first
// exit to a node with no link yet waits for the dial as SendTo does, and,
// as SendTo, it takes no ctx. It is not trapped: a process that traps exits
// ends all the same.
func (p *proc) Exit(to Target, reason string) error {
	return p.n.exit(context.Background(), p.pid, to, reason)
}

// Link links p to target, one way: when target exits, p exits too, with
// target's reason, whatever it is, normal included. If p traps exits (see
// SetTrapExit), it receives a Msg with Exited set instead. A target that does
// not exist ends p with noproc, and one whose node cannot be reached, now or
// later, with noconnection. p's own exit does not affect target, which can
// link to p for that.
//
// A second link to the same target is the same link. Linking to itself, or
// from a process that has exited, does nothing.
func (p *proc) Link(target Target) {
	pid, name := target.target()
	if name == "" && pid == p.pid || name != "" && name == p.name && pid.Node == p.n.id.Name {
		return // itself
	}
	t := monitorTarget{pid: pid, name: name}
	ref := Ref{Node: p.n.id.Name, ID: p.n.nextRef.Add(1)}
	p.mu.Lock()
	if _, linked := p.linkTo(t); linked || p.exited {
		p.mu.Unlock()
		return
	}
	if p.links == nil {
		p.links = map[Ref]monitorTarget{}
	}
	p.links[ref] = t
	p.mu.Unlock()
	// On the wire a link is a monitor: only this node tells its Down from a
	// monitor's, so peers need nothing new.
	p.placeWatch(target, ref)
}

// Unlink removes p's link to target. An Exited already in the mailbox stays
// there.
func (p *proc) Unlink(target Target) {
	pid, name := target.target()
	t := monitorTarget{pid: pid, name: name}
	p.mu.Lock()
	ref, linked := p.linkTo(t)
	delete(p.links, ref)
	p.mu.Unlock()
	if linked {
		_ = p.n.demonitor(p.pid, t, ref.ID)
	}
}

// linkTo finds p's link to t. Called with p.mu held.
func (p *proc) linkTo(t monitorTarget) (Ref, bool) {
	for ref, l := range p.links {
		if l == t {
			return ref, true
		}
	}
	return Ref{}, false
}

// SetTrapExit sets whether p traps exits: whether the exit of a process it
// is linked to reaches it as a Msg with Exited set, rather than ending it.
// It takes effect for exits that arrive from then on. A request to exit,
// Process.Exit or Node.Exit, is never trapped.
func (p *proc) SetTrapExit(trap bool) { p.trapExit.Store(trap) }

// TrapExit reports whether p traps exits.
func (p *proc) TrapExit() bool { return p.trapExit.Load() }

// Parent returns the process that spawned p with Process.Spawn or
// SpawnMonitor, or the zero PID.
func (p *proc) Parent() PID { return p.parent }

// Monitor watches target. When it exits, or its node becomes unreachable,
// this process receives a Msg with Down set and the returned Ref. Monitoring
// a process that does not exist yields Down{Reason: "noproc"}.
func (p *proc) Monitor(target Target) Ref {
	pid, name := target.target()
	ref := Ref{Node: p.n.id.Name, ID: p.n.nextRef.Add(1)}
	p.mu.Lock()
	if p.exited {
		p.mu.Unlock()
		return ref
	}
	if p.monitors == nil {
		p.monitors = map[Ref]monitorTarget{}
	}
	p.monitors[ref] = monitorTarget{pid: pid, name: name}
	p.mu.Unlock()
	p.placeWatch(target, ref)
	return ref
}

// placeWatch sends p's monitor of target, or its link, which is the same on
// the wire. One that cannot be sent is Down at once, with noconnection. If p
// exited while it was on its way, p's exit may have taken it back before it
// arrived, and it would stay on target: it is taken back again, after it.
func (p *proc) placeWatch(target Target, ref Ref) {
	pid, _ := target.target()
	if err := p.n.monitor(p.pid, target, ref.ID); err != nil {
		p.n.deliverDown(pid, p.pid, ref.ID, ReasonNoConnection)
		return
	}
	p.mu.Lock()
	exited := p.exited
	p.mu.Unlock()
	if exited {
		_ = p.n.demonitor(p.pid, target, ref.ID)
	}
}

// Demonitor stops a monitor. A Down already in the mailbox stays there.
func (p *proc) Demonitor(ref Ref) {
	p.mu.Lock()
	t, ok := p.monitors[ref]
	delete(p.monitors, ref)
	p.mu.Unlock()
	if ok {
		_ = p.n.demonitor(p.pid, t, ref.ID)
	}
}

// ---------- internals ----------

func (p *proc) push(it item) bool {
	return p.mbox.push(it)
}

// queueCall queues a call, which from then on is in open, the process's to
// answer: by Reply, which takes it from there, by its exit, or by Stop, if
// the process outlives it. reply is what a caller of this node waits on. It
// reports false, and the caller answers, if the process has exited.
func (p *proc) queueCall(it item, reply chan<- callResult) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	// The exit sets exited, under p.mu, before it closes the mailbox: a call
	// queued here is in the open calls the exit answers.
	if p.exited || !p.push(it) {
		return false
	}
	if p.open == nil {
		p.open = map[openCall]answerTo{}
	}
	p.open[openCall{it.from, it.ref}] = answerTo{ch: reply}
	return true
}

// failLocalCalls answers the open calls of this node's callers with err, for
// Stop, when the process outlives it. A later Reply finds them gone.
func (p *proc) failLocalCalls(err error) {
	failed := map[openCall]answerTo{}
	p.mu.Lock()
	for c, a := range p.open {
		if a.ch != nil { // a peer's caller hears of it from its link
			failed[c] = a
			delete(p.open, c)
		}
	}
	p.mu.Unlock()
	for c, a := range failed {
		p.n.unwatch(c, a.watched)
		answer(a.ch, callResult{err: err})
	}
}

// watchFor places the watch the open call c asked for on w, for c's caller,
// and records it on the call, whose answer names w. Called with n.mu held
// exclusively, and the locks of p, which holds c, and of w, unless w runs
// nowhere yet.
func (p *proc) watchFor(c openCall, w *proc) error {
	a, open := p.open[c]
	switch {
	case !open:
		return errAnswered
	case !a.watched.IsZero():
		return errWatchPlaced
	case w.exited:
		return ErrNoProc
	}
	if w.watchers == nil {
		w.watchers = map[Ref]PID{}
	}
	w.watchers[Ref{Node: c.from.Node, ID: c.ref}] = c.from
	a.watched = w.pid
	p.open[c] = a
	return nil
}

// awaitWatch records the watch a call of p asks for under ref, before the
// call leaves, so that a Down that comes before the answer waits for it. It
// reports false if p has exited.
func (p *proc) awaitWatch(ref Ref, node string, link bool) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.exited {
		return false
	}
	if p.awaiting == nil {
		p.awaiting = map[Ref]*awaited{}
	}
	p.awaiting[ref] = &awaited{node: node, link: link}
	return true
}

// noteWatched records that the answer to p's call under ref names watched,
// the process the callee placed the watch on.
func (p *proc) noteWatched(ref Ref, watched PID) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if a := p.awaiting[ref]; a != nil {
		a.watched = watched
	}
}

// settleWatch settles the watch a call of p asked for under ref, once the
// call has its answer: ok if the call worked. A call that worked leaves p
// watching the process its answer names from then on, and a Down that came
// before the answer is delivered now, after it; a callee that placed none
// makes it Down at once, with noproc. A call that failed leaves no watch,
// and its Down, if one came, is dropped.
//
// A call that ended before its answer came (its ctx did), or whose caller
// exited meanwhile, cannot know the process the answer names: the watch the
// callee placed stays on that process until it exits, and its Down is then
// dropped.
func (p *proc) settleWatch(ref Ref, ok bool) {
	p.mu.Lock()
	a := p.awaiting[ref]
	delete(p.awaiting, ref)
	if a == nil { // p exited while the call waited
		p.mu.Unlock()
		return
	}
	t := monitorTarget{pid: a.watched}
	reason := ""
	switch {
	case !ok || a.link && !t.pid.IsZero() && p.hasLink(t):
		// The call failed, or p was linked to the process already: "a
		// second link to the same target is the same link". A watch the
		// callee placed all the same is taken back.
		p.mu.Unlock()
		if !t.pid.IsZero() {
			_ = p.n.demonitor(p.pid, t, ref.ID)
		}
		return
	case t.pid.IsZero():
		t.pid, reason = PID{Node: a.node}, ReasonNoProc
	case a.down:
		reason = a.reason
	case a.link:
		if p.links == nil {
			p.links = map[Ref]monitorTarget{}
		}
		p.links[ref] = t
	default:
		if p.monitors == nil {
			p.monitors = map[Ref]monitorTarget{}
		}
		p.monitors[ref] = t
	}
	p.mu.Unlock()
	switch {
	case reason == "":
	case a.link:
		p.exitSignal(t.pid, "", reason)
	default:
		p.push(item{from: t.pid, down: &Down{Ref: ref, PID: t.pid, Reason: reason}})
	}
}

// hasLink reports whether p is linked to t. Called with p.mu held.
func (p *proc) hasLink(t monitorTarget) bool {
	_, linked := p.linkTo(t)
	return linked
}

// outgoing is the metadata a send from this process carries: md over what
// the process inherited from the message it is handling.
func (p *proc) outgoing(md Metadata) Metadata {
	cur := p.current.Load()
	switch {
	case cur == nil:
		return md
	case len(md) == 0:
		return *cur
	}
	merged := maps.Clone(*cur)
	maps.Copy(merged, md)
	return merged
}

// endHandling closes what OnReceive started for the previous message and
// drops what sends inherited from it.
func (p *proc) endHandling(err error) {
	p.current.Store(nil)
	if p.handling != nil {
		d := p.handling
		p.handling = nil
		d(err)
	}
}

// receive serves inspect requests between messages, then blocks for the
// next item.
func (p *proc) receive(ctx context.Context) (item, error) {
	p.endHandling(nil)
	p.setState(StateIdle)
	defer p.setState(StateRunning)
	select {
	case r := <-p.sys:
		p.serveInspect(r)
	default:
	}
	for {
		if it, ok := p.mbox.tryPop(); ok {
			return p.took(it), nil
		}
		select {
		case <-p.mbox.notify:
		case r := <-p.sys:
			p.serveInspect(r)
		case <-ctx.Done():
			return item{}, context.Cause(ctx)
		}
	}
}

func (p *proc) took(it item) item {
	p.wakeups.Add(1)
	p.received.Add(1)
	if it.body != nil {
		// Only a change of type allocates; a stream of one type does not.
		if name := typeName(it.body); p.lastMsg.Load() == nil || *p.lastMsg.Load() != name {
			p.lastMsg.Store(new(name))
		}
	}
	if p.n.hooks != nil {
		r := ReceiveInfo{PID: p.pid, Label: p.label, From: it.from, Body: it.body, Down: it.down, Exited: it.exited, Call: it.ref != 0}
		if it.at != 0 {
			r.Waited = time.Since(time.Unix(0, it.at))
		}
		it.md, p.handling = p.n.hooks.OnReceive(r, it.md)
	}
	if len(it.md) > 0 {
		p.current.Store(new(it.md)) // not &it.md: that would move every item to the heap
	}
	return it
}

func (p *proc) serveInspect(r inspectReq) {
	// Closed however this ends: if p.inspect panics, nothing is sent, the
	// process exits, and the closed channel tells inspectNow not to wait.
	defer close(r.ch)
	var m map[string]string
	if p.inspect != nil {
		m = p.inspect()
	}
	r.ch <- m
}

func (p *proc) inspectNow(ctx context.Context) (map[string]string, error) {
	r := inspectReq{ch: make(chan map[string]string, 1)}
	select {
	case p.sys <- r:
	case <-ctx.Done():
		return nil, fmt.Errorf("grpcproc: inspect %s: busy for %s: %w", p.pid, p.busyFor().Round(time.Millisecond), ctx.Err())
	case <-p.ctx.Done():
		return nil, ErrNoProc
	}
	select {
	case m, ok := <-r.ch:
		if !ok {
			return nil, fmt.Errorf("grpcproc: inspect %s: inspect function panicked: %w", p.pid, ErrNoProc)
		}
		return m, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (p *proc) setState(s ProcessState) { p.state.Store(uint32(s)) }

// busyFor is how long ago the process took the batch its current message
// came in (so at least as long as it has been on that message), or since it
// started if it has taken none.
func (p *proc) busyFor() time.Duration {
	if since := p.mbox.takenAt.Load(); since != 0 {
		return time.Since(time.Unix(0, since))
	}
	return time.Since(p.started)
}

// typeString names M for display and as the default label. proto.Message is
// an alias, which reflect reports under its real name, protoreflect.ProtoMessage.
func typeString[M proto.Message]() string {
	if t := reflect.TypeFor[M](); t != reflect.TypeFor[proto.Message]() {
		return t.String()
	}
	return "proto.Message"
}

func (p *proc) info() ProcessInfo {
	p.mu.Lock()
	monitors, links, watchers := len(p.monitors), len(p.links), len(p.watchers)
	p.mu.Unlock()
	info := ProcessInfo{
		PID:           p.pid,
		Name:          p.name,
		Label:         p.label,
		Type:          p.typ,
		Parent:        p.parent,
		State:         ProcessState(p.state.Load()),
		StartedAt:     p.started,
		Received:      p.received.Load(),
		Sent:          p.sent.Load(),
		CallsInFlight: uint32(p.callsInFlight.Load()),
		Monitors:      monitors,
		Links:         links,
		Watchers:      watchers,
		TrapExit:      p.trapExit.Load(),
		Wakeups:       p.wakeups.Load(),
		LogLevel:      p.logLevel(),
	}
	if t := p.lastMsg.Load(); t != nil {
		info.LastMessage = *t
	}
	info.Mailbox.Depth = p.mbox.len()
	if at := p.mbox.oldestStamp(); at != 0 {
		info.Mailbox.OldestAge = time.Since(time.Unix(0, at))
	}
	// Peak is measured at batch swaps; a mailbox nobody is taking from has
	// its peak right now.
	info.Mailbox.Peak = max(int(p.mbox.peak.Load()), info.Mailbox.Depth)
	return info
}

func (p *proc) logLevel() slog.Level {
	if p.levelSet.Load() {
		return slog.Level(p.level.Load())
	}
	return slog.LevelInfo
}

func (p *proc) addWatcher(ref Ref, w PID) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.exited {
		return false
	}
	if p.watchers == nil {
		p.watchers = map[Ref]PID{}
	}
	p.watchers[ref] = w
	return true
}

func (p *proc) removeWatcher(ref Ref) {
	p.mu.Lock()
	delete(p.watchers, ref)
	p.mu.Unlock()
}

// dropWatch removes the monitor or link ref: link says which it was. The
// Down of a watch that waits for its call's answer waits with it, with
// reason, and ok is false.
func (p *proc) dropWatch(ref Ref, reason string) (t monitorTarget, link, ok bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if a := p.awaiting[ref]; a != nil && !a.down {
		a.down, a.reason = true, reason
	}
	if t, ok = p.monitors[ref]; ok {
		delete(p.monitors, ref)
		return t, false, true
	}
	t, ok = p.links[ref]
	delete(p.links, ref)
	return t, true, ok
}

// exitSignal delivers the exit of a process p is linked to: an Exited message
// if p traps exits, otherwise p's own end, with the same reason.
func (p *proc) exitSignal(from PID, name, reason string) {
	if p.trapExit.Load() {
		p.push(item{from: from, exited: &Exited{PID: from, Name: name, Reason: reason}})
		return
	}
	p.cancel(&ExitError{Reason: reason})
}

// peerDown drops every monitor, link and watcher that crossed the link to
// peer, and returns the Downs to deliver and the links' exits.
func (p *proc) peerDown(peer string) (downs []Down, exits []Exited) {
	p.mu.Lock()
	defer p.mu.Unlock()
	for ref, t := range p.monitors {
		if t.pid.Node == peer {
			delete(p.monitors, ref)
			downs = append(downs, Down{Ref: ref, PID: t.pid, Name: t.name, Reason: ReasonNoConnection})
		}
	}
	for ref, t := range p.links {
		if t.pid.Node == peer {
			delete(p.links, ref)
			exits = append(exits, Exited{PID: t.pid, Name: t.name, Reason: ReasonNoConnection})
		}
	}
	for ref := range p.watchers {
		if ref.Node == peer {
			delete(p.watchers, ref)
		}
	}
	// A watch whose call has yet to take its answer: if the answer came
	// before the link went, the watch worked, and this is its Down.
	for _, a := range p.awaiting {
		if a.node == peer && !a.down {
			a.down, a.reason = true, ReasonNoConnection
		}
	}
	return downs, exits
}

func (p *proc) run(fn func() error) {
	defer p.n.wg.Done()
	p.setState(StateRunning)
	var err error
	func() {
		defer func() {
			if r := recover(); r != nil {
				err = fmt.Errorf("panic: %v", r)
				p.log.Error("process panicked", "panic", r, "stack", string(debug.Stack()))
			}
		}()
		err = fn()
	}()
	p.terminate(p.exitReason(err))
}

func (p *proc) exitReason(err error) string {
	if reason, ok := exitReasonOf(p.ctx); ok {
		return reason
	}
	if ee, ok := errors.AsType[*ExitError](err); ok {
		return ee.Reason
	}
	switch {
	case err == nil:
		return ReasonNormal
	case p.n.ctx.Err() != nil && errors.Is(err, context.Canceled):
		return ReasonShutdown
	default:
		return err.Error()
	}
}

func (p *proc) terminate(reason string) {
	var err error
	if reason != ReasonNormal {
		err = errors.New(reason)
	}
	p.endHandling(err)
	p.setState(StateExiting)
	p.mu.Lock()
	p.exited = true
	watchers, monitors, links, open, timers := p.watchers, p.monitors, p.links, p.open, p.timers
	p.watchers, p.monitors, p.links, p.awaiting, p.open, p.timers = nil, nil, nil, nil, nil, nil
	p.mu.Unlock()
	for tm := range timers {
		tm.t.Stop()
	}
	p.cancel(nil)

	n := p.n
	n.mu.Lock()
	delete(n.procs, p.pid.ID)
	delete(n.names, p.name) // only its holder's exit frees a name
	n.mu.Unlock()
	n.exited.Add(1)

	// Whatever is still queued goes nowhere, and a call not yet answered
	// never will be: fail them now rather than let callers time out. The
	// dead letters are counted first, as deliver does: a caller woken here
	// sees its own. terminate runs on the process's goroutine, the mailbox's
	// consumer, so it may collect what Receive had swapped in but not taken.
	for _, it := range append(p.mbox.taken(), p.mbox.close()...) {
		if it.body != nil {
			n.deadLetter(it.from, p.pid, it.body, ReasonNoProc)
		}
	}
	for c, a := range open { // queued or taken
		_ = n.reply(p.pid, c.from, c.ref, a, nil, grpcprocv1.Status_STATUS_NOPROC, "", false)
	}
	// The exit is reported before its watchers hear of it, so an observer
	// sees it before anything it causes: a supervisor's restart, say.
	if n.hooks != nil || n.subs.active() {
		info := p.info()
		if n.hooks != nil {
			n.hooks.OnExit(info, reason)
		}
		n.subs.publish(Event{Kind: EventExit, Process: info, Reason: reason})
	}
	for ref, w := range watchers {
		_ = n.down(p.pid, w, ref.ID, reason, false)
	}
	for ref, t := range monitors {
		_ = n.demonitor(p.pid, t, ref.ID)
	}
	for ref, t := range links {
		_ = n.demonitor(p.pid, t, ref.ID)
	}
}

// levelHandler gives a process its own threshold. Once set, it replaces the
// node handler's: a single process can be made more verbose than the rest of
// the node, or quieter.
type levelHandler struct {
	h slog.Handler
	p *proc
}

func (l *levelHandler) Enabled(ctx context.Context, level slog.Level) bool {
	if l.p.levelSet.Load() {
		return level >= slog.Level(l.p.level.Load())
	}
	return l.h.Enabled(ctx, level)
}
func (l *levelHandler) Handle(ctx context.Context, r slog.Record) error { return l.h.Handle(ctx, r) }
func (l *levelHandler) WithAttrs(a []slog.Attr) slog.Handler {
	return &levelHandler{h: l.h.WithAttrs(a), p: l.p}
}
func (l *levelHandler) WithGroup(g string) slog.Handler {
	return &levelHandler{h: l.h.WithGroup(g), p: l.p}
}
