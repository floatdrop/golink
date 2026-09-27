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

// item is what sits in a mailbox: a message, a call, or a Down.
type item struct {
	from PID
	body proto.Message // nil for a Down
	down *Down
	md   Metadata
	ref  uint64 // call ref; 0 for a plain message
	at   int64  // unix nanos, when it was queued; only when hooks want the wait
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

	// mu guards what follows. Taken inside Node.mu, never around it.
	mu       sync.Mutex
	exited   bool
	watchers map[Ref]PID           // who monitors me
	monitors map[Ref]monitorTarget // whom I monitor
	open     map[openCall]struct{} // calls taken from the mailbox and not yet answered
	timers   map[*Timer]struct{}   // SendAfter timers not yet fired
}

type openCall struct {
	from PID
	ref  uint64
}

type monitorTarget struct {
	pid  PID
	name string
}

// SpawnOption configures Node.Spawn, Process.Spawn and Process.SpawnMonitor.
type SpawnOption func(*spawnOpts)

type spawnOpts struct {
	name    string
	label   string
	inspect func() map[string]string
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

func parentPID(p *proc) PID {
	if p == nil {
		return PID{}
	}
	return p.pid
}

// Process is a goroutine with a mailbox of M and a cluster-wide PID.
type Process[M proto.Message] struct{ *proc }

// Msg is what Receive returns: a message (Body) or a Down, never both.
type Msg[M proto.Message] struct {
	From     PID
	Body     M
	Down     *Down
	Metadata Metadata
	ref      uint64
}

// IsCall reports whether the sender waits for a Reply.
func (m Msg[M]) IsCall() bool { return m.ref != 0 }

// Context returns a context carrying the message's metadata, for Node.Send,
// a Call, or any code taking a ctx, run on behalf of the message.
func (m Msg[M]) Context(parent context.Context) context.Context {
	if len(m.Metadata) == 0 {
		return parent
	}
	return WithMetadata(parent, m.Metadata)
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
// recorded as its parent. That is for inspection only: p does not monitor
// the child, and neither exits when the other does. A process that has
// exited spawns nothing: the error is ErrNoProc.
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
// and monitors the child from before it runs if monitor is set.
func spawn[M proto.Message](n *Node, fn func(*Process[M]) error, opts []SpawnOption, parent *proc, monitor bool) (Addr[M], Ref, error) {
	monitor = monitor && parent != nil
	var o spawnOpts
	for _, opt := range opts {
		opt(&o)
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
	// child registered and its monitor in place. A monitoring parent's
	// monitor exists before the child runs, so its Down cannot be missed.
	n.mu.Lock()
	if parent != nil {
		parent.mu.Lock()
	}
	var err error
	switch {
	case parent != nil && parent.exited:
		err = ErrNoProc // a process that has exited spawns nothing
	case n.stopping:
		err = ErrNodeStopped
	case o.name != "" && n.names[o.name] != nil:
		err = ErrNameTaken
	}
	var ref Ref
	if err == nil && monitor {
		ref = Ref{Node: n.id.Name, ID: n.nextRef.Add(1)}
		if parent.monitors == nil {
			parent.monitors = map[Ref]monitorTarget{}
		}
		parent.monitors[ref] = monitorTarget{pid: p.pid}
		p.watchers = map[Ref]PID{ref: parent.pid}
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

// Context is cancelled when the process is asked to exit or its node stops.
// context.Cause is an *ExitError after Exit.
func (p *proc) Context() context.Context { return p.ctx }

// Log returns a logger with the process's pid and label attached. Its
// threshold can be changed at runtime with Node.SetLogLevel.
func (p *proc) Log() *slog.Logger { return p.log }

// Receive blocks until a message or a Down arrives, or the process is told
// to exit. The error is then context.Canceled (node stop) or an *ExitError.
func (p *Process[M]) Receive() (Msg[M], error) {
	it, err := p.receive(p.ctx)
	return toMsg[M](it), err
}

// ReceiveTimeout is Receive with a deadline; context.DeadlineExceeded on timeout.
func (p *Process[M]) ReceiveTimeout(d time.Duration) (Msg[M], error) {
	ctx, cancel := context.WithTimeout(p.ctx, d)
	defer cancel()
	it, err := p.receive(ctx)
	if err != nil && p.ctx.Err() != nil {
		err = context.Cause(p.ctx)
	}
	return toMsg[M](it), err
}

func toMsg[M proto.Message](it item) Msg[M] {
	m := Msg[M]{From: it.from, Down: it.down, Metadata: it.md, ref: it.ref}
	if it.body != nil {
		m.Body, _ = it.body.(M) // accept() checked this on delivery
	}
	return m
}

// Send delivers m to a typed address, local or remote. It carries the
// metadata of the message the process is handling. A first send to a node
// with no link yet waits for the dial, up to Config.DialTimeout, unless dials
// to it are failing (Config.DialBackoff). See Node.Send.
func (p *Process[M]) Send[N proto.Message](to Addr[N], m N) error {
	return p.n.send(context.Background(), p.pid, p.proc, to.dest(), m, p.outgoing(nil))
}

// SendTo delivers msg to an untyped target such as a Msg's From. The
// target's type is checked on delivery only.
func (p *proc) SendTo(to Target, msg proto.Message) error {
	return p.n.send(context.Background(), p.pid, p, destOf(to), msg, p.outgoing(nil))
}

// Call sends req and waits for the Reply, typed as R:
//
//	resp, err := p.Call[*orderspb.Reserved](ctx, addr, &orderspb.Order{…})
//
// Replies do not pass through the mailbox, so calling from inside a process
// never reorders its messages. Errors are as for Node.Call.
func (p *Process[M]) Call[R, N proto.Message](ctx context.Context, to Addr[N], req N) (R, error) {
	return typed[R](p.n.doCall(ctx, p.pid, p.proc, to.dest(), req, p.outgoing(MetadataFrom(ctx))))
}

// CallTo is Call to an untyped target, such as a Msg's From.
func (p *Process[M]) CallTo[R proto.Message](ctx context.Context, to Target, req proto.Message) (R, error) {
	return typed[R](p.n.doCall(ctx, p.pid, p.proc, destOf(to), req, p.outgoing(MetadataFrom(ctx))))
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

// Reply answers a message for which IsCall is true. It may be called later
// and from any goroutine (a deferred reply).
func (p *Process[M]) Reply(m Msg[M], resp proto.Message, err error) error {
	if m.ref == 0 {
		return ErrNotCall
	}
	p.mu.Lock()
	delete(p.open, openCall{m.From, m.ref})
	p.mu.Unlock()
	if err != nil {
		return p.n.reply(p.pid, m.From, m.ref, resp, grpcprocv1.Status_STATUS_ERROR, err.Error(), false)
	}
	return p.n.reply(p.pid, m.From, m.ref, resp, grpcprocv1.Status_STATUS_OK, "", false)
}

// Exit asks another process, anywhere, to terminate with reason. A first
// exit to a node with no link yet waits for the dial as Send does.
func (p *proc) Exit(to Target, reason string) error {
	return p.n.exit(context.Background(), p.pid, to, reason)
}

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
	if err := p.n.monitor(p.pid, target, ref.ID); err != nil {
		p.n.deliverDown(pid, p.pid, ref.ID, ReasonNoConnection)
	}
	return ref
}

// Demonitor stops a monitor. A Down already in the mailbox stays there.
func (p *proc) Demonitor(ref Ref) {
	p.mu.Lock()
	t, ok := p.monitors[ref]
	delete(p.monitors, ref)
	p.mu.Unlock()
	if ok {
		_ = p.n.demonitor(p.pid, Name{Node: t.pid.Node, Name: t.name}, ref.ID)
		if t.name == "" {
			_ = p.n.demonitor(p.pid, t.pid, ref.ID)
		}
	}
}

// ---------- internals ----------

func (p *proc) push(it item) bool {
	return p.mbox.push(it)
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
	if it.ref != 0 {
		p.mu.Lock()
		if p.open == nil {
			p.open = map[openCall]struct{}{}
		}
		p.open[openCall{it.from, it.ref}] = struct{}{}
		p.mu.Unlock()
	}
	if it.body != nil {
		// Only a change of type allocates; a stream of one type does not.
		if name := typeName(it.body); p.lastMsg.Load() == nil || *p.lastMsg.Load() != name {
			p.lastMsg.Store(new(name))
		}
	}
	if p.n.hooks != nil {
		r := ReceiveInfo{PID: p.pid, Label: p.label, From: it.from, Body: it.body, Down: it.down, Call: it.ref != 0}
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
	monitors, watchers := len(p.monitors), len(p.watchers)
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
		Watchers:      watchers,
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

func (p *proc) dropMonitor(ref Ref) (monitorTarget, bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	t, ok := p.monitors[ref]
	delete(p.monitors, ref)
	return t, ok
}

// peerDown drops every monitor and watcher that crossed the link to peer and
// returns the Downs to deliver.
func (p *proc) peerDown(peer string) []Down {
	p.mu.Lock()
	defer p.mu.Unlock()
	var downs []Down
	for ref, t := range p.monitors {
		if t.pid.Node == peer {
			delete(p.monitors, ref)
			downs = append(downs, Down{Ref: ref, PID: t.pid, Name: t.name, Reason: ReasonNoConnection})
		}
	}
	for ref := range p.watchers {
		if ref.Node == peer {
			delete(p.watchers, ref)
		}
	}
	return downs
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
	watchers, monitors, open, timers := p.watchers, p.monitors, p.open, p.timers
	p.watchers, p.monitors, p.open, p.timers = nil, nil, nil, nil
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

	// Whatever is still queued goes nowhere, and a call taken but never
	// answered never will be: fail them now rather than let callers time out.
	for c := range open {
		_ = n.reply(p.pid, c.from, c.ref, nil, grpcprocv1.Status_STATUS_NOPROC, "", false)
	}
	// terminate runs on the process's goroutine, the mailbox's consumer, so
	// it may collect what Receive had swapped in but not yet taken.
	for _, it := range append(p.mbox.taken(), p.mbox.close()...) {
		if it.ref != 0 {
			_ = n.reply(p.pid, it.from, it.ref, nil, grpcprocv1.Status_STATUS_NOPROC, "", false)
		}
		if it.body != nil {
			n.deadLetter(it.from, p.pid, it.body, ReasonNoProc)
		}
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
		if t.name != "" {
			_ = n.demonitor(p.pid, Name{Node: t.pid.Node, Name: t.name}, ref.ID)
		} else {
			_ = n.demonitor(p.pid, t.pid, ref.ID)
		}
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
