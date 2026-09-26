package golink

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"reflect"
	"runtime/debug"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"google.golang.org/protobuf/proto"

	golinkv1 "github.com/floatdrop/golink/proto/golink/v1"
)

// item is what sits in a mailbox: a message, a call, or a Down.
type item struct {
	from PID
	body proto.Message // nil for a Down
	down *Down
	md   Metadata
	ref  uint64 // call ref; 0 for a plain message
	at   time.Time
}

type inspectReq struct {
	ch chan map[string]string
}

// proc is the untyped half of a process; Process[M] is a typed view of it.
type proc struct {
	n       *Node
	pid     PID
	label   string
	typ     string
	parent  PID
	mbox    *queue[item]
	sys     chan inspectReq
	inspect func() map[string]string
	accept  func(proto.Message) bool
	ctx     context.Context
	cancel  context.CancelCauseFunc
	started time.Time
	log     *slog.Logger

	received, sent, wakeups atomic.Uint64
	callsInFlight           atomic.Int32
	state                   atomic.Uint32
	peak                    atomic.Int64
	lastMsg                 atomic.Pointer[string]
	level                   atomic.Int64
	levelSet                atomic.Bool

	mu       sync.Mutex
	exited   bool
	watchers map[Ref]PID           // who monitors me
	monitors map[Ref]monitorTarget // whom I monitor
	open     map[openCall]struct{} // calls taken from the mailbox and not yet answered
	names    []string              // guarded by n.mu
}

type openCall struct {
	from PID
	ref  uint64
}

type monitorTarget struct {
	pid  PID
	name string
}

// SpawnOption configures Spawn.
type SpawnOption func(*spawnOpts)

type spawnOpts struct {
	name    string
	label   string
	parent  PID
	inspect func() map[string]string
}

// WithName registers the process under name on its node before it runs.
func WithName(name string) SpawnOption { return func(o *spawnOpts) { o.name = name } }

// WithLabel tags the process with a low-cardinality label, the key metrics
// aggregate by. Defaults to the type of M.
func WithLabel(label string) SpawnOption { return func(o *spawnOpts) { o.label = label } }

// WithParent records which process spawned this one, for inspection.
func WithParent(pid PID) SpawnOption { return func(o *spawnOpts) { o.parent = pid } }

// WithInspect lets the process publish what it currently believes. fn runs
// on the process's own goroutine, inside Receive, so it may read the
// process's state without locking. See Node.Inspect.
func WithInspect(fn func() map[string]string) SpawnOption {
	return func(o *spawnOpts) { o.inspect = fn }
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

// Context returns a context carrying the message's metadata, for Send and
// Call made while handling it.
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
func Spawn[M proto.Message](n *Node, fn func(*Process[M]) error, opts ...SpawnOption) (Addr[M], error) {
	var o spawnOpts
	for _, opt := range opts {
		opt(&o)
	}
	typ := reflect.TypeFor[M]().String()
	if o.label == "" {
		o.label = typ
	}
	id := n.nextID.Add(1)
	ctx, cancel := context.WithCancelCause(n.ctx)
	p := &proc{
		n:       n,
		pid:     PID{Node: n.id.Name, Incarnation: n.id.Incarnation, ID: id},
		label:   o.label,
		typ:     typ,
		parent:  o.parent,
		mbox:    newQueue[item](),
		sys:     make(chan inspectReq),
		inspect: o.inspect,
		accept:  func(m proto.Message) bool { _, ok := m.(M); return ok },
		ctx:     ctx,
		cancel:  cancel,
		started: time.Now(),
	}
	p.log = slog.New(&levelHandler{h: n.log.Handler(), p: p}).With(
		"pid", p.pid.String(), "label", o.label)

	n.mu.Lock()
	if n.stopping {
		n.mu.Unlock()
		cancel(nil)
		return Addr[M]{}, ErrNodeStopped
	}
	if o.name != "" {
		if _, taken := n.names[o.name]; taken {
			n.mu.Unlock()
			cancel(nil)
			return Addr[M]{}, ErrNameTaken
		}
		n.names[o.name] = p
		p.names = append(p.names, o.name)
	}
	n.procs[id] = p
	n.wg.Add(1)
	n.mu.Unlock()
	n.spawned.Add(1)
	if n.hooks != nil {
		n.hooks.OnSpawn(p.info())
	}

	go p.run(func() error { return fn(&Process[M]{p}) })
	return Addr[M]{pid: p.pid}, nil
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

// Register binds name to this process on its node.
func (p *proc) Register(name string) error {
	n := p.n
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.procs[p.pid.ID] != p {
		return ErrNoProc
	}
	if _, taken := n.names[name]; taken {
		return ErrNameTaken
	}
	n.names[name] = p
	p.names = append(p.names, name)
	return nil
}

// Receive blocks until a message or a Down arrives, or the process is told
// to exit. The error is then context.Canceled (node stop) or an *ExitError.
func (p *Process[M]) Receive() (Msg[M], error) {
	it, err := p.receive(p.ctx)
	return p.msg(it), err
}

// ReceiveTimeout is Receive with a deadline; context.DeadlineExceeded on timeout.
func (p *Process[M]) ReceiveTimeout(d time.Duration) (Msg[M], error) {
	ctx, cancel := context.WithTimeout(p.ctx, d)
	defer cancel()
	it, err := p.receive(ctx)
	if err != nil && p.ctx.Err() != nil {
		err = context.Cause(p.ctx)
	}
	return p.msg(it), err
}

func (p *Process[M]) msg(it item) Msg[M] {
	m := Msg[M]{From: it.from, Down: it.down, Metadata: it.md, ref: it.ref}
	if it.body != nil {
		m.Body, _ = it.body.(M) // accept() checked this on delivery
	}
	return m
}

// Send delivers m to a typed address, local or remote. See Node.Send.
func (p *Process[M]) Send[N proto.Message](to Addr[N], m N) error {
	return p.n.send(p.pid, p.proc, to, m, nil)
}

// SendContext is Send with the metadata carried by ctx.
func (p *Process[M]) SendContext[N proto.Message](ctx context.Context, to Addr[N], m N) error {
	return p.n.send(p.pid, p.proc, to, m, MetadataFrom(ctx))
}

// SendTo delivers msg to an untyped target such as a Msg's From. The
// target's type is checked on delivery only.
func (p *proc) SendTo(to Target, msg proto.Message) error {
	return p.n.send(p.pid, p, to, msg, nil)
}

// Call sends req and waits for the Reply, typed as R:
//
//	resp, err := p.Call[*orderspb.Reserved](ctx, addr, &orderspb.Order{…})
//
// Replies do not pass through the mailbox, so calling from inside a process
// never reorders its messages. Errors are as for Node.Call.
func (p *Process[M]) Call[R, N proto.Message](ctx context.Context, to Addr[N], req N) (R, error) {
	return typed[R](p.n.doCall(ctx, p.pid, p.proc, to, req))
}

// CallTo is Call to an untyped target, such as a Msg's From.
func (p *Process[M]) CallTo[R proto.Message](ctx context.Context, to Target, req proto.Message) (R, error) {
	return typed[R](p.n.doCall(ctx, p.pid, p.proc, to, req))
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
		return p.n.reply(p.pid, m.From, m.ref, resp, golinkv1.Status_STATUS_ERROR, err.Error())
	}
	return p.n.reply(p.pid, m.From, m.ref, resp, golinkv1.Status_STATUS_OK, "")
}

// Exit asks another process, anywhere, to terminate with reason.
func (p *proc) Exit(to Target, reason string) error { return p.n.exit(p.pid, to, reason) }

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
	if !p.mbox.push(it) {
		return false
	}
	if d := int64(p.mbox.len()); d > p.peak.Load() {
		p.peak.Store(d)
	}
	return true
}

// receive serves inspect requests between messages, then blocks for the
// next item.
func (p *proc) receive(ctx context.Context) (item, error) {
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
		p.lastMsg.Store(new(typeName(it.body)))
	}
	if p.n.hooks != nil {
		p.n.hooks.OnReceive(p.pid, it.body, time.Since(it.at))
	}
	return it
}

func (p *proc) serveInspect(r inspectReq) {
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
		return nil, fmt.Errorf("golink: inspect %s: busy for %s: %w", p.pid, time.Since(p.started).Round(time.Millisecond), ctx.Err())
	case <-p.ctx.Done():
		return nil, ErrNoProc
	}
	select {
	case m := <-r.ch:
		return m, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (p *proc) setState(s ProcessState) { p.state.Store(uint32(s)) }

func (p *proc) info() ProcessInfo {
	p.mu.Lock()
	monitors, watchers := len(p.monitors), len(p.watchers)
	p.mu.Unlock()
	p.n.mu.Lock()
	names := slices.Clone(p.names)
	p.n.mu.Unlock()
	info := ProcessInfo{
		PID:           p.pid,
		Names:         names,
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
	p.mbox.mu.Lock()
	info.Mailbox.Depth = len(p.mbox.items) - p.mbox.head
	if info.Mailbox.Depth > 0 {
		info.Mailbox.OldestAge = time.Since(p.mbox.items[p.mbox.head].at)
	}
	p.mbox.mu.Unlock()
	info.Mailbox.Peak = int(p.peak.Load())
	return info
}

func (p *proc) logLevel() slog.Level {
	if p.levelSet.Load() {
		return slog.Level(p.level.Load())
	}
	return slog.LevelInfo
}

func (p *proc) dropName(name string) {
	if i := slices.Index(p.names, name); i >= 0 {
		p.names = slices.Delete(p.names, i, i+1)
	}
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

func (p *proc) dropMonitor(ref Ref) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	_, ok := p.monitors[ref]
	delete(p.monitors, ref)
	return ok
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
			downs = append(downs, Down{Ref: ref, PID: t.pid, Reason: ReasonNoConnection})
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
	p.setState(StateExiting)
	p.mu.Lock()
	p.exited = true
	watchers, monitors, open := p.watchers, p.monitors, p.open
	p.watchers, p.monitors, p.open = nil, nil, nil
	p.mu.Unlock()
	p.cancel(nil)

	n := p.n
	n.mu.Lock()
	delete(n.procs, p.pid.ID)
	for _, name := range p.names {
		if n.names[name] == p {
			delete(n.names, name)
		}
	}
	n.mu.Unlock()
	n.exited.Add(1)

	// Whatever is still queued goes nowhere, and a call taken but never
	// answered never will be: fail them now rather than let callers time out.
	for c := range open {
		_ = n.reply(p.pid, c.from, c.ref, nil, golinkv1.Status_STATUS_NOPROC, "")
	}
	for _, it := range p.mbox.close() {
		if it.ref != 0 {
			_ = n.reply(p.pid, it.from, it.ref, nil, golinkv1.Status_STATUS_NOPROC, "")
		}
		if it.body != nil {
			n.deadLetter(it.from, p.pid, it.body, ReasonNoProc)
		}
	}
	for ref, w := range watchers {
		_ = n.down(p.pid, w, ref.ID, reason)
	}
	for ref, t := range monitors {
		if t.name != "" {
			_ = n.demonitor(p.pid, Name{Node: t.pid.Node, Name: t.name}, ref.ID)
		} else {
			_ = n.demonitor(p.pid, t.pid, ref.ID)
		}
	}
	if n.hooks != nil {
		n.hooks.OnExit(p.info(), reason)
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
