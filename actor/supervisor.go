package actor

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"maps"
	"reflect"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/emptypb"

	"github.com/floatdrop/grpcproc"
	actorv1 "github.com/floatdrop/grpcproc/proto/grpcproc/actor/v1"
)

// Strategy says which children a supervisor restarts when one exits.
type Strategy uint8

const (
	// OneForOne restarts only the child that exited.
	OneForOne Strategy = iota
	// OneForAll stops every other child and restarts them all.
	OneForAll
	// RestForOne restarts the child and every child started after it.
	RestForOne
)

var strategies = [...]string{"one_for_one", "one_for_all", "rest_for_one"}

func (s Strategy) String() string { return enumName(strategies[:], int(s), "Strategy") }

// Restart says when a child is restarted.
type Restart uint8

const (
	// Permanent children are always restarted.
	Permanent Restart = iota
	// Transient children are restarted only after an abnormal exit: not
	// after "normal" or "shutdown".
	Transient
	// Temporary children are never restarted.
	Temporary
)

var restarts = [...]string{"permanent", "transient", "temporary"}

func (r Restart) String() string { return enumName(restarts[:], int(r), "Restart") }

// AutoShutdown says whether a supervisor ends itself when its significant
// children end (see ChildSpec.Significant). It then exits with reason
// "shutdown", after stopping its other children: a supervisor above it
// restarts it only if it is a permanent child there.
type AutoShutdown uint8

const (
	// NoAutoShutdown: the supervisor has no significant children.
	NoAutoShutdown AutoShutdown = iota
	// AnySignificant: the supervisor ends when any significant child does.
	AnySignificant
	// AllSignificant: the supervisor ends when all its significant children
	// have.
	AllSignificant
)

var autoShutdowns = [...]string{"never", "any_significant", "all_significant"}

func (a AutoShutdown) String() string { return enumName(autoShutdowns[:], int(a), "AutoShutdown") }

// enumName is v's entry in names, or type(v) for a value outside them: a
// String must not panic, whatever a Spec holds.
func enumName(names []string, v int, typ string) string {
	if v < len(names) {
		return names[v]
	}
	return typ + "(" + strconv.Itoa(v) + ")"
}

// Infinity, as a Shutdown, waits for a child to exit however long that
// takes. It is the default for a child that is itself a supervisor, which
// needs the time to stop its own children in order: a subtree that is still
// stopping must not be started again beside itself.
const Infinity time.Duration = -1

// ReasonMaxRestarts is the exit reason of a supervisor that restarted its
// children more often than Spec allows. It is abnormal, so the supervisor's
// own supervisor restarts it: that is how failure escalates.
const ReasonMaxRestarts = "max restarts"

type spawnFunc func(fn func(*grpcproc.Process[proto.Message]) error, opts []grpcproc.SpawnOption) (grpcproc.PID, grpcproc.Ref, error)

// ChildSpec says how to start one child. Build it with Child, ChildFunc or
// ChildSupervisor.
type ChildSpec struct {
	// Name is the child's name in the supervisor, and the name it is
	// registered under on the node, so it stays reachable across restarts
	// with grpcproc.Named; it must be unique on the node. An empty Name
	// registers nothing: the child is anonymous, known by its PID, and a
	// supervisor can have any number of those.
	Name    string
	Restart Restart
	// Shutdown is how long the child has to exit once told to: 0 means the
	// supervisor's Spec.Shutdown for a worker, and Infinity for a child
	// that is a supervisor.
	Shutdown time.Duration
	// Significant children end their supervisor, as its Spec.AutoShutdown
	// says, when they end by themselves for good: a transient one that
	// exits with reason normal or shutdown, a temporary one that exits. A
	// permanent child cannot be significant, nor can the child of a
	// supervisor whose AutoShutdown is NoAutoShutdown.
	Significant bool
	// start spawns the child from sup, with extra spawn options of the
	// start's own: the watch of the call that asked for it (see startChild).
	start      func(sup *grpcproc.Process[proto.Message], extra []grpcproc.SpawnOption) (grpcproc.PID, grpcproc.Ref, error)
	supervisor bool
}

// WithRestart returns a copy of c with restart policy r.
func (c ChildSpec) WithRestart(r Restart) ChildSpec {
	c.Restart = r
	return c
}

// WithShutdown returns a copy of c that has d to exit once told to, or as
// long as it takes with Infinity.
func (c ChildSpec) WithShutdown(d time.Duration) ChildSpec {
	c.Shutdown = d
	return c
}

// WithSignificant returns a copy of c that is significant, or not.
func (c ChildSpec) WithSignificant(significant bool) ChildSpec {
	c.Significant = significant
	return c
}

// ChildFunc is a child that runs fn, registered as name unless name is
// empty. opts are for the child; grpcproc.LinkChild is not one of them.
func ChildFunc[M proto.Message](name string, fn func(*grpcproc.Process[M]) error, opts ...grpcproc.SpawnOption) ChildSpec {
	return ChildSpec{Name: name, start: func(sup *grpcproc.Process[proto.Message], extra []grpcproc.SpawnOption) (grpcproc.PID, grpcproc.Ref, error) {
		a, ref, err := sup.SpawnMonitor[M](fn, append(childOpts(name, opts), extra...)...)
		return a.PID(), ref, err
	}}
}

// Child is a child that runs a Handler, built afresh by newHandler at every
// start so a restart begins from a clean state:
//
//	actor.Child("orders", func() *Orders { return &Orders{repo: repo} })
func Child[M proto.Message, H Handler[M]](name string, newHandler func() H, opts ...grpcproc.SpawnOption) ChildSpec {
	return ChildFunc(name, func(p *grpcproc.Process[M]) error { return Run[M](newHandler())(p) }, opts...)
}

// ChildSupervisor is a child that is itself a supervisor. Its Shutdown is
// Infinity unless set: stopping it waits for its whole subtree.
func ChildSupervisor(name string, spec Spec, opts ...grpcproc.SpawnOption) ChildSpec {
	return ChildSpec{Name: name, supervisor: true, start: func(sup *grpcproc.Process[proto.Message], extra []grpcproc.SpawnOption) (grpcproc.PID, grpcproc.Ref, error) {
		return startSupervisor(spec, append(childOpts(name, opts), extra...), func(fn func(*grpcproc.Process[proto.Message]) error, o []grpcproc.SpawnOption) (grpcproc.PID, grpcproc.Ref, error) {
			a, ref, err := sup.SpawnMonitor[proto.Message](fn, o...)
			return a.PID(), ref, err
		})
	}}
}

// childOpts names a child, if it has a name, and links it to its
// supervisor, a safeguard beside the orderly stop the supervisor makes when
// it ends. opts must not hold grpcproc.LinkChild: the supervisor, which does
// not trap exits, would end whenever the child did.
func childOpts(name string, opts []grpcproc.SpawnOption) []grpcproc.SpawnOption {
	own := []grpcproc.SpawnOption{grpcproc.LinkParent()}
	if name != "" {
		own = append(own, grpcproc.WithName(name))
	}
	return append(own, opts...)
}

// Spec describes a supervisor.
type Spec struct {
	Strategy Strategy
	// MaxRestarts within Within is how many restarts the supervisor makes
	// before giving up and exiting with ReasonMaxRestarts. Zero means 3;
	// a negative value means none.
	MaxRestarts int
	// Within defaults to 5s.
	Within time.Duration
	// Shutdown is how long a worker child has to exit once told to, unless
	// its ChildSpec says otherwise; a child supervisor waits for its
	// subtree. Default 5s, or Infinity. grpcproc cannot kill a goroutine: a
	// child that outlives its Shutdown is reported and left behind, and a
	// named one keeps its name until it exits. The supervisor starts it
	// again, or ends, only once it has, so that nothing started under that
	// name meets it.
	Shutdown time.Duration
	// AutoShutdown says whether the supervisor ends itself when its
	// significant children end.
	AutoShutdown AutoShutdown
	// Children start in order and stop in reverse order. StartChild and
	// StartChildFrom add more, after them.
	Children []ChildSpec
	// Factories are the children StartChildFrom can ask the supervisor for,
	// from any node, by name: each builds a child spec from an argument that
	// crossed the wire, with Go functions and dependencies of this node. They
	// are part of the Spec so that what a supervisor can be asked to start is
	// declared where what it starts is, and outlasts restarts of either node
	// as the Spec does.
	Factories map[string]Factory
}

// Factory builds a child spec from an argument, on the supervisor's node, for
// StartChildFrom. Build it with ChildFactory, and name it in Spec.Factories.
type Factory struct {
	build func(arg proto.Message) (ChildSpec, error)
}

// ChildFactory is a Factory whose argument is an A: fn builds the spec of the
// child to start from it, as Child, ChildFunc or ChildSupervisor would, or
// refuses with an error. StartChildFrom returns that error as its own, a
// *grpcproc.RemoteError that is fn's error to errors.Is, so fn is where
// admission goes, with an error per reason a caller tells apart:
//
//	actor.Spec{Factories: map[string]actor.Factory{
//		"peer": actor.ChildFactory(func(j *roomspb.Join) (actor.ChildSpec, error) {
//			if media.Full() {
//				return actor.ChildSpec{}, ErrFull // the caller tries another node
//			}
//			return actor.Child("peer:"+j.GetPeer(), func() *Peer { return &Peer{join: j, media: media} }).
//				WithRestart(actor.Temporary), nil
//		}),
//	}}
//
// An argument of another type is refused.
func ChildFactory[A proto.Message](fn func(A) (ChildSpec, error)) Factory {
	return Factory{build: func(arg proto.Message) (ChildSpec, error) {
		a, ok := arg.(A)
		if !ok {
			return ChildSpec{}, fmt.Errorf("actor: StartChildFrom: the factory takes %v, not %s", reflect.TypeFor[A](), proto.MessageName(arg))
		}
		return fn(a)
	}}
}

func (s Spec) validate() error {
	switch {
	case int(s.Strategy) >= len(strategies):
		return fmt.Errorf("actor: unknown %v", s.Strategy)
	case int(s.AutoShutdown) >= len(autoShutdowns):
		return fmt.Errorf("actor: unknown %v", s.AutoShutdown)
	case s.Shutdown < 0 && s.Shutdown != Infinity:
		return fmt.Errorf("actor: Shutdown %v is neither positive nor Infinity", s.Shutdown)
	}
	seen := map[string]bool{}
	for i, c := range s.Children {
		if err := s.validateChild(c, seen); err != nil {
			return fmt.Errorf("actor: child %d: %w", i, err)
		}
	}
	for name, f := range s.Factories {
		switch {
		case name == "":
			return errors.New("actor: a factory with no name")
		case f.build == nil:
			return fmt.Errorf("actor: factory %q: build it with ChildFactory", name)
		}
	}
	return nil
}

// validateChild checks c for a supervisor of s whose children have the
// names in seen, and adds c's.
func (s Spec) validateChild(c ChildSpec, seen map[string]bool) error {
	label := labelOf(c.Name)
	switch {
	case c.start == nil:
		return errors.New("build it with Child, ChildFunc or ChildSupervisor")
	case c.Name != "" && seen[c.Name]:
		return fmt.Errorf("two children named %q", c.Name)
	case int(c.Restart) >= len(restarts):
		return fmt.Errorf("%s: unknown %v", label, c.Restart)
	case c.Shutdown < 0 && c.Shutdown != Infinity:
		return fmt.Errorf("%s: Shutdown %v is neither positive nor Infinity", label, c.Shutdown)
	case c.Significant && c.Restart == Permanent:
		return fmt.Errorf("%s: a permanent child cannot be significant", label)
	case c.Significant && s.AutoShutdown == NoAutoShutdown:
		return fmt.Errorf("%s: significant, but the supervisor has no AutoShutdown", label)
	}
	if c.Name != "" {
		seen[c.Name] = true
	}
	return nil
}

// labelOf names a child in messages: by its name, or as anonymous.
func labelOf(name string) string {
	if name == "" {
		return "an anonymous child"
	}
	return strconv.Quote(name)
}

// Supervise starts a supervisor on n and its children, in order. It returns
// once they have all started, or with the first error, after stopping those
// already started. opts apply to the supervisor itself (a name, a label).
func Supervise(n *grpcproc.Node, spec Spec, opts ...grpcproc.SpawnOption) (grpcproc.PID, error) {
	pid, _, err := startSupervisor(spec, opts, func(fn func(*grpcproc.Process[proto.Message]) error, o []grpcproc.SpawnOption) (grpcproc.PID, grpcproc.Ref, error) {
		a, err := n.Spawn(fn, o...)
		return a.PID(), grpcproc.Ref{}, err
	})
	return pid, err
}

func startSupervisor(spec Spec, opts []grpcproc.SpawnOption, spawn spawnFunc) (grpcproc.PID, grpcproc.Ref, error) {
	if err := spec.validate(); err != nil {
		return grpcproc.PID{}, grpcproc.Ref{}, err
	}
	if spec.MaxRestarts == 0 {
		spec.MaxRestarts = 3
	}
	spec.Within = cmp.Or(spec.Within, 5*time.Second)
	spec.Shutdown = cmp.Or(spec.Shutdown, 5*time.Second)
	s := &supervisor{spec: spec, ready: make(chan error, 1)}
	for _, c := range spec.Children {
		s.kids = append(s.kids, &kid{spec: c})
	}
	opts = append([]grpcproc.SpawnOption{grpcproc.WithLabel("supervisor"), grpcproc.WithInspect(s.inspect)}, opts...)
	pid, ref, err := spawn(s.run, opts)
	if err != nil {
		return grpcproc.PID{}, grpcproc.Ref{}, err
	}
	if err := <-s.ready; err != nil {
		return grpcproc.PID{}, grpcproc.Ref{}, err
	}
	return pid, ref, nil
}

// requests holds the child specs StartChild hands to supervisors, by id: a
// spec holds Go functions, which no message can carry.
var (
	requests  sync.Map
	requestID atomic.Uint64
)

// StartChild adds spec to the supervisor sup and starts the child, after
// the children sup has already: it returns once the child has started. from
// is who asks, the Node or a Process, as for grpcproc.Addr.Call, and must be
// on sup's node: a spec holds Go functions, which no message can carry, and
// sup refuses a caller of another node. The child is sup's like the others, except that
// once it ends for good (it is temporary, or transient and ended normally,
// or StopChild stopped it), sup forgets it. Anonymous children, with an
// empty Name, are how a supervisor keeps a pool of workers, each started
// as it is needed.
//
// If sup has a child of spec's name running already, StartChild starts
// nothing and returns that child's PID with ErrAlreadyStarted, so a caller
// that means to start a named child unless it runs takes the PID either
// way. A child of that name that has exited, and whose exit sup has yet to
// handle, is waited for: sup restarts it, which is then the child that
// runs, or forgets it and starts spec.
//
// The spec is checked as in Spec, and a child that fails to start is an
// error, which sup does not count as a restart. sup's errors come back as
// *grpcproc.RemoteError, which is ErrBusy to errors.Is when sup was busy. If
// ctx ends first, the child may still start.
//
// From a process, pass p.Context(): a supervisor that is stopping the
// caller answers no call until it is done, and a caller that waits with a
// ctx of its own cannot exit meanwhile.
func StartChild(ctx context.Context, from grpcproc.Caller, sup grpcproc.Target, spec ChildSpec) (grpcproc.PID, error) {
	id := requestID.Add(1)
	requests.Store(id, spec)
	defer requests.Delete(id)
	r, err := grpcproc.AddrOf[proto.Message](sup).Call[*actorv1.Started](ctx, from, &actorv1.Control{Op: &actorv1.Control_Start{Start: id}})
	if err != nil {
		return grpcproc.PID{}, err
	}
	if r.GetAlready() {
		return grpcproc.PIDFromProto(r.GetPid()), ErrAlreadyStarted
	}
	return grpcproc.PIDFromProto(r.GetPid()), nil
}

// ErrAlreadyStarted is StartChild's error when the supervisor has a child of
// the spec's name running already. StartChild returns that child's PID with
// it.
var ErrAlreadyStarted = errors.New("actor: StartChild: a child of that name is running already")

// StartChildFrom asks the supervisor sup, which may run on another node, to
// start the child its factory named factory builds from arg (see
// Spec.Factories), after the children sup has already, and returns once the
// child has started. It is StartChild for a caller that cannot hand sup a
// spec, Go functions that no message carries: arg is data, and the factory,
// on sup's node, turns it into a spec there. The child is sup's as one
// StartChild added is, forgotten once it ends for good.
//
// With WithMonitor, the caller monitors the child from before it runs, as
// Process.SpawnMonitor does a child of its own node: however soon the child
// exits, its Down comes after StartChildFrom returns, with the Ref it
// returned and the real reason, never noproc. WithLink links the caller to
// the child instead, as grpcproc.LinkChild does. Either needs from to be a
// process, the only kind of caller a Down or an exit reaches. A start that
// fails leaves no monitor or link behind, and no Down comes of it: the error
// says it failed, which a Down never does. Once StartChildFrom has returned
// the child, a link to sup's node that breaks is its Down, with
// noconnection.
//
// As with StartChild, a child of the spec's name that runs already is not
// started again: StartChildFrom returns its PID with ErrAlreadyStarted, and
// the monitor or link, on that child. So when an answer is lost, a link that
// broke or a ctx that ended, a named child may have started unwatched, and
// starting it again finds it, with a watch. An anonymous child has no name to
// be found by: sup lists it (Children), and it runs until it ends. The watch
// is of the child's process: a restart sup makes is a new one, which
// StartChildFrom of the name watches again.
//
//	pid, ref, err := actor.StartChildFrom(p.Context(), p, sup, "peer", &roomspb.Join{Peer: id}, actor.WithMonitor())
//	switch {
//	case errors.Is(err, ErrFull):
//		// try another node
//	case err != nil && !errors.Is(err, actor.ErrAlreadyStarted):
//		return err
//	}
//	// A Down with ref comes when pid exits, however soon.
//
// The factory's refusal is the error, as the factory returned it, which
// errors.Is tells apart; ErrNoFactory when sup has no factory of that name.
// The other errors are StartChild's.
func StartChildFrom(ctx context.Context, from grpcproc.Caller, sup grpcproc.Target, factory string, arg proto.Message, opts ...StartOption) (grpcproc.PID, grpcproc.Ref, error) {
	var o startOpts
	for _, opt := range opts {
		opt(&o)
	}
	a, err := anypb.New(arg)
	if err != nil {
		return grpcproc.PID{}, grpcproc.Ref{}, fmt.Errorf("actor: StartChildFrom: %w", err)
	}
	req := &actorv1.Control{Op: &actorv1.Control_StartFrom{StartFrom: &actorv1.StartFrom{Factory: factory, Arg: a}}}
	to := grpcproc.AddrOf[proto.Message](sup)
	var r *actorv1.Started
	var ref grpcproc.Ref
	switch {
	case o.monitor && o.link:
		err = errors.New("actor: StartChildFrom: WithMonitor or WithLink, not both")
	case o.monitor:
		r, ref, err = to.CallMonitor[*actorv1.Started](ctx, from, req)
	case o.link:
		r, err = to.CallLink[*actorv1.Started](ctx, from, req)
	default:
		r, err = to.Call[*actorv1.Started](ctx, from, req)
	}
	if err != nil {
		return grpcproc.PID{}, grpcproc.Ref{}, err
	}
	if r.GetAlready() {
		return grpcproc.PIDFromProto(r.GetPid()), ref, ErrAlreadyStarted
	}
	return grpcproc.PIDFromProto(r.GetPid()), ref, nil
}

// StartOption configures StartChildFrom.
type StartOption func(*startOpts)

type startOpts struct{ monitor, link bool }

// WithMonitor has the caller of StartChildFrom monitor the child from before
// it runs: StartChildFrom returns the monitor's Ref.
func WithMonitor() StartOption { return func(o *startOpts) { o.monitor = true } }

// WithLink links the caller of StartChildFrom to the child from before it
// runs, as grpcproc.LinkChild links a parent: the child's exit ends the
// caller, with its reason, or reaches it as an Exited if it traps exits.
func WithLink() StartOption { return func(o *startOpts) { o.link = true } }

// ErrNoFactory is StartChildFrom's error when the supervisor has no factory
// of that name.
var ErrNoFactory = errors.New("actor: StartChildFrom: the supervisor has no factory of that name")

// StopChild stops child, a running child of the supervisor sup, and makes
// sup forget it: it is not restarted, whatever its Restart, and a strategy
// no longer counts it, until sup itself is started again from its Spec. A
// significant child stopped this way does not end sup. sup may run on
// another node. from is who asks, as for StartChild; from a process, pass
// p.Context() as StartChild says.
func StopChild(ctx context.Context, from grpcproc.Caller, sup grpcproc.Target, child grpcproc.PID) error {
	_, err := grpcproc.AddrOf[proto.Message](sup).Call[*emptypb.Empty](ctx, from, &actorv1.Control{Op: &actorv1.Control_Stop{Stop: child.Proto()}})
	return err
}

// ChildInfo is one of a supervisor's children, as Children reports it.
type ChildInfo struct {
	// Name is the child's name in the supervisor, and on the node; empty
	// for an anonymous child.
	Name string
	// PID is the child's process while it runs, and zero while it does not:
	// it has ended for good, or it is Restarting.
	PID grpcproc.PID
	// Restarting reports that a restart owes the child a start, which waits
	// for a previous process, the child's own or an earlier child's, to exit.
	Restarting bool
	Restart    Restart
	// Restarts is how many times the supervisor has restarted the child.
	Restarts int
	// Supervisor reports that the child is a supervisor itself.
	Supervisor bool
}

// Children asks the supervisor sup which children it has, in the order it
// starts them: those of its Spec, then those StartChild added, until they
// end for good. It reports them as sup knows them when it answers, so a
// child that has just exited may still be listed as running until sup has
// handled its exit, and the list is out of date as soon as it is taken: to
// start a named child unless it runs, call StartChild, which answers
// ErrAlreadyStarted with the running child's PID. sup may run on another
// node. from is who asks, as for StartChild; from a process, pass
// p.Context() as StartChild says.
func Children(ctx context.Context, from grpcproc.Caller, sup grpcproc.Target) ([]ChildInfo, error) {
	r, err := grpcproc.AddrOf[proto.Message](sup).Call[*actorv1.Children](ctx, from, &actorv1.Control{Op: &actorv1.Control_WhichChildren{WhichChildren: &emptypb.Empty{}}})
	if err != nil {
		return nil, err
	}
	out := make([]ChildInfo, 0, len(r.GetChildren()))
	for _, c := range r.GetChildren() {
		out = append(out, ChildInfo{
			Name:       c.GetName(),
			PID:        grpcproc.PIDFromProto(c.GetPid()), // the zero PID unless it runs
			Restarting: c.GetRestarting(),
			Restart:    Restart(c.GetRestart() - 1),
			Restarts:   int(c.GetRestarts()),
			Supervisor: c.GetSupervisor(),
		})
	}
	return out, nil
}

type kid struct {
	spec     ChildSpec
	pid      grpcproc.PID
	ref      grpcproc.Ref
	running  bool
	pending  bool // a restart owes it a start: for as long as that restart waits (see resume)
	dynamic  bool // added by StartChild: forgotten once it will not run again
	restarts int
	// left is the child's previous process, if it outlived its Shutdown
	// and still holds the child's name; leftRef monitors it. A restart
	// that reaches the child waits for its Down, as does the supervisor's
	// own end.
	left    grpcproc.PID
	leftRef grpcproc.Ref
}

func (k *kid) name() string { return cmp.Or(k.spec.Name, k.pid.String()) }

// supervisor state is touched only by its own process's goroutine,
// including inspect, which grpcproc runs inside Receive.
type supervisor struct {
	spec    Spec
	p       *grpcproc.Process[proto.Message]
	kids    []*kid
	history []time.Time                   // restarts within the window
	saved   []grpcproc.Msg[proto.Message] // taken from the mailbox while waiting (see drain)
	held    []heldStart                   // StartChild calls waiting for a child's Down (see startChild)
	ready   chan error
	once    sync.Once
}

// heldStart is a StartChild call the supervisor answers once it has
// handled a Down.
type heldStart struct {
	m    grpcproc.Msg[proto.Message]
	spec ChildSpec
}

var errAborted = errors.New("actor: supervisor exited while starting")

func (s *supervisor) signal(err error) { s.once.Do(func() { s.ready <- err }) }

func (s *supervisor) run(p *grpcproc.Process[proto.Message]) error {
	s.p = p
	defer s.signal(errAborted) // a no-op once start has reported
	defer s.stopAll()
	for i, k := range s.kids {
		if err := s.start(k); err != nil {
			err = fmt.Errorf("actor: start child %d, %s: %w", i, labelOf(k.spec.Name), err)
			s.signal(err)
			return err
		}
	}
	s.signal(nil)
	for {
		m, err := s.next()
		if err != nil {
			return err
		}
		switch {
		case m.Down != nil:
			if err := s.exited(*m.Down); err != nil {
				return err
			}
			s.forget()
			held := s.held
			s.held = nil
			for _, h := range held {
				s.startChild(h.m, h.spec)
			}
		case m.IsCall():
			s.control(m)
		}
	}
}

// next is what the supervisor handles next: what it saved while waiting,
// then its mailbox.
func (s *supervisor) next() (grpcproc.Msg[proto.Message], error) {
	if len(s.saved) > 0 {
		m := s.saved[0]
		s.saved = s.saved[1:]
		return m, nil
	}
	return s.p.Receive()
}

// drain takes what is queued while the supervisor waits for a child to
// exit, to handle after the wait. Once the wait has lasted busyAfter, it
// answers the calls among it with ErrBusy instead: a child told to exit may
// be calling its supervisor, from a Terminate say, and would otherwise hold
// the wait until its call gave up, or for good; so would every supervisor
// above, waiting for this one. Hooks see what it takes as received then.
func (s *supervisor) drain(busy bool) {
	for {
		m, err := s.p.ReceiveTimeout(0) // takes what is queued even once told to exit
		if err != nil {
			break
		}
		s.saved = append(s.saved, m)
	}
	if busy {
		s.saved = slices.DeleteFunc(s.saved, func(m grpcproc.Msg[proto.Message]) bool {
			if m.IsCall() {
				_ = m.Reply(nil, ErrBusy)
			}
			return m.IsCall()
		})
	}
}

// busyAfter is how long a wait for a child goes before the supervisor
// answers the calls it holds with ErrBusy.
const busyAfter = 100 * time.Millisecond

// ErrBusy is what a supervisor answers a call with, StartChild, StopChild
// and Children included, once it has waited past busyAfter for a child to
// exit: try again.
var ErrBusy = errors.New("actor: the supervisor is waiting for a child to exit; try again")

func (s *supervisor) start(k *kid, extra ...grpcproc.SpawnOption) error {
	pid, ref, err := k.spec.start(s.p, extra)
	if err != nil {
		return err
	}
	k.pid, k.ref, k.running, k.pending = pid, ref, true, false
	return nil
}

// control answers StartChild, StartChildFrom, StopChild and Children, the
// calls a supervisor takes.
func (s *supervisor) control(m grpcproc.Msg[proto.Message]) {
	c, _ := m.Body.(*actorv1.Control)
	switch op := c.GetOp().(type) {
	case *actorv1.Control_Start:
		if m.From.Node != s.p.Node().Name() {
			_ = m.Reply(nil, errors.New("actor: StartChild: only from the supervisor's node"))
			return
		}
		v, ok := requests.LoadAndDelete(op.Start)
		if !ok {
			_ = m.Reply(nil, errors.New("actor: StartChild: no such request here; its caller gave up, or is on another node"))
			return
		}
		s.startChild(m, v.(ChildSpec))
	case *actorv1.Control_StartFrom:
		spec, err := s.build(op.StartFrom)
		if err != nil {
			_ = m.Reply(nil, err)
			return
		}
		s.startChild(m, spec)
	case *actorv1.Control_Stop:
		_ = m.Reply(&emptypb.Empty{}, s.stopChild(grpcproc.PIDFromProto(op.Stop)))
	case *actorv1.Control_WhichChildren:
		_ = m.Reply(s.children(), nil)
	default:
		_ = m.Reply(nil, errors.New("actor: a supervisor takes no calls but StartChild, StartChildFrom, StopChild and Children"))
	}
}

// build is the spec a StartChildFrom asks for, which its factory builds. The
// factory's error is the answer as it is, for its caller to tell apart.
func (s *supervisor) build(r *actorv1.StartFrom) (ChildSpec, error) {
	f, ok := s.spec.Factories[r.GetFactory()]
	if !ok {
		return ChildSpec{}, ErrNoFactory
	}
	arg, err := r.GetArg().UnmarshalNew()
	if err != nil {
		return ChildSpec{}, fmt.Errorf("actor: StartChildFrom: %w", err)
	}
	return f.build(arg)
}

// startChild answers the StartChild or StartChildFrom call m for spec. A
// child of spec's name that runs is the answer. One that has exited, and
// whose Down the supervisor has yet to handle, has freed its name already:
// the call is held until the supervisor has handled a Down, then answered
// afresh, when the child has been restarted, or forgotten. The Down comes,
// since the supervisor monitors every child that runs. The monitor or link
// m's caller asked for goes on the child that answers it, before the answer
// leaves: one that starts has it before it runs.
func (s *supervisor) startChild(m grpcproc.Msg[proto.Message], spec ChildSpec) {
	if i := slices.IndexFunc(s.kids, func(k *kid) bool { return k.running && spec.Name != "" && k.spec.Name == spec.Name }); i >= 0 {
		k := s.kids[i]
		if _, alive := s.p.Node().Process(k.pid); !alive || m.Watch(k.pid) != nil {
			s.held = append(s.held, heldStart{m, spec})
			return
		}
		_ = m.Reply(&actorv1.Started{Pid: k.pid.Proto(), Already: true}, nil)
		return
	}
	pid, err := s.add(spec, grpcproc.WatchedBy(m))
	if err != nil {
		_ = m.Reply(nil, err)
		return
	}
	_ = m.Reply(&actorv1.Started{Pid: pid.Proto()}, nil)
}

// add starts spec as a child StartChild adds, with extra spawn options.
func (s *supervisor) add(spec ChildSpec, extra ...grpcproc.SpawnOption) (grpcproc.PID, error) {
	if slices.ContainsFunc(s.kids, func(k *kid) bool { return k.pending }) {
		// Started now, it would come before children the restart owes a
		// start.
		return grpcproc.PID{}, errors.New("actor: StartChild: a restart is waiting for a child's previous process to exit; try again")
	}
	seen := map[string]bool{}
	for _, k := range s.kids {
		seen[k.spec.Name] = true
	}
	if err := s.spec.validateChild(spec, seen); err != nil {
		return grpcproc.PID{}, fmt.Errorf("actor: StartChild: %w", err)
	}
	k := &kid{spec: spec, dynamic: true}
	if err := s.start(k, extra...); err != nil {
		return grpcproc.PID{}, fmt.Errorf("actor: StartChild, %s: %w", labelOf(spec.Name), err)
	}
	s.kids = append(s.kids, k)
	return k.pid, nil
}

// children is what Children answers: every child but those StopChild
// stopped whose process is still exiting (see forget).
func (s *supervisor) children() *actorv1.Children {
	out := &actorv1.Children{}
	for _, k := range s.kids {
		if k.dynamic && !k.running && !k.pending {
			continue
		}
		c := &actorv1.Child{
			Name:       k.spec.Name,
			Restarting: k.pending,
			Restart:    actorv1.Restart(k.spec.Restart) + 1,
			Restarts:   uint32(k.restarts),
			Supervisor: k.spec.supervisor,
		}
		if k.running {
			c.Pid = k.pid.Proto()
		}
		out.Children = append(out.Children, c)
	}
	return out
}

func (s *supervisor) stopChild(pid grpcproc.PID) error {
	for i, k := range s.kids {
		if k.running && k.pid == pid {
			s.stop(k)
			if k.left.IsZero() {
				s.kids = slices.Delete(s.kids, i, i+1)
			} else {
				// Kept until it has exited, which the supervisor's end
				// waits for, then forgotten.
				k.dynamic, k.pending = true, false
			}
			return nil
		}
	}
	return fmt.Errorf("actor: StopChild: %v is not a running child of this supervisor", pid)
}

// exited handles the Down of a child, or of a child's previous process
// that outlived its Shutdown.
func (s *supervisor) exited(d grpcproc.Down) error {
	for i, k := range s.kids {
		if !k.left.IsZero() && k.leftRef == d.Ref {
			k.left = grpcproc.PID{}
			return s.resume() // its name is free: a restart waiting on it goes on
		}
		if k.running && k.ref == d.Ref {
			k.running = false
			switch {
			case restartable(k.spec.Restart, d.Reason):
				return s.restart(i)
			case k.spec.Significant && (s.spec.AutoShutdown == AnySignificant || !s.significantLeft()):
				return &grpcproc.ExitError{Reason: grpcproc.ReasonShutdown}
			}
			return nil
		}
	}
	return nil // a child already stopped or replaced
}

// significantLeft reports whether a significant child is running, or due
// to run again.
func (s *supervisor) significantLeft() bool {
	return slices.ContainsFunc(s.kids, func(k *kid) bool { return k.spec.Significant && (k.running || k.pending) })
}

// forget drops the children StartChild added that will not run again, once
// no process of theirs is left.
func (s *supervisor) forget() {
	s.kids = slices.DeleteFunc(s.kids, func(k *kid) bool { return k.dynamic && !k.running && !k.pending && k.left.IsZero() })
}

func restartable(r Restart, reason string) bool {
	switch r {
	case Permanent:
		return true
	case Transient:
		return reason != grpcproc.ReasonNormal && reason != grpcproc.ReasonShutdown
	}
	return false
}

// restart applies the strategy for child i, within the restart intensity.
func (s *supervisor) restart(i int) error {
	now := time.Now()
	kept := s.history[:0]
	for _, t := range s.history {
		if now.Sub(t) < s.spec.Within {
			kept = append(kept, t)
		}
	}
	s.history = append(kept, now)
	if len(s.history) > s.spec.MaxRestarts {
		s.p.Log().Error("restart intensity reached, giving up", "child", s.kids[i].name(),
			"restarts", len(s.history), "within", s.spec.Within)
		return &grpcproc.ExitError{Reason: ReasonMaxRestarts}
	}
	lo, hi := i, i+1
	switch s.spec.Strategy {
	case OneForAll:
		lo, hi = 0, len(s.kids)
	case RestForOne:
		hi = len(s.kids)
	}
	// Only what was running comes back: a transient child that had already
	// finished stays finished, and a temporary one is stopped for good. What
	// this restart stops stays due until it starts again, so if a start
	// fails, the retry still brings back the children after it.
	for j := hi - 1; j >= lo; j-- {
		k := s.kids[j]
		k.pending = k.pending || j == i || (k.running && k.spec.Restart != Temporary)
		s.stop(k)
	}
	return s.resume()
}

// resume starts, in order, the children a restart owes a start. It stops at
// a child whose previous process outlived its Shutdown and still holds its
// name: that process's Down resumes it, and meanwhile the supervisor goes
// on handling its mailbox.
func (s *supervisor) resume() error {
	for j, k := range s.kids {
		switch {
		case !k.pending:
			continue
		case !k.left.IsZero():
			s.p.Log().Warn("restart waits for a child's previous process to exit", "child", k.name(), "pid", k.left)
			return nil
		}
		k.restarts++
		if err := s.start(k); err != nil {
			s.p.Log().Error("restart failed", "child", k.name(), "err", err)
			return s.restart(j) // counts against the intensity, so it ends
		}
	}
	return nil
}

// stopAll stops the children in reverse order, then waits for any previous
// process of theirs that still holds a name: a supervisor that replaces
// this one starts its children under the same names.
func (s *supervisor) stopAll() {
	for i := len(s.kids) - 1; i >= 0; i-- {
		s.stop(s.kids[i])
	}
	for _, k := range s.kids {
		if !k.left.IsZero() {
			s.await(k.left, Infinity)
		}
	}
}

// stop tells a running child to exit and waits for it to be gone, up to its
// shutdown time. A named child that outlives it is left behind, monitored:
// see kid.left.
func (s *supervisor) stop(k *kid) {
	if !k.running {
		return
	}
	k.running = false
	s.p.Demonitor(k.ref)
	_ = s.p.Exit(k.pid, grpcproc.ReasonShutdown)
	shutdown := s.shutdownOf(k)
	if s.await(k.pid, shutdown) {
		return
	}
	s.p.Log().Warn("child did not exit in time", "child", k.name(), "pid", k.pid, "shutdown", shutdown)
	if k.spec.Name != "" || k.spec.supervisor { // a subtree holds names, even under an anonymous supervisor
		k.left, k.leftRef = k.pid, s.p.Monitor(k.pid)
	}
}

// await waits for pid to be gone, up to d or however long with Infinity,
// and reports whether it is. It watches node events rather than the
// mailbox, because it also runs after the supervisor itself was told to
// exit, when Receive no longer waits; and it looks again now and then,
// since events can be dropped, and end when the node stops.
func (s *supervisor) await(pid grpcproc.PID, d time.Duration) bool {
	n := s.p.Node()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	events := n.Subscribe(ctx, 256)
	var timeout <-chan time.Time
	if d != Infinity {
		t := time.NewTimer(d)
		defer t.Stop()
		timeout = t.C
	}
	tick := time.NewTicker(50 * time.Millisecond)
	defer tick.Stop()
	start := time.Now()
	for {
		if _, alive := n.Process(pid); !alive {
			return true
		}
		s.drain(time.Since(start) >= busyAfter)
		select {
		case _, ok := <-events:
			if !ok {
				events = nil // the node stopped: ticks only
			}
		case <-tick.C:
		case <-timeout:
			return false
		}
	}
}

// shutdownOf is how long k has to exit once told to.
func (s *supervisor) shutdownOf(k *kid) time.Duration {
	switch {
	case k.spec.Shutdown != 0:
		return k.spec.Shutdown
	case k.spec.supervisor:
		return Infinity
	}
	return s.spec.Shutdown
}

func (s *supervisor) inspect() map[string]string {
	out := map[string]string{
		"strategy": s.spec.Strategy.String(),
		"restarts": strconv.Itoa(len(s.history)) + "/" + strconv.Itoa(s.spec.MaxRestarts) + " in " + s.spec.Within.String(),
	}
	if s.spec.AutoShutdown != NoAutoShutdown {
		out["auto_shutdown"] = s.spec.AutoShutdown.String()
	}
	if len(s.spec.Factories) > 0 {
		out["factories"] = strings.Join(slices.Sorted(maps.Keys(s.spec.Factories)), " ")
	}
	for _, k := range s.kids {
		state := "stopped"
		switch {
		case k.running:
			state = k.pid.String()
		case k.pending && !k.left.IsZero():
			state = "waiting " + k.left.String()
		}
		out["child."+k.name()] = state + " " + k.spec.Restart.String() + " restarts=" + strconv.Itoa(k.restarts)
	}
	return out
}
