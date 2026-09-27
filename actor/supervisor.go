package actor

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"slices"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/emptypb"

	"github.com/floatdrop/grpcproc"
	actorv1 "github.com/floatdrop/grpcproc/proto/grpcproc/actor/v1"
	grpcprocv1 "github.com/floatdrop/grpcproc/proto/grpcproc/v1"
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
	start       func(sup *grpcproc.Process[proto.Message]) (grpcproc.PID, grpcproc.Ref, error)
	supervisor  bool
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
	return ChildSpec{Name: name, start: func(sup *grpcproc.Process[proto.Message]) (grpcproc.PID, grpcproc.Ref, error) {
		a, ref, err := sup.SpawnMonitor[M](fn, childOpts(name, opts)...)
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
	return ChildSpec{Name: name, supervisor: true, start: func(sup *grpcproc.Process[proto.Message]) (grpcproc.PID, grpcproc.Ref, error) {
		return startSupervisor(spec, childOpts(name, opts), func(fn func(*grpcproc.Process[proto.Message]) error, o []grpcproc.SpawnOption) (grpcproc.PID, grpcproc.Ref, error) {
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
	// Children start in order and stop in reverse order. StartChild adds
	// more, after them.
	Children []ChildSpec
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

// StartChild adds spec to the supervisor sup, which must run on n, and
// starts the child, after the children sup has already: it returns once
// the child has started. The child is sup's like the others, except that
// once it ends for good (it is temporary, or transient and ended normally,
// or StopChild stopped it), sup forgets it. Anonymous children, with an
// empty Name, are how a supervisor keeps a pool of workers, each started
// as it is needed.
//
// The spec is checked as in Spec, and a child that fails to start is an
// error, which sup does not count as a restart. sup's errors come back as
// *grpcproc.RemoteError text, but for ErrBusy. If ctx ends first, the child
// may still start.
//
// From a process, pass p.Context(): a supervisor that is stopping the
// caller answers no call until it is done, and a caller that waits with a
// ctx of its own cannot exit meanwhile.
func StartChild(ctx context.Context, n *grpcproc.Node, sup grpcproc.PID, spec ChildSpec) (grpcproc.PID, error) {
	if sup.Node != n.Name() {
		return grpcproc.PID{}, fmt.Errorf("actor: StartChild: supervisor %v is not on node %s", sup, n.Name())
	}
	id := requestID.Add(1)
	requests.Store(id, spec)
	defer requests.Delete(id)
	pid, err := n.CallTo[*grpcprocv1.PID](ctx, sup, &actorv1.Control{Op: &actorv1.Control_Start{Start: id}})
	if err != nil {
		return grpcproc.PID{}, fromSupervisor(err)
	}
	return grpcproc.PID{Node: pid.GetNode(), Incarnation: pid.GetIncarnation(), ID: pid.GetId()}, nil
}

// StopChild stops child, a running child of the supervisor sup, and makes
// sup forget it: it is not restarted, whatever its Restart, and a strategy
// no longer counts it, until sup itself is started again from its Spec. A
// significant child stopped this way does not end sup. sup may run on
// another node. As for StartChild, pass p.Context() from a process.
func StopChild(ctx context.Context, n *grpcproc.Node, sup, child grpcproc.PID) error {
	stop := &grpcprocv1.PID{Node: child.Node, Incarnation: child.Incarnation, Id: child.ID}
	_, err := n.CallTo[*emptypb.Empty](ctx, sup, &actorv1.Control{Op: &actorv1.Control_Stop{Stop: stop}})
	return fromSupervisor(err)
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
	ready   chan error
	once    sync.Once
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
				_ = s.p.Reply(m, nil, ErrBusy)
			}
			return m.IsCall()
		})
	}
}

// busyAfter is how long a wait for a child goes before the supervisor
// answers the calls it holds with ErrBusy.
const busyAfter = 100 * time.Millisecond

// ErrBusy is what a supervisor answers a call with, StartChild or StopChild
// included, once it has waited past busyAfter for a child to exit: try
// again.
var ErrBusy = errors.New("actor: the supervisor is waiting for a child to exit; try again")

// fromSupervisor turns a supervisor's ErrBusy, which comes back as text,
// into ErrBusy again.
func fromSupervisor(err error) error {
	if re, ok := errors.AsType[*grpcproc.RemoteError](err); ok && re.Msg == ErrBusy.Error() {
		return ErrBusy
	}
	return err
}

func (s *supervisor) start(k *kid) error {
	pid, ref, err := k.spec.start(s.p)
	if err != nil {
		return err
	}
	k.pid, k.ref, k.running, k.pending = pid, ref, true, false
	return nil
}

// control answers StartChild and StopChild, the calls a supervisor takes.
func (s *supervisor) control(m grpcproc.Msg[proto.Message]) {
	c, _ := m.Body.(*actorv1.Control)
	switch op := c.GetOp().(type) {
	case *actorv1.Control_Start:
		if m.From.Node != s.p.Node().Name() {
			_ = s.p.Reply(m, nil, errors.New("actor: StartChild: only from the supervisor's node"))
			return
		}
		pid, err := s.startChild(op.Start)
		if err != nil {
			_ = s.p.Reply(m, nil, err)
			return
		}
		_ = s.p.Reply(m, &grpcprocv1.PID{Node: pid.Node, Incarnation: pid.Incarnation, Id: pid.ID}, nil)
	case *actorv1.Control_Stop:
		pid := grpcproc.PID{Node: op.Stop.GetNode(), Incarnation: op.Stop.GetIncarnation(), ID: op.Stop.GetId()}
		_ = s.p.Reply(m, &emptypb.Empty{}, s.stopChild(pid))
	default:
		_ = s.p.Reply(m, nil, errors.New("actor: a supervisor takes no calls but StartChild and StopChild"))
	}
}

func (s *supervisor) startChild(id uint64) (grpcproc.PID, error) {
	v, ok := requests.LoadAndDelete(id)
	if !ok {
		return grpcproc.PID{}, errors.New("actor: StartChild: no such request here; its caller gave up, or is on another node")
	}
	spec := v.(ChildSpec)
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
	if err := s.start(k); err != nil {
		return grpcproc.PID{}, fmt.Errorf("actor: StartChild, %s: %w", labelOf(spec.Name), err)
	}
	s.kids = append(s.kids, k)
	return k.pid, nil
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
