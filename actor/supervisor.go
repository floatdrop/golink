package actor

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"strconv"
	"sync"
	"time"

	"google.golang.org/protobuf/proto"

	"github.com/floatdrop/grpcproc"
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

func (s Strategy) String() string {
	return [...]string{"one_for_one", "one_for_all", "rest_for_one"}[s]
}

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

func (r Restart) String() string { return [...]string{"permanent", "transient", "temporary"}[r] }

// ReasonMaxRestarts is the exit reason of a supervisor that restarted its
// children more often than Spec allows. It is abnormal, so the supervisor's
// own supervisor restarts it: that is how failure escalates.
const ReasonMaxRestarts = "max restarts"

type spawnFunc func(fn func(*grpcproc.Process[proto.Message]) error, opts []grpcproc.SpawnOption) (grpcproc.PID, grpcproc.Ref, error)

// ChildSpec says how to start one child. Build it with Child, ChildFunc or
// ChildSupervisor.
type ChildSpec struct {
	// Name is the child's identity in the supervisor and the name it is
	// registered under on the node, so it stays reachable across restarts,
	// with grpcproc.Named. It must be unique on the node.
	Name    string
	Restart Restart
	start   func(sup *grpcproc.Process[proto.Message]) (grpcproc.PID, grpcproc.Ref, error)
}

// WithRestart returns a copy of c with restart policy r.
func (c ChildSpec) WithRestart(r Restart) ChildSpec {
	c.Restart = r
	return c
}

// ChildFunc is a child that runs fn, registered as name.
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

// ChildSupervisor is a child that is itself a supervisor.
func ChildSupervisor(name string, spec Spec, opts ...grpcproc.SpawnOption) ChildSpec {
	return ChildSpec{Name: name, start: func(sup *grpcproc.Process[proto.Message]) (grpcproc.PID, grpcproc.Ref, error) {
		return startSupervisor(spec, childOpts(name, opts), func(fn func(*grpcproc.Process[proto.Message]) error, o []grpcproc.SpawnOption) (grpcproc.PID, grpcproc.Ref, error) {
			a, ref, err := sup.SpawnMonitor[proto.Message](fn, o...)
			return a.PID(), ref, err
		})
	}}
}

func childOpts(name string, opts []grpcproc.SpawnOption) []grpcproc.SpawnOption {
	return append([]grpcproc.SpawnOption{grpcproc.WithName(name)}, opts...)
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
	// Shutdown is how long a child has to exit once told to; grpcproc cannot
	// kill a goroutine, so a child that ignores Exit is left behind. Default 5s.
	Shutdown time.Duration
	// Children start in order and stop in reverse order.
	Children []ChildSpec
}

func (s Spec) validate() error {
	seen := map[string]bool{}
	for i, c := range s.Children {
		switch {
		case c.start == nil:
			return fmt.Errorf("actor: child %d: build it with Child, ChildFunc or ChildSupervisor", i)
		case c.Name == "":
			return fmt.Errorf("actor: child %d has no name", i)
		case seen[c.Name]:
			return fmt.Errorf("actor: two children named %q", c.Name)
		}
		seen[c.Name] = true
	}
	return nil
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

type kid struct {
	spec     ChildSpec
	pid      grpcproc.PID
	ref      grpcproc.Ref
	running  bool
	restarts int
}

// supervisor state is touched only by its own process's goroutine,
// including inspect, which grpcproc runs inside Receive.
type supervisor struct {
	spec    Spec
	p       *grpcproc.Process[proto.Message]
	kids    []*kid
	history []time.Time // restarts within the window
	ready   chan error
	once    sync.Once
}

var errAborted = errors.New("actor: supervisor exited while starting")

func (s *supervisor) signal(err error) { s.once.Do(func() { s.ready <- err }) }

func (s *supervisor) run(p *grpcproc.Process[proto.Message]) error {
	s.p = p
	defer s.signal(errAborted) // a no-op once start has reported
	defer s.stopAll()
	for _, k := range s.kids {
		if err := s.start(k); err != nil {
			err = fmt.Errorf("actor: start %q: %w", k.spec.Name, err)
			s.signal(err)
			return err
		}
	}
	s.signal(nil)
	for {
		m, err := p.Receive()
		if err != nil {
			return err
		}
		switch {
		case m.Down != nil:
			if err := s.exited(*m.Down); err != nil {
				return err
			}
		case m.IsCall():
			_ = p.Reply(m, nil, errors.New("actor: a supervisor takes no calls"))
		}
	}
}

func (s *supervisor) start(k *kid) error {
	pid, ref, err := k.spec.start(s.p)
	if err != nil {
		return err
	}
	k.pid, k.ref, k.running = pid, ref, true
	return nil
}

// exited handles the Down of a child.
func (s *supervisor) exited(d grpcproc.Down) error {
	for i, k := range s.kids {
		if k.running && k.ref == d.Ref {
			k.running = false
			if restartable(k.spec.Restart, d.Reason) {
				return s.restart(i)
			}
			return nil
		}
	}
	return nil // a child already stopped or replaced
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
		s.p.Log().Error("restart intensity reached, giving up", "child", s.kids[i].spec.Name,
			"restarts", len(s.history), "within", s.spec.Within)
		return &grpcproc.ExitError{Reason: ReasonMaxRestarts}
	}
	group := []int{i}
	switch s.spec.Strategy {
	case OneForAll:
		group = indexes(0, len(s.kids))
	case RestForOne:
		group = indexes(i, len(s.kids))
	}
	// Only what was running comes back: a transient child that had already
	// finished stays finished, and a temporary one is stopped for good.
	running := make([]bool, len(s.kids))
	for j := len(group) - 1; j >= 0; j-- {
		k := s.kids[group[j]]
		running[group[j]] = k.running
		s.stop(k)
	}
	for _, j := range group {
		k := s.kids[j]
		if j != i && (!running[j] || k.spec.Restart == Temporary) {
			continue
		}
		k.restarts++
		if err := s.start(k); err != nil {
			s.p.Log().Error("restart failed", "child", k.spec.Name, "err", err)
			return s.restart(j) // counts against the intensity, so it ends
		}
	}
	return nil
}

func indexes(from, to int) []int {
	out := make([]int, 0, to-from)
	for i := from; i < to; i++ {
		out = append(out, i)
	}
	return out
}

func (s *supervisor) stopAll() {
	for i := len(s.kids) - 1; i >= 0; i-- {
		s.stop(s.kids[i])
	}
}

// stop tells a running child to exit and waits, up to Spec.Shutdown, for it
// to be gone. It watches node events rather than the mailbox, because it
// also runs after the supervisor itself was told to exit, when Receive no
// longer returns anything.
func (s *supervisor) stop(k *kid) {
	if !k.running {
		return
	}
	k.running = false
	n := s.p.Node()
	ctx, cancel := context.WithTimeout(context.Background(), s.spec.Shutdown)
	defer cancel()
	events := n.Subscribe(ctx, 256)
	s.p.Demonitor(k.ref)
	_ = s.p.Exit(k.pid, grpcproc.ReasonShutdown)
	for {
		if _, alive := n.Process(k.pid); !alive {
			return
		}
		if _, ok := <-events; !ok {
			s.p.Log().Warn("child did not exit in time", "child", k.spec.Name, "pid", k.pid, "shutdown", s.spec.Shutdown)
			return
		}
	}
}

func (s *supervisor) inspect() map[string]string {
	out := map[string]string{
		"strategy": s.spec.Strategy.String(),
		"restarts": strconv.Itoa(len(s.history)) + "/" + strconv.Itoa(s.spec.MaxRestarts) + " in " + s.spec.Within.String(),
	}
	for _, k := range s.kids {
		state := "stopped"
		if k.running {
			state = k.pid.String()
		}
		out["child."+k.spec.Name] = state + " " + k.spec.Restart.String() + " restarts=" + strconv.Itoa(k.restarts)
	}
	return out
}
