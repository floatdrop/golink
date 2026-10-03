package leader

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/floatdrop/fsm"
	"google.golang.org/protobuf/proto"

	"github.com/floatdrop/grpcproc"
	"github.com/floatdrop/grpcproc/actor"
	leaderv1 "github.com/floatdrop/grpcproc/leader/proto/grpcproc/leader/v1"
)

// phase is where the singleton is in its life on this node.
type phase uint8

const (
	idle phase = iota
	starting
	running
	stopping
)

func (p phase) String() string { return [...]string{"idle", "starting", "running", "stopping"}[p] }

// phases are the phases as inspect shows them.
var phases = [...]string{"none", "starting", "running", "stopping"}

// singleton is the elector's singleton: its phase, and, once it starts, the
// term it was started for, its PID and the monitor on it.
type singleton struct {
	phase phase
	term  uint64
	pid   grpcproc.PID
	ref   grpcproc.Ref
}

// life is what the singleton's events carry: the elector, when, and what
// the event tells of the singleton.
type life struct {
	e      *elector
	now    time.Time
	term   uint64       // evStarted: the term it was started for
	pid    grpcproc.PID // evStarted
	reason string       // evStartFailed and evDown: why
}

var (
	// evReconcile is the elector's every turn: the guards say whether the
	// singleton is to start or stop, which makes the machine a
	// level-triggered controller.
	evReconcile   = fsm.Define[life]("reconcile")
	evStarted     = fsm.Define[life]("started")
	evStartFailed = fsm.Define[life]("start failed")
	// evDown is the singleton's monitor firing. The phase tells whether it
	// failed (running) or did as it was told (stopping).
	evDown = fsm.Define[life]("down")
)

var (
	errNotLeading = errors.New("this node does not lead, or hands over")
	errCurrent    = errors.New("this node leads in the term it runs for")
)

// lifecycle is the singleton's life on this node. It starts while the node
// leads; it stops when the node no longer does, or leads in another term;
// one that fails to start, or exits by itself, holds off the next campaign
// (see elector.failed).
//
// It is built in init: launch fires it, so a variable initialized with it
// would refer to itself.
var lifecycle *fsm.Machine[phase]

func init() {
	lifecycle = fsm.MustNew("singleton",
		fsm.Initial(idle),

		fsm.From(idle).On(evReconcile).To(starting).
			Guard("this node leads, and does not hand over", leads).
			Action(saveTerm),
		fsm.From(starting).On(evStarted).To(running),
		fsm.From(starting).On(evStartFailed).To(idle),
		fsm.From(running).On(evReconcile).To(stopping).
			Guard("this node stopped leading, or its term is over", outlived),
		fsm.From(running).On(evDown).To(idle),
		fsm.From(stopping).On(evDown).To(idle),

		fsm.OnEnterVia(starting, evReconcile, launch),
		fsm.OnEnterVia(running, evStarted, watch),
		fsm.OnEnterVia(stopping, evReconcile, demote),
		fsm.OnExitVia(starting, evStartFailed, startFailed),
		fsm.OnExitVia(running, evDown, exited),
		fsm.OnEnterVia(idle, evDown, stopped),
		fsm.OnEnterWith(idle, forget),
	)
}

// live fires ev at the elector's singleton. A refusal is no news: the
// machine refuses a reconcile with nothing to do, by its phase or a guard,
// and the other events come only in the phase that takes them. A start
// whose term could not be saved ends the elector's loop (see fire).
func (e *elector) live(ev fsm.Event[life], l life) {
	l.e = e
	fire(e, lifecycle, &e.single.phase, ev, l)
}

func leads(_ context.Context, l life) error {
	if l.e.role != leading {
		return errNotLeading
	}
	return nil
}

func outlived(_ context.Context, l life) error {
	if l.e.role == leading && l.e.single.term == l.e.term {
		return errCurrent
	}
	return nil
}

// saveTerm saves the term before the singleton starts for it: a lease for a
// term this node could lead again after a restart would be no fencing
// token.
func saveTerm(_ context.Context, l life) error {
	if !l.e.save() {
		return l.e.err
	}
	return nil
}

// launch starts the singleton for this term, from a process of its own:
// Confirm and the singleton's start may take their time, and heartbeats
// must go on meanwhile. A state the singleton cannot be built from fails
// the start here, before Confirm is asked.
func launch(_ context.Context, _ fsm.Transition[phase], l life) {
	e := l.e
	build, err := e.build(e.term, e.state.GetValue())
	if err != nil {
		e.live(evStartFailed, life{now: l.now, reason: err.Error()})
		return
	}
	term, top, confirm := e.term, e.p.Parent(), e.spec.Confirm
	e.single.term = term
	// It fails only while the node stops.
	_, _ = e.p.Spawn(func(h *grpcproc.Process[proto.Message]) error {
		started := &leaderv1.Started{Term: term}
		pid, err := startSingleton(h.Context(), h, top, term, confirm, build)
		if err != nil {
			started.Error = err.Error()
		} else {
			started.Pid = pid.Proto()
		}
		return h.SendTo(h.Parent(), started)
	}, grpcproc.LinkParent(), grpcproc.WithLabel("leader starter"))
}

// watch monitors the singleton that started. reconcile stops it at once if
// the term it was started for is over.
func watch(_ context.Context, _ fsm.Transition[phase], l life) {
	l.e.single.term, l.e.single.pid, l.e.single.ref = l.term, l.pid, l.e.p.Monitor(l.pid)
}

func demote(_ context.Context, _ fsm.Transition[phase], l life) {
	_ = l.e.p.Exit(l.e.single.pid, ReasonDemoted)
}

func startFailed(_ context.Context, _ fsm.Transition[phase], l life) {
	l.e.failed(l.now, l.reason)
}

func exited(_ context.Context, _ fsm.Transition[phase], l life) {
	l.e.failed(l.now, "the singleton exited: "+l.reason)
}

// stopped follows a singleton that exited as it was told, from stopping:
// it ends the failures in a row, and a hand-over waiting for it goes on.
// It runs on entry, since the hand-over waits for idle.
func stopped(_ context.Context, tr fsm.Transition[phase], l life) {
	if tr.From == stopping {
		l.e.fails = 0
		l.e.handOver(l.now, false)
	}
}

// forget drops what the elector knew of the singleton that is gone, all
// but its phase, which is the machine's.
func forget(_ context.Context, _ fsm.Transition[phase], l life) {
	l.e.single.term, l.e.single.pid, l.e.single.ref = 0, grpcproc.PID{}, grpcproc.Ref{}
}

func startSingleton(ctx context.Context, from grpcproc.Caller, top grpcproc.PID, term uint64, confirm func(context.Context, uint64) error, build func() (actor.ChildSpec, error)) (grpcproc.PID, error) {
	if confirm != nil {
		if err := confirm(ctx, term); err != nil {
			return grpcproc.PID{}, fmt.Errorf("leader: Confirm: %w", err)
		}
	}
	child, err := build()
	if err != nil {
		return grpcproc.PID{}, fmt.Errorf("leader: Singleton: %w", err)
	}
	return actor.StartChild(ctx, from, top, child.WithRestart(actor.Temporary).WithSignificant(false))
}
