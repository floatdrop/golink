// Package cron runs jobs on schedules written in crontab syntax, from a
// grpcproc process the application starts:
//
//	c, err := cron.Start(node, cron.Spec{Jobs: []cron.Job{{
//		Name:     "nightly-report",
//		Spec:     "10 3 * * *",
//		Location: berlin,
//		Action:   cron.Send(reporter, func(r cron.Run) *reportpb.Make { return &reportpb.Make{Day: r.Time.Format(time.DateOnly)} }),
//	}}}, grpcproc.WithName("cron"))
//
// The process checks the time at the start of every minute, and starts the
// jobs due then. Every run is a process of its own, linked to the cron
// process, so a run that fails is its exit reason, one that is still going
// when the next is due can be left alone, skipped or replaced (Overlap),
// one that takes too long is told to exit (Timeout), and every run shows up
// among the node's processes, labelled "cron:" and its job's name.
//
// A job's Spec is read in its own Location, UTC unless set. A minute that
// clocks skip when they go forward does not exist, so nothing due then
// runs; one they repeat when they go back runs the first time only.
//
// Only this node's jobs are its: every node that starts a cron process runs
// its jobs. For a job that runs once in the cluster, run the cron process
// as a grpcproc/leader singleton, which resumes on the next leader from the
// State the last one reported.
package cron

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/emptypb"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/floatdrop/grpcproc"
	"github.com/floatdrop/grpcproc/actor"
	cronv1 "github.com/floatdrop/grpcproc/cron/proto/grpcproc/cron/v1"
)

// Job is a task run on a schedule.
type Job struct {
	// Name identifies the job in its cron process.
	Name string
	// Spec is when the job runs, in crontab syntax: see Parse.
	Spec string
	// Location is the time zone Spec is read in. Nil means UTC, so that
	// the nodes of a cluster read a spec alike whatever their own zone.
	Location *time.Location
	// Action is what a run does.
	Action Action
	// Overlap says what happens when a run is due while the job's previous
	// run still goes. By default, Allow.
	Overlap Overlap
	// Timeout, if positive, bounds a run: one that outlives it is told to
	// exit, with reason "timeout".
	Timeout time.Duration
	// StartingDeadline, if positive, is how late a run may start. A run
	// missed while the cron process was not running (a node restarted, a
	// leader changed) or was held up starts late if it is still within
	// the deadline, once however many of the job's runs were missed. By
	// default a missed run is skipped.
	StartingDeadline time.Duration
	// Disabled keeps the job without running it, until EnableJob.
	Disabled bool
	// OnFailure, if set, is told of every run that ends with a reason
	// other than normal: the error its Action returned, a panic, "timeout"
	// or "replaced". It runs on the cron process's goroutine, so it must not
	// block.
	OnFailure func(r Run, reason string)
}

// Run is one run of a job, as its Action sees it.
type Run struct {
	Job string
	// Time is the minute the run was due, in its job's Location. A run
	// that starts late, within its job's StartingDeadline, keeps it.
	Time time.Time
	// Deadline is when the run is told to exit, from its job's Timeout:
	// zero for a job with none.
	Deadline time.Time
}

// context is the run's context: its process's, which is cancelled when the
// run is told to exit, ending at its Deadline, so that a call made with it
// carries the time left to the callee.
func (r Run) context(p *grpcproc.Process[proto.Message]) (context.Context, context.CancelFunc) {
	if r.Deadline.IsZero() {
		return p.Context(), func() {}
	}
	return context.WithDeadline(p.Context(), r.Deadline)
}

// Action is what a run does, in a process of its own spawned by the cron
// process and linked to it: its return is the run's exit reason. A run
// told to exit (Overlap Replace, Timeout, or the cron process ending) has
// its context cancelled. A run that returns context.DeadlineExceeded once
// its Deadline has passed ends with reason "timeout", as one told to exit
// at its Deadline does.
type Action func(p *grpcproc.Process[proto.Message], r Run) error

// Func is an Action that calls fn with the run's context, which ends at the
// run's Deadline.
func Func(fn func(ctx context.Context, r Run) error) Action {
	return func(p *grpcproc.Process[proto.Message], r Run) error {
		ctx, cancel := r.context(p)
		defer cancel()
		return fn(ctx, r)
	}
}

// Send is an Action that sends what msg builds for the run to to. The run
// succeeds once the message is sent: what its receiver makes of it is the
// receiver's.
func Send[N proto.Message](to grpcproc.Addr[N], msg func(Run) N) Action {
	return func(p *grpcproc.Process[proto.Message], r Run) error { return p.Send(to, msg(r)) }
}

// Call is an Action that calls to with what req builds for the run. The run
// succeeds when to answers without an error, and fails with the error
// otherwise; the job's Timeout bounds the wait, and to sees the run's
// Deadline as the call's.
func Call[N proto.Message](to grpcproc.Addr[N], req func(Run) N) Action {
	return func(p *grpcproc.Process[proto.Message], r Run) error {
		ctx, cancel := r.context(p)
		defer cancel()
		_, err := p.Call[proto.Message](ctx, to, req(r))
		return err
	}
}

// Overlap says what a job does when a run is due while its previous run
// still goes.
type Overlap uint8

const (
	// Allow starts the run beside the previous one.
	Allow Overlap = iota
	// Forbid skips the run: the previous one goes on.
	Forbid
	// Replace tells the previous run to exit, with reason "replaced", and
	// starts the new one.
	Replace
)

var overlaps = [...]string{"allow", "forbid", "replace"}

func (o Overlap) String() string {
	if int(o) < len(overlaps) {
		return overlaps[o]
	}
	return "Overlap(" + strconv.Itoa(int(o)) + ")"
}

// The exit reasons the cron process gives the runs it tells to exit.
const (
	ReasonTimeout  = "timeout"
	ReasonReplaced = "replaced"
)

var (
	// ErrNoJob is the answer to a change to a job the cron process does
	// not have.
	ErrNoJob = errors.New("cron: no such job")
	// ErrJobExists is the answer to AddJob with a name the cron process
	// already has.
	ErrJobExists = errors.New("cron: a job has that name")
)

// Spec describes a cron process.
type Spec struct {
	// Jobs run from the start of the minute after the process starts.
	Jobs []Job
	// Resume is the State an earlier cron process reported through
	// OnState: each job's last run, from which a job with a
	// StartingDeadline catches up on what it missed since.
	Resume *cronv1.State
	// OnState, if set, is given the process's State every time a job
	// starts a run. It runs on the cron process's goroutine, so it must
	// not block. A cron process that a supervisor restarts resumes from the
	// last State it reported, whether OnState is set or not.
	OnState func(*cronv1.State)
}

// Start checks spec and spawns a cron process for it on n. opts apply to
// the process: a name, to reach it by.
func Start(n *grpcproc.Node, spec Spec, opts ...grpcproc.SpawnOption) (grpcproc.Addr[*cronv1.Control], error) {
	s, err := build(spec)
	if err != nil {
		return grpcproc.Addr[*cronv1.Control]{}, err
	}
	return n.Spawn(s.run, s.options(opts)...)
}

// Child checks spec and returns a child for a supervisor, registered as
// name unless name is empty, that runs a cron process for it.
func Child(name string, spec Spec, opts ...grpcproc.SpawnOption) (actor.ChildSpec, error) {
	s, err := build(spec)
	if err != nil {
		return actor.ChildSpec{}, err
	}
	return actor.ChildFunc(name, s.run, s.options(opts)...), nil
}

// AddJob adds job to the cron process cron, which must run on n: a job
// holds Go functions, which no message can carry. The job's runs are due
// from the minute after it is added.
func AddJob(ctx context.Context, n *grpcproc.Node, cron grpcproc.Target, job Job) error {
	j, err := newJob(job)
	if err != nil {
		return err
	}
	id := requestID.Add(1)
	requests.Store(id, j)
	defer requests.Delete(id)
	return control(ctx, n, cron, &cronv1.Control{Op: &cronv1.Control_Add{Add: id}})
}

// RemoveJob removes the job called name from cron, which may run on any
// node. Its runs already going go on.
func RemoveJob(ctx context.Context, n *grpcproc.Node, cron grpcproc.Target, name string) error {
	return control(ctx, n, cron, &cronv1.Control{Op: &cronv1.Control_Remove{Remove: name}})
}

// EnableJob runs a disabled job again, from the minute after. Runs it
// missed while disabled are not caught up.
func EnableJob(ctx context.Context, n *grpcproc.Node, cron grpcproc.Target, name string) error {
	return control(ctx, n, cron, &cronv1.Control{Op: &cronv1.Control_Enable{Enable: name}})
}

// DisableJob keeps a job but runs it no more until EnableJob. Its runs
// already going go on.
func DisableJob(ctx context.Context, n *grpcproc.Node, cron grpcproc.Target, name string) error {
	return control(ctx, n, cron, &cronv1.Control{Op: &cronv1.Control_Disable{Disable: name}})
}

// requests holds the jobs AddJob hands to cron processes, by id.
var (
	requests  sync.Map
	requestID atomic.Uint64
)

// control calls cron with c. Its answer, ErrNoJob or ErrJobExists, comes
// back as a *grpcproc.RemoteError, which is that sentinel to errors.Is.
func control(ctx context.Context, n *grpcproc.Node, cron grpcproc.Target, c *cronv1.Control) error {
	_, err := n.CallTo[*emptypb.Empty](ctx, cron, c)
	return err
}

// job is a Job as a cron process holds it.
type job struct {
	Job
	sched *Schedule
	// since is when the job was added or enabled: no run is owed before.
	since time.Time
	// last is the minute the job's last run was due.
	last    time.Time
	runs    map[grpcproc.Ref]*run
	failure string // why the last run that failed did
}

type run struct {
	pid      grpcproc.PID
	r        Run
	deadline time.Time // when it is told to exit, if the job has a Timeout
}

func newJob(j Job) (*job, error) {
	if j.Name == "" {
		return nil, errors.New("cron: a job needs a Name")
	}
	sched, err := parse(j.Spec)
	switch {
	case err != nil:
		return nil, fmt.Errorf("cron: job %q: %w", j.Name, err)
	case j.Action == nil:
		return nil, fmt.Errorf("cron: job %q has no Action", j.Name)
	case int(j.Overlap) >= len(overlaps):
		return nil, fmt.Errorf("cron: job %q: unknown %v", j.Name, j.Overlap)
	case j.Timeout < 0, j.StartingDeadline < 0:
		return nil, fmt.Errorf("cron: job %q: a negative Timeout or StartingDeadline", j.Name)
	}
	j.Location = cmp.Or(j.Location, time.UTC)
	return &job{Job: j, sched: sched}, nil
}

// fresh is a copy of j with nothing run yet, for a process to start from.
func (j *job) fresh(since time.Time) *job {
	return &job{Job: j.Job, sched: j.sched, since: since, runs: map[grpcproc.Ref]*run{}}
}

// due is the latest minute up to now at which a run of j was due and may
// still start: now itself, or, within the job's StartingDeadline, one it
// missed.
func (j *job) due(now time.Time) (time.Time, bool) {
	from := later(later(j.last, j.since), now.Add(-time.Minute-j.StartingDeadline))
	var due time.Time
	for t := j.sched.Next(from.In(j.Location)); !t.IsZero() && !t.After(now); t = j.sched.Next(t) {
		due = t
	}
	return due, !due.IsZero()
}

func later(a, b time.Time) time.Time {
	if a.After(b) {
		return a
	}
	return b
}

// process is what Start and Child spawn, again at every restart.
type process struct {
	jobs    []*job
	onState func(*cronv1.State)
	// resume is the last State: Spec's, then what the process reported.
	resume *cronv1.State
	// cur is the running process's state, for inspect, which grpcproc runs
	// on that process's own goroutine.
	cur *cron
}

func build(spec Spec) (*process, error) {
	s := &process{onState: spec.OnState, resume: spec.Resume}
	seen := map[string]bool{}
	for _, jb := range spec.Jobs {
		j, err := newJob(jb)
		if err != nil {
			return nil, err
		}
		if seen[j.Name] {
			return nil, fmt.Errorf("cron: two jobs named %q", j.Name)
		}
		seen[j.Name] = true
		s.jobs = append(s.jobs, j)
	}
	return s, nil
}

func (s *process) options(opts []grpcproc.SpawnOption) []grpcproc.SpawnOption {
	return append([]grpcproc.SpawnOption{grpcproc.WithLabel("cron"), grpcproc.WithInspect(s.inspect)}, opts...)
}

func (s *process) inspect() map[string]string { return s.cur.inspect() }

func (s *process) run(p *grpcproc.Process[*cronv1.Control]) error {
	now := time.Now().Truncate(time.Minute)
	// The minute it starts in is checked at once: a job resumed from a
	// State may owe a run due in it, which the process that reported the
	// State did not start. A job it did not resume is owed nothing yet.
	c := &cron{p: p, checked: now.Add(-time.Minute), report: s.report}
	for _, j := range s.jobs {
		f := j.fresh(now)
		if t, ok := s.resume.GetLastRun()[j.Name]; ok {
			// It ran before this process: what it missed since is owed.
			f.last, f.since = t.AsTime().In(j.Location), time.Time{}
		}
		c.jobs = append(c.jobs, f)
	}
	s.cur = c
	return c.loop()
}

func (s *process) report(st *cronv1.State) {
	s.resume = st
	if s.onState != nil {
		s.onState(st)
	}
}

// cron is a running cron process. It is touched by its own goroutine only.
type cron struct {
	p    *grpcproc.Process[*cronv1.Control]
	jobs []*job
	// checked is the last minute whose due runs have started.
	checked time.Time
	report  func(*cronv1.State)
}

func (c *cron) loop() error {
	for {
		now := time.Now()
		c.expire(now)
		if !now.Before(c.checked.Add(time.Minute)) {
			c.tick(now.Truncate(time.Minute))
			continue
		}
		m, err := c.p.ReceiveTimeout(time.Until(c.wake()))
		switch {
		case errors.Is(err, context.DeadlineExceeded):
			continue
		case err != nil:
			return err
		case m.Down != nil:
			c.ended(*m.Down)
		case m.IsCall():
			_ = m.Reply(&emptypb.Empty{}, c.control(m))
		default:
			c.p.Log().Warn("cron: dropped a message sent without a call", "from", m.From)
		}
	}
}

// wake is when the process has something to do next: the next minute, or a
// run's Timeout.
func (c *cron) wake() time.Time {
	t := c.checked.Add(time.Minute)
	for _, j := range c.jobs {
		for _, r := range j.runs {
			if !r.deadline.IsZero() && r.deadline.Before(t) {
				t = r.deadline
			}
		}
	}
	return t
}

// tick starts the runs due by now, a minute.
func (c *cron) tick(now time.Time) {
	started := false
	for _, j := range c.jobs {
		if j.Disabled {
			continue
		}
		if due, ok := j.due(now); ok {
			c.start(j, due)
			started = true
		}
	}
	c.checked = now
	if started {
		c.report(c.state())
	}
}

func (c *cron) start(j *job, due time.Time) {
	j.last = due
	r := Run{Job: j.Name, Time: due}
	if j.Timeout > 0 {
		r.Deadline = time.Now().Add(j.Timeout)
	}
	if len(j.runs) > 0 {
		switch j.Overlap {
		case Forbid:
			c.p.Log().Info("cron: skipped a run, the previous one still goes", "job", j.Name, "due", due)
			return
		case Replace:
			for _, prev := range j.runs {
				_ = c.p.Exit(prev.pid, ReasonReplaced)
			}
		}
	}
	action := j.Action
	a, ref, err := c.p.SpawnMonitor[proto.Message](func(p *grpcproc.Process[proto.Message]) error {
		err := action(p, r)
		// The run's context got to its Deadline before the cron process
		// told it to exit: the same end, for the same reason.
		if !r.Deadline.IsZero() && errors.Is(err, context.DeadlineExceeded) && !time.Now().Before(r.Deadline) {
			return &grpcproc.ExitError{Reason: ReasonTimeout}
		}
		return err
	}, grpcproc.LinkParent(), grpcproc.WithLabel("cron:"+j.Name))
	if err != nil {
		c.failed(j, r, err.Error())
		return
	}
	j.runs[ref] = &run{pid: a.PID(), r: r, deadline: r.Deadline}
}

// expire tells the runs past their Timeout to exit.
func (c *cron) expire(now time.Time) {
	for _, j := range c.jobs {
		for _, r := range j.runs {
			if !r.deadline.IsZero() && !now.Before(r.deadline) {
				r.deadline = time.Time{}
				_ = c.p.Exit(r.pid, ReasonTimeout)
			}
		}
	}
}

// ended handles a run's Down.
func (c *cron) ended(d grpcproc.Down) {
	for _, j := range c.jobs {
		if r, ok := j.runs[d.Ref]; ok {
			delete(j.runs, d.Ref)
			if d.Reason == grpcproc.ReasonNormal {
				j.failure = ""
				return
			}
			c.failed(j, r.r, d.Reason)
			return
		}
	}
	// A run of a job since removed: nobody is left to tell.
}

func (c *cron) failed(j *job, r Run, reason string) {
	j.failure = reason
	c.p.Log().Warn("cron: a run failed", "job", j.Name, "due", r.Time, "reason", reason)
	if j.OnFailure != nil {
		j.OnFailure(r, reason)
	}
}

func (c *cron) control(m grpcproc.Msg[*cronv1.Control]) error {
	now := time.Now().Truncate(time.Minute)
	switch op := m.Body.GetOp().(type) {
	case *cronv1.Control_Add:
		if m.From.Node != c.p.Node().Name() {
			return errors.New("cron: AddJob: only from the cron process's node")
		}
		v, ok := requests.Load(op.Add)
		if !ok {
			return errors.New("cron: AddJob: the job was withdrawn")
		}
		j := v.(*job)
		if c.find(j.Name) != nil {
			return ErrJobExists
		}
		c.jobs = append(c.jobs, j.fresh(now))
	case *cronv1.Control_Remove:
		if c.find(op.Remove) == nil {
			return ErrNoJob
		}
		c.jobs = slices.DeleteFunc(c.jobs, func(j *job) bool { return j.Name == op.Remove })
	case *cronv1.Control_Enable:
		j := c.find(op.Enable)
		if j == nil {
			return ErrNoJob
		}
		if j.Disabled {
			j.Disabled, j.since = false, now
		}
	case *cronv1.Control_Disable:
		j := c.find(op.Disable)
		if j == nil {
			return ErrNoJob
		}
		j.Disabled = true
	default:
		return errors.New("cron: a Control without an op")
	}
	return nil
}

func (c *cron) find(name string) *job {
	for _, j := range c.jobs {
		if j.Name == name {
			return j
		}
	}
	return nil
}

// state is what a successor resumes from: each job's last run.
func (c *cron) state() *cronv1.State {
	st := &cronv1.State{LastRun: map[string]*timestamppb.Timestamp{}}
	for _, j := range c.jobs {
		if !j.last.IsZero() {
			st.LastRun[j.Name] = timestamppb.New(j.last)
		}
	}
	return st
}

// inspect publishes every job: its spec and zone, when it runs next and
// last ran, and how its runs are going.
func (c *cron) inspect() map[string]string {
	out := make(map[string]string, len(c.jobs))
	for _, j := range c.jobs {
		parts := []string{j.Spec + " " + j.Location.String()}
		switch next := j.sched.Next(later(c.checked, j.last).In(j.Location)); {
		case j.Disabled:
			parts = append(parts, "disabled")
		case !next.IsZero():
			parts = append(parts, "next "+next.Format(time.RFC3339))
		}
		if !j.last.IsZero() {
			parts = append(parts, "last "+j.last.Format(time.RFC3339))
		}
		if len(j.runs) > 0 {
			parts = append(parts, "running "+strconv.Itoa(len(j.runs)))
		}
		if j.failure != "" {
			parts = append(parts, "failed: "+j.failure)
		}
		out[j.Name] = strings.Join(parts, ", ")
	}
	return out
}
