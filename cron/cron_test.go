package cron_test

import (
	"bytes"
	"cmp"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"strings"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/emptypb"
	"google.golang.org/protobuf/types/known/timestamppb"
	"google.golang.org/protobuf/types/known/wrapperspb"

	"github.com/floatdrop/grpcproc"
	"github.com/floatdrop/grpcproc/actor"
	"github.com/floatdrop/grpcproc/cron"
	cronv1 "github.com/floatdrop/grpcproc/cron/proto/grpcproc/cron/v1"
	"github.com/floatdrop/grpcproc/grpcproctest"
)

type logBuf struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (l *logBuf) Write(p []byte) (int, error) { l.mu.Lock(); defer l.mu.Unlock(); return l.b.Write(p) }
func (l *logBuf) String() string              { l.mu.Lock(); defer l.mu.Unlock(); return l.b.String() }

// at runs f in a synctest bubble whose clock reads start, with a node that
// logs to the returned buffer. Time passes as f sleeps.
func at(t *testing.T, start string, f func(t *testing.T, n *grpcproc.Node, logs *logBuf)) {
	t.Helper()
	when, err := time.Parse(time.RFC3339, start)
	if err != nil {
		t.Fatal(err)
	}
	synctest.Test(t, func(t *testing.T) {
		time.Sleep(time.Until(when))
		logs := &logBuf{}
		n, err := grpcproc.NewNode(grpcproc.Config{Name: "a", Resolver: grpcproc.StaticResolver{}, Logger: slog.New(slog.NewTextHandler(logs, nil))})
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = n.Stop(context.Background()) }()
		f(t, n, logs)
	})
}

// sleepUntil advances the bubble's clock to when, and lets every process
// settle.
func sleepUntil(t *testing.T, when string) {
	t.Helper()
	w, err := time.Parse(time.RFC3339, when)
	if err != nil {
		t.Fatal(err)
	}
	time.Sleep(time.Until(w))
	synctest.Wait()
}

// runs records the runs of the jobs whose Action it builds.
type runs struct {
	mu   sync.Mutex
	seen []cron.Run
}

func (r *runs) record(run cron.Run) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.seen = append(r.seen, run)
}

func (r *runs) action() cron.Action {
	return cron.Func(func(_ context.Context, run cron.Run) error { r.record(run); return nil })
}

// String lists the runs by when they were due, then by job: runs due
// together start together, in no particular order.
func (r *runs) String() string {
	r.mu.Lock()
	defer r.mu.Unlock()
	seen := slices.SortedFunc(slices.Values(r.seen), func(a, b cron.Run) int {
		return cmp.Or(a.Time.Compare(b.Time), strings.Compare(a.Job, b.Job))
	})
	out := make([]string, len(seen))
	for i, run := range seen {
		out[i] = run.Job + " " + run.Time.Format("15:04 MST")
	}
	return strings.Join(out, "; ")
}

// failures records OnFailure.
type failures struct {
	mu   sync.Mutex
	seen []string
}

func (f *failures) on(r cron.Run, reason string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.seen = append(f.seen, r.Time.Format("15:04")+" "+reason)
}

func (f *failures) String() string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return strings.Join(f.seen, "; ")
}

// blocking is an Action that holds its run until the run is told to exit.
func blocking(started *runs) cron.Action {
	return cron.Func(func(ctx context.Context, r cron.Run) error {
		started.record(r)
		<-ctx.Done()
		return ctx.Err()
	})
}

func inspect(t *testing.T, n *grpcproc.Node, pid grpcproc.PID) map[string]string {
	t.Helper()
	m, err := n.Inspect(t.Context(), pid)
	if err != nil {
		t.Fatal(err)
	}
	return m
}

func check(t *testing.T, what, got, want string) {
	t.Helper()
	if got != want {
		t.Errorf("%s:\n got %s\nwant %s", what, got, want)
	}
}

func TestRunsOnSchedule(t *testing.T) {
	at(t, "2026-09-28T10:07:30Z", func(t *testing.T, n *grpcproc.Node, _ *logBuf) {
		ny := zone(t, "America/New_York")
		r := &runs{}
		c, err := cron.Start(n, cron.Spec{Jobs: []cron.Job{
			{Name: "quarter", Spec: "*/15 * * * *", Action: r.action()},
			{Name: "ny", Spec: "0 7 * * *", Location: ny, Action: r.action()},
			{Name: "every", Spec: "* * * * *", Action: r.action(), Disabled: true},
		}}, grpcproc.WithName("cron"))
		if err != nil {
			t.Fatal(err)
		}
		sleepUntil(t, "2026-09-28T11:00:30Z")
		check(t, "runs", r.String(), "quarter 10:15 UTC; quarter 10:30 UTC; quarter 10:45 UTC; ny 07:00 EDT; quarter 11:00 UTC")

		info := inspect(t, n, c.PID())
		check(t, "quarter", info["quarter"], "*/15 * * * * UTC, next 2026-09-28T11:15:00Z, last 2026-09-28T11:00:00Z")
		check(t, "ny", info["ny"], "0 7 * * * America/New_York, next 2026-09-29T07:00:00-04:00, last 2026-09-28T07:00:00-04:00")
		check(t, "every", info["every"], "* * * * * UTC, disabled")
		for _, p := range n.Processes() {
			if p.PID == c.PID() && p.Label != "cron" {
				t.Errorf("label %q", p.Label)
			}
		}
	})
}

func TestFirstRunIsTheMinuteAfterStart(t *testing.T) {
	at(t, "2026-09-28T10:07:00Z", func(t *testing.T, n *grpcproc.Node, _ *logBuf) {
		r := &runs{}
		if _, err := cron.Start(n, cron.Spec{Jobs: []cron.Job{{Name: "every", Spec: "* * * * *", Action: r.action()}}}); err != nil {
			t.Fatal(err)
		}
		sleepUntil(t, "2026-09-28T10:09:59Z")
		check(t, "runs", r.String(), "every 10:08 UTC; every 10:09 UTC")
	})
}

func TestRunsAreProcesses(t *testing.T) {
	at(t, "2026-09-28T10:07:30Z", func(t *testing.T, n *grpcproc.Node, _ *logBuf) {
		labels := make(chan string, 1)
		parents := make(chan grpcproc.PID, 1)
		c, err := cron.Start(n, cron.Spec{Jobs: []cron.Job{{Name: "report", Spec: "* * * * *", Action: func(p *grpcproc.Process[proto.Message], r cron.Run) error {
			info, _ := p.Node().Process(p.PID())
			labels <- info.Label
			parents <- p.Parent()
			return nil
		}}}})
		if err != nil {
			t.Fatal(err)
		}
		sleepUntil(t, "2026-09-28T10:08:30Z")
		check(t, "label", <-labels, "cron:report")
		if got := <-parents; got != c.PID() {
			t.Errorf("parent %v, want %v", got, c.PID())
		}
	})
}

func TestSendAndCall(t *testing.T) {
	at(t, "2026-09-28T10:07:30Z", func(t *testing.T, n *grpcproc.Node, _ *logBuf) {
		got := make(chan string, 4)
		target, err := n.Spawn(func(p *grpcproc.Process[*wrapperspb.StringValue]) error {
			for {
				m, err := p.Receive()
				if err != nil {
					return err
				}
				got <- m.Body.GetValue()
				if m.IsCall() {
					var err error
					if strings.HasPrefix(m.Body.GetValue(), "fail") {
						err = errors.New("refused")
					}
					_ = m.Reply(&emptypb.Empty{}, err)
				}
			}
		})
		if err != nil {
			t.Fatal(err)
		}
		msg := func(prefix string) func(cron.Run) *wrapperspb.StringValue {
			return func(r cron.Run) *wrapperspb.StringValue {
				return wrapperspb.String(prefix + " " + r.Time.Format("15:04"))
			}
		}
		f := &failures{}
		_, err = cron.Start(n, cron.Spec{Jobs: []cron.Job{
			{Name: "send", Spec: "8 * * * *", Action: cron.Send(target, msg("sent"))},
			{Name: "call", Spec: "9 * * * *", Action: cron.Call(target, msg("called")), OnFailure: f.on},
			{Name: "fail", Spec: "10 * * * *", Action: cron.Call(target, msg("fail")), OnFailure: f.on},
		}})
		if err != nil {
			t.Fatal(err)
		}
		sleepUntil(t, "2026-09-28T10:10:30Z")
		close(got)
		var all []string
		for s := range got {
			all = append(all, s)
		}
		check(t, "received", strings.Join(all, "; "), "sent 10:08; called 10:09; fail 10:10")
		check(t, "failures", f.String(), "10:10 refused")
	})
}

func TestFailures(t *testing.T) {
	at(t, "2026-09-28T10:07:30Z", func(t *testing.T, n *grpcproc.Node, logs *logBuf) {
		f := &failures{}
		flaky := 0
		c, err := cron.Start(n, cron.Spec{Jobs: []cron.Job{
			{Name: "error", Spec: "* * * * *", OnFailure: f.on, Action: cron.Func(func(context.Context, cron.Run) error {
				flaky++
				if flaky == 1 {
					return errors.New("disk full")
				}
				return nil
			})},
			{Name: "panic", Spec: "8 * * * *", OnFailure: f.on, Action: cron.Func(func(context.Context, cron.Run) error { panic("kaboom") })},
			{Name: "quiet", Spec: "8 * * * *", Action: cron.Func(func(context.Context, cron.Run) error { return errors.New("unheard") })},
		}})
		if err != nil {
			t.Fatal(err)
		}
		sleepUntil(t, "2026-09-28T10:08:30Z")
		got := f.String()
		if !strings.Contains(got, "10:08 disk full") || !strings.Contains(got, "10:08 panic: kaboom") {
			t.Errorf("failures %s", got)
		}
		info := inspect(t, n, c.PID())
		check(t, "error", info["error"], "* * * * * UTC, next 2026-09-28T10:09:00Z, last 2026-09-28T10:08:00Z, failed: disk full")
		check(t, "quiet", info["quiet"], "8 * * * * UTC, next 2026-09-28T11:08:00Z, last 2026-09-28T10:08:00Z, failed: unheard")
		if !strings.Contains(logs.String(), "unheard") {
			t.Errorf("a failure OnFailure does not hear is logged: %s", logs)
		}
		sleepUntil(t, "2026-09-28T10:09:30Z")
		check(t, "error, once it succeeds", inspect(t, n, c.PID())["error"], "* * * * * UTC, next 2026-09-28T10:10:00Z, last 2026-09-28T10:09:00Z")
	})
}

func TestOverlap(t *testing.T) {
	for _, tc := range []struct {
		overlap  cron.Overlap
		started  string
		running  string
		failures string
	}{
		{cron.Allow, "job 10:08 UTC; job 10:09 UTC; job 10:10 UTC", "running 3", ""},
		{cron.Forbid, "job 10:08 UTC", "running 1", ""},
		{cron.Replace, "job 10:08 UTC; job 10:09 UTC; job 10:10 UTC", "running 1, failed: replaced", "10:08 replaced; 10:09 replaced"},
	} {
		t.Run(tc.overlap.String(), func(t *testing.T) {
			at(t, "2026-09-28T10:07:30Z", func(t *testing.T, n *grpcproc.Node, _ *logBuf) {
				started, f := &runs{}, &failures{}
				c, err := cron.Start(n, cron.Spec{Jobs: []cron.Job{{Name: "job", Spec: "* * * * *", Overlap: tc.overlap, Action: blocking(started), OnFailure: f.on}}})
				if err != nil {
					t.Fatal(err)
				}
				sleepUntil(t, "2026-09-28T10:10:30Z")
				check(t, "started", started.String(), tc.started)
				check(t, "failures", f.String(), tc.failures)
				check(t, "job", inspect(t, n, c.PID())["job"], "* * * * * UTC, next 2026-09-28T10:11:00Z, last 2026-09-28T10:10:00Z, "+tc.running)
			})
		})
	}
	check(t, "unknown", cron.Overlap(7).String(), "Overlap(7)")
}

func TestTimeout(t *testing.T) {
	at(t, "2026-09-28T10:07:30Z", func(t *testing.T, n *grpcproc.Node, _ *logBuf) {
		started, f := &runs{}, &failures{}
		_, err := cron.Start(n, cron.Spec{Jobs: []cron.Job{
			{Name: "slow", Spec: "* * * * *", Timeout: 10 * time.Second, Action: blocking(started), OnFailure: f.on},
			{Name: "slower", Spec: "* * * * *", Timeout: 20 * time.Second, Action: blocking(started), OnFailure: f.on},
		}})
		if err != nil {
			t.Fatal(err)
		}
		sleepUntil(t, "2026-09-28T10:08:09Z")
		check(t, "before the timeout", f.String(), "")
		sleepUntil(t, "2026-09-28T10:08:10Z")
		check(t, "at the timeout", f.String(), "10:08 timeout")
		sleepUntil(t, "2026-09-28T10:08:20Z")
		check(t, "then", f.String(), "10:08 timeout; 10:08 timeout")
	})
}

func TestStartingDeadline(t *testing.T) {
	last := func(s string) *cronv1.State {
		w, _ := time.Parse(time.RFC3339, s)
		return &cronv1.State{LastRun: map[string]*timestamppb.Timestamp{
			"nightly": timestamppb.New(w), "tenth": timestamppb.New(w), "strict": timestamppb.New(w),
		}}
	}
	at(t, "2026-09-29T04:00:30Z", func(t *testing.T, n *grpcproc.Node, _ *logBuf) {
		r := &runs{}
		var states []*cronv1.State
		_, err := cron.Start(n, cron.Spec{
			Jobs: []cron.Job{
				// Missed at 03:10, and still within two hours.
				{Name: "nightly", Spec: "10 3 * * *", StartingDeadline: 2 * time.Hour, Action: r.action()},
				// Missed seven times since its last run, 04:00 included, which
				// came before the process's first minute: runs once, for 04:00.
				{Name: "tenth", Spec: "*/10 * * * *", StartingDeadline: time.Hour, Action: r.action()},
				// Missed, and not caught up.
				{Name: "strict", Spec: "10 3 * * *", Action: r.action()},
				// Owed nothing from before it started.
				{Name: "fresh", Spec: "10 3 * * *", StartingDeadline: 2 * time.Hour, Action: r.action()},
			},
			Resume:  last("2026-09-29T02:50:00Z"),
			OnState: func(s *cronv1.State) { states = append(states, s) },
		})
		if err != nil {
			t.Fatal(err)
		}
		sleepUntil(t, "2026-09-29T04:01:30Z")
		check(t, "caught up", r.String(), "nightly 03:10 UTC; tenth 04:00 UTC")
		if len(states) != 1 {
			t.Fatalf("%d states reported", len(states))
		}
		var got []string
		for name, ts := range states[0].GetLastRun() {
			got = append(got, name+" "+ts.AsTime().Format("15:04"))
		}
		slices.Sort(got)
		check(t, "state", strings.Join(got, "; "), "nightly 03:10; strict 02:50; tenth 04:00")
	})
	at(t, "2026-09-29T06:00:30Z", func(t *testing.T, n *grpcproc.Node, _ *logBuf) {
		r := &runs{}
		_, err := cron.Start(n, cron.Spec{
			Jobs:   []cron.Job{{Name: "nightly", Spec: "10 3 * * *", StartingDeadline: 2 * time.Hour, Action: r.action()}},
			Resume: last("2026-09-28T03:10:00Z"),
		})
		if err != nil {
			t.Fatal(err)
		}
		sleepUntil(t, "2026-09-29T06:01:30Z")
		check(t, "past the deadline", r.String(), "")
	})
}

// A cron process that takes over in the middle of a minute starts the run
// due in it that the one before did not: at once, not a minute later.
func TestResumeMidMinute(t *testing.T) {
	at(t, "2026-09-28T10:09:00.300Z", func(t *testing.T, n *grpcproc.Node, _ *logBuf) {
		r := &runs{}
		last, _ := time.Parse(time.RFC3339, "2026-09-28T10:08:00Z")
		_, err := cron.Start(n, cron.Spec{
			Jobs: []cron.Job{
				{Name: "owed", Spec: "* * * * *", Action: r.action()},
				{Name: "fresh", Spec: "* * * * *", Action: r.action()},
			},
			Resume: &cronv1.State{LastRun: map[string]*timestamppb.Timestamp{"owed": timestamppb.New(last)}},
		})
		if err != nil {
			t.Fatal(err)
		}
		synctest.Wait()
		check(t, "at once", r.String(), "owed 10:09 UTC")
		sleepUntil(t, "2026-09-28T10:10:30Z")
		check(t, "then", r.String(), "owed 10:09 UTC; fresh 10:10 UTC; owed 10:10 UTC")
	})
}

func TestControl(t *testing.T) {
	at(t, "2026-09-28T10:07:30Z", func(t *testing.T, n *grpcproc.Node, logs *logBuf) {
		ctx := t.Context()
		r := &runs{}
		c, err := cron.Start(n, cron.Spec{Jobs: []cron.Job{{Name: "a", Spec: "* * * * *", Action: r.action()}}}, grpcproc.WithName("cron"))
		if err != nil {
			t.Fatal(err)
		}
		if err := cron.AddJob(ctx, n, c, cron.Job{Name: "b", Spec: "* * * * *", Action: r.action()}); err != nil {
			t.Fatal(err)
		}
		if err := cron.AddJob(ctx, n, c, cron.Job{Name: "a", Spec: "* * * * *", Action: r.action()}); !errors.Is(err, cron.ErrJobExists) {
			t.Errorf("AddJob of a taken name: %v", err)
		}
		if err := cron.AddJob(ctx, n, c, cron.Job{Name: "c", Spec: "bad"}); err == nil {
			t.Error("AddJob of a bad job")
		}
		sleepUntil(t, "2026-09-28T10:08:30Z")
		check(t, "added", r.String(), "a 10:08 UTC; b 10:08 UTC")

		if err := cron.DisableJob(ctx, n, c, "a"); err != nil {
			t.Fatal(err)
		}
		if err := cron.RemoveJob(ctx, n, grpcproc.Named[*cronv1.Control]("a", "cron"), "b"); err != nil {
			t.Fatal(err)
		}
		sleepUntil(t, "2026-09-28T10:10:30Z")
		check(t, "disabled and removed", r.String(), "a 10:08 UTC; b 10:08 UTC")
		if _, ok := inspect(t, n, c.PID())["b"]; ok {
			t.Error("a removed job is still inspected")
		}

		// Enabled, it runs from the next minute on, without catching up.
		if err := cron.EnableJob(ctx, n, c, "a"); err != nil {
			t.Fatal(err)
		}
		if err := cron.EnableJob(ctx, n, c, "a"); err != nil {
			t.Fatal(err)
		}
		sleepUntil(t, "2026-09-28T10:11:30Z")
		check(t, "enabled", r.String(), "a 10:08 UTC; b 10:08 UTC; a 10:11 UTC")

		for name, err := range map[string]error{
			"remove":  cron.RemoveJob(ctx, n, c, "nope"),
			"enable":  cron.EnableJob(ctx, n, c, "nope"),
			"disable": cron.DisableJob(ctx, n, c, "nope"),
		} {
			if !errors.Is(err, cron.ErrNoJob) {
				t.Errorf("%s of an unknown job: %v", name, err)
			}
		}
		if err := cron.RemoveJob(ctx, n, grpcproc.Named[*cronv1.Control]("a", "nocron"), "a"); !errors.Is(err, grpcproc.ErrNoProc) {
			t.Errorf("RemoveJob from no cron process: %v", err)
		}
		for what, op := range map[string]*cronv1.Control{
			"without an op":         {},
			"the job was withdrawn": {Op: &cronv1.Control_Add{Add: 1 << 60}},
		} {
			if _, err := n.Call[*emptypb.Empty](ctx, c, op); err == nil || !strings.Contains(err.Error(), what) {
				t.Errorf("%s: %v", what, err)
			}
		}
		if err := n.Send(ctx, c, &cronv1.Control{}); err != nil {
			t.Fatal(err)
		}
		synctest.Wait()
		if !strings.Contains(logs.String(), "dropped a message sent without a call") {
			t.Errorf("a message sent without a call is not logged: %s", logs)
		}
	})
}

func TestAddJobOnlyFromItsNode(t *testing.T) {
	c := grpcproctest.New(t, "a", "b")
	if _, err := cron.Start(c.Node("a"), cron.Spec{}, grpcproc.WithName("cron")); err != nil {
		t.Fatal(err)
	}
	remote := grpcproc.Named[*cronv1.Control]("a", "cron")
	err := cron.AddJob(t.Context(), c.Node("b"), remote, cron.Job{Name: "x", Spec: "@daily", Action: cron.Func(func(context.Context, cron.Run) error { return nil })})
	if err == nil || !strings.Contains(err.Error(), "only from the cron process's node") {
		t.Errorf("AddJob from another node: %v", err)
	}
	if err := cron.RemoveJob(t.Context(), c.Node("b"), remote, "x"); !errors.Is(err, cron.ErrNoJob) {
		t.Errorf("RemoveJob from another node: %v", err)
	}
}

func TestInvalidSpecs(t *testing.T) {
	ok := cron.Func(func(context.Context, cron.Run) error { return nil })
	for want, spec := range map[string]cron.Spec{
		"cron: a job needs a Name":                  {Jobs: []cron.Job{{Spec: "@daily", Action: ok}}},
		`cron: job "x": "@often": unknown macro`:    {Jobs: []cron.Job{{Name: "x", Spec: "@often", Action: ok}}},
		`cron: job "x" has no Action`:               {Jobs: []cron.Job{{Name: "x", Spec: "@daily"}}},
		`cron: job "x": unknown Overlap(3)`:         {Jobs: []cron.Job{{Name: "x", Spec: "@daily", Action: ok, Overlap: 3}}},
		`cron: job "x": a negative Timeout`:         {Jobs: []cron.Job{{Name: "x", Spec: "@daily", Action: ok, Timeout: -1}}},
		`cron: job "x": a negative Timeout or Star`: {Jobs: []cron.Job{{Name: "x", Spec: "@daily", Action: ok, StartingDeadline: -1}}},
		`cron: two jobs named "x"`:                  {Jobs: []cron.Job{{Name: "x", Spec: "@daily", Action: ok}, {Name: "x", Spec: "@daily", Action: ok}}},
	} {
		if _, err := cron.Start(nil, spec); err == nil || !strings.HasPrefix(err.Error(), want) {
			t.Errorf("Start: %v, want %s", err, want)
		}
		if _, err := cron.Child("cron", spec); err == nil || !strings.HasPrefix(err.Error(), want) {
			t.Errorf("Child: %v, want %s", err, want)
		}
	}
}

func TestChild(t *testing.T) {
	at(t, "2026-09-28T10:07:30Z", func(t *testing.T, n *grpcproc.Node, _ *logBuf) {
		r := &runs{}
		child, err := cron.Child("cron", cron.Spec{Jobs: []cron.Job{{Name: "every", Spec: "* * * * *", StartingDeadline: time.Hour, Action: r.action()}}})
		if err != nil {
			t.Fatal(err)
		}
		if _, err := actor.Supervise(n, actor.Spec{Children: []actor.ChildSpec{child}}); err != nil {
			t.Fatal(err)
		}
		sleepUntil(t, "2026-09-28T10:08:30Z")
		first, _ := n.Whereis("cron")
		if err := n.Exit(t.Context(), first, "boom"); err != nil {
			t.Fatal(err)
		}
		synctest.Wait()
		if again, _ := n.Whereis("cron"); again == first || again.IsZero() {
			t.Fatalf("not restarted: %v", again)
		}
		sleepUntil(t, "2026-09-28T10:09:30Z")
		check(t, "runs across the restart", r.String(), "every 10:08 UTC; every 10:09 UTC")
	})
}

// A run due while the node stops cannot start: it is a failure like any
// other.
func TestRunDueWhileTheNodeStops(t *testing.T) {
	at(t, "2026-09-28T10:07:30Z", func(t *testing.T, n *grpcproc.Node, _ *logBuf) {
		hold := make(chan struct{})
		f := &failures{}
		var once sync.Once
		_, err := cron.Start(n, cron.Spec{Jobs: []cron.Job{{
			Name: "job", Spec: "* * * * *",
			Action: cron.Func(func(context.Context, cron.Run) error { return errors.New("failed") }),
			OnFailure: func(r cron.Run, reason string) {
				f.on(r, reason)
				once.Do(func() { <-hold }) // the cron process is busy, past the next minute
			},
		}}})
		if err != nil {
			t.Fatal(err)
		}
		sleepUntil(t, "2026-09-28T10:08:30Z")
		stopped := make(chan error)
		go func() { stopped <- n.Stop(context.Background()) }()
		sleepUntil(t, "2026-09-28T10:09:30Z")
		close(hold)
		if err := <-stopped; err != nil {
			t.Fatal(err)
		}
		check(t, "failures", f.String(), fmt.Sprintf("10:08 failed; 10:09 %v", grpcproc.ErrNodeStopped))
	})
}
