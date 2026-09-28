# grpcproc/cron

Jobs on crontab schedules for [grpcproc](https://floatdrop.github.io/grpcproc/),
in a process the application starts. Each run is a process of its own, so a
run's exit reason is its result, an overlapping run can be skipped or
replaced, and a slow one is told to exit. A separate module so it can be
versioned on its own; it depends on grpcproc alone.

```sh
go get github.com/floatdrop/grpcproc/cron
```

```go
c, err := cron.Start(node, cron.Spec{Jobs: []cron.Job{{
    Name:     "nightly-report",
    Spec:     "10 3 * * *",
    Location: berlin, // UTC if nil
    Action:   cron.Call(reporter, func(r cron.Run) *reportpb.Make { return &reportpb.Make{Day: r.Time.Format(time.DateOnly)} }),
    Overlap:  cron.Forbid,
    Timeout:  10 * time.Minute,
    OnFailure: func(r cron.Run, reason string) { alerts.Page("report", reason) },
}}}, grpcproc.WithName("cron"))

// or under a supervisor
child, err := cron.Child("cron", spec)
```

## Schedules

Five fields: minute, hour, day of month, month, day of week. Each is `*` or a
list of values, ranges and steps (`0,30`, `9-17`, `*/15`, `0-30/10`, `5/20`).
Months and weekdays take names (`JAN`, `mon-fri`); Sunday is `0` or `7`. When
both day fields are restricted a day matching either runs; when one starts
with `*` a day must match both, as in Vixie cron.

| Extension | Field | Means |
| --- | --- | --- |
| `L` | day of month | the month's last day |
| `5L` | day of week | the month's last Friday |
| `5#2` | day of week | the month's second Friday |
| `@hourly`, `@daily` (`@midnight`), `@weekly`, `@monthly`, `@yearly` (`@annually`) | whole spec | `0 * * * *`, `0 0 * * *`, `0 0 * * 0`, `0 0 1 * *`, `0 0 1 1 *` |

`cron.Parse(spec)` returns the `Schedule`, whose `Next(t)` is the first
minute after `t` it names, in `t`'s zone: a preview of when a job runs.

## Time zones and clock changes

A job's `Spec` is read in its `Location`, UTC when nil, so every node reads it
alike. A minute that clocks skip when they go forward does not exist, and a
job due then does not run that day. A minute they repeat when they go back
runs the first time only: `30 1 * * *` in New York runs at 01:30 EDT on
November 1, not again at 01:30 EST. `Next` gives the same answers, so it
previews exactly what runs.

The process checks the time at the start of every minute, by the wall clock:
a clock stepped back does not run a minute twice, and one stepped forward
skips minutes as a stopped process would.

## Runs

Each run is a process spawned by the cron process, linked to it (it ends if
the cron process does), labelled `cron:<job>`. Its `Action` is one of:

| Action | The run |
| --- | --- |
| `cron.Send(to, build)` | sends `build(run)` to `to`, and succeeds once sent |
| `cron.Call(to, build)` | calls `to`, on this node or another, and succeeds if the answer is not an error |
| `cron.Func(fn)` | runs `fn(ctx, run)`; `ctx` ends when the run is told to exit |
| any `func(*grpcproc.Process[proto.Message], cron.Run) error` | is that process |

`Run.Time` is the minute the run was due, in the job's zone.

A run fails with any exit reason but `normal`: the error it returned, a panic,
`timeout` or `replaced`. `OnFailure` hears of it on the cron process's
goroutine (it must not block), and the process logs it and publishes it
through its inspect map, so `grpcprocctl inspect` shows every job, when it
runs next and last ran, how many runs are going and why the last one failed.

| `Overlap` | A run due while the job's previous run still goes |
| --- | --- |
| `Allow` (default) | starts beside it |
| `Forbid` | is skipped |
| `Replace` | starts, once the previous one is told to exit with `replaced` |

## Missed runs

A run is due at the start of its minute. One missed while the cron process
was not running, or was held up, is skipped, unless the job has a
`StartingDeadline`: then the latest run it missed starts late, once, if it is
still within the deadline. The process reports its `*cronv1.State`, each
job's last run, through `Spec.OnState` whenever a run starts, and a process
started with it as `Spec.Resume` catches up from there. It checks the minute
it starts in at once: a run due in it that the process it takes over from did
not start, starts then. A supervisor that restarts a cron process resumes it
from its last state by itself.

That is how a job runs once in a cluster: a
[grpcproc/leader](../leader/README.md) singleton runs the cron process on the
leader, checkpoints its state, and the next leader resumes from it.

```go
leader.Start(node, leader.Spec[*cronv1.State]{
    Cluster: "cron",
    Voters:  []string{"a", "b", "c"},
    Singleton: func(l *leader.Lease[*cronv1.State], last *cronv1.State) (actor.ChildSpec, error) {
        return cron.Child("cron", cron.Spec{Jobs: jobs, Resume: last, OnState: l.Save})
    },
})
```

## Changing jobs

```go
cron.AddJob(ctx, node, c, job)            // only on c's node: a job holds Go functions
cron.DisableJob(ctx, node, c, "nightly")  // from any node
cron.EnableJob(ctx, node, c, "nightly")   // runs from the next minute; nothing missed is caught up
cron.RemoveJob(ctx, node, c, "nightly")
```

Runs already going go on. A cron process a supervisor restarts starts again
from its `Spec`'s jobs.

From a terminal, [`grpcprocctl cron`](../tools/README.md#cron-jobs) lists every
cron process of the cluster and its jobs, and enables, disables or removes
one; an AI agent gets the same as MCP tools.
