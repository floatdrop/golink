import cronExample from '../../../cron/example_test.go?raw';
import singleton from '../../../examples/singleton/singleton_test.go?raw';

import { Code, region } from '../code.tsx';
import { A, C, Ext, Table } from '../components/prose.tsx';
import { file } from '../config.ts';
import type { Doc } from './types.ts';

export const guidesCron: Doc = {
	path: 'guides/cron/',
	title: 'Cron jobs',
	description:
		'Jobs on crontab schedules, in a process the application starts: every run is a process of its own, whose exit reason is its result.',
	lead: (
		<p>
			<C>grpcproc/cron</C> runs jobs on crontab schedules from a process you start, like any other,
			on a node or under a supervisor. Every run is a process too, spawned by the cron process and
			linked to it, so a run's exit reason is its result, a run still going when the next is due can
			be left alone, skipped or replaced, and one that takes too long is told to exit. It is a
			separate module, versioned on its own.
		</p>
	),
	sections: [
		{
			id: 'start',
			title: 'Install and start',
			body: (
				<>
					<Code lang="sh">{'go get github.com/floatdrop/grpcproc/cron'}</Code>
					<Code>{`c, err := cron.Start(node, cron.Spec{Jobs: []cron.Job{{
	Name:     "nightly-report",
	Spec:     "10 3 * * *",
	Location: berlin, // UTC if nil
	Action: cron.Call(reporter, func(r cron.Run) *reportpb.Make {
		return &reportpb.Make{Day: r.Time.Format(time.DateOnly)}
	}),
	Overlap:   cron.Forbid,
	Timeout:   10 * time.Minute,
	OnFailure: func(r cron.Run, reason string) { alerts.Page("report", reason) },
}}}, grpcproc.WithName("cron"))`}</Code>
					<p>
						<C>Start</C> checks the whole spec first, every schedule parsed and every job
						complete, so a mistake is its error rather than a process that fails later. It returns
						the cron process's address. Under a supervisor, <C>cron.Child(name, spec)</C> returns
						the <C>actor.ChildSpec</C> instead, checked the same way. Every node that starts a cron
						process runs its jobs; for a job that runs once in a cluster, see{' '}
						<A to="guides/cron/#cluster">the last section</A>.
					</p>
				</>
			)
		},
		{
			id: 'schedules',
			title: 'Schedules',
			body: (
				<>
					<p>
						A spec is five fields: minute, hour, day of month, month and day of week. A field is{' '}
						<C>*</C> or a list of values, ranges and steps: <C>0,30</C>, <C>9-17</C>, <C>*/15</C>,{' '}
						<C>0-30/10</C>, and <C>5/20</C> for <C>5-59/20</C>. Months and weekdays take names,{' '}
						<C>JAN</C> or <C>mon-fri</C>, and Sunday is <C>0</C> or <C>7</C>. When both day fields
						are restricted, a day that matches either runs; when one of them starts with <C>*</C>, a
						day must match both, as in Vixie cron.
					</p>
					<Table
						head={['Beyond the standard', 'Means']}
						rows={[
							[<C>L</C>, 'In the day of month: the month’s last day.'],
							[<C>5L</C>, 'In the day of week: the month’s last Friday.'],
							[<C>5#2</C>, 'In the day of week: the month’s second Friday.'],
							[
								<>
									<C>@hourly</C>, <C>@daily</C>, <C>@weekly</C>, <C>@monthly</C>, <C>@yearly</C>
								</>,
								<>
									The whole spec: <C>0 * * * *</C>, <C>0 0 * * *</C>, <C>0 0 * * 0</C>,{' '}
									<C>0 0 1 * *</C>, <C>0 0 1 1 *</C>. <C>@midnight</C> and <C>@annually</C> are
									the same as <C>@daily</C> and <C>@yearly</C>; <C>@reboot</C> is not a schedule.
								</>
							]
						]}
					/>
					<p>
						<C>cron.Parse(spec)</C> returns the <C>Schedule</C>, and <C>Next(t)</C> the first
						minute after <C>t</C> it names, in <C>t</C>'s zone: when a job runs next, previewed with
						exactly the rules the cron process runs by.
					</p>
				</>
			)
		},
		{
			id: 'zones',
			title: 'Time zones and clock changes',
			body: (
				<>
					<p>
						A job's <C>Spec</C> is read in its <C>Location</C>, UTC when it is nil, so the nodes of
						a cluster read a spec alike whatever their own zone. Two jobs of one process can run on
						two continents' clocks.
					</p>
					<p>
						Where clocks move, the wall clock decides. A minute clocks skip when they go forward
						does not exist, and a job due in it does not run that day. A minute they repeat when
						they go back runs the first time only. <C>Next</C> answers the same way, which this
						example from the module's tests checks:
					</p>
					<Code caption="cron/example_test.go">{region(cronExample, /^func ExampleSchedule_Next/, /^}/)}</Code>
					<p>
						The cron process checks the time at the start of every minute by the wall clock. A
						clock stepped back runs no minute twice, and one stepped forward skips minutes as a
						process that was stopped would, which the next section is about.
					</p>
				</>
			)
		},
		{
			id: 'runs',
			title: 'Runs are processes',
			body: (
				<>
					<p>
						Each run is spawned by the cron process, linked to it (it ends if the cron process
						does), and labelled <C>{'cron:<job>'}</C>, so the node's process list shows every run
						going. Its <C>Action</C> is one of:
					</p>
					<Table
						head={['Action', 'The run']}
						rows={[
							[<C>cron.Send(to, build)</C>, <>Sends <C>build(run)</C> to <C>to</C>, and succeeds once it is sent.</>],
							[
								<C>cron.Call(to, build)</C>,
								<>
									Calls <C>to</C>, on this node or another, and succeeds if the answer is not an
									error: the work runs where the process that does it runs.
								</>
							],
							[<C>cron.Func(fn)</C>, <>Runs <C>fn(ctx, run)</C>; <C>ctx</C> ends when the run is told to exit.</>],
							[
								<C>{'func(*grpcproc.Process[proto.Message], cron.Run) error'}</C>,
								'Is the run’s process: it can monitor, call and spawn like any other.'
							]
						]}
					/>
					<p>
						<C>Run.Time</C> is the minute the run was due, in its job's zone, whenever it actually
						starts. A run fails with any exit reason but <C>normal</C>: the error it returned, a
						panic, <C>timeout</C> or <C>replaced</C>. <C>OnFailure</C> hears of each failure on the
						cron process's goroutine, so it must not block; the process also logs it, and keeps the
						reason where the Inspector shows it.
					</p>
					<Table
						head={['Overlap', 'A run due while the job’s last run still goes']}
						rows={[
							[<C>Allow</C>, 'Starts beside it. The default.'],
							[<C>Forbid</C>, 'Is skipped: the last run goes on.'],
							[<C>Replace</C>, <>Starts, once the last run is told to exit with reason <C>replaced</C>.</>]
						]}
					/>
					<p>
						A job's <C>Timeout</C> bounds its runs: one that outlives it is told to exit, with
						reason <C>timeout</C>. Telling is all grpcproc can do to a goroutine, so an{' '}
						<C>Action</C> that does I/O should pass its context on. That context ends at the run's{' '}
						<C>Deadline</C>, so a call made with it, <C>cron.Call</C>'s included, carries the time left
						to the callee.
					</p>
				</>
			)
		},
		{
			id: 'missed',
			title: 'Missed runs',
			body: (
				<>
					<p>
						A run is due at the start of its minute. One missed while the cron process was not
						running, or was held up, is skipped, unless its job has a <C>StartingDeadline</C>: then
						the latest run it missed starts late, if it is still within the deadline, and once,
						however many it missed. A job added to a running process owes nothing from before it was
						added, and one enabled again nothing from while it was disabled.
					</p>
					<p>
						Across processes, the cron process's state carries it. Whenever a run starts, the
						process reports each job's last run, a <C>*cronv1.State</C>, through{' '}
						<C>Spec.OnState</C>; a process started with that state as <C>Spec.Resume</C> catches up
						from there. It checks the minute it starts in at once: a run due in it, which the process
						it takes over from did not start, starts then rather than a minute late. A supervisor
						that restarts a cron process resumes it from its last state by itself.
					</p>
				</>
			)
		},
		{
			id: 'changes',
			title: 'Changing jobs',
			body: (
				<>
					<Code>{`c.AddJob(ctx, node, job)           // from c's node only: a job holds Go functions
c.DisableJob(ctx, node, "nightly") // from any node
c.EnableJob(ctx, node, "nightly")  // runs from the next minute; what it missed is not caught up
c.RemoveJob(ctx, node, "nightly")

c = cron.Named("a", "cron")        // a cron process by its name`}</Code>
					<p>
						<C>c</C> is a <C>cron.Crontab</C>, what <C>cron.Start</C> returns. Each change takes who
						asks, the node or a process from inside its handler, as an address's <C>Call</C> does.
					</p>
					<p>
						Runs already going go on. An unknown name is <C>cron.ErrNoJob</C>, a name taken{' '}
						<C>cron.ErrJobExists</C>, whichever node asked. A cron process a supervisor restarts
						starts again from its spec's jobs: what <C>AddJob</C> added is gone with the process
						that had it.
					</p>
				</>
			)
		},
		{
			id: 'inspector',
			title: 'In the Inspector',
			body: (
				<>
					<p>
						The cron process publishes a line per job, which <C>grpcprocctl inspect</C> and the{' '}
						<A to="guides/inspector/">Inspector</A> show among what the process says about itself:
						its spec and zone, when it runs next, when it last ran, how many runs are going, and why
						the last one that failed did.
					</p>
					<Code lang="txt">{`  every-quarter:   */15 * * * * UTC, disabled
  nightly-report:  10 3 * * * Europe/Berlin, next 2026-09-29T03:10:00+02:00, last 2026-09-28T03:10:00+02:00, running 1, failed: timeout`}</Code>
					<p>
						Each run is in the process list, under its label, with its parent the cron process, so a
						run that is stuck is found, and inspected, like any other process.
					</p>
				</>
			)
		},
		{
			id: 'cluster',
			title: 'Once in a cluster',
			body: (
				<>
					<p>
						A job that must run once in the cluster, not once per node, runs where the cluster's
						leader runs: the cron process as the singleton of{' '}
						<A to="guides/leader/">grpcproc/leader</A>. The singleton's state is the cron process's,
						saved as each run starts, and the next leader's cron process resumes from it:
					</p>
					<Code caption="examples/singleton/singleton_test.go">{region(singleton, /^func election/, /^}/)}</Code>
					<p>
						The two modules do not import each other: they meet at <C>actor.ChildSpec</C>, which{' '}
						<C>cron.Child</C> returns and a singleton is, and at the state, which <C>OnState</C>{' '}
						reports and <C>Lease.Save</C> takes. The{' '}
						<Ext href={file('examples/singleton/singleton_test.go')}>example's test</Ext> crashes
						the leader a moment before a job is due, and checks that every minute ran once, the one
						due during the election included.
					</p>
					<p>
						That test, and the module's own, run in a <C>testing/synctest</C> bubble: its clock is
						fake and moves on when every goroutine in it waits, so the minutes and hours a schedule
						spans pass at once, and a test can start on the day clocks change.
					</p>
				</>
			)
		}
	]
};
