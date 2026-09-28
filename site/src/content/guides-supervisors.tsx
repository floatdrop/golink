import inventory from '../../../examples/guide/internal/inventory/inventory.go?raw';
import supervisor from '../../../examples/supervisor/main.go?raw';
import supervisorOutput from '../../../examples/supervisor/output.txt?raw';

import { Code, Output, region } from '../code.tsx';
import { A, C, Ext, Table } from '../components/prose.tsx';
import { file } from '../config.ts';
import type { Doc } from './types.ts';

const inspectOutput = `name:                  orders-sup
label:                 supervisor
monitors:              2
  child.payments:      <orders-1.1718.3> permanent restarts=0
  child.reservations:  <orders-1.1718.2> permanent restarts=0
  restarts:            0/3 in 5s
  strategy:            one_for_one`;

export const guidesSupervisors: Doc = {
	path: 'guides/supervisors/',
	title: 'Supervisors',
	description:
		'Building a supervision tree with grpcproc/actor: specs, children, strategies, restarts, and adding or stopping children at run time.',
	lead: (
		<p>
			This page is the API. <A to="concepts/supervision/">Supervision trees</A> says why a tree, which
			strategy to pick and where state should live; read it first if supervisors are new. The code
			here is from <Ext href={file('examples/supervisor/main.go')}>examples/supervisor</Ext> and the
			tutorial's <Ext href={file('examples/guide/internal/inventory/inventory.go')}>inventory service</Ext>.
		</p>
	),
	sections: [
		{
			id: 'spec',
			title: 'A supervisor is a Spec',
			body: (
				<>
					<p>
						<C>actor.Spec</C> describes a supervisor: what it restarts, how often it may, how long
						a child has to stop, and the children themselves. <C>actor.Supervise(node, spec, opts...)</C>{' '}
						starts it on a node with its children, in order, and returns the supervisor's PID
						once they all run, or the first error after stopping those already started. The
						options are the supervisor's own: a name, a label.
					</p>
					<Code caption="examples/supervisor/main.go">{region(supervisor, /^\/\/ tree is the supervision tree/, /^}/)}</Code>
					<Table
						head={['Field', 'What it says']}
						rows={[
							[
								<C>Strategy</C>,
								<>
									Which children restart when one exits: <C>OneForOne</C>, the child alone;{' '}
									<C>OneForAll</C>, every child, the others stopped first; <C>RestForOne</C>, the
									child and those started after it. The zero value is <C>OneForOne</C>.
								</>
							],
							[
								<>
									<C>MaxRestarts</C>, <C>Within</C>
								</>,
								<>
									The restart intensity: more than <C>MaxRestarts</C> restarts within <C>Within</C>{' '}
									and the supervisor gives up, exiting with <C>ReasonMaxRestarts</C>. Zero means 3
									in 5s; a negative <C>MaxRestarts</C> means none.
								</>
							],
							[
								<C>Shutdown</C>,
								<>
									How long a worker child has to exit once told to, unless its own spec says
									otherwise. 5s by default. A child that is a supervisor gets <C>Infinity</C>
									instead: stopping it waits for its subtree.
								</>
							],
							[
								<C>AutoShutdown</C>,
								<>
									Whether the supervisor ends itself when its significant children end:{' '}
									<C>NoAutoShutdown</C> (the default), <C>AnySignificant</C> or{' '}
									<C>AllSignificant</C>.
								</>
							],
							[
								<C>Children</C>,
								<>
									Started in order, stopped in reverse. <C>StartChild</C> adds more, after them.
								</>
							]
						]}
					/>
					<p>
						A spec is checked when the supervisor starts: an unknown strategy, a name used twice, a
						significant permanent child, are all errors from <C>Supervise</C>, not surprises later.
					</p>
				</>
			)
		},
		{
			id: 'children',
			title: 'Children',
			body: (
				<>
					<p>
						A <C>ChildSpec</C> says how to start one child. Three functions build one:
					</p>
					<ul>
						<li>
							<C>actor.Child(name, newHandler)</C> runs an actor. It takes a factory rather than a
							handler, and calls it at every start, so a restart begins from a fresh struct and
							never sees the state that crashed. The factory closes over the dependencies:{' '}
							<C>func() *Inventory {'{'} return &amp;Inventory{'{'}ledger: ledger{'}'} {'}'}</C>.
						</li>
						<li>
							<C>actor.ChildFunc(name, fn)</C> runs a plain process function, for a child that is
							not an actor.
						</li>
						<li>
							<C>actor.ChildSupervisor(name, spec)</C> runs another supervisor, which is how a tree
							gets its depth.
						</li>
					</ul>
					<p>
						The name is the child's name in the supervisor and the name it is registered under on
						the node, so it stays reachable with <C>grpcproc.Named</C> across restarts. It must be
						unique on the node. An empty name registers nothing: the child is anonymous, known by
						its PID, and a supervisor can have any number of those.
					</p>
					<p>
						Each builder takes spawn options for the child, a label say. <C>LinkChild</C> is not
						one of them: the supervisor does not trap exits, and would end whenever the child did.
						The supervisor adds <C>LinkParent</C> itself, so a child never outlives it.
					</p>
					<p>
						Three methods return a copy of the spec with one field changed:
					</p>
					<Table
						rows={[
							[
								<C>WithRestart(r)</C>,
								<>
									When the child is restarted: <C>Permanent</C> always, the default;{' '}
									<C>Transient</C> after an abnormal exit only, not after <C>normal</C> or{' '}
									<C>shutdown</C>; <C>Temporary</C> never.
								</>
							],
							[
								<C>WithShutdown(d)</C>,
								<>
									How long this child has to exit once told to, in place of the supervisor's{' '}
									<C>Shutdown</C>; <C>actor.Infinity</C> waits however long it takes.
								</>
							],
							[
								<C>WithSignificant(true)</C>,
								<>
									The child counts for <C>AutoShutdown</C>: a transient one that ends with{' '}
									<C>normal</C> or <C>shutdown</C>, or a temporary one that ends at all. A permanent
									child cannot be significant.
								</>
							]
						]}
					/>
					<Code>{`actor.Spec{
	Strategy:     actor.OneForOne,
	AutoShutdown: actor.AllSignificant,
	Children: []actor.ChildSpec{
		actor.Child("importer", newImporter).WithRestart(actor.Transient).WithSignificant(true),
		actor.ChildFunc("progress", progress).WithRestart(actor.Temporary).WithShutdown(time.Second),
	},
}`}</Code>
					<p>
						That tree is a job: it ends itself, with <C>shutdown</C>, once the importer has finished
						on its own. A supervisor above it restarts it only if it is a permanent child there.
					</p>
				</>
			)
		},
		{
			id: 'tree',
			title: 'A tree of trees',
			body: (
				<>
					<p>
						<C>ChildSupervisor</C> is a child whose spec is another <C>Spec</C>. That is how an
						application gets one supervisor per service, each with its own strategy and intensity,
						under one root. The tutorial's inventory service builds its subtree in a function that
						takes the service's dependencies from the container:
					</p>
					<Code caption="examples/guide/internal/inventory/inventory.go">{region(inventory, /^\/\/ tree is the service's supervision tree/, /^}/)}</Code>
					<p>
						The root, in <A to="shop/platform/">the tutorial's platform</A>, is a <C>OneForOne</C>{' '}
						supervisor whose children are every service's tree. A service that gives up, past its
						own intensity, exits with <C>max restarts</C>, an abnormal reason; the root restarts that
						subtree alone, and the others carry on. That is escalation, and it stops at the root:
						a root that gives up ends the program, for whatever runs it to start again.
					</p>
					<p>
						Stopping runs the other way. A supervisor told to exit stops its children in reverse
						order, and a child supervisor has as long as it takes, <C>Infinity</C>, to stop its own.
						grpcproc cannot kill a goroutine, so a worker that outlives its <C>Shutdown</C> is
						logged and left behind; a named one keeps its name until it exits, and the supervisor
						starts it again, or ends, only once it has. Nothing is ever started under a name an old
						process still holds.
					</p>
				</>
			)
		},
		{
			id: 'dynamic',
			title: 'Adding and stopping children',
			body: (
				<>
					<p>
						<C>actor.StartChild(ctx, node, sup, spec)</C> adds a child to a running supervisor and
						starts it, after the children it already has; it returns the child's PID once it runs.
						The child is the supervisor's like the others, with one difference: once it ends for
						good, because it is temporary, or transient and ended normally, or <C>StopChild</C>{' '}
						stopped it, the supervisor forgets it. A pool of workers is a <C>OneForOne</C>{' '}
						supervisor and anonymous children added as they are needed, and it does not grow with
						every worker that ever ran.
					</p>
					<Code>{`worker, err := actor.StartChild(ctx, node, pool, actor.ChildFunc("", handle(job)).WithRestart(actor.Temporary))`}</Code>
					<p>
						A spec holds Go functions, which no message can carry, so <C>StartChild</C> works only
						for a supervisor on the same node: it registers the spec locally and calls the supervisor
						with its id. A child that fails to start is an error, and the supervisor does not count
						it as a restart. <C>StartChild</C> is refused while a restart waits on a stuck child,
						since the new one would start before those owed a start.
					</p>
					<p>
						<C>actor.StopChild(ctx, node, sup, child)</C> stops a child for good: it is not
						restarted, whatever its <C>Restart</C>, and a strategy no longer counts it, until the
						supervisor itself is started again from its <C>Spec</C>. It carries a PID, so the
						supervisor may be on another node. A significant child stopped this way does not end its
						supervisor; only a child that ends by itself does.
					</p>
				</>
			)
		},
		{
			id: 'busy',
			title: 'A busy supervisor',
			body: (
				<>
					<p>
						A supervisor waits for a child to exit outside its receive loop, and so does every
						supervisor above it, each waiting for its subtree. A child that calls its supervisor
						while it is being stopped, from <C>Terminate</C> say, would hold the whole chain until
						the call gave up. So a supervisor that has waited more than 100ms answers the calls
						queued meanwhile, <C>StartChild</C> and <C>StopChild</C> among them, with{' '}
						<C>actor.ErrBusy</C>, and handles the rest once the wait is over. A quick stop is
						invisible to callers; a stuck one holds none of them.
					</p>
					<p>
						From a process, call a supervisor with <C>p.Context()</C>. A supervisor that is stopping
						the caller answers no call until it is done, and a caller waiting with a context of its
						own could not exit meanwhile. <C>p.Context()</C> ends when the process is told to, and
						so does the call.
					</p>
				</>
			)
		},
		{
			id: 'inspect',
			title: 'What a supervisor says about itself',
			body: (
				<>
					<p>
						Every supervisor publishes its state through <C>WithInspect</C>: its strategy, its
						restarts against the intensity, and each child with its PID or <C>stopped</C>, its
						restart policy and how many times it has been restarted. A child the supervisor is
						waiting on shows as <C>waiting</C> with the PID it waits for. <C>node.Inspect</C> reads
						it, and so does <C>grpcprocctl</C> through the <A to="guides/inspector/">Inspector</A>:
					</p>
					<Code lang="txt" caption="grpcprocctl inspect orders-sup">{inspectOutput}</Code>
					<p>
						Restarts are the number to watch. A count that keeps climbing without reaching the
						limit is a child that fails, waits out the window and fails again: it is never healthy,
						and never escalates either.
					</p>
				</>
			)
		},
		{
			id: 'example',
			title: 'A crash and a restart, walked through',
			body: (
				<>
					<p>
						The example's inventory keeps its levels in a <C>Ledger</C>, outside the actor, and
						loads them in <C>Init</C>. A restock of zero is a bug, and the error ends the actor;
						the program follows what the supervisor does through the node's events:
					</p>
					<Code caption="examples/supervisor/main.go">{region(supervisor, /Crash it, and follow what the supervisor does/, /^\t\t\tbreak/) + '\n\t\t}\n\t}'}</Code>
					<p>
						The name reaches whichever process currently runs the child, so the next call after
						the restart goes to the new actor, whose state came from the ledger. The supervisor's
						own count is read with <C>node.Inspect</C>:
					</p>
					<Code caption="examples/supervisor/main.go">{region(supervisor, /Same name, new process, state loaded back/, /fmt.Println\("supervisor restarts:"/)}</Code>
					<Output>{supervisorOutput}</Output>
					<p>
						One restart of three within the minute. A fourth crash in that window would end the
						supervisor with <C>max restarts</C>, and with it, in this program, the tree; in the
						tutorial, the root would start the service again.
					</p>
				</>
			)
		}
	]
};
