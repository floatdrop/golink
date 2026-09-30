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
									Started in order, stopped in reverse. <C>StartChild</C> and <C>StartChildFrom</C>{' '}
									add more, after them.
								</>
							],
							[
								<C>Factories</C>,
								<>
									What <C>StartChildFrom</C> can ask the supervisor for, from any node, by name: see{' '}
									<A to="guides/supervisors/#remote">Starting a child from another node</A>.
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
						starts it again, or ends, only once it has.
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
						<C>actor.StartChild(ctx, from, sup, spec)</C> adds a child to a running supervisor and
						starts it, after the children it already has; it returns the child's PID once it runs.{' '}
						<C>from</C> is who asks, the node or a process from inside its handler, as for an
						address's <C>Call</C>, and <C>sup</C> is a PID or a name. The child is the supervisor's
						like the others, with one difference: once it ends for good, because it is temporary, or
						transient and ended normally, or <C>StopChild</C> stopped it, the supervisor forgets it.
						So a pool of workers — a <C>OneForOne</C> supervisor with anonymous children added as
						they are needed — does not grow with every worker that ever ran.
					</p>
					<Code>{`worker, err := actor.StartChild(ctx, node, pool, actor.ChildFunc("", handle(job)).WithRestart(actor.Temporary))`}</Code>
					<p>
						A spec holds Go functions, which no message can carry, so <C>StartChild</C> works only
						from the supervisor's own node, which the supervisor checks: it registers the spec
						locally and calls the supervisor with its id. A child that fails to start is an error, and the supervisor does not count
						it as a restart. <C>StartChild</C> is refused while a restart waits on a stuck child,
						since the new one would start before those owed a start.
					</p>
					<p>
						A name belongs to one child. <C>StartChild</C> of a name the supervisor runs a child
						under starts nothing, and returns that child's PID with{' '}
						<C>actor.ErrAlreadyStarted</C>, whether the supervisor started it from its spec or an
						earlier <C>StartChild</C> did, and whether or not it has been restarted since. So code
						that starts a child per key keeps no map of its own, which the first restart would leave
						pointing at a process that is gone:
					</p>
					<Code>{`room, err := actor.StartChild(ctx, node, rooms, actor.Child("room:"+id, newRoom).WithRestart(actor.Transient))
if err != nil && !errors.Is(err, actor.ErrAlreadyStarted) {
	return err
}
// room runs, started now or before.`}</Code>
					<p>
						A child of that name that has just exited, and whose exit the supervisor has yet to
						handle, has freed the name already. The call waits for the supervisor to handle it, and
						gets the child's restart, or, if the supervisor forgets the child, a fresh start of the
						spec.
					</p>
					<p>
						<C>actor.StopChild(ctx, from, sup, child)</C> stops a child for good: it is not
						restarted, whatever its <C>Restart</C>, and a strategy no longer counts it, until the
						supervisor itself is started again from its <C>Spec</C>. It carries a PID, so the
						supervisor may be on another node. A significant child stopped this way does not end its
						supervisor; only a child that ends by itself does.
					</p>
				</>
			)
		},
		{
			id: 'remote',
			title: 'Starting a child from another node',
			body: (
				<>
					<p>
						A spec is Go functions, and no message carries those. A supervisor that other nodes ask
						for children declares what they may ask for instead: <C>Spec.Factories</C>, a factory per
						name, each built with <C>actor.ChildFactory</C> from a function that takes an argument, a
						protobuf message, and returns the spec to start. The functions, and the dependencies they
						close over, stay on the supervisor's node; only the name and the argument travel.
					</p>
					<Code>{`rooms, err := actor.Supervise(node, actor.Spec{Factories: map[string]actor.Factory{
	"peer": actor.ChildFactory(func(j *roomspb.Join) (actor.ChildSpec, error) {
		if media.Full() {
			return actor.ChildSpec{}, ErrFull
		}
		return actor.Child("peer:"+j.GetPeer(), func() *Peer { return &Peer{join: j, media: media} }).
			WithRestart(actor.Temporary), nil
	}),
}}, grpcproc.WithName("rooms"))`}</Code>
					<p>
						<C>actor.StartChildFrom(ctx, from, sup, factory, arg, opts...)</C> asks for one, from any
						node, and returns the child's PID once it runs. The child is the supervisor's as one{' '}
						<C>StartChild</C> added is. The factory is where admission goes: what it refuses with is{' '}
						<C>StartChildFrom</C>'s error, as it returned it, so <C>errors.Is</C> tells a full node,
						worth trying elsewhere, from a request that is wrong everywhere.{' '}
						<C>actor.ErrNoFactory</C> is a name the supervisor has no factory for.
					</p>
					<Code>{`sup := grpcproc.Name{Node: pick(), Name: "rooms"}
pid, ref, err := actor.StartChildFrom(p.Context(), p, sup, "peer", &roomspb.Join{Peer: id}, actor.WithMonitor())
switch {
case errors.Is(err, ErrFull):
	// try another node
case err != nil && !errors.Is(err, actor.ErrAlreadyStarted):
	return err
}
// A Down with ref comes when pid exits, however soon.`}</Code>
					<p>
						<C>actor.WithMonitor()</C> has the caller monitor the child from before it runs, as{' '}
						<C>SpawnMonitor</C> does on one node: a child that exits at once is a <C>Down</C> with its
						reason, after <C>StartChildFrom</C> returns, never <C>noproc</C>. A start that fails
						leaves no monitor, and no <C>Down</C> comes of it, so an error means nothing started, or
						nothing the caller will hear of. <C>actor.WithLink()</C> links the caller to the child
						instead. Either needs the caller to be a process.
					</p>
					<p>
						A call is answered at most once, and an answer can be lost: a link that breaks, a context
						that ends. The child may have started then, unwatched. A named child is found again:{' '}
						<C>StartChildFrom</C> of its name answers <C>ErrAlreadyStarted</C> with its PID and the
						monitor, as <C>StartChild</C> does. An anonymous one runs until it ends, and{' '}
						<C>actor.Children</C> lists it. The monitor is of the child's process: a restart the
						supervisor makes is a new one, which <C>StartChildFrom</C> of the name monitors again.
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
						restarts against the intensity, the names of its factories, and each child with its PID or <C>stopped</C>, its
						restart policy and how many times it has been restarted. A child the supervisor is
						waiting on shows as <C>waiting</C> with the PID it waits for. <C>node.Inspect</C> reads
						it, and so does <C>grpcprocctl</C> through the <A to="guides/inspector/">Inspector</A>:
					</p>
					<Code lang="txt" caption="grpcprocctl inspect orders-sup">{inspectOutput}</Code>
					<p>
						A program asks with <C>actor.Children(ctx, from, sup)</C>, which answers the same as data,
						from any node: the children in the order the supervisor starts them, each with its name,
						its PID while it runs, whether a restart owes it a start, its restart policy and its
						restarts, and whether it is a supervisor. It is what the supervisor knows when it
						answers, and out of date as soon as it is taken; to start a child unless it runs, call{' '}
						<C>StartChild</C>.
					</p>
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
