import idsExample from '../../../examples/singleton/ids_test.go?raw';
import singleton from '../../../examples/singleton/singleton_test.go?raw';

import { Code, Output, region } from '../code.tsx';
import { Drawing } from '../components/Figure.tsx';
import { Election } from '../components/diagrams-leader.tsx';
import { A, C, Ext, Table } from '../components/prose.tsx';
import { file } from '../config.ts';
import type { Doc } from './types.ts';

export const guidesLeader: Doc = {
	path: 'guides/leader/',
	title: 'Leader election',
	description:
		'Elect one node of a cluster to run a singleton, which starts when its node wins and exits when it loses, and carry its state from one leader to the next.',
	lead: (
		<p>
			<C>grpcproc/leader</C> elects one node of a cluster, and runs a singleton there: a child spec
			of yours, started when its node wins and told to exit when it loses. Leadership is the
			singleton's lifetime, so no handler asks whether it still leads. The singleton can checkpoint
			its state, and whichever node leads next starts from it. It is a separate module, versioned
			on its own, and depends on grpcproc alone.
		</p>
	),
	sections: [
		{
			id: 'start',
			title: 'Install and start',
			body: (
				<>
					<Code lang="sh">{'go get github.com/floatdrop/grpcproc/leader'}</Code>
					<Code>{`leader.Start(node, leader.Spec[*schedpb.State]{
	Cluster: "scheduler",
	Voters:  []string{"a", "b", "c"},
	Singleton: func(l *leader.Lease[*schedpb.State], last *schedpb.State) (actor.ChildSpec, error) {
		return actor.Child("scheduler", func() *Scheduler { return &Scheduler{lease: l, state: last} }), nil
	},
})`}</Code>
					<p>
						Every node of the cluster runs the same call. <C>Cluster</C> names the election, so one
						set of nodes can hold several; <C>Singleton</C> builds what runs on the leader, from the
						state the last leader checkpointed, the zero value before any has. The type parameter
						is that state: <C>proto.Message</C> for any, <C>*emptypb.Empty</C> for none.{' '}
						<C>Start</C> returns a supervisor, of the elector and, while the node leads, the
						singleton; <C>leader.Child(name, spec)</C> puts the same supervisor in a tree of yours.
					</p>
					<p>
						The singleton's name is registered on the leader's node only. To reach it, ask any
						node that takes part where it runs:
					</p>
					<Code>{`info, err := leader.Status(ctx, node, "scheduler") // Role, Term, Leader, View, Quorum, Singleton
sched := grpcproc.Named[*schedpb.Msg](info.Leader, "scheduler")`}</Code>
					<p>
						A call to a node that no longer leads fails with <C>grpcproc.ErrNoProc</C>, and while no
						leader is known <C>info.Leader</C> is empty: ask again. <C>leader.Call</C> does both, as{' '}
						<A to="guides/leader/#calling">Calling the leader</A> shows.
					</p>
				</>
			)
		},
		{
			id: 'singleton',
			title: 'The singleton',
			body: (
				<>
					<p>
						A node that wins an election starts the singleton, and a node that stops leading tells
						it to exit with reason <C>demoted</C>. Its <C>Init</C> is "became the leader", its
						context ending is "stopped being the leader", and its <C>Terminate</C> runs either way,
						in time to save its last word. A singleton that does I/O with its process's context has
						that I/O cancelled on demotion, rather than finish it as a leader that no longer is one.
					</p>
					<Drawing caption="a leader and a follower of one election">
						<Election />
					</Drawing>
					<p>
						A singleton that exits by itself, or cannot start, ends its node's leadership. The node
						steps down, and campaigns again only after a backoff that doubles with each failure in a
						row, while the others elect one of themselves: a failing singleton moves across the
						cluster rather than restart in place. For restarts in place, make the singleton an{' '}
						<C>actor.ChildSupervisor</C>, whose own children restart as a{' '}
						<A to="guides/supervisors/">supervisor</A>'s do.
					</p>
					<p>
						<C>Spec.Confirm</C>, if set, runs after a win and before the singleton starts, with the
						term: an error withholds leadership, with the same backoff. It is where leadership waits
						for a lock outside the cluster, an etcd lease or a Kubernetes Lease.
					</p>
				</>
			)
		},
		{
			id: 'election',
			title: 'The election',
			body: (
				<>
					<p>
						Each node runs an elector process, registered as <C>{'leader/<cluster>'}</C>. The
						election is Raft's without the log:
					</p>
					<ul>
						<li>
							<strong>Terms</strong> order leaderships. A candidate starts a term and asks for
							votes, a node votes once in a term, and a majority makes a leader, which asserts
							itself with heartbeats. A follower that hears none for an election timeout campaigns.
						</li>
						<li>
							<strong>Pre-votes</strong> come first: a node asks whether it would win before it
							starts a term, so a node cut off from the others does not raise its term alone, and
							depose the leader with it once it is back.
						</li>
						<li>
							<strong>A leader steps down</strong> once it has not heard from a majority for two
							election timeouts: cut off from the others, it stops leading by itself.
						</li>
						<li>
							<strong>Followers stick</strong> to a leader they heard from within an election
							timeout, and ignore requests for votes, unless the leader handed over.
						</li>
						<li>
							<strong>Sends go through relays</strong>, a process per peer, which also monitor the
							peer's elector. A send or a monitor that waits for a dial to a node that does not
							answer holds that relay, never the elector and its heartbeats. When a leader's
							elector exits, its followers see it at once, and campaign without waiting out a
							timeout.
						</li>
					</ul>
					<Table
						head={['Spec field', 'Default, and what it is']}
						rows={[
							[
								<C>ElectionTimeout</C>,
								'150ms. How long a follower goes without a heartbeat before it campaigns: a random time between it and twice it.'
							],
							[<C>HeartbeatInterval</C>, '50ms. How often a leader asserts itself; shorter than ElectionTimeout.'],
							[<C>MinClusterSize</C>, '3. The smallest view that elects, without Voters.'],
							[<C>GhostTTL</C>, '5s. How long a node that cannot be reached still counts, without Voters or Membership.']
						]}
					/>
				</>
			)
		},
		{
			id: 'voters',
			title: 'Who votes',
			body: (
				<>
					<p>
						With <C>Voters</C>, the view is that fixed set of nodes, and a majority of it elects,
						whether the others are up or not. Two majorities of one set share a node, so a
						partition never elects two leaders. This is the one to reach for.
					</p>
					<p>
						Without <C>Voters</C>, the view is dynamic: this node, <C>Peers</C>, and the nodes a{' '}
						<C>Membership</C> reports up, the same one a node is{' '}
						<A to="concepts/discovery/">configured with</A>, <A to="guides/etcd/">on etcd</A> say:
						left unset, it is the node's own. With a <C>Membership</C>, only those take part;
						without, on a node with none either, a node that talks to this one joins too. No leader is elected in a view smaller than <C>MinClusterSize</C>, and the
						relays' monitors keep the view current:
					</p>
					<Table
						head={['The peer’s elector', 'The peer']}
						rows={[
							[
								<>
									exits (<C>shutdown</C>, a crash), or never ran (<C>noproc</C>)
								</>,
								'Leaves the view at once.'
							],
							[
								<>
									cannot be reached (<C>noconnection</C>)
								</>,
								<>
									Stays, as a ghost, until <C>Membership</C> reports it gone, or for <C>GhostTTL</C>{' '}
									without one.
								</>
							],
							['is heard from again', 'Is back.']
						]}
					/>
					<p>
						A dynamic view trades safety for availability: nodes whose views differ can each count
						a majority of their own.
					</p>
				</>
			)
		},
		{
			id: 'state',
			title: 'State from one leader to the next',
			body: (
				<>
					<p>
						The singleton checkpoints its state through its <C>Lease</C>:
					</p>
					<Code>{`err := lease.Checkpoint(ctx, state) // returns once a majority of the cluster holds it
lease.Save(state)                   // the same, without the wait`}</Code>
					<p>
						The leader sends the state to its followers with its heartbeats, and a follower votes
						only for a candidate whose state is at least as recent as its own. So whichever node
						leads next starts its singleton from every checkpoint that returned, or a later one.
						Once the lease's term is over, <C>Checkpoint</C> fails with <C>leader.ErrNotLeader</C>,
						and nothing a demoted singleton saves reaches anyone.
					</p>
					<p>
						<C>leader.Resign(ctx, node, cluster)</C> hands over on purpose. The leader stops its
						singleton, whose <C>Terminate</C> can still save; waits for the follower with the latest
						state to hold the leader's; and has it campaign at once. Call it before stopping a
						leader's node, and the next leader starts from the singleton's last word rather than its
						last checkpoint. On a node that does not lead it is <C>ErrNotLeader</C>, and on a leader
						alone, <C>ErrNoSuccessor</C>.
					</p>
					<p>
						Keep the state to what the next leader needs to carry on, the jobs a scheduler ran last,
						not the data they worked on: every checkpoint carries all of it.
					</p>
					<p>
						Without a <C>Store</C>, the state lives in memory, as do the terms: a cluster that loses
						a majority of its nodes at once loses them, and a cluster of one loses them at every
						restart. With one on each node, each keeps its term, its vote and the state, and saves
						them before it tells anyone of them, as Raft does. A cluster that restarts whole starts
						its singleton from the last checkpoint that returned, and its terms go on growing.
					</p>
					<Code>{`Store: leader.File("/var/lib/app/cron.election"), // each node its own, on its own disk`}</Code>
					<p>
						A <C>Store</C> shared by nodes breaks the election, and so does a file restored from a
						backup, which has forgotten the votes cast since: delete it instead, and the node starts
						afresh.
					</p>
				</>
			)
		},
		{
			id: 'calling',
			title: 'Calling the leader',
			body: (
				<>
					<p>
						<C>leader.Call</C> calls the process registered under a name on whichever node leads,
						from a node or from inside a process, and follows leadership as it moves. This
						singleton, from <Ext href={file('examples/singleton/ids_test.go')}>examples/singleton</Ext>,
						hands out numbers that never repeat: each is checkpointed to a majority before it is
						handed out, so whichever node leads next starts past it.
					</p>
					<Code caption="examples/singleton/ids_test.go">{region(idsExample, /^func ids/, /^}/)}</Code>
					<p>Any node that takes part asks for one the same way:</p>
					<Code caption="examples/singleton/ids_test.go">{region(idsExample, /^func nextID/, /^}/)}</Code>
					<p>
						The example's test asks from every node, kills the leader, and asks again: the numbers
						run on from where they were. <C>Call</C> asks again, for as long as its context allows,
						only when the call cannot have reached the singleton:
					</p>
					<Table
						head={['The call', 'Asked again?']}
						rows={[
							['No leader is known yet.', 'Yes.'],
							[
								'The node named has no such process: it no longer leads, or has not started its singleton yet.',
								'Yes.'
							],
							[
								<>
									It never left this node: a <C>*grpcproc.LinkError</C> whose <C>Unsent</C> is set.
								</>,
								'Yes.'
							],
							['It left, and its link broke, or its context ended, while it waited.', 'No: it may have been handled.'],
							['It was answered with an error.', 'No: that is the answer.']
						]}
					/>
					<p>
						Delivery is at most once, so repeating a call that may have been handled is the
						caller's choice: fine for a read, or for a request the singleton de-duplicates. For the
						numbers, a repeat can only skip one, never hand one out twice.
					</p>
				</>
			)
		},
		{
			id: 'maintenance',
			title: 'Moving the leader, and taking a node out',
			body: (
				<>
					<Code>{`leader.Transfer(ctx, node, "scheduler", "b") // b leads next; "" for the most up-to-date follower
leader.Cordon(ctx, node, "scheduler", "c")   // c may not lead: for work on its host
leader.Uncordon(ctx, node, "scheduler", "c")`}</Code>
					<p>
						All three go to whichever node leads, found through <C>node</C>'s elector, from any
						node that takes part. <C>Transfer</C> hands over as <C>Resign</C> does, to the node it
						names: it is refused for a node that leads already, is not in the view, or is
						cordoned, and ends with <C>ErrNoSuccessor</C> if that follower cannot be reached.
					</p>
					<p>
						<C>Cordon</C> keeps a node from leading until <C>Uncordon</C>: it campaigns no more,
						voters refuse it, and if it leads, it hands over. The cordoned nodes are part of the
						state the leader replicates, and <C>Cordon</C> returns once a majority holds it, so a
						cordon outlasts the cordoned node's restarts, and the leader's. Cordon a node, stop
						it, work on its host, start it again: it stays a follower until you uncordon it. It
						still votes, so the cluster keeps its quorum meanwhile. A cordon that would leave no
						node of the view to lead is refused.
					</p>
					<p>
						From a terminal, <A to="guides/grpcprocctl/#leader">grpcprocctl</A> does the same
						through the Inspector, and shows what every node's elector believes once they agree:
					</p>
					<Code lang="sh">{'grpcprocctl --plaintext leader cordon scheduler b   # b led: it hands over'}</Code>
					<Output>{`NODE  ROLE      TERM  LEADER  VIEW   STATE  CORDONED  UNREACHABLE  SINGLETON  BACKOFF  ERROR
a     leader    2     a       a,b,c  2.3    b                      <a.1.11>
b     follower  2     a       a,b,c  2.3    b                      none       200ms
c     follower  2     a       a,b,c  2.3    b                      none`}</Output>
					<p>
						<C>grpcprocctl leader scheduler</C> shows the same table; <C>leader move scheduler</C>{' '}
						(with <C>--to</C> a node) and <C>leader uncordon scheduler b</C> are the other two
						operations, and an agent gets them as MCP tools.
					</p>
					<p>
						A node that restarts holds no state, and voters refuse a candidate whose state is
						older than theirs; so a voter that refuses one sends its own with the refusal, and
						the candidate campaigns again with it. A restarted node catches up even when no
						leader is left to send it anything, as when every other node is cordoned.
					</p>
				</>
			)
		},
		{
			id: 'two-leaders',
			title: 'Two leaders, briefly',
			body: (
				<>
					<p>
						Two nodes can both believe they lead for a while: a leader cut off from the others leads
						until it notices, two election timeouts at most, and they elect another meanwhile. Its
						singleton is told to exit then, but what it did before is done. <C>Lease.Term</C> grows
						with every election: a resource that remembers the highest term it has seen can refuse a
						deposed leader's writes, which is what fencing a database or a queue takes.
						Leadership waiting for an external lock, through <C>Confirm</C>, is the other way.
					</p>
				</>
			)
		},
		{
			id: 'cron',
			title: 'A job that runs once in a cluster',
			body: (
				<>
					<p>
						<A to="guides/cron/">grpcproc/cron</A> as the singleton: the leader runs a cron process,
						which reports each job's last run as its state, and the next leader's resumes from it,
						catching up on a run it missed within each job's <C>StartingDeadline</C>. From{' '}
						<Ext href={file('examples/singleton/singleton_test.go')}>examples/singleton</Ext>, where
						every node runs this:
					</p>
					<Code caption="examples/singleton/singleton_test.go">{region(singleton, /^func election/, /^}/)}</Code>
					<p>
						The example's test crashes the leader a moment before 10:10, and checks that every
						minute ran exactly once, on the old leader before 10:10 and on the new one from then,
						the minute of the election included:
					</p>
					<Code caption="examples/singleton/singleton_test.go">{region(singleton, /\/\/ The leader crashes a moment before 10:10/, /sleepUntil\("10:12:30"\)/)}</Code>
					<p>
						It runs in a <C>testing/synctest</C> bubble over a{' '}
						<A to="guides/testing/">grpcproctest</A> cluster: the bubble's clock is fake, and moves
						on when every goroutine in it waits, so the minutes pass at once and an election's
						timeouts are the same on every run.
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
						The elector publishes what it believes, which <C>grpcprocctl inspect leader/scheduler</C>{' '}
						shows, on every node: comparing the nodes is how a split view shows up.
					</p>
					<Table
						head={['Key', 'What it says']}
						rows={[
							[
								<C>role</C>,
								<>
									<C>leader</C>, <C>candidate</C>, <C>follower</C>, or <C>unclustered</C> in a view
									smaller than <C>MinClusterSize</C>.
								</>
							],
							[
								<>
									<C>term</C>, <C>leader</C>, <C>voted_for</C>
								</>,
								'The current term, the leader known in it, and whom this node voted for.'
							],
							[
								<>
									<C>view</C>, <C>quorum</C>
								</>,
								'The nodes whose majority elects, and how many that is.'
							],
							[<C>unreachable</C>, 'Nodes of the view not heard from since their link broke, or their elector exited.'],
							[
								<C>state</C>,
								'The version of the state this node holds: the term of the leader that made it, and its sequence.'
							],
							[<C>cordoned</C>, 'The nodes that may not lead.'],
							[<C>checkpoints_waiting</C>, 'On the leader: checkpoints a majority does not hold yet.'],
							[<C>singleton</C>, 'Its PID on the leader, or none, starting, stopping.'],
							[<C>backoff</C>, 'How long a node waits before it campaigns again: after its singleton failed, or after it handed over.']
						]}
					/>
					<p>
						The elector's helpers are its children in the process list: its relays, labelled{' '}
						<C>leader relay</C>; the process that starts the singleton, <C>leader starter</C>; and
						the watch of a <C>Membership</C>, <C>leader membership</C>.
					</p>
				</>
			)
		}
	]
};
