import quickstart from '../../../examples/quickstart/main.go?raw';

import { Code, region } from '../code.tsx';
import { A, Aside, C, Ext } from '../components/prose.tsx';
import { Table } from '../components/prose.tsx';
import { file } from '../config.ts';
import type { Doc } from './types.ts';

export const conceptsMonitorsAndLinks: Doc = {
	path: 'concepts/monitors-and-links/',
	title: 'Monitors and links',
	description:
		'How a process learns that another one is gone: a monitor turns an exit into a message, a link turns it into an exit of its own, and both work the same across nodes.',
	lead: (
		<>
			<p>
				A process runs until it returns, and then it is gone. What is left is the question of who
				needs to know. A caller waiting for a reply needs to know, so its call can fail instead of
				hang. A supervisor needs to know, so it can start a replacement. A worker whose only reason to
				exist is another process needs to know, so it can stop too. grpcproc has two ways to tell
				them, and one way to ask a process to go.
			</p>
		</>
	),
	sections: [
		{
			id: 'monitor',
			title: 'A monitor turns an exit into a message',
			body: (
				<>
					<p>
						<C>p.Monitor(target)</C> watches a process. When the target exits, <C>p</C> receives a
						message with <C>Down</C> set, through the same <C>Receive</C> as everything else. The
						target can be a PID, a name on some node, or a typed address; it can run on this node
						or on any other, and the watcher's code is the same.
					</p>
					<Code>{`ref := p.Monitor(grpcproc.Named[*shoppb.Reserve]("warehouse", "stock"))

m, err := p.Receive()
if err != nil {
	return err
}
if m.Down != nil && m.Down.Ref == ref {
	p.Log().Info("stock is gone", "reason", m.Down.Reason)
}`}</Code>
					<p>
						<C>Monitor</C> returns a <C>Ref</C>, and the <C>Down</C> carries it back, so a process
						that watches several targets tells their notices apart. The <C>Down</C> also carries the{' '}
						<C>PID</C> that exited, the <C>Name</C> the monitor was placed by, if it was, and the{' '}
						<C>Reason</C>. <C>Demonitor(ref)</C> stops watching; a <C>Down</C> already in the mailbox
						stays there, since taking it back would leave the mailbox with a hole in what it
						promised.
					</p>
					<p>
						Two cases need no exit at all. A monitor on a process that does not exist, because it
						never did or already went, answers at once with reason <C>noproc</C>: the watcher asked
						about something gone, and the answer is that it is gone. A monitor on a process whose
						node cannot be reached, now or later, answers with <C>noconnection</C>, the moment the
						node is declared down. In both, the watcher gets exactly one <C>Down</C>, so it never
						waits for one that cannot come.
					</p>
				</>
			)
		},
		{
			id: 'order',
			title: 'The Down comes last',
			body: (
				<>
					<p>
						A process's <C>Down</C> arrives after every message that process sent to the watcher.
						That is a guarantee, not a likelihood, and it is what makes the notice useful: when a
						worker sends its result and then exits, the watcher sees the result, then the{' '}
						<C>Down</C>, never the other way around. A watcher that gets a <C>Down</C> knows there is
						nothing more to come from that process.
					</p>
					<p>
						Within a node it holds because the <C>Down</C> is queued in the watcher's mailbox like a
						message, by the same code, behind what came before. Across nodes it holds because
						everything one node sends to another travels on one gRPC stream, in order: messages,
						replies, monitor notices. There is no side channel a <C>Down</C> could overtake on.{' '}
						<A to="concepts/nodes/">Nodes and the cluster</A> has the link in detail.
					</p>
					<p>
						The quick start's watcher relies on this. It monitors the stock, asks it to exit, and
						the next thing it receives is the <C>Down</C>, with the reason it asked for:
					</p>
					<Code caption="examples/quickstart/main.go">
						{region(quickstart, /A monitor across nodes works/, /fmt.Println\("stock exited:"/)}
					</Code>
				</>
			)
		},
		{
			id: 'spawn-monitor',
			title: 'Monitoring from before the start',
			body: (
				<>
					<p>
						There is a gap between <C>Spawn</C> returning an address and <C>Monitor</C> being called
						on it. A child that fails at once, in its first line, exits inside that gap, and a monitor
						placed after it finds no process and reports <C>noproc</C>. The real reason, the error
						the child returned, is lost. A supervisor reading <C>noproc</C> would take a transient
						child's clean, immediate exit for a crash and restart it in a loop.
					</p>
					<p>
						<C>p.SpawnMonitor(fn)</C> closes the gap: it spawns the child and monitors it in one
						step, inside the section that admits the child, so the monitor exists before the child
						runs. However soon the child exits, the <C>Down</C> has its real reason. It is Erlang's{' '}
						<C>spawn_monitor</C>, and it is how <A to="concepts/supervision/">supervisors</A> start
						every child.
					</p>
					<Code>{`child, ref, err := p.SpawnMonitor(worker, grpcproc.WithLabel("worker"))
if err != nil {
	return err
}
// A Down with ref arrives when child exits, with its reason, even
// if it exits before this line runs.`}</Code>
				</>
			)
		},
		{
			id: 'exit',
			title: 'Asking a process to exit',
			body: (
				<>
					<p>
						<C>p.Exit(target, reason)</C> asks a process anywhere to stop, with a reason of the
						caller's choosing; <C>node.Exit(ctx, target, reason)</C> does the same from outside a
						process. The target's context is cancelled with the reason as its cause, its next{' '}
						<C>Receive</C> returns an <C>*ExitError</C>, and it returns. Its watchers get a{' '}
						<C>Down</C> with that reason, its supervisor decides from the reason whether to start it
						again, and the node stops its processes this way, with <C>shutdown</C>.
					</p>
					<Code>{`m, err := p.Receive()
if err != nil {
	// After Exit: err is an *ExitError, and context.Cause(p.Context())
	// is the same value. Return it, and the reason is the process's.
	return err
}`}</Code>
					<p>
						An exit request cannot be refused. A Go goroutine cannot be killed from outside, so the
						request is delivered as the error above and the process is expected to return. That is
						why grpcproc makes it the one signal a process cannot configure away: if trapping exits
						could turn a request into a message, a process could keep running through the node's
						shutdown, and nothing could stop it. A process that needs to clean up does so on the way
						out, after <C>Receive</C> returns, or in an actor's <C>Terminate</C>.
					</p>
				</>
			)
		},
		{
			id: 'link',
			title: 'A link turns an exit into an exit',
			body: (
				<>
					<p>
						Sometimes a process has no use for a <C>Down</C>, because the only thing it would do with
						one is stop. A session that serves one connection, a worker that feeds one consumer: when
						the other side is gone, so is their reason to run. <C>p.Link(target)</C> says that. When
						the target exits, <C>p</C> exits too, with the target's reason, whatever it is.
					</p>
					<Code>{`// A session cannot outlive the connection process it serves.
p.Link(conn)

// A target that does not exist ends p with noproc; one whose node
// cannot be reached, now or later, ends it with noconnection.`}</Code>
					<p>
						A link is one way. <C>p.Link(b)</C> ties <C>p</C>'s fate to <C>b</C>'s and says nothing
						about <C>b</C>: when <C>p</C> exits, <C>b</C> is not affected. If <C>b</C> should follow{' '}
						<C>p</C> as well, <C>b</C> links to <C>p</C>. <C>Unlink(target)</C> removes a link; an{' '}
						<C>Exited</C> already in the mailbox stays there, as a <C>Down</C> does.
					</p>
					<p>
						The linker takes the reason as it is, <C>normal</C> included. A link says "this process
						cannot go on without that one", and a target that finished cleanly is gone all the same.
						This matters for supervision: a child linked to a supervisor that was stopped in an
						orderly way must still go, and a transient child whose dependency ended <C>normal</C>{' '}
						ended normally too, so its own supervisor does not restart it.
					</p>
					<p>
						On the wire a link is a monitor. Only the linker's node tells the two apart: when the{' '}
						<C>Down</C> arrives, a monitor's is queued as a message and a link's ends the process,
						the way an <C>Exit</C> request would. So a link gets everything a monitor gets, the{' '}
						<C>noproc</C> and <C>noconnection</C> cases and the ordering behind the target's last
						messages, and a peer on another node needs nothing beyond monitors.
					</p>
				</>
			)
		},
		{
			id: 'spawn-linked',
			title: 'Spawning linked',
			body: (
				<>
					<p>
						A link placed after <C>Spawn</C> has the same gap a monitor has, so links to a child are
						made at spawn, by option. <C>LinkParent()</C> links the child to the process that spawns
						it: when the parent exits, the child does too. <C>LinkChild()</C> links the parent to the
						child: when the child exits, the parent does. Both together are Erlang's{' '}
						<C>spawn_link</C>, a link both ways.
					</p>
					<Code>{`// A helper that must not outlive p, and whose failure must not end p.
helper, err := p.Spawn(fetch, grpcproc.LinkParent())

// A pair that live and die together.
twin, err := p.Spawn(other, grpcproc.LinkParent(), grpcproc.LinkChild())`}</Code>
					<p>
						Both options need a parent, so they are for <C>Process.Spawn</C> and{' '}
						<C>SpawnMonitor</C>; <C>Node.Spawn</C>, which no process calls, refuses them. A supervisor
						spawns each child with <C>LinkParent</C>, so a supervisor that is gone takes its children
						with it even if the orderly stop it makes when it ends did not reach them.
					</p>
				</>
			)
		},
		{
			id: 'trap',
			title: 'Trapping exits',
			body: (
				<>
					<p>
						A process that wants to know when a linked process exits, and do something other than
						exit, calls <C>p.SetTrapExit(true)</C>. From then on, the exit of a process it is linked
						to arrives as a message with <C>Exited</C> set, in order with the rest, instead of ending
						it. <C>Exited</C> carries the <C>PID</C>, the <C>Name</C> the link was placed by, if it
						was, and the <C>Reason</C>; <C>p.TrapExit()</C> reports the setting.
					</p>
					<Code>{`p.SetTrapExit(true)
for {
	m, err := p.Receive()
	if err != nil {
		return err // an Exit request: still not a message
	}
	if m.Exited != nil {
		p.Log().Warn("a linked process exited", "pid", m.Exited.PID, "reason", m.Exited.Reason)
		continue
	}
	// …
}`}</Code>
					<p>
						Trapping turns a link into something between a link and a monitor: the process learns of
						the exit and chooses what to do. It applies to links only. An <C>Exit</C> request still
						ends the process, for the reason given above. And an actor run by <C>actor.Run</C> still
						ends when its parent exits, whether it traps or not, as a <C>gen_server</C> does; that
						rule lives in the actor loop, and a raw process that traps exits decides for itself.
					</p>
				</>
			)
		},
		{
			id: 'reasons',
			title: 'The reasons',
			body: (
				<>
					<p>
						A <C>Down</C> and an <C>Exited</C> carry a reason, a string. It is what a supervisor
						reads to decide whether an exit was normal, and what a log or the Inspector shows.
						These are the ones grpcproc sets; <C>Exit</C> sets whatever the caller asked for.
					</p>
					<Table
						head={['Reason', 'What happened']}
						rows={[
							[<C>normal</C>, 'The process function returned nil.'],
							[
								'the error’s text',
								<>
									The function returned an error, and this is <C>err.Error()</C>. Abnormal.
								</>
							],
							[
								<C>panic: …</C>,
								'The function panicked; the message follows, and the stack goes to the log. Abnormal.'
							],
							[
								<C>shutdown</C>,
								'An orderly stop: the node is stopping, or a supervisor stopped the child. Not abnormal.'
							],
							[<C>killed</C>, 'The default reason of the Inspector’s Exit, from grpcprocctl or an agent.'],
							[
								<C>noproc</C>,
								'The process monitored or linked to does not exist: it never did, or it exited before the monitor was placed.'
							],
							[
								<C>noconnection</C>,
								'The node the process runs on cannot be reached: its link broke, or the cluster declared it gone. The process may still run there.'
							],
							[
								<C>max restarts</C>,
								'A supervisor gave up: more restarts than its Spec allows within the window. Abnormal, so its own supervisor restarts it.'
							]
						]}
					/>
					<p>
						<A to="reference/errors/">Errors and exit reasons</A> has the errors a sender sees for
						the same events: <C>ErrNoProc</C> and <C>ErrNoConnection</C> are the caller's side of{' '}
						<C>noproc</C> and <C>noconnection</C>.
					</p>
				</>
			)
		},
		{
			id: 'supervision',
			title: 'From here to supervision',
			body: (
				<>
					<p>
						A supervisor is nothing but these pieces and a policy. It starts each child with{' '}
						<C>SpawnMonitor</C> and <C>LinkParent</C>, so it learns of every exit with its real reason
						and no child outlives it. When a <C>Down</C> arrives, it reads the reason, compares it with
						the child's restart type, and starts a fresh child under the same name or gives up and
						exits with <C>max restarts</C>, which is a <C>Down</C> for the supervisor above it. There
						is no mechanism in it that this page has not shown.{' '}
						<A to="concepts/supervision/">Supervision trees</A> is the rest.
					</p>
					<Aside title="Coming from Erlang">
						<p>
							Two things differ. A link is one way, as in{' '}
							<Ext href="https://ergo.services">ergo</Ext>: <C>p.Link(b)</C> is a promise about{' '}
							<C>p</C> only. Erlang's two-way link needs the rule that a non-trapping process
							ignores a <C>normal</C> exit, or a helper that finishes would take its parent with it;
							and it needs both nodes to agree on the link, which is why the link protocol gained
							unlink ids and acknowledgements in OTP 23. A one-way link is a monitor on the wire,
							which each side handles alone. And an exit signal from <C>Exit</C> is never trapped,
							where Erlang's <C>exit/2</C> is, unless the reason is <C>kill</C>: grpcproc cannot kill
							a goroutine, so the request is the only <C>kill</C> it has. The{' '}
							<Ext href={file('docs/DESIGN.md')}>design notes</Ext> have the full reasoning.
						</p>
					</Aside>
				</>
			)
		}
	]
};
