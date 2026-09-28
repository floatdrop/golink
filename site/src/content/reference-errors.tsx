import { Code } from '../code.tsx';
import { A, C, Table } from '../components/prose.tsx';
import type { Doc } from './types.ts';

export const referenceErrors: Doc = {
	path: 'reference/errors/',
	title: 'Errors and exit reasons',
	description:
		'What a failed send or call returns, what a Call’s error says about whether the callee saw the request, and the reasons a process exits with.',
	lead: (
		<p>
			grpcproc has two kinds of failure. An <em>error</em> is what a call to the API returns to the
			code that made it. An <em>exit reason</em> is a string that says why a process ended, and it
			reaches the processes that watch it as a message. The two meet in one place: a process that
			was asked to exit sees an <C>*ExitError</C>, whose reason is what its watchers get.
		</p>
	),
	sections: [
		{
			id: 'errors',
			title: 'Errors',
			body: (
				<>
					<Table
						head={['Error', 'When']}
						rows={[
							[
								<C>ErrNoProc</C>,
								<>
									No such process: a <C>Call</C> to a PID or a name nothing holds, or a callee that exited
									before answering. A <C>Send</C> to no process is a dead letter, not an error.
								</>
							],
							[
								<C>ErrNoConnection</C>,
								<>
									The peer cannot be reached. It comes as a <C>*LinkError</C> wrapping the transport error,
									with the peer's name and <C>Unsent</C>; <C>errors.Is</C> matches it.
								</>
							],
							[
								<C>ErrType</C>,
								<>
									The process does not accept this message type, or a <C>Call</C>'s reply is not the{' '}
									<C>R</C> the caller asked for.
								</>
							],
							[
								<C>*RemoteError</C>,
								<>
									The error a <C>Call</C> handler returned, carried back as text. The type says where it
									came from; the text is the handler's.
								</>
							],
							[<C>ErrNameTaken</C>, <>A <C>Spawn</C> with <C>WithName</C> while another process holds the name.</>],
							[<C>ErrNodeStopped</C>, <>The node has stopped, or stopped while a call waited.</>],
							[<C>ErrNotCall</C>, <>A <C>Reply</C> to a message nobody waits on.</>],
							[
								<C>ErrNotLocal</C>,
								<>
									Declared for a PID of another node handed to a method that takes local ones. The
									core's own methods, <C>Inspect</C> and <C>SetLogLevel</C>, answer <C>ErrNoProc</C>{' '}
									for such a PID instead: a process of another incarnation, or another node, is not
									one this node has.
								</>
							],
							[
								<C>*ExitError</C>,
								<>
									What <C>Receive</C> returns, and what <C>context.Cause(p.Context())</C> is, once a process
									was asked to exit or a process it is linked to exited. Its <C>Reason</C> is the exit
									reason. When the node stops instead, both are <C>context.Canceled</C>.
								</>
							],
							[
								<C>actor.ErrBusy</C>,
								<>
									A supervisor's reply to a call while it has waited more than 100 ms for a child to
									exit; from a process, call it with <C>p.Context()</C>.
								</>
							],
							[
								<C>actor.ErrNoReply</C>,
								<>
									Returned from <C>HandleCall</C>: the actor answers later, with <C>m.Reply</C>, from any
									goroutine.
								</>
							],
							[
								<C>actor.ErrStop</C>,
								<>
									Returned from a handler: the actor ends normally, after replying if it was a call.
								</>
							],
							[
								<C>grpcprocetcd.ErrSuperseded</C>,
								<>
									<C>Register</C> found a newer incarnation of this node in etcd. The instance was
									replaced, and does not take the name back.
								</>
							]
						]}
					/>
					<p>
						<C>*LinkError</C> implements <C>Is</C> for <C>ErrNoConnection</C> and <C>Unwrap</C> for
						the transport error, so both tests work, and <C>errors.AsType</C> gets at the fields:
					</p>
					<Code>{`_, err := node.Call[*shoppb.Reserved](ctx, stock, req)
if le, ok := errors.AsType[*grpcproc.LinkError](err); ok && le.Unsent {
	// The message never left this node: sending it again cannot deliver it twice.
}
if errors.Is(err, grpcproc.ErrNoConnection) {
	// Any link failure, whether or not the message left.
}`}</Code>
				</>
			)
		},
		{
			id: 'calls',
			title: 'What a failed call tells you',
			body: (
				<>
					<p>
						Delivery is at most once. A <C>Send</C> that returns <C>nil</C> was handed to delivery,
						which means the mailbox for a local process and the link's queue for a remote one; a{' '}
						<C>Call</C> that returns a reply was handled. Every other outcome leaves one question:
						did the callee see the request? The error answers it, or says it cannot.
					</p>
					<ul>
						<li>
							<strong>A <C>*LinkError</C> with <C>Unsent</C> set</strong> is the one error that
							says no. The peer could not be reached, or dials to it are backed off, and the
							message is still on this node. Sending it again cannot deliver it twice.
						</li>
						<li>
							<strong>A <C>*LinkError</C> without it</strong> says the message may have been
							handled: the link broke while the call waited for its reply. So does a call that
							ended with its context, and one that was waiting when its node stopped, which fails
							with <C>ErrNodeStopped</C>.
						</li>
						<li>
							<strong><C>ErrNoProc</C></strong> says no when the process did not exist, and maybe
							when the callee exited before answering: it may have done the work and crashed on the
							reply.
						</li>
						<li>
							<strong><C>ErrType</C> and a <C>*RemoteError</C></strong> say the callee saw the
							request, refused it, or handled it and returned an error.
						</li>
					</ul>
					<p>
						<C>Unsent</C> is the field, rather than <C>Sent</C>, so that a <C>LinkError</C> built
						without it claims nothing. A caller that retries on anything else risks doing the work
						twice, and whether that is safe is the handler's business, not the transport's. The
						tutorial's desk shows the pattern: on an error it leaves things as they are, logs the
						order for reconciliation, and fails the call.
					</p>
				</>
			)
		},
		{
			id: 'reasons',
			title: 'Exit reasons',
			body: (
				<>
					<p>
						A process exits with a reason, a string. Its watchers get it in a <C>Down</C>, the
						processes linked to it exit with it or receive it in an <C>Exited</C>, the node publishes
						it in an <C>EventExit</C>, and hooks see it in <C>OnExit</C>. The reasons that grpcproc
						itself produces are constants: <C>ReasonNormal</C>, <C>ReasonShutdown</C> and so on.
					</p>
					<Table
						head={['Reason', 'Where it comes from']}
						rows={[
							[<C>normal</C>, <>The process function returned <C>nil</C>, or the actor returned <C>actor.ErrStop</C>.</>],
							['the error’s text', 'The process function returned an error, or an actor’s handler did.'],
							[<C>panic: …</C>, 'The process panicked; the stack is logged.'],
							['what Exit asked for', <>A process or the node called <C>Exit</C> with that reason. It is never trapped.</>],
							[<C>shutdown</C>, <>The node is stopping, or a supervisor is stopping its child, or a supervisor ended itself because its significant children ended.</>],
							[<C>killed</C>, <>The Inspector's <C>Exit</C>, and <C>grpcprocctl exit</C>, with no reason given.</>],
							[<C>max restarts</C>, <>A supervisor restarted its children more often than its <C>Spec</C> allows. Abnormal, so its own supervisor restarts it.</>],
							[<C>noproc</C>, <>Only in a <C>Down</C> or through a link: the monitored process does not exist. No process ever exits with it.</>],
							[<C>noconnection</C>, <>Only in a <C>Down</C> or through a link: the node the process runs on cannot be reached. Its monitors and links fire with it; the process itself may still run.</>],
							[<C>type</C>, <>A dead letter's reason: the message was not of the type the process accepts. Not an exit reason.</>]
						]}
					/>
					<p>
						A supervisor reads the reason to decide on a restart: a <C>Transient</C> child comes back
						after any reason but <C>normal</C> and <C>shutdown</C>, and a process linked to another
						takes the target's reason as it is, <C>normal</C> included. Only the last two rows come
						from the node rather than a process, and only monitors and links carry them.{' '}
						<A to="concepts/monitors-and-links/">Monitors and links</A> has the rules.
					</p>
				</>
			)
		},
		{
			id: 'model',
			title: 'The error model',
			body: (
				<>
					<p>
						Reasons, and the type check on delivery, are the whole error model. A process function
						returns an error or panics, and the reason says which. A message of the wrong type is a
						dead letter with reason <C>type</C>, and <C>ErrType</C> to a caller, never a panic in the
						process: local sends are checked by the compiler through the typed address, and remote
						ones are checked on delivery, because types do not cross the wire. Nothing that arrives
						from the network can crash a process by its shape alone; what the process does with a
						well-typed message it does not like is its own decision, and the tutorial's stock logs
						and drops such a message rather than exit.
					</p>
				</>
			)
		},
		{
			id: 'dead-letters',
			title: 'Dead letters',
			body: (
				<>
					<p>
						A dead letter is a message that was sent and could not be delivered: to a process that
						does not exist (<C>noproc</C>), to one that does not accept its type (<C>type</C>), or
						queued on a link that broke before or while writing it (<C>noconnection</C>). A message
						that could not be sent at all is not one: its sender got the error. A <C>Send</C> to no
						process succeeds and becomes a dead letter, because a send has no reply to carry the
						failure on; a <C>Call</C> in the same situation fails with <C>ErrNoProc</C>.
					</p>
					<p>
						Each one goes three places: <C>Hooks.OnDeadLetter</C> with the sender, the target, the
						body and the reason; the <C>DeadLetters</C> counter in <C>NodeInfo</C>; and an{' '}
						<C>EventDeadLetter</C> on <C>node.Subscribe</C>, with the message's type. A dead-letter
						count that grows is a sender using a stale address, or two services that disagree about
						a contract, and the event says which sender and which type.
					</p>
				</>
			)
		}
	]
};
