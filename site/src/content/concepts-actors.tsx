import quickstart from '../../../examples/quickstart/main.go?raw';

import { Code, region } from '../code.tsx';
import { ActorsPicture } from '../components/diagrams-actors.tsx';
import { Drawing } from '../components/Figure.tsx';
import { A, Aside, C, Ext } from '../components/prose.tsx';
import type { Doc } from './types.ts';

export const conceptsActors: Doc = {
	path: 'concepts/actors/',
	title: 'The actor model',
	description:
		'What a process is, why it talks in messages, why its address is not a pointer, and where supervision comes from: the model behind grpcproc, for a Go developer who has not met it.',
	lead: (
		<p>
			grpcproc takes its shape from Erlang, where the unit of a program is a process: something with
			state of its own, a queue of messages in front of it, and an address anyone can send to. This
			page says what that buys, and what it costs, for a reader who has goroutines and channels and
			wonders why they are not enough.
		</p>
	),
	sections: [
		{
			id: 'process',
			title: 'A process is a goroutine with a mailbox',
			body: (
				<>
					<p>
						A process here is a Go function, run on a goroutine of its own, that takes messages from
						a mailbox one at a time. The mailbox is a queue. Anything can append to it, from any
						goroutine or any node, and only the process reads from it. The function keeps whatever
						state it needs in local variables, and since nothing else touches those variables, it
						needs no lock.
					</p>
					<Code caption="examples/quickstart/main.go">{region(quickstart, /^func inventory/, /^}/)}</Code>
					<p>
						That is the whole discipline: state lives inside one goroutine, and the only way to
						change it is to send a message. A caller that wants an answer sends a call and waits
						for the reply; a caller that does not sends and moves on. The process decides, per
						message, what to do with it. It cannot be interrupted between two messages, so every
						handler sees a consistent state and leaves one.
					</p>
					<p>
						A mailbox is typed. This one holds <C>*shoppb.Reserve</C> and nothing else, and the
						compiler checks that only such messages are sent to it. The type is a protobuf message,
						so it already has a wire encoding: the same value a process gets from its neighbour in
						the binary, it gets from a process on another machine.
					</p>
				</>
			)
		},
		{
			id: 'messages',
			title: 'Messages instead of shared memory',
			body: (
				<>
					<p>
						Go already says "do not communicate by sharing memory; share memory by
						communicating". A channel is that advice made concrete, and a process is the next step:
						a channel with an owner, an address, and a lifetime you can observe.
					</p>
					<p>
						A channel has no name. To send on it you need the channel value, which means the sender
						and the receiver were wired together by whoever created both, usually in one function.
						A channel does not cross a program boundary: a service on another machine cannot hold
						one. And a channel cannot tell you that the goroutine reading it has died; a send to it
						blocks for ever, or never happens, and nothing reports which.
					</p>
					<p>
						A process fixes each of those. It has an address, a PID or a registered name, that can
						be passed around, stored, and sent inside a message. The address works from any node in
						the cluster. And a process can be monitored: when it exits, or when the node it runs on
						is lost, whoever asked is told, with a reason.
					</p>
					<p>
						What a process does not fix is shared state, on purpose. Two processes never share a
						variable; they share nothing but the messages between them. A value that many parts of a
						program read and write becomes a process that owns it and answers questions about it.
						That is more typing than a mutex, and it is also the reason the process can move to
						another machine without anything else changing.
					</p>
				</>
			)
		},
		{
			id: 'address',
			title: 'An address, not a pointer',
			body: (
				<>
					<p>
						A message is sent to an address, never to a process value. The address is a PID, which
						names a node and a process on it, or a name the process registered on its node. Both
						are small comparable values that mean the same thing on every node.
					</p>
					<p>
						This is what makes the location of a process nobody's business. <C>Send</C>, <C>Call</C>,{' '}
						<C>Monitor</C> and <C>Exit</C> take an address, look at the node in it, and either
						deliver within the program or write the message to the link with that node. The code
						that sends is the same in both cases, and so is the code that receives. A local send
						does not even encode the message; a remote one does, and the reply comes back the same
						way.
					</p>
					<Drawing caption="the same call, within a node and across nodes">
						<ActorsPicture />
					</Drawing>
					<p>
						The difference between the two is not hidden, it is placed where it belongs: in what can
						go wrong. A remote call can fail because the link broke, and the caller finds out. A
						monitor on a remote process fires when the node cannot be reached, with a reason that
						says so. The <A to="shop/">tutorial</A> is one application run as one program and as
						three, from the same code, and the only thing that changes is the configuration.
					</p>
				</>
			)
		},
		{
			id: 'rpc',
			title: 'A process is not an RPC service',
			body: (
				<>
					<p>
						A gRPC service method runs once per request, on whatever goroutine the server gives it,
						concurrently with every other request. It has no state between calls but what it keeps
						in a database or behind a mutex. That is the right shape for a stateless handler, and
						grpcproc leaves your services as they are.
					</p>
					<p>
						A process is the other shape. It has state, it has a queue, and it handles one message at
						a time. A stock that must not be reserved twice, a session that must see its requests in
						order, a job that runs in steps and can be asked how far it got: each is naturally one
						process, and a mutex around a struct is the usual way of writing one without saying so.
					</p>
					<p>
						The two meet at the edge. In the tutorial, an HTTP handler is not a process; it calls one
						through the node, waits for the answer with the request's context, and turns it into a
						status code. The handler is stateless, the process behind it is not, and the call
						between them is typed like everything else.
					</p>
				</>
			)
		},
		{
			id: 'failure',
			title: 'Failure is a message',
			body: (
				<>
					<p>
						A process ends when its function returns, and it has a reason: <C>normal</C> for a nil
						return, the error's text otherwise, <C>panic: …</C> for a panic, or whatever an <C>Exit</C>{' '}
						request asked for. The reason is not lost. Any process that monitored it receives a{' '}
						<C>Down</C> carrying it, in its mailbox, after the last message the exiting process sent.
					</p>
					<p>
						A node that cannot be reached is reported the same way: every monitor on a process there
						gets a <C>Down</C> with reason <C>noconnection</C>, and every call waiting on it fails.
						So a watcher does not distinguish a crash from a lost machine unless it wants to, and
						the code that handles one handles the other.
					</p>
					<p>
						This is the piece that goroutines lack entirely. A goroutine that panics takes the program
						down; one that returns leaves no trace; one whose machine is gone is not a concept. With
						exits as messages, failure becomes something a program can be written about, and{' '}
						<A to="concepts/monitors-and-links/">Monitors and links</A> is the vocabulary for it.
					</p>
				</>
			)
		},
		{
			id: 'supervision',
			title: 'Let it crash',
			body: (
				<>
					<p>
						Erlang's answer to a process that fails is not to handle every error inside it. It is to
						let the process die and have another one, a supervisor, start it again from a known
						state. The reasoning: most failures come from a state the code did not anticipate, and
						the surest way out of such a state is to leave it. Code that tries to recover in place
						keeps the bad state and adds guesses to it.
					</p>
					<p>
						A supervisor is itself a process. It starts its children, monitors them, and when one
						exits it applies a policy: restart that child, restart all of them, or give up and exit
						itself, which is its own supervisor's problem. Supervisors of supervisors form a tree,
						and the tree is the program's plan for failure, written down once rather than scattered
						through every handler.
					</p>
					<p>
						In grpcproc a supervisor is nothing the core knows about: it is an ordinary process built
						on <C>SpawnMonitor</C>, <C>Down</C> and <C>Exit</C>, in the optional{' '}
						<C>grpcproc/actor</C> package. <A to="concepts/supervision/">Supervision trees</A> explains
						the strategies and what state should live outside the process so a restart can load it.
					</p>
				</>
			)
		},
		{
			id: 'grpcproc',
			title: 'What grpcproc adds to Go',
			body: (
				<>
					<ul>
						<li>
							<strong>Typed mailboxes.</strong> A process accepts one protobuf message type, or an
							interface over several, and its address carries that type. The compiler checks a send
							on the sending node, and the receiving node checks again on delivery, since types do not
							cross the wire.
						</li>
						<li>
							<strong>Your gRPC server as the transport.</strong> A node is one gRPC service
							registered next to your others, with your credentials, interceptors and keepalive. Two
							nodes hold one stream in each direction, and everything between them travels on it in
							order.
						</li>
						<li>
							<strong>At-most-once delivery, in order per sender.</strong> A message is delivered
							once or not at all, never twice, and two messages from one sender to one receiver arrive
							in the order they were sent. A <C>Down</C> comes after the last message of the process
							it reports on. What is lost is reported where it can be: a call fails, a send to a
							process that does not exist is a dead letter.
						</li>
						<li>
							<strong>Unbounded mailboxes.</strong> A send never blocks on the receiver. Every process
							on a node shares the link to a peer, so a mailbox that refused messages would stall the
							link for all of them. Backpressure is the application's, and the depth of every
							mailbox is visible to make it one.
						</li>
					</ul>
					<Aside title="Coming from Erlang">
						<p>
							A process is a process, its pid is a <C>PID</C> with a creation number called an
							incarnation, and a registered name is per node, as in <C>register/2</C>. There is no
							selective receive: the mailbox's type is fixed at spawn, and a message of another type
							is a dead letter. <C>gen_server</C> is the <C>actor</C> package's <C>Handler</C>, with{' '}
							<C>HandleCall</C> and <C>HandleMessage</C> for <C>handle_call</C> and{' '}
							<C>handle_cast</C>; a supervisor is <C>actor.Supervise</C>, with the same strategies and
							restart types. Links are one way, as in ergo, and an exit signal is not trapped, since a
							goroutine cannot be killed. Nodes connect over gRPC streams rather than the
							distribution protocol, and there is no <C>global</C>.
						</p>
					</Aside>
				</>
			)
		},
		{
			id: 'next',
			title: 'Where next',
			body: (
				<ul>
					<li>
						<A to="concepts/processes/">Processes and messages</A>: what a process can do, message
						by message.
					</li>
					<li>
						<A to="concepts/supervision/">Supervision trees</A>: the strategies, and how failure
						moves up the tree.
					</li>
					<li>
						<A to="guides/actors/">Actors</A>: a struct with a method per kind of message, instead of
						a receive loop.
					</li>
					<li>
						The reasons behind the choices, and what was rejected, are in the{' '}
						<Ext href="https://github.com/floatdrop/grpcproc/blob/main/docs/DESIGN.md">design notes</Ext>.
					</li>
				</ul>
			)
		}
	]
};
