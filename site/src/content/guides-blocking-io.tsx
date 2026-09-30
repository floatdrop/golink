import blockingio from '../../../examples/blockingio/main.go?raw';
import blockingioOutput from '../../../examples/blockingio/output.txt?raw';
import connProto from '../../../examples/connpb/conn.proto?raw';

import { Code, Output, region } from '../code.tsx';
import { A, Aside, C, Ext } from '../components/prose.tsx';
import { file } from '../config.ts';
import type { Doc } from './types.ts';

export const guidesBlockingIO: Doc = {
	path: 'guides/blocking-io/',
	title: 'Blocking I/O',
	description:
		'Sockets, pipes and other blocking reads in processes: when a process may block, how one that takes messages hands its reads to a goroutine that only sends, and what other goroutines may call.',
	lead: (
		<p>
			A process is a goroutine you write, and <C>Receive</C> is a call it makes, so nothing stops it
			from blocking on a socket. What it gives up while blocked is its mailbox: when that is fine,
			and what to do when it is not. The code is from{' '}
			<Ext href={file('examples/blockingio/main.go')}>examples/blockingio</Ext>, a line server whose
			listener and connections are processes.
		</p>
	),
	sections: [
		{
			id: 'block',
			title: 'A process that takes no messages may block',
			body: (
				<>
					<p>
						The listener accepts connections, and nothing is sent to it, so it blocks in{' '}
						<C>Accept</C> as any Go code would. It does not call <C>Receive</C> meanwhile, so a
						request to inspect it waits, and when its deadline ends it says how long the listener has
						been busy: for a listener, the whole truth.
					</p>
					<p>
						What it must still do is end when asked. An <C>Exit</C> cancels the process's context,
						but nothing can interrupt a goroutine, so <C>context.AfterFunc</C> closes the listener
						when the context ends. <C>Accept</C> returns, and the process returns the context's
						cause, the <C>*ExitError</C> with the reason it was asked to exit with.
					</p>
					<Code caption="examples/blockingio/main.go">{region(blockingio, /^\/\/ listener accepts connections/, /^}/)}</Code>
					<p>
						Each connection is a process of its own, spawned with <C>LinkParent</C>: linked to the
						listener, one way, so that it ends when the listener does. That is all the ownership a
						connection needs.
					</p>
				</>
			)
		},
		{
			id: 'reader',
			title: 'A process that also takes messages',
			body: (
				<>
					<p>
						A connection's process has two things to do at once: read what the peer sends, and write
						what other processes ask it to. Blocked in <C>Read</C>, it could not do the second. So
						the process keeps its mailbox and its state, and the blocking reads go to a goroutine of
						its own, which touches none of that state: it only sends each line it reads to the
						process's mailbox, where the line waits in order with the writes. The process's loop is
						the one place that handles either.
					</p>
					<Code caption="examples/blockingio/main.go">{region(blockingio, /^\/\/ serve runs one connection/, /^}/)}</Code>
					<p>
						The mailbox is still the only thing the process waits on, and <C>Receive</C> returns a
						line, a write, or the end of the connection. Because the process waits there and not in{' '}
						<C>Read</C>, it stays what any process is: it answers inspection, makes calls, holds
						monitors.
					</p>
					<p>
						A mailbox holds one message type, and it is a protobuf message, so a line from a socket
						becomes one even though it never leaves the node. The example's <C>Event</C> is a oneof
						of the three things a connection takes. Local delivery passes the pointer and encodes
						nothing, so this costs a type, not time.
					</p>
					<Code lang="proto" caption="examples/connpb/conn.proto">
						{connProto}
					</Code>
				</>
			)
		},
		{
			id: 'goroutines',
			title: 'What another goroutine may call',
			body: (
				<>
					<p>
						<C>Receive</C> and <C>ReceiveTimeout</C> belong to the process's own goroutine. Other
						goroutines may send and call as the process, ask another process to exit, and use its{' '}
						<C>PID</C>, <C>Addr</C>, <C>Node</C>, <C>Context</C> and <C>Log</C>; a message's{' '}
						<C>Reply</C> works from anywhere.
					</p>
					<p>
						Sending as the process from another goroutine has two side effects. The send inherits
						the metadata of whatever message the process is handling at that moment, so a trace
						would tie a line from the socket to an unrelated request; and a <C>Call</C> made there
						shows the process as waiting on a reply while its own goroutine may be idle. The reader
						in the example sends as the node instead, <C>self.Send(ctx, node, …)</C>: the message
						comes from the node's own PID and carries nobody's metadata.
					</p>
				</>
			)
		},
		{
			id: 'ending',
			title: 'How it ends',
			body: (
				<>
					<p>
						Every way out closes the connection, and the blocked goroutine learns of it from the
						connection. The process's exit, whether it was asked to exit, the listener's link ended
						it, or the node is stopping, cancels its context, and <C>AfterFunc</C> closes the
						connection: the reader's <C>Read</C> fails, and the reader stops. When the peer hangs up
						first, the reader's <C>Read</C> fails on its own; the reader sends why, and the loop
						returns nil, a normal exit. The reader checks the context before it sends that, so an
						exit the process started leaves no dead letter behind.
					</p>
					<p>
						The program talks to its server as a client would, then asks the listener to exit. The
						listener's link ends the connection's process, whose exit closes the connection under
						the client:
					</p>
					<Code caption="examples/blockingio/main.go">{region(blockingio, /A client, as the outside world sees/, /connection open:/)}</Code>
					<Output>{blockingioOutput}</Output>
				</>
			)
		},
		{
			id: 'edge',
			title: 'The edge needs no process',
			body: (
				<>
					<p>
						Code outside any process reaches processes with the node as the sender: an address's{' '}
						<C>Send</C> and <C>Call</C>, or the node's <C>SendTo</C> and <C>CallTo</C>, work from
						any goroutine. An HTTP or gRPC handler is such code: it calls the process that does the
						work and writes the answer, with the request's context bounding the wait, as{' '}
						<A to="shop/services/#edge">the shop's web front</A> does. A process per connection is
						for what a server library does not already handle: a raw socket, a pipe, a device.
					</p>
					<Aside title="Coming from Ergo">
						<p>
							Ergo has meta-processes because an Ergo process never owns a goroutine: its callbacks run
							on one started when a message arrives, so there is nowhere to block. A meta-process adds
							that goroutine, with rules of its own: it cannot make calls or links or monitors, and its
							children can only be meta-processes. Here the process is the goroutine, so a
							meta-process is a process and a goroutine that only sends: its <C>Start</C> is the
							reader, its <C>HandleMessage</C> the process's loop, and <C>SpawnMeta</C> is{' '}
							<C>Spawn</C> with <C>LinkParent</C>, without the restrictions. Its TCP, UDP, web and port
							meta-processes are loops like the listener's around <C>net</C>, <C>net/http</C> and{' '}
							<C>os/exec</C>.
						</p>
					</Aside>
				</>
			)
		}
	]
};
