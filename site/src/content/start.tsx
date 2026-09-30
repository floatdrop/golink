import quickstart from '../../../examples/quickstart/main.go?raw';
import quickstartOutput from '../../../examples/quickstart/output.txt?raw';
import shopProto from '../../../examples/shoppb/shop.proto?raw';

import { Code, Output, region } from '../code.tsx';
import { A, Aside, C, Ext } from '../components/prose.tsx';
import { file } from '../config.ts';
import type { Doc } from './types.ts';

export const start: Doc = {
	path: 'start/',
	title: 'Getting started',
	description:
		'Install grpcproc, put a node on your gRPC server, spawn a process, and reach it from another node by name.',
	lead: (
		<p>
			The program on the front page, built one piece at a time. It is{' '}
			<Ext href={file('examples/quickstart/main.go')}>examples/quickstart</Ext> in the repository;
			the code here is cut from it.
		</p>
	),
	sections: [
		{
			id: 'install',
			title: 'Install',
			body: (
				<>
					<Code lang="sh">{'go get github.com/floatdrop/grpcproc'}</Code>
					<p>
						grpcproc needs Go 1.27 or later. Messages are protobuf messages, and a mailbox holds one
						type. The example uses two, compiled with <C>protoc-gen-go</C>:
					</p>
					<Code lang="proto" caption="examples/shoppb/shop.proto">
						{region(shopProto, /^\/\/ Reserve asks/, /^}/) + '\n\n' + region(shopProto, /^\/\/ Reserved is/, /^}/)}
					</Code>
				</>
			)
		},
		{
			id: 'node',
			title: 'A node on your gRPC server',
			body: (
				<>
					<p>
						A <em>node</em> is grpcproc in one program: it hosts processes and links to the nodes of
						other programs. Construct it, register it on your gRPC server, start it once the server
						serves, and stop it before the server stops:
					</p>
					<Code caption="examples/quickstart/main.go">{region(quickstart, /^\/\/ peers is where/, /^}/)}</Code>
					<p>
						<C>Config</C> needs a <C>Name</C>, which is how peers address the node, and a{' '}
						<C>Resolver</C>, which turns a peer's name into an address to dial. The example's is a
						map; <A to="concepts/discovery/">Discovery</A> covers the rest. <C>DialOptions</C> are
						for every connection the node opens to a peer: the credentials, and the keepalive that
						turns a silent partition into a broken link. <A to="guides/configuration/">Configuration</A>{' '}
						goes through every field.
					</p>
					<p>
						<C>Register</C> mounts one gRPC service, <C>grpcproc.v1.Node</C>, next to yours. Peers
						open a stream to it and this node opens one to them; every message, call, reply and
						monitor between the two travels on those streams, in order.
					</p>
				</>
			)
		},
		{
			id: 'process',
			title: 'A process',
			body: (
				<>
					<p>
						A process is a function over a typed mailbox, run on its own goroutine until it returns.
						This one holds a stock of apples. Its mailbox holds <C>*shoppb.Reserve</C> and nothing
						else, and every message it takes is a <em>call</em>: the sender waits for an answer,
						which <C>Reply</C> gives, as a message or an error.
					</p>
					<Code caption="examples/quickstart/main.go">{region(quickstart, /^\/\/ inventory is a process/, /^}/)}</Code>
					<p>
						<C>Receive</C> blocks until a message arrives, and returns an error when the process
						should stop: it was asked to exit, or its node is stopping. Returning that error is the
						whole of the exit handling. A process that returns <C>nil</C> ends with reason{' '}
						<C>normal</C>; one that returns an error, or panics, ends with that as its reason, and
						whoever monitors it learns which.
					</p>
					<Aside title="Coming from Erlang">
						<p>
							A process is a goroutine, a mailbox is a channel-backed queue, and <C>Receive</C> is a{' '}
							<C>receive</C> with no pattern matching: the mailbox's type is fixed at spawn, and a
							message of another type is a dead letter before it gets in. <C>Reply</C> is{' '}
							<C>gen_server:reply</C>; it can run later, from any goroutine.
						</p>
					</Aside>
				</>
			)
		},
		{
			id: 'call',
			title: 'Spawn it, and call it from another node',
			body: (
				<>
					<p>
						<C>Spawn</C> runs the function as a process on the node; <C>WithName</C> registers it
						under a name for as long as it runs. From another node, the process is a node name and a
						process name, and <C>Named</C> makes an address from the two:
					</p>
					<Code caption="examples/quickstart/main.go">
						{region(quickstart, /Two nodes, each on its own/, /fmt.Println\("reserved, left:"/) + '\n\t}'}
					</Code>
					<p>
						The address is typed: <C>Addr[*shoppb.Reserve]</C> says what the process behind it
						accepts, so the compiler checks every send to it, wherever it runs. <C>Call</C> sends and
						waits for the reply, typed by what the caller asks for. An error the handler returned
						comes back as a <C>*RemoteError</C>, its text carried across the link. The context
						bounds the whole call.
					</p>
					<p>
						Nothing in the shop's code says the process is remote: the same call works with a local
						address.
					</p>
				</>
			)
		},
		{
			id: 'monitor',
			title: 'Monitor it',
			body: (
				<>
					<p>
						A process can watch another. <C>Monitor</C> asks for a <C>Down</C> when the target
						exits, or when its node cannot be reached, and the <C>Down</C> arrives in the watcher's
						mailbox like any message, after everything the target sent it. Here a process on the
						shop monitors the stock and then asks it to exit:
					</p>
					<Code caption="examples/quickstart/main.go">
						{region(quickstart, /A monitor across nodes works/, /fmt.Println\("no such process:"/)}
					</Code>
					<p>
						The watcher's mailbox is <C>proto.Message</C>, the untyped one, since it expects nothing
						but the <C>Down</C>. <C>Exit</C> asks a process anywhere to stop, with a reason; the
						process's next <C>Receive</C> returns an error, it returns, and its watchers get the
						reason. The last call finds no process under the name, and fails with <C>ErrNoProc</C>.
					</p>
				</>
			)
		},
		{
			id: 'run',
			title: 'Run it',
			body: (
				<>
					<Code lang="sh">{'git clone https://github.com/floatdrop/grpcproc && cd grpcproc/examples\ngo run ./quickstart'}</Code>
					<Output>{quickstartOutput}</Output>
				</>
			)
		},
		{
			id: 'next',
			title: 'Where next',
			body: (
				<>
					<ul>
						<li>
							<A to="concepts/actors/">The actor model</A> says why processes and mailboxes, for a
							reader who has not met them, and <A to="concepts/processes/">Processes and messages</A>{' '}
							goes through what a process can do.
						</li>
						<li>
							A receive loop with a <C>switch</C> in it grows old. <A to="guides/actors/">Actors</A>{' '}
							puts a method per kind of message on a struct, and{' '}
							<A to="concepts/supervision/">Supervision trees</A> restart it when it fails.
						</li>
						<li>
							<A to="shop/">The tutorial</A> builds a whole application: three services, run as one
							program or as three nodes from the same code.
						</li>
					</ul>
				</>
			)
		}
	]
};
