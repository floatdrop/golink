import quickstart from '../../../examples/quickstart/main.go?raw';
import quickstartOutput from '../../../examples/quickstart/output.txt?raw';

import { Code, Output, region } from '../code.tsx';
import { A, C, Cards, Ext } from '../components/prose.tsx';
import { PKG_DOC, file } from '../config.ts';
import type { Doc } from './types.ts';

/** The landing page's opening. */
export const hero = {
	title: 'Erlang-style processes for Go, on the gRPC server you already run.',
	lead: (
		<>
			A process is a goroutine with a typed mailbox and an address the whole cluster can use.{' '}
			<C>Send</C>, <C>Call</C>, <C>Monitor</C> and <C>Exit</C> work the same whether it runs in this
			binary or on another node, and supervisors restart what fails.
		</>
	),
	install: 'go get github.com/floatdrop/grpcproc'
};

export const landing: Doc = {
	path: '',
	title: 'Overview',
	description:
		'Erlang-style processes for Go, on the gRPC server you already run: typed mailboxes, calls, monitors and supervision trees that work the same within a node and across nodes.',
	sections: [
		{
			id: 'look',
			title: 'A quick look',
			body: (
				<>
					<p>
						Two nodes, each on its own gRPC server. A process runs on one under a name, and the
						other calls it, monitors it and asks it to exit. Nothing in the code says which of the
						two is remote:
					</p>
					<Code caption="examples/quickstart/main.go">
						{region(quickstart, /\/\/ inventory is a process/, /check\(warehouse.Stop\(ctx\)\)/) + '\n}'}
					</Code>
					<Output>{quickstartOutput}</Output>
					<p>
						<Ext href={file('examples/quickstart/main.go')}>The whole program</Ext> is a few lines longer:
						the <C>node</C> function builds a node, registers it on a gRPC server and starts it.{' '}
						<A to="start/">Getting started</A> walks through it.
					</p>
				</>
			)
		},
		{
			id: 'map',
			title: 'Where to go',
			body: (
				<>
					<Cards
						items={[
							{
								to: 'start/',
								title: 'Getting started',
								text: 'Install, put a node on your gRPC server, spawn a process and call it from another node.'
							},
							{
								to: 'concepts/actors/',
								title: 'Concepts',
								text: 'The actor model for readers with no Erlang: processes, addresses, monitors, links, supervision trees, nodes.'
							},
							{
								to: 'guides/actors/',
								title: 'Guides',
								text: 'Actors and supervisors, pub/sub, configuration, testing a cluster, observability, the Inspector, etcd, cron jobs, leader election.'
							},
							{
								to: 'shop/',
								title: 'Tutorial',
								text: 'A shop whose services are actors, run as one program or as three nodes from the same code.'
							},
							{
								to: PKG_DOC,
								title: 'API reference',
								text: 'Every type and function, on pkg.go.dev.'
							},
							{
								to: 'reference/errors/',
								title: 'Errors and exit reasons',
								text: 'What a failed send or call returns, and the reasons a process exits with.'
							}
						]}
					/>
				</>
			)
		},
		{
			id: 'why',
			title: 'What makes it different',
			body: (
				<>
					<ul>
						<li>
							<strong>It is a library, not a runtime.</strong> Your application brings its{' '}
							<C>*grpc.Server</C>, credentials, discovery, logger and lifecycle; grpcproc registers one
							gRPC service on that server and starts no goroutine outside <C>Start</C> and <C>Stop</C>.
							The core depends on gRPC and protobuf only.
						</li>
						<li>
							<strong>Mailboxes are typed.</strong> An address carries the message type its process
							accepts, so a send to it is checked by the compiler, on the same node or another. The
							messages are the protobuf messages you already define.
						</li>
						<li>
							<strong>Failure is a message.</strong> A monitor turns the exit of a process, or the
							loss of the node it runs on, into a <C>Down</C> with a reason, delivered in order after
							the process's last messages. Supervisors are built on nothing else.
						</li>
						<li>
							<strong>Introspection comes first.</strong> Every process has counters, a mailbox depth
							and a state; a process can publish what it believes; and an Inspector serves all of it
							over gRPC to a command line, a Graphviz drawing or an AI agent.
						</li>
					</ul>
					<p>
						The <Ext href={file('docs/DESIGN.md')}>design notes</Ext> have the wire protocol, the reasons
						behind these choices and what was rejected; <A to="reference/performance/">Performance</A>{' '}
						measures grpcproc against GoAkt, Hollywood, Proto.Actor and Ergo.
					</p>
				</>
			)
		}
	]
};
