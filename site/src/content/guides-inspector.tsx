import { Code } from '../code.tsx';
import { A, C, Ext, Table } from '../components/prose.tsx';
import { file } from '../config.ts';
import type { Doc } from './types.ts';

export const guidesInspector: Doc = {
	path: 'guides/inspector/',
	title: 'The Inspector',
	description:
		'A gRPC service that serves every node’s processes, links and events, and forwards to every other node’s, for grpcprocctl, an AI agent, or grpcurl.',
	lead: (
		<p>
			Everything <A to="guides/observability/">Observability</A> describes is a Go API on the node.
			The Inspector serves it over gRPC, on the server the node already runs, and forwards to
			every other node's, so one endpoint reaches the whole cluster.{' '}
			<A to="guides/grpcprocctl/">grpcprocctl</A> is its command line, and{' '}
			<A to="guides/mcp/">an AI agent</A> asks it the same questions over MCP.
		</p>
	),
	sections: [
		{
			id: 'serving',
			title: 'Serving it',
			body: (
				<>
					<p>
						<C>grpcproc/inspect</C> is part of the core module. It registers a second service,{' '}
						<C>grpcproc.inspect.v1.Inspector</C>, next to the node's own:
					</p>
					<Code>{`node.Register(grpcServer)
insp := inspect.New(node) // reaches other nodes' Inspectors as the node reaches the nodes
insp.Register(grpcServer)
defer insp.Close()`}</Code>
					<p>
						Every request names a node. One that is not this node is forwarded to that node's
						Inspector, which the server dials as the node dials its peers, <C>node.Dial</C>, through
						the node's own resolver and dial options: one connection per peer, closed by{' '}
						<C>Close</C>. <C>WithResolver</C> takes another resolver or other dial options, for
						Inspectors served elsewhere than on the port the nodes link through, and{' '}
						<C>WithPeers</C> any other way of reaching a peer's Inspector; <C>WithPeers(nil)</C>{' '}
						keeps a server to its own node. A process targeted by PID routes to the PID's
						node when the request names none. So a tool pointed at one node can ask about any, which
						is what a cluster of three programs on three hosts needs.
					</p>
					<p>
						<C>inspect.ReadOnly()</C> refuses the four writes: <C>Send</C>, <C>Call</C>,{' '}
						<C>Exit</C> and <C>SetLogLevel</C>. Anything finer is the job of the interceptors and transport
						credentials that guard your other services, as for any gRPC service you register. The
						tutorial's platform serves the Inspector read-only because it shares the node's port and
						nothing there authenticates.
					</p>
				</>
			)
		},
		{
			id: 'service',
			title: 'The service',
			body: (
				<>
					<Table
						head={['Method', 'What it answers']}
						rows={[
							[<C>GetNode</C>, <>The node's <C>NodeInfo</C>: counters, dead letters, and its links.</>],
							[
								<C>ListProcesses</C>,
								'Every process of a node, filtered by name, label, state or minimum mailbox depth.'
							],
							[
								<C>GetProcess</C>,
								<>
									One process's snapshot, and with <C>inspect: true</C> what it publishes through{' '}
									<C>WithInspect</C>. A process too busy to answer still gets its snapshot, with{' '}
									<C>inspect_error</C> saying so.
								</>
							],
							[<C>SetLogLevel</C>, "One process's log threshold."],
							[<C>Send</C>, <>A message, as an <C>Any</C>, from a tool.</>],
							[
								<C>Call</C>,
								<>
									A call, as <C>Node.CallTo</C> makes it: the answer as an <C>Any</C>, the request's
									deadline as the call's, and an answer that is an error as <C>Unknown</C> with its
									text.
								</>
							],
							[<C>Exit</C>, <>A request to exit; the reason defaults to <C>killed</C>.</>],
							[<C>Watch</C>, <><C>Node.Subscribe</C> over the wire: spawns, exits, links, dead letters.</>]
						]}
					/>
					<p>
						A watch holds a buffer of up to 4096 events, about 1.7 MB, and the client picks the
						size below that. The server allocates it, so limit how many streams a client may open,
						with <C>grpc.MaxConcurrentStreams</C> or an interceptor.
					</p>
					<p>
						It is a plain gRPC service with reflection-friendly messages, so <C>grpcurl</C> works on
						it too.
					</p>
				</>
			)
		},
		{
			id: 'clients',
			title: 'Its clients',
			body: (
				<>
					<p>
						<A to="guides/grpcprocctl/">grpcprocctl</A> is its command line: processes and their
						mailboxes, nodes and links, events as they happen, leader elections and cron jobs.{' '}
						<A to="guides/mcp/">An AI agent over MCP</A> asks the same questions through{' '}
						<C>grpcprocctl mcp</C>. Both live in{' '}
						<Ext href={file('tools/README.md')}>grpcproc/tools</Ext>, a separate module, so grpcproc
						itself carries no CLI or MCP dependencies.
					</p>
				</>
			)
		}
	]
};
