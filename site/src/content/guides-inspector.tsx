import { Code, Output } from '../code.tsx';
import { A, C, Ext, Table } from '../components/prose.tsx';
import { file } from '../config.ts';
import type { Doc } from './types.ts';

export const guidesInspector: Doc = {
	path: 'guides/inspector/',
	title: 'The Inspector and grpcprocctl',
	description:
		'A gRPC service that serves every node’s processes, links and events to a command line, a Graphviz drawing or an AI agent over MCP.',
	lead: (
		<p>
			Everything <A to="guides/observability/">Observability</A> describes is a Go API on the node.
			The Inspector serves it over gRPC, on the server the node already runs, and{' '}
			<C>grpcprocctl</C> is its command line. One endpoint reaches the whole cluster, and an AI
			agent can ask the same questions over MCP.
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
insp := inspect.New(node, inspect.WithResolver(resolver, dialOptions...)) // reaches other nodes' Inspectors
insp.Register(grpcServer)
defer insp.Close()`}</Code>
					<p>
						Every request names a node. One that is not this node is forwarded to that node's
						Inspector, which <C>WithResolver</C> dials through the node's own resolver and dial
						options, one connection per peer, closed by <C>Close</C>. <C>WithPeers</C> takes any
						other way of reaching a peer's Inspector. A process targeted by PID routes to the PID's
						node when the request names none. So a tool pointed at one node can ask about any, which
						is what a cluster of three programs on three hosts needs.
					</p>
					<p>
						<C>inspect.ReadOnly()</C> refuses the three writes: <C>Send</C>, <C>Exit</C> and{' '}
						<C>SetLogLevel</C>. Anything finer is the job of the interceptors and transport
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
			id: 'grpcprocctl',
			title: 'grpcprocctl',
			body: (
				<>
					<p>
						<Ext href={file('tools/README.md')}>grpcproc/tools</Ext> is a separate module, so grpcproc
						itself carries no CLI or MCP dependencies.
					</p>
					<Code lang="sh">{`go install github.com/floatdrop/grpcproc/tools/cmd/grpcprocctl@latest
export GRPCPROC_ADDR=10.0.0.5:9000   # or --addr; TLS by default, --plaintext without
grpcprocctl --plaintext ps --sort mailbox`}</Code>
					<Output>{`PID                  NAME           LABEL        STATE    MAILBOX  OLDEST  RECEIVED  SENT  LAST MESSAGE    UPTIME
<orders-1.1718.4>    ledger-writer  ledger       running  41       1.111s  1         0     ledger.v1.Post  1.112s
<orders-1.1718.1>    orders-sup     supervisor   idle     0                0         0                     1.112s
<orders-1.1718.2>    reservations   reservation  idle     0                0         0                     1.112s
<orders-1.1718.3>    payments       payment      idle     0                0         0                     1.112s
<orders-1.1718.5>    bank-session   session      idle     0                0         0                     1.112s`}</Output>
					<p>
						<C>ledger-writer</C> has been running one message for a second while 41 wait. Ask it what
						it believes, and it cannot answer, because it is busy:
					</p>
					<Code lang="sh">{'grpcprocctl --plaintext inspect --wait 50ms ledger-writer'}</Code>
					<Output>{`state:            running
mailbox:          41 (peak 42, oldest 1.136s)
last message:     ledger.v1.Post
inspect:          grpcproc: inspect <orders-1.1718.4>: busy for 1.187s: context deadline exceeded`}</Output>
					<p>
						A process that is free answers with whatever it publishes; a supervisor lists its
						children and its restarts:
					</p>
					<Output>{`name:                  orders-sup
label:                 supervisor
monitors:              2
  child.payments:      <orders-1.1718.3> permanent restarts=0
  child.reservations:  <orders-1.1718.2> permanent restarts=0
  restarts:            0/3 in 5s
  strategy:            one_for_one`}</Output>
					<Table
						head={['Command', '']}
						rows={[
							[<C>node [name]</C>, 'Counters and links of a node.'],
							[<C>nodes</C>, 'Every node reachable from this one, following links.'],
							[
								<C>ps</C>,
								<>
									Processes: <C>--node</C>, <C>--name</C>, <C>--label</C>, <C>--state</C>,{' '}
									<C>--min-mailbox</C>, <C>--sort pid|mailbox|received|sent</C>, <C>--limit</C>.
								</>
							],
							[
								<C>inspect &lt;pid|name&gt;</C>,
								<>
									One process, with what it says about itself: <C>--node</C>, <C>--wait</C>.
								</>
							],
							[
								<C>watch</C>,
								<>
									Stream spawns, exits, links, dead letters: <C>--node</C>, <C>--kind</C>, <C>--count</C>.
								</>
							],
							[<C>exit &lt;pid|name&gt; [reason]</C>, 'Ask a process to exit.'],
							[<C>loglevel &lt;pid|name&gt; &lt;level&gt;</C>, "Change one process's log level."],
							[
								<C>dot</C>,
								<>
									Graphviz of processes and who started whom: <C>--node</C>, <C>--cluster</C>.
								</>
							],
							[<C>mcp</C>, <>Serve these as MCP tools over stdio: <C>--allow-writes</C>.</>]
						]}
					/>
					<p>
						A pid is written as grpcproc prints it, <C>&lt;node.incarnation.id&gt;</C>; a name is
						looked up on <C>--node</C>, by default the node serving the Inspector. Connection flags
						follow grpcurl: <C>--plaintext</C>, <C>--cacert</C>, <C>--cert</C> and <C>--key</C> for
						mutual TLS, <C>--servername</C>. <C>grpcprocctl --version</C> prints the version it was
						installed at, which is also what its MCP server reports.
					</p>
					<p>
						<C>--json</C> before <C>node</C>, <C>nodes</C>, <C>ps</C>, <C>inspect</C> or <C>watch</C>{' '}
						prints the same data as JSON: one indented value, or for <C>watch</C> one compact event
						per line, so <C>grpcprocctl --json watch | jq</C> sees events as they happen. The objects
						are those the MCP tools return, which wrap lists in an object of their own.
					</p>
					<Code lang="sh">{'grpcprocctl --plaintext dot --cluster | dot -Tsvg -o processes.svg'}</Code>
					<p>
						draws each node as a cluster, each supervisor bold, an edge from each process to those it
						started, and any process with waiting messages in red.
					</p>
				</>
			)
		},
		{
			id: 'mcp',
			title: 'For an AI agent',
			body: (
				<>
					<Code lang="sh">{'claude mcp add grpcproc -- grpcprocctl --plaintext --addr 10.0.0.5:9000 mcp'}</Code>
					<p>
						The server explains grpcproc to the agent, what a pid and a label are, what a deep mailbox
						or a busy process means, and offers:
					</p>
					<Table
						head={['Tool', '']}
						rows={[
							[<C>cluster_nodes</C>, 'Every reachable node, and which could not be reached.'],
							[<C>node_info</C>, 'One node: counts, dead letters, link traffic and errors.'],
							[<C>list_processes</C>, 'Filter and sort, by mailbox say, to find backlogs.'],
							[<C>get_process</C>, 'One process, with what it says about itself.'],
							[<C>watch_events</C>, 'Collect events for a few seconds.'],
							[
								<>
									<C>exit_process</C>, <C>set_log_level</C>
								</>,
								<>Only with <C>--allow-writes</C>.</>
							]
						]}
					/>
					<p>
						So "orders are slow since the deploy" becomes: list processes by mailbox, find{' '}
						<C>ledger-writer</C> with 41 waiting, inspect it, see it busy on one message for a
						second, watch events for exits and dead letters. The agent reasons over the same
						snapshots a person would read, and the read-only default keeps it from acting on them
						unless told it may.
					</p>
				</>
			)
		}
	]
};
