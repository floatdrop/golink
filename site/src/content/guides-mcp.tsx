import { Code } from '../code.tsx';
import { A, C, Table } from '../components/prose.tsx';
import type { Doc } from './types.ts';

export const guidesMcp: Doc = {
	path: 'guides/mcp/',
	title: 'An AI agent over MCP',
	description:
		'grpcprocctl serves the Inspector as MCP tools, so an agent can investigate a cluster the way a person would: read-only unless told otherwise.',
	lead: (
		<p>
			<C>grpcprocctl mcp</C> is an MCP server over stdio. It offers an agent the questions{' '}
			<A to="guides/grpcprocctl/">grpcprocctl</A> answers, as tools that return the same objects its{' '}
			<C>--json</C> prints, and explains grpcproc to the agent first: what a pid and a label are, what a
			deep mailbox or a busy process means. So "orders are slow since the deploy" becomes a symptom it
			can chase through the cluster itself.
		</p>
	),
	sections: [
		{
			id: 'connecting',
			title: 'Connecting an agent',
			body: (
				<>
					<p>For Claude Code:</p>
					<Code lang="sh">{'claude mcp add grpcproc -- grpcprocctl --plaintext --addr 10.0.0.5:9000 mcp'}</Code>
					<p>For any client that starts stdio servers from a configuration file:</p>
					<Code lang="txt">{`{
  "mcpServers": {
    "grpcproc": {
      "command": "grpcprocctl",
      "args": ["--plaintext", "--addr", "10.0.0.5:9000", "mcp"]
    }
  }
}`}</Code>
					<p>
						The connection flags are grpcprocctl's own: TLS unless <C>--plaintext</C>,{' '}
						<C>--cacert</C>, <C>--cert</C> and <C>--key</C>. <C>--timeout</C> bounds each request a
						tool makes, and the server reports grpcprocctl's version to the client.
					</p>
				</>
			)
		},
		{
			id: 'tools',
			title: 'Tools',
			body: (
				<>
					<Table
						head={['Tool', 'What it answers']}
						rows={[
							[<C>cluster_nodes</C>, 'Every node reachable from the one serving the Inspector, and which could not be reached.'],
							[<C>node_info</C>, 'One node: counts, dead letters, and each link with its traffic, queue and last error.'],
							[
								<C>list_processes</C>,
								'Processes of a node, filtered by name, label, state or mailbox depth, and sorted: by mailbox, to find backlogs. At most 100 unless asked for more.'
							],
							[<C>get_process</C>, 'One process by pid or name, with what it says about itself, waiting a while for a busy one.'],
							[<C>watch_events</C>, 'A node’s events, collected for a few seconds: spawns, exits with reasons, links, dead letters.'],
							[<C>election</C>, 'A leader election, as each node that takes part sees it, and who leads.'],
							[<C>cron_jobs</C>, 'Cron processes and their jobs, on a node or all of them.'],
							[<C>global_names</C>, 'Who holds a global name, or every name with a prefix, such as room:.']
						]}
					/>
					<p>
						With <C>grpcprocctl mcp --allow-writes</C>, it also offers the tools that change things:
					</p>
					<Table
						head={['Tool', 'What it does']}
						rows={[
							[<C>exit_process</C>, 'Asks a process to exit; its supervisor, if any, may restart it.'],
							[<C>set_log_level</C>, 'Sets one process’s log level, to see more of it without restarting anything.'],
							[<C>move_leader</C>, 'Has an election’s leader hand over, to a node or to the most up-to-date follower.'],
							[
								<>
									<C>cordon_node</C>, <C>uncordon_node</C>
								</>,
								'Keeps a node from leading, to work on its host, or lets it lead again.'
							],
							[
								<>
									<C>enable_cron_job</C>, <C>disable_cron_job</C>, <C>remove_cron_job</C>
								</>,
								'Changes a job of a cron process.'
							]
						]}
					/>
					<p>
						The tools that stop a process, move a leader, cordon a node or remove a job are marked
						destructive, which a client can use to ask before it lets the agent run one.
					</p>
				</>
			)
		},
		{
			id: 'investigating',
			title: 'An investigation',
			body: (
				<>
					<p>Asked why orders are slow, an agent would:</p>
					<ol>
						<li>
							call <C>cluster_nodes</C>, and see every node up, and no link failing;
						</li>
						<li>
							call <C>list_processes</C> on <C>orders-1</C> sorted by mailbox, and find{' '}
							<C>ledger-writer</C> with 41 messages waiting, the oldest for a second;
						</li>
						<li>
							call <C>get_process</C> on it, and read <C>inspect_error: busy for 1.187s</C>: it is
							inside a handler, on one message;
						</li>
						<li>
							call <C>watch_events</C>, and see no exits or dead letters: the process is slow, not
							failing, so the cause is what that handler waits on.
						</li>
					</ol>
					<p>
						It reasons over the snapshots a person would read in grpcprocctl's tables, and says what
						it saw; with writes allowed, it can then act, raising the log level of the process to
						see what its handler is doing.
					</p>
				</>
			)
		},
		{
			id: 'safety',
			title: 'What keeps it safe',
			body: (
				<>
					<ul>
						<li>
							<strong>Read-only unless told.</strong> Without <C>--allow-writes</C>, the tools that
							change things are not offered at all.
						</li>
						<li>
							<strong>The Inspector decides too.</strong> An Inspector served with{' '}
							<C>inspect.ReadOnly()</C> refuses every write, whatever the tool; credentials and
							interceptors guard it as they guard your other services.
						</li>
						<li>
							<strong>Bounded answers.</strong> Lists are capped, watches last at most a minute, and
							every input an agent sends is checked before it reaches a node, so a tool cannot be
							made to hold a node or return everything it has.
						</li>
					</ul>
				</>
			)
		}
	]
};
