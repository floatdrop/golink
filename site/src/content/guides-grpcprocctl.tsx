import { Code, Output } from '../code.tsx';
import { Screenshot } from '../components/Figure.tsx';
import { A, C, Ext, Table } from '../components/prose.tsx';
import { file } from '../config.ts';
import type { Doc } from './types.ts';

export const guidesGrpcprocctl: Doc = {
	path: 'guides/grpcprocctl/',
	title: 'grpcprocctl',
	description:
		'The command line of the Inspector: processes and their mailboxes, nodes and links, events as they happen, leader elections and cron jobs, for the whole cluster from one endpoint, in a terminal or a browser.',
	lead: (
		<p>
			<C>grpcprocctl</C> asks a node's <A to="guides/inspector/">Inspector</A>, which forwards to
			every other node's, so one address reaches the whole cluster. It reads what the node's Go API
			says, changes what the Inspector lets it change, and serves the same as a page in the{' '}
			<A to="guides/grpcprocctl/#web">browser</A> and as MCP tools for an{' '}
			<A to="guides/mcp/">AI agent</A>. It is{' '}
			<Ext href={file('tools/README.md')}>grpcproc/tools</Ext>, a separate module.
		</p>
	),
	sections: [
		{
			id: 'connecting',
			title: 'Install and connect',
			body: (
				<>
					<Code lang="sh">{`go install github.com/floatdrop/grpcproc/tools/cmd/grpcprocctl@latest
export GRPCPROC_ADDR=10.0.0.5:9000   # or --addr on each command
grpcprocctl --plaintext nodes`}</Code>
					<p>
						It connects with TLS unless told <C>--plaintext</C>. The flags follow grpcurl's:{' '}
						<C>--cacert</C> for a CA of your own, <C>--cert</C> and <C>--key</C> for mutual TLS,{' '}
						<C>--servername</C> when the name to verify differs from the address.{' '}
						<C>--timeout</C> bounds each request, 5s by default, and <C>--version</C> prints the
						version it was installed at. Connection flags come before the command; the command's
						own flags come after its name.
					</p>
				</>
			)
		},
		{
			id: 'backlogs',
			title: 'Finding a backlog',
			body: (
				<>
					<p>
						<C>ps</C> lists a node's processes. Sorted by mailbox, the ones that cannot keep up come
						first:
					</p>
					<Code lang="sh">{'grpcprocctl --plaintext ps --sort mailbox'}</Code>
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
						A process that is free answers with whatever it publishes through{' '}
						<C>WithInspect</C>; a supervisor lists its children and its restarts:
					</p>
					<Output>{`name:                  orders-sup
label:                 supervisor
monitors:              2
  child.payments:      <orders-1.1718.3> permanent restarts=0
  child.reservations:  <orders-1.1718.2> permanent restarts=0
  restarts:            0/3 in 5s
  strategy:            one_for_one`}</Output>
					<p>
						<C>ps</C> filters by <C>--name</C> (a substring), <C>--label</C>, <C>--state</C> (idle,
						running, waiting-reply, exiting) and <C>--min-mailbox</C>; sorts by pid, mailbox,
						received or sent; and stops at <C>--limit</C>. A pid is written as grpcproc prints it,{' '}
						<C>&lt;node.incarnation.id&gt;</C>, and reaches its node wherever it is; a name is
						looked up on <C>--node</C>, by default the node serving the Inspector.
					</p>
				</>
			)
		},
		{
			id: 'nodes',
			title: 'Nodes and links',
			body: (
				<>
					<p>
						<C>nodes</C> walks the cluster from the node it asks, following links, and says which
						peers it could not reach. <C>node [name]</C> shows one: its processes, dead letters, and
						each link with its traffic, the envelopes queued on it and their bytes, and its last
						error. A down outbound link with a <C>RETRY IN</C> is a peer whose dials failed: sends to
						it fail at once until then.
					</p>
				</>
			)
		},
		{
			id: 'events',
			title: 'Watching events',
			body: (
				<>
					<p>
						<C>watch</C> streams a node's events as they happen: spawns, exits with their reasons,
						links going up and down, dead letters. <C>--kind</C> keeps some of them,{' '}
						<C>--count</C> stops after so many:
					</p>
					<Code lang="sh">{'grpcprocctl --plaintext watch --node orders-1 --kind exit,dead-letter'}</Code>
					<p>
						With <C>--json</C>, each event is a line of its own, so{' '}
						<C>grpcprocctl --json watch | jq</C> sees them as they come.
					</p>
				</>
			)
		},
		{
			id: 'changing',
			title: 'Changing a process',
			body: (
				<>
					<p>
						<C>exit &lt;pid|name&gt; [reason]</C> asks a process to exit, with reason{' '}
						<C>killed</C> unless told another; its supervisor, if it has one, starts it again.{' '}
						<C>loglevel &lt;pid|name&gt; &lt;level&gt;</C> lowers or raises one process's log
						threshold (debug, info, warn, error, or a number), to see more of it without restarting
						anything. An Inspector served with <C>inspect.ReadOnly()</C> refuses both.
					</p>
				</>
			)
		},
		{
			id: 'leader',
			title: 'Leader elections',
			body: (
				<>
					<p>
						<C>leader &lt;cluster&gt;</C> shows a <A to="guides/leader/">grpcproc/leader</A> election
						as each node that takes part sees it. Nodes that name different leaders, or lag a term
						behind, show a partition. <C>move</C>, <C>cordon</C> and <C>uncordon</C> go to whichever
						node leads, and print the table once every node agrees:
					</p>
					<Code lang="sh">{'grpcprocctl --plaintext leader cordon scheduler b   # b led: it hands over'}</Code>
					<Output>{`NODE  ROLE      TERM  LEADER  VIEW   STATE  CORDONED  UNREACHABLE  SINGLETON  BACKOFF  ERROR
a     leader    2     a       a,b,c  2.3    b                      <a.1.11>
b     follower  2     a       a,b,c  2.3    b                      none       200ms
c     follower  2     a       a,b,c  2.3    b                      none`}</Output>
					<p>
						<C>leader move &lt;cluster&gt;</C> hands leadership to the follower with the latest
						state, or <C>--to</C> the node named. A cordoned node campaigns no more until{' '}
						<C>leader uncordon</C>, across its own restarts: cordon a node, work on its host, start
						it again, uncordon it. If the nodes do not all agree by <C>--timeout</C>, the command
						shows the table as it stands and says so.
					</p>
				</>
			)
		},
		{
			id: 'cron',
			title: 'Cron jobs',
			body: (
				<>
					<p>
						<C>cron</C> finds every <A to="guides/cron/">grpcproc/cron</A> process, on{' '}
						<C>--node</C> or on every node, and lists its jobs. <C>enable</C>, <C>disable</C> and{' '}
						<C>remove</C> change one job, then list the process again:
					</p>
					<Code lang="sh">{'grpcprocctl --plaintext cron disable --node b billing yearly'}</Code>
					<Output>{`NODE  CRON     JOB     SPEC        ZONE  NEXT                  LAST  RUNNING  LAST FAILURE  ERROR
b     billing  leap    0 0 29 2 *  UTC   2028-02-29T00:00:00Z        0
b     billing  paused  0 * * * *   UTC   disabled                    0
b     billing  yearly  @yearly     UTC   disabled                    0`}</Output>
					<p>
						A cron process is known by the message it takes, whatever it is named. Its runs are
						processes of their own, labelled <C>cron:&lt;job&gt;</C>:{' '}
						<C>grpcprocctl ps --label cron:yearly</C> shows those going.
					</p>
				</>
			)
		},
		{
			id: 'dot',
			title: 'Drawing the cluster',
			body: (
				<>
					<Code lang="sh">{'grpcprocctl --plaintext dot --cluster | dot -Tsvg -o processes.svg'}</Code>
					<p>
						draws each node as a cluster, each supervisor bold, an edge from each process to those it
						started, and any process with waiting messages in red. Without <C>--cluster</C> it draws
						one node, <C>--node</C> or the one it asks.
					</p>
				</>
			)
		},
		{
			id: 'web',
			title: 'In a browser',
			body: (
				<>
					<Code lang="sh">{'grpcprocctl --plaintext web   # http://localhost:9911'}</Code>
					<p>
						serves a page, in the spirit of Erlang's observer, that reads the cluster through the
						same Inspector and refreshes every second. The Cluster view maps the nodes and the
						messages per second on each link; Node charts one node's counters and links over the
						last minute; Processes is the table <C>ps</C> prints, with messages in and out per
						second, its scope (name, label, state, mailbox) sent to the node and its search and order
						kept in the page; Supervision draws each process under the one that started it; Events
						streams what <C>watch</C> prints; Cron and Elections find every cron job and leader
						election reachable from here.
					</p>
					<Screenshot
						caption="grpcprocctl web: the cluster"
						name="grpcprocctl-web-cluster"
						alt="The Cluster view: three nodes a, b and c, each with its process count, joined by arrows labelled with messages per second, above a table of the nodes with their uptime, processes, spawns and exits per second, dead letters and peers."
					/>
					<p>
						Clicking a process opens it beside the page: its mailbox and messages per second
						charted, what it says about itself, and what it started. Asking what it says takes the
						process a turn, so the page asks when told to, or again on an interval of your choosing.
						Here <C>ledger</C> handles about 20 messages a second while more arrive, and its mailbox
						climbs:
					</p>
					<Screenshot
						caption="grpcprocctl web: a backlog"
						name="grpcprocctl-web-processes"
						alt="The Processes view sorted by mailbox, ledger first with hundreds of messages waiting, and ledger open beside it: running, its oldest message waiting over twenty seconds, receiving about 19 a second, a chart of its mailbox climbing, and what it publishes about itself."
					/>
					<p>
						The address says what is open, so a view can be shared. The page is read-only unless
						started with <C>--allow-writes</C>, which adds what the commands above change (exiting
						a process, its log level, cron jobs, leader moves and cordons), each confirmed first. It
						listens on localhost unless <C>--listen</C> says otherwise, and then answers only to a
						localhost <C>Host</C>, so a page on another site cannot read it through the browser; on
						any other address, put it behind something that authenticates.
					</p>
				</>
			)
		},
		{
			id: 'json',
			title: 'JSON',
			body: (
				<p>
					<C>--json</C>, before <C>node</C>, <C>nodes</C>, <C>ps</C>, <C>inspect</C>, <C>watch</C>,{' '}
					<C>leader</C> or <C>cron</C>, prints what the table shows as JSON: one indented value, or
					for <C>watch</C> one compact event per line. The objects are those the{' '}
					<A to="guides/mcp/">MCP tools</A> return, which wrap lists in an object of their own, so a
					script and an agent read the same fields.
				</p>
			)
		},
		{
			id: 'reference',
			title: 'Commands',
			body: (
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
						[<C>leader [status] &lt;cluster&gt;</C>, 'A leader election, as each node that takes part sees it.'],
						[<C>leader move &lt;cluster&gt;</C>, <>Hand leadership over; <C>--to</C> a node.</>],
						[
							<C>leader cordon|uncordon &lt;cluster&gt; &lt;node&gt;</C>,
							'Keep a node from leading, to work on its host, or let it lead again.'
						],
						[<C>cron [list] [&lt;pid|name&gt;]</C>, <>Cron processes and their jobs: <C>--node</C>.</>],
						[
							<C>cron enable|disable|remove &lt;pid|name&gt; &lt;job&gt;</C>,
							<>
								Change a job of a cron process: <C>--node</C>.
							</>
						],
						[
							<C>dot</C>,
							<>
								Graphviz of processes and who started whom: <C>--node</C>, <C>--cluster</C>.
							</>
						],
						[<C>mcp</C>, <>Serve these as MCP tools over stdio: <C>--allow-writes</C>.</>],
						[
							<C>web</C>,
							<>
								Serve a page that shows the cluster live: <C>--listen</C>, <C>--allow-writes</C>.
							</>
						]
					]}
				/>
			)
		}
	]
};
