import { Code } from '../code.tsx';
import { Drawing } from '../components/Figure.tsx';
import { Links } from '../components/diagrams-nodes.tsx';
import { A, Aside, C, Ext } from '../components/prose.tsx';
import { file } from '../config.ts';
import type { Doc } from './types.ts';

export const conceptsNodes: Doc = {
	path: 'concepts/nodes/',
	title: 'Nodes and the cluster',
	description:
		'What a node is, how two nodes are linked, what travels between them and in what order, and what happens when a peer cannot be reached.',
	lead: (
		<p>
			A process lives on a node, and a node lives in a program. Everything on the other pages
			about processes holds within one node and across nodes alike; this page is about the
			nodes themselves: how they find each other, what the link between two of them carries,
			and what a node concludes when the link breaks.
		</p>
	),
	sections: [
		{
			id: 'node',
			title: 'A node is grpcproc in one program',
			body: (
				<>
					<p>
						A node hosts processes and links to the nodes of other programs. It is a value the
						application constructs, registers on the gRPC server it already runs, starts and
						stops; grpcproc opens no listener of its own and starts no goroutine outside{' '}
						<C>Start</C> and <C>Stop</C>. One program normally runs one node.
					</p>
					<Code>{`node, err := grpcproc.NewNode(grpcproc.Config{
	Name:        "warehouse",
	Resolver:    grpcproc.StaticResolver{"shop": "10.0.0.7:9000"},
	DialOptions: []grpc.DialOption{grpc.WithTransportCredentials(creds)},
})
node.Register(grpcServer) // grpcproc.v1.Node, next to your services
err = node.Start(ctx)     // once the server serves
defer node.Stop(ctx)      // before the server stops`}</Code>
					<p>
						A node has a <C>Name</C>, which is how peers address it, and an <em>incarnation</em>,
						a number for this start of it: by default the time it started, in nanoseconds. The two
						together are its <C>NodeID</C>, and every PID on the node carries both. A node that
						restarts has the same name and a new incarnation, so a PID from before the restart
						names a process that no longer exists, and gets <C>noproc</C> rather than a message
						delivered to a stranger. <A to="concepts/addressing/">Addressing</A> has the rest.
					</p>
				</>
			)
		},
		{
			id: 'links',
			title: 'A stream each way',
			body: (
				<>
					<p>
						Two nodes are linked by two gRPC streams, one in each direction. A node opens its
						stream to a peer when it first has something to send there: a call to the peer's{' '}
						<C>grpcproc.v1.Node</C> service, held open for as long as both run. The peer opens its
						own stream back when it first has something to send this way, a reply included. Two
						streams, rather than one shared, means neither side has to win a race to dial first.
					</p>
					<Drawing caption="two nodes, a stream each way">
						<Links />
					</Drawing>
					<p>
						Everything from one node to another travels on that one stream, in order: sends,
						calls and their replies, monitors and demonitors, the <C>Down</C> a monitor produces,
						exit requests, and the <C>Hello</C> that opens the stream with the node's identity.
						A call is an envelope on the same stream as sends, matched to its reply by an id,
						never a separate RPC, so a call cannot overtake a message sent before it by the same
						process, or be overtaken by one sent after.
					</p>
					<p>
						That single ordered stream is what gives the guarantee the rest of grpcproc rests on:
						a process's last message reaches a watcher before the <C>Down</C> that says the process
						is gone. The writer sends everything queued since its last write as one frame, split
						at about a megabyte, so under load many envelopes share the cost of one gRPC message,
						and at low load a frame holds one envelope and nothing waits.
					</p>
				</>
			)
		},
		{
			id: 'wire',
			title: 'What travels',
			body: (
				<>
					<p>
						An envelope is one interaction between two processes. It is a flat message, so that
						decoding one is cheap, and it carries no node names: every envelope on a stream goes
						from a process of the node that opened it to a process of the node that accepted it,
						so a PID travels as an incarnation and an id, and the reader fills the node names in
						from the stream.
					</p>
					<Code lang="proto">{`message Envelope {
  Kind kind = 1;                      // send, call, reply, monitor, demonitor, down, exit, hello
  uint64 from_incarnation = 2;        // the process on the sending node; id 0 is the node itself
  uint64 from_id = 3;
  uint64 to_incarnation = 4;          // the process on the receiving node,
  uint64 to_id = 5;                   // unless to_name addresses it by name
  string to_name = 6;
  uint64 ref = 7;                     // a call's id, echoed by its reply; a monitor's, by its down
  Status status = 8;                  // a reply's status
  string reason = 9;                  // a reply's error, or the reason of a down or an exit
  string body_type = 10;              // the body's message type, by full name
  bytes body = 11;                    // and its encoding
  Hello hello = 12;
  int64 timeout_nanos = 13;           // a call's time left, so that clocks need not agree
  map<string, string> metadata = 15;  // trace context, tenant: never read by grpcproc
}`}</Code>
					<p>
						A body is a protobuf message, sent as its type's full name and its encoding. Generated
						types register themselves with the protobuf registry, so nothing has to be registered
						with grpcproc, and the receiving node checks the decoded type against the mailbox's
						on delivery. The metadata map is carried untouched with every message; it is where a
						tracer puts its context, as <A to="guides/observability/">Observability</A> shows.
					</p>
				</>
			)
		},
		{
			id: 'down',
			title: 'When a peer is down',
			body: (
				<>
					<p>
						The two directions are independent streams, so an envelope the peer sent can still be
						in flight on the inbound stream when the outbound one fails. A node declares a peer
						down only once the stream <em>from</em> the peer has ended, when everything the peer
						sent has been dispatched, in order, or when there was no such stream at all. An
						outbound failure alone drops that stream, and the next send dials again.
					</p>
					<p>Once a peer is down:</p>
					<ul>
						<li>
							every monitor that crossed the link fires a <C>Down</C> with reason{' '}
							<C>noconnection</C>, and every link ends its process with the same reason;
						</li>
						<li>
							every call waiting on the peer fails with <C>ErrNoConnection</C>. The peer may have
							handled it: the request left this node, and only the reply is lost;
						</li>
						<li>
							calls still queued on the broken stream, never written, fail too, and their
							messages become dead letters.
						</li>
					</ul>
					<p>
						Every link failure is a <C>*LinkError</C>, which <C>errors.Is</C> matches to{' '}
						<C>ErrNoConnection</C>. Its <C>Unsent</C> field is the one thing that makes a retry safe:
						it says the message never left this node, because the peer could not be reached or dials
						to it are backed off, so sending it again cannot deliver it twice. A <C>LinkError</C>{' '}
						without <C>Unsent</C> claims nothing, and the message may have been handled.
					</p>
					<p>
						A reply or a <C>Down</C> that cannot be routed back to a peer cuts that peer's stream to
						this node, with a status saying so. The peer waits for those on its own stream, which
						would stay up while this node cannot reach it back, and it would otherwise never learn
						that a monitor of its will not fire. Ending its stream makes it see this node as
						unreachable, as Erlang's single connection would.
					</p>
				</>
			)
		},
		{
			id: 'keepalive',
			title: 'Keepalive, timeouts and backoff',
			body: (
				<>
					<p>
						A peer that dies without closing its connections, behind a half-open TCP connection or
						a partition, sends nothing and closes nothing. What turns that silence into a stream
						error is gRPC keepalive, on both sides: <C>keepalive.ClientParameters</C> in the node's{' '}
						<C>DialOptions</C>, and <C>keepalive.ServerParameters</C> with an enforcement policy on
						the server. grpcproc does not set it, since the server is the application's; set it,
						or a silent partition is noticed only by <A to="concepts/discovery/">Membership</A>, or
						never. <A to="guides/configuration/">Configuring a node</A> has the values the tutorial
						uses.
					</p>
					<p>
						A dial is bounded by <C>DialTimeout</C>: resolving the peer, connecting, and the
						handshake, five seconds by default. After a dial fails, everything routed to that peer
						fails at once with <C>ErrNoConnection</C> for a while, rather than each send waiting out
						a dial of its own; a process sending to a dead node would otherwise stall for the whole
						timeout per message. The wait starts at a 32nd of <C>DialBackoff</C> and doubles up to
						it, with jitter. Then one send dials again while the others keep failing, so a hung peer
						holds one sender at a time: a half-open circuit breaker. <C>Membership</C> reporting the
						peer up ends the wait, and <C>LinkInfo</C> shows the peer as a down outbound link with
						its <C>RetryAt</C> and <C>LastError</C>.
					</p>
				</>
			)
		},
		{
			id: 'incarnations',
			title: 'Incarnations fence the links',
			body: (
				<>
					<p>
						Two instances can claim one node name at once: an instance that was replaced but still
						runs, after a partition heals, or two deploys given the same name. If a stream from
						either replaced the other's, each would take its peers' links down for the other, and
						the two would knock each other off for as long as both ran.
					</p>
					<p>
						So a node remembers, per peer name, the newest incarnation it has seen, over a stream in
						either direction or from <C>Membership</C>, and refuses an older one: an inbound stream
						before its <C>Hello</C>, so the old instance's dial fails and backs off, and a dial that
						reaches one through an address that still points at it. A newer incarnation's streams
						replace the older one's, whose monitors fire. The node forgets the newest when{' '}
						<C>Membership</C> reports it gone, or on <C>Disconnect</C>, and then lets an older one
						in.
					</p>
					<p>
						The rule needs incarnations that grow with each start. The default, the start time,
						does as long as the hosts' clocks agree to within the time between two starts of a
						node; a random incarnation would be refused whenever it came out lower than the last.
						The fence covers grpcproc traffic only: an old instance can still write to a
						database, which needs fencing of its own.
					</p>
				</>
			)
		},
		{
			id: 'stop',
			title: 'A graceful stop',
			body: (
				<>
					<p>
						<C>Stop</C> asks every process on the node to exit with reason <C>shutdown</C> and waits
						for them; the ctx bounds the wait. Their <C>Down</C> envelopes are queued on the
						outbound streams, every stream flushes and half-closes at once, and each is waited on
						until its peer ends the stream or the ctx ends. Only then are the connections closed, and
						a <C>Registrar</C> told to withdraw the node. So peers see <C>Down{'{'}shutdown{'}'}</C>,
						not <C>noconnection</C>, for the processes a stopping node ran, and a peer that never
						ends its stream holds its own link to the deadline, not the others.
					</p>
					<p>
						A call still waiting when its node stops fails with <C>ErrNodeStopped</C>; as with a
						broken link, the peer may have handled it.
					</p>
				</>
			)
		},
		{
			id: 'local',
			title: 'Local and remote',
			body: (
				<>
					<p>
						Nothing in a send says whether the target is local. A local send appends the message
						to the mailbox and passes the pointer as it is: no encoding, no copy, and no
						allocation when the mailbox keeps up. The rule that comes with it is not to mutate a
						message after sending it. <C>Config.CopyLocal</C> clones every locally delivered
						message instead, for a codebase that wants the isolation the wire gives for free.
					</p>
					<p>
						A remote send encodes the body, queues the envelope on the stream to the peer's node,
						and returns; a first send to a peer waits for the dial, which is what the ctx of{' '}
						<C>Node.Send</C> bounds. Mailboxes are unbounded on purpose: a full mailbox in one
						process would otherwise stall the shared stream for every other process behind it.
						Backpressure is the application's, and the mailbox depth and each link's queue are
						visible so it can be built.
					</p>
					<Aside title="Coming from Erlang">
						<p>
							A link between two nodes is not the one TCP connection of the Erlang distribution: it
							is two gRPC streams on the application's server, one opened by each side, so a
							one-way partition shows as one dead direction. The <C>net_kernel</C> tick has two
							counterparts: gRPC keepalive for the fast, local signal, and <C>Membership</C> for the
							cluster-wide verdict. Node names are per cluster and process names are per node; there
							is no <C>global</C> registry yet. The{' '}
							<Ext href={file('docs/DESIGN.md')}>design notes</Ext> compare the rest.
						</p>
					</Aside>
				</>
			)
		}
	]
};
