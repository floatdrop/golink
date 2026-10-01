import platform from '../../../examples/guide/internal/platform/platform.go?raw';

import { Code, region } from '../code.tsx';
import { A, C, Ext, Table } from '../components/prose.tsx';
import { PKG_DOC } from '../config.ts';
import type { Doc } from './types.ts';

export const guidesConfiguration: Doc = {
	path: 'guides/configuration/',
	title: 'Configuring a node',
	description:
		'Every field of grpcproc.Config, what it defaults to, and the order a node is built, registered, started and stopped in.',
	lead: (
		<p>
			A node is configured once, in <C>NewNode</C>, which fails on a bad configuration rather than
			later. Two fields are required, <C>Name</C> and <C>Resolver</C>; the rest have defaults that
			suit a first program and need thought for a deployment. The{' '}
			<Ext href={`${PKG_DOC}#Config`}>API reference</Ext> has each field's full comment.
		</p>
	),
	sections: [
		{
			id: 'summary',
			title: 'At a glance',
			body: (
				<Table
					head={['Field', 'Default, and what it is']}
					rows={[
						[<C>Name</C>, 'Required. The node\'s name, which peers address it by.'],
						[<C>Resolver</C>, 'Required. Turns a peer\'s name into an address to dial.'],
						[<C>Advertise</C>, 'None. Where peers dial this node; published by the Registrar.'],
						[<C>Incarnation</C>, 'The start time in nanoseconds. Must grow with each start.'],
						[<C>Registrar</C>, 'None. Publishes the node on Start, withdraws it on Stop.'],
						[<C>Membership</C>, 'None. The cluster\'s view of who is alive, watched from Start.'],
						[<C>DialOptions</C>, 'None. Credentials, keepalive and interceptors for every outbound connection.'],
						[<C>DialOptionsFor</C>, 'None. More options for the connections to one peer, after DialOptions, so they win.'],
						[<C>DialTimeout</C>, '5s. Bounds a dial: resolve, connect, handshake.'],
						[<C>DialBackoff</C>, '5s. The longest wait before dialing a peer again after failed dials; negative dials again at once.'],
						[<C>MaxQueued</C>, 'None. How many envelopes the link to a peer may hold before sends and calls to it fail at once.'],
						[<C>MaxQueuedBytes</C>, 'None. The same bound, in bytes of message bodies.'],
						[<C>Admit</C>, 'None. Runs for every inbound link: refuses it, or admits it with a Policy of what the peer may ask.'],
						[<C>Logger</C>, 'slog.Default().'],
						[<C>Hooks</C>, 'None. The observability tap.'],
						[<C>CopyLocal</C>, 'Off. Clone every locally delivered message.']
					]}
				/>
			)
		},
		{
			id: 'identity',
			title: 'Identity',
			body: (
				<>
					<p>
						<C>Name</C> is how peers address the node: a <C>Named</C> address is a node name and a
						process name, and a PID carries the node name too. It is unique per cluster. Two
						instances given one name are two incarnations of one node, and the newer takes the
						older's place.
					</p>
					<p>
						<C>Advertise</C> is the address peers dial to reach this node's gRPC server. The core
						only reports it, in <C>NodeInfo</C> and in the <C>Hello</C> that opens a link; a{' '}
						<C>Registrar</C> publishes it, and then it must be the address other hosts see, not a
						wildcard or loopback.
					</p>
					<p>
						<C>Incarnation</C> distinguishes this start of the node from earlier ones, and must
						grow with each start: a peer that has seen an incarnation refuses links from older
						ones, so that an instance that was replaced, and still runs, cannot take the links of
						its replacement. Zero picks the current Unix time in nanoseconds, which grows as long as
						the clocks of the hosts the node starts on agree to within the time between two starts.
						A random or hashed value would be refused whenever it came out lower than the last;
						a counter from a deployment system, or an etcd revision, works.{' '}
						<A to="concepts/nodes/#incarnations">Nodes and the cluster</A> has why.
					</p>
				</>
			)
		},
		{
			id: 'peers',
			title: 'Peers',
			body: (
				<>
					<p>
						<C>Resolver</C> maps a peer's name to an address; <C>StaticResolver</C> is a map, and{' '}
						<C>ResolverFunc</C> adapts a function. It is called before a dial, with a ctx bounded by{' '}
						<C>DialTimeout</C>, and must return once that ctx is done.
					</p>
					<p>
						<C>Registrar</C> and <C>Membership</C> are optional and usually come together, from{' '}
						<A to="guides/etcd/">grpcproc/etcd</A>. With <C>Membership</C> set, a peer that leaves
						the cluster, or comes back as a newer incarnation, has its links dropped, which fires{' '}
						<C>Down{'{'}noconnection{'}'}</C> for monitors across them and fails pending calls, even
						when its connection never closed. <A to="concepts/discovery/">Discovery and membership</A>{' '}
						explains the three together.
					</p>
				</>
			)
		},
		{
			id: 'connections',
			title: 'Connections',
			body: (
				<>
					<p>
						<C>DialOptions</C> are used for every outbound connection: the transport credentials,
						keepalive, and any interceptors. Keepalive is what turns a peer that went silent into a
						broken link, and so into <C>Down</C>s and failed calls; grpcproc does not set it, because
						the server it must match is the application's. The tutorial's platform sets both sides:
					</p>
					<Code caption="examples/guide/internal/platform/platform.go">
						{region(platform, /^\/\/ dialOptions are for every connection/, /^}/)}
					</Code>
					<Code caption="examples/guide/internal/platform/platform.go">
						{region(platform, /^func newServer/, /^}/)}
					</Code>
					<p>
						The client pings every ten seconds and gives up five seconds after an unanswered ping;
						the server's enforcement policy has to allow pings that often, and without an active
						stream, or it closes the connection for pinging too much. A partition is then a link
						error within fifteen seconds, and the calls waiting on it fail rather than hang.
					</p>
					<p>
						<C>DialTimeout</C> bounds one dial, five seconds by default, as long as the resolver and
						the interceptors honour their ctx. <C>DialBackoff</C> is the longest the node waits
						before dialing a peer again after dials to it failed; meanwhile everything routed to
						the peer fails at once with <C>ErrNoConnection</C>, and a <C>Monitor</C> of a process
						there gets <C>Down{'{'}noconnection{'}'}</C> at once. The first wait is a 32nd of it,
						and each failure doubles it. Set it negative to dial again at once, which is what{' '}
						<C>grpcproctest</C> does so that a test's call right after a heal reaches the peer.
					</p>
					<p>
						<C>MaxQueued</C> and <C>MaxQueuedBytes</C> bound the link to each peer, which is
						unbounded by default. While a link holds that many envelopes not yet written, or that
						many bytes of message bodies, a send or a call to its peer fails at once with a{' '}
						<C>*LinkError</C> whose <C>Err</C> is <C>ErrLinkBusy</C> and whose <C>Unsent</C> is set:
						the peer cannot keep up, and the message is still on this node, for the sender to drop
						or send again later. Only that peer's senders are refused. Replies, <C>Down</C>s,
						monitors and exits are queued regardless. <C>LinkInfo.Queued</C> and{' '}
						<C>QueuedBytes</C> show how close each link is to its bound.
					</p>
				</>
			)
		},
		{
			id: 'security',
			title: 'Security',
			body: (
				<>
					<p>
						A link is a gRPC stream, so it is secured the way the server's other services are: TLS
						or mutual TLS in the server's credentials, and the matching{' '}
						<C>grpc.WithTransportCredentials</C> in <C>DialOptions</C>. The node's identity travels
						in the stream's metadata: its name, incarnation and protocol version. Nothing checks by
						itself that the certificate a peer presents belongs to the name it claims; that is what{' '}
						<C>Admit</C> is for. It runs for every inbound link, with the peer's transport
						credentials in the ctx, before the link is accepted:
					</p>
					<Code>{`Admit: func(ctx context.Context, peer grpcproc.NodeID) (grpcproc.Policy, error) {
	p, ok := grpcpeer.FromContext(ctx) // google.golang.org/grpc/peer
	if !ok {
		return nil, errors.New("no peer")
	}
	tls, ok := p.AuthInfo.(credentials.TLSInfo)
	if !ok || len(tls.State.PeerCertificates) == 0 {
		return nil, errors.New("no client certificate")
	}
	if cn := tls.State.PeerCertificates[0].Subject.CommonName; cn != peer.Name {
		return nil, fmt.Errorf("certificate %q does not match node %q", cn, peer.Name)
	}
	return nil, nil // a nil Policy: the peer may ask anything
},`}</Code>
					<p>
						A refused peer gets <C>PermissionDenied</C>, its dial fails, and it backs off like any
						other failed dial.
					</p>
					<p>
						An admitted peer may ask anything of any process, <C>Exit</C> included, which a process
						cannot trap. That suits the nodes of one installation. A node of another one, a
						partner's or a tenant's, gets a <C>Policy</C> instead: it judges every message, call,
						monitor and exit the peer sends over the link, by the name of the process it is for.{' '}
						<C>grpcproc.Export</C> is the usual one: the names listed, by name or PID, for sends,
						calls and monitors, and no exits. What a policy refuses does not exist for the peer: a
						call fails with <C>ErrNoProc</C>, a monitor gets <C>Down{'{'}noproc{'}'}</C>, and this
						node counts a dead letter with reason <C>denied</C>, which <C>grpcprocctl watch</C>{' '}
						shows.
					</p>
					<Code>{`Admit: func(ctx context.Context, peer grpcproc.NodeID) (grpcproc.Policy, error) {
	if err := checkCertificate(ctx, peer); err != nil {
		return nil, err
	}
	if strings.HasPrefix(peer.Name, "partner-") {
		return grpcproc.Export("quotes", "orders"), nil
	}
	return nil, nil
},
DialOptionsFor: func(peer string) []grpc.DialOption {
	if strings.HasPrefix(peer, "partner-") { // another CA, another client certificate
		return []grpc.DialOption{grpc.WithTransportCredentials(partnerCreds)}
	}
	return nil
},`}</Code>
					<p>
						<C>DialOptionsFor</C> gives the connections to one peer options of their own, after{' '}
						<C>DialOptions</C>, so they win: the credentials for another installation's CA, here.
						Replies and <C>Down</C>s always pass a policy, since they answer what this node asked, so
						a process here can call and monitor the partner's processes whatever the partner may
						reach here. The Inspector, if the node serves one, is a second gRPC service on
						the same server and takes the same interceptors; <C>inspect.ReadOnly()</C> refuses its
						writes outright, for a deployment that shares the node's port without authenticating.{' '}
						<A to="guides/inspector/">The Inspector</A> has the rest.
					</p>
				</>
			)
		},
		{
			id: 'observability',
			title: 'Logging and hooks',
			body: (
				<>
					<p>
						<C>Logger</C> is the <C>slog.Logger</C> the node and its processes log through;{' '}
						<C>p.Log()</C> is it with the process's PID and label attached, and each process's
						level can be raised or lowered at runtime. <C>Hooks</C> is the one synchronous tap for
						everything the node does: spawns, exits, sends, receives, dead letters, links up and
						down. <C>grpcproc/otel</C> implements it with OpenTelemetry, and <C>JoinHooks</C>{' '}
						combines several. <A to="guides/observability/">Observability</A> covers both.
					</p>
					<p>
						<C>CopyLocal</C> clones every locally delivered message, so a sender can keep mutating
						what it sent. It is off by default: a local send shares the pointer, and the rule is not
						to touch a message after sending it, which is also the rule the wire imposes for free.
					</p>
				</>
			)
		},
		{
			id: 'lifecycle',
			title: 'The lifecycle',
			body: (
				<>
					<p>The order is fixed by what depends on what:</p>
					<ul>
						<li>
							<C>NewNode</C> checks the configuration and builds the node. A missing name or resolver
							fails here, and nothing runs yet.
						</li>
						<li>
							<C>Register</C> mounts <C>grpcproc.v1.Node</C> on the gRPC server, and must come
							before <C>Serve</C>, as any service registration does. Processes can be spawned from
							here on; they run before the node starts.
						</li>
						<li>
							<C>Start</C> publishes the node: it begins watching <C>Membership</C>, then registers
							with the <C>Registrar</C>. Call it once the server serves, so that a peer that learns
							of the node can dial it.
						</li>
						<li>
							<C>Stop</C> tells every process to exit with <C>shutdown</C>, waits for them, flushes
							and closes the links, and withdraws the node; its ctx bounds the wait. Call it before
							the server stops, since the links are streams on that server.
						</li>
					</ul>
					<p>
						With a dependency-injection container, that order falls out of the dependencies: the
						tutorial's platform builds the server, then the node on it, then the Inspector, then
						the root supervisor, and stops them in reverse.
					</p>
					<Code caption="examples/guide/internal/platform/platform.go">
						{region(platform, /^\/\/ Module registers the node and what it stands on/, /^}/)}
					</Code>
					<p>
						The root starts the node itself, once the services' trees run, so that with a registry
						the node is published only when its processes exist; see{' '}
						<A to="shop/platform/">The platform</A>.
					</p>
				</>
			)
		}
	]
};
