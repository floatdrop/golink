import platform from '../../../examples/guide/internal/platform/platform.go?raw';

import { Code, region } from '../code.tsx';
import { A, C, Table } from '../components/prose.tsx';
import type { Doc } from './types.ts';

export const conceptsDiscovery: Doc = {
	path: 'concepts/discovery/',
	title: 'Discovery and membership',
	description:
		'How a node finds its peers, how it announces itself, and how the cluster decides that a node is gone when its links have not noticed.',
	lead: (
		<p>
			A node addresses a peer by name. Three small interfaces turn that name into an address,
			publish this node for others to find, and tell the node who is alive. The core ships the
			smallest implementation of the first; <A to="guides/etcd/">grpcproc/etcd</A> implements all
			three on etcd leases.
		</p>
	),
	sections: [
		{
			id: 'interfaces',
			title: 'Three interfaces',
			body: (
				<>
					<Code>{`type Resolver interface {
	Resolve(ctx context.Context, node string) (addr string, err error)
}

type Member struct {
	Name        string
	Incarnation uint64
	Addr        string // where peers dial it: its Config.Advertise
}

type Registrar interface {
	Register(ctx context.Context, self Member) (withdraw func(context.Context) error, err error)
}

type Membership interface {
	Watch(ctx context.Context) (<-chan MemberEvent, error)
}

type MemberEvent struct {
	Member Member
	Up     bool
}`}</Code>
					<Table
						head={['Interface', 'What it answers']}
						rows={[
							[<C>Resolver</C>, 'Where does a peer with this name listen? Called before a dial, bounded by Config.DialTimeout.'],
							[<C>Registrar</C>, 'Publishes this node on Start, keeps it published, and withdraws it on Stop.'],
							[<C>Membership</C>, 'Who is alive? A stream of members joining and leaving, from Start until Stop.']
						]}
					/>
					<p>
						<C>Resolver</C> is required; the other two are optional. A node with only a resolver
						works: it dials peers as it needs them and notices a lost one through its links. The
						other two are for clusters whose nodes come and go, and for the failure the links
						cannot see.
					</p>
				</>
			)
		},
		{
			id: 'resolver',
			title: 'Resolving a name',
			body: (
				<>
					<p>
						<C>StaticResolver</C> is a map from node name to address, and <C>ResolverFunc</C> adapts
						a function. Either is enough for a fixed set of programs whose addresses are in their
						configuration. The tutorial's platform builds one from what the program is told:
					</p>
					<Code caption="examples/guide/internal/platform/platform.go">
						{region(platform, /^\/\/ peers resolves the other nodes/, /^}/)}
					</Code>
					<p>
						<C>Resolve</C> runs once per dial, not per message: a node keeps its stream to a peer
						open, and asks again only when it has to dial again. It must return once its ctx is
						done, since that ctx is what bounds a dial. An address a resolver returns is whatever
						the node's <C>DialOptions</C> can dial: a host and port, or a target a gRPC name
						resolver understands.
					</p>
				</>
			)
		},
		{
			id: 'registrar',
			title: 'Announcing a node',
			body: (
				<>
					<p>
						<C>Config.Advertise</C> is the address peers dial to reach this node's gRPC server. The
						core does not use it to listen, since the server is the application's; it reports it
						in <C>NodeInfo</C> and in the <C>Hello</C> that opens a stream, and hands it to the{' '}
						<C>Registrar</C> as the member's <C>Addr</C>. Peers then resolve this node to it.
					</p>
					<p>
						<C>Start</C> registers after it has begun watching membership, so that no event is
						missed between the two, and <C>Stop</C> withdraws last, after the node's processes have
						exited and their <C>Down</C> notices have reached its peers. The order matters: peers
						that saw the node leave the cluster before they saw its processes exit would report{' '}
						<C>noconnection</C> where the truth is <C>shutdown</C>. An error from <C>Register</C>{' '}
						must leave the node unpublished, because <C>Start</C> may call it again.
					</p>
				</>
			)
		},
		{
			id: 'membership',
			title: 'Why membership, when links notice a peer down',
			body: (
				<>
					<p>
						A node notices a lost link by itself, as fast as gRPC keepalive allows, and that is the
						faster signal. It is also a local one with a blind spot: a peer that dies without
						closing its connections is noticed only when keepalive gives up, or never if keepalive
						is not configured. And a link breaking says nothing about whether the peer is gone or
						only unreachable from here.
					</p>
					<p>
						<C>Membership</C> is the cluster-wide verdict on top of that. With etcd, it is the
						peer's lease expiring: every node watching the cluster learns of it at once, whatever
						its own link was doing. Erlang has the same split, between the <C>net_kernel</C> tick
						that guards one connection and an external registry that decides who is in.
					</p>
					<p>When the node's watch reports a change, the node acts on it:</p>
					<ul>
						<li>
							a member <em>down</em>, for the incarnation the node has streams to, drops those
							streams: monitors across them fire <C>Down{'{'}noconnection{'}'}</C>, links end their
							processes, and pending calls fail with <C>ErrNoConnection</C> and the cause, "left the
							cluster";
						</li>
						<li>
							a member <em>up</em> as a newer incarnation than the one the node has streams to drops
							them the same way, with the cause "restarted as incarnation N", and the next send dials
							the new instance;
						</li>
						<li>
							a member up as an older incarnation than the node has seen is ignored, as a stream
							from it would be refused; see{' '}
							<A to="concepts/nodes/#incarnations">incarnations</A>;
						</li>
						<li>
							a member up, unless it is an older incarnation than the node has seen, ends any dial
							backoff for that peer: it is worth dialing again now.
						</li>
					</ul>
					<p>
						A down with incarnation zero means whichever incarnation it was; an up with zero, one
						not known. The node forgets the newest incarnation it saw of a peer when membership
						reports that one gone, which is what lets an instance with an older incarnation, or a
						clock behind the last one's, join again.
					</p>
				</>
			)
		},
		{
			id: 'etcd',
			title: 'On etcd',
			body: (
				<>
					<p>
						<A to="guides/etcd/">grpcproc/etcd</A> implements the three on one <C>Cluster</C> value:
						a key per node under a lease the node keeps alive, resolved by reading the key, and
						membership by listing the keys and then watching the prefix. It is a separate module,
						and a deployment that wants another registry implements the same three interfaces. The
						services on the node do not change either way: the tutorial's{' '}
						<A to="shop/platform/">platform</A> takes its resolver, registrar and membership from
						the configuration, and the services see only the node.
					</p>
				</>
			)
		}
	]
};
