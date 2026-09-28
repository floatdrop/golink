import { Code } from '../code.tsx';
import { A, C, Ext, Table } from '../components/prose.tsx';
import { file } from '../config.ts';
import type { Doc } from './types.ts';

export const guidesEtcd: Doc = {
	path: 'guides/etcd/',
	title: 'Running on etcd',
	description:
		'Cluster membership on etcd leases: nodes register under a lease, peers resolve them from it, and an expired lease drops the links to a node that stopped answering.',
	lead: (
		<p>
			<C>grpcproc/etcd</C> implements <C>Resolver</C>, <C>Registrar</C> and <C>Membership</C> on
			one etcd client. A node registers under a lease it keeps alive; peers resolve its address
			from the key; and when the lease ends, because the node stopped or stopped answering, every
			node watching the cluster drops its links to it. It is a separate module, so grpcproc itself
			does not depend on the etcd client. <A to="concepts/discovery/">Discovery and membership</A>{' '}
			says what the three interfaces are for.
		</p>
	),
	sections: [
		{
			id: 'install',
			title: 'Install and wire',
			body: (
				<>
					<Code lang="sh">{'go get github.com/floatdrop/grpcproc/etcd'}</Code>
					<Code>{`import grpcprocetcd "github.com/floatdrop/grpcproc/etcd"

cluster := grpcprocetcd.New(etcdClient, "/grpcproc/prod")
node, err := grpcproc.NewNode(grpcproc.Config{
	Name:       "orders-1",
	Advertise:  "10.0.0.5:9000", // this node's gRPC server, as peers reach it
	Resolver:   cluster,
	Registrar:  cluster,
	Membership: cluster,
})
err = node.Start(ctx) // registers; Stop withdraws`}</Code>
					<p>
						<C>New</C> takes a <C>*clientv3.Client</C> and a prefix; every key the cluster writes is
						under it, so one etcd serves several clusters under different prefixes. <C>Advertise</C>{' '}
						is required here where the core does not need it: it is what the node is registered
						with, and what peers dial. Give it the address the server listens on as other hosts see
						it, not <C>0.0.0.0</C> or <C>localhost</C>.
					</p>
					<p>
						One <C>Cluster</C> value serves every node in a program, and whatever reaches nodes
						through the node, <C>node.Dial</C>: the Inspector forwards to the others through the
						same registry with no option of its own; see <A to="guides/inspector/">The Inspector</A>.
						A <C>leader</C> elector with no <C>Membership</C> of its own follows the node's.
					</p>
				</>
			)
		},
		{
			id: 'options',
			title: 'Options',
			body: (
				<>
					<Table
						head={['Option', 'What it sets']}
						rows={[
							[
								<C>WithTTL(d)</C>,
								'The lease TTL: how long a node that stops answering stays registered. Rounded up to whole seconds. Default 10s.'
							],
							[
								<C>WithRetry(d)</C>,
								'The pause before registering again after a lost lease, and before watching again after a broken watch. Default 1s.'
							],
							[<C>WithLogger(l)</C>, 'The logger. Default slog.Default().']
						]}
					/>
					<p>
						The TTL is the detection time for a node that died silently: its peers learn of it once
						the lease expires, so a short TTL means a faster verdict and more keepalive traffic to
						etcd. A node that loses its lease while it is still running registers again after the
						retry interval, so a TTL shorter than an etcd hiccup makes nodes flap in and out of the
						cluster, and their peers' links with them.
					</p>
				</>
			)
		},
		{
			id: 'roles',
			title: 'What it does in etcd',
			body: (
				<>
					<Table
						head={['grpcproc role', 'In etcd']}
						rows={[
							[
								<C>Registrar</C>,
								<>
									One key per node, <C>{'<prefix>/nodes/<name>'}</C>, holding the name, the incarnation
									and the address, attached to a lease the node keeps alive. If the lease is lost, because
									etcd was unreachable for longer than the TTL, the node registers again every retry
									interval until it succeeds, or until it finds a newer incarnation registered.{' '}
									<C>Stop</C> revokes the lease, which removes the key at once.
								</>
							],
							[<C>Resolver</C>, 'Reads the key, and answers ErrNotRegistered for a node absent from etcd.'],
							[
								<C>Membership</C>,
								'Lists the keys, reporting every node up, then watches the prefix: a put is a member up, a delete a member down, with the incarnation taken from the previous value. If the watch breaks, through a compaction or an etcd restart, it lists again and reports what changed in between.'
							]
						]}
					/>
				</>
			)
		},
		{
			id: 'superseding',
			title: 'A restarted node supersedes itself',
			body: (
				<>
					<p>
						A node that registers a name already present replaces it: a restarted node supersedes
						its previous incarnation, whose lease may not have expired yet. Its peers see the new
						incarnation come up and drop their links to the old one, with the cause "restarted as
						incarnation N", so a fast restart is not mistaken for a node that was never gone.
					</p>
					<p>
						An older incarnation never replaces a newer one's record, which a compare-and-swap
						settles. <C>Register</C> fails with <C>ErrSuperseded</C> while a newer one holds the key,
						and an instance that was replaced, and comes back to etcd after losing its lease, stops
						registering when it finds a newer incarnation registered, rather than take the name
						back. Peers that have seen the newer one refuse the old one's links too, as{' '}
						<A to="concepts/nodes/#incarnations">Nodes and the cluster</A> explains. Incarnations
						must therefore grow with each start: with the default, the start time, the hosts'
						clocks must agree to within the time between two starts of a node.
					</p>
				</>
			)
		},
		{
			id: 'why',
			title: 'Why a lease, and not just the links',
			body: (
				<>
					<p>
						grpcproc notices a lost link by itself, as fast as gRPC keepalive allows. A node that
						dies without closing its connections, behind a half-open TCP connection or a network
						partition, is noticed only when keepalive gives up, or never if keepalive is not
						configured. Its lease ends after the TTL regardless: every watching node then drops its
						links, monitors across them fire <C>Down{'{'}noconnection{'}'}</C>, and pending calls
						fail with "left the cluster". The lease is the verdict the whole cluster shares; a
						broken link is one node's view.
					</p>
					<p>
						Both signals are worth having. Keepalive is faster and needs no round trip to etcd; set
						it in the node's <C>DialOptions</C> and on the server, as{' '}
						<A to="guides/configuration/#connections">Configuring a node</A> shows. The lease catches
						what keepalive misses, and it is what removes a dead node from the addresses peers
						resolve.
					</p>
				</>
			)
		},
		{
			id: 'operations',
			title: 'In operation',
			body: (
				<>
					<ul>
						<li>
							<strong>Detection time is the TTL.</strong> A node that dies silently is declared gone
							when its lease expires; a node that stops cleanly is gone at once, since <C>Stop</C>{' '}
							revokes the lease, after its processes' <C>Down{'{'}shutdown{'}'}</C> notices have
							reached its peers.
						</li>
						<li>
							<strong>etcd down is not the cluster down.</strong> Links stay up while etcd is
							unreachable; only registration and the watch pause, and both resume after the retry
							interval. A lease may expire meanwhile, in which case the node registers again when it
							can, and its peers see it leave and come back.
						</li>
						<li>
							<strong>Clocks.</strong> The default incarnation is the start time, and a node whose
							clock is behind the last instance's cannot supersede it. Hosts that run the same node
							name need clocks that agree to within a restart, or a <C>Config.Incarnation</C> taken
							from something that grows: a deployment counter, an etcd revision.
						</li>
						<li>
							<strong>Names.</strong> A node name is a key under the prefix, so it is unique per
							cluster. Two instances started with the same name are two incarnations of one node,
							and the newer wins.
						</li>
					</ul>
					<p>
						The module's tests run against an embedded etcd, with no server to install;{' '}
						<Ext href={file('etcd/etcd_test.go')}>etcd_test.go</Ext> shows the set-up.
					</p>
				</>
			)
		}
	]
};
