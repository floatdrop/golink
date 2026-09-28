import example from '../../../examples/pubsub/main.go?raw';
import exampleOutput from '../../../examples/pubsub/output.txt?raw';

import { Code, Output, region } from '../code.tsx';
import { A, Aside, C, Ext, Table } from '../components/prose.tsx';
import { file } from '../config.ts';
import type { Doc } from './types.ts';

export const guidesPubsub: Doc = {
	path: 'guides/pubsub/',
	title: 'Pub/sub',
	description:
		'The grpcproc/pubsub package: topics a process publishes to without knowing who listens, subscribed to from any node, with the last few events kept for whoever comes late.',
	lead: (
		<p>
			<C>grpcproc/pubsub</C> is publish/subscribe on top of processes, built on the public API only,
			as <C>grpcproc/actor</C> is. A process publishes what changes to a topic; any process, on any
			node, subscribes to it, and the publisher never learns who they are. The code is from{' '}
			<Ext href={file('examples/pubsub/main.go')}>examples/pubsub</Ext>, whole and runnable.
		</p>
	),
	sections: [
		{
			id: 'topics',
			title: 'A topic is a process',
			body: (
				<>
					<p>
						A topic keeps its subscribers and the last <C>Config.Buffer</C> events, and sends each
						event it is given to every subscriber. It is spawned, named and addressed like any other
						process, in one of two ways:
					</p>
					<Table
						rows={[
							[
								<C>pubsub.SpawnOwned[E](p, cfg, …)</C>,
								<>
									A topic owned by <C>p</C>, linked to it: it ends when <C>p</C> does, with{' '}
									<C>p</C>'s reason. For events that mean something only while their producer runs.
								</>
							],
							[
								<C>pubsub.Spawn[E](node, cfg, …)</C>,
								'A topic for the whole node, whoever publishes to it. It lasts until it is asked to exit or the node stops.'
							]
						]}
					/>
					<p>
						The rest of the arguments are <C>grpcproc.SpawnOption</C>s: a name, a label. The example's
						inventory owns a topic of stock levels and publishes to it after each reservation:
					</p>
					<Code caption="examples/pubsub/main.go">{region(example, /^\/\/ inventory reserves stock/, /^}/)}</Code>
					<p>
						<C>Publish(ctx, from, e)</C> is a send to the topic, from a <C>Node</C> or a{' '}
						<C>Process</C>, as <C>Addr.Send</C> is. The topic sends each event on in the order it
						received them, so one publisher's events arrive in the order it published them, each
						with the metadata it was published with, trace context included.
					</p>
					<p>
						<C>Topic[E]</C> is an address that carries the event type, as <C>Addr[M]</C> carries a
						mailbox's: <C>pubsub.Named[E](node, name)</C> reaches a topic by its name, and{' '}
						<C>pubsub.TopicOf[E](pid)</C> by its PID. <C>topic.Addr()</C> is its process, to monitor,
						link to, or ask to exit. A topic whose <C>E</C> is an interface, <C>proto.Message</C>{' '}
						say, takes any message of it.
					</p>
				</>
			)
		},
		{
			id: 'subscribing',
			title: 'Subscribing',
			body: (
				<>
					<p>
						A process subscribes from inside its handler with <C>topic.Subscribe(ctx, p)</C>. When
						it returns, the events the topic kept are already in <C>p</C>'s mailbox, and everything
						published after them follows: nothing is missed between the two, and nothing arrives
						twice. The events are plain messages, from <C>sub.From</C>, so <C>p</C>'s mailbox must
						accept <C>E</C>; <C>Subscribe</C> fails with <C>ErrType</C> when it does not, or when the
						topic publishes another type.
					</p>
					<Code caption="examples/pubsub/main.go">{region(example, /^\/\/ dashboard subscribes/, /^}/)}</Code>
					<Table
						head={['When', 'What the subscriber sees']}
						rows={[
							[
								'The topic exits',
								<>
									The <C>Down</C> with <C>sub.Ref</C>, with the topic's reason: for an owned topic,
									its owner's.
								</>
							],
							[
								"The topic's node becomes unreachable",
								<>
									The same <C>Down</C>, with <C>noconnection</C>.
								</>
							],
							[
								<C>sub.Cancel(p)</C>,
								'Nothing more from the topic. Events already in the mailbox stay there.'
							],
							['The subscriber exits', 'Nothing to do: the topic monitors its subscribers, and forgets one that exits.']
						]}
					/>
					<p>
						A subscriber that should end with the topic, rather than be told, links to it as well:{' '}
						<C>p.Link(topic.Addr())</C>. A process that subscribes twice still gets each event once. <C>ctx</C> bounds
						the whole of <C>Subscribe</C>; if it ends first, <C>Subscribe</C> tells the topic to
						forget <C>p</C>, in case the request reached it after all.
					</p>
				</>
			)
		},
		{
			id: 'demand',
			title: 'Working only while someone listens',
			body: (
				<>
					<p>
						A producer whose events are costly to make can make them only while someone wants them.{' '}
						<C>Config.Notify</C> names a process the topic tells: a <C>*pubsubv1.Demand</C> with{' '}
						<C>Subscribed</C> true when the first subscriber comes, and false after the last one
						leaves, by <C>Cancel</C> or by exiting.
					</p>
					<Code>{`func quotes(p *grpcproc.Process[proto.Message]) error {
	topic, err := pubsub.SpawnOwned[*pricespb.Quote](p, pubsub.Config{Notify: p.PID()})
	if err != nil {
		return err
	}
	polling := false
	for {
		m, err := p.Receive()
		if err != nil {
			return err
		}
		if d, ok := m.Body.(*pubsubv1.Demand); ok {
			polling = d.GetSubscribed() // ask the exchange for quotes only while someone listens
		}
		…
	}
}`}</Code>
					<p>
						The <C>Demand</C> is a message like any other, so the notified process's mailbox must
						accept it: <C>proto.Message</C> here, or an interface both types implement.
					</p>
				</>
			)
		},
		{
			id: 'nodes',
			title: 'Across nodes',
			body: (
				<>
					<p>
						A process that subscribes to a topic of another node subscribes through a relay on its
						own node: a process named <C>{'pubsub:{stock@warehouse}'}</C>, which the first such
						subscriber starts. The relay is a topic too. Its events are the upstream topic's, and it
						keeps as many as the upstream does, so the second subscriber on a node gets the kept
						events without another trip across the network. The upstream topic sends each event
						once to each node, however many subscribers the node has.
					</p>
					<p>
						For the subscriber, this changes little. <C>sub.From</C> is the relay, and the{' '}
						<C>Down</C> comes from the relay, with the topic's reason: the relay ends with the topic,
						for the same reason, and ends as well when its last subscriber leaves, after which the
						topic forgets it.
					</p>
					<p>
						The relay subscribes to the topic while its first subscriber waits. A call's deadline
						does not reach the process called, so <C>Subscribe</C> sends the relay the time left on
						its <C>ctx</C>, and the relay gives up when the subscriber does. Give <C>Subscribe</C> a
						deadline, as the dashboard does: with none, the relay waits until the topic answers or
						its node becomes unreachable. A topic that does not exist is <C>ErrNoProc</C>, and a node
						that cannot be reached <C>ErrNoConnection</C>, as for a call.
					</p>
				</>
			)
		},
		{
			id: 'example',
			title: 'The example, run',
			body: (
				<>
					<p>
						Two nodes in one binary, as in the <A to="start/">quick start</A>: inventory and its
						topic on <C>warehouse</C>, a dashboard on <C>shop</C>. Three reservations come before
						the dashboard subscribes, so it starts with the two levels the topic kept; the fourth
						reaches it as it is published; and when inventory is asked to exit, the dashboard is
						told the topic ended, and why.
					</p>
					<Code caption="examples/pubsub/main.go">{region(example, /Three reservations before anyone listens/, /^\t<-done/)}</Code>
					<Output>{exampleOutput}</Output>
				</>
			)
		},
		{
			id: 'limits',
			title: 'What it does not do',
			body: (
				<>
					<ul>
						<li>
							Delivery is at most once, as for any message. A subscriber that must not miss an
							event needs acknowledgements, built above this.
						</li>
						<li>
							Nothing pushes back on a publisher. A subscriber slower than its topic has a mailbox
							that grows, as any process's does.
						</li>
						<li>
							A node-wide topic that a supervisor restarts comes back with no subscribers. They
							receive a <C>Down</C>, and subscribe again.
						</li>
						<li>
							A topic addressed by its name and by its PID gets two relays on a node, one for
							each.
						</li>
					</ul>
					<Aside title="Not the node's events">
						<C>node.Subscribe</C> is a different thing with a similar name: a Go channel of what
						happens on the node itself, spawns, exits, links up and down, for tools and tests.{' '}
						<A to="guides/observability/#events">Observability</A> covers it. A topic is for an
						application's own events, between processes.
					</Aside>
				</>
			)
		}
	]
};
