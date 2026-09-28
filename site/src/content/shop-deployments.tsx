import billing from '../../../examples/guide/cmd/billing/main.go?raw';
import front from '../../../examples/guide/cmd/front/main.go?raw';
import local from '../../../examples/guide/cmd/local/main.go?raw';
import warehouse from '../../../examples/guide/cmd/warehouse/main.go?raw';
import clusterTree from '../../../examples/guide/testdata/cluster.txt?raw';
import localTree from '../../../examples/guide/testdata/local.txt?raw';

import { Code } from '../code.tsx';
import { Drawing } from '../components/Figure.tsx';
import { ClusterPicture, LocalPicture } from '../components/diagrams.tsx';
import { A, C, Ext } from '../components/prose.tsx';
import { REPO } from '../config.ts';
import type { Doc } from './types.ts';

const localShell = `git clone https://github.com/floatdrop/grpcproc && cd grpcproc/examples
go build -o bin/ ./guide/cmd/...
bin/local &
sleep 1
curl -d '{"sku":"apple","qty":2,"card":"4242"}' localhost:8080/orders
kill %1`;

const clusterShell = `export PEERS=front=127.0.0.1:9101,warehouse=127.0.0.1:9102,billing=127.0.0.1:9103
NODE=warehouse LISTEN=127.0.0.1:9102 bin/warehouse &
NODE=billing LISTEN=127.0.0.1:9103 bin/billing &
NODE=front LISTEN=127.0.0.1:9101 PLACEMENT=inventory=warehouse,payments=billing bin/front &
sleep 1
curl -d '{"sku":"pear","qty":3,"card":"4242"}' localhost:8080/orders`;

export const shopDeployments: Doc = {
	path: 'shop/deployments/',
	title: 'One program, or three',
	description:
		'The same modules composed two ways: every service on one node for development, or split between a front, a warehouse and a billing node, each main one line.',
	lead: (
		<p>
			<A to="shop/platform/">The previous chapter</A> built the platform, whose <C>Run</C> is every
			entry point's <C>main</C>. This one writes the entry points: the one program, and the three.
		</p>
	),
	sections: [
		{
			id: 'local',
			title: 'One program',
			body: (
				<>
					<p>
						<C>cmd/local</C> composes every service and needs no configuration: with no placement,
						every service runs on this node, called <C>shop</C>.
					</p>
					<Code caption="cmd/local/main.go">{local}</Code>
					<Drawing caption="one program">
						<LocalPicture />
					</Drawing>
					<p>
						The tree is the root, a supervisor per service and each service's process, in
						composition order. A test starts <C>cmd/local</C>'s composition and draws it from the
						processes' parents:
					</p>
					<Code lang="txt" caption="what the node runs">
						{localTree}
					</Code>
					<p>
						A call from the desk to the stock never leaves the program. A local send appends to the
						process's mailbox, and the message is not even encoded.
					</p>
					<Code lang="sh" caption="shell">
						{localShell}
					</Code>
				</>
			)
		},
		{
			id: 'cluster',
			title: 'Three programs',
			body: (
				<>
					<p>
						The distributed deployment splits the same modules between three entry points: the front
						takes the orders and serves HTTP, the warehouse keeps the stock, and billing takes the
						payments. Each <C>main</C> is one line.
					</p>
					<Code caption="cmd/front/main.go">{front}</Code>
					<Code caption="cmd/warehouse/main.go">{warehouse}</Code>
					<Code caption="cmd/billing/main.go">{billing}</Code>
					<p>
						The front's placement puts the inventory on <C>warehouse</C> and the payments on{' '}
						<C>billing</C>, so the desk's addresses name those nodes; nothing in the desk changed.
						The warehouse and billing are told the front's address although they never call it,
						because a reply travels back on the replier's own link. A placement naming a node not
						among the peers is refused at start.
					</p>
					<Drawing caption="three programs">
						<ClusterPicture />
					</Drawing>
					<p>Each node's tree holds only the services its entry point composed:</p>
					<Code lang="txt" caption="what each node runs">
						{clusterTree}
					</Code>
					<p>
						Here the peers are a static list. A link that breaks fires a <C>Down</C> for every
						process monitored across it and fails the calls waiting on it. While dials to a node
						fail, calls to it fail at once, and it is dialed again within five seconds, so a
						restarted warehouse is back in the front's orders soon after it starts. For nodes that
						come and go,{' '}
						<Ext href={`${REPO}/tree/main/etcd`}>
							<C>grpcproc/etcd</C>
						</Ext>{' '}
						publishes each node under an etcd lease and resolves the others through it; the platform
						takes its resolver, registrar and membership from there, and the services do not
						change. <A to="guides/etcd/">Running with etcd</A> shows how.
					</p>
					<Code lang="sh" caption="shell">
						{clusterShell}
					</Code>
				</>
			)
		}
	]
};
