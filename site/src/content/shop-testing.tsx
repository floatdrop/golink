import clusterTest from '../../../examples/guide/cluster_test.go?raw';
import ordersTest from '../../../examples/guide/internal/orders/orders_test.go?raw';

import { Code } from '../code.tsx';
import { A, C, Ext } from '../components/prose.tsx';
import { file } from '../config.ts';
import type { Doc } from './types.ts';

const inspect = `go install github.com/floatdrop/grpcproc/tools/cmd/grpcprocctl@latest
export GRPCPROC_ADDR=127.0.0.1:9101          # the front's Inspector
grpcprocctl --plaintext nodes                # the nodes it is linked to, and their links
grpcprocctl --plaintext ps --node warehouse  # the warehouse's processes, asked there
grpcprocctl --plaintext inspect root         # the front's root: its children, its restarts
grpcprocctl --plaintext dot --cluster | dot -Tsvg -o shop.svg
kill %1 %2 %3                                # the three programs`;

export const shopTesting: Doc = {
	path: 'shop/testing/',
	title: 'Testing and looking inside',
	description:
		'A service tested alone with fakes behind the addresses it calls, the deployments tested whole, and the running cluster inspected with grpcprocctl.',
	lead: (
		<p>
			<A to="shop/deployments/">The previous chapter</A> ran the shop as one program and as three.
			This one tests both, and looks inside the three while they run.
		</p>
	),
	sections: [
		{
			id: 'testing',
			title: 'Testing',
			body: (
				<>
					<p>
						A service is tested alone with fakes behind the addresses it calls: processes registered
						under the contracts' names on the node the placement points at, which with no placement
						is this one. <C>di.Test</C> gives a scope stopped when the test ends; the test composes
						the platform and the orders module in it.
					</p>
					<Code caption="internal/orders/orders_test.go">{ordersTest}</Code>
					<p>
						The deployments are tested whole. Each node is composed as its entry point composes it
						and told what a deployment would: its peers, and where the services it does not run are.
						The test binds the three ports first, so it knows them, and hands each node its listener
						with an override, which di requires to be marked as one. It places orders through the
						front, pins what each node runs, and stops billing to see the front answer 503.
					</p>
					<Code caption="cluster_test.go">{clusterTest}</Code>
				</>
			)
		},
		{
			id: 'inspect',
			title: 'Looking inside',
			body: (
				<>
					<p>
						Every node serves the Inspector, and one asked about another node forwards the question
						there, so <C>grpcprocctl</C> pointed at one node can ask about any. <C>nodes</C> and{' '}
						<C>dot --cluster</C> list the nodes it is linked to, their links and processes, and who
						started whom; for the front, once it has taken an order, that is all three.{' '}
						<C>inspect</C> shows what a process says about itself; a supervisor lists its children
						and restarts. The platform's Inspector is read-only, so <C>exit</C> and{' '}
						<C>loglevel</C> are refused.
					</p>
					<Code lang="sh" caption="shell">
						{inspect}
					</Code>
					<p>
						That is the whole shop. <A to="concepts/actors/">The concept pages</A> explain what it
						stands on: processes, addresses, monitors and links, supervision trees, nodes.{' '}
						<A to="guides/observability/">Observability</A> covers the counters, hooks and
						OpenTelemetry, and <A to="guides/inspector/">The Inspector</A> the rest of what{' '}
						<C>grpcprocctl</C> can do. <Ext href={file('docs/DESIGN.md')}>The design notes</Ext> have
						the wire protocol and the reasons behind the choices.
					</p>
				</>
			)
		}
	]
};
