import config from '../../../examples/guide/internal/platform/config.go?raw';
import platform from '../../../examples/guide/internal/platform/platform.go?raw';
import root from '../../../examples/guide/internal/platform/root.go?raw';
import run from '../../../examples/guide/internal/platform/run.go?raw';
import modules from '../../../examples/guide/testdata/modules.txt?raw';

import { Code } from '../code.tsx';
import { Drawing } from '../components/Figure.tsx';
import { Lifecycle } from '../components/diagrams.tsx';
import { A, C } from '../components/prose.tsx';
import type { Doc } from './types.ts';

export const shopPlatform: Doc = {
	path: 'shop/platform/',
	title: 'The platform',
	description:
		'What every program of the shop runs: its configuration, a gRPC server with the node and the Inspector on it, the root supervisor, and the main that composes them with the services.',
	lead: (
		<p>
			<A to="shop/services/">The previous chapter</A> wrote the services; each knows the node and the
			configuration, and nothing else of what runs it. This one is that: the platform, which every
			entry point composes with its services.
		</p>
	),
	sections: [
		{
			id: 'config',
			title: 'Configuration',
			body: (
				<>
					<p>
						Every program runs the same platform. It starts from the configuration, the one thing
						the programs of a deployment differ in: which node this is, where it listens, where the
						other nodes are, and which node runs each service it does not. <C>Where</C> is what the
						desk and the front ask.
					</p>
					<Code caption="internal/platform/config.go">{config}</Code>
				</>
			)
		},
		{
			id: 'platform',
			title: 'The node and the server',
			body: (
				<>
					<p>
						Then it builds a gRPC server, the node registered on it, the Inspector beside the node,
						and the root supervisor. <C>grpcprocctl</C> talks to the Inspector. The node is a
						dependency like any other: a constructor that needs it takes it as a parameter.
					</p>
					<Code caption="internal/platform/platform.go">{platform}</Code>
				</>
			)
		},
		{
			id: 'lifecycle',
			title: 'Start and stop',
			body: (
				<>
					<p>
						The container builds eager services in registration order and starts them in build
						order, so the node and the Inspector are registered on the server before it serves.
						Stopping runs the other way, which is what a node wants: the root stops the trees first,
						then the node closes its links, and the server goes last.
					</p>
					<Drawing caption="start and stop">
						<Lifecycle />
					</Drawing>
					<p>
						Keepalive in the dial options is what turns a silent peer into a broken link. The
						Inspector is read-only, since it shares the node's port and nothing here checks who
						calls it.
					</p>
				</>
			)
		},
		{
			id: 'root',
			title: 'The root supervisor',
			body: (
				<>
					<p>
						The root is built from the group the service modules add their trees to: one supervisor
						per node, <C>actor.OneForOne</C>, so a service whose supervisor gives up is restarted
						without the others. It starts the node once the services run, which with a registry is
						when peers learn of the node. A worker watches it: a root that exits on its own, past
						its restart limit, means the services have given up, and the worker's error stops the
						program for whatever runs it to start again.
					</p>
					<Code caption="internal/platform/root.go">{root}</Code>
				</>
			)
		},
		{
			id: 'run',
			title: 'Run and Compose',
			body: (
				<>
					<p>
						<C>Run</C> is every entry point's <C>main</C>: it loads the configuration, composes the
						platform with the services, validates the graph before building anything, and runs until
						a signal arrives or a worker fails. <C>Compose</C> is the part the tests use.
					</p>
					<Code caption="internal/platform/run.go">{run}</Code>
					<p>
						Each module declares what it provides and what its constructors need, so the container
						can report a composition without building it. This is <C>cmd/local</C>'s, as the tests
						compose it; another test checks that each <C>main</C> passes the modules its test
						composes.
					</p>
					<Code lang="txt" caption="app.Modules(), as the tests compose cmd/local">
						{modules}
					</Code>
				</>
			)
		}
	]
};
