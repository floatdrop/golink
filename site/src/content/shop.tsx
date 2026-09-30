import inventoryAddress from '../../../examples/guide/proto/inventory/v1/address.go?raw';
import inventoryProto from '../../../examples/guide/proto/inventory/v1/inventory.proto?raw';

import { Code } from '../code.tsx';
import { Drawing } from '../components/Figure.tsx';
import { Layout } from '../components/diagrams.tsx';
import { C, Ext } from '../components/prose.tsx';
import { REPO } from '../config.ts';
import type { Doc } from './types.ts';

/** The annotated directory listing of examples/guide. */
const tree = [
	{ path: 'cmd/local/', comment: 'one program: every service on one node' },
	{ path: 'cmd/front/', comment: 'or three: the web front and the orders,' },
	{ path: 'cmd/warehouse/', comment: 'the inventory,' },
	{ path: 'cmd/billing/', comment: 'and the payments' },
	{ path: 'internal/platform/', comment: 'what every program runs: config, node, root supervisor' },
	{ path: 'internal/inventory/', comment: 'the stock, kept in a store' },
	{ path: 'internal/payments/', comment: 'the cashier, in front of a payment gateway' },
	{ path: 'internal/orders/', comment: 'the desk, which calls the other two' },
	{ path: 'internal/web/', comment: 'the HTTP front' },
	{ path: 'proto/<service>/v1/', comment: "a service's contract: messages and a process name" },
	{ path: '*_test.go, testdata/', comment: 'the tests, and the output this tutorial shows' }
];

const pad = Math.max(...tree.map((row) => row.path.length)) + 2;
const treeText = tree.map((row) => `${row.path.padEnd(pad)}${row.comment}`).join('\n');

export const shop: Doc = {
	path: 'shop/',
	title: 'The shop',
	description:
		'A tutorial: one application, read chapter by chapter. A shop whose services are grpcproc processes, wired with golang.yandex/di, run as one program or as three nodes from the same code.',
	sections: [
		{
			id: 'build',
			title: 'What we build',
			body: (
				<>
					<p>
						This tutorial is one application, read chapter by chapter: a shop that takes orders over
						HTTP, reserves stock and charges a card. Its services are grpcproc processes, built and
						wired by <Ext href="https://github.com/yandex/di">golang.yandex/di</Ext>, a
						dependency-injection container. The same code runs as one program, for development, or
						as three programs on three nodes, as deployed.
					</p>
					<p>
						Every file shown is from{' '}
						<Ext href={`${REPO}/tree/main/examples/guide`}>
							<C>examples/guide</C>
						</Ext>{' '}
						in the repository, and every output is pinned by its tests, so what you read is what
						runs.
					</p>
				</>
			)
		},
		{
			id: 'shape',
			title: 'The shape of an application',
			body: (
				<>
					<p>
						The shop's code comes in four kinds of package. A <em>service</em> exports one function,{' '}
						<C>Module</C>, which registers with the container what the service needs and, if it runs
						processes, their supervision tree. A <em>contract</em> is the messages a service's
						process accepts and the name it is registered under; it is all another service may
						import. The <em>platform</em> is what every program runs. An <em>entry point</em> is a{' '}
						<C>main</C> that picks the services.
					</p>
					<Code lang="txt" caption="examples/guide">
						{treeText}
					</Code>
					<p>
						No service knows where the others run. The orders service reaches the inventory through
						the inventory's contract and a node name from the placement in the configuration, which
						says which node runs each service. Whether that node is its own is not its business. So
						the shop as one program and the shop as three differ in two things only: which services
						each entry point composes, and what each program is told.
					</p>
					<Drawing caption="the four layers, and what imports the contracts">
						<Layout />
					</Drawing>
				</>
			)
		},
		{
			id: 'contracts',
			title: 'Contracts',
			body: (
				<>
					<p>
						A contract is a protobuf package: the messages a service's process accepts and answers.
						A mailbox holds one message type, so the stock takes a <C>Command</C> whose oneof
						carries each operation: a reservation, a call answered with <C>Reserved</C>, and a
						release, a send nobody waits for.
					</p>
					<Code lang="proto" caption="proto/inventory/v1/inventory.proto">
						{inventoryProto}
					</Code>
					<p>
						A refusal is part of the answer: <C>Reserved</C> says what is left, or why nothing was
						reserved, so an error means the stock failed, not that it said no. The registered name
						is part of the contract too. The package exports the address as a type of its own, made
						from a node name: it embeds <C>Addr[*Command]</C>, so the compiler checks every send to
						it, wherever the process runs. Its methods are the protocol: <C>Reserve</C> is a call
						answered with <C>Reserved</C>, <C>Release</C> is a send, and the <C>Command</C> around
						each is wrapped here, once. Each takes its sender, a node or a process, as a{' '}
						<C>grpcproc.Caller</C>.
					</p>
					<Code caption="proto/inventory/v1/address.go">{inventoryAddress}</Code>
				</>
			)
		}
	]
};
