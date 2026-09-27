import { Link } from '../uikit.ts';

import { C } from '../components/prose.tsx';
import { PKG_DOC, REPO } from '../config.ts';
import type { Content } from './types.ts';

const file = (path: string) => `${REPO}/blob/main/${path}`;

export const content: Content = {
	meta: {
		title: 'grpcproc: Erlang-style processes for Go',
		description:
			'Erlang-style processes for Go, on the gRPC server you already run. One application, top to bottom: services as actors wired with golang.yandex/di, run as one program or as three.'
	},

	hero: {
		title: 'Erlang-style processes for Go, on the gRPC server you already run.',
		lead: (
			<>
				A process is a goroutine with a mailbox and a PID the whole cluster can address.{' '}
				<C>Send</C>, <C>Call</C>, <C>Monitor</C> and <C>Exit</C> work the same whether it runs in
				this binary or on another node.
			</>
		),
		install: 'go get github.com/floatdrop/grpcproc'
	},

	labels: {
		steps: 'Steps',
		copy: 'Copy the install command',
		toLight: 'Switch to the light theme',
		toDark: 'Switch to the dark theme'
	},

	intro: (
		<>
			<p>
				This page is one application, read top to bottom: a shop that takes orders over HTTP,
				reserves stock and charges a card. Its services are grpcproc processes, built and wired by{' '}
				<Link href="https://github.com/yandex/di">golang.yandex/di</Link>, a dependency-injection
				container. The same code runs as one program, for development, or as three programs on
				three nodes, as deployed.
			</p>
			<p>
				Every file shown is from{' '}
				<Link href={`${REPO}/tree/main/examples/guide`}>
					<C>examples/guide</C>
				</Link>{' '}
				in the repository, and every output is pinned by its tests, so what you read is what runs.
			</p>
		</>
	),

	tree: [
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
		{ path: '*_test.go, testdata/', comment: 'the tests, and the output this page shows' }
	],

	steps: [
		{
			id: 'shape',
			title: 'The shape of an application',
			body: (f) => (
				<>
					<p>
						A <em>node</em> is grpcproc in one program: it hosts processes and links to the nodes
						of other programs. The shop's code comes in four kinds of package. A <em>service</em>{' '}
						exports one function, <C>Module</C>, which registers with the container what the
						service needs and, if it runs processes, their supervision tree, the supervisors that
						start and restart them. A <em>contract</em>{' '}
						is the messages a service's process accepts and the name it is registered under; it
						is all another service may import. The <em>platform</em> is what every program runs.
						An <em>entry point</em> is a <C>main</C> that picks the services.
					</p>
					{f.tree}
					<p>
						No service knows where the others run. The orders service reaches the inventory
						through the inventory's contract and a node name from the placement in the configuration, which
						says which node runs each service. Whether that node is its own is not its business.
						So the shop as one program and the shop as three differ in two things only: which
						services each entry point composes, and what each program is told.
					</p>
					{f.layout}
				</>
			)
		},
		{
			id: 'contracts',
			title: 'Contracts',
			body: (f) => (
				<>
					<p>
						A contract is a protobuf package: the messages a service's process accepts and
						answers. A mailbox holds one message type, so the stock takes a <C>Command</C> whose
						oneof carries each operation: a reservation, a call answered with <C>Reserved</C>, and
						a release, a send nobody waits for.
					</p>
					{f.inventoryProto}
					<p>
						A refusal is part of the answer: <C>Reserved</C> says what is left, or why nothing was
						reserved, so an error means the stock failed, not that it said no. The registered name
						is part of the contract too. The package exports it with a function that makes the
						address from a node name; <C>Addr[*Command]</C> carries the message type, so the
						compiler checks every send to it, wherever the process runs.
					</p>
					{f.inventoryAddress}
				</>
			)
		},
		{
			id: 'service',
			title: 'A service is a module',
			body: (f) => (
				<>
					<p>
						The stock is an actor: a struct holding its dependencies, with a method per kind of
						message. <C>Init</C> runs before the first message. <C>HandleCall</C> answers
						reservations, with a refusal in the answer or, when the store fails, an error; the
						process carries on either way. <C>HandleMessage</C> takes releases. A reservation
						sent without a call is the sender's bug: it is logged and dropped, since bad input
						from another service is no reason to crash.
					</p>
					<p>
						The levels live in a <C>Store</C>, not in the actor. When the store fails on a release
						the process crashes, and its supervisor starts a new one; <C>actor.Child</C> builds a
						fresh actor at every start, so it begins from what the store says, not from whatever
						crashed. The store is an interface the container serves. The guide's store keeps the levels in
						memory; a database would be built, started and stopped by the container the same
						way, and nothing else would change.
					</p>
					<p>
						<C>Module</C> registers the store and the service's tree. The tree is not started
						here: it joins the group of <C>actor.ChildSpec</C> the platform builds the root
						supervisor from, so a program runs whichever services its entry point composes, and
						nothing keeps a list of them.
					</p>
					{f.inventory}
				</>
			)
		},
		{
			id: 'calls',
			title: 'A service that only answers',
			body: (f) => (
				<>
					<p>
						The payments service's cashier answers calls and nothing else. It embeds <C>actor.CallsOnly</C> as its{' '}
						<C>HandleMessage</C>: a plain send to it is logged and dropped. Its <C>Gateway</C>, the payment
						provider's, comes from the container as the store did. A declined card is part of the answer; any
						other gateway error is a failure, after which the card may or may not have been
						charged. The guide's gateway is a sandbox that declines cards starting with 4000.
					</p>
					{f.payments}
				</>
			)
		},
		{
			id: 'orders',
			title: 'Calling other services',
			body: (f) => (
				<>
					<p>
						The desk takes an order: it reserves the items, charges for them and answers with the
						receipt. The stock and the cashier belong to other services, perhaps on other nodes;
						the desk calls them the same way wherever they run: <C>p.Call</C> to a typed address
						under a deadline.
					</p>
					<p>
						A refusal from either is passed on in the desk's answer, and after a declined card the
						items are released, with a send, since nothing waits for it. An error from either, no
						answer or a failure, means what happened is not known: the items may be reserved, the card may be charged.
						The desk then leaves things as they are, logs the order for reconciliation, and fails
						the call.
					</p>
					<p>
						The addresses are made once, in <C>tree</C>, from the placement; every desk the
						supervisor starts gets the same ones. A desk handles one order at a time: while it
						waits on the stock and the cashier, the next order waits in its mailbox. A desk that
						must not wait would return <C>actor.ErrNoReply</C> and answer from another goroutine.
					</p>
					{f.orders}
					{f.flow}
				</>
			)
		},
		{
			id: 'edge',
			title: 'The edge',
			body: (f) => (
				<>
					<p>
						The web front is not a process. An HTTP handler calls the desk through the node, as
						any code outside a process does. <C>node.Call</C> gets the request's context, so a
						client that hangs up stops the wait, not the order: the desk still takes it.
					</p>
					<p>
						A refusal becomes a 422 with the reason; an error a 503, whether the desk could not be
						reached or could not settle the order. What went wrong between the nodes goes to the
						log, not to the client. The server is registered with the
						container with hooks: it listens when it starts and drains before anything else
						stops, so an order in flight still finds the desk.
					</p>
					{f.web}
				</>
			)
		},
		{
			id: 'platform',
			title: 'The platform',
			body: (f) => (
				<>
					<p>
						Every program runs the same platform. It starts from the configuration, the one thing
						the programs of a deployment differ in: which node this is, where it listens, where
						the other nodes are, and which node runs each service it does not. <C>Where</C> is
						what the desk and the front ask.
					</p>
					{f.config}
					<p>
						Then it builds a gRPC server, the node registered on it, the Inspector beside the
						node, and the root supervisor. <C>grpcprocctl</C> talks to the Inspector. The node is
						a dependency like any other: a constructor that needs it takes it as a parameter.
					</p>
					{f.platform}
					<p>
						The container builds eager services in registration order and starts them in build
						order, so the node and the Inspector are registered on the server before it serves.
						Stopping runs the other way, which is what a node wants: the root stops the trees
						first, then the node closes its links, and the server goes last.
					</p>
					{f.lifecycle}
					<p>
						Keepalive in the dial options is what turns a silent peer into a broken link. The
						Inspector is read-only, since it shares the node's port and nothing here checks who
						calls it.
					</p>
					<p>
						The root is built from the group the service modules add their trees to: one
						supervisor per node, <C>actor.OneForOne</C>, so a service whose supervisor gives up is
						restarted without the others. It starts the node once the services run, which with a
						registry is when peers learn of the node. A worker watches it: a root that exits on its own,
						past its restart limit, means the services have given up, and the worker's error
						stops the program for whatever runs it to start again.
					</p>
					{f.root}
					<p>
						<C>Run</C> is every entry point's <C>main</C>: it loads the configuration, composes
						the platform with the services, validates the graph before building anything, and
						runs until a signal arrives or a worker fails. <C>Compose</C> is the part the tests
						use.
					</p>
					{f.run}
					<p>
						Each module declares what it provides and what its constructors need, so the
						container can report a composition without building it. This is <C>cmd/local</C>'s,
						as the tests compose it; another test checks that each <C>main</C> passes the modules
						its test composes.
					</p>
					{f.modules}
				</>
			)
		},
		{
			id: 'local',
			title: 'One program',
			body: (f) => (
				<>
					<p>
						<C>cmd/local</C> composes every service and needs no configuration: with no placement,
						every service runs on this node, called <C>shop</C>.
					</p>
					{f.local}
					{f.localPicture}
					<p>
						The tree is the root, a supervisor per service and each service's process, in
						composition order. A test starts <C>cmd/local</C>'s composition and draws it from the
						processes' parents:
					</p>
					{f.localTree}
					<p>
						A call from the desk to the stock never leaves the program. A local send appends to
						the process's mailbox, and the message is not even encoded.
					</p>
					{f.localShell}
				</>
			)
		},
		{
			id: 'cluster',
			title: 'Three programs',
			body: (f) => (
				<>
					<p>
						The distributed deployment splits the same modules between three entry points: the
						front takes the orders and serves HTTP, the warehouse keeps the stock, and billing
						takes the payments. Each <C>main</C> is one line.
					</p>
					{f.front}
					{f.warehouse}
					{f.billing}
					<p>
						The front's placement puts the inventory on{' '}
						<C>warehouse</C> and the payments on <C>billing</C>, so the desk's addresses name
						those nodes; nothing in the desk changed. The warehouse and billing are told the
						front's address although they never call it, because a reply travels back on the
						replier's own link. A placement naming a node not among the peers is refused at
						start.
					</p>
					{f.clusterPicture}
					<p>Each node's tree holds only the services its entry point composed:</p>
					{f.clusterTree}
					<p>
						Here the peers are a static list. A link that breaks fires a <C>Down</C> for every
						process monitored across it and fails the calls waiting on it. While dials to a node
						fail, calls to it fail at once, and it is dialed again within five seconds, so a
						restarted warehouse is back in the front's orders soon after it starts. For nodes
						that come and go,{' '}
						<Link href={`${REPO}/tree/main/etcd`}>
							<C>grpcproc/etcd</C>
						</Link>{' '}
						publishes each node under an etcd lease and resolves the others through it; the
						platform takes its resolver, registrar and membership from there, and the services do
						not change.
					</p>
					{f.clusterShell}
				</>
			)
		},
		{
			id: 'testing',
			title: 'Testing',
			body: (f) => (
				<>
					<p>
						A service is tested alone with fakes behind the addresses it calls: processes
						registered under the contracts' names on the node the placement points at, which with
						no placement is this one. <C>di.Test</C> gives a scope stopped when the test ends; the
						test composes the platform and the orders module in it.
					</p>
					{f.ordersTest}
					<p>
						The deployments are tested whole. Each node is composed as its entry point composes
						it and told what a deployment would: its peers, and where the services it does not
						run are. The test binds the three ports first, so it knows them, and hands each node
						its listener with an override, which di requires to be marked as one. It places orders through the front, pins what each
						node runs, and stops billing to see the front answer 503.
					</p>
					{f.clusterTest}
				</>
			)
		},
		{
			id: 'inspect',
			title: 'Looking inside',
			body: (f) => (
				<>
					<p>
						Every node serves the Inspector, and one asked about another node forwards the
						question there, so <C>grpcprocctl</C> pointed at one node can ask about any.{' '}
						<C>nodes</C> and <C>dot --cluster</C> list the nodes it is linked to, their links and
						processes, and who started whom; for the front, once it has taken an order, that is
						all three. <C>inspect</C> shows what a process says about itself; a supervisor
						lists its children and restarts. The platform's Inspector is read-only, so{' '}
						<C>exit</C> and <C>loglevel</C> are refused.
					</p>
					{f.inspect}
					<p>
						The <Link href={`${REPO}#readme`}>README</Link> covers the rest of grpcproc:
						monitors, timers, hooks and OpenTelemetry.{' '}
						<Link href={file('docs/DESIGN.md')}>The design document</Link> has the wire protocol
						and the reasons behind the choices.
					</p>
				</>
			)
		}
	],

	footer: (
		<>
			<Link href={REPO}>github.com/floatdrop/grpcproc</Link> · <Link href={PKG_DOC}>pkg.go.dev</Link>{' '}
			· MIT licensed · This page is built from the repository's <C>site/</C> directory and the
			code it shows from <C>examples/guide/</C>; its build is adapted from{' '}
			<Link href="https://github.com/yandex/di">golang.yandex/di</Link>'s site.
		</>
	)
};
