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
				reserves stock and charges a card. Its services are made of grpcproc processes, the
				actors, and <Link href="https://github.com/yandex/di">golang.yandex/di</Link>, a
				dependency-injection container, builds them and what they depend on. The shop runs two
				ways from the same code: as one program, the way a developer runs it, and as three
				programs on three nodes, the way it is deployed.
			</p>
			<p>
				Every Go file shown is from{' '}
				<Link href={`${REPO}/tree/main/examples/guide`}>
					<C>examples/guide</C>
				</Link>{' '}
				in the repository, and every output is pinned by its tests. The Go toolchain compiles
				and tests them on every change, so what you read is what runs.
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
						A <em>node</em> is grpcproc in one program: it hosts processes, and links to the nodes
						of other programs. The shop's code comes in four kinds of package. A{' '}
						<em>service</em> is a package with one exported function, <C>Module</C>, that
						registers with the container what the service needs and, when it runs processes,
						their supervision tree: the supervisors that start them and restart them when they
						fail. A <em>contract</em> is the messages a service's process accepts and the name it
						is registered under, and it is all another service may import. The{' '}
						<em>platform</em> is what every program runs. An <em>entry point</em> is a{' '}
						<C>main</C> that picks the services.
					</p>
					{f.tree}
					<p>
						Nothing in a service knows where the others run. The orders service reaches the
						inventory through the inventory's contract and a node name from the configuration's
						placement, which says which node runs each service. Whether that node is its own is
						not its business. So between the shop as one program and the shop as three, only two
						things differ: which services each entry point composes, and what each program is
						told.
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
						A service's contract is a protobuf package: the messages its process accepts and
						answers. A mailbox holds one message type, so the stock process takes a{' '}
						<C>Command</C> whose oneof carries each operation: a reservation, which is a call
						answered with <C>Reserved</C>, and a release, which is a send that nobody waits for.
					</p>
					{f.inventoryProto}
					<p>
						A no is part of an answer too: <C>Reserved</C> says what is left, or why nothing was
						reserved. An error then means the stock failed, not that it said no, and the caller
						can tell the two apart. The name the process is registered under is part of the
						contract as well. The package exports it, with a function that makes the address from
						a node name. An{' '}
						<C>Addr[*Command]</C> carries the message type, so the compiler checks every send to
						it, whether the process is in this program or not.
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
						The stock process is an actor: a struct holding its dependencies, with a method per
						kind of message. <C>HandleCall</C> answers reservations, with a refusal in the answer
						or, if the store fails, an error; either way the process carries on.{' '}
						<C>HandleMessage</C> takes releases. A reservation sent without a call is the
						sender's bug, so it is logged and dropped: bad input from another service is no
						reason to crash. <C>Init</C> runs before the first message.
					</p>
					<p>
						The levels live in a <C>Store</C>, not in the actor. When the store fails on a
						release, the process crashes, and its supervisor starts a new one. <C>actor.Child</C> builds a
						fresh actor at every start, so the new process begins from what the store says rather
						than from whatever crashed. The store is an interface the container serves. The
						guide's keeps the levels in memory; a database would be built, started and stopped
						by the container the same way, and nothing else would change.
					</p>
					<p>
						<C>Module</C> registers the store and the service's tree. The tree is not started
						here. It joins the group of <C>actor.ChildSpec</C> that the platform builds each
						node's root supervisor from, so a program runs the services whose modules its entry
						point composes, and nothing keeps a list of them.
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
						The payments service's cashier answers calls and nothing else. It embeds{' '}
						<C>actor.CallsOnly</C>, which is its <C>HandleMessage</C>: a message sent to it
						without a call is logged and dropped. Its dependency, the payment provider's{' '}
						<C>Gateway</C>, comes from the container as the store did. A card the provider
						declines is part of the cashier's answer; any other gateway error is a failure,
						after which the card may or may not have been charged. The guide's gateway is the
						provider's sandbox, which declines cards starting with 4000.
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
						The desk takes an order: it reserves the items, charges for them, and answers with
						the receipt. The inventory and the payments are other services, perhaps on other
						nodes, and the desk calls them the same way either way, with <C>p.Call</C> to a typed
						address under a deadline.
					</p>
					<p>
						A no is part of each answer: the stock's that it cannot reserve, the cashier's that
						the card was declined. The desk passes a no on in its own answer, and when the card
						was declined it gives the items back. That is a send, since the desk does not need
						to wait for it. An error from either is no answer, or a failure, and what happened
						is not known: the items may or may not be reserved, and the card may have been
						charged. So the desk leaves things as they are, logs the order for whoever
						reconciles orders, and fails the call.
					</p>
					<p>
						The addresses are made once, in <C>tree</C>, from the placement, and every desk the
						supervisor starts gets the same ones. A desk handles one order at a time: while it
						waits on the stock and the cashier, the next order waits in its mailbox. That keeps
						it simple. A desk that must not wait would return <C>actor.ErrNoReply</C> and answer
						from another goroutine.
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
						The web front is not a process. An HTTP handler is ordinary code, and it calls the
						desk through the node, as any code outside a process does. <C>node.Call</C> gets the
						request's context, so a client that hangs up stops the wait. It does not stop the
						order: the desk still takes it.
					</p>
					<p>
						A refusal in the desk's answer becomes a 422, with the reason. An error becomes a
						503: the desk could not be reached, or could not settle the order. What went wrong between the nodes goes to
						the log, not to the client. The server is registered with the container with hooks, as in any
						di application: it listens when it starts, and drains before anything is stopped, so
						an order in flight still finds the desk.
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
						Every program runs the same platform, whatever services it hosts. It starts from the
						configuration, the one thing the programs of a deployment differ in: which node this
						is, where it listens, where the other nodes are, and which node runs each service it
						does not. <C>Where</C> is what the desk and the front ask.
					</p>
					{f.config}
					<p>
						Then it builds a gRPC server, the node registered on it, the Inspector beside the
						node, and the root supervisor. The Inspector is the gRPC service that the{' '}
						<C>grpcprocctl</C> command line talks to. The node is registered with the container
						like any other dependency, so a constructor that needs it takes it as a parameter.
					</p>
					{f.platform}
					<p>
						The container builds eager services in the order they were registered, and starts
						them in the order it built them. So the node and the Inspector are registered on the
						gRPC server before it serves. Stopping runs the other way, which is the order a node
						wants: the root supervisor stops the services' trees first, then the node closes its
						links, and the server goes last.
					</p>
					{f.lifecycle}
					<p>
						Keepalive, in the dial options, is what turns a peer that went silent into a broken
						link. The Inspector is read-only, because it shares the node's port and nothing
						here checks who calls it.
					</p>
					<p>
						The root is built from the group each service module adds its tree to. It is one
						supervisor per node, <C>actor.OneForOne</C>, so a service whose supervisor gives up
						is restarted without the others. It starts the node once the services run; with a
						registry, that is when peers learn of the node. And it is watched. A worker returns
						when the root exits, and a root that exits on its own, past its restart limit, means
						the program's services have given up. The worker's error stops the program, for
						whatever runs it to start again.
					</p>
					{f.root}
					<p>
						<C>Run</C> is every entry point's <C>main</C>. It loads the configuration, composes
						the platform with the services, checks the graph before anything is built, and runs
						until a signal arrives or a worker fails. <C>Compose</C> is the part the tests use.
					</p>
					{f.run}
					<p>
						Each module declares what it provides and what its constructors need, so the
						container can report a composition without building any of it. This is{' '}
						<C>cmd/local</C>'s, as the tests compose it. Another test checks that each entry
						point's <C>main</C> passes the modules the tests compose.
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
						<C>cmd/local</C> composes every service and needs no configuration. With no
						placement, every service runs on this node, called <C>shop</C>. It is how a developer
						runs the shop.
					</p>
					{f.local}
					{f.localPicture}
					<p>
						The node's supervision tree is the root, a supervisor per service, and each service's
						process, in the order the modules were composed. A test starts <C>cmd/local</C>'s
						composition and draws this from the processes' parents:
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
						The distributed deployment splits the same modules between three entry points. The
						front takes the orders and serves HTTP, the warehouse keeps the stock, and billing
						takes the payments. Each <C>main</C> is one line.
					</p>
					{f.front}
					{f.warehouse}
					{f.billing}
					<p>
						What each program is told differs. The front's placement puts the inventory on{' '}
						<C>warehouse</C> and the payments on <C>billing</C>, so the desk's addresses name
						those nodes, and nothing in the desk changed. The warehouse and billing are told the
						front's address although they never call it, because a reply travels back to the
						caller's node on the replier's own link. A placement naming a node the peers do not
						include is refused at start.
					</p>
					{f.clusterPicture}
					<p>Each node's tree holds only the services its entry point composed:</p>
					{f.clusterTree}
					<p>
						Here the peers are a static list. A link that breaks fires a <C>Down</C> for every
						process monitored across it and fails the calls waiting on it, and keepalive makes a
						silent peer break its links too. For a cluster whose nodes come and go,{' '}
						<Link href={`${REPO}/tree/main/etcd`}>
							<C>grpcproc/etcd</C>
						</Link>{' '}
						publishes each node under an etcd lease and resolves the others through it. The
						platform then takes its resolver, and the node its registrar and membership, from
						there. The services do not change.
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
						A service is tested alone by putting fakes behind the addresses it calls: processes
						registered under the names the contracts give, on the node the placement points at,
						which with no placement is this one. <C>di.Test</C> gives a scope that is stopped when
						the test ends, and the test composes the platform and the orders module in it.
					</p>
					{f.ordersTest}
					<p>
						The deployments are tested whole. Each node is composed as its entry point composes
						it, and told what a deployment would tell it: its peers, and where the services it
						does not run are. The test binds the three ports first, so that it knows them in
						advance, and hands each node its own with an override, which di requires to be
						marked as one. It places orders through the front, pins what each node runs, and
						stops billing to see the front answer 503.
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
						Every node serves the Inspector, and an Inspector asked about another node forwards
						the question to that node's, wherever its resolver finds it. So{' '}
						<C>grpcprocctl</C> pointed at one node can ask about any. <C>nodes</C> and{' '}
						<C>dot --cluster</C> list the nodes it is linked to, which for the front, once it has
						taken an order, is all three: their links, their processes, and who started whom.{' '}
						<C>inspect</C> shows what a process says about itself; a supervisor lists its
						children and restarts. The platform's Inspector is read-only, so <C>exit</C> and{' '}
						<C>loglevel</C> are refused.
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
