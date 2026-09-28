import actors from '../../../examples/actors/main.go?raw';
import actorsOutput from '../../../examples/actors/output.txt?raw';

import { Code, Output, region } from '../code.tsx';
import { A, Aside, C, Ext, Table } from '../components/prose.tsx';
import { file } from '../config.ts';
import type { Doc } from './types.ts';

export const guidesActors: Doc = {
	path: 'guides/actors/',
	title: 'Actors',
	description:
		'The grpcproc/actor package: a struct with a method per kind of message in place of a receive loop, built by a constructor and run as a process.',
	lead: (
		<p>
			<C>grpcproc/actor</C> is optional structure on top of processes, built on the public API only.
			This page is about its handler loop; <A to="guides/supervisors/">Supervisors</A> covers the
			other half. The code is from{' '}
			<Ext href={file('examples/actors/main.go')}>examples/actors</Ext>, whole and runnable.
		</p>
	),
	sections: [
		{
			id: 'why',
			title: 'Why not a receive loop',
			body: (
				<>
					<p>
						A process is a function over a mailbox, and the function is a loop: receive, switch on
						what came, reply or not. That is the right shape for a process that does one thing.
						For a service it grows old. The switch gets a case per operation, a case for{' '}
						<C>Down</C>, another for a call that must be answered later, and the state the loop
						closes over is reachable from nothing but the loop.
					</p>
					<p>
						An actor is that loop turned inside out. It is a struct holding its dependencies and its
						state, with a method per kind of message: one for messages, one for calls, one for the
						exit of something it watches. The struct is built by a constructor, which is what a DI
						container calls, and the loop is <C>actor.Run</C>, which is the same for every actor.
					</p>
					<Code>{`type Orders struct{ repo *Repo }

func NewOrders(repo *Repo) *Orders { return &Orders{repo: repo} }

func (o *Orders) HandleMessage(p *grpcproc.Process[*orderspb.Order], m grpcproc.Msg[*orderspb.Order]) error {
	return o.repo.Save(p.Context(), m.Body)
}

addr, err := node.Spawn(actor.Run(NewOrders(repo)), grpcproc.WithName("orders"))`}</Code>
					<p>
						<C>Run</C> turns the handler into a process function, the one <C>Spawn</C> takes, so an
						actor is spawned, named, monitored and addressed like any process. Nothing about it is
						visible from outside.
					</p>
				</>
			)
		},
		{
			id: 'handlers',
			title: 'The handler and its methods',
			body: (
				<>
					<p>
						One method is required, <C>HandleMessage</C>: it is the <C>Handler[M]</C> interface. The
						rest are optional interfaces that <C>Run</C> looks for once, by type assertion, when the
						actor starts. An actor implements the ones it needs and no others.
					</p>
					<Table
						head={['Method', 'Runs for, and what its result means']}
						rows={[
							[
								<C>Init(p) error</C>,
								<>
									Once, on the actor's goroutine, before the first message. An error ends the actor
									before it handles anything, and <C>Terminate</C> does not run.
								</>
							],
							[
								<C>HandleMessage(p, m) error</C>,
								<>
									A message sent with <C>Send</C>. An error ends the actor with the error as its
									exit reason; <C>actor.ErrStop</C> ends it normally.
								</>
							],
							[
								<C>HandleCall(p, m) (proto.Message, error)</C>,
								<>
									A message sent with <C>Call</C>. What it returns, a message or an error, is the
									reply, and the actor carries on either way. <C>actor.ErrNoReply</C> means the
									answer comes later, through <C>m.Reply</C>. Without this method, every call is
									answered with an error.
								</>
							],
							[
								<C>HandleDown(p, d) error</C>,
								<>
									A <C>Down</C> from a process the actor monitors. As for <C>HandleMessage</C>.
									Without it, Downs are ignored.
								</>
							],
							[
								<C>HandleExited(p, e) error</C>,
								<>
									An <C>Exited</C> from a process the actor is linked to, when it traps exits. As
									for <C>HandleMessage</C>. One from the actor's parent ends the actor instead,
									whatever this method would say.
								</>
							],
							[
								<C>Terminate(p, err)</C>,
								<>
									Once, when the actor ends, however it ends. The exit reason is already decided:{' '}
									<C>err</C> is nil after <C>ErrStop</C>, the handler's error, an{' '}
									<C>*ExitError</C> after <C>Exit</C>, or <C>context.Canceled</C> when the node
									stops.
								</>
							]
						]}
					/>
					<p>
						The method names are the interfaces' names shifted: <C>Initializer</C>,{' '}
						<C>CallHandler</C>, <C>DownHandler</C>, <C>ExitedHandler</C> and <C>Terminator</C>, each
						with one method. A test can assert an actor satisfies one with a variable of the
						interface type.
					</p>
					<p>
						All of them run on the actor's goroutine, one at a time, in the order the messages
						arrived. An actor handles one message at a time, so its fields need no lock, and a call
						it makes from a handler holds the actor until the reply comes. The next message waits in
						the mailbox meanwhile.
					</p>
					<Aside title="Coming from Erlang">
						<p>
							This is <C>gen_server</C> with the callbacks named after what they handle:{' '}
							<C>handle_cast</C> is <C>HandleMessage</C>, <C>handle_call</C> is <C>HandleCall</C>,
							and <C>handle_info</C> is split into <C>HandleDown</C> and <C>HandleExited</C>,
							since those are the only system messages a process gets. There is no state
							argument: the state is the struct.
						</p>
					</Aside>
				</>
			)
		},
		{
			id: 'errors',
			title: 'An error is a reply, or a reason',
			body: (
				<>
					<p>
						The same Go error means two different things in two handlers, and the difference is the
						point of the package. From <C>HandleCall</C>, an error is the answer: the caller gets it
						as a <C>*RemoteError</C>, and the actor goes on to the next message. A refused
						reservation, a bad quantity, an item that does not exist: none of these is the actor's
						failure, and none should end it.
					</p>
					<p>
						From <C>HandleMessage</C>, <C>HandleDown</C> and <C>HandleExited</C>, an error is the
						exit reason. Nobody waits for an answer to a send, so there is nobody to give the error
						to but the actor's supervisor, which sees it as a crash and starts a fresh actor. That
						is the reason to keep what must survive outside the actor, in the dependency the
						constructor takes, and to load it in <C>Init</C>.
					</p>
					<p>
						Two sentinel errors change the rule. <C>actor.ErrStop</C> from any handler ends the
						actor with reason <C>normal</C>; from <C>HandleCall</C>, the reply is sent first, so a
						caller that asked the actor to stop gets its answer. <C>actor.ErrNoReply</C> from{' '}
						<C>HandleCall</C> sends nothing: the actor keeps the message and answers later with{' '}
						<C>m.Reply(resp, err)</C>, from a later handler or from any goroutine. The caller waits
						until then, or until its context ends.
					</p>
				</>
			)
		},
		{
			id: 'calls-only',
			title: 'An actor that only answers calls',
			body: (
				<>
					<p>
						Many actors are pure request and reply: a pricer, a cashier, a lookup. They have no
						use for <C>HandleMessage</C>, but the interface requires it. <C>actor.CallsOnly[M]</C>,
						embedded by value, provides one: a message sent without a call is logged at Warn and
						dropped, and the actor carries on.
					</p>
					<Code>{`type Pricer struct {
	actor.CallsOnly[*pricespb.Quote]
	table *Table
}

func (pr *Pricer) HandleCall(p *grpcproc.Process[*pricespb.Quote], m grpcproc.Msg[*pricespb.Quote]) (proto.Message, error) {
	return &pricespb.Price{Cents: pr.table.Lookup(m.Body.Sku)}, nil
}

addr, err := node.Spawn(actor.Run(&Pricer{table: table}))`}</Code>
					<p>
						Dropping rather than crashing is deliberate. The message was delivered, so it is not a
						dead letter; but a stray sender on another node is not a reason to end this actor, as{' '}
						<C>gen_server</C>'s default <C>handle_info</C> decides too. The log line names the
						sender and the type, which is what finding the stray needs.
					</p>
					<p>
						<C>Run</C> panics for an actor that embeds <C>CallsOnly</C> and has no{' '}
						<C>HandleCall</C>. The usual way to get there is to spawn <C>Pricer{'{}'}</C> where{' '}
						<C>HandleCall</C> has a pointer receiver: the value has the embedded method and not the
						real one. The panic happens at spawn, with a message saying so, rather than at the first
						call, with an error nobody reads.
					</p>
				</>
			)
		},
		{
			id: 'example',
			title: 'An inventory, walked through',
			body: (
				<>
					<p>
						The example's actor holds a stock and records reservations in a <C>Ledger</C>, its one
						dependency. Its mailbox holds <C>*shoppb.Stock</C>, a oneof of a reservation and a
						restock. <C>Init</C> runs on the actor's goroutine before the first message:
					</p>
					<Code caption="examples/actors/main.go">{region(actors, /^\/\/ Inventory is an actor/, /^}/) + '\n\n' + region(actors, /^\/\/ Init runs on the actor/, /^}/)}</Code>
					<p>
						A reservation is a call. When there is not enough stock, the actor parks the message
						and returns <C>ErrNoReply</C>; the caller waits, and gets its answer when a restock
						arrives:
					</p>
					<Code caption="examples/actors/main.go">{region(actors, /^\/\/ HandleCall gets what was sent/, /^}/)}</Code>
					<p>
						A restock is a send. <C>HandleMessage</C> adds the stock and answers the parked calls
						it now can, each with its own <C>Reply</C>. A reservation that arrives as a send breaks the
						protocol, and the returned error ends the actor:
					</p>
					<Code caption="examples/actors/main.go">{region(actors, /^\/\/ HandleMessage gets what was sent/, /^}/)}</Code>
					<p>
						<C>Terminate</C> runs however the actor ends. The calls it never answered need no
						handling here: a caller whose callee exits gets <C>ErrNoProc</C> on its own.
					</p>
					<Code caption="examples/actors/main.go">{region(actors, /^\/\/ Terminate runs however/, /^}/)}</Code>
					<p>
						The program spawns the actor under a name, parks a reservation until a restock, gets a
						refusal back as an error, then breaks the protocol on purpose:
					</p>
					<Code caption="examples/actors/main.go">{region(actors, /Nothing is in stock, so this call/, /fmt.Println\("then:", err\)/)}</Code>
					<Output>{actorsOutput}</Output>
					<p>
						The last line is the reservation queued behind the bad restock: the actor had exited by
						the time it was next in the mailbox, so the call failed with <C>ErrNoProc</C>. Under a
						supervisor, a new actor would be running by then, under the same name, with a fresh
						state; <A to="guides/supervisors/">Supervisors</A> shows that.
					</p>
				</>
			)
		},
		{
			id: 'parent',
			title: 'An actor and its parent',
			body: (
				<>
					<p>
						An actor spawned from another process with <C>p.Spawn</C> records that process as its
						parent, and a supervisor spawns its children with <C>LinkParent</C>: the child is linked
						to the supervisor, one way, so when the supervisor exits the child does too. That link
						can be trapped, as any link can, with <C>p.SetTrapExit(true)</C> in <C>Init</C>, after
						which a linked process's exit arrives as an <C>Exited</C> message.
					</p>
					<p>
						With one exception. An <C>Exited</C> from the actor's own parent ends the actor all
						the same, with the parent's reason, before <C>HandleExited</C> sees it. A{' '}
						<C>gen_server</C> ends when its parent does, whatever it traps, and an actor here does
						too: a subtree whose supervisor is gone must not go on running under nobody. That rule
						is in <C>actor.Run</C>, not in the core; a plain process that traps exits decides for
						itself. <A to="concepts/monitors-and-links/">Monitors and links</A> has the details.
					</p>
				</>
			)
		},
		{
			id: 'testing',
			title: 'Testing an actor alone',
			body: (
				<>
					<p>
						An actor is a process, so a test spawns it on a node and talks to it. A node with no
						peers needs only a name and an empty resolver, and stops when the test ends:
					</p>
					<Code>{`func TestInventoryParksAReservation(t *testing.T) {
	node, err := grpcproc.NewNode(grpcproc.Config{Name: "test", Resolver: grpcproc.StaticResolver{}})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = node.Stop(context.Background()) })

	inv, err := node.Spawn(actor.Run(&Inventory{ledger: &Ledger{}}))
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 100*time.Millisecond)
	defer cancel()
	if _, err := node.Call[*shoppb.Reserved](ctx, inv, reserve("apple", 1)); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("a reservation with no stock should wait, got %v", err)
	}
}`}</Code>
					<p>
						An actor that calls other actors takes their addresses as dependencies, and a test puts
						fakes behind them: plain process functions registered under the same names.{' '}
						<A to="guides/testing/">Testing</A> shows that with the tutorial's orders desk, and{' '}
						<C>grpcproctest</C> for anything that needs more than one node.
					</p>
				</>
			)
		}
	]
};
