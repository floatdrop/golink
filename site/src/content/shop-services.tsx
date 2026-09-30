import inventory from '../../../examples/guide/internal/inventory/inventory.go?raw';
import orders from '../../../examples/guide/internal/orders/orders.go?raw';
import payments from '../../../examples/guide/internal/payments/payments.go?raw';
import web from '../../../examples/guide/internal/web/web.go?raw';

import { Code } from '../code.tsx';
import { Drawing } from '../components/Figure.tsx';
import { Flow } from '../components/diagrams.tsx';
import { A, C } from '../components/prose.tsx';
import type { Doc } from './types.ts';

export const shopServices: Doc = {
	path: 'shop/services/',
	title: 'Services',
	description:
		'The three services of the shop as actors: the stock, which keeps its levels in a store; the cashier, which only answers calls; the desk, which calls both; and the HTTP front, which is not a process at all.',
	lead: (
		<p>
			<A to="shop/">The previous chapter</A> laid out the shape of the application and the contracts
			its services import. This one writes the services themselves, one module each.
		</p>
	),
	sections: [
		{
			id: 'service',
			title: 'A service is a module',
			body: (
				<>
					<p>
						The stock is an actor: a struct holding its dependencies, with a method per kind of
						message. <C>Init</C> runs before the first message. <C>HandleCall</C> answers
						reservations, with a refusal in the answer or, when the store fails, an error; the
						process carries on either way. <C>HandleMessage</C> takes releases. A reservation sent
						without a call is the sender's bug: it is logged and dropped, since bad input from
						another service is no reason to crash.
					</p>
					<p>
						The levels live in a <C>Store</C>, not in the actor. When the store fails on a release
						the process crashes, and its supervisor starts a new one; <C>actor.Child</C> builds a
						fresh actor at every start, so it begins from what the store says, not from whatever
						crashed. The store is an interface the container serves: the tutorial's keeps the levels
						in memory, and a database would be built, started and stopped the same way, with nothing
						else changing.
					</p>
					<p>
						<C>Module</C> registers the store and the service's tree. The tree is not started here:
						it joins the group of <C>actor.ChildSpec</C> the platform builds the root supervisor
						from, so a program runs whichever services its entry point composes, and nothing keeps
						a list of them.
					</p>
					<Code caption="internal/inventory/inventory.go">{inventory}</Code>
				</>
			)
		},
		{
			id: 'calls',
			title: 'A service that only answers',
			body: (
				<>
					<p>
						The payments service's cashier answers calls and nothing else. It embeds{' '}
						<C>actor.CallsOnly</C> as its <C>HandleMessage</C>: a plain send to it is logged and
						dropped. Its <C>Gateway</C>, the payment provider's, comes from the container as the
						store did. A declined card is part of the answer; any other gateway error is a failure,
						after which the card may or may not have been charged. The tutorial's gateway is a
						sandbox that declines cards starting with 4000.
					</p>
					<Code caption="internal/payments/payments.go">{payments}</Code>
				</>
			)
		},
		{
			id: 'orders',
			title: 'Calling other services',
			body: (
				<>
					<p>
						The desk takes an order: it reserves the items, charges for them and answers with the
						receipt. The stock and the cashier belong to other services, perhaps on other nodes; the
						desk calls them the same way wherever they run: through the methods of their addresses,
						as its process <C>p</C>, under a deadline.
					</p>
					<p>
						A refusal from either is passed on in the desk's answer, and after a declined card the
						items are released, with a send, since nothing waits for it. An error from either, no
						answer or a failure, means what happened is not known: the items may be reserved, the
						card may be charged. The desk then leaves things as they are, logs the order for
						reconciliation, and fails the call.
					</p>
					<p>
						The addresses are made once, in <C>tree</C>, from the placement; every desk the
						supervisor starts gets the same ones. A desk handles one order at a time: while it waits
						on the stock and the cashier, the next order waits in its mailbox. A desk that must not
						wait would return <C>actor.ErrNoReply</C> and answer from another goroutine.
					</p>
					<Code caption="internal/orders/orders.go">{orders}</Code>
					<Drawing caption="one order">
						<Flow />
					</Drawing>
				</>
			)
		},
		{
			id: 'edge',
			title: 'The edge',
			body: (
				<>
					<p>
						The web front is not a process. An HTTP handler calls the desk through the node, as any
						code outside a process does: the node is the sender it passes to the desk's{' '}
						<C>Place</C>. The call gets the request's context, so a client that hangs up stops the
						wait, not the order: the desk still takes it.
					</p>
					<p>
						A refusal becomes a 422 with the reason; an error a 503, whether the desk could not be
						reached or could not settle the order. What went wrong between the nodes goes to the
						log, not to the client. The server is registered with the container with hooks: it
						listens when it starts and drains before anything else stops, so an order in flight
						still finds the desk.
					</p>
					<Code caption="internal/web/web.go">{web}</Code>
				</>
			)
		}
	]
};
