import actors from '../../../examples/actors/main.go?raw';
import quickstart from '../../../examples/quickstart/main.go?raw';

import { Code, region } from '../code.tsx';
import { A, Aside, C, Table } from '../components/prose.tsx';
import type { Doc } from './types.ts';

export const conceptsProcesses: Doc = {
	path: 'concepts/processes/',
	title: 'Processes and messages',
	description:
		'What a process can do, message by message: spawning, receiving, sending and calling, replying, timers, its context, its options, and how it ends.',
	lead: (
		<p>
			A process is a function <C>func(p *grpcproc.Process[M]) error</C>, run on its own goroutine until
			it returns. <C>M</C> is the type of message its mailbox holds. Everything below is a method of{' '}
			<C>p</C>, or an option to the spawn that started it.
		</p>
	),
	sections: [
		{
			id: 'spawn',
			title: 'Spawning',
			body: (
				<>
					<p>
						<C>Node.Spawn</C> starts a process from outside any process, and returns its typed
						address. A process starts another with <C>p.Spawn</C>, which records <C>p</C> as the
						child's parent; the parent is for inspection, and neither exits when the other does
						unless the spawn asks for a link.
					</p>
					<Code>{`addr, err := node.Spawn(inventory, grpcproc.WithName("stock"))

// From inside a process: the child's parent is p.
child, err := p.Spawn(worker)

// Monitored from before it runs, so no exit is missed: Erlang's spawn_monitor.
child, ref, err := p.SpawnMonitor(worker)`}</Code>
					<p>
						<C>SpawnMonitor</C> places the monitor before the child's first line runs. A supervisor
						that monitored after spawning could miss a child that exits at once, and would then read
						its instant normal exit as <C>noproc</C>. Processes may be spawned before the node is
						started; they run immediately, and <C>Start</C> only makes the node known to its peers.
					</p>
				</>
			)
		},
		{
			id: 'receive',
			title: 'Receiving',
			body: (
				<>
					<p>
						<C>Receive</C> blocks until something arrives, and returns a <C>Msg[M]</C>. It holds one
						of three things: a <C>Body</C> of type <C>M</C>, a <C>Down</C> from a monitor, or an{' '}
						<C>Exited</C> from a link when the process traps exits. <C>From</C> is the sender's PID,{' '}
						<C>Metadata</C> what travelled with the message, and <C>IsCall</C> says whether the
						sender waits for a <C>Reply</C>.
					</p>
					<Code>{`for {
	m, err := p.Receive()
	if err != nil {
		return err // asked to exit, linked process gone, or the node stops
	}
	switch {
	case m.Down != nil:
		p.Log().Info("watched process gone", "pid", m.Down.PID, "reason", m.Down.Reason)
	case m.IsCall():
		_ = m.Reply(answer(m.Body), nil)
	default:
		apply(m.Body)
	}
}`}</Code>
					<p>
						The error is the process's signal to stop, and returning it is all the handling it
						needs: an <C>*ExitError</C> carrying the reason after <C>Exit</C> or a linked process's
						exit, <C>context.Canceled</C> when the node stops. <C>ReceiveTimeout</C> is the same with
						a deadline, and returns <C>context.DeadlineExceeded</C> when nothing came.
					</p>
					<p>
						Messages come out in the order they went in, per sender, and a <C>Down</C> comes after
						the last message of the process it reports on. The mailbox has no priorities and no
						selective receive; what a process cannot handle now, it keeps in a variable, as the
						actor in <A to="guides/actors/">Actors</A> does with reservations it cannot yet serve.
					</p>
				</>
			)
		},
		{
			id: 'type',
			title: 'The mailbox type',
			body: (
				<>
					<p>
						<C>M</C> must be a <C>proto.Message</C>, and it is one of three things:
					</p>
					<Table
						head={['M', 'Use']}
						rows={[
							[
								<C>*shoppb.Stock</C>,
								<>
									A generated message with a <C>oneof</C>: one contract per process, visible in the{' '}
									<C>.proto</C>, and the type every send through its address takes. The examples use
									this.
								</>
							],
							[
								<C>StockMsg</C>,
								<>
									Your own interface, <C>interface{'{ proto.Message; stockMsg() }'}</C>, implemented by
									several generated types with one extra method each. The address takes any of them
									as it is, <C>addr.Send(ctx, p, &amp;shoppb.Reserve{'{}'})</C>, with no wrapper.
								</>
							],
							[
								<C>proto.Message</C>,
								<>
									Anything at all: the untyped process, for one that expects only a <C>Down</C>, or
									takes what a tool sends it. <C>SendTo</C> and <C>CallTo</C> reach it.
								</>
							]
						]}
					/>
					<p>
						The type lives on the address, so a remote sender is checked by its compiler, and the
						receiving node checks again on delivery, because types do not cross the wire. A message
						of the wrong type never reaches the process: it is a dead letter with reason{' '}
						<C>type</C>, counted and reported, and a caller gets <C>ErrType</C>.
					</p>
				</>
			)
		},
		{
			id: 'send-call',
			title: 'Send and Call',
			body: (
				<>
					<p>
						<C>Send</C> queues a message and returns; nobody waits for an answer, and a process that
						does not exist is a dead letter, not an error. <C>Call</C> sends and waits for the reply,
						typed by what the caller asks for, under the context's deadline. Both are methods of the
						address, and take the sender: <C>p</C> from inside a process, or the node for code outside
						any.
					</p>
					<Code>{`err := stock.Send(ctx, p, &shoppb.Stock{Op: &shoppb.Stock_Restock{Restock: restock}})

r, err := stock.Call[*shoppb.Reserved](ctx, p, &shoppb.Stock{Op: &shoppb.Stock_Reserve{Reserve: reserve}})`}</Code>
					<p>
						The oneof and the reply type are the protocol; spelling them at every call is the job of
						the package that owns the process, not of its callers. It writes each operation once, as
						a method on an address type of its own, which takes the sender, <C>p</C> or the node, as
						an argument. The examples' <C>shoppb.StockAddr</C> does, and{' '}
						<A to="concepts/addressing/#protocol">an address with its protocol</A> shows how:
					</p>
					<Code>{`inventory := shoppb.StockAddr{Addr: stock}

err := inventory.Restock(ctx, p, restock)

r, err := inventory.Reserve(ctx, p, reserve)`}</Code>
					<p>
						A call is answered with the message's <C>Reply</C>: a message, or an error, which the
						caller gets back as a <C>*RemoteError</C>. The reply does not go through the caller's
						mailbox. A process that calls another waits on the reply alone, and the messages queued
						behind it stay in order. The message holds all a reply takes, so it can be kept and
						answered later, from any goroutine, or by another process it was handed to.
					</p>
					<Code caption="examples/actors/main.go">{region(actors, /^\/\/ HandleCall gets what was sent/, /^}/)}</Code>
					<p>
						The caller's deadline travels with the call. <C>m.Deadline()</C> says when the caller
						stops waiting, and <C>m.Context(parent)</C> gives a context that ends then, so work for
						a caller that gave up can stop, a deferred reply's above all. Between nodes it travels
						as the time left, as gRPC's does, so clocks need not agree, and counts from when the
						call arrives. It is not applied to the callee's own sends and calls unless it passes
						that context on, since a callee may have to finish what it started. Only the deadline
						travels, not the caller's cancellation.
					</p>
					<Code>{`ctx, cancel := m.Context(p.Context())
defer cancel()
rows, err := db.QueryContext(ctx, query) // ends when nobody waits for the answer`}</Code>
					<p>
						A callee that exits before answering fails the call with <C>ErrNoProc</C>; a peer that
						cannot be reached fails it with a <C>*LinkError</C>, which says whether the request ever
						left the node. <A to="reference/errors/">Errors and exit reasons</A> lists them all.
					</p>
				</>
			)
		},
		{
			id: 'timers',
			title: 'Timers, context and logging',
			body: (
				<>
					<p>
						<C>SendAfter</C> schedules a send from the process after a delay. The timer belongs to
						the process: it is cancelled if the process exits first, and <C>Stop</C> cancels it by
						hand. The message carries the metadata the process holds when the timer is set, not
						whatever it is handling when the timer fires.
					</p>
					<Code>{`t := p.SendAfter(30*time.Second, p.Addr(), &shoppb.Stock{Op: tick})
// …
if t.Stop() {
	// the tick had not fired yet
}`}</Code>
					<p>
						<C>p.Context()</C> is cancelled when the process is asked to exit, when a process it is
						linked to exits, or when the node stops. Hand it to whatever the process calls, a
						database or another process, so that a request to exit interrupts the work;{' '}
						<C>context.Cause</C> is then the <C>*ExitError</C>.
					</p>
					<p>
						<C>p.Log()</C> is a <C>*slog.Logger</C> with the process's PID and label attached. Its
						threshold can be changed at runtime, per process, with <C>Node.SetLogLevel</C> or from
						the <A to="guides/inspector/">Inspector</A>, which is how a noisy process is turned up
						in production without restarting anything.
					</p>
				</>
			)
		},
		{
			id: 'options',
			title: 'Spawn options',
			body: (
				<>
					<Table
						rows={[
							[
								<C>WithName(name)</C>,
								<>
									Registers the process under <C>name</C> on its node until it exits, so that{' '}
									<C>Named</C> reaches it. While another process holds the name, the spawn fails with{' '}
									<C>ErrNameTaken</C>.
								</>
							],
							[
								<C>WithLabel(label)</C>,
								<>
									A low-cardinality tag, the key that metrics aggregate by. It defaults to the type of{' '}
									<C>M</C>.
								</>
							],
							[
								<C>WithInspect(fn)</C>,
								<>
									What the process publishes about itself: a <C>map[string]string</C> the Inspector
									shows. <C>fn</C> runs on the process's own goroutine, between two messages, so it
									reads the process's state without a lock.
								</>
							],
							[
								<>
									<C>LinkParent()</C>, <C>LinkChild()</C>
								</>,
								<>
									Link the child to its parent, or the parent to the child, before the child runs.
									Both together are Erlang's <C>spawn_link</C>. Only for <C>p.Spawn</C> and{' '}
									<C>p.SpawnMonitor</C>: a <C>Node.Spawn</C> has no parent.
								</>
							],
							[
								<C>WatchedBy(m)</C>,
								<>
									Place the monitor or link the caller of the call <C>m</C> asked for on the child,
									before the child runs: the caller may be on another node. See{' '}
									<A to="concepts/monitors-and-links/#spawn-monitor">Monitoring from before the start</A>.
								</>
							]
						]}
					/>
				</>
			)
		},
		{
			id: 'exit',
			title: 'How a process ends',
			body: (
				<>
					<p>
						A process ends when its function returns, and it ends with a reason, a string that
						travels to whoever monitors it:
					</p>
					<ul>
						<li>
							<C>normal</C> for a <C>nil</C> return, the error's text for any other.
						</li>
						<li>
							<C>panic: …</C> for a panic, which is recovered, logged with its stack, and reported
							like an error. The node keeps running.
						</li>
						<li>
							What <C>Exit</C> asked for: a request to exit reaches the process as an error from{' '}
							<C>Receive</C>, and the reason is the one the request carried. The node stops its
							processes with <C>shutdown</C>, and the Inspector's <C>Exit</C> defaults to{' '}
							<C>killed</C>.
						</li>
						<li>
							The reason of a process it was linked to, when it does not trap exits.
						</li>
					</ul>
					<p>
						A process cannot be killed: a goroutine has no such operation. <C>Exit</C> is a request
						the process sees at its next <C>Receive</C>, and one that is busy in a handler ends when
						the handler returns. That is why long work takes <C>p.Context()</C>, and why a
						supervisor gives a child a shutdown timeout rather than a guarantee.
					</p>
					<Code caption="examples/quickstart/main.go">
						{region(quickstart, /A monitor across nodes works/, /fmt.Println\("stock exited:"/)}
					</Code>
				</>
			)
		},
		{
			id: 'delivery',
			title: 'What delivery guarantees',
			body: (
				<>
					<ul>
						<li>
							<strong>At most once.</strong> A message is delivered once or not at all, never twice.
							Nothing is retried, since a retry could deliver a message a second time; what failed
							is reported instead, and a <C>*LinkError</C> whose <C>Unsent</C> is set is the one
							case where sending again is known to be safe.
						</li>
						<li>
							<strong>In order per sender.</strong> Two messages from one process to another arrive
							in the order they were sent, on one node or across a link, and a <C>Down</C> arrives
							after the last message of the process it reports on. Calls travel on the same link as
							messages, so they cannot overtake or be overtaken.
						</li>
						<li>
							<strong>Unbounded mailboxes.</strong> A send never blocks on the receiver. All the
							processes of a node share its link to a peer, so one full mailbox would stall every
							other process behind it. The depth and the age of the oldest message are in every
							process's snapshot, for the application to act on.
						</li>
						<li>
							<strong>Local sends share the pointer.</strong> Within a node a message is not encoded,
							and the receiver gets the value the sender built; the sender must not change it after
							sending. <C>Config.CopyLocal</C> clones every local message instead, for a team that
							wants the isolation of the wire everywhere.
						</li>
					</ul>
					<Aside title="Coming from Erlang">
						<p>
							The same guarantees, with one difference in kind: an Erlang exit signal can be trapped,
							and grpcproc's <C>Exit</C> cannot, because the goroutine behind the process cannot be
							killed if it refused. What a link delivers is trapped, as{' '}
							<A to="concepts/monitors-and-links/">Monitors and links</A> explains.
						</p>
					</Aside>
				</>
			)
		}
	]
};
