import inventoryAddress from '../../../examples/guide/proto/inventory/v1/address.go?raw';
import quickstart from '../../../examples/quickstart/main.go?raw';

import { Code, region } from '../code.tsx';
import { A, Aside, C, Ext, Table } from '../components/prose.tsx';
import { file } from '../config.ts';
import type { Doc } from './types.ts';

export const conceptsAddressing: Doc = {
	path: 'concepts/addressing/',
	title: 'PIDs, names and addresses',
	description:
		'How a process is named: PIDs and their incarnation, registered names, typed addresses, what a message can be sent to, dead letters, and the metadata that travels with every message.',
	lead: (
		<p>
			A message goes to an address, never to a process value. An address is small, comparable, and means
			the same on every node, which is what lets a process be reached from anywhere and stored inside
			a message. There are two kinds, and a typed wrapper over both.
		</p>
	),
	sections: [
		{
			id: 'pid',
			title: 'PID',
			body: (
				<>
					<p>
						A <C>PID</C> identifies one process, anywhere in the cluster, for the whole of its life:
					</p>
					<Code>{`type PID struct {
	Node        string // the node it runs on
	Incarnation uint64 // that node's start
	ID          uint64 // the process, on that start
}`}</Code>
					<p>
						It prints as <C>{'<shop.1718.4>'}</C>. <C>Spawn</C> returns its address, <C>p.PID()</C> is
						the process's own, and <C>Msg.From</C> is the sender's. A PID is a value: it can be
						compared, used as a map key, and put inside a protobuf message to tell another process
						whom to answer.
					</p>
					<p>
						The incarnation is why a PID is safe to keep. A node that restarts numbers its processes
						from one again, so without it the PID of a process from before the restart would point
						at whichever process now has that number. With it, the PID names a process that no longer
						exists: a send to it is a dead letter, a call fails with <C>ErrNoProc</C>, and a monitor
						on it fires <C>Down{'{noproc}'}</C>. A message is never delivered to a stranger. By
						default the incarnation is the node's start time; <A to="concepts/nodes/">Nodes</A> says
						what else it fences.
					</p>
					<Aside title="Coming from Erlang">
						<p>
							The incarnation is the creation number of a pid, kept for the same reason. It is a
							field rather than a hidden part of the value, and it grows with each start rather than
							counting modulo four, so a peer can tell which of two incarnations is the newer.
						</p>
					</Aside>
				</>
			)
		},
		{
			id: 'name',
			title: 'Name',
			body: (
				<>
					<p>
						A process may register a name on its node when it is spawned, and hold it until it
						exits. A <C>Name</C> is the node and that name; it prints as <C>{'{stock@warehouse}'}</C>.
					</p>
					<Code>{`_, err := warehouse.Spawn(inventory, grpcproc.WithName("stock"))
// ErrNameTaken while another process holds the name.

pid, ok := warehouse.Whereis("stock") // on this node, now`}</Code>
					<p>
						Names are per node. Two nodes can each have a <C>stock</C>, and nothing but a node name
						tells them apart; that is a feature, since it is what lets the same service run on every
						node of the <A to="shop/">tutorial</A>. A name is freed when its process exits and taken by
						whatever is spawned under it next, so an address by name outlives any one process: a
						supervisor restarts a child under the same name and its callers notice nothing but the
						gap. A message sent by name while nobody holds it is a dead letter, and a monitor placed
						by name fires at once with <C>noproc</C>.
					</p>
					<p>
						There is no cluster-wide registry. A process is found by knowing which node it is on,
						from configuration or from a message that carried its PID. A global registry with a
						fencing token is listed under later work in the{' '}
						<Ext href={file('docs/DESIGN.md')}>design notes</Ext>; Erlang keeps <C>global</C> apart
						from local registration for the same reason.
					</p>
				</>
			)
		},
		{
			id: 'addr',
			title: 'Addr[M] and Target',
			body: (
				<>
					<p>
						An <C>Addr[M]</C> is a PID or a Name together with the message type the process behind
						it accepts. It is what every typed send and call takes, and it comes from three places:
					</p>
					<Table
						rows={[
							[
								<C>Spawn</C>,
								<>
									Returns the new process's address, typed by the function it was given. <C>p.Addr()</C>{' '}
									is the process's own.
								</>
							],
							[
								<C>Named[M](node, name)</C>,
								<>
									An address by name, with the type the caller asserts. This is how one service
									reaches another: the name and the type are the contract, exported by the package
									that owns the process.
								</>
							],
							[
								<C>AddrOf[M](target)</C>,
								<>
									Types an untyped target: a PID that came in a message, say. Nothing checks the
									assertion until a message is delivered.
								</>
							]
						]}
					/>
					<Code caption="examples/quickstart/main.go">
						{region(quickstart, /From shop it is a node name/, /stock := grpcproc.Named/)}
					</Code>
					<p>
						A <C>Target</C> is anything a message can go to: a <C>PID</C>, a <C>Name</C>, or any{' '}
						<C>Addr</C>. <C>Monitor</C>, <C>Link</C> and <C>Exit</C> take a target, since they do not
						care about the mailbox's type; <C>SendTo</C> and <C>CallTo</C> take one too, and the type
						is checked on delivery only.
					</p>
				</>
			)
		},
		{
			id: 'protocol',
			title: 'An address with its protocol',
			body: (
				<>
					<p>
						A typed call names its reply, <C>node.Call[*shoppb.Reserved](ctx, stock, req)</C>, and
						every call site does, wrapping the request in the mailbox's oneof when it has one. The
						package that owns a process can do both once. An address has <C>Call</C> and{' '}
						<C>Send</C> of its own, which take the sender as an argument, a <C>Caller</C>: the{' '}
						<C>*Node</C>, or a <C>*Process</C> from inside a handler, as <C>Node.Call</C> or{' '}
						<C>Process.Call</C> would send it. A contract wraps them in a method per operation, on an
						address type of its own:
					</p>
					<Code caption="examples/guide/proto/inventory/v1/address.go">
						{`${region(inventoryAddress, /^\/\/ StockAddr addresses/, /^type StockAddr/)}

${region(inventoryAddress, /^\/\/ Reserve takes items/, /^}/)}`}
					</Code>
					<p>
						A caller then writes <C>stock.Reserve(ctx, p, req)</C> from a handler, or{' '}
						<C>stock.Reserve(ctx, node, req)</C> from anywhere else, and names neither the reply nor
						the oneof. The sender is an argument rather than part of the address because who sends
						matters: a process's call carries the metadata of the message it is handling, and an
						actor keeps its addresses in fields but has its process only inside a handler. The type
						embeds <C>Addr</C>, so it is still a <C>Target</C>, and its raw <C>Call</C> and{' '}
						<C>Send</C> are still there: the methods make the protocol the easy way, not the only
						one. <A to="shop/#contracts">The shop</A>'s contracts are written this way.
					</p>
				</>
			)
		},
		{
			id: 'types',
			title: 'Types stop at the wire',
			body: (
				<>
					<p>
						The type on an address is the sender's claim. On the sending node the compiler enforces
						it: a <C>Send</C> to an <C>Addr[*shoppb.Reserve]</C> takes a <C>*shoppb.Reserve</C> and
						nothing else. But the claim does not travel. On the wire a message is its type's full
						name and its encoding, and the receiving node decodes it and checks, on delivery, that
						the process's mailbox accepts it.
					</p>
					<p>
						So a remote sender can be wrong, by naming the right process with the wrong type, and the
						process is protected all the same. The message never enters the mailbox: it is a dead
						letter with reason <C>type</C>, a caller gets <C>ErrType</C>, and the process's own code
						never sees a value of a type it did not declare. A local send skips the check, since the
						compiler already made it.
					</p>
				</>
			)
		},
		{
			id: 'dead-letters',
			title: 'Dead letters',
			body: (
				<>
					<p>
						A message that cannot be delivered is a dead letter: there is no process at the PID,
						nobody holds the name, or the type is wrong. For a <C>Send</C> that is not an error. The
						sender did not wait for anything, and a process that exited a moment ago is the ordinary
						case, not a fault in the sender; the node counts the dead letter, reports it to{' '}
						<C>Hooks.OnDeadLetter</C> and to subscribers of its events, and moves on.
					</p>
					<p>
						A <C>Call</C> is different, because the caller waits. It fails with <C>ErrNoProc</C>{' '}
						when there is no such process, or when the process exits before answering, and with{' '}
						<C>ErrType</C> when the process does not take the request or the reply is not what the
						caller asked for. What a send can return an error for is only the node: the message
						could not be encoded, the peer could not be reached, or the context ended while waiting
						for a first connection to it.
					</p>
					<p>
						<A to="guides/observability/">Observability</A> shows where dead letters are counted and
						watched; a steady trickle of them under one name is usually a caller with a stale
						configuration.
					</p>
				</>
			)
		},
		{
			id: 'metadata',
			title: 'Metadata',
			body: (
				<>
					<p>
						Every message carries a <C>Metadata</C>, a <C>map[string]string</C> that grpcproc passes
						along and never reads: trace context, a tenant, a request id, whatever the application's
						interceptors would carry. From outside a process it is set on the context:
					</p>
					<Code>{`ctx = grpcproc.WithMetadata(ctx, grpcproc.Metadata{"tenant": "acme"})
r, err := node.Call[*shoppb.Reserved](ctx, stock, reserve)

md := grpcproc.MetadataFrom(ctx) // reads it back`}</Code>
					<p>
						Inside a process it needs no context at all. A process remembers the metadata of the
						message it is handling, and every <C>Send</C>, <C>Call</C> and <C>SendAfter</C> it makes
						meanwhile inherits it. A request that enters at the edge with a trace id leaves the same
						id on every message its handling causes, across processes and nodes, without a single
						handler threading a context through. A call's context can add to what is inherited, and{' '}
						<C>m.Context(parent)</C> puts a message's metadata into a context for code that wants
						one, a database client for instance. For a call, that context also ends at the caller's
						deadline; <A to="concepts/processes/#send-call">Send and Call</A> has how it travels.
					</p>
					<p>
						This is what <A to="guides/observability/">grpcproc/otel</A> builds its traces on: a span
						opened when a message is taken, and every message sent while handling it a child of that
						span.
					</p>
				</>
			)
		},
		{
			id: 'node-pid',
			title: "The node's own PID",
			body: (
				<>
					<p>
						A message sent from outside any process, with <C>Node.Send</C> or <C>Node.Call</C>, still
						has a sender. It is the node's pseudo-process, <C>node.PID()</C>: the node's name and
						incarnation with ID zero. It has no mailbox, so a process cannot reply to it with a send;
						a call from it is answered through the call, as any other. It shows up in <C>Msg.From</C>,
						in dead letters and in traces, so that a message from the edge is as attributable as one
						from a process.
					</p>
				</>
			)
		}
	]
};
