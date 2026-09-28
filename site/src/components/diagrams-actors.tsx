// The drawing on the actor model page: two nodes, three processes with their
// mailboxes, and two calls that look the same whether or not they cross a
// node boundary. Drawn with the primitives of diagrams.tsx.
import { Box, Diagram, Frame, Line, Note } from './diagrams.tsx';

/** A process: its box, and the mailbox queued in front of it. */
function Proc({ x, y, title, sub, depth }: { x: number; y: number; title: string; sub: string; depth: number }) {
	// Four slots; the first `depth` hold a message. A slot is drawn with the
	// classes the other drawings use, so it follows the theme like they do.
	const cells = [];
	for (let i = 0; i < 4; i++) {
		cells.push(
			<g key={i} className={i < depth ? 'gp-d-box gp-d-box_contract' : 'gp-d-box gp-d-box_outside'}>
				<rect x={x - 60 + i * 14} y={y + 11} width={11} height={14} rx={2} />
			</g>
		);
	}
	return (
		<g>
			{cells}
			<Note x={x - 60} y={y + 38}>
				mailbox
			</Note>
			<Box x={x} y={y} w={116} h={36} title={title} sub={sub} kind="actor" />
		</g>
	);
}

export function ActorsPicture() {
	const id = 'actors';
	return (
		<Diagram id={id} width={720} height={250} label="Two nodes; a call from one process to another looks the same within a node and across the link between nodes">
			<Frame x={8} y={8} w={400} h={232} title="node shop" />
			<Proc x={110} y={60} title="desk" sub="orders" depth={1} />
			<Proc x={270} y={160} title="cashier" sub="payments" depth={2} />
			<Line id={id} d="M168,96 L168,128 L200,128 L200,171 L206,171" />
			<Note x={72} y={140}>
				p.Call(cashier, charge)
			</Note>

			<Frame x={448} y={8} w={264} h={232} title="node warehouse" />
			<Proc x={556} y={110} title="stock" sub="inventory" depth={3} />
			<Line id={id} d="M226,78 L470,78 L470,121 L492,121" />
			<Note x={290} y={70}>
				p.Call(stock, reserve)
			</Note>
			<Line id={id} kind="dashed" end={false} d="M408,200 L448,200" />
			<Note x={428} y={220} anchor="middle">
				gRPC link
			</Note>
		</Diagram>
	);
}
