// The drawing for "Nodes and the cluster": two nodes, a stream each way,
// and the envelopes on one of them in the order they travel.
import { Box, Diagram, Frame, Line, Note } from './diagrams.tsx';

/** Two nodes linked by one gRPC stream in each direction. */
export function Links() {
	const id = 'links';
	return (
		<Diagram id={id} width={720} height={250} label="Two nodes, a gRPC stream from each to the other, and the envelopes on one stream in order">
			<Frame x={16} y={16} w={220} h={200} title="node a" />
			<Box x={56} y={90} w={140} h={44} title="stock" sub="a process" kind="actor" />

			<Frame x={484} y={16} w={220} h={200} title="node b" />
			<Box x={524} y={90} w={140} h={44} title="desk" sub="a process" kind="actor" />

			<Line id={id} d="M236,80 L484,80" />
			<Note x={360} y={66} anchor="middle">
				a → b: the stream a opened
			</Note>
			<Box x={262} y={90} w={48} h={24} title="down" kind="outside" />
			<Box x={318} y={90} w={48} h={24} title="call" kind="outside" />
			<Box x={374} y={90} w={48} h={24} title="send" kind="outside" />
			<Note x={360} y={134} anchor="middle">
				in order: the send, then the call, then the down
			</Note>

			<Line id={id} d="M484,170 L236,170" />
			<Note x={360} y={196} anchor="middle">
				b → a: the stream b opened, with b's sends and its replies
			</Note>
		</Diagram>
	);
}
