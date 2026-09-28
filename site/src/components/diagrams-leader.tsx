// The drawing for "Leader election": the processes of a leader's part in an
// election, and of a follower's, with the primitives of diagrams.tsx.
import { Box, Diagram, Frame, Line, Note } from './diagrams.tsx';

/**
 * Two nodes of one election: on each a supervisor over the elector, which
 * has a relay per peer; on the leader, the supervisor runs the singleton
 * too. A heartbeat goes from the leader's relay to the follower's elector.
 */
export function Election() {
	const id = 'election';
	return (
		<Diagram id={id} width={720} height={272} label="A leader and a follower: each runs a supervisor over its elector and the elector's relays; only the leader runs the singleton">
			<Frame x={16} y={16} w={330} h={210} title="node a: the leader" />
			<Box x={106} y={44} w={150} h={28} title="supervisor" kind="supervisor" />
			<path className="gp-d-line gp-d-line_tree" d="M181,72 L181,89 L96,89 L96,106" />
			<path className="gp-d-line gp-d-line_tree" d="M181,89 L256,89 L256,104" />
			<Box x={36} y={106} w={120} h={30} title="singleton" kind="actor" />
			<Note x={96} y={158} anchor="middle">
				yours: it runs
			</Note>
			<Note x={96} y={174} anchor="middle">
				while a leads
			</Note>
			<Box x={196} y={104} w={120} title="elector" sub="leader/sched" kind="platform" />
			<path className="gp-d-line gp-d-line_tree" d="M256,140 L256,154 L222,154 L222,168" />
			<path className="gp-d-line gp-d-line_tree" d="M256,154 L294,154 L294,168" />
			<Box x={190} y={168} w={64} h={28} title="relay c" />
			<Box x={262} y={168} w={64} h={28} title="relay b" />

			<Frame x={374} y={16} w={330} h={210} title="node b: a follower" />
			<Box x={464} y={44} w={150} h={28} title="supervisor" kind="supervisor" />
			<path className="gp-d-line gp-d-line_tree" d="M539,72 L539,89 L464,89 L464,104" />
			<Box x={404} y={104} w={120} title="elector" sub="leader/sched" kind="platform" />
			<Note x={620} y={125} anchor="middle">
				no singleton
			</Note>
			<path className="gp-d-line gp-d-line_tree" d="M464,140 L464,154 L426,154 L426,168" />
			<path className="gp-d-line gp-d-line_tree" d="M464,154 L502,154 L502,168" />
			<Box x={394} y={168} w={64} h={28} title="relay a" />
			<Box x={470} y={168} w={64} h={28} title="relay c" />

			<Line id={id} d="M326,182 L360,182 L360,121 L404,121" />
			<Note x={360} y={246} anchor="middle">
				a's relay for b carries a's heartbeats to b's elector; b's relay for a carries the acks back
			</Note>
			<Note x={360} y={264} anchor="middle">
				a relay also monitors its peer's elector, and waits for a dial that hangs, so the elector never does
			</Note>
		</Diagram>
	);
}
