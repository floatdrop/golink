// The drawings of the supervision page, with the primitives of diagrams.tsx.
import { Box, Diagram, Note } from './diagrams.tsx';

/**
 * A small tree: a supervisor over three workers, drawn three times, once per
 * strategy. In each, the middle worker exited; the boxes say which of the
 * three come back.
 */
export function Strategies() {
	const id = 'strategies';
	const columns: { x: number; title: string; sub: string; restarted: boolean[] }[] = [
		{ x: 120, title: 'OneForOne', sub: 'only the one that exited', restarted: [false, true, false] },
		{ x: 360, title: 'OneForAll', sub: 'every child', restarted: [true, true, true] },
		{ x: 600, title: 'RestForOne', sub: 'it, and those started after it', restarted: [false, true, true] }
	];
	return (
		<Diagram id={id} width={720} height={228} label="Which children a supervisor restarts under each strategy when the middle child exits">
			{columns.map((c) => {
				const kids = [c.x - 76, c.x, c.x + 76];
				return (
					<g key={c.title}>
						<text className="gp-d-title" x={c.x} y={22} textAnchor="middle">
							{c.title}
						</text>
						<Note x={c.x} y={38} anchor="middle">
							{c.sub}
						</Note>
						<Box x={c.x - 48} y={54} w={96} h={30} title="supervisor" kind="supervisor" />
						{kids.map((kx, i) => (
							<g key={kx}>
								<path
									className="gp-d-line gp-d-line_tree"
									d={`M${c.x},84 L${c.x},100 L${kx},100 L${kx},116`}
								/>
								<Box
									x={kx - 32}
									y={116}
									w={64}
									h={30}
									title={`w${i + 1}`}
									kind={i === 1 ? 'outside' : c.restarted[i] ? 'actor' : 'plain'}
								/>
								<Note x={kx} y={166} anchor="middle">
									{i === 1 ? 'exited' : ''}
								</Note>
								<Note x={kx} y={c.restarted[i] ? 184 : 166} anchor="middle">
									{c.restarted[i] ? 'restarted' : i === 1 ? '' : 'untouched'}
								</Note>
							</g>
						))}
					</g>
				);
			})}
			<Note x={360} y={216} anchor="middle">
				a dashed box is the child that exited; a filled one comes back new
			</Note>
		</Diagram>
	);
}

/**
 * Escalation: a worker that keeps failing takes its supervisor past its
 * limit, and the supervisor above starts that whole subtree again.
 */
export function Escalation() {
	const id = 'escalation';
	const services = [
		{ x: 140, name: 'inventory', worker: 'stock' },
		{ x: 360, name: 'payments', worker: 'cashier' },
		{ x: 580, name: 'orders', worker: 'desk' }
	];
	return (
		<Diagram id={id} width={720} height={262} label="A worker exceeding its supervisor's restart limit ends that supervisor, and the root restarts it">
			<Box x={316} y={10} w={88} h={30} title="root" kind="supervisor" />
			<Note x={414} y={30}>OneForOne</Note>
			{services.map((s, i) => (
				<g key={s.name}>
					<path className="gp-d-line gp-d-line_tree" d={`M360,40 L360,58 L${s.x},58 L${s.x},76`} />
					<Box x={s.x - 52} y={76} w={104} h={30} title={s.name} kind={i === 1 ? 'outside' : 'supervisor'} />
					<path className="gp-d-line gp-d-line_tree" d={`M${s.x},106 L${s.x},124`} />
					<Box x={s.x - 52} y={124} w={104} h={30} title={s.worker} kind={i === 1 ? 'outside' : 'actor'} />
				</g>
			))}
			<Note x={360} y={180} anchor="middle">
				1. cashier fails 4 times in 5s
			</Note>
			<Note x={360} y={198} anchor="middle">
				2. payments gives up: exit reason "max restarts"
			</Note>
			<Note x={360} y={216} anchor="middle">
				3. root sees an abnormal exit and starts payments again, with a new cashier
			</Note>
			<Note x={360} y={246} anchor="middle">
				inventory and orders run on throughout: OneForOne at the root
			</Note>
		</Diagram>
	);
}
