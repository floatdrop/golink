// The guide's drawings, as inline SVG. Every colour is a Gravity UI token, set
// in main.css, so a drawing follows the theme the way the text does; the
// shapes are the only thing here. Coordinates are in the viewBox, which is
// drawn at the width of the column.
import type { ReactNode } from 'react';

type Kind = 'plain' | 'actor' | 'supervisor' | 'platform' | 'contract' | 'outside';

interface BoxProps {
	x: number;
	y: number;
	w: number;
	h?: number;
	title: string;
	sub?: string | undefined;
	kind?: Kind;
}

/** A labelled box; x and y are its top-left corner. */
function Box({ x, y, w, h = 36, title, sub, kind = 'plain' }: BoxProps) {
	const cx = x + w / 2;
	return (
		<g className={`gp-d-box gp-d-box_${kind}`}>
			<rect x={x} y={y} width={w} height={h} rx={6} />
			<text className="gp-d-title" x={cx} y={sub ? y + h / 2 - 3 : y + h / 2 + 4} textAnchor="middle">
				{title}
			</text>
			{sub && (
				<text className="gp-d-sub" x={cx} y={y + h / 2 + 12} textAnchor="middle">
					{sub}
				</text>
			)}
		</g>
	);
}

/** A process boundary: everything drawn inside runs in one OS process. */
function Frame({ x, y, w, h, title }: { x: number; y: number; w: number; h: number; title: string }) {
	return (
		<g className="gp-d-frame">
			<rect x={x} y={y} width={w} height={h} rx={10} />
			<text className="gp-d-frame-title" x={x + 12} y={y + 20}>
				{title}
			</text>
		</g>
	);
}

interface LineProps {
	d: string;
	/** Which diagram's markers to use: marker ids are per drawing. */
	id: string;
	end?: boolean;
	start?: boolean;
	kind?: 'solid' | 'dashed' | 'tree';
}

function Line({ d, id, end = true, start = false, kind = 'solid' }: LineProps) {
	return (
		<path
			className={`gp-d-line gp-d-line_${kind}`}
			d={d}
			markerEnd={end ? `url(#${id}-end)` : undefined}
			markerStart={start ? `url(#${id}-start)` : undefined}
		/>
	);
}

function Note({ x, y, children, anchor = 'start' }: { x: number; y: number; children: ReactNode; anchor?: 'start' | 'middle' | 'end' }) {
	return (
		<text className="gp-d-note" x={x} y={y} textAnchor={anchor}>
			{children}
		</text>
	);
}

/** The svg element, its accessible name, and this drawing's arrowheads. */
function Diagram({ id, width, height, label, children }: { id: string; width: number; height: number; label: string; children: ReactNode }) {
	return (
		<svg className="gp-diagram" viewBox={`0 0 ${width} ${height}`} role="img" aria-label={label}>
			<defs>
				<marker id={`${id}-end`} viewBox="0 0 10 10" refX="9" refY="5" markerWidth="7" markerHeight="7" orient="auto">
					<path className="gp-d-head" d="M0,0 L10,5 L0,10 z" />
				</marker>
				<marker id={`${id}-start`} viewBox="0 0 10 10" refX="1" refY="5" markerWidth="7" markerHeight="7" orient="auto">
					<path className="gp-d-head" d="M10,0 L0,5 L10,10 z" />
				</marker>
			</defs>
			{children}
		</svg>
	);
}

/**
 * The four layers, and which way imports go: an entry point composes
 * modules, every process runs the platform, a service imports contracts and
 * never another service.
 */
export function Layout() {
	const id = 'layout';
	const services = ['inventory', 'payments', 'orders', 'web'];
	const contracts = ['inventory/v1', 'payments/v1', 'orders/v1'];
	const sx = (i: number) => 24 + i * 130;
	const cx = (i: number) => 60 + i * 160;
	const edges: [number, number][] = [
		[0, 0],
		[1, 1],
		[2, 0],
		[2, 1],
		[2, 2],
		[3, 2]
	];
	return (
		<Diagram id={id} width={720} height={292} label="Entry points, the platform, services and contracts, and the imports between them">
			{['cmd/local', 'cmd/front', 'cmd/warehouse', 'cmd/billing'].map((c, i) => (
				<Box key={c} x={24 + i * 130} y={14} w={116} title={c} kind="outside" />
			))}
			<Note x={560} y={36}>which services</Note>
			<Box x={24} y={70} w={506} title="platform" sub="config · node · gRPC server · Inspector · root supervisor" kind="platform" />
			<Note x={560} y={92}>every program</Note>
			{services.map((s, i) => (
				<Box key={s} x={sx(i)} y={136} w={116} title={s} kind={s === 'web' ? 'plain' : 'actor'} />
			))}
			<Note x={560} y={158}>a Module each</Note>
			{contracts.map((c, i) => (
				<Box key={c} x={cx(i)} y={236} w={120} title={c} kind="contract" />
			))}
			<Note x={560} y={258}>messages, names</Note>
			{edges.map(([s, c]) => (
				<Line key={`${s}-${c}`} id={id} d={`M${sx(s) + 58},172 L${cx(c) + 60},234`} />
			))}
		</Diagram>
	);
}

/** One order, as the processes see it. */
export function Flow() {
	const id = 'flow';
	const lanes = [
		{ x: 70, title: 'HTTP client', sub: '' },
		{ x: 220, title: 'web front', sub: 'not a process' },
		{ x: 370, title: 'desk', sub: 'orders' },
		{ x: 520, title: 'stock', sub: 'inventory' },
		{ x: 660, title: 'cashier', sub: 'payments' }
	];
	const x = (i: number) => lanes[i]!.x;
	const msg = (from: number, to: number, y: number, text: string, kind: 'solid' | 'dashed' = 'solid') => (
		<g key={`${from}-${to}-${y}`}>
			<Line id={id} kind={kind} d={`M${x(from) + (to > from ? 4 : -4)},${y} L${x(to) + (to > from ? -4 : 4)},${y}`} />
			<Note x={(x(from) + x(to)) / 2} y={y - 6} anchor="middle">
				{text}
			</Note>
		</g>
	);
	return (
		<Diagram id={id} width={730} height={372} label="The sequence of calls and sends for one order">
			{lanes.map((l) => (
				<g key={l.title}>
					<Box x={l.x - 58} y={8} w={116} h={40} title={l.title} sub={l.sub || undefined} kind={l.title === 'HTTP client' ? 'outside' : l.title === 'web front' ? 'plain' : 'actor'} />
					<path className="gp-d-lifeline" d={`M${l.x},48 L${l.x},362`} />
				</g>
			))}
			{msg(0, 1, 82, 'POST /orders')}
			{msg(1, 2, 118, 'Call Place')}
			{msg(2, 3, 154, 'Call Reserve')}
			{msg(3, 2, 186, 'Reserved', 'dashed')}
			{msg(2, 4, 222, 'Call Charge')}
			{msg(4, 2, 254, 'Charged, or an error', 'dashed')}
			{msg(2, 3, 290, 'Send Release, if declined')}
			{msg(2, 1, 322, 'Placed, or the error', 'dashed')}
			{msg(1, 0, 354, '200, 422 or 503', 'dashed')}
		</Diagram>
	);
}

/** What the platform builds, in order, and the order things stop in. */
export function Lifecycle() {
	const id = 'lifecycle';
	const steps: { title: string; sub: string; kind: Kind }[] = [
		{ title: 'gRPC server', sub: 'and its listener', kind: 'platform' },
		{ title: 'node', sub: 'served on it', kind: 'platform' },
		{ title: 'Inspector', sub: 'beside the node', kind: 'platform' },
		{ title: 'root', sub: "services' trees", kind: 'supervisor' },
		{ title: 'HTTP server', sub: 'web front', kind: 'plain' }
	];
	return (
		<Diagram id={id} width={720} height={170} label="Services start left to right and stop right to left">
			<Line id={id} d="M24,24 L696,24" />
			<Note x={24} y={16}>built, then started, in this order</Note>
			{steps.map((s, i) => (
				<Box key={s.title} x={24 + i * 136} y={46} w={120} h={44} title={s.title} sub={s.sub} kind={s.kind} />
			))}
			<Line id={id} d="M696,118 L24,118" />
			<Note x={696} y={140} anchor="end">
				stopped in reverse, once the HTTP server has drained
			</Note>
			<Note x={696} y={158} anchor="end">
				so the trees stop before the node, and the node before the server
			</Note>
		</Diagram>
	);
}

interface TreeProps {
	x: number;
	y: number;
	services: { name: string; actor: string }[];
}

/** A node's supervision tree: the root, a supervisor per service, its process. */
function Tree({ x, y, services }: TreeProps) {
	const gap = 118;
	const left = x - ((services.length - 1) * gap) / 2;
	return (
		<g>
			<Box x={x - 44} y={y} w={88} h={30} title="root" kind="supervisor" />
			{services.map((s, i) => {
				const cx = left + i * gap;
				return (
					<g key={s.name}>
						<path className="gp-d-line gp-d-line_tree" d={`M${x},${y + 30} L${x},${y + 42} L${cx},${y + 42} L${cx},${y + 54}`} />
						<Box x={cx - 52} y={y + 54} w={104} h={30} title={s.name} kind="supervisor" />
						<path className="gp-d-line gp-d-line_tree" d={`M${cx},${y + 84} L${cx},${y + 100}`} />
						<Box x={cx - 52} y={y + 100} w={104} h={30} title={s.actor} kind="actor" />
					</g>
				);
			})}
		</g>
	);
}

/** The local deployment: one process, one node, every service. */
export function LocalPicture() {
	const id = 'local';
	return (
		<Diagram id={id} width={720} height={300} label="One program running one node with every service">
			<Box x={8} y={132} w={98} h={36} title="curl" kind="outside" />
			<Box x={8} y={236} w={98} h={36} title="grpcprocctl" kind="outside" />
			<Frame x={124} y={8} w={588} h={284} title="cmd/local · node shop" />
			<Box x={144} y={132} w={150} h={36} title="web front" sub=":8080" kind="plain" />
			<Box x={144} y={236} w={150} h={36} title="gRPC server" sub=":9100 · Inspector" kind="platform" />
			<Tree
				x={522}
				y={40}
				services={[
					{ name: 'inventory', actor: 'stock' },
					{ name: 'payments', actor: 'cashier' },
					{ name: 'orders', actor: 'desk' }
				]}
			/>
			<Line id={id} d="M106,150 L142,150" />
			<Line id={id} d="M106,254 L142,254" />
			<Line id={id} d="M294,150 L322,150 L322,200 L640,200 L640,172" />
			<Note x={332} y={218}>node.Call: no network in between</Note>
		</Diagram>
	);
}

/** The distributed deployment: the same services on three nodes. */
export function ClusterPicture() {
	const id = 'cluster';
	return (
		<Diagram id={id} width={720} height={396} label="Three programs, one node each, linked by gRPC streams">
			<Box x={8} y={116} w={80} h={36} title="curl" kind="outside" />
			<Frame x={100} y={8} w={250} h={250} title="cmd/front · node front" />
			<Box x={116} y={116} w={100} h={36} title="web front" sub=":8080" kind="plain" />
			<Tree x={282} y={46} services={[{ name: 'orders', actor: 'desk' }]} />
			<Box x={116} y={206} w={218} h={36} title="gRPC server" sub=":9101 · Inspector" kind="platform" />
			<Line id={id} d="M88,134 L114,134" />
			<Line id={id} d="M216,134 L222,134 L222,161 L228,161" />

			<Frame x={480} y={8} w={232} h={180} title="cmd/warehouse · node warehouse" />
			<Tree x={552} y={36} services={[{ name: 'inventory', actor: 'stock' }]} />
			<Box x={616} y={136} w={88} h={36} title="gRPC server" sub=":9102" kind="platform" />

			<Frame x={480} y={206} w={232} h={182} title="cmd/billing · node billing" />
			<Tree x={552} y={236} services={[{ name: 'payments', actor: 'cashier' }]} />
			<Box x={616} y={336} w={88} h={36} title="gRPC server" sub=":9103" kind="platform" />

			<Line id={id} start d="M350,120 L478,90" />
			<Line id={id} start d="M350,200 L478,290" />
			<Note x={416} y={156} anchor="middle">
				gRPC links,
			</Note>
			<Note x={416} y={172} anchor="middle">
				a stream each way
			</Note>

			<Box x={100} y={300} w={120} h={36} title="grpcprocctl" kind="outside" />
			<Line id={id} kind="dashed" d="M160,300 L160,244" />
			<Note x={100} y={360}>an Inspector forwards to the node asked about</Note>
		</Diagram>
	);
}
