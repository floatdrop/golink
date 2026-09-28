// Draws public/scene.svg, the hero's scene: a node hosting processes (the
// mark, as in favicon.svg), linked to peer nodes with processes of their own,
// and requests going between processes, inside a node and across the links.
//
//	node scripts/scene.ts
//
// The page shows it as an <img>, which runs no script, so the motion is CSS
// keyframes, sampled here from routes along the drawing's own lines. With
// prefers-reduced-motion it is the still drawing: two requests on the links.
import { writeFileSync } from 'node:fs';
import { dirname, join } from 'node:path';
import { fileURLToPath } from 'node:url';

const out = join(dirname(fileURLToPath(import.meta.url)), '..', 'public', 'scene.svg');

const blue = '#3b82c4';
const cyan = '#7fd0e5';
const teal = '#2a9d8f';
const orange = '#e76f51';

type Point = readonly [number, number];
type Proc = { at: Point; color: string };

type Node = {
	centre: Point;
	radius: number;
	stroke: number;
	procR: number;
	/** How the processes are joined: a triangle for three, a line for two. */
	edges: 'triangle' | 'line' | 'none';
	edgeStroke: number;
	/** Where the link to the centre node leaves this one, and arrives there; unset for the centre. */
	link?: { here: Point; there: Point };
	procs: Record<string, Proc>;
};

const nodes: Record<string, Node> = {
	c: {
		centre: [280, 160],
		radius: 74,
		stroke: 3.5,
		procR: 14.7,
		edges: 'triangle',
		edgeStroke: 3.2,
		procs: {
			a: { at: [280, 138], color: cyan },
			b: { at: [256, 182.8], color: teal },
			c: { at: [304, 182.8], color: orange }
		}
	},
	tl: {
		centre: [74, 74],
		radius: 36,
		stroke: 2.5,
		procR: 6.2,
		edges: 'triangle',
		edgeStroke: 1.8,
		link: { here: [110.7, 89.3], there: [210.4, 130.9] },
		procs: {
			a: { at: [74, 64.9], color: cyan },
			b: { at: [63.9, 83.8], color: teal },
			c: { at: [84.1, 83.8], color: orange }
		}
	},
	tr: {
		centre: [486, 74],
		radius: 36,
		stroke: 2.5,
		procR: 6.2,
		edges: 'line',
		edgeStroke: 1.8,
		link: { here: [449.3, 89.3], there: [349.6, 130.9] },
		procs: {
			a: { at: [475.9, 74], color: cyan },
			c: { at: [496.1, 74], color: orange }
		}
	},
	bl: {
		centre: [74, 246],
		radius: 36,
		stroke: 2.5,
		procR: 7.3,
		edges: 'none',
		edgeStroke: 1.8,
		link: { here: [110.7, 230.7], there: [210.4, 189.1] },
		procs: {
			b: { at: [74, 246], color: teal }
		}
	},
	br: {
		centre: [486, 246],
		radius: 36,
		stroke: 2.5,
		procR: 6.2,
		edges: 'line',
		edgeStroke: 1.8,
		link: { here: [449.3, 230.7], there: [349.6, 189.1] },
		procs: {
			b: { at: [475.9, 246], color: teal },
			a: { at: [496.1, 246], color: cyan }
		}
	}
};

/** One loop of the animation, in seconds. */
const period = 16;
/** How fast a request travels, in units a second, and the least time a hop takes. */
const speed = 105;
const minHop = 0.9;
/** How long a process holds a request before it sends the next one. */
const dwell = 0.35;
/** The request's radius on a link and in the centre node; inside a peer it shrinks to fit. */
const dotR = 4.5;
const peerScale = 0.6;
/** How long a process's ripple lasts when a request reaches it. */
const ripple = 0.7;
/** Long enough apart for keyframe percentages to stay distinct. */
const eps = 0.02;

// Each chain is a conversation: a request goes out, is passed along, and the
// answer comes back the way it went. Hops are "node.proc>node.proc"; a
// request between two nodes goes out along the link joining them.
const chains: { start: number; hops: string[] }[] = [
	{
		start: 0,
		hops: ['tl.c>c.a', 'c.a>c.c', 'c.c>br.b', 'br.b>br.a', 'br.a>br.b', 'br.b>c.c', 'c.c>c.a', 'c.a>tl.c']
	},
	{
		start: 5,
		hops: ['tr.c>tr.a', 'tr.a>c.c', 'c.c>c.b', 'c.b>bl.b', 'bl.b>c.b', 'c.b>c.c', 'c.c>tr.a', 'tr.a>tr.c']
	},
	{ start: 13.8, hops: ['tl.b>c.b', 'c.b>bl.b', 'bl.b>c.b', 'c.b>tl.b'] },
	{ start: 4.2, hops: ['tl.a>tl.b', 'tl.b>tl.a'] },
	{ start: 11.4, hops: ['br.a>br.b', 'br.b>br.a'] }
];

// The still drawing's two requests, where reduced motion leaves them.
const stills: { at: Point; color: string }[] = [
	{ at: [168.5, 113.5], color: orange },
	{ at: [409.4, 214], color: teal }
];

function proc(ref: string): { node: string; proc: Proc } {
	const [node = '', name = ''] = ref.split('.');
	const p = nodes[node]?.procs[name];
	if (!p) {
		throw new Error(`scene: no process ${ref}`);
	}
	return { node, proc: p };
}

function route(from: string, to: string): Point[] {
	const a = proc(from);
	const b = proc(to);
	if (a.node === b.node) {
		return [a.proc.at, b.proc.at];
	}
	if (a.node !== 'c' && b.node !== 'c') {
		throw new Error(`scene: ${from} and ${to} are not linked`);
	}
	const link = nodes[a.node === 'c' ? b.node : a.node]?.link;
	if (!link) {
		throw new Error(`scene: no link between ${from} and ${to}`);
	}
	return a.node === 'c'
		? [a.proc.at, link.there, link.here, b.proc.at]
		: [a.proc.at, link.here, link.there, b.proc.at];
}

const dist = (a: Point, b: Point) => Math.hypot(b[0] - a[0], b[1] - a[1]);

function length(path: Point[]): number {
	let n = 0;
	for (let i = 1; i < path.length; i++) {
		n += dist(path[i - 1]!, path[i]!);
	}
	return n;
}

/** The point a fraction f of the way along a path. */
function along(path: Point[], f: number): Point {
	let left = f * length(path);
	for (let i = 1; i < path.length; i++) {
		const a = path[i - 1]!;
		const b = path[i]!;
		const d = dist(a, b);
		if (left <= d || i === path.length - 1) {
			const t = d === 0 ? 0 : Math.min(left / d, 1);
			return [a[0] + (b[0] - a[0]) * t, a[1] + (b[1] - a[1]) * t];
		}
		left -= d;
	}
	return path[0]!;
}

/** A request shrinks as it goes into a peer, whose processes are small. */
function scaleAt(p: Point): number {
	let near = Infinity;
	for (const [name, n] of Object.entries(nodes)) {
		if (name !== 'c') {
			near = Math.min(near, dist(p, n.centre));
		}
	}
	const t = Math.min(Math.max((near - 24) / 14, 0), 1);
	return peerScale + (1 - peerScale) * t * t * (3 - 2 * t);
}

const ease = (t: number) => (1 - Math.cos(Math.PI * t)) / 2;

const num = (n: number, digits = 1) => String(Number(n.toFixed(digits)));
const fixed = (n: number) => n.toFixed(1);

type Frame = { t: number; props: Record<string, number> };

/**
 * Keyframes for one loop, from frames at times that may run past its end:
 * the loop is a circle, so they wrap, and 0% and 100% are the same moment.
 */
function keyframes(name: string, frames: Frame[], css: (props: Record<string, number>) => string): string {
	const wrapped = frames
		.map((f) => ({ t: ((f.t % period) + period) % period, props: f.props }))
		.sort((a, b) => a.t - b.t);
	const first = wrapped[0]!;
	const last = wrapped[wrapped.length - 1]!;
	const span = first.t + period - last.t;
	const f = span === 0 ? 0 : (period - last.t) / span;
	const at0: Record<string, number> = {};
	for (const k of Object.keys(first.props)) {
		const a = last.props[k] ?? 0;
		at0[k] = a + ((first.props[k] ?? 0) - a) * f;
	}
	const all = [{ t: 0, props: at0 }, ...wrapped, { t: period, props: at0 }];
	const body = all.map((fr) => `${num((fr.t / period) * 100, 3)}%{${css(fr.props)}}`).join('');
	return `@keyframes ${name}{${body}}`;
}

const moveCss = (p: Record<string, number>) =>
	`transform:translate(${num(p.x!)}px,${num(p.y!)}px) scale(${num(p.s!, 2)});opacity:${p.o}`;
const rippleCss = (p: Record<string, number>) => `stroke-width:${num(p.w!)};opacity:${num(p.o!, 2)}`;

const styles: string[] = [];
const dots: string[] = [];
const rings: string[] = [];

let hopN = 0;
for (const chain of chains) {
	let t = chain.start;
	for (const hop of chain.hops) {
		const [from = '', to = ''] = hop.split('>');
		const path = route(from, to);
		const dur = Math.max(minHop, length(path) / speed);
		const start = t;
		const end = t + dur;
		const src = path[0]!;
		const dst = path[path.length - 1]!;
		const id = ++hopN;

		// The request travels hidden under the sending process's circle until it
		// leaves it, and under the receiving one's once it arrives; between hops
		// it is transparent, so it can go back to where the next one starts.
		const frames: Frame[] = [];
		const state = (at: Point, o: number) => ({ x: at[0], y: at[1], s: scaleAt(at), o });
		frames.push({ t: start - eps, props: state(src, 0) });
		const steps = Math.max(2, Math.ceil(dur / 0.1));
		for (let i = 0; i <= steps; i++) {
			const f = i / steps;
			frames.push({ t: start + dur * f, props: state(along(path, ease(f)), 1) });
		}
		frames.push({ t: end + eps, props: state(dst, 0) });
		styles.push(`.m${id}{animation-name:m${id}}`, keyframes(`m${id}`, frames, moveCss));
		dots.push(`<circle class="m m${id}" r="${dotR}" fill="${proc(from).proc.color}"/>`);

		// The receiving process ripples: a ring behind its circle, so only the
		// part outside it shows.
		const target = proc(to);
		const wide = (nodes[target.node]?.procR ?? 0) > 10 ? 12 : 6;
		const ringFrames: Frame[] = [
			{ t: end - eps, props: { w: 0, o: 0 } },
			{ t: end, props: { w: 0, o: 0.55 } },
			{ t: end + ripple, props: { w: wide, o: 0 } }
		];
		styles.push(`.p${id}{animation-name:p${id}}`, keyframes(`p${id}`, ringFrames, rippleCss));
		const r = nodes[target.node]?.procR ?? 0;
		rings.push(
			`<circle class="p p${id}" cx="${fixed(target.proc.at[0])}" cy="${fixed(target.proc.at[1])}" r="${r}" fill="none" stroke="${target.proc.color}"/>`
		);

		t = end + dwell;
	}
	if (t - chain.start > period) {
		throw new Error(`scene: a chain starting at ${chain.start}s outlasts the ${period}s loop`);
	}
}

function hexagon(n: Node): string {
	const [x, y] = n.centre;
	const w = n.radius * Math.cos(Math.PI / 6);
	const h = n.radius / 2;
	const pts: Point[] = [
		[x, y - n.radius],
		[x + w, y - h],
		[x + w, y + h],
		[x, y + n.radius],
		[x - w, y + h],
		[x - w, y - h]
	];
	return `<polygon points="${pts.map((p) => `${fixed(p[0])},${fixed(p[1])}`).join(' ')}" fill="none" stroke="${blue}" stroke-width="${n.stroke}" stroke-linejoin="round"/>`;
}

function edges(n: Node): string {
	const at = Object.values(n.procs).map((p) => p.at);
	const pt = (p: Point) => `${fixed(p[0])} ${fixed(p[1])}`;
	switch (n.edges) {
		case 'triangle':
			return `<path d="M${pt(at[0]!)} L${pt(at[1]!)} L${pt(at[2]!)} Z" fill="none" stroke="${blue}" stroke-width="${n.edgeStroke}" stroke-linejoin="round"/>`;
		case 'line':
			return `<path d="M${pt(at[0]!)} L${pt(at[1]!)}" fill="none" stroke="${blue}" stroke-width="${n.edgeStroke}" stroke-linejoin="round" stroke-linecap="round"/>`;
		case 'none':
			return '';
	}
}

const all = Object.values(nodes);
const links = all
	.flatMap((n) => (n.link ? [n.link] : []))
	.map(
		(l) =>
			`<line x1="${fixed(l.there[0])}" y1="${fixed(l.there[1])}" x2="${fixed(l.here[0])}" y2="${fixed(l.here[1])}" stroke="${blue}" stroke-width="2.5" stroke-opacity="0.6" stroke-linecap="round"/>`
	);
const circles = all.flatMap((n) =>
	Object.values(n.procs).map((p) => `<circle cx="${fixed(p.at[0])}" cy="${fixed(p.at[1])}" r="${n.procR}" fill="${p.color}"/>`)
);

const svg = [
	'<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 560 320" role="img" aria-label="A grpcproc node, its processes, and the nodes it is linked to">',
	'<!-- The scene: a node hosts processes (the mark, as in favicon.svg), and links to peer nodes, each with processes of its own; requests go between processes, inside a node and across the links. Drawn by site/scripts/scene.ts. -->',
	'<style>',
	`.m,.p{animation-duration:${period}s;animation-timing-function:linear;animation-iteration-count:infinite}`,
	'.m{opacity:0}.p{opacity:0}.still{display:none}',
	'@media (prefers-reduced-motion:reduce){.m,.p{display:none}.still{display:inline}}',
	...styles,
	'</style>',
	...links,
	...all.map(hexagon),
	...all.map(edges).filter(Boolean),
	...stills.map((s) => `<circle class="still" cx="${fixed(s.at[0])}" cy="${fixed(s.at[1])}" r="${dotR}" fill="${s.color}"/>`),
	...dots,
	...rings,
	...circles,
	'</svg>',
	''
].join('\n');

writeFileSync(out, svg);
console.log(`scene: wrote ${out} (${svg.length} bytes, ${hopN} requests)`);
