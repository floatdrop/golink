'use strict';

// grpcprocctl web: a view per page of the sidebar, each fetching what it
// shows from the API on every tick, and a drawer for one process. The URL
// says what is open (#/processes?node=a&proc=<a.1.4>), so a view can be
// shared. Nothing here uses innerHTML: what nodes and processes say about
// themselves is text, and stays text.

// ---- DOM ----------------------------------------------------------------

function h(tag, props, ...kids) {
	const el = document.createElement(tag);
	append(el, kids);
	setProps(el, props); // after the children, so a select's value finds its option
	return el;
}

function svg(tag, props, ...kids) {
	const el = document.createElementNS('http://www.w3.org/2000/svg', tag);
	append(el, kids);
	setProps(el, props);
	return el;
}

function setProps(el, props) {
	for (const [k, v] of Object.entries(props || {})) {
		if (v == null || v === false) continue;
		if (k === 'style') Object.assign(el.style, v);
		else if (k.startsWith('on')) el.addEventListener(k.slice(2), v);
		else if (['value', 'checked', 'disabled', 'hidden'].includes(k)) el[k] = v;
		else el.setAttribute(k, v === true ? '' : v);
	}
}

function append(el, kids) {
	for (const kid of [kids].flat(Infinity)) {
		if (kid == null || kid === false || kid === '') continue;
		el.append(kid instanceof Node ? kid : String(kid));
	}
}

const $ = (sel) => document.querySelector(sel);
const cls = (...names) => names.filter(Boolean).join(' ');

// ---- formatting ---------------------------------------------------------

const nf = new Intl.NumberFormat('en-US');
const fmtInt = (n) => (n == null ? '' : nf.format(n));

function fmtRate(v) {
	if (v == null || !isFinite(v)) return '';
	if (v === 0) return '0';
	if (v >= 100) return nf.format(Math.round(v));
	if (v >= 10) return v.toFixed(1).replace(/\.0$/, '');
	return v.toFixed(2).replace(/\.?0+$/, '');
}

function fmtBytes(n) {
	if (n == null || !isFinite(n)) return '';
	const units = ['B', 'KiB', 'MiB', 'GiB', 'TiB'];
	let i = 0;
	n = Number(n);
	while (n >= 1024 && i < units.length - 1) {
		n /= 1024;
		i++;
	}
	return (i ? n.toFixed(n >= 10 ? 0 : 1) : Math.round(n)) + ' ' + units[i];
}

// parseDur reads a Go duration as the tools print one (1.112s, 3m2s, 512µs)
// into seconds, for sorting.
function parseDur(s) {
	const unit = { h: 3600, m: 60, s: 1, ms: 1e-3, 'µs': 1e-6, us: 1e-6, ns: 1e-9 };
	let t = 0;
	for (const [, n, u] of (s || '').matchAll(/([\d.]+)(h|ms|m|s|µs|us|ns)/g)) t += Number(n) * unit[u];
	return t;
}

function clock(iso) {
	const d = new Date(iso);
	if (isNaN(d)) return iso;
	const p = (n, w = 2) => String(n).padStart(w, '0');
	return `${p(d.getHours())}:${p(d.getMinutes())}:${p(d.getSeconds())}.${p(d.getMilliseconds(), 3)}`;
}

// A pid as grpcproc prints it: <node.incarnation.id>. Node names may hold
// dots; incarnations and ids do not.
function pidParts(pid) {
	const m = /^<(.*)\.(\d+)\.(\d+)>$/.exec(pid || '');
	return m ? { node: m[1], inc: Number(m[2]), id: Number(m[3]) } : null;
}
const nodeOf = (pid) => pidParts(pid)?.node || '';

// pidText shortens a pid for a table: the incarnation, which only tells a
// node's restarts apart, is left out. The full pid is its title.
function pidText(pid) {
	const p = pidParts(pid);
	return p ? `<${p.node}.….${p.id}>` : pid;
}
const pidSpan = (pid) => h('span', { class: 'mono', title: pid }, pidText(pid));

function pidCmp(a, b) {
	const x = pidParts(a), y = pidParts(b);
	if (!x || !y) return String(a).localeCompare(String(b));
	return x.node.localeCompare(y.node) || x.inc - y.inc || x.id - y.id;
}

function debounce(fn, ms) {
	let t = 0;
	return (...args) => {
		clearTimeout(t);
		t = setTimeout(() => fn(...args), ms);
	};
}

// rates remembers the counters it last saw per key and turns the next into
// per-second rates: null for a key it sees the first time.
function rates() {
	let prev = new Map(), at = 0;
	return (items, key, fields) => {
		const now = performance.now(), dt = (now - at) / 1000, next = new Map(), out = new Map();
		for (const it of items) {
			const k = key(it), vals = fields.map((f) => Number(it[f]) || 0), p = prev.get(k);
			next.set(k, vals);
			out.set(k, p && dt > 0 ? Object.fromEntries(fields.map((f, i) => [f, Math.max(0, (vals[i] - p[i]) / dt)])) : null);
		}
		prev = next;
		at = now;
		return out;
	};
}

const sum = (xs, f) => xs.reduce((a, x) => a + (Number(f(x)) || 0), 0);

// ---- API ----------------------------------------------------------------

function query(params) {
	const q = new URLSearchParams();
	for (const [k, v] of Object.entries(params || {})) if (v !== '' && v != null) q.set(k, v);
	return q.size ? '?' + q : '';
}

async function answer(res) {
	let body = null;
	try {
		body = await res.json();
	} catch {
		// not JSON: a proxy's error page
	}
	if (!res.ok) {
		const e = new Error(body?.error || `${res.status} ${res.statusText}`);
		e.status = res.status;
		throw e;
	}
	return body;
}

// Incarnations are nanoseconds since 1970, past what a JS number holds
// exactly: the page shows none, and tells processes apart by their pid
// strings.
const api = async (path, params) => answer(await fetch(path + query(params), { headers: { Accept: 'application/json' } }));

const post = async (path, body) =>
	answer(await fetch(path, { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(body) }));

// act changes something, once confirmed, and says how it went.
async function act(what, path, body, question) {
	if (question && !confirm(question)) return;
	try {
		await post(path, body);
		toast(what);
	} catch (e) {
		toast(`${what}: ${e.message}`, true);
	}
	refresh();
}

function toast(msg, bad) {
	const d = h('div', { class: bad ? 'bad' : '' }, msg);
	$('#toast').append(d);
	setTimeout(() => d.remove(), bad ? 7000 : 3000);
}

// ---- small components ---------------------------------------------------

// Gravity's xmark icon (@gravity-ui/icons, MIT).
const XMARK =
	'M3.47 3.47a.75.75 0 0 1 1.06 0L8 6.94l3.47-3.47a.75.75 0 1 1 1.06 1.06L9.06 8l3.47 3.47a.75.75 0 1 1-1.06 1.06L8 9.06l-3.47 3.47a.75.75 0 0 1-1.06-1.06L6.94 8 3.47 4.53a.75.75 0 0 1 0-1.06';
const icon = (d) =>
	svg('svg', { width: 16, height: 16, viewBox: '0 0 16 16', 'aria-hidden': 'true' }, svg('path', { fill: 'currentColor', 'fill-rule': 'evenodd', 'clip-rule': 'evenodd', d }));

// pill is Gravity's Label, in the theme that says what kind names: a state,
// a link's state, an elector's role, an event's kind, or one of bad, warn.
const LABEL_THEME = {
	idle: 'unknown', running: 'success', 'waiting-reply': 'warning', exiting: 'danger',
	up: 'success', connecting: 'warning', down: 'danger',
	leader: 'success', follower: 'info', candidate: 'warning', unclustered: 'unknown',
	spawn: 'success', exit: 'unknown', 'link-up': 'info', 'link-down': 'warning', 'dead-letter': 'danger',
	info: 'info', bad: 'danger', warn: 'warning',
};
const pill = (text, kind) => h('span', { class: `label label_${LABEL_THEME[kind ?? text] || 'unknown'}` }, text);

// head is a page's title and, under it, what the page shows.
const head = (title, lead) => h('header', { class: 'head' }, h('h1', null, title), h('p', { class: 'lead' }, lead));

// select wraps a select for Gravity's chevron.
const select = (el) => h('span', { class: 'select' }, el);
const delta = (v, fmt = fmtRate) => (v > 0 ? h('span', { class: 'delta' }, `+${fmt(v)}/s`) : null);
const card = (k, v, sub, kind) =>
	h('div', { class: cls('card', kind) }, h('div', { class: 'k' }, k), h('div', { class: 'v' }, v), h('div', { class: 's' }, sub));

function banner() {
	const el = h('div', { class: 'banner bad', hidden: true });
	return {
		el,
		set(msg) {
			el.hidden = !msg;
			el.textContent = msg || '';
		},
	};
}

// pidLink opens the process in the drawer, over whatever page is showing.
function pidLink(pid, text) {
	if (!pid) return null;
	const r = route();
	return h('a', { class: 'mono', title: pid, href: href(r.page, { ...r.params, proc: pid }), onclick: (e) => e.stopPropagation() }, text || pidText(pid));
}

// linkify turns the pids in a line a process says about itself into links.
const linkify = (s) => s.split(/(<[^<>\s]+\.\d+\.\d+>)/).map((part, i) => (i % 2 ? pidLink(part) : part));

// table draws rows under cols: {key, t (title), num, wrap, get(row), sort(row)}.
function table(cols, rows, opt = {}) {
	const { onRow, rowClass, sort, onSort, limit } = opt;
	const head = cols.map((c) =>
		h(
			'th',
			{ class: cls(c.num && 'num', c.sort && onSort && 'sort', sort && sort.key === c.key && 'on'), onclick: c.sort && onSort ? () => onSort(c) : null },
			c.t,
			sort && sort.key === c.key ? (sort.dir < 0 ? ' ↓' : ' ↑') : '',
		),
	);
	const shown = limit ? rows.slice(0, limit) : rows;
	const body = shown.map((r) =>
		h(
			'tr',
			{ class: cls(onRow && 'click', rowClass?.(r)), onclick: onRow ? () => onRow(r) : null },
			cols.map((c) => h('td', { class: cls(c.num && 'num', c.wrap && 'wrap') }, c.get(r))),
		),
	);
	return h(
		'div',
		{ class: 'tablewrap' },
		h('table', null, h('thead', null, h('tr', null, head)), h('tbody', null, body)),
		rows.length === 0 ? h('div', { class: 'empty' }, opt.empty || 'Nothing here.') : null,
		limit && rows.length > limit ? h('div', { class: 'more' }, `Showing ${limit} of ${fmtInt(rows.length)}: narrow the scope to see the rest.`) : null,
	);
}

// sorter orders rows by the column the params name: sort=<key>, dir=asc|desc.
function sorter(cols, params) {
	const col = cols.find((c) => c.key === params.sort && c.sort);
	if (!col) return { sort: null, apply: (rows) => rows };
	const dir = params.dir === 'asc' ? 1 : -1;
	const cmp = col.cmp || ((a, b) => (a < b ? -1 : a > b ? 1 : 0));
	return {
		sort: { key: col.key, dir },
		apply: (rows) => rows.map((r) => [col.sort(r), r]).sort((a, b) => dir * cmp(a[0], b[0])).map((x) => x[1]),
	};
}

// A click on a column sorts by it: numbers largest first, text A to Z; a
// second click turns it round.
function sortBy(params) {
	return (c) => {
		const same = params().sort === c.key;
		const dir = same ? (params().dir === 'asc' ? 'desc' : 'asc') : c.num ? 'desc' : 'asc';
		set({ sort: c.key, dir });
	};
}

// ---- charts -------------------------------------------------------------

const charts = new Set();

// Chart plots the last minute or so of one or more series, one point per
// tick. A null point (a rate before there are two samples) is a gap.
class Chart {
	constructor(title, series, opt = {}) {
		this.fmt = opt.fmt || fmtRate;
		this.n = opt.points || 60;
		this.series = series.map(([name, color]) => ({ name, color, data: [], val: h('b', null, '–') }));
		this.canvas = h('canvas');
		this.el = h(
			'div',
			{ class: 'chart' },
			h(
				'div',
				{ class: 'head' },
				h('span', { class: 'title' }, title),
				this.series.map((s) => h('span', { class: 'leg' }, h('i', { class: 'sw', style: { background: `var(${s.color})` } }), s.name, ' ', s.val)),
			),
			this.canvas,
		);
		charts.add(this);
	}

	push(...vals) {
		this.series.forEach((s, i) => {
			const v = vals[i] ?? null;
			s.data.push(v);
			if (s.data.length > this.n) s.data.shift();
			s.val.textContent = v == null ? '–' : this.fmt(v);
		});
		this.draw();
	}

	draw() {
		const c = this.canvas, w = c.clientWidth, ht = c.clientHeight;
		if (!w || !ht) return;
		const dpr = window.devicePixelRatio || 1;
		if (c.width !== Math.round(w * dpr) || c.height !== Math.round(ht * dpr)) {
			c.width = Math.round(w * dpr);
			c.height = Math.round(ht * dpr);
		}
		const g = c.getContext('2d');
		g.setTransform(dpr, 0, 0, dpr, 0, 0);
		g.clearRect(0, 0, w, ht);
		const css = getComputedStyle(document.documentElement);
		let max = 0;
		for (const s of this.series) for (const v of s.data) if (v != null && v > max) max = v;
		max = niceCeil(max);
		const top = 12, bottom = ht - 2, span = bottom - top;
		g.strokeStyle = css.getPropertyValue('--g-color-line-generic');
		g.lineWidth = 1;
		for (const f of [0, 0.5, 1]) {
			const y = Math.round(bottom - f * span) + 0.5;
			g.beginPath();
			g.moveTo(0, y);
			g.lineTo(w, y);
			g.stroke();
		}
		g.fillStyle = css.getPropertyValue('--g-color-text-secondary');
		g.font = '10px ' + css.getPropertyValue('--g-font-family-monospace');
		g.fillText(this.fmt(max), 2, top - 2);
		const step = w / (this.n - 1);
		for (const s of this.series) {
			g.strokeStyle = css.getPropertyValue(s.color).trim();
			g.lineWidth = 1.6;
			g.lineJoin = 'round';
			g.beginPath();
			const off = this.n - s.data.length;
			let pen = false;
			s.data.forEach((v, i) => {
				if (v == null) {
					pen = false;
					return;
				}
				const x = (off + i) * step, y = bottom - (v / max) * span;
				if (pen) g.lineTo(x, y);
				else g.moveTo(x, y);
				pen = true;
			});
			g.stroke();
		}
	}
}

function niceCeil(v) {
	if (v <= 0) return 1;
	const p = Math.pow(10, Math.floor(Math.log10(v)));
	for (const m of [1, 2, 2.5, 5, 10]) if (v <= m * p) return m * p;
	return 10 * p;
}

function redrawCharts() {
	for (const c of charts) {
		if (c.el.isConnected) c.draw();
		else charts.delete(c);
	}
}

// ---- pages --------------------------------------------------------------

function clusterPage() {
	const b = banner();
	const cards = h('div', { class: 'cards' });
	const map = h('div', { class: 'map' });
	const list = h('div');
	const nodeRates = rates(), linkRates = rates();
	const el = h('div', null, head('Cluster', 'Every node this Inspector reaches, and what goes between them.'), b.el, cards, map, h('h2', null, 'Nodes'), list);
	const linkKey = (l) => `${l.from}>${l.peer}>${l.direction}`;

	return {
		el,
		banner: b.set,
		async tick() {
			const nodes = await api('/api/nodes');
			setNodes(nodes);
			const nr = nodeRates(nodes, (n) => n.name, ['spawned', 'exited', 'dead_letters']);
			const links = nodes.flatMap((n) => (n.links || []).map((l) => ({ ...l, from: n.name })));
			const lr = linkRates(links, linkKey, ['messages', 'bytes']);
			const up = nodes.filter((n) => !n.error);
			const outs = links.filter((l) => l.direction === 'out');
			const linksUp = outs.filter((l) => l.state === 'up').length;
			const queued = sum(outs, (l) => l.queued);
			const deadRate = sum(up, (n) => nr.get(n.name)?.dead_letters);
			cards.replaceChildren(
				card('Nodes', up.length, nodes.length > up.length ? `${nodes.length - up.length} unreachable` : 'all reachable', nodes.length > up.length && 'bad'),
				card('Processes', fmtInt(sum(up, (n) => n.processes)), `${fmtInt(sum(up, (n) => n.spawned))} spawned in all`),
				card('Spawns / s', fmtRate(sum(up, (n) => nr.get(n.name)?.spawned)), `exits ${fmtRate(sum(up, (n) => nr.get(n.name)?.exited))}/s`),
				card('Dead letters', fmtInt(sum(up, (n) => n.dead_letters)), deadRate > 0 ? `+${fmtRate(deadRate)}/s` : 'none lately', deadRate > 0 && 'warn'),
				card('Links out', `${linksUp} / ${outs.length} up`, queued ? `${fmtInt(queued)} queued` : 'nothing queued', linksUp < outs.length && 'warn'),
				card('Messages / s', fmtRate(sum(outs, (l) => lr.get(linkKey(l))?.messages)), `${fmtBytes(sum(outs, (l) => lr.get(linkKey(l))?.bytes))}/s between nodes`),
			);
			map.replaceChildren(
				h('div', { class: 'caption' }, 'nodes and links'),
				drawMap(nodes, (l) => lr.get(linkKey(l))),
				h('div', { class: 'legend' }, h('span', null, 'An arrow is a node sending to a peer; its width grows with messages per second.'), h('span', null, 'Dashed: connecting or down. Click a node to open it.')),
			);
			list.replaceChildren(
				table(
					[
						{ t: 'Node', get: (n) => [h('b', null, n.name), n === nodes[0] ? ' ' : null, n === nodes[0] ? pill('inspector', 'info') : null] },
						{ t: 'Advertise', get: (n) => h('span', { class: 'mono' }, n.advertise) },
						{ t: 'Uptime', get: (n) => n.uptime },
						{ t: 'Processes', num: true, get: (n) => fmtInt(n.error ? null : n.processes) },
						{ t: 'Spawns / s', num: true, get: (n) => fmtRate(nr.get(n.name)?.spawned) },
						{ t: 'Exits / s', num: true, get: (n) => fmtRate(nr.get(n.name)?.exited) },
						{ t: 'Dead letters', num: true, get: (n) => [fmtInt(n.error ? null : n.dead_letters), delta(nr.get(n.name)?.dead_letters)] },
						{ t: 'Peers', get: (n) => peers(n) },
						{ t: 'Error', wrap: true, get: (n) => (n.error ? pill(n.error, 'bad') : '') },
					],
					nodes,
					{ onRow: (n) => go('node', { node: n.name }) },
				),
			);
		},
	};
}

// peers are a node's peers, each once, with the state of its link to it.
function peers(n) {
	const seen = new Map();
	for (const l of n.links || []) {
		const s = seen.get(l.peer);
		seen.set(l.peer, s === 'up' || l.state === 'up' ? 'up' : s || l.state);
	}
	return h('span', { class: 'row' }, [...seen].map(([p, s]) => pill(p, s)));
}

// drawMap lays the nodes out on an ellipse, the inspector's node first,
// and draws each out link as an arrow bent to its right, so the two
// directions between a pair of nodes stay apart.
function drawMap(nodes, rateOf) {
	const W = 800, H = 340, cx = W / 2, cy = H / 2, R = 30;
	const n = nodes.length;
	const rx = n < 2 ? 0 : Math.min(300, 90 + n * 40), ry = n < 3 ? 0 : 118;
	const a0 = n === 2 ? Math.PI : -Math.PI / 2;
	const pos = new Map(nodes.map((node, i) => [node.name, { x: cx + rx * Math.cos(a0 + (i * 2 * Math.PI) / n), y: cy + ry * Math.sin(a0 + (i * 2 * Math.PI) / n) }]));
	const edges = [], labels = [];
	for (const node of nodes) {
		for (const l of node.links || []) {
			const p = pos.get(node.name), q = pos.get(l.peer);
			if (l.direction !== 'out' || !q) continue;
			const dx = q.x - p.x, dy = q.y - p.y, len = Math.hypot(dx, dy) || 1, ux = dx / len, uy = dy / len, nx = -uy, ny = ux;
			const sx = p.x + ux * R + nx * 7, sy = p.y + uy * R + ny * 7, ex = q.x - ux * (R + 3) + nx * 7, ey = q.y - uy * (R + 3) + ny * 7;
			const mx = (p.x + q.x) / 2 + nx * 30, my = (p.y + q.y) / 2 + ny * 30;
			const r = rateOf({ ...l, from: node.name });
			const width = 1.5 + Math.min(5, Math.log10(1 + (r?.messages || 0)) * 1.4);
			const tip = `${node.name} → ${l.peer}: ${l.state}, ${fmtInt(l.messages)} messages, ${fmtBytes(l.bytes)}` + (l.queued ? `, ${l.queued} queued` : '') + (l.last_error ? `\nlast error: ${l.last_error}` : '');
			edges.push(svg('path', { class: cls('link', l.state), d: `M${sx},${sy} Q${mx},${my} ${ex},${ey}`, 'stroke-width': width, 'marker-end': 'url(#arrow)' }, svg('title', null, tip)));
			if (r?.messages > 0 || l.queued) {
				const lx = 0.25 * sx + 0.5 * mx + 0.25 * ex + nx * 8, ly = 0.25 * sy + 0.5 * my + 0.25 * ey + ny * 8;
				labels.push(svg('text', { class: 'rate', x: lx, y: ly }, `${fmtRate(r?.messages || 0)}/s` + (l.queued ? ` q${l.queued}` : '')));
			}
		}
	}
	const circles = nodes.map((node, i) => {
		const p = pos.get(node.name);
		const queued = (node.links || []).some((l) => l.queued > 0);
		const name = node.name.length > 11 ? node.name.slice(0, 10) + '…' : node.name;
		return svg(
			'g',
			{ class: cls('node', node.error && 'bad', queued && 'warn', i === 0 && 'here'), transform: `translate(${p.x},${p.y})`, onclick: () => go('node', { node: node.name }) },
			svg('title', null, node.error ? `${node.name}: ${node.error}` : `${node.name}: ${node.processes} processes, up ${node.uptime}`),
			svg('circle', { r: R }),
			svg('text', { y: -1 }, name),
			svg('text', { class: 'sub', y: 12 }, node.error ? 'down' : fmtInt(node.processes)),
		);
	});
	return svg(
		'svg',
		{ viewBox: `0 0 ${W} ${H}`, preserveAspectRatio: 'xMidYMid meet', role: 'img', 'aria-label': 'Nodes and the links between them' },
		svg('defs', null, svg('marker', { id: 'arrow', viewBox: '0 0 10 10', refX: 8, refY: 5, markerWidth: 9, markerHeight: 9, markerUnits: 'userSpaceOnUse', orient: 'auto' }, svg('path', { d: 'M0,0 L10,5 L0,10 z' }))),
		edges,
		labels,
		circles,
	);
}

function nodePage(node) {
	const b = banner();
	const cards = h('div', { class: 'cards' });
	const links = h('div');
	const cProc = new Chart('Processes', [['running', '--c1']], { fmt: fmtInt });
	const cLife = new Chart('Spawns and exits / s', [['spawns', '--c3'], ['exits', '--c2']]);
	const cDead = new Chart('Dead letters / s', [['dead letters', '--c5']]);
	const cMsgs = new Chart('Messages / s between nodes', [['out', '--c1'], ['in', '--c4']]);
	const cBytes = new Chart('Bytes / s between nodes', [['out', '--c1'], ['in', '--c4']], { fmt: fmtBytes });
	const nr = rates(), lr = rates();
	const el = h(
		'div',
		null,
		head(['Node ', h('span', { class: 'mono' }, node || '(inspector)')], "Its counters and its links to peers, charted over the last minute."),
		b.el,
		cards,
		h('div', { class: 'charts' }, cProc.el, cLife.el, cDead.el, cMsgs.el, cBytes.el),
		h('h2', null, 'Links'),
		links,
		h('p', null, h('a', { href: href('processes', { node }) }, 'Processes on this node →'), '   ', h('a', { href: href('events', { node }) }, 'Events →')),
	);
	const linkKey = (l) => `${l.peer}>${l.direction}`;

	return {
		el,
		banner: b.set,
		async tick() {
			const n = await api('/api/node', { node });
			const d = nr([n], (x) => x.name, ['spawned', 'exited', 'dead_letters']).get(n.name);
			const ls = n.links || [];
			const ld = lr(ls, linkKey, ['messages', 'bytes']);
			const total = (dir, f) => (d ? sum(ls.filter((l) => l.direction === dir), (l) => ld.get(linkKey(l))?.[f]) : null);
			const queued = sum(ls, (l) => l.queued);
			cards.replaceChildren(
				card('Processes', fmtInt(n.processes), `${fmtInt(n.spawned)} spawned, ${fmtInt(n.exited)} exited`),
				card('Spawns / s', fmtRate(d?.spawned) || '–', `exits ${fmtRate(d?.exited) || '–'}/s`),
				card('Dead letters', fmtInt(n.dead_letters), d?.dead_letters ? `+${fmtRate(d.dead_letters)}/s` : 'none lately', d?.dead_letters > 0 && 'warn'),
				card('Queued to peers', fmtInt(queued), queued ? fmtBytes(sum(ls, (l) => l.queued_bytes)) : 'nothing waits', queued > 0 && 'warn'),
				card('Uptime', n.uptime || '–', 'since this incarnation started'),
				card('Advertise', h('span', { class: 'mono' }, n.advertise || '–'), 'what peers dial'),
			);
			cProc.push(n.processes);
			cLife.push(d?.spawned, d?.exited);
			cDead.push(d?.dead_letters);
			cMsgs.push(total('out', 'messages'), total('in', 'messages'));
			cBytes.push(total('out', 'bytes'), total('in', 'bytes'));
			links.replaceChildren(
				table(
					[
						{ t: 'Peer', get: (l) => [h('a', { href: href('node', { node: l.peer }), onclick: (e) => e.stopPropagation() }, l.peer), l.incarnation ? null : h('span', { class: 'muted' }, ' never reached')] },
						{ t: 'Dir', get: (l) => (l.direction === 'out' ? '→ out' : '← in') },
						{ t: 'State', get: (l) => pill(l.state) },
						{ t: 'Age', get: (l) => l.age },
						{ t: 'Queued', num: true, get: (l) => (l.direction === 'out' ? [fmtInt(l.queued), l.queued ? h('span', { class: 'muted' }, ` ${fmtBytes(l.queued_bytes)}`) : null] : '') },
						{ t: 'Messages', num: true, get: (l) => [fmtInt(l.messages), delta(ld.get(linkKey(l))?.messages)] },
						{ t: 'Bytes', num: true, get: (l) => [fmtBytes(l.bytes), delta(ld.get(linkKey(l))?.bytes, fmtBytes)] },
						{ t: 'Reconnects', num: true, get: (l) => fmtInt(l.reconnects) },
						{ t: 'Retry in', get: (l) => l.retry_in },
						{ t: 'Last error', wrap: true, get: (l) => l.last_error },
					],
					ls,
					{ empty: 'No links: this node has not talked to another yet.', rowClass: (l) => l.queued > 0 && 'backlog' },
				),
			);
		},
	};
}

const STATES = ['idle', 'running', 'waiting-reply', 'exiting'];

function processesPage(node, params) {
	let p = params;
	const b = banner();
	const labels = h('datalist', { id: 'labels' });
	const known = new Set();
	const name = h('input', { type: 'search', placeholder: 'name contains', value: p.name || '', oninput: debounce((e) => set({ name: e.target.value }), 300) });
	const label = h('input', { type: 'search', placeholder: 'label', list: 'labels', value: p.label || '', onchange: (e) => set({ label: e.target.value }) });
	const state = h('select', { value: p.state || '', onchange: (e) => set({ state: e.target.value }) }, ['', ...STATES].map((s) => h('option', { value: s }, s || 'any state')));
	const min = h('input', { type: 'number', min: 0, placeholder: '0', value: p.min || '', onchange: (e) => set({ min: e.target.value }) });
	const search = h('input', { type: 'search', placeholder: 'search pid, name, label, type, message', value: p.q || '', oninput: (e) => set({ q: e.target.value }) });
	const count = h('span', { class: 'muted' });
	const cMsgs = new Chart('Messages / s', [['in', '--c1'], ['out', '--c2']]);
	const cBox = new Chart('Waiting in mailboxes', [['all', '--c2'], ['deepest', '--c5']], { fmt: fmtInt });
	const cState = new Chart('Processes by state', [['running', '--c3'], ['waiting reply', '--c2'], ['idle', '--c1']], { fmt: fmtInt });
	const list = h('div');
	const pr = rates();
	let rows = [], r = new Map();

	const cols = [
		{ key: 'pid', t: 'PID', get: (x) => pidSpan(x.pid), sort: (x) => x.pid, cmp: pidCmp },
		{ key: 'name', t: 'Name', get: (x) => x.name, sort: (x) => x.name || '' },
		{ key: 'label', t: 'Label', get: (x) => x.label, sort: (x) => x.label },
		{ key: 'state', t: 'State', get: (x) => pill(x.state), sort: (x) => x.state },
		{ key: 'mailbox', t: 'Mailbox', num: true, get: (x) => [fmtInt(x.mailbox), x.mailbox ? h('span', { class: 'bar', style: { width: `${Math.max(3, Math.min(60, 60 * Math.log10(1 + x.mailbox) / 3))}px` } }) : null], sort: (x) => x.mailbox },
		{ key: 'oldest', t: 'Oldest', num: true, get: (x) => x.oldest_wait, sort: (x) => parseDur(x.oldest_wait) },
		{ key: 'in', t: 'In / s', num: true, get: (x) => fmtRate(r.get(x.pid)?.received), sort: (x) => r.get(x.pid)?.received || 0 },
		{ key: 'out', t: 'Out / s', num: true, get: (x) => fmtRate(r.get(x.pid)?.sent), sort: (x) => r.get(x.pid)?.sent || 0 },
		{ key: 'received', t: 'Received', num: true, get: (x) => fmtInt(x.received), sort: (x) => x.received },
		{ key: 'sent', t: 'Sent', num: true, get: (x) => fmtInt(x.sent), sort: (x) => x.sent },
		{ key: 'calls', t: 'Calls', num: true, get: (x) => (x.calls_in_flight ? fmtInt(x.calls_in_flight) : ''), sort: (x) => x.calls_in_flight || 0 },
		{ key: 'last', t: 'Last message', get: (x) => h('span', { class: 'mono' }, x.last_message), sort: (x) => x.last_message || '' },
		{ key: 'uptime', t: 'Uptime', num: true, get: (x) => x.uptime, sort: (x) => parseDur(x.uptime) },
		{ key: 'log', t: 'Log', get: (x) => h('span', { class: 'muted' }, x.log_level) },
	];

	function render() {
		const q = (p.q || '').toLowerCase();
		const hits = q ? rows.filter((x) => [x.pid, x.name, x.label, x.type, x.last_message].some((f) => f && f.toLowerCase().includes(q))) : rows;
		const s = sorter(cols, p);
		count.textContent = q ? `${fmtInt(hits.length)} of ${fmtInt(rows.length)}` : `${fmtInt(rows.length)} processes`;
		list.replaceChildren(
			table(cols, s.apply(hits), {
				sort: s.sort,
				onSort: sortBy(() => p),
				limit: 1000,
				onRow: (x) => openProc(x.pid),
				rowClass: (x) => cls(x.mailbox > 0 && 'backlog', x.pid === app.params.proc && 'sel'),
				empty: rows.length ? 'No process matches the search.' : 'No process matches the scope.',
			}),
		);
	}

	const page = {
		el: h(
			'div',
			null,
			head(['Processes ', h('span', { class: 'mono' }, node || '(inspector)')], 'Every process on the node. The node applies the scope; the search and the order apply here, to what it sent.'),
			b.el,
			h('div', { class: 'charts' }, cMsgs.el, cBox.el, cState.el),
			h(
				'div',
				{ class: 'scope' },
				h('b', null, 'Scope'),
				h('label', null, name),
				h('label', null, label, labels),
				select(state),
				h('label', null, 'mailbox ≥', min),
				h('span', { class: 'sep' }),
				h('label', null, search),
				h('span', { class: 'spacer' }),
				count,
			),
			list,
		),
		banner: b.set,
		// The scope goes to the node, which sends only what matches; the search
		// and the order apply to what came back.
		update(np) {
			const refetch = ['name', 'label', 'state', 'min'].some((k) => (np[k] || '') !== (p[k] || ''));
			p = np;
			if (refetch) run(page);
			else render();
		},
		async tick() {
			rows = await api('/api/processes', { node, name: p.name, label: p.label, state: p.state, min_mailbox: p.min });
			r = pr(rows, (x) => x.pid, ['received', 'sent']);
			for (const x of rows) {
				if (!known.has(x.label)) {
					known.add(x.label);
					labels.append(h('option', { value: x.label }));
				}
			}
			const ready = [...r.values()].some(Boolean);
			cMsgs.push(ready ? sum(rows, (x) => r.get(x.pid)?.received) : null, ready ? sum(rows, (x) => r.get(x.pid)?.sent) : null);
			cBox.push(sum(rows, (x) => x.mailbox), Math.max(0, ...rows.map((x) => x.mailbox)));
			const by = (st) => rows.filter((x) => x.state === st).length;
			cState.push(by('running'), by('waiting-reply'), by('idle'));
			render();
		},
	};
	return page;
}

function treePage(node, params) {
	let p = params;
	const b = banner();
	const collapsed = new Set();
	const pr = rates();
	let rows = [], r = new Map();
	const color = h(
		'select',
		{ value: p.color || 'state', onchange: (e) => set({ color: e.target.value }) },
		h('option', { value: 'state' }, 'colour by state'),
		h('option', { value: 'mailbox' }, 'colour by mailbox'),
		h('option', { value: 'activity' }, 'colour by messages / s'),
	);
	const search = h('input', { type: 'search', placeholder: 'find name, pid, label', value: p.q || '', oninput: (e) => set({ q: e.target.value }) });
	const legend = h('span', { class: 'muted' });
	const count = h('span', { class: 'muted' });
	const view = h('div', { class: 'tree' });

	const heat = (v, steps) => 'h' + steps.filter((s) => v >= s).length;
	function colour(x) {
		if (p.color === 'mailbox') return heat(x.mailbox, [1, 10, 100]);
		if (p.color === 'activity') return heat(r.get(x.pid)?.received || 0, [0.01, 1, 100]);
		return x.state;
	}
	const legends = {
		state: 'grey idle · green running · amber waiting on a call · red exiting',
		mailbox: 'pale: empty · amber: some waiting · red: 100 or more',
		activity: 'pale: quiet · amber: some messages / s · red: 100 / s or more',
	};

	function render() {
		const byPid = new Map(rows.map((x) => [x.pid, x]));
		const kids = new Map(), roots = [];
		for (const x of rows) {
			if (x.parent && byPid.has(x.parent)) {
				if (!kids.has(x.parent)) kids.set(x.parent, []);
				kids.get(x.parent).push(x);
			} else roots.push(x);
		}
		const q = (p.q || '').toLowerCase();
		const hit = (x) => q && [x.pid, x.name, x.label].some((f) => f && f.toLowerCase().includes(q));
		// With a search, only hits and the processes above them are shown.
		let keep = null;
		if (q) {
			keep = new Set();
			for (const x of rows) {
				for (let y = hit(x) ? x : null; y && !keep.has(y.pid); y = byPid.get(y.parent)) keep.add(y.pid);
			}
		}
		const order = (a, b) => (kids.has(b.pid) ? 1 : 0) - (kids.has(a.pid) ? 1 : 0) || pidCmp(a.pid, b.pid);
		const row = (x) => {
			if (keep && !keep.has(x.pid)) return null;
			const ch = kids.get(x.pid) || [];
			const open = keep ? true : !collapsed.has(x.pid);
			const toggle = (e) => {
				e.stopPropagation();
				if (collapsed.has(x.pid)) collapsed.delete(x.pid);
				else collapsed.add(x.pid);
				render();
			};
			return [
				h(
					'div',
					{ class: cls('trow', hit(x) && 'hit'), onclick: () => openProc(x.pid) },
					h('span', { class: 'tog', onclick: ch.length ? toggle : null }, ch.length ? (open ? '▾' : '▸') : ''),
					h('span', { class: cls('dot', colour(x)), title: `${x.state}, ${x.mailbox} waiting` }),
					h('span', { class: 'tname' }, x.name || x.label),
					h('span', { class: 'mono muted', title: x.pid }, pidText(x.pid)),
					x.name ? h('span', { class: 'muted' }, x.label) : null,
					x.mailbox ? pill(`${fmtInt(x.mailbox)} waiting`, 'warn') : null,
					!open && ch.length ? h('span', { class: 'muted' }, `${ch.length} below`) : null,
					x.parent && !byPid.has(x.parent) ? h('span', { class: 'muted' }, 'started by ', pidLink(x.parent)) : null,
				),
				open && ch.length ? h('div', { class: 'tkids' }, ch.sort(order).map(row)) : null,
			];
		};
		legend.textContent = legends[p.color] || legends.state;
		count.textContent = `${fmtInt(rows.length)} processes, ${fmtInt(roots.filter((x) => kids.has(x.pid)).length)} trees`;
		view.replaceChildren();
		append(view, roots.sort(order).map(row));
		if (!view.childElementCount) view.append(h('div', { class: 'empty' }, rows.length ? 'Nothing matches.' : 'No processes.'));
	}

	const page = {
		el: h(
			'div',
			null,
			head(['Supervision ', h('span', { class: 'mono' }, node || '(inspector)')], ['Each process under the one that started it: a supervisor over its children. ', legend]),
			b.el,
			h(
				'div',
				{ class: 'scope' },
				select(color),
				h('label', null, search),
				h('button', { type: 'button', onclick: () => (collapsed.clear(), render()) }, 'Expand all'),
				h('button', { type: 'button', onclick: () => (rows.forEach((x) => x.parent && collapsed.add(x.parent)), render()) }, 'Collapse all'),
				h('span', { class: 'spacer' }),
				count,
			),
			view,
		),
		banner: b.set,
		update(np) {
			p = np;
			render();
		},
		async tick() {
			rows = await api('/api/processes', { node });
			r = pr(rows, (x) => x.pid, ['received']);
			render();
		},
	};
	return page;
}

const KINDS = ['spawn', 'exit', 'link-up', 'link-down', 'dead-letter'];

function eventsPage(node) {
	const b = banner();
	const on = new Set(KINDS);
	const counts = Object.fromEntries(KINDS.map((k) => [k, 0]));
	let buf = [], paused = false, held = 0, text = '', missed = 0, broke = '';
	const note = () => broke || (missed ? `${fmtInt(missed)} events were dropped: the node sent them faster than this page read them.` : '');
	const status = h('span', { class: 'muted' }, 'connecting…');
	const tbody = h('tbody');
	const chips = KINDS.map((k) => {
		const n = h('span', { class: 'n' }, '0');
		const btn = h('button', { type: 'button', class: 'kind on', 'aria-pressed': 'true', onclick: () => (on.has(k) ? on.delete(k) : on.add(k), btn.setAttribute('aria-pressed', String(btn.classList.toggle('on'))), redraw()) }, k, n);
		return { k, btn, n };
	});
	const pause = h('button', { type: 'button', onclick: () => ((paused = !paused), (held = 0), (pause.textContent = paused ? 'Resume' : 'Pause'), paused || redraw()) }, 'Pause');
	const chart = new Chart('Events / s', [['spawns', '--c3'], ['exits', '--c2'], ['dead letters', '--c5'], ['links', '--c4']]);
	const matches = (e) => on.has(e.kind) && (!text || JSON.stringify(e).toLowerCase().includes(text));

	function evRow(e, fresh) {
		const p = e.process;
		const detail = [];
		for (const [k, v] of [['reason', e.reason], ['peer', e.peer], ['type', e.type], ['error', e.error]]) if (v) detail.push(`${k}: ${v}`);
		const who = p
			? [pidLink(p.pid), ' ', h('span', { class: 'muted' }, p.name || p.label)]
			: e.kind === 'dead-letter'
				? [pidLink(e.from) || h('span', { class: 'muted' }, 'outside'), ' → ', pidLink(e.to) || '?']
				: '';
		return h(
			'tr',
			{ class: fresh ? 'fresh' : '' },
			h('td', { class: 'mono' }, clock(e.time)),
			h('td', null, pill(e.kind)),
			h('td', null, who),
			h('td', { class: 'wrap' }, detail.join(' · '), e.missed ? [' ', pill(`${fmtInt(e.missed)} missed before this`, 'warn')] : null),
		);
	}
	function redraw() {
		const shown = buf.filter(matches).slice(-500).reverse();
		tbody.replaceChildren(...shown.map((e) => evRow(e, false)));
	}

	const src = new EventSource('/api/events' + query({ node }));
	src.onopen = () => {
		status.textContent = `streaming from ${node || 'the inspector'}`;
		broke = '';
		b.set(note());
	};
	src.onerror = () => (status.textContent = 'reconnecting…');
	src.addEventListener('failure', (e) => b.set((broke = `The event stream broke: ${JSON.parse(e.data)}. Reconnecting.`)));
	src.onmessage = (m) => {
		const e = JSON.parse(m.data);
		counts[e.kind] = (counts[e.kind] || 0) + 1;
		missed += e.missed || 0;
		buf.push(e);
		if (buf.length > 5000) buf = buf.slice(-4000);
		if (paused) {
			held++;
			status.textContent = `paused: ${fmtInt(held)} new`;
		} else if (matches(e)) {
			tbody.prepend(evRow(e, true));
			while (tbody.childElementCount > 500) tbody.lastChild.remove();
		}
	};

	let prev = { ...counts }, at = performance.now();
	const filter = h('input', { type: 'search', placeholder: 'filter: pid, name, reason, type…', oninput: (e) => ((text = e.target.value.toLowerCase()), redraw()) });
	return {
		el: h(
			'div',
			null,
			head(['Events ', h('span', { class: 'mono' }, node || '(inspector)')], 'Spawns, exits with their reasons, links and dead letters, as they happen. The node keeps no history: the stream starts when the page opens.'),
			b.el,
			h('div', { class: 'charts' }, chart.el),
			h(
				'div',
				{ class: 'scope' },
				chips.map((c) => c.btn),
				h('span', { class: 'sep' }),
				h('label', null, filter),
				pause,
				h('button', { type: 'button', onclick: () => ((buf = []), redraw()) }, 'Clear'),
				h('span', { class: 'spacer' }),
				status,
			),
			h('div', { class: 'tablewrap' }, h('table', null, h('thead', null, h('tr', null, ['Time', 'Kind', 'Process', 'Detail'].map((t) => h('th', null, t)))), tbody)),
		),
		banner: (msg) => b.set(msg || note()),
		tick() {
			const now = performance.now(), dt = (now - at) / 1000;
			const d = (k) => (counts[k] - prev[k]) / dt;
			if (dt > 0.2) {
				chart.push(d('spawn'), d('exit'), d('dead-letter'), d('link-up') + d('link-down'));
				prev = { ...counts };
				at = now;
			}
			for (const c of chips) c.n.textContent = fmtInt(counts[c.k]);
		},
		unmount: () => src.close(),
	};
}

const readOnlyNote = () =>
	app.info.allow_writes ? null : h('p', { class: 'muted' }, 'Read-only. Start grpcprocctl web with --allow-writes to change things from here.');

function cronPage(_node, params) {
	let p = params;
	const b = banner();
	const where = h('select', { onchange: (e) => set({ on: e.target.value }) });
	const list = h('div');
	let crons = [];
	function render() {
		const nodes = [...new Set(crons.map((c) => c.node))];
		where.replaceChildren(h('option', { value: '' }, 'every node'), ...nodes.map((n) => h('option', { value: n }, n)));
		where.value = p.on || '';
		const rows = crons.filter((c) => !p.on || c.node === p.on).flatMap((c) => (c.jobs.length ? c.jobs.map((j) => ({ c, j })) : [{ c, j: null }]));
		const job = (x, op, label, danger) =>
			h('button', { type: 'button', class: danger ? 'danger' : '', onclick: () => act(`${op} ${x.j.name}`, '/api/cron', { node: x.c.node, target: x.c.pid, op, job: x.j.name }, danger && `Remove job ${x.j.name} from ${x.c.process} on ${x.c.node}? It comes back only when the cron process restarts from its spec.`) }, label);
		list.replaceChildren(
			table(
				[
					{ t: 'Node', get: (x) => x.c.node },
					{ t: 'Cron', get: (x) => pidLink(x.c.pid, x.c.process) },
					{ t: 'Job', get: (x) => (x.j ? h('b', null, x.j.name) : '') },
					{ t: 'Spec', get: (x) => (x.j ? h('span', { class: 'mono' }, x.j.spec) : '') },
					{ t: 'Zone', get: (x) => x.j?.location },
					{ t: 'Next', get: (x) => (x.j?.disabled ? pill('disabled', 'idle') : x.j?.next) },
					{ t: 'Last', get: (x) => x.j?.last },
					{ t: 'Running', num: true, get: (x) => (x.j?.running ? pill(String(x.j.running), 'running') : '') },
					{ t: 'Last failure', wrap: true, get: (x) => (x.j?.failure ? pill(x.j.failure, 'bad') : '') },
					{ t: 'Error', wrap: true, get: (x) => x.c.error },
					...(app.info.allow_writes
						? [{ t: '', get: (x) => (x.j ? h('span', { class: 'actions' }, x.j.disabled ? job(x, 'enable', 'Enable') : job(x, 'disable', 'Disable'), job(x, 'remove', 'Remove', true)) : '') }]
						: []),
				],
				rows,
				{ empty: 'No grpcproc/cron process runs on the nodes reachable from here.' },
			),
		);
	}
	return {
		every: 5000,
		el: h(
			'div',
			null,
			head('Cron', 'Every grpcproc/cron job on the nodes reachable from here. Each run is a process of its own, labelled cron:<job>.'),
			b.el,
			h('div', { class: 'scope' }, h('label', null, 'on', select(where))),
			list,
			readOnlyNote(),
		),
		banner: b.set,
		update(np) {
			p = np;
			render();
		},
		async tick() {
			crons = await api('/api/crons');
			render();
		},
	};
}

function electionsPage() {
	const b = banner();
	const list = h('div');
	const moveTo = new Map(); // what each election's "to" select was left at
	return {
		every: 5000,
		el: h('div', null, head('Elections', 'Every grpcproc/leader election reachable from here, as each node that takes part sees it. Nodes that follow different leaders, or lag a term behind, point at a partition.'), b.el, list, readOnlyNote()),
		banner: b.set,
		async tick() {
			const all = await api('/api/elections');
			if (!all.length) {
				list.replaceChildren(h('div', { class: 'empty' }, 'No node reachable from here takes part in a grpcproc/leader election.'));
				return;
			}
			list.replaceChildren(...all.map((e) => election(e, moveTo)));
		},
	};
}

function election(e, moveTo) {
	const answered = e.electors.filter((v) => !v.error);
	const leaders = new Set(answered.filter((v) => v.leader).map((v) => v.leader));
	const top = Math.max(0, ...answered.map((v) => v.term || 0));
	const cordoned = new Set(e.electors.flatMap((v) => v.cordoned || []));
	const head = [
		h('span', { class: 't' }, e.cluster),
		e.leading ? pill(`${e.leading} leads, term ${top}`, 'leader') : pill('no leader', 'bad'),
		leaders.size > 1 ? pill(`split: nodes follow ${[...leaders].join(', ')}`, 'bad') : answered.length && answered.every((v) => v.leader === e.leading) ? pill('all agree', 'up') : null,
		answered.some((v) => v.term < top) ? pill('some nodes a term behind', 'warn') : null,
	];
	if (app.info.allow_writes && e.leading) {
		const to = h(
			'select',
			{ value: moveTo.get(e.cluster) || '', onchange: (ev) => moveTo.set(e.cluster, ev.target.value) },
			h('option', { value: '' }, 'to the most up-to-date follower'),
			answered.filter((v) => v.node !== e.leading).map((v) => h('option', { value: v.node }, `to ${v.node}`)),
		);
		head.push(
			h('span', { class: 'spacer' }),
			h(
				'span',
				{ class: 'actions' },
				select(to),
				h('button', { type: 'button', onclick: () => act(`moved ${e.cluster}`, '/api/leader', { cluster: e.cluster, op: 'move', to: to.value }, `Hand leadership of ${e.cluster} over from ${e.leading} ${to.value ? 'to ' + to.value : 'to the most up-to-date follower'}? Its singleton stops, and starts on the new leader.`) }, 'Move leader'),
			),
		);
	}
	const cols = [
		{ t: 'Node', get: (v) => h('a', { href: href('node', { node: v.node }), onclick: (ev) => ev.stopPropagation() }, v.node) },
		{ t: 'Role', get: (v) => (v.role ? pill(v.role) : '') },
		{ t: 'Term', num: true, get: (v) => [v.term || '', v.term && v.term < top ? h('span', { class: 'muted' }, ' behind') : null] },
		{ t: 'Follows', get: (v) => v.leader },
		{ t: 'Voted for', get: (v) => v.voted_for },
		{ t: 'View', get: (v) => (v.view || []).join(', ') },
		{ t: 'Quorum', num: true, get: (v) => v.quorum || '' },
		{ t: 'State', get: (v) => h('span', { class: 'mono' }, v.state) },
		{ t: 'Cordoned', get: (v) => (v.cordoned || []).join(', ') },
		{ t: 'Unreachable', get: (v) => (v.unreachable || []).map((n) => pill(n, 'bad')) },
		{ t: 'Singleton', get: (v) => (v.singleton?.startsWith('<') ? pidLink(v.singleton) : v.singleton) },
		{ t: 'Backoff', get: (v) => v.backoff },
		{ t: 'Error', wrap: true, get: (v) => v.error },
	];
	if (app.info.allow_writes) {
		cols.push({
			t: '',
			get: (v) =>
				cordoned.has(v.node)
					? h('button', { type: 'button', onclick: () => act(`uncordoned ${v.node}`, '/api/leader', { cluster: e.cluster, op: 'uncordon', node: v.node }) }, 'Uncordon')
					: h('button', { type: 'button', class: 'danger', onclick: () => act(`cordoned ${v.node}`, '/api/leader', { cluster: e.cluster, op: 'cordon', node: v.node }, `Keep ${v.node} from leading ${e.cluster} until uncordoned?${v.node === e.leading ? ' It leads now, so it hands over.' : ''} It still votes.`) }, 'Cordon'),
		});
	}
	return h('div', { class: 'election' }, h('div', { class: 'ehead' }, head), table(cols, e.electors, { rowClass: (v) => v.error && 'backlog' }));
}

// ---- one process, in the drawer ------------------------------------------

const LEVELS = ['debug', 'info', 'warn', 'error'];

function procDrawer(pid) {
	const node = nodeOf(pid);
	const b = banner();
	const title = h('span', { class: 't' }, pid);
	const sub = h('div', { class: 'muted' });
	const state = h('span');
	const cards = h('div', { class: 'cards' });
	const cMsgs = new Chart('Messages / s', [['in', '--c1'], ['out', '--c2']]);
	const cBox = new Chart('Mailbox', [['waiting', '--c2']], { fmt: fmtInt });
	const facts = h('div', { class: 'kv' });
	const said = h('div', { class: 'kv' });
	const saidNote = h('span', { class: 'muted' });
	const kids = h('div');
	const pr = rates();
	let ticks = 0, gone = '', auto = false, every = 5000, asked = 0, last = null;

	async function ask() {
		asked = Date.now();
		saidNote.textContent = 'asking…';
		try {
			const v = await api('/api/process', { node, target: pid, inspect: 1, wait: '1s' });
			const keys = Object.keys(v.inspect || {}).sort();
			said.replaceChildren(...keys.flatMap((k) => [h('span', { class: 'k' }, k), h('span', { class: 'v' }, linkify(v.inspect[k]))]));
			said.hidden = !keys.length;
			saidNote.textContent = v.inspect_error ? v.inspect_error : keys.length ? `answered at ${new Date().toLocaleTimeString()}` : 'It publishes nothing: see grpcproc.WithInspect.';
		} catch (e) {
			saidNote.textContent = e.message;
		}
	}

	const level = h('select', null, LEVELS.map((l) => h('option', { value: l }, l)));
	const reason = h('input', { type: 'text', placeholder: 'reason (killed)' });
	const actions = app.info.allow_writes
		? [
				h('h3', null, 'Act'),
				h(
					'div',
					{ class: 'actions' },
					h('label', null, 'log level', select(level)),
					h('button', { type: 'button', onclick: () => act(`log level ${level.value}`, '/api/loglevel', { node, target: pid, level: level.value }) }, 'Set'),
					h('span', { class: 'spacer' }),
					reason,
					h(
						'button',
						{
							type: 'button',
							class: 'danger',
							onclick: () =>
								act(`asked ${last?.name || pid} to exit`, '/api/exit', { node, target: pid, reason: reason.value }, `Ask ${last?.name || pid} to exit with reason "${reason.value || 'killed'}"? Its supervisor, if any, may restart it.`),
						},
						'Exit',
					),
				),
			]
		: [h('h3', null, 'Act'), readOnlyNote()];

	const autoBox = h('input', { type: 'checkbox', onchange: (e) => ((auto = e.target.checked), auto && ask()) });
	const everySel = h('select', { value: '5000', onchange: (e) => (every = Number(e.target.value)) }, [1000, 5000, 10000, 30000].map((ms) => h('option', { value: ms }, `every ${ms / 1000}s`)));

	const el = h(
		'div',
		null,
		h('div', { class: 'dhead' }, h('div', null, h('div', { class: 'row' }, title, state), sub), h('span', { class: 'spacer' }), h('button', { type: 'button', class: 'icon-button', title: 'Close (Esc)', 'aria-label': 'Close', onclick: closeProc }, icon(XMARK))),
		b.el,
		cards,
		h('div', { class: 'charts' }, cMsgs.el, cBox.el),
		h('h3', null, 'What it says about itself'),
		h('div', { class: 'row' }, h('button', { type: 'button', onclick: ask }, 'Ask'), h('label', null, autoBox, 'keep asking'), select(everySel), saidNote),
		h('p', { class: 'muted' }, 'Asking takes the process a turn: it answers between messages, so a busy one keeps you waiting.'),
		said,
		h('h3', null, 'Process'),
		facts,
		h('h3', null, 'Started by it'),
		kids,
		actions,
	);

	return {
		pid,
		el,
		banner: (msg) => b.set(msg || gone),
		first: ask,
		async tick() {
			if (gone) return;
			let v;
			try {
				v = await api('/api/process', { node, target: pid });
			} catch (e) {
				if (e.status !== 404) throw e;
				gone = `${last?.name || pid} is not running on ${node} any more. Its exit, and why, shows on the Events page while that is open.`;
				state.replaceChildren(pill('gone', 'exiting'));
				return;
			}
			if (!last) level.value = LEVELS.find((l) => l === v.log_level.toLowerCase()) || 'info';
			last = v;
			const d = pr([v], (x) => x.pid, ['received', 'sent']).get(pid);
			title.textContent = v.name || v.label;
			sub.replaceChildren(h('span', { class: 'mono' }, pid), v.name ? ` · ${v.label}` : '', ' · ', h('span', { class: 'mono' }, v.type));
			state.replaceChildren(pill(v.state));
			cards.replaceChildren(
				card('Mailbox', fmtInt(v.mailbox), v.mailbox ? `oldest ${v.oldest_wait}, peak ${fmtInt(v.mailbox_peak)}` : `peak ${fmtInt(v.mailbox_peak)}`, v.mailbox > 0 && 'warn'),
				card('Received', fmtInt(v.received), d ? `${fmtRate(d.received)}/s` : ''),
				card('Sent', fmtInt(v.sent), d ? `${fmtRate(d.sent)}/s` : ''),
				card('Calls out', fmtInt(v.calls_in_flight || 0), v.state === 'waiting-reply' ? 'waiting on a reply' : ''),
				card('Uptime', v.uptime, `log level ${v.log_level}`),
			);
			cMsgs.push(d?.received, d?.sent);
			cBox.push(v.mailbox);
			const kv = [
				['parent', pidLink(v.parent) || h('span', { class: 'muted' }, 'none: spawned by the node')],
				['last message', v.last_message],
				['monitors', `${v.monitors || 0} held`],
				['links', `${v.links || 0}${v.trap_exit ? ', trapping exits' : ''}`],
				['watchers', `${v.watchers || 0} monitoring or linked to it`],
			];
			facts.replaceChildren(...kv.flatMap(([k, val]) => [h('span', { class: 'k' }, k), h('span', { class: 'v' }, val)]));
			if (ticks++ % 5 === 0) {
				const ps = await api('/api/processes', { node });
				const mine = ps.filter((x) => x.parent === pid).sort((a, c) => pidCmp(a.pid, c.pid));
				kids.replaceChildren(
					mine.length
						? h('div', { class: 'kv' }, mine.flatMap((x) => [h('span', { class: 'k' }, pidLink(x.pid)), h('span', { class: 'v' }, [x.name || x.label, ' ', pill(x.state), x.mailbox ? ` ${x.mailbox} waiting` : ''])]))
						: h('span', { class: 'muted' }, 'nothing on this node'),
				);
			}
			if (auto && Date.now() - asked >= every) await ask();
		},
	};
}

// ---- the app ------------------------------------------------------------

const pages = {
	cluster: { make: clusterPage, title: 'Cluster' },
	node: { make: nodePage, node: true, title: 'Node' },
	processes: { make: processesPage, node: true, title: 'Processes' },
	tree: { make: treePage, node: true, title: 'Supervision' },
	events: { make: eventsPage, node: true, title: 'Events' },
	cron: { make: cronPage, title: 'Cron' },
	elections: { make: electionsPage, title: 'Elections' },
};

const app = { info: { allow_writes: false }, nodes: [], params: {}, page: null, key: '', drawer: null, interval: 1000, timer: 0, busy: false };

const home = () => app.nodes[0]?.name || '';

function route() {
	const [path, q] = location.hash.replace(/^#\/?/, '').split('?');
	return { page: pages[path] ? path : 'cluster', params: Object.fromEntries(new URLSearchParams(q || '')) };
}

const href = (page, params) => '#/' + page + query(params);

// go opens page with params. With replace, the address changes in place:
// typing in a filter does not fill the history.
function go(page, params, replace) {
	const url = href(page, params || {});
	if (replace) {
		history.replaceState(null, '', url);
		onRoute();
	} else location.hash = url;
}

function set(changes, replace = true) {
	const r = route();
	go(r.page, { ...r.params, ...changes }, replace);
}

const openProc = (pid) => set({ proc: pid }, false);
const closeProc = () => set({ proc: '' }, false);

function onRoute() {
	const { page, params } = route();
	app.params = params;
	const node = params.node || home();
	selectNode(node);
	// A page starts over when it, or the node it shows, changes; other params
	// it takes as they come.
	const key = page + '|' + (pages[page].node ? node : '');
	if (key !== app.key) {
		app.page?.unmount?.();
		app.key = key;
		app.page = pages[page].make(node, params);
		$('#main').replaceChildren(app.page.el);
		document.title = `${pages[page].title} · grpcprocctl web`;
		for (const a of document.querySelectorAll('.side a')) a.classList.toggle('on', a.dataset.page === page);
		run(app.page);
	} else app.page.update?.(params);
	if ((params.proc || '') !== (app.drawer?.pid || '')) openDrawer(params.proc || '');
}

function openDrawer(pid) {
	const d = $('#drawer');
	app.drawer = null;
	d.hidden = !pid;
	d.replaceChildren();
	if (!pid) return;
	app.drawer = procDrawer(pid);
	d.append(app.drawer.el);
	run(app.drawer);
	app.drawer.first();
}

// run ticks a view once, showing what went wrong in its banner.
async function run(v) {
	v.last = Date.now();
	try {
		await v.tick();
		v.banner(null);
		live(true);
	} catch (e) {
		v.banner(e.message);
		live(false, e.message);
	}
}

async function tick() {
	if (app.busy || document.hidden) return;
	app.busy = true;
	const now = Date.now();
	await Promise.all([app.page, app.drawer].filter((v) => v && !(v.every && now - (v.last || 0) < v.every)).map(run));
	app.busy = false;
}

// refresh ticks now, as after a change: what it changed shows at once.
function refresh() {
	for (const v of [app.page, app.drawer]) if (v) run(v);
}

function live(ok, msg) {
	const el = $('#live');
	if (!app.interval) {
		el.className = 'live';
		el.textContent = 'paused';
		el.title = 'Click to refresh once';
		return;
	}
	el.className = ok ? 'live ok' : 'live bad';
	el.textContent = ok ? 'live' : 'unreachable';
	el.title = ok ? `Updated ${new Date().toLocaleTimeString()}. Click to refresh now.` : msg;
}

function startTimer() {
	clearInterval(app.timer);
	if (app.interval) app.timer = setInterval(tick, app.interval);
	live(true);
}

function setNodes(nodes) {
	app.nodes = nodes;
	selectNode(app.params.node || home());
}

function selectNode(node) {
	const sel = $('#node');
	const names = app.nodes.map((n) => n.name);
	if (!names.includes(node) && node) names.push(node);
	if (sel.options.length !== names.length || [...sel.options].some((o, i) => o.value !== names[i])) {
		sel.replaceChildren(...names.map((n) => h('option', { value: n }, n + (app.nodes.find((x) => x.name === n)?.error ? ' (unreachable)' : ''))));
	}
	sel.value = node;
}

// The theme follows the site's: a g-root_theme_* class on the root, the one
// chosen last with the button, or the system's, as theme.js settled it
// before the page painted.
function setTheme(theme) {
	const root = document.documentElement;
	root.classList.toggle('g-root_theme_dark', theme === 'dark');
	root.classList.toggle('g-root_theme_light', theme !== 'dark');
	redrawCharts();
}

function chosenTheme() {
	try {
		return localStorage.getItem('gp-theme');
	} catch {
		return null; // storage is off: follow the system
	}
}

function toggleTheme() {
	const theme = document.documentElement.classList.contains('g-root_theme_dark') ? 'light' : 'dark';
	try {
		localStorage.setItem('gp-theme', theme);
	} catch {
		// storage is off: the choice lasts as long as the page
	}
	setTheme(theme);
}

async function boot() {
	try {
		app.info = await api('/api/info');
	} catch (e) {
		live(false, e.message);
	}
	const mode = $('#mode');
	mode.textContent = app.info.allow_writes ? 'writes allowed' : 'read-only';
	mode.className = `label label_${app.info.allow_writes ? 'warning' : 'unknown'}`;
	$('#about').textContent = `grpcprocctl ${app.info.version || ''}\n${app.info.target || ''}`;
	$('#about').title = 'The Inspector this page reads through';
	try {
		app.nodes = await api('/api/nodes');
	} catch (e) {
		live(false, e.message);
	}
	$('#node').addEventListener('change', (e) => set({ node: e.target.value }, false));
	$('#interval').addEventListener('change', (e) => {
		app.interval = Number(e.target.value);
		startTimer();
	});
	$('#live').addEventListener('click', refresh);
	$('#theme').addEventListener('click', toggleTheme);
	addEventListener('hashchange', onRoute);
	addEventListener('resize', debounce(redrawCharts, 100));
	addEventListener('keydown', (e) => e.key === 'Escape' && app.drawer && closeProc());
	matchMedia('(prefers-color-scheme: dark)').addEventListener('change', (e) => chosenTheme() || setTheme(e.matches ? 'dark' : 'light'));
	// The node list follows the cluster while pages other than Cluster show.
	setInterval(async () => {
		if (app.interval && !document.hidden && !app.key.startsWith('cluster')) {
			try {
				setNodes(await api('/api/nodes'));
			} catch {
				// the page's own tick says so
			}
		}
	}, 15000);
	onRoute();
	startTimer();
}

boot();
