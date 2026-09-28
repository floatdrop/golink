import ArrowUpRightFromSquare from '@gravity-ui/icons/ArrowUpRightFromSquare';
import { Icon, Link, Text } from '../uikit.ts';

import { url } from '../config.ts';
import type { ReactNode } from 'react';

/**
 * Inline code inside a sentence. code-inline-3 is 16px against the 17px of
 * body-3, which is the one step that keeps it on the line; code-inline-2 is
 * 14px and reads as a different size of text rather than a different kind.
 */
export function C({ children }: { children: ReactNode }) {
	return (
		<Text as="code" variant="code-inline-3" className="gp-code">
			{children}
		</Text>
	);
}

/** A link to another page of the site, by its Doc.path, with an optional anchor. */
export function A({ to, children }: { to: string; children: ReactNode }) {
	const [path, hash] = to.split('#');
	return <Link href={url(path ?? '') + (hash ? `#${hash}` : '')}>{children}</Link>;
}

/** A link that leaves the site. */
export function Ext({ href, children }: { href: string; children: ReactNode }) {
	return (
		<Link href={href} className="gp-ext">
			{children}
			<Icon data={ArrowUpRightFromSquare} size={12} className="gp-ext__icon" />
		</Link>
	);
}

/**
 * An aside: something the main line of the page does not need, but a
 * reader with a particular background wants. `title` says who.
 */
export function Aside({ title, children }: { title: string; children: ReactNode }) {
	return (
		<aside className="gp-aside">
			<div className="gp-aside__title">{title}</div>
			<div className="gp-aside__body">{children}</div>
		</aside>
	);
}

/** A two-column table: a name, and what it means. */
export function Table({ head, rows }: { head?: [ReactNode, ReactNode]; rows: [ReactNode, ReactNode][] }) {
	return (
		<div className="gp-table-wrap">
			<table className="gp-table">
				{head && (
					<thead>
						<tr>
							<th>{head[0]}</th>
							<th>{head[1]}</th>
						</tr>
					</thead>
				)}
				<tbody>
					{rows.map((row, i) => (
						<tr key={i}>
							<td>{row[0]}</td>
							<td>{row[1]}</td>
						</tr>
					))}
				</tbody>
			</table>
		</div>
	);
}

/** A card each, linking onward: the landing page's map of the site. `to` is a page's path, or a full URL. */
export function Cards({ items }: { items: { to: string; title: string; text: ReactNode }[] }) {
	return (
		<div className="gp-cards">
			{items.map((item) => (
				<a key={item.to} className="gp-card" href={/^https?:/.test(item.to) ? item.to : url(item.to)}>
					<span className="gp-card__title">{item.title}</span>
					<span className="gp-card__text">{item.text}</span>
				</a>
			))}
		</div>
	);
}
