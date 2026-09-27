import type { ReactNode } from 'react';

/**
 * A captioned block: a file of the guide, output a Go test pins, or a
 * drawing. The caption is the figure's own, so it names the block for a
 * screen reader as well; the card around both is drawn in main.css.
 */
export function Figure({ caption, children }: { caption: string; children: ReactNode }) {
	return (
		<figure className="gp-figure">
			<figcaption className="gp-figure__caption">{caption}</figcaption>
			{children}
		</figure>
	);
}

/** Markup Shiki produced at build time. */
export function Highlighted({ html }: { html: string }) {
	return <div className="gp-figure__body" dangerouslySetInnerHTML={{ __html: html }} />;
}

/** Text shown as it is: a process tree, a Modules report. */
export function Plain({ children }: { children: ReactNode }) {
	return (
		<pre className="gp-figure__body">
			<code>{children}</code>
		</pre>
	);
}

/**
 * A drawing from src/components/diagrams.tsx, at the width of the column.
 * Below a width its text would be too small to read, and it scrolls instead.
 */
export function Drawing({ children }: { children: ReactNode }) {
	return <div className="gp-figure__drawing">{children}</div>;
}
