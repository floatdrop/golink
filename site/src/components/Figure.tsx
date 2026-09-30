import type { ReactNode } from 'react';

import { url } from '../config.ts';

/**
 * A captioned block: a file of the examples, output a Go test pins, a
 * fragment, or a drawing. The caption is the figure's own, so it names the
 * block for a screen reader as well; the card around both is drawn in
 * main.css. Without a caption, the card is the code alone.
 */
export function Figure({ caption, children }: { caption?: string | undefined; children: ReactNode }) {
	return (
		<figure className="gp-figure">
			{caption && <figcaption className="gp-figure__caption">{caption}</figcaption>}
			{children}
		</figure>
	);
}

/** Markup Shiki produced at build time. */
export function Highlighted({ html }: { html: string }) {
	return <div className="gp-figure__body" dangerouslySetInnerHTML={{ __html: html }} />;
}

/** Text shown as it is: a process tree, a Modules report, what a program printed. */
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
export function Drawing({ caption, children }: { caption: string; children: ReactNode }) {
	return (
		<Figure caption={caption}>
			<div className="gp-figure__drawing">{children}</div>
		</Figure>
	);
}

/**
 * A screenshot from public/screenshots, taken once in each theme as
 * <name>-light.webp and <name>-dark.webp; main.css shows the one that
 * matches the page's. Both are 2560x1600, a 1280x800 window at 2x.
 */
export function Screenshot({ caption, name, alt }: { caption: string; name: string; alt: string }) {
	return (
		<Figure caption={caption}>
			{(['light', 'dark'] as const).map((theme) => (
				<img
					key={theme}
					className={`gp-shot gp-shot_${theme}`}
					src={url(`screenshots/${name}-${theme}.webp`)}
					alt={alt}
					width={2560}
					height={1600}
					loading="lazy"
					decoding="async"
				/>
			))}
		</Figure>
	);
}
