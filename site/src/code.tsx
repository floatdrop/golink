// Code on the pages: Go, protobuf and shell, highlighted once at build time
// with Shiki, so the pages ship the markup and no JavaScript. The
// highlighter is synchronous, which lets a page call `Code` inline, like any
// other component, instead of listing its blocks ahead of rendering.
import { createHighlighterCoreSync } from 'shiki/core';
import { createJavaScriptRegexEngine } from 'shiki/engine/javascript';
import go from '@shikijs/langs/go';
import proto from '@shikijs/langs/proto';
import sh from '@shikijs/langs/shellscript';
import githubDark from '@shikijs/themes/github-dark';
import githubLight from '@shikijs/themes/github-light';

import { Figure, Highlighted, Plain } from './components/Figure.tsx';

export type Lang = 'go' | 'proto' | 'sh' | 'txt';

const highlighter = createHighlighterCoreSync({
	themes: [githubLight, githubDark],
	langs: [go, proto, sh],
	engine: createJavaScriptRegexEngine()
});

// Both themes are emitted as custom properties on every token; the theme
// class on the html element picks one, so switching re-highlights nothing.
export const highlight = (code: string, lang: Lang) =>
	highlighter.codeToHtml(code.trimEnd(), {
		lang: lang === 'sh' ? 'shellscript' : lang,
		themes: { light: 'github-light', dark: 'github-dark' },
		defaultColor: false
	});

/**
 * A block of code. `caption` names where it comes from -- a file of the
 * examples, or `shell` -- and a block with no caption is a fragment written
 * for the page.
 */
export function Code({ lang = 'go', caption, children }: { lang?: Lang; caption?: string; children: string }) {
	const body = lang === 'txt' ? <Plain>{children.trimEnd()}</Plain> : <Highlighted html={highlight(children, lang)} />;
	return <Figure caption={caption}>{body}</Figure>;
}

/** What a program printed, as its tests pin it. */
export function Output({ caption = 'output', children }: { caption?: string; children: string }) {
	return (
		<Figure caption={caption}>
			<Plain>{children.trimEnd()}</Plain>
		</Figure>
	);
}

/**
 * The lines of `source` from the first line matching `from` to the first
 * line after it matching `to`, both included, as embedmd cuts them. The
 * examples are whole programs, and a page shows the part it is about.
 */
export function region(source: string, from: RegExp, to: RegExp): string {
	const lines = source.split('\n');
	const start = lines.findIndex((l) => from.test(l));
	if (start < 0) {
		throw new Error(`region: no line matches ${from}`);
	}
	const end = lines.findIndex((l, i) => i > start && to.test(l));
	if (end < 0) {
		throw new Error(`region: no line after ${from} matches ${to}`);
	}
	return dedent(lines.slice(start, end + 1).join('\n'));
}

/** Strips the indentation the first line has from every line. */
function dedent(code: string): string {
	const indent = /^\t*/.exec(code)?.[0] ?? '';
	if (!indent) {
		return code;
	}
	return code
		.split('\n')
		.map((l) => (l.startsWith(indent) ? l.slice(indent.length) : l))
		.join('\n');
}
