import { renderToStaticMarkup } from 'react-dom/server';

import { App } from './App.tsx';
import { loadCode } from './code.ts';
import { url } from './config.ts';
import { content } from './content/en.tsx';
import { inlineScript } from './inline-script.ts';
import {
	COPY_BUTTON,
	COPY_DONE,
	GITHUB_BUTTON,
	SECTION,
	STARS_CLASS,
	STARS_COUNT,
	THEME_TOGGLE,
	TOC_ACTIVE,
	TOC_HIDDEN,
	TOC_LINK,
	TOC_TOGGLE
} from './selectors.ts';

// Importing the stylesheet here is what puts it in the build: every Gravity UI
// component imports its own CSS, so the server bundle's CSS is exactly the CSS
// the rendered markup needs, and `ssrEmitAssets` writes it out.
import './styles/main.css';

const escapeAttribute = (value: string) =>
	value.replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/"/g, '&quot;');

/** Safe to drop inside a <script> element: no `</script>` can come out. */
const literal = (value: unknown) => JSON.stringify(value).replace(/</g, '\\u003c');

export interface Page {
	/** Where the file goes, relative to the build directory. */
	file: string;
	html: string;
}

/** How the page gets its CSS, which is the one thing dev and the build differ on. */
export type Styles =
	/** The stylesheet the build emitted, relative to the build directory. */
	| { link: string }
	/** A module that pulls the same CSS in as a side effect; see src/dev-styles.ts. */
	| { module: string };

/** The site is one page. */
export async function render(styles: Styles): Promise<Page[]> {
	const code = await loadCode();
	return [{ file: 'index.html', html: page(styles, renderToStaticMarkup(<App content={content} code={code} />)) }];
}

function page(styles: Styles, body: string): string {
	const { hero, meta, labels } = content;

	// Runs before the body is parsed, so the theme is settled before the first
	// paint. See src/inline-script.ts for why it is inlined this way.
	const script = `(${inlineScript.toString()})(${literal({
		themeToggle: THEME_TOGGLE,
		copyButton: COPY_BUTTON,
		copyDone: COPY_DONE,
		section: SECTION,
		tocLink: TOC_LINK,
		tocActive: TOC_ACTIVE,
		// Below the sticky topbar, and below `scroll-margin-top` as well: a
		// section jumped to from the contents lands exactly on its scroll
		// margin, and a line at that same height makes it a coin toss whether
		// the section you just jumped to counts as reached.
		tocOffset: 88,
		tocToggle: TOC_TOGGLE,
		tocHidden: TOC_HIDDEN,
		githubButton: GITHUB_BUTTON,
		starsCount: STARS_COUNT,
		starsClass: STARS_CLASS,
		starsApi: 'https://api.github.com/repos/floatdrop/grpcproc',
		copyText: hero.install,
		labels: {
			toLight: labels.toLight,
			toDark: labels.toDark,
			hideToc: labels.hideSteps,
			showToc: labels.showSteps
		}
	})})`;

	const stylesheet =
		'link' in styles
			? `<link rel="stylesheet" href="${url(styles.link)}" />`
			: `<script type="module" src="${styles.module}"></script>`;

	return `<!doctype html>
<html lang="en" class="g-root g-root_theme_light">
	<head>
		<meta charset="utf-8" />
		<meta name="viewport" content="width=device-width, initial-scale=1" />
		<title>${escapeAttribute(meta.title)}</title>
		<meta name="description" content="${escapeAttribute(meta.description)}" />
		<link rel="icon" href="${url('favicon.svg')}" />
		${stylesheet}
		<script>${script}</script>
	</head>
	<body>${body}</body>
</html>
`;
}
