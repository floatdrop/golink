import { renderToStaticMarkup } from 'react-dom/server';

import { SITE_TITLE, url } from './config.ts';
import { docs } from './content/index.ts';
import { labels } from './content/labels.ts';
import type { Doc } from './content/types.ts';
import { inlineScript } from './inline-script.ts';
import { NotFound, Page } from './Page.tsx';
import {
	COPY_BUTTON,
	COPY_DONE,
	GITHUB_BUTTON,
	NAV_HIDDEN,
	NAV_TOGGLE,
	SECTION,
	STARS_CLASS,
	STARS_COUNT,
	THEME_TOGGLE,
	TOC_ACTIVE,
	TOC_LINK
} from './selectors.ts';

// Importing the stylesheet here is what puts it in the build: every Gravity UI
// component imports its own CSS, so the server bundle's CSS is exactly the CSS
// the rendered markup needs, and `ssrEmitAssets` writes it out.
import './styles/main.css';

const escapeAttribute = (value: string) =>
	value.replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/"/g, '&quot;');

/** Safe to drop inside a <script> element: no `</script>` can come out. */
const literal = (value: unknown) => JSON.stringify(value).replace(/</g, '\\u003c');

export interface RenderedPage {
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

/** Every page of the site, and the one for addresses that have none. */
export async function render(styles: Styles): Promise<RenderedPage[]> {
	const pages = docs.map((doc) => ({
		file: `${doc.path}index.html`,
		html: document(styles, doc, renderToStaticMarkup(<Page doc={doc} docs={docs} />))
	}));
	pages.push({
		file: '404.html',
		html: document(styles, null, renderToStaticMarkup(<NotFound docs={docs} />))
	});
	return pages;
}

function document(styles: Styles, doc: Doc | null, body: string): string {
	const title = doc === null ? `Not found · ${SITE_TITLE}` : doc.path === '' ? `${SITE_TITLE}: Erlang-style processes for Go` : `${doc.title} · ${SITE_TITLE}`;
	const description = doc?.description ?? '';

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
		// section jumped to from the outline lands exactly on its scroll
		// margin, and a line at that same height makes it a coin toss whether
		// the section you just jumped to counts as reached.
		tocOffset: 88,
		navToggle: NAV_TOGGLE,
		navHidden: NAV_HIDDEN,
		githubButton: GITHUB_BUTTON,
		starsCount: STARS_COUNT,
		starsClass: STARS_CLASS,
		starsApi: 'https://api.github.com/repos/floatdrop/grpcproc',
		labels: {
			toLight: labels.toLight,
			toDark: labels.toDark,
			hideNav: labels.hideNav,
			showNav: labels.showNav
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
		<title>${escapeAttribute(title)}</title>
		<meta name="description" content="${escapeAttribute(description)}" />
		<link rel="icon" href="${url('favicon.svg')}" />
		${stylesheet}
		<script>${script}</script>
	</head>
	<body>${body}</body>
</html>
`;
}
