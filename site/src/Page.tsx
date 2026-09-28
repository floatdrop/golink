import ArrowLeft from '@gravity-ui/icons/ArrowLeft';
import ArrowRight from '@gravity-ui/icons/ArrowRight';
import ArrowUpRightFromSquare from '@gravity-ui/icons/ArrowUpRightFromSquare';
import Check from '@gravity-ui/icons/Check';
import Copy from '@gravity-ui/icons/Copy';
import LayoutSideContentLeft from '@gravity-ui/icons/LayoutSideContentLeft';
import { Button, Icon, Text, ThemeProvider, Toc } from './uikit.ts';

import { Topbar } from './components/Topbar.tsx';
import { PKG_DOC, REPO, url } from './config.ts';
import { hero } from './content/landing.tsx';
import { labels } from './content/labels.ts';
import type { Doc } from './content/types.ts';
import { external, groups } from './nav.ts';

export interface PageProps {
	doc: Doc;
	/** Every page, in reading order, for the navigation and the previous/next links. */
	docs: Doc[];
}

/** The site's navigation: the groups of pages, the current one marked. */
function Nav({ doc, docs }: PageProps) {
	const byPath = new Map(docs.map((d) => [d.path, d]));
	return (
		<nav className="gp-nav__groups" aria-label={labels.nav}>
			{groups.map((group) => (
				<div key={group.title} className="gp-nav__group">
					<div className="gp-nav__group-title">{group.title}</div>
					<ul className="gp-nav__list">
						{group.paths.map((path) => {
							const page = byPath.get(path);
							if (!page) {
								throw new Error(`nav: no page at ${JSON.stringify(path)}`);
							}
							const current = page.path === doc.path;
							return (
								<li key={path}>
									<a
										className={current ? 'gp-nav__link gp-nav__link_current' : 'gp-nav__link'}
										href={url(path)}
										aria-current={current ? 'page' : undefined}
									>
										{page.title}
									</a>
								</li>
							);
						})}
					</ul>
				</div>
			))}
			<div className="gp-nav__group">
				<div className="gp-nav__group-title">Elsewhere</div>
				<ul className="gp-nav__list">
					{external.map((link) => (
						<li key={link.href}>
							<a className="gp-nav__link gp-nav__link_external" href={link.href}>
								{link.title}
								<Icon data={ArrowUpRightFromSquare} size={12} />
							</a>
						</li>
					))}
				</ul>
			</div>
		</nav>
	);
}

/** The landing page's opening: the pitch, the install line, and the scene. */
function Hero() {
	return (
		<header className="gp-page gp-hero">
			<div className="gp-hero__copy">
				<Text as="h1" variant="display-3" className="gp-hero__title">
					{hero.title}
				</Text>
				<Text as="p" variant="body-3" color="secondary" className="gp-hero__lead">
					{hero.lead}
				</Text>
				<div className="gp-hero__install">
					<code className="gp-hero__install-code">{hero.install}</code>
					{/* Driven by the inlined script; the icons swap on a class. */}
					<Button className="gp-copy" view="flat" size="s" aria-label={labels.copy} data-copy={hero.install}>
						<Button.Icon>
							<span className="gp-copy-icon gp-copy-icon_idle">
								<Icon data={Copy} size={16} />
							</span>
							<span className="gp-copy-icon gp-copy-icon_done">
								<Icon data={Check} size={16} />
							</span>
						</Button.Icon>
					</Button>
				</div>
			</div>
			{/* The scene: a node, its processes, and the nodes it is linked to. The
			    lead says the same in words, so it is decorative. */}
			<img className="gp-hero__art" src={url('scene.svg')} alt="" width={560} height={320} />
		</header>
	);
}

/** Where the reading order goes from here, and where it came from. */
function PrevNext({ doc, docs }: PageProps) {
	const i = docs.findIndex((d) => d.path === doc.path);
	const prev = docs[i - 1];
	const next = docs[i + 1];
	if (!prev && !next) {
		return null;
	}
	return (
		<nav className="gp-prevnext" aria-label="Previous and next page">
			{prev ? (
				<a className="gp-prevnext__link gp-prevnext__link_prev" href={url(prev.path)} rel="prev">
					<span className="gp-prevnext__label">
						<Icon data={ArrowLeft} size={14} /> {labels.previous}
					</span>
					<span className="gp-prevnext__title">{prev.title}</span>
				</a>
			) : (
				<span />
			)}
			{next && (
				<a className="gp-prevnext__link gp-prevnext__link_next" href={url(next.path)} rel="next">
					<span className="gp-prevnext__label">
						{labels.next} <Icon data={ArrowRight} size={14} />
					</span>
					<span className="gp-prevnext__title">{next.title}</span>
				</a>
			)}
		</nav>
	);
}

export function Page({ doc, docs }: PageProps) {
	const landing = doc.path === '';
	const outline = doc.sections.length > 1;

	return (
		<ThemeProvider theme="light" lang="en">
			<Topbar />

			{landing && <Hero />}

			<div className="gp-page gp-layout">
				<aside className="gp-nav">
					<div className="gp-nav__head">
						{/* Driven by the inlined script, which keeps its label and
						    aria-expanded in step with the class it sets on the root.
						    It leads the row so that hiding the list leaves it where
						    it was, under the pointer, to bring the list back. */}
						<Button
							id="gp-nav-toggle"
							view="flat-secondary"
							size="s"
							aria-label={labels.hideNav}
							aria-expanded={true}
							aria-controls="gp-nav-list"
						>
							<Button.Icon>
								<Icon data={LayoutSideContentLeft} size={16} />
							</Button.Icon>
						</Button>
						<span className="gp-nav__title">{labels.nav}</span>
					</div>
					<div id="gp-nav-list" className="gp-nav__body">
						<Nav doc={doc} docs={docs} />
					</div>
				</aside>

				<main className="gp-main gp-prose">
					{/* The same navigation, folded, for widths where the aside is not shown. */}
					<details className="gp-nav-mobile">
						<summary className="gp-nav-mobile__summary">{labels.nav}</summary>
						<Nav doc={doc} docs={docs} />
					</details>

					{!landing && (
						<header className="gp-doc-head">
							<Text as="h1" variant="display-1" className="gp-doc-head__title">
								{doc.title}
							</Text>
							<Text as="p" variant="body-3" color="secondary" className="gp-doc-head__lead">
								{doc.description}
							</Text>
						</header>
					)}

					{doc.lead && <div className="gp-intro">{doc.lead}</div>}

					{doc.sections.map((section) => (
						<section key={section.id} id={section.id} className="gp-section">
							<Text as="h2" variant="header-2" className="gp-section__heading">
								<a className="gp-section__anchor" href={`#${section.id}`} aria-hidden="true">
									#
								</a>
								{section.title}
							</Text>
							{section.body}
						</section>
					))}

					<PrevNext doc={doc} docs={docs} />
				</main>

				<aside className="gp-outline" aria-label={labels.onThisPage}>
					{outline && (
						<>
							<div className="gp-outline__title">{labels.onThisPage}</div>
							<Toc
								items={doc.sections.map((section) => ({
									value: section.id,
									href: `#${section.id}`,
									content: section.title
								}))}
							/>
						</>
					)}
				</aside>
			</div>

			<footer className="gp-footer">
				<div className="gp-page">
					<Text as="p" variant="body-1" color="secondary">
						<a href={REPO}>github.com/floatdrop/grpcproc</a> · <a href={PKG_DOC}>pkg.go.dev</a> · MIT
						licensed · This site is built from the repository's <code>site/</code> directory, and the
						code it shows from <code>examples/</code>; its build is adapted from{' '}
						<a href="https://github.com/yandex/di">golang.yandex/di</a>'s site.
					</Text>
				</div>
			</footer>
		</ThemeProvider>
	);
}

/** The page GitHub Pages serves for a path that does not exist. */
export function NotFound({ docs }: { docs: Doc[] }) {
	return (
		<ThemeProvider theme="light" lang="en">
			<Topbar />
			<main className="gp-page gp-prose gp-notfound">
				<Text as="h1" variant="display-1" className="gp-doc-head__title">
					No page here
				</Text>
				<p>
					The address may be from an older layout of this site. The pages are listed on the{' '}
					<a href={url('')}>front page</a>; these are the ones most looked for:
				</p>
				<ul>
					{docs
						.filter((d) => ['start/', 'concepts/actors/', 'concepts/supervision/', 'shop/'].includes(d.path))
						.map((d) => (
							<li key={d.path}>
								<a href={url(d.path)}>{d.title}</a>
							</li>
						))}
				</ul>
			</main>
		</ThemeProvider>
	);
}
