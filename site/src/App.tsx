import Check from '@gravity-ui/icons/Check';
import Copy from '@gravity-ui/icons/Copy';
import { Button, Icon, Text, ThemeProvider, Toc } from './uikit.ts';

import type { Code, Highlighted as Source } from './code.ts';
import { Drawing, Figure, Highlighted, Plain } from './components/Figure.tsx';
import { ClusterPicture, Flow, Layout, Lifecycle, LocalPicture } from './components/diagrams.tsx';
import { Mark } from './components/Mark.tsx';
import { Topbar } from './components/Topbar.tsx';
import type { Content, Figures } from './content/types.ts';
import type { ReactNode } from 'react';

/** Where each highlighted figure comes from, which is also its caption. */
const captions: Record<Source, string> = {
	inventoryProto: 'proto/inventory/v1/inventory.proto',
	inventoryAddress: 'proto/inventory/v1/address.go',
	inventory: 'internal/inventory/inventory.go',
	payments: 'internal/payments/payments.go',
	orders: 'internal/orders/orders.go',
	web: 'internal/web/web.go',
	platform: 'internal/platform/platform.go',
	root: 'internal/platform/root.go',
	run: 'internal/platform/run.go',
	config: 'internal/platform/config.go',
	local: 'cmd/local/main.go',
	front: 'cmd/front/main.go',
	warehouse: 'cmd/warehouse/main.go',
	billing: 'cmd/billing/main.go',
	ordersTest: 'internal/orders/orders_test.go',
	clusterTest: 'cluster_test.go',
	localShell: 'shell',
	clusterShell: 'shell',
	inspect: 'shell'
};

function buildFigures(code: Code, content: Content): Figures {
	const pad = Math.max(...content.tree.map((row) => row.path.length)) + 2;
	const source = (name: Source) => (
		<Figure caption={captions[name]}>
			<Highlighted html={code.html[name]} />
		</Figure>
	);
	const drawing = (caption: string, svg: ReactNode) => (
		<Figure caption={caption}>
			<Drawing>{svg}</Drawing>
		</Figure>
	);

	return {
		tree: (
			<Figure caption="examples/guide">
				<Plain>
					{content.tree.map((row) => (
						<span key={row.path}>
							{row.path.padEnd(pad)}
							<span className="gp-figure__comment">{row.comment}</span>
							{'\n'}
						</span>
					))}
				</Plain>
			</Figure>
		),
		layout: drawing('the four layers, and what imports the contracts', <Layout />),
		inventoryProto: source('inventoryProto'),
		inventoryAddress: source('inventoryAddress'),
		inventory: source('inventory'),
		payments: source('payments'),
		orders: source('orders'),
		flow: drawing('one order', <Flow />),
		web: source('web'),
		platform: source('platform'),
		root: source('root'),
		lifecycle: drawing('start and stop', <Lifecycle />),
		run: source('run'),
		modules: (
			<Figure caption="app.Modules(), as the tests compose cmd/local">
				<Plain>{code.modules}</Plain>
			</Figure>
		),
		config: source('config'),
		local: source('local'),
		localPicture: drawing('one program', <LocalPicture />),
		localTree: (
			<Figure caption="what the node runs">
				<Plain>{code.localTree}</Plain>
			</Figure>
		),
		localShell: source('localShell'),
		front: source('front'),
		warehouse: source('warehouse'),
		billing: source('billing'),
		clusterPicture: drawing('three programs', <ClusterPicture />),
		clusterTree: (
			<Figure caption="what each node runs">
				<Plain>{code.clusterTree}</Plain>
			</Figure>
		),
		clusterShell: source('clusterShell'),
		ordersTest: source('ordersTest'),
		clusterTest: source('clusterTest'),
		inspect: source('inspect')
	};
}

export interface AppProps {
	content: Content;
	code: Code;
}

export function App({ content, code }: AppProps) {
	const figures = buildFigures(code, content);
	const { hero, labels, steps } = content;

	return (
		<ThemeProvider theme="light" lang="en">
			<Topbar content={content} />

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
						<Button id="gp-copy" className="gp-copy" view="flat" size="s" aria-label={labels.copy}>
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
				<Mark className="gp-hero__art" />
			</header>

			<div className="gp-page gp-layout">
				<aside className="gp-toc" aria-label={labels.steps}>
					<Toc
						items={steps.map((step) => ({
							value: step.id,
							href: `#${step.id}`,
							content: step.title
						}))}
					/>
				</aside>

				<main className="gp-main gp-prose">
					<div className="gp-intro">{content.intro}</div>

					{steps.map((step, i) => (
						<section key={step.id} id={step.id} className="gp-section">
							<Text as="h2" variant="header-2" className="gp-section__heading">
								<span className="gp-section__number" aria-hidden="true">
									{i + 1}
								</span>
								{step.title}
							</Text>
							{step.body(figures)}
						</section>
					))}
				</main>
			</div>

			<footer className="gp-footer">
				<div className="gp-page">
					<Text as="p" variant="body-1" color="secondary">
						{content.footer}
					</Text>
				</div>
			</footer>
		</ThemeProvider>
	);
}
