// The site's map: which pages there are, in which groups, in what order.
// Plain data with no React in it, so the dev server (scripts/serve.ts) can
// list the pages without rendering them. The pages themselves are under
// src/content/, one file each, registered in src/content/index.ts.

export interface NavGroup {
	title: string;
	/** Page paths, as Doc.path spells them, in reading order. */
	paths: string[];
}

export const groups: NavGroup[] = [
	{ title: 'Getting started', paths: ['', 'start/'] },
	{
		title: 'Concepts',
		paths: [
			'concepts/actors/',
			'concepts/processes/',
			'concepts/addressing/',
			'concepts/monitors-and-links/',
			'concepts/supervision/',
			'concepts/nodes/',
			'concepts/discovery/'
		]
	},
	{
		title: 'Guides',
		paths: [
			'guides/actors/',
			'guides/supervisors/',
			'guides/blocking-io/',
			'guides/pubsub/',
			'guides/configuration/',
			'guides/testing/',
			'guides/observability/',
			'guides/inspector/',
			'guides/etcd/',
			'guides/cron/',
			'guides/leader/'
		]
	},
	{
		title: 'Tutorial: the shop',
		paths: ['shop/', 'shop/services/', 'shop/platform/', 'shop/deployments/', 'shop/testing/']
	},
	{ title: 'Reference', paths: ['reference/errors/', 'reference/performance/'] }
];

/** Every page, in reading order. */
export const pagePaths = groups.flatMap((g) => g.paths);

/** Links in the navigation that leave the site. */
export const external = [
	{ title: 'API reference', href: 'https://pkg.go.dev/github.com/floatdrop/grpcproc' },
	{ title: 'Design notes', href: 'https://github.com/floatdrop/grpcproc/blob/main/docs/DESIGN.md' },
	{ title: 'Examples', href: 'https://github.com/floatdrop/grpcproc/tree/main/examples' }
];
