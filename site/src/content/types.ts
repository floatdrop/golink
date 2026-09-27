import type { ReactNode } from 'react';

/**
 * The code blocks and diagrams, ready to drop into a section. They are Go
 * source, output the Go tests pin, and drawings, so the words around them
 * never touch them, and a section reaches the ones it shows by name.
 */
export type FigureId =
	| 'tree'
	| 'layout'
	| 'inventoryProto'
	| 'inventoryAddress'
	| 'inventory'
	| 'payments'
	| 'orders'
	| 'flow'
	| 'web'
	| 'platform'
	| 'root'
	| 'lifecycle'
	| 'run'
	| 'modules'
	| 'config'
	| 'local'
	| 'localPicture'
	| 'localTree'
	| 'localShell'
	| 'front'
	| 'warehouse'
	| 'billing'
	| 'clusterPicture'
	| 'clusterTree'
	| 'clusterShell'
	| 'ordersTest'
	| 'clusterTest'
	| 'inspect';

export type Figures = Record<FigureId, ReactNode>;

/** The steps, in order; the id is the anchor. */
export type StepId =
	| 'shape'
	| 'contracts'
	| 'service'
	| 'calls'
	| 'orders'
	| 'edge'
	| 'platform'
	| 'local'
	| 'cluster'
	| 'testing'
	| 'inspect';

export interface Step {
	id: StepId;
	/** Shown in the table of contents and as the heading. */
	title: string;
	/** Prose with the figures interleaved where the text puts them. */
	body: (figures: Figures) => ReactNode;
}

/** Everything on the page that is words. */
export interface Content {
	meta: { title: string; description: string };

	hero: {
		title: string;
		lead: ReactNode;
		/** The install line, and what the copy button puts on the clipboard. */
		install: string;
	};

	labels: {
		steps: string;
		copy: string;
		toLight: string;
		toDark: string;
	};

	intro: ReactNode;

	/** The annotated directory listing in the first step. */
	tree: { path: string; comment: string }[];

	steps: Step[];

	footer: ReactNode;
}
