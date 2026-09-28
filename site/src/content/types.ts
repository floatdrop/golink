import type { ReactNode } from 'react';

/**
 * A heading on a page, and what is under it. The id is the anchor, and the
 * page's outline ("on this page") lists the sections in order.
 */
export interface Section {
	id: string;
	title: string;
	body: ReactNode;
}

/** One page of the site: its place, its words, and its sections. */
export interface Doc {
	/**
	 * Where the page is served, relative to the site's root and ending in a
	 * slash: 'concepts/actors/'. The landing page is ''.
	 */
	path: string;
	/** Shown in the navigation and as the heading. */
	title: string;
	/** One sentence: the meta description, and the lead under the heading. */
	description: string;
	/** Prose before the first section. */
	lead?: ReactNode;
	sections: Section[];
}

/** The words of the chrome around a page, named once. */
export interface Labels {
	nav: string;
	hideNav: string;
	showNav: string;
	onThisPage: string;
	copy: string;
	toLight: string;
	toDark: string;
	previous: string;
	next: string;
}
