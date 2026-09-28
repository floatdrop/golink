export const REPO = 'https://github.com/floatdrop/grpcproc';
export const PKG_DOC = 'https://pkg.go.dev/github.com/floatdrop/grpcproc';
export const SITE_TITLE = 'grpcproc';

/**
 * GitHub Pages serves a project site under /<repo>; the workflow sets
 * BASE_PATH=/grpcproc. Every in-site URL goes through `url`.
 */
export const BASE = (process.env['BASE_PATH'] ?? '').replace(/\/$/, '');

/**
 * An in-site URL, absolute from the site's root: the pages live at several
 * depths, so a relative URL would differ from page to page. `npm run
 * preview` serves the build under its base path, and `npm run dev` at the
 * root, which is what an absolute URL needs; opening build/ straight off the
 * filesystem is not supported.
 */
export const url = (path: string) => `${BASE}/${path.replace(/^\//, '')}`;

/** A file in the repository, on GitHub. */
export const file = (path: string) => `${REPO}/blob/main/${path}`;
