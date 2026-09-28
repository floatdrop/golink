/**
 * What the inlined script (src/inline-script.ts) looks for, named once here
 * because three places have to agree: the markup that carries the id, the
 * script that finds it, and the stylesheet that draws its states.
 */
export const THEME_TOGGLE = '#gp-theme';
export const COPY_BUTTON = '.gp-copy';
/** Worn by a copy button for a moment after a successful copy. */
export const COPY_DONE = 'gp-copy_done';

/** The sections the page's outline points at, and its links to them. */
export const SECTION = '.gp-section';
export const TOC_LINK = '.gp-outline a[href^="#"]';
/**
 * uikit's own active modifier, on the element it puts it on. Setting it is all
 * the highlight takes -- Toc ships the brand rail and the text colour, and
 * only ever applies them from the `value` prop, which a page with no React
 * cannot change as you scroll.
 */
export const TOC_ACTIVE = 'g-toc-item__section_active';

/**
 * The button that hides the site's navigation, and the class that is on the
 * root while it is hidden. The root, because the script settles it from the
 * head before the aside exists, and because what changes is the grid the
 * aside sits in, not the aside alone.
 */
export const NAV_TOGGLE = '#gp-nav-toggle';
export const NAV_HIDDEN = 'gp-nav-hidden';

/**
 * The GitHub button, and the count the script appends to it. The class is
 * uikit's own text-slot class plus ours, because the element is built at
 * runtime and has to look like the one uikit would have rendered.
 */
export const GITHUB_BUTTON = '#gp-github';
export const STARS_COUNT = '.gp-stars';
export const STARS_CLASS = 'g-button__text gp-stars';
