'use strict';

// Settles the theme before the page paints, as the documentation site's
// inlined script does; the page's CSP allows no inline script, so it is a
// file of its own, loaded without defer. The theme is the one chosen last
// with the button, or the system's.
(() => {
	let theme = null;
	try {
		theme = localStorage.getItem('gp-theme');
	} catch {
		// storage is off: follow the system
	}
	if (theme !== 'dark' && theme !== 'light') theme = matchMedia('(prefers-color-scheme: dark)').matches ? 'dark' : 'light';
	document.documentElement.classList.add('g-root', `g-root_theme_${theme}`);
})();
