export interface InlineConfig {
	/** Selector for the theme button. */
	themeToggle: string;
	/** Selector for a copy button; its data-copy attribute is what it copies. */
	copyButton: string;
	/** Class a copy button wears for a moment after a successful copy. */
	copyDone: string;
	/** Selector for the sections the page's outline points at. */
	section: string;
	/** Selector for the outline's links. */
	tocLink: string;
	/** uikit's active modifier, set on each link's parent. */
	tocActive: string;
	/** How far below the viewport top a section counts as current. */
	tocOffset: number;
	/** Selector for the button that hides and shows the site's navigation. */
	navToggle: string;
	/** Class the root wears while the navigation is hidden. */
	navHidden: string;
	/** Selector for the GitHub button the star count is appended to. */
	githubButton: string;
	/** Selector for that count once it exists. */
	starsCount: string;
	/** Class it is created with. */
	starsClass: string;
	/** The GitHub API endpoint for this repository. */
	starsApi: string;
	labels: {
		/** Announced by the theme button while the dark theme is on. */
		toLight: string;
		/** Announced while the light theme is on. */
		toDark: string;
		/** Announced by the navigation button while the navigation is shown. */
		hideNav: string;
		/** Announced while it is hidden. */
		showNav: string;
	};
}

/**
 * The only JavaScript the page ships. It is inlined into the head by calling
 * `Function.prototype.toString` on it (see entry-server.tsx), which is why it
 * closes over nothing and takes what it needs as an argument: everything it
 * refers to has to survive being cut out of the module.
 *
 * It runs before the body is parsed, so the theme class is right before the
 * first paint and there is no flash. Nothing it drives exists yet at that
 * point, which is why every handler is delegated to the document.
 */
export function inlineScript(config: InlineConfig) {
	const KEY = 'gp-theme';
	const DARK = 'g-root_theme_dark';
	const LIGHT = 'g-root_theme_light';
	const root = document.documentElement;
	const darkMedia = window.matchMedia('(prefers-color-scheme: dark)');

	/** What was chosen last, or null; storage can be denied outright. */
	function chosen(): string | null {
		try {
			return localStorage.getItem(KEY);
		} catch {
			return null;
		}
	}

	function system(): string {
		return darkMedia.matches ? 'dark' : 'light';
	}

	function apply(theme: string) {
		root.classList.toggle(DARK, theme === 'dark');
		root.classList.toggle(LIGHT, theme !== 'dark');
		const button = document.querySelector(config.themeToggle);
		if (button) {
			button.setAttribute('aria-label', theme === 'dark' ? config.labels.toLight : config.labels.toDark);
		}
	}

	/**
	 * Hides or shows the site's navigation. Hidden is a choice kept across
	 * visits, like the theme, and settled from the head for the same reason: a
	 * page that drew its navigation and then took it away would jump the prose
	 * sideways under the reader.
	 */
	const NAV_KEY = 'gp-nav';

	function navShown(): boolean {
		try {
			return localStorage.getItem(NAV_KEY) !== 'hidden';
		} catch {
			return true;
		}
	}

	function showNav(shown: boolean) {
		root.classList.toggle(config.navHidden, !shown);
		const button = document.querySelector(config.navToggle);
		if (button) {
			button.setAttribute('aria-expanded', String(shown));
			button.setAttribute('aria-label', shown ? config.labels.hideNav : config.labels.showNav);
		}
	}

	/**
	 * Marks the section being read in the page's outline. uikit's Toc takes
	 * its active item from a `value` prop, and this page has no React to change
	 * one, so the class it would have set is set here instead.
	 */
	function spy() {
		const sections = document.querySelectorAll(config.section);
		// The first section stands in until one has actually passed the line, so
		// the rail is drawn on arrival rather than on the first scroll.
		let current = sections.item(0)?.id ?? null;
		sections.forEach(function (section) {
			if (section.getBoundingClientRect().top <= config.tocOffset) {
				current = section.id;
			}
		});
		// The last sections are shorter than the viewport, so the page runs out
		// of scroll before their tops reach the line and they could never be
		// current. At the bottom, the last one is what you are reading.
		if (window.innerHeight + window.scrollY >= document.documentElement.scrollHeight - 2) {
			current = sections.item(sections.length - 1)?.id ?? current;
		}
		document.querySelectorAll(config.tocLink).forEach(function (link) {
			const on = current !== null && link.getAttribute('href') === '#' + current;
			link.parentElement?.classList.toggle(config.tocActive, on);
		});
	}

	/**
	 * The star count, which is the one thing on the page that comes from
	 * somewhere else. Last visit's number goes in first so the button does not
	 * change width halfway through reading, and a failure of any kind -- denied
	 * storage, rate limit, no network, GitHub down -- leaves the button as
	 * rendered rather than showing an error nobody asked about.
	 */
	function stars() {
		const button = document.querySelector(config.githubButton);
		if (!button) {
			return;
		}

		function show(text: string) {
			let el = button!.querySelector(config.starsCount);
			if (!el) {
				el = document.createElement('span');
				el.className = config.starsClass;
				button!.appendChild(el);
			}
			el.textContent = text;
		}

		const KEY = 'gp-stars';
		try {
			const cached = localStorage.getItem(KEY);
			if (cached) {
				show(cached);
			}
		} catch {
			// No storage; the fetch below is the only source then.
		}
		fetch(config.starsApi, { headers: { accept: 'application/vnd.github+json' } })
			.then(function (response) {
				return response.ok ? response.json() : null;
			})
			.then(function (repo) {
				const count: unknown = repo?.stargazers_count;
				if (typeof count !== 'number') {
					return;
				}
				const text =
					count >= 1000 ? (count / 1000).toFixed(1).replace(/\.0$/, '') + 'k' : String(count);
				show(text);
				try {
					localStorage.setItem(KEY, text);
				} catch {
					// Nothing to carry to the next visit.
				}
			})
			.catch(function () {
				// Left as rendered.
			});
	}

	apply(chosen() ?? system());
	showNav(navShown());

	// The first calls settle the theme and the contents before anything
	// paints, which is the point of running in the head -- but the buttons they
	// label are parsed after them, so the labels are set again once there is
	// something to set them on. The same goes for everything else here: none of
	// it exists yet.
	function ready() {
		apply(root.classList.contains(DARK) ? 'dark' : 'light');
		showNav(!root.classList.contains(config.navHidden));

		stars();

		let queued = false;
		spy();
		document.addEventListener(
			'scroll',
			function () {
				if (queued) {
					return;
				}
				queued = true;
				requestAnimationFrame(function () {
					queued = false;
					spy();
				});
			},
			{ passive: true }
		);
	}

	if (document.readyState === 'loading') {
		document.addEventListener('DOMContentLoaded', ready);
	} else {
		ready();
	}

	document.addEventListener('click', function (event) {
		const target = event.target;
		if (!(target instanceof Element)) {
			return;
		}

		if (target.closest(config.themeToggle)) {
			const next = root.classList.contains(DARK) ? 'light' : 'dark';
			try {
				localStorage.setItem(KEY, next);
			} catch {
				// Storage is denied; the choice holds for this page only.
			}
			apply(next);
			return;
		}

		if (target.closest(config.navToggle)) {
			const show = root.classList.contains(config.navHidden);
			try {
				if (show) {
					localStorage.removeItem(NAV_KEY);
				} else {
					localStorage.setItem(NAV_KEY, 'hidden');
				}
			} catch {
				// Storage is denied; the navigation stays as set for this page only.
			}
			showNav(show);
			return;
		}

		const copy = target.closest(config.copyButton);
		const text = copy?.getAttribute('data-copy');
		if (copy && text && navigator.clipboard) {
			navigator.clipboard.writeText(text).then(
				function () {
					copy.classList.add(config.copyDone);
					setTimeout(function () {
						copy.classList.remove(config.copyDone);
					}, 1600);
				},
				function () {
					// Denied. Nothing to say about it that the page can say well.
				}
			);
			return;
		}
	});

	// Someone who never used the button keeps following the system.
	darkMedia.addEventListener('change', function () {
		if (!chosen()) {
			apply(system());
		}
	});
}
