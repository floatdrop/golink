// One icon per import rather than the package's barrel, which reaches every
// icon there is and would be tree-shaken back down to these three.
import LogoGithub from '@gravity-ui/icons/LogoGithub';
import Moon from '@gravity-ui/icons/Moon';
import Sun from '@gravity-ui/icons/Sun';
import { Button, Icon } from '../uikit.ts';

import { REPO, url } from '../config.ts';
import type { Content } from '../content/types.ts';
import { Mark } from './Mark.tsx';

export function Topbar({ content }: { content: Content }) {
	const { labels } = content;

	return (
		<div className="gp-topbar">
			<div className="gp-page gp-topbar__inner">
				<a className="gp-topbar__brand" href={url('')}>
					<Mark className="gp-topbar__mark" />
					grpcproc
				</a>

				<div className="gp-controls">
					{/* Both icons are in the markup and CSS shows the one that
					    applies; labelled by the inlined script. */}
					<Button id="gp-theme" view="normal" size="m" aria-label={labels.toDark}>
						<Button.Icon>
							<span className="gp-theme-icon gp-theme-icon_on-light">
								<Icon data={Moon} size={16} />
							</span>
							<span className="gp-theme-icon gp-theme-icon_on-dark">
								<Icon data={Sun} size={16} />
							</span>
						</Button.Icon>
					</Button>

					{/* Rendered as the bare mark. The inlined script appends the star
					    count if GitHub answers, and uikit's own `:has(:only-child)`
					    rule then stops treating the button as icon-only and opens it
					    into a pill. With no JavaScript, no network or no answer, the
					    markup is exactly this and the circle is what it was. */}
					<Button id="gp-github" view="normal" size="m" pin="circle-circle" href={REPO} aria-label="GitHub">
						<Button.Icon>
							<Icon data={LogoGithub} size={16} />
						</Button.Icon>
					</Button>
				</div>
			</div>
		</div>
	);
}
