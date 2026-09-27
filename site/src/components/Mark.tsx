/**
 * The grpcproc mark: a node, and three processes and the links between them, the
 * same drawing as public/favicon.svg and the centre of scene.svg. Decorative: the
 * text beside it names it.
 */
export function Mark({ className }: { className: string }) {
	return (
		<svg className={className} viewBox="0 0 32 32" aria-hidden="true" focusable="false">
			<polygon points="16.00,1.50 28.56,8.75 28.56,23.25 16.00,30.50 3.44,23.25 3.44,8.75" fill="none" stroke="#3b82c4" strokeWidth="2" strokeLinejoin="round" />
			<path d="M16 9 L10 19.5 L22 19.5 Z" fill="none" stroke="#3b82c4" strokeWidth="1.5" strokeLinejoin="round" />
			<circle cx="16" cy="9" r="4" fill="#7fd0e5" />
			<circle cx="10" cy="19.5" r="4" fill="#2a9d8f" />
			<circle cx="22" cy="19.5" r="4" fill="#e76f51" />
		</svg>
	);
}
