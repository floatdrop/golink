/**
 * The grpcproc mark: three processes and the links between them, the same
 * drawing as public/favicon.svg. Decorative: the text beside it names it.
 */
export function Mark({ className }: { className: string }) {
	return (
		<svg className={className} viewBox="0 0 32 32" aria-hidden="true" focusable="false">
			<path d="M16 8.5 L8.5 22.5 L23.5 22.5 Z" fill="none" stroke="#3b82c4" strokeWidth="2.2" strokeLinejoin="round" />
			<circle cx="16" cy="8.5" r="4.6" fill="#3b82c4" />
			<circle cx="8.5" cy="22.5" r="4.6" fill="#2a9d8f" />
			<circle cx="23.5" cy="22.5" r="4.6" fill="#e76f51" />
		</svg>
	);
}
