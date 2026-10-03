#!/usr/bin/env bash
# Tags a release of grpcproc and its nested modules. See RELEASING.md.
#
#   scripts/release.sh v0.6.0        a minor release: the core, then every
#                                    nested module pinned to it and tagged
#                                    with the same version
#   scripts/release.sh v0.6.1        a patch of the core alone
#   scripts/release.sh otel/v0.6.1   a patch of one nested module, which must
#                                    require the core of the same minor
#
# -n prints the plan and stops; -y skips the confirmation. Run it on main,
# clean and up to date with origin. It pushes tags, and for a minor release
# the commit that pins the nested modules, to origin.
set -euo pipefail

core=github.com/floatdrop/grpcproc
# The nested modules that are released, in the order they are pinned. One
# may require another only of an earlier release: the pin commit cannot
# require a tag it comes before.
modules=(otel etcd tools)
# Nested modules that are not released, whose requirement on the core is
# bumped with the pin all the same: they build against the working tree.
unreleased=(examples benchmarks)

dry=false yes=false
while getopts ny opt; do
	case $opt in
	n) dry=true ;;
	y) yes=true ;;
	*) exit 2 ;;
	esac
done
shift $((OPTIND - 1))
[ $# -eq 1 ] || { sed -n '2,15p' "$0" | sed 's/^# \{0,1\}//'; exit 2; }

die() { echo "release: $*" >&2; exit 1; }
run() { echo "+ $*"; $dry || "$@"; }

arg=$1
module=${arg%/*}
version=${arg##*/}
[ "$module" = "$arg" ] && module=
[[ $version =~ ^v(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)$ ]] ||
	die "$version is not vMAJOR.MINOR.PATCH"
minor=v${BASH_REMATCH[1]}.${BASH_REMATCH[2]}
patch=${BASH_REMATCH[3]}
if [ -n "$module" ]; then
	[[ " ${modules[*]} " == *" $module "* ]] || die "$module is not a released module: ${modules[*]}"
fi

cd "$(git rev-parse --show-toplevel)"
[ "$(git branch --show-current)" = main ] || die "not on main"
[ -z "$(git status --porcelain)" ] || die "the working tree is not clean"
git fetch -q --tags origin
[ "$(git rev-parse HEAD)" = "$(git rev-parse origin/main)" ] || die "main is not origin/main"

# tag prefix of a module: "" for the core, "otel/" for otel
prefix() { [ -n "$1" ] && echo "$1/" || true; }
# newer reports whether $2 is a later version than $1
newer() { [ "$1" != "$2" ] && [ "$(printf '%s\n%s\n' "$1" "$2" | sort -V | tail -1)" = "$2" ]; }
# fresh checks that tag is new, and later than the module's latest
fresh() {
	local m=$1 tag latest
	tag=$(prefix "$m")$version
	git rev-parse -q --verify "refs/tags/$tag" >/dev/null && die "$tag exists"
	latest=$(git tag -l "$(prefix "$m")v*" | sed "s|^$(prefix "$m")||" | sort -V | tail -1)
	[ -z "$latest" ] || newer "$latest" "$version" || die "$tag is not later than $(prefix "$m")$latest"
}
# requires prints the core version module m's go.mod requires
requires() { (cd "$1" && go list -m -f '{{.Version}}' "$core"); }
title() { [ -n "$1" ] && echo "grpcproc/$1 $version" || echo "grpcproc $version"; }
# list joins its arguments as "a, b and c"
list() {
	local out=$1
	shift
	while [ $# -gt 1 ]; do out+=", $1"; shift; done
	[ $# -eq 1 ] && out+=" and $1"
	echo "$out"
}
# opened checks that a patch of module m has its minor's first release to
# patch
opened() {
	local t
	t=$(prefix "$1")$minor.0
	git rev-parse -q --verify "refs/tags/$t" >/dev/null || die "$(prefix "$1")$version patches $t, which was never released"
}

tagged=()
tag() {
	local t
	t=$(prefix "$1")$version
	run git tag -a "$t" -m "$(title "$1")"
	tagged+=("$t")
}

if [ -n "$module" ]; then
	# A patch of one nested module: it must stay on its minor's core.
	fresh "$module"
	[ "$patch" = 0 ] || opened "$module"
	req=$(requires "$module")
	[[ $req == "$minor".* ]] || die "$module requires the core at $req, not $minor.x"
	echo "Tag $module/$version, on the core $req."
elif [ "$patch" != 0 ]; then
	fresh ""
	opened ""
	echo "Tag $version, a patch of the core alone; the nested modules stay as they are."
else
	fresh ""
	for m in "${modules[@]}"; do fresh "$m"; done
	echo "Tag $version, pin $(list "${modules[@]}" "${unreleased[@]}") to it in a commit to main,"
	echo "then tag $(list $(printf "%s/$version " "${modules[@]}")) on that commit."
fi
if ! $dry && ! $yes; then
	# read fails at once without a terminal, which set -e would end the
	# script on without a word.
	[ -t 0 ] || die "no terminal to confirm on: pass -y once the plan above is right"
	read -r -p "Proceed? [y/N] " ok || true
	[ "$ok" = y ] || die "stopped"
fi

if [ -n "$module" ] || [ "$patch" != 0 ]; then
	tag "$module"
	run git push origin "${tagged[@]}"
	exit 0
fi

# A minor release. The core first: the nested modules can only require a
# version the module proxy serves.
tag ""
run git push origin "$version"

for m in "${modules[@]}"; do
	# Requirements on modules of this repository that are modules no more
	# (cron and leader, folded into the core at v0.6.0) go with the pin:
	# their packages come from the core now, and both would be ambiguous.
	gone=()
	while read -r dep; do
		[ "$dep" = "$core" ] && continue
		[ -f "${dep#"$core"/}/go.mod" ] || gone+=("$dep@none")
	done < <(cd "$m" && go list -m -f '{{if not .Main}}{{.Path}}{{end}}' all | grep "^$core" || true)
	# The proxy learns of a new tag on the first request for it, which
	# can fail for a minute while it fetches.
	for try in 1 2 3 4 5 6; do
		if (cd "$m" && run go get "$core@$version" ${gone[@]+"${gone[@]}"}); then break; fi
		[ $try = 6 ] && die "the proxy does not serve $core@$version"
		sleep 20
	done
	(cd "$m" && run go mod tidy && run go build ./...)
done
for m in "${unreleased[@]}"; do
	(cd "$m" && run go mod edit -require="$core@$version")
done
if ! $dry; then
	for m in "${modules[@]}"; do
		[ "$(requires "$m")" = "$version" ] || die "$m requires the core at $(requires "$m")"
	done
fi
run git commit -qam "Pin $(list "${modules[@]}" "${unreleased[@]}") to grpcproc $version"
run git push origin HEAD:main
for m in "${modules[@]}"; do tag "$m"; done
run git push origin "${tagged[@]:1}"

echo
echo "Tagged ${tagged[*]}. Write the release notes:"
echo "  gh release create $version --draft --title '$(title "")' --notes-file NOTES.md"
