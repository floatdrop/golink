# Releasing

The repository holds the core module, `github.com/floatdrop/grpcproc`, and
three nested modules that are released: `otel`, `etcd` and `tools`. Each
has its own tags (`v0.6.0`, `otel/v0.6.0`, …). `cron` and `leader` are
packages of the core, released with it. `examples` and `benchmarks` are
modules too, but never released: they build against the working tree.

## Versions

**The minor version is shared.** A minor release tags every module with the
same version, changed or not, and pins each nested module to the core of
that version. So `X.Y` of any module requires the core at `X.Y`, and a
reader of `otel/v0.6.2` knows it goes with the core's `v0.6.x` without
opening its `go.mod`.

**Patches are not.** A fix to one module is a patch of that module alone:
`v0.6.1` for the core, `etcd/v0.6.1` for etcd. A nested module's patch stays
on the core of its minor; one that needs a newer core waits for the next
minor release.

**A new feature in any module is a minor release of all of them.** Before
1.0 a minor release may break the API, the node protocol or both; the
release notes say which, and how to move.

Do not use pre-release versions (`v0.6.0-1`) to mean anything else: they
sort before `v0.6.0`, and `go get …@latest` skips them.

## Cutting a release

From `main`, clean and up to date with origin, with CI green:

```sh
scripts/release.sh -n v0.6.0     # the plan, changing nothing
scripts/release.sh v0.6.0        # a minor release
scripts/release.sh v0.6.1        # a patch of the core
scripts/release.sh otel/v0.6.1   # a patch of a nested module
```

It asks before it changes anything. From a shell with no terminal to ask
on, an editor's or an agent's, it stops instead: check the plan with `-n`,
then run it again with `-y`.

A minor release:

1. tags the core and pushes the tag, so that the module proxy serves it;
2. pins each nested module to it (`go get`, `go mod tidy`, `go build`),
   dropping requirements on modules of this repository that are modules no
   more, and bumps the requirement of `examples` and `benchmarks`;
3. commits that to `main` as "Pin … to grpcproc vX.Y.0" and pushes it;
4. tags each nested module on that commit and pushes the tags.

Then write the release notes, as a draft first:

```sh
gh release create v0.6.0 --draft --title 'grpcproc v0.6.0' --notes-file NOTES.md
```

They open with what the release is for, then list every module's tag and
the `go get` and `go install` lines, then **Breaking** (with how to move),
**Added** and **Fixed**. A change to the node protocol says whether nodes of
the last release and this one can run in one cluster.

## v0.6.0: cron and leader become packages

Until v0.5, `cron` and `leader` were nested modules. From v0.6.0 they are
packages of the core, at the same import paths. A project that required
them as modules drops the requirements as it upgrades, or the build stops
with "ambiguous import: found package … in multiple modules":

```sh
go get github.com/floatdrop/grpcproc@v0.6.0 \
	github.com/floatdrop/grpcproc/cron@none \
	github.com/floatdrop/grpcproc/leader@none
```
