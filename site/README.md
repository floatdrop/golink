# site

The documentation at <https://floatdrop.github.io/grpcproc/>: a set of
pages, prerendered to static HTML with no JavaScript but a small inlined
script for the theme, the navigation and the outline. Its code blocks are the
files of [`examples/`](../examples), imported as text at build time and
highlighted with Shiki, and the output they show is what the examples' tests
pin, so a page cannot drift from the code.

```sh
npm ci
npm run dev       # renders a page on request, reloads on change
npm run check     # type-check
BASE_PATH=/grpcproc npm run build   # build/, as Pages serves it
npm run preview   # serves build/ under its base path
```

## Layout

- `src/nav.ts` is the map: the groups of pages and their order. A page in
  the map with no file, or a file with no place in the map, fails the
  render.
- `src/content/` holds one file per page, exporting a `Doc` (`types.ts`):
  its path, title, one-line description, and sections, each a heading and
  JSX. `index.ts` registers them. `landing.tsx` also holds the front page's
  hero, and `labels.ts` the words of the chrome.
- `src/code.tsx` highlights code synchronously, so a page writes
  `<Code>` inline; `region()` cuts a fragment out of an example file the way
  embedmd does. `src/components/` has the prose helpers (`C`, `A`, `Ext`,
  `Aside`, `Table`, `Cards`), the figure card, and the drawings, which are
  inline SVG built from a few primitives.
- `src/Page.tsx` is the chrome: topbar, navigation, the page, its outline,
  previous and next. `src/entry-server.tsx` renders every page and the 404.

After changing an example's code, run its tests with `-update` to refresh
the pinned output: `cd examples && go test ./quickstart ./actors
./supervisor ./blockingio ./pubsub -update` and `go test ./guide -update`.

.github/workflows/pages.yml builds the site on every change to it, to the
examples, or to cron and leader, and deploys it from `main`.

## Notice

Everything under `site/` except `src/content/`, `src/nav.ts`,
`src/Page.tsx`, `src/code.tsx`, `src/components/diagrams*.tsx`,
`src/components/prose.tsx`, `src/components/Mark.tsx` and
`public/favicon.svg` is copied or adapted from the site of
[golang.yandex/di](https://github.com/yandex/di), under its MIT license:

> Copyright (c) 2026 YANDEX LLC
>
> Permission is hereby granted, free of charge, to any person obtaining a copy
> of this software and associated documentation files (the "Software"), to deal
> in the Software without restriction, including without limitation the rights
> to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
> copies of the Software, and to permit persons to whom the Software is
> furnished to do so, subject to the following conditions:
>
> The above copyright notice and this permission notice shall be included in all
> copies or substantial portions of the Software.
>
> THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
> IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
> FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
> AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
> LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
> OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
> SOFTWARE.

The page bundles the styles and icons of [Gravity UI](https://github.com/gravity-ui),
MIT licensed, Copyright (c) 2021 YANDEX LLC.
