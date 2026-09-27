# site

The guide at <https://floatdrop.github.io/grpcproc/>: one page, prerendered
to static HTML. Its code blocks are the files of [`examples/guide`](../examples/guide),
imported as text at build time and highlighted with Shiki, and the output it
shows is what the guide's tests pin, so the page cannot drift from the code.

```sh
npm ci
npm run dev       # renders the page on request, reloads on change
npm run check     # type-check
BASE_PATH=/grpcproc npm run build   # build/, as Pages serves it
npm run preview   # serves build/ under its base path
```

The prose is `src/content/en.tsx`, the drawings `src/components/diagrams.tsx`.
After changing the guide's code, run its tests with `-update` to refresh the
pinned output: `cd examples && go test ./guide -update`.

.github/workflows/pages.yml builds the site on every change to it or to the
guide, and deploys it from `main`.

## Notice

Everything under `site/` except `src/content/en.tsx`,
`src/components/diagrams.tsx`, `src/components/Mark.tsx` and
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
