<!-- SPDX-License-Identifier: Apache-2.0 -->

# JupyterLite hosting

The JupyterLite site runs the PySpark Connect client in Pyodide. The single hard
requirement for any host is **cross-origin isolation** - without it,
`SharedArrayBuffer` is unavailable and the blocking `.collect()` bridge cannot
work.

## Why cross-origin isolation is mandatory

`SharedArrayBuffer` and `Atomics.wait` (the backbone of the blocking bridge) only
exist when the page is **cross-origin isolated**, which requires it be served
with:

```
Cross-Origin-Opener-Policy:   same-origin
Cross-Origin-Embedder-Policy: require-corp
```

The demo notebook asserts `crossOriginIsolated === true` before importing, so a
misconfigured host fails loud and early instead of hanging on a non-shared
buffer. This is a hard invariant (`the design notes` #4).

## Hosting matrix - which host needs what

| Host | Can set headers? | What to do |
|---|---|---|
| Envoy / `docker compose` (local e2e) | yes | Serves COOP/COEP directly (`deploy/`). Nothing extra. |
| Netlify / Cloudflare Pages | yes (via `_headers`) | Ship `_headers` (already provided). No service worker needed. |
| **GitHub Pages** | **no** | **Use `coi-serviceworker.js`** - include it as a `<script>` before everything; it injects COOP/COEP via a service worker and reloads once so `crossOriginIsolated` becomes true. |
| `python -m http.server` (dev) | no | Use the `serve_coi.py` snippet below, or `coi-serviceworker.js`. |

!!! note "Two different GitHub Pages sites"
    This documentation site is built with MkDocs and deployed to GitHub Pages by
    `.github/workflows/docs.yml` - that is a plain static docs site and needs no
    isolation headers. The **JupyterLite app** is a separate artifact; if you host
    *it* on GitHub Pages, it needs the `coi-serviceworker.js` shim described above
    because GitHub Pages cannot set COOP/COEP headers.

## COEP caveat (all isolated hosts)

`require-corp` blocks any *cross-origin* subresource that lacks CORP/CORS
headers. The CDN Pyodide build and the wheel must be CORS-enabled or hosted
same-origin. jsDelivr (the default `pyodideUrl`) sends permissive CORS, so it
works; if you self-host, copy `pyodide` + the wheel into the site root and point
`pyodideUrl` / `PCW_WHEEL_URL` at them.

## Local dev server with the right headers

`jupyter lite serve` and `python -m http.server` do **not** set COOP/COEP by
default. For local dev, use a server that honours `_headers`, or this tiny
helper:

```python
# serve_coi.py - python http server that sets COOP/COEP
import http.server, functools
class H(http.server.SimpleHTTPRequestHandler):
    def end_headers(self):
        self.send_header("Cross-Origin-Opener-Policy", "same-origin")
        self.send_header("Cross-Origin-Embedder-Policy", "require-corp")
        super().end_headers()
http.server.test(HandlerClass=H, port=8000)
```

## How the bridge integrates with the kernel (no fork)

The `@jupyterlite/pyodide-kernel` runs Pyodide in **its own** ES-module Web
Worker, which cannot be replaced. Integration is non-invasive, in two halves:

* **Page side - `pcw_kernel_bridge.js`.** Loaded *before* the JupyterLite app
  bundle, it wraps the global `Worker` constructor so every kernel worker the app
  spawns gets a `Bridge` attached. The Bridge does the real cross-origin `fetch`
  and writes response windows back into the SAB. It reacts only to the namespaced
  envelope `{__pcw__:{...}}` and ignores the kernel's own message framing, so the
  two coexist.
* **Worker side - `pyspark_connect_web.worker.kernel_bootstrap`.** Imported once
  inside the kernel (the demo's `import pyspark_connect_web; pcw.install()` is
  enough). `SabSyncChannel` auto-detects it is in a kernel worker and uses
  `transport="kernel"`: it allocates the SAB, posts the namespaced envelope, then
  parks on `Atomics.wait`. No notebook code beyond `pcw.install()` is needed.

### Load order

`pcw_kernel_bridge.js` (and, on header-less hosts, `coi-serviceworker.js`) MUST
run before JupyterLite reads `Worker` off the global scope. Inject them as
`<script>` tags in the JupyterLite `index.html` template head, *before* the app
bundle:

```html
<head>
  <script src="./coi-serviceworker.js"></script>     <!-- header-less hosts only -->
  <script type="module" src="./pcw_kernel_bridge.js"></script>
  <!-- JupyterLite app bundle loads after these -->
</head>
```

## Building the site

See [Packaging & release](packaging-release.md) for the full `jupyter lite build`
flow (build the wheel, build the lite site, copy `_headers` + the wheel into the
output, serve with COOP/COEP).
