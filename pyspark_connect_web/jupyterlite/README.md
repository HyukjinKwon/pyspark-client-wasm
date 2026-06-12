<!-- SPDX-License-Identifier: Apache-2.0 -->

# JupyterLite packaging (lane 3)

Build a JupyterLite site that runs the PySpark Connect client in Pyodide, with
the COOP/COEP headers required for the Atomics/SharedArrayBuffer bridge.

## Build

```bash
# in a local venv (NOT shipped to the browser):
pip install jupyterlite-core jupyterlite-pyodide-kernel build

# 1. build the wheel that micropip installs in the browser
python -m build --wheel        # -> dist/pyspark_connect_web-*.whl

# 2. build the lite site; --contents ships the demo notebook, the wheel is
#    referenced by URL (see worker_bootstrap.js PCW_WHEEL_URL).
jupyter lite build \
  --config pyspark_connect_web/jupyterlite/jupyter-lite.json \
  --contents pyspark_connect_web/jupyterlite/demo.ipynb \
  --output-dir _output

# 3. copy the COOP/COEP headers + the wheel into the output
cp pyspark_connect_web/jupyterlite/_headers _output/_headers
cp dist/pyspark_connect_web-*.whl _output/

# 4. serve. The dev server MUST emit COOP/COEP or crossOriginIsolated is false.
jupyter lite serve --output-dir _output
```

`jupyter lite serve` / `python -m http.server` do **not** set COOP/COEP by
default. For local dev use a server that honours `_headers`, or the tiny helper
below; lane 5's Envoy/compose sets them for the hosted e2e.

## Cross-origin isolation (mandatory — DECISIONS.md #4)

`SharedArrayBuffer` and `Atomics.wait` only exist when the page is
**cross-origin isolated**:

```
Cross-Origin-Opener-Policy:   same-origin
Cross-Origin-Embedder-Policy: require-corp
```

These live in `_headers`. The demo notebook asserts `crossOriginIsolated` before
importing, so a misconfigured host fails loud and early instead of hanging on a
non-shared buffer.

## Local dev server with the right headers

```python
# serve_coi.py — python http server that sets COOP/COEP
import http.server, functools
class H(http.server.SimpleHTTPRequestHandler):
    def end_headers(self):
        self.send_header("Cross-Origin-Opener-Policy", "same-origin")
        self.send_header("Cross-Origin-Embedder-Policy", "require-corp")
        super().end_headers()
http.server.test(HandlerClass=H, port=8000)
```

## How the bridge is wired into the JupyterLite kernel (no fork)

The pyodide kernel (`@jupyterlite/pyodide-kernel`) runs Pyodide in **its own**
ES-module Web Worker; we cannot replace it. We integrate non-invasively, in two
halves:

- **Page side — `pcw_kernel_bridge.js`.** Loaded *before* the JupyterLite app
  bundle (see "Load order" below), it wraps the global `Worker` constructor so
  every kernel worker the app spawns gets a `Bridge` (from `../worker/bridge.js`)
  attached. The Bridge does the real cross-origin `fetch` and writes response
  windows back into the SAB. It reacts only to our namespaced envelope
  `{__pcw__:{...}}` and ignores the kernel's own message framing (comlink /
  coincident), so the two coexist.

- **Worker side — `pyspark_connect_web.worker.kernel_bootstrap`.** Imported once
  inside the kernel (the demo's `import pyspark_connect_web; pcw.install()` is
  enough). `SabSyncChannel` auto-detects it is in a kernel worker (Pyodide, no
  `js.__pcw_register_sab` hook) and uses `transport="kernel"`: it allocates the
  SAB, posts `{__pcw__:{type:"sab",...}}` once and `{__pcw__:{type:"rpc"}}` per
  request, then parks on `Atomics.wait`. No notebook code beyond `pcw.install()`.

When the page is cross-origin isolated the kernel already uses **coincident**
(itself SAB+Atomics-based), so `SharedArrayBuffer` is available in the worker and
our allocation just works alongside it.

### Load order

`pcw_kernel_bridge.js` (and, on header-less hosts, `coi-serviceworker.js`) MUST
run before JupyterLite reads `Worker` off the global scope. Inject them as
`<script>` tags in the JupyterLite `index.html` template head, *before* the app
bundle. With the CLI, place an `index.template.html` (or use
`--apps`/`jupyter_lite_config.json` to add the scripts) so the build emits:

```html
<head>
  <script src="./coi-serviceworker.js"></script>     <!-- header-less hosts only -->
  <script type="module" src="./pcw_kernel_bridge.js"></script>
  <!-- JupyterLite app bundle loads after these -->
</head>
```

## Hosting matrix — which host needs what

| Host | Can set headers? | What to do |
|---|---|---|
| Lane 5 Envoy / `docker compose` (local e2e) | yes | Serves COOP/COEP directly (`deploy/`). Nothing extra. |
| Netlify / Cloudflare Pages | yes (via `_headers`) | Ship `_headers` (already provided). No service worker needed. |
| **GitHub Pages** | **no** | **Use `coi-serviceworker.js`** — include it as a `<script>` before everything; it injects COOP/COEP via a service worker and reloads once so `crossOriginIsolated` becomes true. |
| `python -m http.server` (dev) | no | Use the `serve_coi.py` snippet above, or `coi-serviceworker.js`. |

**COEP caveat (all isolated hosts):** `require-corp` blocks any *cross-origin*
subresource that lacks CORP/CORS headers. The CDN Pyodide build and the wheel
must be CORS-enabled or hosted same-origin. jsDelivr (the default `pyodideUrl`)
sends permissive CORS, so it works; if you self-host, copy `pyodide` + the wheel
into the site root and point `pyodideUrl`/`PCW_WHEEL_URL` at them.

## Open coordination points

- **Wheel URL**: `worker_bootstrap.js` reads `self.PCW_WHEEL_URL` (standalone
  harness). Inside JupyterLite the wheel is `micropip.install`-ed from the site
  root by the demo notebook; the lite build serves it there.
- **Endpoint**: the demo uses `sc://localhost:8081/;transport=grpcweb` (lane 5's
  Envoy). CORS on Envoy must allow the lite origin (lane 5 owns that).
- **Real-browser validation**: the kernel `Worker`-wrap + SAB handshake +
  `crossOriginIsolated` can only be confirmed in a cross-origin-isolated browser
  with lane 5's stack up — see `team/findings-lane3-bridge.md` "needs a browser".
