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

## Open coordination points

- **Wheel URL**: `worker_bootstrap.js` reads `self.PCW_WHEEL_URL`; the lite build
  serves the wheel from the site root. The JupyterLite pyodide kernel runs its
  *own* worker — integrating our SAB bridge into that kernel (vs. the standalone
  `worker_bootstrap.js` harness) is the open item flagged in
  `team/findings-lane3-bridge.md`.
- **Endpoint**: the demo uses `sc://localhost:8081/;transport=grpcweb` (lane 5's
  Envoy). CORS on Envoy must allow the lite origin.
