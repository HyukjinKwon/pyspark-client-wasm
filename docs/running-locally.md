<!-- SPDX-License-Identifier: Apache-2.0 -->

# Running it locally

End-to-end local setup: a Spark Connect server, the Envoy grpc-web proxy, the
JupyterLite site, the reference generator, and the e2e harness.

> Status: the server side (`deploy/`) and the e2e scaffold (`tests/e2e/`) are
> ready. The browser client (lanes 1-4) and the JupyterLite build (lane 3) are
> in progress; until they land, the e2e harness skips the in-browser checklist
> items and only the `crossOriginIsolated` gate is fully live.

## Prerequisites

* Docker + docker compose
* Node 18+ (for the Playwright e2e)
* Python 3.11+ with a venv (for the reference generator and unit tests)

## 1. Bring up the server stack

```bash
docker compose -f deploy/compose.yaml up
```

This starts:

| Service | Port | What |
|---------|------|------|
| `spark-connect` | 15002 | Spark 4.0.0 Connect server (gRPC) |
| `envoy` | 8081 | grpc-web endpoint for the browser client |
| `envoy` | 8000 | JupyterLite static host with COOP/COEP |
| `envoy` | 9901 | Envoy admin |

Cold start downloads the `spark-connect` package on first run (~1 min). Wait for
the `spark-connect` container to report healthy. See `deploy/README.md` for
quick `curl` checks of the CORS preflight and the isolation headers.

## 2. Point the client at the proxy

```python
import pyspark_connect_web as pcw
pcw.install()

from pyspark.sql import SparkSession
spark = SparkSession.builder.remote("sc://localhost:8081/;transport=grpcweb").getOrCreate()
spark.range(10).filter("id % 2 = 0").toPandas()
```

In JupyterLite, open <http://localhost:8000/> and run the demo notebook. Confirm
isolation first:

```js
crossOriginIsolated === true   // must be true; else SharedArrayBuffer is unavailable
```

## 3. Generate reference results (ground truth)

The e2e suite compares the browser's `toPandas()` against a native run. Generate
it from the same `:15002` Connect server (needs `grpcio`, which is fine outside
the package - see DECISIONS.md #1):

```bash
python -m venv .venv && . .venv/bin/activate
pip install -e ".[dev]"
python tests/e2e/reference.py --remote sc://localhost:15002 --out tests/e2e/reference.json
```

## 4. Run the e2e harness

```bash
cd tests/e2e
npm install
npx playwright install chromium
E2E_BASE_URL=http://localhost:8000 npx playwright test
```

When the stack is down the suite skips. To make a missing stack a hard failure
(the CI gate to flip once everything lands):

```bash
E2E_REQUIRE_STACK=1 npx playwright test
```

## 5. Unit tests (no browser, no grpcio)

```bash
pytest -q
```

Unit tests stub the transport; they never import `grpcio` and never touch a
browser (AGENTS.md / DECISIONS.md #1).

## Troubleshooting

| Symptom | Cause / fix |
|---------|-------------|
| `crossOriginIsolated === false` | COOP/COEP not reaching the browser. Check `curl -sI http://localhost:8000/ \| grep -i cross-origin`. A reverse proxy or CDN may be stripping them. |
| grpc-web call blocked by CORS | Origin not allowed. The Envoy CORS policy is permissive (`*`); confirm you hit `:8081`, not `:15002` directly. |
| Long `.collect()` hangs then errors | Envoy stream timeout. `envoy.yaml` sets `stream_idle_timeout: 0s` and `timeout: 0s` on the grpc-web route for exactly this. |
| `reference.py` cannot connect | Spark Connect not up on `:15002`, or `grpcio` not installed in the dev venv. Generate the reference on a machine that has `grpcio`. |
| Spark Connect container unhealthy | First-run `--packages` download still in flight, or insufficient memory. Give it ~60s and check `docker logs pcw-spark-connect`. |
