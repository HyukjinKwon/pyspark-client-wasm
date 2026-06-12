<!-- SPDX-License-Identifier: Apache-2.0 -->

# tests/e2e — headless-browser end-to-end harness

This is lane 5's e2e harness. It drives a real headless browser against the
JupyterLite page served by `deploy/` and asserts the **"v0 done ="** checklist
from `DECISIONS.md`:

- [ ] `crossOriginIsolated === true` on the JupyterLite page
- [ ] `spark.range(10).collect()` returns 10 rows
- [ ] `spark.range(100).filter(...).select(...).groupBy(...).agg(...).toPandas()` matches reference
- [ ] `spark.createDataFrame(pandas_df)` round-trips
- [ ] `spark.sql("select 1 as x").collect()` works
- [ ] a mid-stream disconnect recovers via ReattachExecute

## Status: WIRED (gated)

Every checklist item is a real Playwright test that drives lane 3's
`window.__pcwRunPython(src)` bridge
(`pyspark_connect_web/jupyterlite/run_python_bridge.js`) and asserts the
DECISIONS.md v0 matrix. There are **no more `test.fixme` markers** — instead the
suite degrades gracefully on two axes:

1. **Stack down** (JupyterLite page unreachable at `E2E_BASE_URL`): every test
   skips, unless `E2E_REQUIRE_STACK=1` (then it is a hard failure — the CI gate
   to flip once the stack lands).
2. **Bridge not wired** (page is up but `window.__pcwRunPython` is absent, e.g.
   the JupyterLite-kernel integration in `team/findings-lane3-bridge.md` #1 is
   still pending): the bridge-dependent tests skip with a clear reason, again
   unless `E2E_REQUIRE_STACK=1`. The `crossOriginIsolated` test needs no bridge
   and runs whenever the page is up.

The query in test #3 (filter/groupBy/agg) is kept **byte-for-byte** in lockstep
with `reference.py::build_reference`.

The mid-stream-disconnect test (DECISIONS.md #6) arms a one-shot Playwright
`page.route()` that aborts the first `ExecutePlan` POST after it starts; the
client's reattachable iterator must recover via `ReattachExecute` (a path we do
**not** intercept) and still return the full count.

## Layout

| File | Purpose |
|------|---------|
| `playwright.config.ts` | Playwright config; reads `E2E_BASE_URL` (default `http://localhost:8000`) |
| `v0-checklist.spec.ts` | One test per DECISIONS.md checklist item, driving the bridge |
| `helpers.ts` | Shared helpers: stack-up probe, bridge probe, kernel-ready wait, run-cell, mid-stream-disconnect injector |
| `reference.py` | Reference-result generator — runs the same queries on a **native** Spark Connect client and writes `reference.json` for the browser run to compare against |
| `package.json` | npm deps (`@playwright/test`) + scripts |

## Dependencies

Browser side (npm):

```bash
cd tests/e2e
npm install          # installs @playwright/test
npx playwright install chromium   # headless browser binary
```

Reference generator (pip, native — local dev venv only, NEVER in the browser):

```bash
pip install -e ".[dev]"   # pyspark + pyarrow + pandas (NO grpcio; native gRPC client uses grpcio-less Connect path? see note)
```

> NOTE: the *native* reference client (`reference.py`) runs a normal PySpark
> Connect session against `sc://localhost:15002`. PySpark Connect's own client
> uses `grpcio` — that is fine **here** because `reference.py` lives under
> `tests/` and is NOT part of `pyspark_connect_web/` (DECISIONS.md #1 forbids
> `grpcio` only inside the package). The CI grpcio-guard scopes its check to
> `pyspark_connect_web/` exactly for this reason. If your dev env lacks
> `grpcio`, generate the reference on a machine that has it and commit the
> `reference.json`.

## Running

```bash
# 1. (optional) bring the stack up
docker compose -f deploy/compose.yaml up -d

# 2. generate reference results from the native client (needs Spark Connect on :15002)
python tests/e2e/reference.py --remote sc://localhost:15002 --out tests/e2e/reference.json

# 3. run the browser e2e
cd tests/e2e
E2E_BASE_URL=http://localhost:8000 npx playwright test
```

If the stack is down, step 3 reports the checklist items as skipped, not failed.

## Env vars

| Var | Default | Meaning |
|-----|---------|---------|
| `E2E_BASE_URL` | `http://localhost:8000` | JupyterLite page URL (Envoy static host) |
| `E2E_SPARK_REMOTE` | `sc://localhost:8081/;transport=grpcweb` | endpoint the in-page client connects to |
| `E2E_REFERENCE` | `tests/e2e/reference.json` | reference results to compare `toPandas()` against |
| `E2E_KERNEL_TIMEOUT_MS` | `150000` | how long to wait for the Pyodide kernel + `pyspark` import before failing |
| `E2E_REQUIRE_STACK` | unset | if `1`, a missing stack **or** a missing `window.__pcwRunPython` bridge is a hard failure (CI gate once stack lands) |
