<!-- SPDX-License-Identifier: Apache-2.0 -->

# pyspark-connect-web - Team Coordination

**Goal:** Run the *real* PySpark Connect Python client inside JupyterLite/Pyodide,
talking to a Spark Connect server through a grpc-web/fetch transport. Framing:
**"PySpark in JupyterLite."** We monkey-patch `pyspark.sql.connect`; we do not
fork it. License: Apache-2.0 header on every source file.

**v0 target (locked):** full read-path parity - `range/select/filter/groupBy/agg`,
`toPandas`, `createDataFrame`, and `spark.sql(...)`, all returning correct results
e2e through the browser. Exact done-criteria in `DECISIONS.md`.

## How the client works (so we patch the right seam)
The Connect client is pure Python above its gRPC stub: it builds protobuf plans
and calls `self._stub.ExecutePlan(req)` etc. We replace **only the stub** with a
grpc-web transport, and make it *blocking* via a Web Worker + Atomics/SAB bridge
so `.collect()` returns data synchronously. See `API_CONTRACT.md` for the seam.

## Module ownership (claim/adjust your row; use your lane tag)
| Lane | Area | File(s) | Owner | Status |
|------|------|---------|-------|--------|
| 1 | grpc-web stub + framing | `pyspark_connect_web/transport/*` | lane1 | v0 done (framing+stub+40 tests) |
| 2 | monkey-patch + integration | `pyspark_connect_web/__init__.py`, `patch.py`, `_contract.py` | lane2 | v0 done (patch+tests; pending lanes 1/3 factories) |
| 3 | Pyodide sync bridge + JupyterLite | `pyspark_connect_web/worker/*`, `jupyterlite/*` | lane3 | v0: SabSyncChannel + JS bridge + lite config + 13 tests |
| 4 | Arrow result decoding | `pyspark_connect_web/arrow/*` | lane4 | v0 done (decode+encode+SPARK-53525 reassembly+17 tests) |
| 5 | Envoy proxy + e2e tests + docs | `deploy/*`, `tests/e2e/*`, `docs/*`, `README.md`, `.github/workflows/ci.yml` | lane5 | v0 scaffold (proxy+e2e+docs+CI) |
| - | contract / scaffold / integration | `API_CONTRACT.md`, `DECISIONS.md`, `_contract_seam.py`, `pyproject.toml` | INTEGRATOR | done (v0 scaffold) |

## Conventions
- Package: `pyspark_connect_web`. Python 3.11 locally; **target Pyodide >= 0.28 / Py 3.13** in browser.
- Deps already in Pyodide: `pyarrow>=22`, `pandas`, `protobuf>=7`, `numpy`. **`grpcio` is NOT available - never import it.**
- Local dev/tests: use a venv. Do NOT import `grpcio` even in tests; stub the transport.
- Apache-2.0 header on every source file. Pin a tested `pyspark` range (DECISIONS.md).
- **Don't rewrite a file another lane owns.** Build on the contract; if you need a
  contract change, edit `API_CONTRACT.md` AND append a note here first.
- Record findings in `team/findings-lane<N>-<topic>.md`, not inline in code.
- Keep `DECISIONS.md` invariants green; add a guard test when you fix a subtle bug.

## Notes log (append below - newest last; prefix with your lane + timestamp)
- INTEGRATOR 2026-06-12: Scaffolded repo, froze the stub seam in `API_CONTRACT.md`
  + `_contract_seam.py`, locked v0 = full read-path parity, bridge = Atomics+SAB
  (so COOP/COEP is mandatory - see DECISIONS.md). Lanes: claim your row above and
  start. The hardest seam is lane1<->lane3 (the `SyncChannel` byte boundary) - agree
  on it here before diverging.
- LANE 2 2026-06-12: `install()` implemented in `patch.py` (idempotent, version-guarded
  to `>=4.0,<4.2`). Patches 3 private symbols (VERIFIED on pyspark 4.0.0): replaces
  `proto.base_pb2_grpc.SparkConnectServiceStub` (one attribute covers both the core
  + ArtifactManager stub sites), wraps `DefaultChannelBuilder.__init__` (web-scheme
  parsing) and `.toChannel()` (returns a `WebChannel` marker, never calls `grpc.*`).
  Web scheme `sc://host:port/;transport=grpcweb` + `https://`/`http://` shorthand.
  Lanes 1/3 wiring is via pluggable hooks `set_stub_factory`/`set_channel_factory`
  (lazy defaults, no import cycle). **ACTION lane 1:** default factory calls
  `GrpcWebStub(sync_channel, metadata=[(k,v),...])`. **ACTION lane 3:** default calls
  `SabSyncChannel(base_url="http(s)://host:port")`. Conform or ping me. Full details
  + exact symbols in `team/findings-lane2-patch.md`. 17 guard tests green; no grpcio.
- LANE 1 2026-06-12: `transport/framing.py` + `transport/grpcweb.py` landed. 40 tests
  green, all 56 repo tests green; no grpcio anywhere. **CONFORMED to lane 2's seam:**
  `GrpcWebStub(sync_channel, metadata=[(k,v),...])` works (made `metadata=` the
  canonical channel-default kwarg; `default_metadata=` kept as alias). Guard test
  `test_lane2_default_stub_factory_builds_real_stub` drives lane 2's real
  `_default_stub_factory` with a fake channel end-to-end - fails loudly if either
  side drifts. **Seam notes for integrator/lane 3:** (1) `AddArtifacts` is
  client-streaming but `SyncChannel` has no client-stream method - I lower it to
  frame-concatenate + single `unary()` POST (grpc-web has no true client streaming).
  No SyncChannel change needed; flag me if you disagree. (2) `SparkConnectGrpcException`
  is at `pyspark.errors.exceptions.connect` in 4.0.0, NOT re-exported from
  `pyspark.errors` as API_CONTRACT section 1 implies - I import resiliently. (3) Dropped
  stream (no trailer frame) raises so PySpark's reattachable iterator recovers
  (DECISIONS.md #6). Details in `team/findings-lane1-transport.md`.
- LANE 5 2026-06-12: Landed `deploy/` (envoy.yaml grpc_web+CORS on :8081, static
  host with COOP/COEP on :8000, compose.yaml = apache/spark:4.0.0 Connect server
  on :15002 + Envoy + static; README), `tests/e2e/` (Playwright harness scaffold:
  one test per DECISIONS.md "v0 done =" item with TODO hooks + graceful skip when
  stack down; `reference.py` native-client ground-truth generator), `docs/`
  (architecture + running-locally), expanded `README.md` quickstart, and
  `.github/workflows/ci.yml` (pytest + grpcio-guard scoped to `pyspark_connect_web/`
  + COOP/COEP-present guard on deploy config). **Ports decided:** client uses
  `sc://localhost:8081/;transport=grpcweb` (per API_CONTRACT section 2); upstream gRPC on
  :15002 (Spark default, also host-exposed for `reference.py`). **ACTION lane 3:**
  e2e needs (a) `jupyterlite build` output into `./_output` (served by the `static`
  container) and (b) a `window.__pcwRunPython(src)` JS bridge that runs Python in
  the kernel and returns JSON - see TODO hooks in `tests/e2e/helpers.ts`. Only the
  `crossOriginIsolated` check + CI guards are live today; the rest are `test.fixme`
  pending lanes 1-4. No grpcio in the package. Versions/gotchas in
  `team/findings-lane5-deploy.md`.
- LANE 3 2026-06-12: Bridge landed. `worker/sab_channel.py` = `SabSyncChannel`
  (the `SyncChannel`) over a pluggable `SyncBackend`: `_AtomicsBackend`
  (SAB+Atomics.wait) under Pyodide, injected fake in tests. **CONFORMS to lane 2's
  seam:** `SabSyncChannel(base_url="http(s)://host:port")` positional, matches your
  default channel factory. **To lane 1:** `unary`/`server_stream` block and match
  your usage - your AddArtifacts->single `unary()` lowering and dropped-stream->raise
  (reattach) both work as-is; `server_stream` is a lazy generator so a broken stream
  surfaces promptly (DECISIONS.md #6). One open ask: `HttpResponse.headers` is `{}`
  today (status only) - confirm grpc-web trailers ride in the body frame, not HTTP
  headers (I believe yes per API_CONTRACT section 1). JS glue: `worker/bridge.js` (main
  thread fetch+SAB writeback) + `worker/worker_bootstrap.js` (Pyodide load, micropip,
  SAB alloc). **To lane 5:** (a) build into `_output` per `jupyterlite/README.md`;
  (b) `window.__pcwRunPython(src)` provided by `jupyterlite/run_python_bridge.js`
  (standalone harness shape works; JupyterLite-kernel shape is the flagged open
  item - kernel runs its own worker). COOP/COEP in `jupyterlite/_headers`; demo
  asserts `crossOriginIsolated`. 13 lane-3 tests green (no browser/grpcio/net). Full
  SAB layout + Atomics state machine + open questions in `team/findings-lane3-bridge.md`.
- LANE 4 2026-06-12: `arrow/results.py` landed. `decode_arrow_batches(responses)`
  + `encode_local_relation(pdf)` per API_CONTRACT section 3, plus additive helper
  `reassemble_record_batches` (purely the bytes->RecordBatch step, exported for
  reuse/testing; no seam change). **Decision: REIMPLEMENT** the reassembly+IPC
  decode, **REUSE** pyarrow `Table.to_pandas` - measured pyspark 4.0.0's
  `to_pandas`/`_execute_and_fetch` and it needs a live client+plan+config RPCs,
  not callable on a bare response iterable; and its loop predates SPARK-53525.
  `encode_local_relation` is byte-identical to `plan.LocalRelation.plan` framing
  (guard test). **SPARK-53525 handled:** reassemble chunks by
  `chunk_index`/`num_chunks_in_batch` (proto3 optional, Spark 4.1+); on pyspark
  4.0.0 those fields don't exist, so `HasField` raises ValueError - we catch it
  and treat every batch as whole (correct; 4.0 never chunks). 17 tests green incl.
  multi-chunk split-batch + integrity guards; no grpcio. **Heads-up lane 2
  (parity/DECISIONS.md #7):** lane 4 does NOT apply session-timezone localization
  or struct-handling-mode (no client config available from a response iterable) -
  if the parity test diverges on a timestamp/struct column, wrap our DataFrame
  with `_create_converter_to_pandas` using live session config, or pass us the tz.
  Only known gap to byte-exact parity; details in `team/findings-lane4-arrow.md`.
- INTEGRATION 2026-06-12: Real end-to-end PROVEN. Built `tests/integration/`
  (real in-process Spark Connect server on an ephemeral free port + a pure-Python
  grpc-web<->gRPC bridge = Envoy stand-in, grpcio test-only). v0 matrix PASSES vs
  the real engine: range(10).collect, sql, groupBy/agg/toPandas EXACT-parity vs
  native client, createDataFrame round-trip, 200k-row multi-response stream.
  FIXED a real DECISIONS.md #6 bug in `transport/grpcweb.py`: a trailer-less
  (dropped) stream RAISED instead of ending cleanly, so PySpark's reattachable
  iterator never issued ReattachExecute (its retry path only retries
  grpc.RpcError) - now returns StopIteration; mid-stream-cut recovery verified
  live (ReattachExecute called, all rows recovered). Updated 2 lane-1 guard tests
  to the corrected contract; all 87 unit tests still green; 94 total. STILL
  UNTESTED: tz/struct-mode parity, AddArtifacts, the lane-3 SAB browser path (the
  test injects the bridge directly, bypassing Atomics/SAB). No grpcio in the package.
- LANE 5 2026-06-12 (prod-hardening): Added prod overlay (`deploy/envoy.prod.yaml`
  + `deploy/compose.prod.yaml`: TLS, exact-origin CORS, bearer-token Lua gate ->
  jwt_authn/ext_authz, 32MiB limits, :8089 health/ready, loopback admin, private
  Spark); `Makefile` + `scripts/` (build_site.sh, render_envoy_prod.sh,
  gen_dev_cert.sh, validate_deploy.py). WIRED the e2e to `window.__pcwRunPython`
  (no more test.fixme; query #3 now matches reference.py; a one-shot page.route
  ExecutePlan-abort drives the ReattachExecute recovery test - consistent with
  INTEGRATION's verified dropped-stream->StopIteration->reattach fix; two-axis
  graceful skip). CI: py matrix + ruff lint (scoped) + build-wheel(no-grpcio) +
  validate_deploy headers/CORS guard. Docs: `docs/security.md` (5-area threat
  model), `docs/packaging-release.md`, trademark/identity disclaimer + repo-vs-
  package name note in README/docs. 105 unit tests green; no grpcio in package.
  **ACTION lanes 1-4/integrator:** (1) ship a `py.typed` marker + package-data
  (`worker/*.js`,`jupyterlite/*`) so the wheel is complete + typed - lane 5 won't
  edit package source; (2) fix 2 ruff findings (F401 transport/grpcweb.py
  TRAILER_FLAG, F841 worker/sab_channel.py `length`) to unblock whole-repo lint.
  **ACTION lane 3:** wire `window.__pcwRunPython` into the JupyterLite kernel
  (your open item #1) - until then the e2e bridge tests skip. Details in
  `team/findings-lane5-deploy.md`.
- LANE 3 2026-06-12 (hardening): Closed my two prior blockers. (1) **JupyterLite
  kernel integration** without forking the pyodide kernel: page-side
  `jupyterlite/pcw_kernel_bridge.js` wraps the global `Worker` so each kernel
  worker gets our fetch `Bridge`; `_AtomicsBackend` gains an auto-selected
  `transport="kernel"` mode that posts a namespaced `{__pcw__:{...}}` envelope the
  kernel's coincident/comlink framing ignores - `pcw.install()` is all a notebook
  needs. Wired `run_python_bridge.js` Shape B to the real kernel execute (lane 5's
  `__pcwRunPython` no longer throws). Header-less hosts get
  `jupyterlite/coi-serviceworker.js` (COOP/COEP via SW + one reload); hosting matrix
  in `jupyterlite/README.md`. **ACTION lane 5:** inject `coi-serviceworker.js` +
  `pcw_kernel_bridge.js` as `<script>`s before the app bundle (template in README);
  add `jupyterlite/*.js` to package-data per your earlier ask. (2) **Large results**:
  response side now uses bounded-window transfer (`meta.more` + CHUNK_ACK
  reassembly) + request-side SAB realloc - the 16 MiB ceiling is gone. (3) **Errors**:
  typed `TransportError`/`TransportTimeout`/`TransportAborted`; `HttpResponse.headers`
  now populated (resolves my prior open Q2 so lane 1's grpc-status-in-headers fallback
  works). Fixed lane 5's flagged ruff F841 (`length` in sab_channel.py). +11 unit tests
  (`tests/test_sab_atomics_backend.py`, fake-js handshake); 105 non-e2e green; no grpcio.
  **ASK lane 1** (findings #6): let a raw `TransportError` propagate, or want me to wrap
  dropped-connection as `SparkConnectGrpcException` UNAVAILABLE for uniform reattach?
- LANE 5 2026-06-12 (CI e2e gate): Added `.github/workflows/e2e.yml` - the REAL
  headless-browser e2e gate (SEPARATE from ci.yml, which is untouched). On
  push/PR/dispatch, ubuntu-latest: Python 3.11 + Java 17 + Node 20, `make site`
  into `_output`, `docker compose -f deploy/compose.yaml up -d --wait`, host-poll
  health (`:15002` TCP, Envoy `/ready` :9901, static :8000 + COOP/COEP, grpc-web
  CORS preflight :8081), native `reference.py`, then `npx playwright install
  --with-deps chromium` + `E2E_REQUIRE_STACK=1 playwright test` to HARD-FAIL the
  full DECISIONS.md v0 matrix (crossOriginIsolated/range/parity/createDataFrame/
  sql/reattach). Rich always-on artifacts (Playwright HTML report + traces, per-
  service compose logs, Envoy /clusters+/stats, reference.json) + teardown.
  concurrency=e2e-${ref} cancel-in-progress, timeout 30m. NO compose/envoy edits
  needed (`_output` already mounted, all ports published, COOP/COEP already
  served). Validated statically only (no docker/net here): YAML parses, all
  inline bash `bash -n`-clean, paths/ports confirmed. **HEADS-UP lane 3:** first
  run will HARD-FAIL items 2-6 until `scripts/build_site.sh` injects
  `pcw_kernel_bridge.js`/`run_python_bridge.js` into the page so
  `window.__pcwRunPython` exists (your open item #1 / the inject-scripts ACTION
  to me) - that build-wiring gap is the gate's first real blocker, not a workflow
  bug. Details + full first-run risk list in `team/findings-lane5-deploy.md`.
- DOCS 2026-06-12: Stood up the MkDocs + Material + mkdocstrings[python] docs site
  (`mkdocs.yml`, `docs/` pages: index/installation(conda)/quickstart/connection-
  patterns/jupyterlite-hosting/api-reference, plus the existing architecture/
  running-locally/security/packaging-release folded into the nav) and
  `.github/workflows/docs.yml` (build + `actions/deploy-pages` to GitHub Pages on
  push-to-main/dispatch, pinned versions, `pages: write`+`id-token: write`).
  REMOVED the trademark "Unofficial personal project..." disclaimer block from all
  docs that had it (architecture/running-locally/security/packaging-release) and
  trimmed two dangling references in packaging-release.md. **ACTION integrator:**
  enable Pages -> Source = **GitHub Actions** in repo settings, else the deploy job
  fails. Validated YAML only (offline; no `mkdocs build`). Details in
  `team/findings-docs.md`. Did not touch README.md / ci.yml / e2e.yml / pyproject.
- RELEASE 2026-06-12: Added `.github/workflows/release.yml` (tag `v*` -> `python -m
  build` sdist+wheel, assert no-grpcio [reuses ci.yml's check], publish to PyPI via
  OIDC trusted publishing [env `pypi`, `id-token: write`], build the JupyterLite
  site as a Release asset, GitHub Release with notes from CHANGELOG.md;
  `workflow_dispatch` dry_run = build-only|testpypi) + `CHANGELOG.md` (Keep a
  Changelog; `0.1.0` first entry + `Unreleased`; runbook in a comment). Versioning:
  semver, `vX.Y.Z` tags, single source = `version` in pyproject; build HARD-FAILS on
  tag!=version. Validated statically only (YAML parses, inline scripts `bash -n`-
  clean, awk notes-extractor checked vs `[0.1.0]`); OIDC upload + Release creation
  can only run on a real tag push. **ACTION integrator:** add the `integration` CI
  job (Java 17 + `pip install -e .[dev]` + test-only `grpcio grpcio-status` +
  `pytest -q tests/integration`), `[project.urls]`, and (optional) a top-level
  Apache-2.0 `LICENSE` - exact snippets in `team/findings-release.md`. **ACTION
  maintainer:** register the repo as a PyPI/TestPyPI trusted publisher for the
  `pypi`/`testpypi` Environments before the first tag.
- TEST-COVERAGE 2026-06-12: Measured unit coverage (pytest-cov, `--ignore=tests/e2e
  --ignore=tests/integration`): **83% -> 96%** after adding 47 offline tests across 4
  new test-only files (`tests/test_grpc_shim.py`, `test_coverage_gaps.py`,
  `test_arrow_coverage_gaps.py`, `test_sab_channel_gaps.py`); 145 pass + 1 xfail.
  Headline gap was `_grpc_shim.py` 19%->100% (untested locally since real grpcio
  early-returns the install; covered via a `sys.meta_path` blocker). `grpcweb.py`
  ->100%, `patch.py`->99%, `arrow/results.py`->99%. No source edits, no other lanes'
  test files touched. One genuine parity gap pinned `xfail(strict)`: lane 4 applies
  no `spark.sql.session.timeZone` localization (tz-aware timestamp decode). No real
  bugs found. **ACTION integrator:** add `pytest-cov` to `[dev]`, a
  `[tool.coverage.run]`/`[tool.coverage.report]` block (omit `worker/kernel_bootstrap.py`,
  browser-only), and a `--cov-fail-under=90` coverage step to ci.yml's `unit` job -
  exact snippets in `team/findings-coverage.md`.
