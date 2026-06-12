<!-- SPDX-License-Identifier: Apache-2.0 -->

# Changelog

All notable changes to **pyspark-connect-web** ("PySpark in JupyterLite") are
documented here.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

> Unofficial personal project. Not affiliated with, sponsored by, or endorsed by
> the Apache Software Foundation. "Apache Spark", "Spark", and "PySpark" are
> trademarks of the ASF, used here only to describe interoperability. See the
> README disclaimer.

Distribution name on PyPI: `pyspark-connect-web`. Import/package name:
`pyspark_connect_web`. Both are pure-Python, `py3-none-any`, and (by invariant)
carry **no** `grpcio` dependency - .

## [Unreleased]

### Fixed
- **SAB bridge deadlock on `spark.sql`** - an eager command runs two
  server-streaming RPCs back-to-back; the second's `S_REQ_READY` could race the
  abandoned first stream's `S_IDLE`, leaving the main thread parked forever
  (intermittent ~30-50% hang). The bridge now treats `S_REQ_READY` as
  "exchange abandoned" and re-dispatches; STATE writes are generation-guarded.
  Regression-covered in `tests/js/bridge.test.mjs`.

### Added
- **Real JupyterLite-kernel e2e** (`tests/e2e/kernel.spec.ts`): boots the actual
  lite kernel, installs in-kernel, and runs `range`/`spark.sql`/`groupBy` through
  the kernel SAB bridge - not just the standalone harness.

### Changed
- **JupyterLite bumped to core 0.7.6 / pyodide-kernel 0.7.2** (module-worker
  kernel; the 0.6.1 classic worker was incompatible with recent Pyodide). Pyodide
  is vendored same-origin at the exact version the kernel expects (0.29.3), shared
  by the kernel and the harness. `exposeAppInBrowser` lets the kernel bootstrap
  reach the app.

<!--
Release runbook (kept here so it travels with the changelog; docs/ is owned by
the DOCS agent - see docs/packaging-release.md for the longer-form checklist):

  1. Pre-flight, green on `main`:
       make test            # unit; transport stubbed, no browser, no grpcio
       make lint            # ruff check + format --check
       make validate-deploy # COOP/COEP present, no wildcard prod CORS
     and the e2e gate (.github/workflows/e2e.yml) green on the commit, plus the
     `integration` CI job (real Spark Connect round-trip) green.
  2. Move the "Unreleased" entries below into a new "## [X.Y.Z] - YYYY-MM-DD"
     section. Keep the heading shape EXACT - release.yml's awk extractor matches
     `^## \[X.Y.Z\]` and copies until the next `## [`.
  3. Bump `version` in pyproject.toml to X.Y.Z (drop any `.devN`/`rcN` suffix for
     a final release) AND `appVersion` in jupyterlite/jupyter-lite.json +
     `PCW_WHEEL_URL` default in worker/worker_bootstrap.js to the same string.
     release.yml's `build` job HARD-FAILS if the `vX.Y.Z` tag != package version.
  4. Optional rehearsal (no real PyPI, no Release):
       Actions -> release -> "Run workflow" -> dry_run = testpypi
     uploads to TestPyPI via OIDC so you can `pip install -i .../simple ...`.
  5. Cut it:
       git tag -a vX.Y.Z -m "vX.Y.Z" && git push origin vX.Y.Z
     The tag push runs release.yml: build sdist+wheel (assert no grpcio) ->
     publish to PyPI with the `PYPI_TOKEN` repo secret -> build the JupyterLite
     site -> create the GitHub Release with these notes + dist/* + the site
     tarball attached.
  6. Post-release: smoke-test the published site (crossOriginIsolated === true,
     demo notebook end-to-end against a reachable Connect server); update the
     README status; file a the project notes note for anything surprising.

One-time setup (maintainer): store a PyPI API token as the `PYPI_TOKEN` repo
secret (Settings -> Secrets and variables -> Actions). The TestPyPI dry-run path
still uses OIDC trusted publishing for the `testpypi` environment.
-->

## [0.1.0] - 2026-06-12

First tagged release of the **PySpark in JupyterLite** client: run the *real*
PySpark Connect Python client inside a browser (JupyterLite/Pyodide), talking to
a Spark Connect server over a grpc-web transport. Existing PySpark code runs
unchanged - no reimplementation, no local JVM, no Python backend.

### Added
- **grpc-web transport** - a `fetch`-based grpc-web stub (length-prefixed
  framing, `0x80` trailer frame) that replaces *only* PySpark's Connect service
  stub. We patch, we do not fork. Implements
  ExecutePlan / ReattachExecute / ReleaseExecute (see Known limitations for the
  browser reattach constraint).
- **Slim client (`pyspark-client`)** - the browser loads the pure-Python Spark
  Connect client (`pyspark-client`, no JVM/py4j), installed via micropip with
  `deps=False` (its grpcio/grpcio-status base deps are stubbed by the shim).
  Verified against real Spark **4.0.0 and 4.1.2** servers; `pcw.install()` guards
  `pyspark>=4.0`.
- **JupyterLite/Pyodide bridge** - a Web Worker + `Atomics`/`SharedArrayBuffer`
  channel that makes Connect calls *blocking*, so `.collect()` / `.toPandas()`
  return synchronously and the public PySpark API stays unchanged. Page-side `Worker` wrapping wires the bridge into the
  JupyterLite Pyodide kernel without forking it; a COI service worker covers
  header-less hosts. Cross-origin isolation (COOP/COEP) is mandatory and
  asserted.
- **Arrow result decoding** - IPC reassembly to pandas, including SPARK-53525
  multi-chunk split-batch handling, byte/row-exact against a native Connect
  reference.
- **`pcw.install()`** - idempotent, version-guarded to `pyspark>=4.0`; raises a clear error outside the range. Accepts
  `sc://host:port/;transport=grpcweb` plus `http(s)://` shorthand.
- **Real Spark-Connect-verified Python vertical** - the full v0 read-path matrix
  (`range`, `select`/`filter`/`groupBy`/`agg`, `toPandas`, `createDataFrame`,
  `spark.sql(...)`, 200k-row multi-response stream, mid-stream reattach) proven
  against a real in-process Spark Connect server in `tests/integration/` with a
  pure-Python grpc-web<->gRPC bridge standing in for Envoy.
- **Deploy stack** - dev `docker compose` (Spark 4.1.2 Connect + Envoy grpc-web
  proxy + COOP/COEP static host) and a hardened prod overlay (TLS, exact-origin
  CORS, bearer-token gate, size limits, health/readiness).
- **Pure-Python wheel** - `py3-none-any`, `dependencies = []`, **no `grpcio`**; CI guards the no-grpcio invariant at source, wheel-metadata,
  and import time.
- **Packaging & release automation** - `python -m build` sdist + wheel, PyPI
  publish via API token on a `vX.Y.Z` tag, JupyterLite site built as a release
  asset, GitHub Release notes sourced from this changelog
  (`.github/workflows/release.yml`).

### Known limitations
- **In-browser mid-stream reattach recovery is unavailable.** PySpark recovers a
  dropped operation by reading `INVALID_HANDLE.OPERATION_NOT_FOUND` from the gRPC
  trailer status via `grpcio-status`, which grpc-web cannot carry in the browser.
  A browser query whose connection drops mid-stream errors rather than resuming.
  Reattach recovery over real gRPC IS verified server-side
  (`tests/integration/`); the browser e2e marks this case as a known skip.
- The package does not yet ship a PEP 561 `py.typed` marker (owned by the
  package source lanes; tracked in CONTRIBUTING.md / docs/packaging-release.md).
- `AddArtifacts` is lowered to a single grpc-web `unary()` POST (grpc-web has no
  true client streaming); not exercised end-to-end yet.

[Unreleased]: https://github.com/HyukjinKwon/pyspark-connect-web/compare/v0.1.0...HEAD
[0.1.0]: https://github.com/HyukjinKwon/pyspark-connect-web/releases/tag/v0.1.0
