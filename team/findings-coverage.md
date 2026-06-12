<!-- SPDX-License-Identifier: Apache-2.0 -->

# findings-coverage.md — test coverage measurement + gap fill

TEST-COVERAGE agent, 2026-06-12. Goal: answer "is coverage enough?" with data,
then raise it. Env: Python 3.11.8, pyspark 4.0.0, pyarrow 22.0.0, grpcio present
locally (so `_grpc_shim` early-returns on import here — see note below).

Command (the brief's exact invocation), unit scope only:

```
pytest --cov=pyspark_connect_web --cov-report=term-missing \
       --ignore=tests/e2e --ignore=tests/integration
```

## Verdict

Coverage was good but not enough for the production gate: 83% overall, with one
brand-new module (`_grpc_shim.py`) at **19%** — effectively untested locally
because real grpcio is present, so `install_grpc_shim()` early-returns and the
whole shim builder/install path was never exercised. After the gap-fill,
**overall 83% -> 96%**, and every module the brief named is at 98-100%. The only
remaining misses are browser/Pyodide-only code (no offline coverage possible) and
two defensive branches that need live lane-1/lane-3 wiring through `install()`.

## Per-module coverage: BEFORE -> AFTER

| Module | Before | After | Notes |
|---|---|---|---|
| `__init__.py` | 100% | 100% | |
| `_contract.py` | 100% | 100% | |
| **`_grpc_shim.py`** | **19%** | **100%** | the headline gap; was 0 dedicated tests |
| `arrow/__init__.py` | 100% | 100% | |
| `arrow/results.py` | 95% | **99%** | 1 defensive line left (see below) |
| `patch.py` | 88% | **99%** | 2 lines need real lane1/3 wiring |
| `transport/__init__.py` | 100% | 100% | |
| `transport/framing.py` | 100% | 100% | already exhaustive |
| **`transport/grpcweb.py`** | 95% | **100%** | |
| `worker/__init__.py` | 100% | 100% | |
| `worker/kernel_bootstrap.py` | 0% | 0% | **browser-only** (imports `js`, COOP/COEP); e2e-covered, not unit-testable offline |
| `worker/sab_channel.py` | 92% | **94%** | remaining misses are `_AtomicsBackend` (browser-only) |
| **TOTAL** | **83%** | **96%** | tests 98 -> 145 (+1 xfail) |

## New test files (all offline; SPDX-headed; do NOT touch other lanes' files)

| File | Tests | Targets |
|---|---|---|
| `tests/test_grpc_shim.py` | 15 | `_grpc_shim`: `_build_module` (version/marker, StatusCode enum + canonical values, RpcError, marker classes, channel ctors raise, PEP-562 `__getattr__` guard, Compression/Connectivity), `_build_grpc_status_modules` (`from_call`->None, `to_status` raises), `install_grpc_shim` (no-op w/ real grpcio, already-shim, real-grpc-loaded, **actual install via sys.meta_path blocker**, find_spec-exception fallback, `import grpc` resolves to shim). |
| `tests/test_coverage_gaps.py` | 21 | `patch.py`: `WebChannel.close` (closeable/non-closeable/error-swallow), base_url variants, lazy default channel/stub factory deferred errors, non-web passthrough, `_is_web_url` unknown scheme, uninstall-when-not-installed. `grpcweb.py`: unary OK-trailer-no-message, present-trailer-no-grpc-status, compressed frame on unary + stream decode, client-stream header fallback + empty-message + OK-no-message, **status-code->exception mapping for 6 representative non-OK trailers (3/5/7/13/14/16)**, message-less non-OK trailer, empty-chunk skip in stream. |
| `tests/test_arrow_coverage_gaps.py` | 11 (1 xfail) | `arrow/results.py`: first-chunk `chunk_index!=0` reject, empty-schema named frame, pyarrow version-probe garbage/true, decode w/o coerce flag; type-fidelity PINS for decimal128/decimal256, struct/map, tz-naive timestamp; **xfail(strict) pinning the tz-localization parity gap**. |
| `tests/test_sab_channel_gaps.py` | 6 | `sab_channel.py`: `is_pyodide` (emscripten / `js` importable / cpython), `make_channel` (with backend / no-backend raise off-browser). |

## How `_grpc_shim` is tested without uninstalling grpcio

The shim only installs when grpcio is *absent*. Locally grpcio is present, so the
install branch never runs on import. `tests/test_grpc_shim.py` uses a
`sys.meta_path` finder that raises `ModuleNotFoundError` for `grpc`/`grpc_status`
(plus a cleaned `sys.modules`) so `importlib.util.find_spec('grpc')` resolves to
absent and the real install branch executes deterministically — then asserts it
plants the stub, is idempotent, and that `import grpc` resolves to our shim. The
builders (`_build_module`, `_build_grpc_status_modules`) are also exercised
directly, needing no grpcio manipulation at all.

NOTE for CI: in the `unit` job grpcio is **deliberately not installed**, so there
the shim genuinely installs on import — these tests pass in both worlds (grpcio
present locally, absent in CI).

## Genuine parity gap marked xfail (NOT a red, tracked not hidden)

`tests/test_arrow_coverage_gaps.py::test_session_timezone_localization_parity_gap`
— `@pytest.mark.xfail(strict=True, reason=...)`. Lane 4 (`arrow/results.py`) is a
pure decoder and applies **no** `spark.sql.session.timeZone` localization (that
needs a live client config it does not have). A tz-aware Arrow timestamp decodes
to its encoded (UTC) wall clock, NOT the session-tz-localized value the native
client would produce. This is exactly the boundary documented in
`findings-lane4-arrow.md` ("timezone localization ... belongs in lane 2's
integration") and `findings-integration.md` ("Timezone/struct-mode parity is
UNTESTED"). The xfail demonstrates and pins the gap. If lane 2 adds tz
localization, this test starts XPASSing (strict=True), flagging that the gap
closed and the test should be promoted to a real parity assertion.

## Real bugs found

**None.** All asserted behaviour matched the source. The one previously-known
real bug (the dropped-stream reattach path in `grpcweb._stream_responses`) was
already found and fixed by the INTEGRATION agent and is guarded by existing unit
tests; my new `grpcweb` tests are additive (status mapping, compressed frames,
header fallback, OK-no-message) and all passed against the current source without
modification.

## Remaining uncovered lines (intentional, documented)

- `worker/kernel_bootstrap.py` (all): browser/Pyodide-only (imports `js`, asserts
  `crossOriginIsolated`). Covered by lane-5 headless e2e, not offline-unit-testable.
- `worker/sab_channel.py` 298,314,343-344,383,460,462,495,502,526,528,553-554:
  inside `_AtomicsBackend` (Pyodide-only; imports `js`, drives `Atomics.wait`).
  Line 169 (no-backend raise) IS covered by lane-3's existing test.
- `arrow/results.py` 209: the empty-result-with-known-schema return. The branch
  *runs* correctly (verified manually — returns a named empty frame), but
  pyarrow's schema-only IPC stream decode makes `coverage` attribute the line
  inconsistently across builds; defensive, low-risk, not worth contorting.
- `patch.py` 219, 352: `_default_channel_factory`'s real `SabSyncChannel(...)`
  construction and `patched_to_channel`'s non-web `return orig_to_channel(self)`.
  Both need real lane-1/lane-3 wiring through a full `install()` + `toChannel()`
  on a non-web builder; exercised end-to-end by `tests/integration`, left out of
  the offline unit lane on purpose.

## Integrator handoff — EXACT snippets to add (I did NOT edit shared files)

### 1. `pyproject.toml` — add `pytest-cov` to `[dev]`

```toml
dev = [
    "pyspark>=4.0,<4.2",
    "pyarrow>=22",
    "pandas",
    "protobuf>=7",
    "googleapis-common-protos>=1.56.4",
    "pytest",
    "pytest-cov",
]
```

### 2. `pyproject.toml` — coverage config (append after `[tool.pytest.ini_options]`)

```toml
[tool.coverage.run]
source = ["pyspark_connect_web"]
branch = true
# Browser/Pyodide-only modules cannot run off-browser (they import `js` /
# require crossOriginIsolated); they are covered by the headless e2e job, not
# the offline unit lane. Omit so the unit threshold reflects testable code.
omit = [
    "pyspark_connect_web/worker/kernel_bootstrap.py",
]

[tool.coverage.report]
show_missing = true
exclude_lines = [
    "pragma: no cover",
    "if TYPE_CHECKING:",
    "raise NotImplementedError",
    "@(abc\\.)?abstractmethod",
]
```

Suggested unit threshold: **`--cov-fail-under=90`** (current testable-code
coverage is 96%; 90 leaves headroom for churn without letting it silently rot).
With `kernel_bootstrap.py` omitted the floor comfortably clears 90.

### 3. `.github/workflows/ci.yml` — `unit` job: replace the `- name: pytest` step

```yaml
      - name: pytest (+ coverage)
        # Unit tests only. Excludes:
        #   tests/e2e         — Playwright/TS, needs a browser + live stack
        #   tests/integration — needs a real Spark Connect server + real grpcio
        # grpcio/grpcio-status are deliberately NOT installed here, so this job
        # also proves the package imports under the Pyodide constraint (_grpc_shim).
        run: >
          pytest -q --ignore=tests/e2e --ignore=tests/integration
          --cov=pyspark_connect_web --cov-report=term-missing
          --cov-report=xml --cov-fail-under=90
```

(`pytest-cov` comes in via `pip install -e ".[dev]"` once snippet #1 lands.)

## State

- 145 unit tests pass + 1 xfail (the documented tz parity gap). 0 reds.
- New files only under `tests/`; no edits to `pyspark_connect_web/` source or to
  other lanes' existing test files; no edits to `pyproject.toml`/`ci.yml`.
- grpcio guard intact: no `grpc` import added under `pyspark_connect_web/`; the
  new shim tests manipulate `sys.modules`/`meta_path` only (test-side).
