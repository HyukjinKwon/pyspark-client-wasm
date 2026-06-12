<!-- SPDX-License-Identifier: Apache-2.0 -->

# pyspark-connect-web — Team Coordination

**Goal:** Run the *real* PySpark Connect Python client inside JupyterLite/Pyodide,
talking to a Spark Connect server through a grpc-web/fetch transport. Framing:
**"PySpark in JupyterLite."** We monkey-patch `pyspark.sql.connect`; we do not
fork it. License: Apache-2.0 header on every source file.

**v0 target (locked):** full read-path parity — `range/select/filter/groupBy/agg`,
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
| 1 | grpc-web stub + framing | `pyspark_connect_web/transport/*` | (open) | |
| 2 | monkey-patch + integration | `pyspark_connect_web/__init__.py`, `patch.py`, `_contract.py` | (open) | |
| 3 | Pyodide sync bridge + JupyterLite | `pyspark_connect_web/worker/*`, `jupyterlite/*` | (open) | |
| 4 | Arrow result decoding | `pyspark_connect_web/arrow/*` | (open) | |
| 5 | Envoy proxy + e2e tests + docs | `deploy/*`, `tests/*`, `README.md`, `docs/*` | (open) | |
| — | contract / scaffold / integration | `API_CONTRACT.md`, `DECISIONS.md`, `_contract_seam.py`, `pyproject.toml` | INTEGRATOR | done (v0 scaffold) |

## Conventions
- Package: `pyspark_connect_web`. Python 3.11 locally; **target Pyodide ≥ 0.28 / Py 3.13** in browser.
- Deps already in Pyodide: `pyarrow>=22`, `pandas`, `protobuf>=7`, `numpy`. **`grpcio` is NOT available — never import it.**
- Local dev/tests: use a venv. Do NOT import `grpcio` even in tests; stub the transport.
- Apache-2.0 header on every source file. Pin a tested `pyspark` range (DECISIONS.md).
- **Don't rewrite a file another lane owns.** Build on the contract; if you need a
  contract change, edit `API_CONTRACT.md` AND append a note here first.
- Record findings in `team/findings-lane<N>-<topic>.md`, not inline in code.
- Keep `DECISIONS.md` invariants green; add a guard test when you fix a subtle bug.

## Notes log (append below — newest last; prefix with your lane + timestamp)
- INTEGRATOR 2026-06-12: Scaffolded repo, froze the stub seam in `API_CONTRACT.md`
  + `_contract_seam.py`, locked v0 = full read-path parity, bridge = Atomics+SAB
  (so COOP/COEP is mandatory — see DECISIONS.md). Lanes: claim your row above and
  start. The hardest seam is lane1⟷lane3 (the `SyncChannel` byte boundary) — agree
  on it here before diverging.
