<!-- SPDX-License-Identifier: Apache-2.0 -->

# Contributing to pyspark-connect-web

Contributions of all kinds - bug reports, documentation, examples, and code -
are welcome. This project runs the *real* PySpark Connect client in
JupyterLite/Pyodide by monkey-patching its gRPC stub with a grpc-web transport.
We **patch, we do not fork** PySpark; please keep changes within that model.

## Development setup

Use a **conda** environment (Python 3.11 locally; the browser runs Pyodide /
Python 3.13):

```bash
conda create -n pcw python=3.11
conda activate pcw
pip install -e ".[dev]"
```

The `dev` extra installs `pyspark>=4.0,<4.2`, `pyarrow`, `pandas`, `protobuf`,
`googleapis-common-protos`, and `pytest`. Note what is **deliberately absent**:

* **No `grpcio` / `grpcio-status`.** They are not available in Pyodide and must
  never be imported (DECISIONS.md #1). The package registers a lightweight gRPC
  shim (`pyspark_connect_web/_grpc_shim.py`) *before* PySpark is imported so
  `import grpc` resolves. The shim is a no-op if real `grpcio` is present, so it
  never shadows the genuine library in environments that happen to have it.

## Running tests

```bash
pytest -q
```

Unit tests **stub the transport** - they never import `grpcio` and never touch a
browser. CI fails if `grpcio` is imported anywhere under `pyspark_connect_web/`,
so keep test doubles in the test tree, not in the package.

Linting (scoped to the package):

```bash
ruff check pyspark_connect_web
```

### Browser end-to-end tests

The full DECISIONS.md "v0 done" matrix runs in a real headless browser against
the deploy stack:

```bash
docker compose -f deploy/compose.yaml up -d   # Spark Connect + Envoy + static host
make site                                     # build the JupyterLite site into _output/
cd tests/e2e && npm install && npx playwright install chromium
E2E_BASE_URL=http://localhost:8000 npx playwright test
```

See [`docs/running-locally.md`](docs/running-locally.md) for the reference
generator and troubleshooting. When the stack is down the e2e suite skips; set
`E2E_REQUIRE_STACK=1` to make a missing stack a hard failure (the CI gate).

## Lanes and coordination

Work is split into **lanes** with frozen interfaces; see [`COORDINATION.md`](COORDINATION.md)
for the lane map and ownership table, and [`API_CONTRACT.md`](API_CONTRACT.md)
for the stub seam between lanes. Two rules matter most:

* **Don't rewrite a file another lane owns.** Build on the contract.
* If you need a contract change, edit `API_CONTRACT.md` **and** append a dated
  note to `COORDINATION.md` first - every other lane builds against those shapes.

Keep the [`DECISIONS.md`](DECISIONS.md) invariants green, and add a guard test
when you fix a subtle bug. Apache-2.0 / SPDX header on every source file.

## Pull requests

1. Open an issue first for anything beyond a small fix.
2. Branch from the default branch.
3. Add or update tests; `pytest -q` and `ruff check pyspark_connect_web` must pass.
4. Keep the no-`grpcio` invariant and the lane contract intact.
5. Update docs/examples when behavior or the public surface changes.

By contributing, you agree that your contributions will be licensed under the
Apache License 2.0.
