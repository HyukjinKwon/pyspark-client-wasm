<!-- SPDX-License-Identifier: Apache-2.0 -->

# DECISIONS.md - invariants that must not regress

These are load-bearing. If you must change one, append to COORDINATION.md with the
reason, and update the guard test.

1. **No `grpcio`, ever.** It is not in Pyodide and never will be (C-ext + raw
   sockets). All transport is grpc-web over `fetch`. CI must fail if `grpcio` is
   imported anywhere in `pyspark_connect_web/`.
2. **We patch, we do not fork.** Zero copies of `pyspark.sql.connect` source. The
   only thing we replace at runtime is the service stub (and the connection
   parser). If a patch needs a private symbol, pin the pyspark range that has it.
3. **Pinned pyspark range.** v0 targets `pyspark>=4.0,<4.2` (Connect default,
   reattachable execute present). `install()` raises a clear error outside the range.
4. **Cross-origin isolation is mandatory.** SharedArrayBuffer requires the page to
   be served with `Cross-Origin-Opener-Policy: same-origin` and
   `Cross-Origin-Embedder-Policy: require-corp`. Envoy/JupyterLite host config
   MUST set these; e2e MUST assert `crossOriginIsolated === true` before importing.
5. **`.collect()` stays blocking.** The public PySpark API is synchronous. The
   Atomics/SAB bridge must make the worker block; we do NOT expose an async fork of
   the DataFrame API. Guard: a test that calls a plain `.collect()` and gets rows.
6. **Reattachable execute is in-scope.** The client issues ExecutePlan +
   ReattachExecute + ReleaseExecute. The stub must implement all three, not just
   the stream - broken streams must recover. Guard: kill a stream mid-flight, assert
   recovery via reattach.
7. **Arrow correctness over speed.** Results must be byte/row-exact vs a
   reference run of the same query on plain PySpark Connect. Guard: a parity test
   comparing `toPandas()` between web client and a native Connect client.

## v0 done = all green, e2e in a real headless browser:
- [ ] `crossOriginIsolated === true` on the JupyterLite page
- [ ] `spark.range(10).collect()` returns 10 rows
- [ ] `spark.range(100).filter(...).select(...).groupBy(...).agg(...).toPandas()` matches reference
- [ ] `spark.createDataFrame(pandas_df)` round-trips
- [ ] `spark.sql("select 1 as x").collect()` works
- [ ] a mid-stream disconnect recovers via ReattachExecute
