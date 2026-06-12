// SPDX-License-Identifier: Apache-2.0
//
// e2e: the v0 matrix checklist, in a real headless browser.
//
// These tests drive the standalone harness (pyspark_connect_web/jupyterlite/
// harness.html) which boots Pyodide in a Web Worker, micropip-installs pyspark
// + the wheel, runs pcw.install(), binds a SparkSession over grpc-web, and
// exposes window.__pcwRunPython(src). We assert the v0 matrix from the design notes.
//
// SINGLE BOOT: Pyodide cold start (loadPackage pyarrow/pandas + micropip
// pyspark) is ~60-90s, so we boot ONCE in beforeAll on a shared page and run
// all checks against it (serial mode). Re-booting per test was ~5x slower and
// spammed the logs.
//
// Mapping to the v0 matrix:
//   1. crossOriginIsolated === true                          (#4)
//   2. spark.range(10).collect() == 10 rows
//   3. filter/select/groupBy/agg toPandas == native reference (#7 Arrow parity)
//   4. createDataFrame(pandas_df) round-trips
//   5. spark.sql("select 1 as x").collect() works
//   6. mid-stream disconnect recovers via ReattachExecute    (#6) - see fixme note

import { test, expect, type Page } from "@playwright/test";
import * as fs from "node:fs";
import * as path from "node:path";
import {
  isStackUp,
  requireStack,
  crossOriginIsolated,
  bridgeAvailable,
  waitForKernel,
  runPython,
} from "./helpers";

const BASE_URL = process.env.E2E_BASE_URL || "http://localhost:8000";
const REFERENCE_PATH =
  process.env.E2E_REFERENCE || path.join(__dirname, "reference.json");

// One booted page shared by every test in this file.
test.describe.configure({ mode: "serial" });

let page: Page;
let stackUp = false;
let bridgeReady = false;

test.beforeAll(async ({ browser }) => {
  stackUp = await isStackUp(BASE_URL);
  if (!stackUp) {
    if (requireStack()) {
      throw new Error(
        `E2E_REQUIRE_STACK=1 but the page at ${BASE_URL} is not reachable. ` +
          `Bring up deploy/compose.yaml and build the site (scripts/build_site.sh).`,
      );
    }
    return;
  }

  page = await browser.newPage();
  // Surface in-browser diagnostics in the CI log (console, page errors, failed
  // requests) - the exact reason a fetch died (CORS/net::ERR_*), etc.
  page.on("console", (m) => console.log(`[browser:${m.type()}]`, m.text()));
  page.on("pageerror", (e) => console.log("[browser:pageerror]", e.message));
  page.on("requestfailed", (r) =>
    console.log("[browser:requestfailed]", r.url(), r.failure()?.errorText ?? ""),
  );

  await page.goto(BASE_URL);

  bridgeReady = await bridgeAvailable(page);
  if (!bridgeReady) {
    if (requireStack()) {
      throw new Error(
        `E2E_REQUIRE_STACK=1 but window.__pcwRunPython is not present on ${BASE_URL}.`,
      );
    }
    return;
  }
  // Boot Pyodide + bind `spark` ONCE (slow); all bridge tests reuse it.
  await waitForKernel(page);
});

test.afterAll(async () => {
  if (page) await page.close();
});

function skipUnlessBridge(testInfo: import("@playwright/test").TestInfo): boolean {
  if (!stackUp) {
    testInfo.skip(true, `stack down at ${BASE_URL}`);
    return false;
  }
  if (!bridgeReady) {
    testInfo.skip(true, `window.__pcwRunPython not wired on the page`);
    return false;
  }
  return true;
}

// ---------------------------------------------------------------------------
// 1. crossOriginIsolated === true  - server headers only (no bridge needed)
// ---------------------------------------------------------------------------
test("crossOriginIsolated is true on the page", async ({}, testInfo) => {
  if (!stackUp) {
    testInfo.skip(true, `stack down at ${BASE_URL}`);
    return;
  }
  const isolated = await crossOriginIsolated(page);
  expect(
    isolated,
    "crossOriginIsolated must be true - check COOP: same-origin + COEP on the host",
  ).toBe(true);
});

// ---------------------------------------------------------------------------
// 2. spark.range(10).collect() returns 10 rows
// ---------------------------------------------------------------------------
test("spark.range(10).collect() returns 10 rows", async ({}, testInfo) => {
  if (!skipUnlessBridge(testInfo)) return;
  const result = (await runPython(
    page,
    `import json; print(json.dumps(len(spark.range(10).collect())))`,
  )) as number;
  expect(result).toBe(10);
});

// ---------------------------------------------------------------------------
// 3. filter/select/groupBy/agg toPandas matches the native reference
//. MUST match tests/e2e/reference.py::build_reference.
// ---------------------------------------------------------------------------
test("filter/groupBy/agg toPandas matches reference", async ({}, testInfo) => {
  if (!skipUnlessBridge(testInfo)) return;
  expect(
    fs.existsSync(REFERENCE_PATH),
    `reference results missing at ${REFERENCE_PATH}; run tests/e2e/reference.py`,
  ).toBe(true);
  const reference = JSON.parse(fs.readFileSync(REFERENCE_PATH, "utf-8"));

  const browserResult = (await runPython(
    page,
    `
import json
from pyspark.sql import functions as F
df = (spark.range(100)
        .filter("id % 2 = 0")
        .select((F.col("id") % 10).alias("bucket"), F.col("id"))
        .groupBy("bucket")
        .agg(F.count("*").alias("n"), F.sum("id").alias("sum_id"))
        .orderBy("bucket"))
print(json.dumps(df.toPandas().to_dict(orient="records")))
`,
  )) as unknown[];

  expect(browserResult).toEqual(reference.filter_groupby_agg);
});

// ---------------------------------------------------------------------------
// 4. createDataFrame(pandas_df) round-trips
// ---------------------------------------------------------------------------
test("createDataFrame(pandas_df) round-trips", async ({}, testInfo) => {
  if (!skipUnlessBridge(testInfo)) return;
  const ok = (await runPython(
    page,
    `
import json, pandas as pd
pdf = pd.DataFrame({"a": [1, 2, 3], "b": ["x", "y", "z"]})
out = spark.createDataFrame(pdf).toPandas()
print(json.dumps(bool(out.equals(pdf))))
`,
  )) as boolean;
  expect(ok).toBe(true);
});

// ---------------------------------------------------------------------------
// 5. spark.sql("select 1 as x").collect() works
// ---------------------------------------------------------------------------
test("spark.sql round-trips", async ({}, testInfo) => {
  if (!skipUnlessBridge(testInfo)) return;
  const rows = (await runPython(
    page,
    `import json; print(json.dumps([r.asDict() for r in spark.sql("select 1 as x").collect()]))`,
  )) as Array<Record<string, number>>;
  expect(rows).toEqual([{ x: 1 }]);
});

// ---------------------------------------------------------------------------
// 6. mid-stream disconnect recovers via ReattachExecute
//
// FIXME / KNOWN LIMITATION (not a regression): this browser test aborts the
// INITIAL ExecutePlan at the network layer, so no operation ever starts on the
// server. PySpark's reattachable iterator can only recover that case by reading
// `INVALID_HANDLE.OPERATION_NOT_FOUND` from the gRPC error's google.rpc.Status
// via grpcio-status (grpc_status.rpc_status.from_call). That is fundamentally
// unavailable over grpc-web in the browser (no real gRPC call / trailing
// metadata; our Pyodide grpc_status stub returns None), so PySpark loops on
// ReattachExecute instead of restarting. ReattachExecute recovery for a REAL
// mid-stream cut (operation exists, stream breaks) IS verified against real
// Spark by tests/integration/test_real_round_trip.py::
// test_midstream_disconnect_recovers_via_reattach (green in the `ci` workflow).
// ---------------------------------------------------------------------------
test.fixme(
  "mid-stream disconnect recovers via ReattachExecute (covered by the integration test)",
  async () => {
    // Intentionally not executed in-browser - see the note above. Reattach
    // resilience is verified server-side by the integration suite.
  },
);
