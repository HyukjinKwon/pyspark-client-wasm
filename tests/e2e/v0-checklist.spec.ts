// SPDX-License-Identifier: Apache-2.0
//
// e2e: the DECISIONS.md "v0 done =" checklist, in a real headless browser.
//
// These tests drive lane 3's window.__pcwRunPython(src) bridge
// (pyspark_connect_web/jupyterlite/run_python_bridge.js) and assert the v0
// matrix from DECISIONS.md.
//
// SKIP SEMANTICS (graceful degradation):
//   * beforeAll probes the JupyterLite page. If the stack is DOWN, every test
//     is skipped - unless E2E_REQUIRE_STACK=1, which turns "down" into a hard
//     failure (the CI gate to flip once the stack lands).
//   * The crossOriginIsolated test needs only server headers and runs whenever
//     the page is up.
//   * The bridge-dependent tests additionally require window.__pcwRunPython.
//     If the page is up but the bridge is not wired yet (e.g. the JupyterLite
//     kernel integration in team/findings-lane3-bridge.md #1 is still pending),
//     they skip with a clear reason - unless E2E_REQUIRE_STACK=1, where a
//     missing bridge is a hard failure.
//
// Mapping to DECISIONS.md "v0 done =":
//   1. crossOriginIsolated === true                          (#4)
//   2. spark.range(10).collect() == 10 rows
//   3. filter/select/groupBy/agg toPandas == native reference (#7 Arrow parity)
//   4. createDataFrame(pandas_df) round-trips
//   5. spark.sql("select 1 as x").collect() works
//   6. mid-stream disconnect recovers via ReattachExecute    (#6)
//
// Query #3 is kept BYTE-FOR-BYTE in lockstep with tests/e2e/reference.py.

import { test, expect } from "@playwright/test";
import * as fs from "node:fs";
import * as path from "node:path";
import {
  isStackUp,
  requireStack,
  crossOriginIsolated,
  bridgeAvailable,
  waitForKernel,
  runPython,
  injectMidStreamDisconnect,
  SPARK_REMOTE,
} from "./helpers";

const BASE_URL = process.env.E2E_BASE_URL || "http://localhost:8000";
const REFERENCE_PATH =
  process.env.E2E_REFERENCE || path.join(__dirname, "reference.json");

let stackUp = false;

test.beforeAll(async () => {
  stackUp = await isStackUp(BASE_URL);
  if (!stackUp && requireStack()) {
    throw new Error(
      `E2E_REQUIRE_STACK=1 but the JupyterLite page at ${BASE_URL} is not ` +
        `reachable. Bring up deploy/compose.yaml and build the JupyterLite site ` +
        `(scripts/build_site.sh).`,
    );
  }
});

test.beforeEach(async ({ page }, testInfo) => {
  if (!stackUp) {
    testInfo.skip(
      true,
      `stack down at ${BASE_URL}; skipping (set E2E_REQUIRE_STACK=1 to fail)`,
    );
    return;
  }
  // Surface in-browser diagnostics in the CI log: console messages, page
  // errors, and failed network requests (the exact reason a fetch died, e.g.
  // CORS/net::ERR_*). Invaluable while stabilising the SAB/grpc-web round-trip.
  page.on("console", (m) => console.log(`[browser:${m.type()}]`, m.text()));
  page.on("pageerror", (e) => console.log("[browser:pageerror]", e.message));
  page.on("requestfailed", (r) =>
    console.log(
      "[browser:requestfailed]",
      r.url(),
      r.failure()?.errorText ?? "",
    ),
  );
  await page.goto(BASE_URL);
});

/**
 * Gate a bridge-dependent test: skip if window.__pcwRunPython is absent (unless
 * E2E_REQUIRE_STACK=1), else wait for the kernel to be ready.
 */
async function gateBridge(page: import("@playwright/test").Page, testInfo: import("@playwright/test").TestInfo) {
  const haveBridge = await bridgeAvailable(page);
  if (!haveBridge) {
    if (requireStack()) {
      throw new Error(
        `E2E_REQUIRE_STACK=1 but window.__pcwRunPython is not present on ${BASE_URL}. ` +
          `Lane 3's run_python_bridge.js must be wired into the page (see ` +
          `team/findings-lane3-bridge.md open item #1).`,
      );
    }
    testInfo.skip(
      true,
      `window.__pcwRunPython not wired on the page yet; skipping (set ` +
        `E2E_REQUIRE_STACK=1 to fail). Remote=${SPARK_REMOTE}`,
    );
    return false;
  }
  await waitForKernel(page);
  return true;
}

// ---------------------------------------------------------------------------
// 1. crossOriginIsolated === true  - server headers only (no bridge needed)
// ---------------------------------------------------------------------------
test("crossOriginIsolated is true on the JupyterLite page", async ({ page }) => {
  const isolated = await crossOriginIsolated(page);
  expect(
    isolated,
    "crossOriginIsolated must be true - check Cross-Origin-Opener-Policy: " +
      "same-origin and Cross-Origin-Embedder-Policy: require-corp on the static host",
  ).toBe(true);
});

// ---------------------------------------------------------------------------
// 2. spark.range(10).collect() returns 10 rows
// ---------------------------------------------------------------------------
test("spark.range(10).collect() returns 10 rows", async ({ page }, testInfo) => {
  if (!(await gateBridge(page, testInfo))) return;
  const result = (await runPython(
    page,
    `import json; print(json.dumps(len(spark.range(10).collect())))`,
  )) as number;
  expect(result).toBe(10);
});

// ---------------------------------------------------------------------------
// 3. filter/select/groupBy/agg toPandas matches the native reference
//    (DECISIONS.md #7 - byte/row exact vs a native Connect run). The query
//    MUST match tests/e2e/reference.py::build_reference exactly.
// ---------------------------------------------------------------------------
test("filter/groupBy/agg toPandas matches reference", async ({ page }, testInfo) => {
  if (!(await gateBridge(page, testInfo))) return;

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
test("createDataFrame(pandas_df) round-trips", async ({ page }, testInfo) => {
  if (!(await gateBridge(page, testInfo))) return;
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
test("spark.sql round-trips", async ({ page }, testInfo) => {
  if (!(await gateBridge(page, testInfo))) return;
  const rows = (await runPython(
    page,
    `import json; print(json.dumps([r.asDict() for r in spark.sql("select 1 as x").collect()]))`,
  )) as Array<Record<string, number>>;
  expect(rows).toEqual([{ x: 1 }]);
});

// ---------------------------------------------------------------------------
// 6. mid-stream disconnect recovers via ReattachExecute (DECISIONS.md #6)
// ---------------------------------------------------------------------------
test("mid-stream disconnect recovers via ReattachExecute", async ({ page }, testInfo) => {
  if (!(await gateBridge(page, testInfo))) return;
  // Arm a one-shot abort of the next ExecutePlan stream, then run a query big
  // enough to span multiple response frames. PySpark's reattachable iterator
  // must reconnect (ReattachExecute, a different path we do NOT abort) and
  // still return the full, correct result.
  await injectMidStreamDisconnect(page);
  const count = (await runPython(
    page,
    `import json; print(json.dumps(spark.range(1_000_000).count()))`,
  )) as number;
  expect(count).toBe(1_000_000);
});
