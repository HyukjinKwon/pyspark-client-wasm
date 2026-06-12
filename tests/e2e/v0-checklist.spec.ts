// SPDX-License-Identifier: Apache-2.0
//
// e2e: the DECISIONS.md "v0 done =" checklist, in a real headless browser.
//
// SCAFFOLD STATE
// --------------
// The browser stack (lanes 1–4 + lane 3's JupyterLite build) is not runnable
// yet, so every checklist item below is a real Playwright test with a TODO hook
// describing exactly what to assert. The suite is safe to run today:
//   * `beforeAll` probes the stack; if it is DOWN, every test is SKIPPED
//     (unless E2E_REQUIRE_STACK=1, which turns "down" into a hard failure — the
//     CI gate to flip once the stack lands).
//   * items that depend on un-wired helpers are marked test.fixme() so they are
//     reported as expected-to-fail rather than red. Remove the fixme + fill the
//     hook as each lane lands.
//
// Mapping to DECISIONS.md:
//   1. crossOriginIsolated === true               (COOP/COEP, #4)  <-- implementable now
//   2. spark.range(10).collect() == 10 rows
//   3. filter/select/groupBy/agg toPandas == reference (#7 Arrow parity)
//   4. createDataFrame(pandas_df) round-trips
//   5. spark.sql("select 1 as x").collect() works
//   6. mid-stream disconnect recovers via ReattachExecute  (#6)

import { test, expect } from "@playwright/test";
import * as fs from "node:fs";
import * as path from "node:path";
import {
  isStackUp,
  requireStack,
  crossOriginIsolated,
  waitForKernel,
  runPython,
  injectMidStreamDisconnect,
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
        `reachable. Bring up deploy/compose.yaml and build the JupyterLite site.`,
    );
  }
});

test.beforeEach(async ({ page }, testInfo) => {
  if (!stackUp) {
    testInfo.skip(true, `stack down at ${BASE_URL}; skipping (set E2E_REQUIRE_STACK=1 to fail)`);
    return;
  }
  await page.goto(BASE_URL);
});

// ---------------------------------------------------------------------------
// 1. crossOriginIsolated === true  — implementable today (server headers only)
// ---------------------------------------------------------------------------
test("crossOriginIsolated is true on the JupyterLite page", async ({ page }) => {
  // This needs no lane wiring: it only checks that Envoy served COOP/COEP and
  // the browser turned on isolation. If this is false, SharedArrayBuffer (and
  // therefore the whole blocking bridge) cannot work. DECISIONS.md #4.
  const isolated = await crossOriginIsolated(page);
  expect(
    isolated,
    "crossOriginIsolated must be true — check Cross-Origin-Opener-Policy: " +
      "same-origin and Cross-Origin-Embedder-Policy: require-corp on the static host",
  ).toBe(true);
});

// ---------------------------------------------------------------------------
// 2. spark.range(10).collect() returns 10 rows
// ---------------------------------------------------------------------------
test.fixme("spark.range(10).collect() returns 10 rows", async ({ page }) => {
  await waitForKernel(page);
  // TODO: snippet should return the row count as JSON.
  const result = (await runPython(
    page,
    `import json; print(json.dumps(len(spark.range(10).collect())))`,
  )) as number;
  expect(result).toBe(10);
});

// ---------------------------------------------------------------------------
// 3. filter/select/groupBy/agg toPandas matches the native reference
// ---------------------------------------------------------------------------
test.fixme("filter/groupBy/agg toPandas matches reference", async ({ page }) => {
  await waitForKernel(page);

  // Reference produced by tests/e2e/reference.py against a native Connect client.
  expect(
    fs.existsSync(REFERENCE_PATH),
    `reference results missing at ${REFERENCE_PATH}; run tests/e2e/reference.py`,
  ).toBe(true);
  const reference = JSON.parse(fs.readFileSync(REFERENCE_PATH, "utf-8"));

  // TODO: run the SAME query in the browser and return its toPandas() as
  // records JSON, so we can compare row-for-row (DECISIONS.md #7 byte/row exact).
  const browserResult = (await runPython(
    page,
    `
import json
df = (spark.range(100)
        .filter("id % 2 = 0")
        .select((spark.range(0).id).alias("id"))  # placeholder; mirror reference.py
     )
print(json.dumps(df.toPandas().to_dict(orient="records")))
`,
  )) as unknown[];

  expect(browserResult).toEqual(reference.filter_groupby_agg);
});

// ---------------------------------------------------------------------------
// 4. createDataFrame(pandas_df) round-trips
// ---------------------------------------------------------------------------
test.fixme("createDataFrame(pandas_df) round-trips", async ({ page }) => {
  await waitForKernel(page);
  // TODO: build a small pandas DataFrame in-kernel, createDataFrame it, collect
  // it back, and assert the round-trip equals the input (lane 4 encode path).
  const ok = (await runPython(
    page,
    `
import json, pandas as pd
pdf = pd.DataFrame({"a": [1, 2, 3], "b": ["x", "y", "z"]})
out = spark.createDataFrame(pdf).toPandas()
print(json.dumps(out.equals(pdf)))
`,
  )) as boolean;
  expect(ok).toBe(true);
});

// ---------------------------------------------------------------------------
// 5. spark.sql("select 1 as x").collect() works
// ---------------------------------------------------------------------------
test.fixme("spark.sql round-trips", async ({ page }) => {
  await waitForKernel(page);
  const rows = (await runPython(
    page,
    `import json; print(json.dumps([r.asDict() for r in spark.sql("select 1 as x").collect()]))`,
  )) as Array<Record<string, number>>;
  expect(rows).toEqual([{ x: 1 }]);
});

// ---------------------------------------------------------------------------
// 6. mid-stream disconnect recovers via ReattachExecute (DECISIONS.md #6)
// ---------------------------------------------------------------------------
test.fixme("mid-stream disconnect recovers via ReattachExecute", async ({ page }) => {
  await waitForKernel(page);
  // Arrange a query large enough to span multiple response frames, drop the
  // connection mid-stream, and assert the reattachable iterator recovers and
  // still returns the full, correct result.
  await injectMidStreamDisconnect(page);
  const count = (await runPython(
    page,
    `import json; print(json.dumps(spark.range(1_000_000).count()))`,
  )) as number;
  expect(count).toBe(1_000_000);
});
