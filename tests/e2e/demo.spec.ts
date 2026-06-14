// SPDX-License-Identifier: Apache-2.0
//
// e2e: the EMBEDDED BI QUERY CELL demo (demo/index.html), driven through its UI.
//
// The harness spec (v0-checklist.spec.ts) and the kernel spec (kernel.spec.ts)
// drive Python via window.__pcwRunPython. THIS spec instead exercises the demo
// the way a user does: it loads /demo/ (staged into the built site by e2e.yml),
// waits for the page to boot Pyodide + bind `spark` + seed the retail dataset,
// then clicks through the actual UI - the table sidebar, the SQL editor, the Run
// button - and asserts the result grid shows real Spark results.
//
// It proves the "embed pyspark-connect-web as a query cell in your own web page"
// use case end-to-end: same boot path as the harness, plus the seeding +
// table-picker + grid that make it a usable BI surface.
//
// RECORDING: the whole session runs in a video-recording context; the video is
// saved to tests/e2e/demo-video/demo.webm and uploaded as a CI artifact (e2e.yml),
// which is the source for the README/docs demo GIF (a REAL run, not a mock). The
// final "guided showcase" test is a paced walkthrough that exists to make that
// recording watchable.
//
// The demo sets window.__pcwDemoReady (or window.__pcwDemoError) when its boot
// finishes - a behavior-neutral hook we wait on instead of polling the overlay.
// Boot (Pyodide cold start + in-browser wheel install + seeding) is slow, so we
// boot once in beforeAll and reuse the page in serial mode.

import { test, expect, type Page, type BrowserContext } from "@playwright/test";
import * as path from "node:path";
import { isStackUp, requireStack, crossOriginIsolated } from "./helpers";

// The demo page, served by the same Envoy static host (COOP/COEP) as the rest of
// the site, under /demo/. e2e.yml stages demo/index.html into _output/demo/.
const DEMO_URL = process.env.E2E_DEMO_URL || "http://localhost:8000/demo/";

// Pyodide cold start + in-browser pyspark-client wheel install + dataset seeding
// dwarfs the per-test default; match the kernel spec's budget.
const BOOT_TIMEOUT_MS = Number(
  process.env.E2E_DEMO_BOOT_TIMEOUT_MS || 240_000,
);

// A roomy viewport so the recorded video frames the sidebar + editor + grid.
const VIEW = { width: 1280, height: 800 };

test.describe.configure({ mode: "serial" });

let context: BrowserContext;
let page: Page;
let ready = false;
let stackUp = false;

test.beforeAll(async ({ browser }) => {
  test.setTimeout(BOOT_TIMEOUT_MS + 60_000);
  stackUp = await isStackUp(DEMO_URL);
  if (!stackUp) {
    if (requireStack()) {
      throw new Error(
        `E2E_REQUIRE_STACK=1 but the BI demo at ${DEMO_URL} is not reachable. ` +
          `Build the site, stage demo/index.html into _output/demo/, and bring ` +
          `up deploy/compose.yaml.`,
      );
    }
    return;
  }

  // Record the whole session to a webm; saved deterministically in afterAll.
  context = await browser.newContext({
    viewport: VIEW,
    recordVideo: { dir: path.join(__dirname, "demo-video"), size: VIEW },
  });
  page = await context.newPage();
  page.on("console", (m) => console.log(`[demo:${m.type()}]`, m.text()));
  page.on("pageerror", (e) => console.log("[demo:pageerror]", e.message));
  page.on("requestfailed", (r) =>
    console.log("[demo:requestfailed]", r.url(), r.failure()?.errorText ?? ""),
  );

  await page.goto(DEMO_URL);

  // The demo flips one of these once boot resolves (success or failure).
  await page.waitForFunction(
    () =>
      (globalThis as any).__pcwDemoReady === true ||
      typeof (globalThis as any).__pcwDemoError === "string",
    null,
    { timeout: BOOT_TIMEOUT_MS },
  );
  const err = await page.evaluate(() => (globalThis as any).__pcwDemoError);
  if (err) throw new Error(`BI demo boot failed: ${err}`);
  ready = true;
});

test.afterAll(async () => {
  // Video is finalized on close; save it to a stable path for the CI artifact.
  const vid = page ? page.video() : null;
  if (page) await page.close();
  if (context) await context.close();
  if (vid) {
    try {
      await vid.saveAs(path.join(__dirname, "demo-video", "demo.webm"));
    } catch (e) {
      console.log("[demo] video save failed:", (e as Error).message);
    }
  }
});

function gate(testInfo: import("@playwright/test").TestInfo): boolean {
  if (!stackUp) {
    testInfo.skip(true, `BI demo down at ${DEMO_URL}`);
    return false;
  }
  if (!ready) {
    testInfo.skip(true, `BI demo not ready at ${DEMO_URL}`);
    return false;
  }
  return true;
}

// Run a query through the demo UI and wait for its result grid to render.
async function runSql(sql: string): Promise<void> {
  await page.fill("#sql", sql);
  await page.click("#run");
  // Run button re-enables and a fresh grid renders when the query returns.
  await page.waitForFunction(
    () => {
      const btn = document.getElementById("run") as HTMLButtonElement | null;
      const grid = document.querySelector("#results table.grid tbody tr");
      const stat = document.getElementById("runstat")?.textContent ?? "";
      return !!grid && !!btn && !btn.disabled && stat !== "running...";
    },
    null,
    { timeout: 60_000 },
  );
}

test("demo page is cross-origin isolated (COOP/COEP -> SharedArrayBuffer)", async ({}, testInfo) => {
  if (!stackUp) {
    testInfo.skip(true, `BI demo down at ${DEMO_URL}`);
    return;
  }
  expect(
    await crossOriginIsolated(page),
    "crossOriginIsolated must be true on the demo page",
  ).toBe(true);
});

test("seeded retail tables appear in the sidebar (SHOW TABLES on real Spark)", async ({}, testInfo) => {
  if (!gate(testInfo)) return;
  const names = await page.$$eval("#tables .tbl", (els) =>
    els.map((e) => (e.textContent ?? "").trim()),
  );
  for (const t of ["customers", "orders", "products"]) {
    expect(
      names.some((n) => n.includes(t)),
      `expected table '${t}' in sidebar, got ${JSON.stringify(names)}`,
    ).toBe(true);
  }
});

test("clicking a table loads its schema (DESCRIBE TABLE)", async ({}, testInfo) => {
  if (!gate(testInfo)) return;
  await page.click("#tables .tbl:has-text('products')");
  await page.waitForFunction(
    () => {
      const cols = Array.from(
        document.querySelectorAll("#tables .schema .col .name"),
      ).map((e) => (e.textContent ?? "").trim());
      return cols.includes("product_name") && cols.includes("price");
    },
    null,
    { timeout: 30_000 },
  );
});

test("running a query renders real Spark results in the grid", async ({}, testInfo) => {
  if (!gate(testInfo)) return;
  // A DETERMINISTIC query (not the rand()-seeded price columns): orders is seeded
  // from range(5000), so the count is exactly 5000 on every run.
  await runSql("SELECT count(*) AS n FROM orders");
  const cell = (await page.textContent("#results table.grid tbody tr td"))?.trim();
  expect(cell).toBe("5000");
});

test("an analytics join query returns grouped rows with the right columns", async ({}, testInfo) => {
  if (!gate(testInfo)) return;
  await runSql(
    "SELECT c.country, count(*) AS orders " +
      "FROM orders o JOIN customers c ON o.customer_id = c.customer_id " +
      "GROUP BY c.country ORDER BY orders DESC",
  );
  const headers = await page.$$eval("#results table.grid thead th", (th) =>
    th.map((h) => (h.textContent ?? "").trim()),
  );
  expect(headers).toEqual(["country", "orders"]);
  const rowCount = await page.$$eval("#results table.grid tbody tr", (tr) => tr.length);
  expect(rowCount).toBeGreaterThan(0);
});

test("guided showcase (paced walkthrough for the demo recording)", async ({}, testInfo) => {
  if (!gate(testInfo)) return;
  const pause = (ms: number) => page.waitForTimeout(ms);

  // Browse a couple of tables (schema expands) to show the catalog side.
  for (const t of ["customers", "orders"]) {
    await page.click(`#tables .tbl:has-text('${t}')`);
    await pause(1100);
  }

  // Click each example chip and run it, pausing on results so the recording is
  // readable. Labels match demo/index.html's EXAMPLES.
  const examples = [
    "Top products by revenue",
    "Revenue by country",
    "Monthly revenue",
    "Top customers",
  ];
  for (const label of examples) {
    const chip = page.locator(`#examples .chip`, { hasText: label });
    if ((await chip.count()) === 0) continue;
    await chip.first().click(); // chip sets the SQL editor to the example query
    await pause(500);
    await page.click("#run");
    await page.waitForSelector("#results table.grid tbody tr", { timeout: 60_000 });
    await pause(1700);
  }
});
