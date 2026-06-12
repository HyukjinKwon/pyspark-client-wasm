// SPDX-License-Identifier: Apache-2.0
//
// e2e: the REAL JupyterLite Pyodide kernel path (not the standalone harness).
//
// The harness spec (v0-checklist.spec.ts) drives harness.html, which boots its
// OWN worker via worker_bootstrap.js. THIS spec instead loads the built
// JupyterLite site root, where build_site.sh injected pcw_kernel_bridge.js (wraps
// the kernel's Web Worker with the SAB Bridge) + pcw_runpython_bootstrap.js
// (starts a real kernel and binds window.__pcwRunPython to it via
// run_python_bridge.js Shape B). So this exercises the kernel transport
// ("transport=kernel": the namespaced {__pcw__:{...}} envelope), the self-hosted
// Pyodide (jupyter-lite.json pyodideUrl -> /pyodide/pyodide.js), and the in-kernel
// micropip install of the slim pyspark-client + the pcw wheel - the path the demo
// notebook uses. It is the integration the harness path cannot cover.
//
// SINGLE BOOT: the kernel cold start + in-kernel micropip install is very slow,
// so we boot once in beforeAll and reuse the kernel (it keeps namespace state) in
// serial mode.

import { test, expect, type Page } from "@playwright/test";
import {
  isStackUp,
  requireStack,
  crossOriginIsolated,
  runPython,
} from "./helpers";

// The built JupyterLite app root (NOT the harness). Served by the same Envoy
// static host with COOP/COEP, on :8000 by default.
const LITE_URL = process.env.E2E_LITE_URL || "http://localhost:8000/";
const SPARK_REMOTE =
  process.env.E2E_SPARK_REMOTE || "sc://localhost:8081/;transport=grpcweb";

// The kernel boots JupyterLite + Pyodide, then we micropip-install in-kernel;
// both are slow, so allow more than the per-test default.
const KERNEL_BOOT_TIMEOUT_MS = Number(
  process.env.E2E_KERNEL_BOOT_TIMEOUT_MS || 240_000,
);

test.describe.configure({ mode: "serial" });

let page: Page;
let kernelReady = false;
let stackUp = false;

test.beforeAll(async ({ browser }) => {
  // Kernel cold start + in-kernel micropip install dwarfs the config timeout.
  test.setTimeout(KERNEL_BOOT_TIMEOUT_MS + 60_000);
  stackUp = await isStackUp(LITE_URL);
  if (!stackUp) {
    if (requireStack()) {
      throw new Error(
        `E2E_REQUIRE_STACK=1 but the JupyterLite site at ${LITE_URL} is not ` +
          `reachable. Bring up deploy/compose.yaml and build the site.`,
      );
    }
    return;
  }

  page = await browser.newPage();
  page.on("console", (m) => console.log(`[kernel:${m.type()}]`, m.text()));
  page.on("pageerror", (e) => console.log("[kernel:pageerror]", e.message));
  page.on("requestfailed", (r) =>
    console.log("[kernel:requestfailed]", r.url(), r.failure()?.errorText ?? ""),
  );

  await page.goto(LITE_URL);

  // pcw_runpython_bootstrap.js starts a kernel off the Lite service manager and
  // binds window.__pcwRunPython to it. Wait for that hook (kernel boot is slow).
  await page.waitForFunction(
    () => typeof (globalThis as any).__pcwRunPython === "function",
    null,
    { timeout: KERNEL_BOOT_TIMEOUT_MS },
  );

  // Install the slim client + pcw IN THE KERNEL and bind `spark` (mirrors
  // demo.ipynb). micropip prints progress, so we check for a sentinel rather
  // than JSON-parsing. State persists in the kernel namespace across executes.
  const out = String(
    await runPython(
      page,
      `
import js, micropip
origin = js.location.origin
await micropip.install("protobuf>=7")
await micropip.install("googleapis-common-protos>=1.56.4")
await micropip.install("zstandard")
await micropip.install(f"{origin}/pyspark_client-4.1.2-py3-none-any.whl", deps=False)
await micropip.install(f"{origin}/pyspark_connect_web-0.1.0-py3-none-any.whl")
import pyspark_connect_web as pcw
pcw.install()
from pyspark.sql import SparkSession
globals()["spark"] = SparkSession.builder.remote("${SPARK_REMOTE}").getOrCreate()
print("PCW_KERNEL_INSTALL_OK")
`,
    ),
  );
  if (!out.includes("PCW_KERNEL_INSTALL_OK")) {
    throw new Error(`in-kernel install/bind failed; kernel output:\n${out}`);
  }
  kernelReady = true;
});

test.afterAll(async () => {
  if (page) await page.close();
});

function skipUnlessKernel(testInfo: import("@playwright/test").TestInfo): boolean {
  if (!stackUp) {
    testInfo.skip(true, `JupyterLite site down at ${LITE_URL}`);
    return false;
  }
  if (!kernelReady) {
    testInfo.skip(true, `kernel not ready on ${LITE_URL}`);
    return false;
  }
  return true;
}

test("lite page is cross-origin isolated (self-hosted Pyodide, COEP credentialless)", async ({}, testInfo) => {
  if (!stackUp) {
    testInfo.skip(true, `JupyterLite site down at ${LITE_URL}`);
    return;
  }
  expect(
    await crossOriginIsolated(page),
    "crossOriginIsolated must be true on the lite page",
  ).toBe(true);
});

test("kernel: spark.range(10).collect() returns 10 rows", async ({}, testInfo) => {
  if (!skipUnlessKernel(testInfo)) return;
  const n = (await runPython(
    page,
    `import json; print(json.dumps(len(spark.range(10).collect())))`,
  )) as number;
  expect(n).toBe(10);
});

test("kernel: spark.sql round-trips (command path through the kernel bridge)", async ({}, testInfo) => {
  if (!skipUnlessKernel(testInfo)) return;
  const rows = (await runPython(
    page,
    `import json; print(json.dumps([r.asDict() for r in spark.sql("select 1 as x").collect()]))`,
  )) as Array<Record<string, number>>;
  expect(rows).toEqual([{ x: 1 }]);
});

test("kernel: groupBy/agg toPandas executes", async ({}, testInfo) => {
  if (!skipUnlessKernel(testInfo)) return;
  const total = (await runPython(
    page,
    `
import json
from pyspark.sql import functions as F
df = spark.range(100).filter("id % 2 = 0").agg(F.count("*").alias("n"))
print(json.dumps(int(df.collect()[0]["n"])))
`,
  )) as number;
  expect(total).toBe(50);
});
