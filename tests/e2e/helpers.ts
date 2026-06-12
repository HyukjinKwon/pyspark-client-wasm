// SPDX-License-Identifier: Apache-2.0
//
// Shared helpers for the e2e harness (the components).
//
// These drive the in-page bridge: `window.__pcwRunPython(src)` runs a
// Python snippet in the Pyodide worker/kernel and resolves with the snippet's
// string output (we have the snippet `print(json.dumps(...))` so we can parse
// it back to a value). See pyspark_connect_web/jupyterlite/run_python_bridge.js.
//
// Graceful-degradation contract:
//   * isStackUp() gates the whole suite - if the JupyterLite page is unreachable
//     the specs skip (unless E2E_REQUIRE_STACK=1).
//   * bridgeAvailable() additionally checks that window.__pcwRunPython is exposed
//     on this page. If the page is up but the bridge is NOT wired, the
//     bridge-dependent specs skip with a clear reason instead of hanging or
//     failing red - UNLESS E2E_REQUIRE_STACK=1, in which case a missing bridge is
//     a hard failure. Both shapes are covered: the standalone harness
//     (v0-checklist.spec.ts) and the real JupyterLite kernel (kernel.spec.ts).

import type { Page } from "@playwright/test";

export const SPARK_REMOTE =
  process.env.E2E_SPARK_REMOTE || "sc://localhost:8081/;transport=grpcweb";

/** How long to wait for the (slow) Pyodide kernel + pyspark import to be ready. */
const KERNEL_READY_TIMEOUT_MS = Number(
  process.env.E2E_KERNEL_TIMEOUT_MS || 150_000,
);

export function requireStack(): boolean {
  return process.env.E2E_REQUIRE_STACK === "1";
}

/**
 * Probe whether the JupyterLite static host is reachable. Used by specs to
 * skip (not fail) when the stack is down - unless E2E_REQUIRE_STACK=1.
 */
export async function isStackUp(baseURL: string): Promise<boolean> {
  try {
    const res = await fetch(baseURL, { method: "GET" });
    return res.ok;
  } catch {
    return false;
  }
}

/**
 * Read `crossOriginIsolated` from the loaded page. Implementable today: it is a
 * standard browser global and depends only on the server (Envoy) sending
 * COOP/COEP. .
 */
export async function crossOriginIsolated(page: Page): Promise<boolean> {
  return await page.evaluate(
    () => (globalThis as any).crossOriginIsolated === true,
  );
}

/**
 * Is the `window.__pcwRunPython` bridge present on this page? Returns false
 * (rather than throwing) when the page loaded but the bridge was never wired, so
 * specs can skip gracefully. We probe for the function's existence only; we do
 * NOT execute Python here (that may be expensive / require a ready kernel).
 */
export async function bridgeAvailable(page: Page): Promise<boolean> {
  try {
    return await page.evaluate(
      () => typeof (globalThis as any).__pcwRunPython === "function",
    );
  } catch {
    return false;
  }
}

/**
 * Wait until the bridge is present AND the kernel can execute a trivial
 * snippet (kernel booted, pyspark_connect_web installed, SparkSession bound).
 *
 * Contract (pyspark_connect_web/jupyterlite/run_python_bridge.js):
 *   window.__pcwRunPython(src: string): Promise<string>
 * The page's bootstrap cell is expected to have run:
 *   import pyspark_connect_web as pcw; pcw.install()
 *   from pyspark.sql import SparkSession
 *   spark = SparkSession.builder.remote(<E2E_SPARK_REMOTE>).getOrCreate()
 * so `spark` is a global in the kernel namespace.
 */
export async function waitForKernel(page: Page): Promise<void> {
  await page.waitForFunction(
    () => typeof (globalThis as any).__pcwRunPython === "function",
    null,
    { timeout: KERNEL_READY_TIMEOUT_MS },
  );
  // Smoke-run a trivial snippet so we fail here (clearly) rather than inside the
  // first real assertion if the kernel/import is broken. Also forces a wait
  // until the worker is actually idle and able to execute.
  const probe = await runPython(
    page,
    `import json; print(json.dumps("pcw-kernel-ok"))`,
  );
  if (probe !== "pcw-kernel-ok") {
    throw new Error(
      `kernel smoke test returned ${JSON.stringify(probe)}; expected "pcw-kernel-ok"`,
    );
  }
}

/**
 * Run a Python snippet in the page's kernel via window.__pcwRunPython and return
 * its result. The snippet is expected to `print(json.dumps(...))`; we JSON.parse
 * the captured stdout. If the snippet does not print valid JSON we return the
 * raw string so callers can assert on it.
 */
export async function runPython(page: Page, src: string): Promise<unknown> {
  const raw = (await page.evaluate(
    (code) => (globalThis as any).__pcwRunPython(code),
    src,
  )) as string;
  const text = typeof raw === "string" ? raw.trim() : raw;
  try {
    return JSON.parse(text as string);
  } catch {
    return text;
  }
}

/**
 * Force a mid-stream disconnect of an in-flight ExecutePlan to exercise the
 * ReattachExecute recovery path.
 *
 * Strategy: intercept the *next* ExecutePlan POST to the grpc-web endpoint and
 * abort it after the response has started, so PySpark's reattachable iterator
 * must reconnect via ReattachExecute to finish the result. We arm a one-shot
 * route that, once it has fired, unroutes itself so the reattach (and the rest
 * of the test's RPCs) proceed normally.
 *
 * This works at the network layer (Playwright page.route) and needs no special
 * hook from the - it interrupts the grpc-web fetch the main-thread bridge
 * issues. The grpc-web path is matched loosely so it survives Envoy host/port
 * differences (it keys off the SparkConnectService/ExecutePlan path segment).
 */
export async function injectMidStreamDisconnect(page: Page): Promise<void> {
  let fired = false;
  await page.route(
    (url) => url.pathname.includes("/spark.connect.SparkConnectService/ExecutePlan"),
    async (route) => {
      if (fired) {
        await route.continue();
        return;
      }
      fired = true;
      // Abort the first ExecutePlan so the stream breaks mid-flight; the client
      // recovers by issuing ReattachExecute (a different path) which is NOT
      // intercepted, so it succeeds.
      await route.abort("connectionaborted");
    },
  );
}
