// SPDX-License-Identifier: Apache-2.0
//
// Shared helpers for the e2e harness (lane 5).
//
// These are intentionally thin: the heavy lifting (loading JupyterLite, getting
// a kernel, executing a cell and reading its result) depends on lane 3's
// JupyterLite kernel config and demo notebook, which do not exist yet. Each
// helper documents the contract it expects so lane 3 can wire to it, and throws
// a NOT_WIRED error until then. Specs gate on `isStackUp()` and skip when the
// stack is down, so a NOT_WIRED throw only surfaces once someone runs against a
// live page that hasn't implemented the hook.

import type { Page } from "@playwright/test";

export const SPARK_REMOTE =
  process.env.E2E_SPARK_REMOTE || "sc://localhost:8081/;transport=grpcweb";

export class NotWiredError extends Error {
  constructor(hook: string) {
    super(
      `e2e hook "${hook}" is not wired yet — pending lane 3 JupyterLite kernel ` +
        `+ demo notebook and lanes 1/2/4. See TODO in this file.`,
    );
    this.name = "NotWiredError";
  }
}

/**
 * Probe whether the JupyterLite static host is reachable. Used by specs to
 * skip (not fail) when the stack is down — unless E2E_REQUIRE_STACK=1.
 */
export async function isStackUp(baseURL: string): Promise<boolean> {
  try {
    const res = await fetch(baseURL, { method: "GET" });
    return res.ok;
  } catch {
    return false;
  }
}

export function requireStack(): boolean {
  return process.env.E2E_REQUIRE_STACK === "1";
}

/**
 * Read `crossOriginIsolated` from the loaded page. This is the one helper that
 * is fully implementable today: it is a standard browser global and depends
 * only on the server (Envoy) sending COOP/COEP. DECISIONS.md #4.
 */
export async function crossOriginIsolated(page: Page): Promise<boolean> {
  return await page.evaluate(() => (globalThis as any).crossOriginIsolated === true);
}

/**
 * Wait until the JupyterLite kernel is ready and `pyspark_connect_web` has been
 * installed + a SparkSession bound to a known global, ready to run cells.
 *
 * TODO(lane3): implement against the JupyterLite demo page. Expected contract:
 *   - the demo notebook (or a bootstrap cell) runs:
 *       import pyspark_connect_web as pcw; pcw.install()
 *       from pyspark.sql import SparkSession
 *       spark = SparkSession.builder.remote(<E2E_SPARK_REMOTE>).getOrCreate()
 *   - the page exposes a small JS bridge to run a Python snippet in that kernel
 *     and return the repr/JSON of the result, e.g. window.__pcwRunPython(src).
 * This helper should resolve once that bridge exists and the kernel is idle.
 */
export async function waitForKernel(page: Page): Promise<void> {
  void page;
  throw new NotWiredError("waitForKernel");
}

/**
 * Run a Python snippet in the page's kernel and return its JSON-serialized
 * result. The snippet is expected to `print(json.dumps(...))` or assign a value
 * the bridge serializes.
 *
 * TODO(lane3): implement via the same window.__pcwRunPython bridge.
 */
export async function runPython(page: Page, src: string): Promise<unknown> {
  void page;
  void src;
  throw new NotWiredError("runPython");
}

/**
 * Force a mid-stream disconnect of an in-flight ExecutePlan, to exercise the
 * ReattachExecute recovery path (DECISIONS.md #6). Strategy options for whoever
 * wires this:
 *   - use Playwright's `page.route()` to abort the first ExecutePlan response
 *     stream after N bytes, letting the client's reattachable iterator recover;
 *   - or expose a test hook in lane 3's main-thread fetch bridge to drop the
 *     connection once.
 *
 * TODO(lane1/lane3): implement the disconnect injection.
 */
export async function injectMidStreamDisconnect(page: Page): Promise<void> {
  void page;
  throw new NotWiredError("injectMidStreamDisconnect");
}
