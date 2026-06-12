// SPDX-License-Identifier: Apache-2.0
//
// Playwright config for the pyspark-connect-web e2e harness (the components).
//
// The JupyterLite page requires cross-origin isolation (COOP/COEP, the design notes
// #4) so that SharedArrayBuffer is available to the Atomics/SAB blocking bridge.
// Chromium honours those headers from the server (Envoy static host) and exposes
// `crossOriginIsolated` - the first checklist assertion. No special launch flag
// is needed as long as the page is served with the headers; we keep the launch
// minimal and let the server-sent headers drive isolation.

import { defineConfig, devices } from "@playwright/test";

const BASE_URL = process.env.E2E_BASE_URL || "http://localhost:8000";

export default defineConfig({
  testDir: ".",
  testMatch: /.*\.spec\.ts/,
  // Pyodide cold start (loadPackage pyarrow/pandas + micropip pyspark + the
  // wheel) runs fresh on each test's page load and is slow.
  timeout: 150_000,
  expect: { timeout: 30_000 },
  fullyParallel: false,
  // Browser e2e against a freshly-booted full stack (Pyodide cold start + Spark
  // Connect/Envoy warmup) is occasionally flaky on the first commands; a
  // transient stall once wedged spark.sql for the whole timeout. Retry in CI so
  // an intermittent stall re-runs (in serial mode Playwright re-runs the whole
  // booted describe block) instead of failing the gate; a real break still fails
  // all attempts. Local runs do not retry (fail fast while iterating).
  retries: process.env.CI ? 2 : 0,
  reporter: process.env.CI ? "github" : "list",
  use: {
    baseURL: BASE_URL,
    headless: true,
    trace: "retain-on-failure",
    screenshot: "only-on-failure",
  },
  projects: [
    {
      name: "chromium",
      use: { ...devices["Desktop Chrome"] },
    },
  ],
});
