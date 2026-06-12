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
  // No retries: the gate must be honest. The intermittent spark.sql hang was a
  // real SAB-bridge deadlock (a back-to-back RPC whose S_REQ_READY raced the
  // abandoned stream's S_IDLE), now fixed at the source in bridge.js /
  // sab_channel.py and regression-covered in tests/js/bridge.test.mjs. We do not
  // paper over it with retries.
  retries: 0,
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
