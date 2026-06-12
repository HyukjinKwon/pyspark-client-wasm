// SPDX-License-Identifier: Apache-2.0
//
// Playwright config for the pyspark-connect-web e2e harness (lane 5).
//
// The JupyterLite page requires cross-origin isolation (COOP/COEP, DECISIONS.md
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
  // JupyterLite + Pyodide cold start (download + import pyspark) is slow.
  timeout: 180_000,
  expect: { timeout: 30_000 },
  fullyParallel: false,
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
