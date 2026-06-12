// SPDX-License-Identifier: Apache-2.0
//
// pcw_runpython_bootstrap.js - closes the last browser-integration gap:
// installs `window.__pcwRunPython` (the hook tests/e2e drives) by attaching it
// to a LIVE JupyterLite kernel, using run_python_bridge.js Shape B.
//
// Why a tiny page script instead of a JupyterLab extension: we don't need the
// notebook UI. `__pcwRunPython` runs arbitrary code in the kernel, so the e2e
// can do its own `import pyspark_connect_web; pcw.install(); ...`. We just need
// ONE started kernel + the hook bound to it. We locate the booted front-end app
// by SHAPE (an object exposing `.serviceManager.kernels`) rather than a fixed
// global name (which varies across JupyterLite/Lab versions), start a python
// kernel off its service manager, and bind the hook. Validated in a real
// browser by tests/e2e/kernel.spec.ts.

"use strict";

import { installRunPythonForJupyterLite } from "./run_python_bridge.js";

const LOG = (...a) => console.log("[pcw-runpython]", ...a);

// A Jupyter front-end app exposes `.serviceManager` with `.kernels` +
// `.kernelspecs`. The global it lives on is not stable across JupyterLite/Lab
// versions (jupyterapp, jupyterlab, ...), so find it by SHAPE rather than name:
// scan globalThis for the first object carrying a service manager.
function findJupyterApp() {
  for (const k of ["jupyterapp", "jupyterlab", "_jupyterapp", "jupyterlite"]) {
    const v = globalThis[k];
    if (v && v.serviceManager && v.serviceManager.kernels) return v;
  }
  for (const k of Object.keys(globalThis)) {
    let v;
    try { v = globalThis[k]; } catch (_) { continue; }
    if (
      v && typeof v === "object" &&
      v.serviceManager && v.serviceManager.kernels && v.serviceManager.kernelspecs
    ) {
      LOG("found app on global:", k);
      return v;
    }
  }
  return null;
}

function waitForApp({ tries = 600, intervalMs = 200 } = {}) {
  return new Promise((resolve, reject) => {
    let n = 0;
    const t = setInterval(() => {
      const app = findJupyterApp();
      if (app) { clearInterval(t); resolve(app); return; }
      if (n % 10 === 0) {
        // Diagnostic: what jupyter-ish globals exist while we wait?
        const keys = Object.keys(globalThis).filter((k) => /jup|lite|lab/i.test(k));
        LOG(`waiting for app at ${globalThis.location && globalThis.location.href}; ` +
            `jupyter-ish globals: ${keys.join(", ") || "(none)"}`);
      }
      if (++n >= tries) {
        clearInterval(t);
        reject(new Error("pcw: timed out waiting for a JupyterLite app with a serviceManager"));
      }
    }, intervalMs);
  });
}

async function boot() {
  if (globalThis.__pcwRunPython) return; // already wired
  const app = await waitForApp();
  if (app.restored) { try { await app.restored; } catch (_) {} }
  const sm = app.serviceManager;
  if (!sm) throw new Error("pcw: app has no serviceManager");
  if (sm.ready) { try { await sm.ready; } catch (_) {} }

  // Start a fresh python kernel (Pyodide kernel registers as "python").
  const specs = sm.kernelspecs && sm.kernelspecs.specs;
  const name = (specs && specs.default) || "python";
  LOG("starting kernel:", name);
  const kernel = await sm.kernels.startNew({ name });

  installRunPythonForJupyterLite(kernel);
  LOG("window.__pcwRunPython is ready");
}

boot().catch((e) => console.error("[pcw-runpython] failed:", e));
