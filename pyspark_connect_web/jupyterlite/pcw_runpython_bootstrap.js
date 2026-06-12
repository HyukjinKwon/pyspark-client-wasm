// SPDX-License-Identifier: Apache-2.0
//
// pcw_runpython_bootstrap.js - closes the last browser-integration gap:
// installs `window.__pcwRunPython` (the hook tests/e2e drives) by attaching it
// to a LIVE JupyterLite kernel, using run_python_bridge.js Shape B.
//
// Why a tiny page script instead of a JupyterLab extension: we don't need the
// notebook UI. `__pcwRunPython` runs arbitrary code in the kernel, so the e2e
// can do its own `import pyspark_connect_web; pcw.install(); ...`. We just need
// ONE started kernel + the hook bound to it. JupyterLab 4 / JupyterLite expose
// the booted app as `window.jupyterapp`; we start a python kernel off its
// service manager and bind the hook. Validated only in a real browser (CI).

"use strict";

import { installRunPythonForJupyterLite } from "./run_python_bridge.js";

const LOG = (...a) => console.log("[pcw-runpython]", ...a);

function waitFor(get, { tries = 200, intervalMs = 100 } = {}) {
  return new Promise((resolve, reject) => {
    let n = 0;
    const t = setInterval(() => {
      let v;
      try { v = get(); } catch (_) { v = undefined; }
      if (v) { clearInterval(t); resolve(v); }
      else if (++n >= tries) { clearInterval(t); reject(new Error("pcw: timed out waiting for JupyterLite app/kernel")); }
    }, intervalMs);
  });
}

async function boot() {
  if (globalThis.__pcwRunPython) return; // already wired
  const app = await waitFor(() => globalThis.jupyterapp || globalThis.jupyterlab);
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
