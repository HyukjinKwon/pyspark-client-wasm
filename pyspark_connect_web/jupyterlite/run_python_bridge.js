// SPDX-License-Identifier: Apache-2.0
//
// run_python_bridge.js — the `window.__pcwRunPython(src)` hook lane 5's e2e
// harness drives (see tests/e2e/helpers.ts). It runs a snippet of Python in the
// Pyodide worker and resolves with the JSON-serialised result.
//
// Two deployment shapes:
//
//   A. Standalone demo harness (worker_bootstrap.js + bridge.js): this file
//      wires the page to that worker directly. `installRunPython(worker)`.
//
//   B. Inside JupyterLite: the kernel owns the worker and its own exec protocol.
//      There, __pcwRunPython is implemented by dispatching a kernel execute
//      request and reading the reply. That path is the open integration item in
//      team/findings-lane3-bridge.md (#1) and is stubbed here with a clear throw
//      so the e2e harness surfaces "not wired" rather than hanging.
//
// The contract the e2e harness expects (helpers.ts):
//   window.__pcwRunPython(src: string): Promise<string>
//   - runs `src` in the kernel, returns the repr/JSON of the last expression.

"use strict";

// Shape A: standalone harness over worker_bootstrap.js.
export function installRunPython(worker) {
  let seq = 0;
  const pending = new Map();

  worker.addEventListener("message", (ev) => {
    const msg = ev.data || {};
    if (msg.type === "pcw_result" && pending.has(msg.id)) {
      pending.get(msg.id).resolve(msg.result);
      pending.delete(msg.id);
    } else if (msg.type === "pcw_run_error" && pending.has(msg.id)) {
      pending.get(msg.id).reject(new Error(msg.message));
      pending.delete(msg.id);
    }
  });

  globalThis.__pcwRunPython = function (src) {
    const id = ++seq;
    return new Promise((resolve, reject) => {
      pending.set(id, { resolve, reject });
      worker.postMessage({ type: "pcw_run", id, code: src });
    });
  };
}

// Shape B: JupyterLite kernel. Open integration item — see findings #1.
export function installRunPythonForJupyterLite(/* kernelConnection */) {
  globalThis.__pcwRunPython = function () {
    return Promise.reject(
      new Error(
        "__pcwRunPython is not wired into the JupyterLite kernel yet — see " +
          "team/findings-lane3-bridge.md open item #1 (kernel worker integration)."
      )
    );
  };
}
