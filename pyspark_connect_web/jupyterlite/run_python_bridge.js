// SPDX-License-Identifier: Apache-2.0
//
// run_python_bridge.js - the `window.__pcwRunPython(src)` hook the e2e
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
//      request and reading the reply (installRunPythonForJupyterLite). This is
//      wired up by pcw_runpython_bootstrap.js and exercised end-to-end by
//      tests/e2e/kernel.spec.ts (against jupyterlite-pyodide-kernel >= 0.7).
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

// Shape B: JupyterLite kernel. Drives the kernel's own execute protocol via a
// Jupyter `Kernel.IKernelConnection` (the object the JupyterLab/Lite services
// manager hands you). The kernel worker already runs Pyodide (and, when the
// page is cross-origin isolated, our SAB bridge is attached via
// pcw_kernel_bridge.js). We just submit code and collect the reply text.
//
// `kernelConnection` must expose `.requestExecute({code})` returning an
// IFuture with `.onIOPub` and `.done` - the standard @jupyterlab/services shape.
export function installRunPythonForJupyterLite(kernelConnection) {
  if (!kernelConnection || typeof kernelConnection.requestExecute !== "function") {
    throw new Error(
      "installRunPythonForJupyterLite: expected a Jupyter IKernelConnection " +
        "(with .requestExecute). Pass the kernel from the Lite services manager."
    );
  }
  globalThis.__pcwRunPython = function (src) {
    return new Promise((resolve, reject) => {
      const future = kernelConnection.requestExecute({ code: src });
      let out = "";
      future.onIOPub = (msg) => {
        const t = msg.header && msg.header.msg_type;
        const c = msg.content || {};
        if (t === "execute_result" && c.data && c.data["text/plain"]) {
          out = c.data["text/plain"];
        } else if (t === "stream" && c.text) {
          out += c.text;
        } else if (t === "error") {
          reject(new Error(((c.ename || "") + ": " + (c.evalue || "")).trim()));
        }
      };
      future.done.then(() => resolve(out)).catch(reject);
    });
  };
}
