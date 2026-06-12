// SPDX-License-Identifier: Apache-2.0
//
// pcw_kernel_bridge.js — PAGE-side integration of lane 3's blocking transport
// into the JupyterLite pyodide kernel.
//
// Why this file exists
// --------------------
// `worker_bootstrap.js` is a *standalone* harness: it owns the worker. Inside
// JupyterLite we do NOT own the worker — `@jupyterlite/pyodide-kernel` creates
// its own ES-module worker (`new Worker(...)`) and runs Pyodide there with its
// own comms (coincident when cross-origin isolated, comlink otherwise). We
// cannot replace that worker.
//
// So we wire our bridge into it *non-invasively* from the page, by wrapping the
// global `Worker` constructor BEFORE JupyterLite boots. Every worker the kernel
// spawns gets our `message` listener attached. Our listener only reacts to our
// own namespaced envelope (`{ __pcw__: {...} }`) — every other message (the
// kernel's `_kernelMessage`/`_logMessage`, Comlink frames, coincident's
// reserved CHANNEL field) is ignored, and we never post anything those layers
// would try to interpret (our envelopes have no `id` field). This mirrors how
// the kernel itself namespaces its own messages.
//
// The Python half (`pyspark_connect_web.worker.kernel_bootstrap`, run inside the
// kernel worker) allocates the SAB, posts `{__pcw__:{type:"sab",...}}` once, and
// then `{__pcw__:{type:"rpc"}}` per request before parking on Atomics.wait. This
// page-side listener does the real cross-origin `fetch` and writes the response
// windows back into the SAB — exactly like `bridge.js`, reusing its `Bridge`.
//
// Load order (see jupyter-lite.json / index.template.html): this script and
// `coi-serviceworker.js` (for header-less hosts like GitHub Pages) MUST run
// before the JupyterLite app bundle so the Worker wrapper is in place and the
// page is cross-origin isolated.

"use strict";

import { installBridge, Bridge } from "../worker/bridge.js";

// Guard against double-install (HMR / multiple imports).
if (!globalThis.__pcwKernelBridgeInstalled) {
  globalThis.__pcwKernelBridgeInstalled = true;

  const NativeWorker = globalThis.Worker;

  // A Worker subclass that attaches a pcw Bridge to every kernel worker and
  // adapts our namespaced envelope to bridge.js's {type:"pcw_sab"|"pcw_rpc"}.
  class PcwWorker extends NativeWorker {
    constructor(scriptURL, options) {
      super(scriptURL, options);
      const bridge = new Bridge();
      this.__pcwBridge = bridge;
      this.addEventListener("message", (ev) => {
        const data = ev && ev.data;
        const env = data && data.__pcw__;
        if (!env) return; // not ours — let the kernel handle it
        if (env.type === "sab") {
          bridge.attach(env.control, env.data);
        } else if (env.type === "rpc") {
          bridge.handleRpc();
        }
      });
    }
  }

  // Only wrap once. JupyterLite reads `Worker` off the global scope at boot.
  try {
    globalThis.Worker = PcwWorker;
  } catch (e) {
    // Some environments make Worker non-writable; fall back to a no-op and log.
    // eslint-disable-next-line no-console
    console.warn("[pcw] could not wrap Worker; kernel bridge inactive:", e);
  }
}

// Re-export for tests / advanced wiring.
export { installBridge, Bridge };
