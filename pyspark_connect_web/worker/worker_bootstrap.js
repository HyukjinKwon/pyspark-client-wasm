// SPDX-License-Identifier: Apache-2.0
//
// worker_bootstrap.js — runs *inside* the Web Worker.
//
// Responsibilities:
//   1. Load Pyodide.
//   2. micropip-install pyarrow/pandas/protobuf/pyspark + the pyspark_connect_web
//      wheel.
//   3. Allocate the control + data SharedArrayBuffers and hand them to the main
//      thread (which runs bridge.js).
//   4. Expose those SABs to Python (via the `js` module) so SabSyncChannel's
//      _AtomicsBackend can drive the Atomics.wait handshake.
//
// This file is the worker entry: `new Worker("worker_bootstrap.js", {type:"module"})`.
// The page must be cross-origin isolated (COOP/COEP) or SharedArrayBuffer is
// undefined and we bail early — see DECISIONS.md #4.

"use strict";

// ---- SAB sizing / control layout (must match sab_channel.py + bridge.js) --
const CONTROL_SLOTS = 8; // Int32 slots
const DATA_BYTES = 16 * 1024 * 1024; // 16 MiB payload region

const PYODIDE_INDEX_URL =
  self.PCW_PYODIDE_INDEX_URL || "https://cdn.jsdelivr.net/pyodide/v0.28.0/full/";

// Packages Pyodide ships / we install. grpcio + grpcio-status are intentionally
// absent (C-ext, not in Pyodide) — pyspark_connect_web's _grpc_shim stubs them.
// zstandard IS a Pyodide package and IS required by pyspark.sql.connect's
// check_dependencies, so it must be loaded.
const PURE_PYODIDE_PKGS = ["micropip", "pyarrow", "pandas", "numpy", "zstandard"];
const MICROPIP_PKGS = [
  "protobuf>=7",
  // pure-Python, required by pyspark.sql.connect (google.rpc.*); NOT in Pyodide.
  "googleapis-common-protos>=1.56.4",
  "pyspark>=4.0,<4.2",
  // The wheel is served alongside the page; URL injected by the host config.
  self.PCW_WHEEL_URL || "pyspark_connect_web-0.0.1.dev0-py3-none-any.whl",
];

function assertIsolated() {
  if (typeof SharedArrayBuffer === "undefined" || self.crossOriginIsolated !== true) {
    throw new Error(
      "Not cross-origin isolated: SharedArrayBuffer is unavailable. Serve the " +
        "page with COOP: same-origin and COEP: require-corp (DECISIONS.md #4)."
    );
  }
}

let pyodide = null;
let controlSab = null;
let dataSab = null;

async function boot() {
  assertIsolated();

  // Allocate the shared buffers up front and announce them to the main thread,
  // so bridge.js can attach views before the first RPC.
  controlSab = new SharedArrayBuffer(CONTROL_SLOTS * 4);
  dataSab = new SharedArrayBuffer(DATA_BYTES);
  self.postMessage({ type: "pcw_sab", control: controlSab, data: dataSab });

  importScripts(PYODIDE_INDEX_URL + "pyodide.js");
  // eslint-disable-next-line no-undef
  pyodide = await loadPyodide({ indexURL: PYODIDE_INDEX_URL });

  await pyodide.loadPackage(PURE_PYODIDE_PKGS);
  const micropip = pyodide.pyimport("micropip");
  for (const pkg of MICROPIP_PKGS) {
    await micropip.install(pkg);
  }

  // Make the SABs reachable from Python via the `js` module. _AtomicsBackend
  // looks for `js.__pcw_register_sab` / reads these globals.
  self.__pcw_control_sab = controlSab;
  self.__pcw_data_sab = dataSab;
  self.__pcw_register_sab = function (c, d) {
    // Called by _AtomicsBackend whenever it (re)binds the data SAB — including a
    // realloc to a larger buffer for a big result. Re-announce to the main
    // thread so bridge.js attaches the *new* buffer before the next RPC. The
    // worker is between RPCs (STATE == IDLE) when this fires, so it is safe.
    self.__pcw_control_sab = c;
    self.__pcw_data_sab = d;
    self.postMessage({ type: "pcw_sab", control: c, data: d });
  };

  // Bridge Python's print/stderr if desired; minimal here.
  self.postMessage({ type: "pcw_ready" });
}

self.addEventListener("message", async (ev) => {
  const msg = ev.data || {};
  if (msg.type === "pcw_boot") {
    try {
      await boot();
    } catch (e) {
      self.postMessage({ type: "pcw_error", message: String(e && e.message ? e.message : e) });
    }
  } else if (msg.type === "pcw_run") {
    // Run user Python (e.g. the notebook kernel dispatches here). The actual
    // JupyterLite kernel uses its own message protocol; this hook is for the
    // standalone demo harness + lane 5's window.__pcwRunPython bridge.
    try {
      const result = await pyodide.runPythonAsync(msg.code);
      self.postMessage({ type: "pcw_result", id: msg.id, result: String(result) });
    } catch (e) {
      self.postMessage({ type: "pcw_run_error", id: msg.id, message: String(e) });
    }
  }
});

// Note on the Atomics handshake: once Python (SabSyncChannel._AtomicsBackend)
// writes a request and stores STATE = S_REQ_READY, it calls self.postMessage(
// {type:"pcw_rpc"}). That nudge is delivered to the *main* thread's bridge.js,
// which performs the fetch and writes the response back into dataSab, flipping
// STATE and Atomics.notify-ing. The worker thread, parked in Python on
// Atomics.wait(ctrl, C_STATE, S_REQ_READY), wakes and reads the bytes. The
// worker never yields its thread to the event loop during a blocking RPC — that
// is exactly what keeps PySpark's .collect() synchronous (DECISIONS.md #5).
