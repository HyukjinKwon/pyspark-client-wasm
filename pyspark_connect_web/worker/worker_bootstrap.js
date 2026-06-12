// SPDX-License-Identifier: Apache-2.0
//
// worker_bootstrap.js - runs *inside* the Web Worker.
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
// undefined and we bail early - .

"use strict";

// ---- SAB sizing / control layout (must match sab_channel.py + bridge.js) --
const CONTROL_SLOTS = 8; // Int32 slots
const DATA_BYTES = 16 * 1024 * 1024; // 16 MiB payload region

// Pyodide is loaded SAME-ORIGIN from /pyodide/ (vendored next to the site).
// A cross-origin CDN does NOT work here: under cross-origin isolation the
// worker's importScripts() of a CDN pyodide.js is blocked by COEP (neither
// require-corp - jsdelivr sends no CORP - nor credentialless permits it in
// Chromium). Same-origin sidesteps COEP entirely (and is faster). v314.0.0
// ships pyarrow 22.0.0 + zstandard 0.25.0 + pandas 3.0.2 + numpy 2.4.3 (Py3.14).
// Override with self.PCW_PYODIDE_INDEX_URL if you host it elsewhere same-origin.
const PYODIDE_INDEX_URL =
  self.PCW_PYODIDE_INDEX_URL || new URL("/pyodide/", self.location.origin).href;

// Packages Pyodide ships / we install. grpcio + grpcio-status are intentionally
// absent (C-ext, not in Pyodide) - pyspark_connect_web's _grpc_shim stubs them.
// zstandard IS a Pyodide package and IS required by pyspark.sql.connect's
// check_dependencies, so it must be loaded.
const PURE_PYODIDE_PKGS = ["micropip", "pyarrow", "pandas", "numpy", "zstandard"];
const MICROPIP_PKGS = [
  "protobuf>=7",
  // pure-Python, required by pyspark.sql.connect (google.rpc.*); NOT in Pyodide.
  // These have pure wheels on PyPI, which micropip reaches from the browser.
  "googleapis-common-protos>=1.56.4",
  // The pyspark_connect_web wheel is served at the site root too (no deps).
  self.PCW_WHEEL_URL ||
    new URL(
      "/pyspark_connect_web-0.1.0-py3-none-any.whl",
      self.location.origin,
    ).href,
];

// The slim Spark Connect Python client: `pyspark-client` (NOT full `pyspark`) -
// pure-Python, no JVM / no py4j, exactly the thin client this project drives. We
// build a wheel in CI and host it same-origin so the worker never reaches across
// origins for it. Version 4.1.2 matches the Spark Connect server image (a
// mismatched client reads configs the server lacks -> SQL_CONF_NOT_FOUND).
// Overridable via self.PCW_PYSPARK_WHEEL_URL.
const PYSPARK_CLIENT_WHEEL =
  self.PCW_PYSPARK_WHEEL_URL ||
  new URL("/pyspark_client-4.1.2-py3-none-any.whl", self.location.origin).href;

function assertIsolated() {
  if (typeof SharedArrayBuffer === "undefined" || self.crossOriginIsolated !== true) {
    throw new Error(
      "Not cross-origin isolated: SharedArrayBuffer is unavailable. Serve the " +
        "page with COOP: same-origin and COEP: credentialless."
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

  // MODULE worker: recent Pyodide (v314.x) refuses classic workers ("Classic
  // web workers are not supported"), so load the ESM build via dynamic import
  // (not importScripts, which does not exist in a module worker anyway).
  const { loadPyodide } = await import(PYODIDE_INDEX_URL + "pyodide.mjs");
  pyodide = await loadPyodide({ indexURL: PYODIDE_INDEX_URL });

  await pyodide.loadPackage(PURE_PYODIDE_PKGS);
  const micropip = pyodide.pyimport("micropip");
  for (const pkg of MICROPIP_PKGS) {
    await micropip.install(pkg);
  }
  // deps=False: pyspark-client lists grpcio/grpcio-status as BASE requirements
  // (unlike full pyspark, where they are extras), and neither has a Pyodide
  // wheel. The _grpc_shim stubs them at runtime; everything else pyspark-client
  // needs (pyarrow/pandas/numpy via loadPackage, protobuf + googleapis-common-
  // protos above) is already present. callKwargs is how a JS caller passes a
  // Python keyword arg through the PyProxy.
  await micropip.install.callKwargs(PYSPARK_CLIENT_WHEEL, { deps: false });

  // Make the SABs reachable from Python via the `js` module. _AtomicsBackend
  // looks for `js.__pcw_register_sab` / reads these globals.
  self.__pcw_control_sab = controlSab;
  self.__pcw_data_sab = dataSab;
  self.__pcw_register_sab = function (c, d) {
    // Called by _AtomicsBackend whenever it (re)binds the data SAB - including a
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
    // Run user Python and return its CAPTURED STDOUT (the run_python_bridge /
    // e2e contract: snippets `print(json.dumps(...))` and the caller JSON-parses
    // stdout). Returning runPythonAsync's value instead would yield the last
    // expression (often None) - the smoke test then sees "undefined".
    let out = "";
    try {
      pyodide.setStdout({ batched: (s) => { out += s; } });
      await pyodide.runPythonAsync(msg.code);
      self.postMessage({ type: "pcw_result", id: msg.id, result: out });
    } catch (e) {
      self.postMessage({ type: "pcw_run_error", id: msg.id, message: String(e) });
    } finally {
      pyodide.setStdout({}); // restore default stdout
    }
  }
});

// Note on the Atomics handshake: once Python (SabSyncChannel._AtomicsBackend)
// writes a request and stores STATE = S_REQ_READY, it calls self.postMessage(
// {type:"pcw_rpc"}). That nudge is delivered to the *main* thread's bridge.js,
// which performs the fetch and writes the response back into dataSab, flipping
// STATE and Atomics.notify-ing. The worker thread, parked in Python on
// Atomics.wait(ctrl, C_STATE, S_REQ_READY), wakes and reads the bytes. The
// worker never yields its thread to the event loop during a blocking RPC - that
// is exactly what keeps PySpark's .collect() synchronous.
