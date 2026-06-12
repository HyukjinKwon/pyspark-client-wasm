// SPDX-License-Identifier: Apache-2.0
//
// coi-serviceworker.js - make a static, header-less host (e.g. GitHub Pages)
// cross-origin isolated so SharedArrayBuffer/Atomics work.
//
// Hosts like GitHub Pages cannot set response headers, so `crossOriginIsolated`
// is false and our Atomics blocking bridge cannot run. This service worker
// intercepts the page's own responses and *re-emits them with* COOP/COEP set,
// which flips `crossOriginIsolated` to true after a one-time reload.
//
// This is a clean-room, minimal reimplementation of the well-known
// "coi-serviceworker" technique (Guido Zuidhof). It must be:
//   * served from YOUR OWN origin (not a CDN),
//   * in its own file (this one), referenced as a <script>, and
//   * on HTTPS or localhost.
//
// COEP `require-corp` means every CROSS-ORIGIN subresource (CDN Pyodide, wheels,
// fonts) must send CORP/CORS headers or it is blocked. Prefer hosting Pyodide +
// the wheel same-origin, or ensure those resources are CORS-enabled. See
// jupyterlite/README.md "Hosting without header control".

/* eslint-disable no-restricted-globals */
"use strict";

if (typeof self !== "undefined" && "serviceWorker" in (self.navigator || {})) {
  // ---- Page context: register the SW, then reload once so it controls us. ----
  (function registerCoi() {
    const nav = self.navigator;
    // Already isolated? Nothing to do.
    if (self.crossOriginIsolated) return;
    // Don't loop forever if registration silently fails.
    if (self.sessionStorage && self.sessionStorage.getItem("pcwCoiReloaded")) {
      return;
    }
    const url = self.document && self.document.currentScript
      ? self.document.currentScript.src
      : "coi-serviceworker.js";
    nav.serviceWorker
      .register(url, { scope: "./" })
      .then((reg) => {
        reg.addEventListener("updatefound", () => {});
        // If the SW is now active and controlling, reload once to take effect.
        if (nav.serviceWorker.controller) {
          // Already controlled but not isolated -> headers not applied yet.
          return;
        }
        if (self.sessionStorage) {
          self.sessionStorage.setItem("pcwCoiReloaded", "1");
        }
        // Wait for control then reload.
        nav.serviceWorker.addEventListener("controllerchange", () => {
          self.location.reload();
        });
      })
      .catch((e) => {
        // eslint-disable-next-line no-console
        console.error("[pcw] coi-serviceworker registration failed:", e);
      });
  })();
} else if (typeof self !== "undefined" && self.registration) {
  // ---- Service-worker context: claim clients and inject the headers. ----
  self.addEventListener("install", () => self.skipWaiting());
  self.addEventListener("activate", (e) => e.waitUntil(self.clients.claim()));

  self.addEventListener("fetch", (event) => {
    const req = event.request;
    // Only same-origin navigations / subresources we serve get rewritten;
    // cross-origin requests pass through (they must already be CORP/CORS-ok).
    if (req.cache === "only-if-cached" && req.mode !== "same-origin") return;
    event.respondWith(
      fetch(req)
        .then((res) => {
          if (res.status === 0) return res; // opaque; leave as-is
          const headers = new Headers(res.headers);
          headers.set("Cross-Origin-Opener-Policy", "same-origin");
          headers.set("Cross-Origin-Embedder-Policy", "require-corp");
          return new Response(res.body, {
            status: res.status,
            statusText: res.statusText,
            headers,
          });
        })
        .catch((e) => {
          // eslint-disable-next-line no-console
          console.error("[pcw] coi-serviceworker fetch error:", e);
          throw e;
        })
    );
  });
}
