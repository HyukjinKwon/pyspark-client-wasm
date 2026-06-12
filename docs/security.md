<!-- SPDX-License-Identifier: Apache-2.0 -->

# Security review - pyspark-connect-web

This is a defense-oriented threat model of the deployment topology:

```
browser tab (JupyterLite/Pyodide)
  |  grpc-web over fetch (HTTPS in prod)
  v
Envoy proxy  --grpc_web-->  Spark Connect server (no built-in auth)
  static host (COOP/COEP) -->  JupyterLite site
```

Trust boundaries: (a) the user's browser <-> Envoy; (b) Envoy <-> Spark Connect; (c)
the page <-> any cross-origin resource it loads (wheels, Pyodide); (d) notebook
output <-> the JupyterLite DOM. Each section below: **threat -> impact -> mitigation**.

---

## 1. SharedArrayBuffer / cross-origin isolation

**Why it matters.** The blocking `.collect()` bridge needs `SharedArrayBuffer` +
`Atomics.wait`, which the browser only exposes when the page is **cross-origin
isolated** (`Cross-Origin-Opener-Policy: same-origin` +
`Cross-Origin-Embedder-Policy: require-corp`). DECISIONS.md #4 makes this a hard
invariant.

**Threats.**

* *Spectre-class side channels.* COOP/COEP exist precisely because
  `SharedArrayBuffer` is a high-resolution timer primitive usable for
  speculative-execution side-channel attacks. Cross-origin isolation walls the
  page off from cross-origin documents/popups so a malicious embeddee cannot
  share memory or precise timers with attacker content.
* *Isolation silently lost.* If a reverse proxy/CDN strips COOP/COEP, the page
  loads but `crossOriginIsolated === false`; the bridge then hangs or throws.
  Worse, a partial config can leave the page in a non-isolated state that still
  exposes timing primitives without the intended walls.
* *COEP breaks embeds.* `require-corp` blocks any cross-origin subresource that
  does not opt in via `Cross-Origin-Resource-Policy` or CORS - e.g. CDN wheels.

**Mitigations.**

* Envoy sets COOP/COEP on the static host in **both** dev (`deploy/envoy.yaml`)
  and prod (`deploy/envoy.prod.yaml`); `scripts/validate_deploy.py` + the CI
  `headers-guard` fail the build if either drops them. The e2e suite asserts
  `crossOriginIsolated === true` as the **first** gate (DECISIONS.md #4), so a
  stripped header is caught loud, not silent.
* `worker_bootstrap.js` calls `assertIsolated()` and bails before allocating the
  SAB if isolation is off - fail-closed, never run on a non-isolated page.
* Vendor wheels behind the **same** isolated origin (`scripts/build_site.sh`
  copies the wheel into `_output`) so COEP does not block the import. If you must
  load from a CDN, that CDN must send `Cross-Origin-Resource-Policy: cross-origin`.
* Keep isolation **scoped to this app's origin**; do not relax COOP to allow
  popups from untrusted origins.

## 2. CORS

**Threat.** The grpc-web endpoint is reachable by `fetch` from a browser. With a
wildcard CORS policy (`allow_origin "*"`), **any** website a user visits can make
the user's browser issue grpc-web calls to your Connect server. Combined with a
server that has no auth, that is a confused-deputy / CSRF-style path straight to
Spark: arbitrary plan execution against your cluster's data and compute.

The dev config (`deploy/envoy.yaml`) intentionally uses wildcard CORS for laptop
convenience - `team/findings-lane5-deploy.md` flags this. It is **not** safe past
localhost.

**Mitigations (prod - `deploy/envoy.prod.yaml`).**

* **Tightened CORS:** an explicit origin allowlist (`exact:` match), **no** `.*`
  regex. `scripts/validate_deploy.py` + CI fail if the prod config regresses to a
  wildcard origin (`prod-cors` check).
* `allow_methods` narrowed to `POST, OPTIONS` (grpc-web needs no GET); `max_age`
  reduced from the dev 20 days to 1 day so a stale preflight cache is short-lived.
* `allow_credentials: true` is only meaningful with a concrete origin (the spec
  forbids credentials with `*`) - another reason wildcards are banned in prod.
* CORS is a browser-enforced control, **not** an authorization mechanism. It
  limits which *web origins* can call you; it does nothing against a non-browser
  client. Real access control is section 3.

## 3. Authentication to the Connect server

**Threat.** **Spark Connect has no built-in authentication.** Anyone who can
reach the gRPC port can run arbitrary Spark plans - read any table the driver can
read, write, execute UDFs, exfiltrate. In this topology the browser reaches Spark
*through Envoy*, so Envoy is the only enforcement point. If Envoy is open, Spark
is open.

**Mitigations.**

* **Never expose Spark Connect directly.** Prod keeps the Connect server private:
  `deploy/compose.prod.yaml` removes the `15002:15002` host port mapping that dev
  uses for `reference.py`. Only the proxy is public.
* **Auth at the proxy.** `deploy/envoy.prod.yaml` ships a minimal Lua gate that
  rejects any request lacking `Authorization: Bearer <token>` with `401` (CORS
  preflights pass through). This stops anonymous access. **It does not validate
  the token** - for production, replace it with `jwt_authn` (validate a JWT
  against your IdP's JWKS) or `ext_authz` (delegate to an authz service). Both
  sketched at the bottom of `envoy.prod.yaml`. The `authorization` header is
  forwarded to Spark, so a Spark-side interceptor can re-check if configured.
* **TLS everywhere public.** The prod listeners terminate TLS (`TLSv1_2`
  minimum); browsers also require a secure context for `crossOriginIsolated` off
  localhost. The token rides over TLS, never plaintext. Cert mounting is
  documented in `deploy/README.md`.
* **Network segmentation.** Put Spark on a private subnet/namespace; allow
  ingress only from the proxy. Bind Envoy admin to loopback (`127.0.0.1:9901` in
  the prod config) - never publish `:9901`.
* **Token handling in the browser.** The token lives in the page's JS context and
  is sent as a grpc-web header. Treat it as a bearer credential: short-lived,
  scoped, refreshable; do not log it; do not put it in URLs (it would land in
  proxy/access logs and `Referer`).

## 4. An untrusted / malicious Connect server

**Threat.** The client trusts whatever the server returns. A malicious or
compromised server (or a MITM if TLS is absent) can:

* Return **crafted Arrow IPC bytes** to attack the decoder (`pyarrow`) - malformed
  buffers, huge declared sizes (memory-exhaustion / DoS), or schema tricks.
* Return responses designed to drive the client into pathological reattach loops
  (DECISIONS.md #6), or stream unboundedly to exhaust the tab's memory.
* Send **error messages / column values** containing active content that later
  gets rendered (feeds into section 5, notebook-output XSS).

**Mitigations.**

* **Authenticate the server, not just the client:** always TLS in prod so the
  client knows it is talking to the real endpoint (prevents MITM-injected
  responses). Pin the host; for high-assurance, certificate-pin.
* **Decoder robustness:** lane 4 decodes via `pyarrow` (sandboxed inside WASM -
  a decoder crash is contained to the tab, not the host). Treat all server bytes
  as untrusted input; lane 4's reassembly validates `row_count`/chunk integrity
  (SPARK-53525 handling) and should reject inconsistent batches rather than trust
  declared sizes. The 32 MiB per-connection buffer limit in `envoy.prod.yaml`
  bounds a single upload; result streams are bounded by the tab's own memory -
  document that very large `.collect()` can OOM the tab (use `.limit()` /
  pagination for huge results).
* **WASM containment:** Pyodide runs in a WASM sandbox in a Web Worker; a
  malicious response cannot escape to the host filesystem or network beyond what
  the page's fetch already allows. This is a real defense-in-depth win of the
  browser model.
* **Treat server strings as data, not markup** - see section 5.

## 5. Notebook-output XSS

**Threat.** Query results and error messages flow from an untrusted server into
the JupyterLite DOM. JupyterLite/Jupyter renders several MIME types, and
`text/html` output is rendered as **HTML**. If a string column value or an error
message contains `<script>` / `<img onerror=...>` and is rendered as HTML, that
is stored/reflected XSS executing in the (privileged, token-bearing,
cross-origin-isolated) notebook origin - it could read the bearer token, issue
its own Spark calls, or tamper with the page.

**Mitigations.**

* **Render results as text/data, not HTML, by default.** A DataFrame's default
  repr is `text/plain`; pandas' `to_html` (and any custom HTML repr) must
  **escape** cell contents. Prefer `text/plain` reprs for server-derived data;
  if HTML is used, ensure escaping is on (pandas escapes by default in `to_html`
  - do not disable it with `escape=False` for untrusted data).
* **Sanitize.** JupyterLab/JupyterLite sanitize rendered HTML output via
  DOMPurify in the standard output renderers; keep that path (do not route
  server output through a custom unsanitized renderer). Never `display(HTML(...))`
  on raw server strings.
* **Error messages are attacker-influenced too.** `SparkConnectGrpcException`
  carries server-supplied `grpc-message` text; render it as plain text.
* **CSP as defense-in-depth.** Consider a `Content-Security-Policy` on the static
  host that forbids inline script execution in rendered output where feasible.
  (Note JupyterLite itself relies on some dynamic execution; test before
  tightening.) The prod static host already sets `X-Content-Type-Options:
  nosniff` to stop MIME-confusion.
* **Origin isolation limits blast radius:** because the page is cross-origin
  isolated, an XSS cannot trivially reach into other origins' documents - but it
  fully owns *this* origin, so the above content-handling rules are the primary
  defense.

---

## Residual risks / explicitly out of scope (v0)

* The shipped Lua auth gate is presence-only; **real token validation
  (jwt_authn/ext_authz) is a deployment responsibility**, not provided turnkey.
* No rate limiting / quota on Spark plan execution at the proxy - add Envoy local
  or global rate limiting if the endpoint is broadly reachable.
* No multi-tenant isolation between users sharing one Connect server - Spark
  Connect sessions are the boundary; treat the server as single-trust-domain.
* Supply chain: the wheel + Pyodide + CDN assets must be integrity-checked
  (subresource integrity / pinned hashes) for a hardened deployment; v0 pins
  versions but not hashes.

## Quick checklist for a public deployment

- [ ] HTTPS/TLS on both listeners (`envoy.prod.yaml`); real cert, not self-signed.
- [ ] CORS origin allowlist set to your real JupyterLite origin(s); no `.*`.
- [ ] Real auth in front of Spark (jwt_authn/ext_authz), not just the Lua gate.
- [ ] Spark Connect on a private network; only the proxy is public; admin on loopback.
- [ ] Wheels/Pyodide served from the isolated origin (or CORP-enabled CDN).
- [ ] Notebook output rendered as text/sanitized HTML; no raw `display(HTML(...))`.
- [ ] `scripts/validate_deploy.py` green (COOP/COEP present, no wildcard prod CORS).
