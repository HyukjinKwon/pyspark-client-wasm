<!-- SPDX-License-Identifier: Apache-2.0 -->

# Findings — DOCS (MkDocs documentation site)

Built a real documentation site matching the maintainer's sibling projects
(`spark-connect-scala3` uses MkDocs; both publish to
`hyukjinkwon.github.io/<repo>/`). Stack: **MkDocs + Material + mkdocstrings[python]**.

## What I created (owned files only)

- `mkdocs.yml` — Material theme, `mkdocstrings` python handler, `site_url`
  (`https://hyukjinkwon.github.io/pyspark-client-wasm/`), `repo_url`
  (`https://github.com/HyukjinKwon/pyspark-client-wasm`), full nav.
- `docs/index.md` — overview + the "thin client, not local compute" framing + a
  signpost table to every page.
- `docs/installation.md` — conda env setup (`conda create -n pcw python=3.11 &&
  conda activate pcw && pip install pyspark-connect-web`); pip for the package,
  conda for the env; dev extras; browser micropip install; version guard.
- `docs/quickstart.md` — conda env -> compose stack -> client; "what just happened".
- `docs/connection-patterns.md` — `sc://...;transport=grpcweb` scheme, http(s)
  shorthand normalization table, TLS rationale, bearer-token auth at Envoy,
  endpoint cheat-sheet.
- `docs/jupyterlite-hosting.md` — hosting matrix from
  `pyspark_connect_web/jupyterlite/README.md` (Envoy / Netlify / GitHub Pages
  needs coi-serviceworker.js / http.server), COEP caveat, kernel-bridge load
  order. Note distinguishing the MkDocs docs-site Pages deploy (no isolation)
  from the JupyterLite app Pages deploy (needs the SW shim).
- `docs/api-reference.md` — mkdocstrings auto-generated for the public surface:
  install/uninstall/is_installed/__version__ from the package, plus
  set_stub_factory/set_channel_factory/check_pyspark_version/
  UnsupportedPySparkError/SUPPORTED_PYSPARK_RANGE from `.patch`.
- `.github/workflows/docs.yml` — see below.

## Nav structure

- Home -> index.md
- Getting started -> Installation, Quickstart, Running locally
- Guides -> Connection patterns, JupyterLite hosting, Packaging & release
- Reference -> Architecture, Security, API reference

Existing standalone docs (architecture/running-locally/security/packaging-release)
folded into the nav, content reused as-is apart from disclaimer removal.

## GitHub Pages deploy flow (.github/workflows/docs.yml)

- Triggers: push to main + workflow_dispatch.
- permissions: contents:read, pages:write, id-token:write; concurrency group=pages.
- build job: checkout -> setup-python 3.11 -> pip-install pinned mkdocs==1.6.1,
  mkdocs-material==9.5.49, mkdocstrings[python]==0.27.0, pymdown-extensions==10.14
  -> pip install -e ".[dev]" (so mkdocstrings can import the package + resolve
  pyspark) -> mkdocs build --strict -> actions/upload-pages-artifact@v3 (path site).
- deploy job: actions/deploy-pages@v4 into the github-pages environment.
- All action + tool versions pinned.

## Where I used conda

docs/installation.md (primary env setup, plain + [dev] extras) and
docs/quickstart.md step 1. pip kept for the package; conda only manages the env,
with a callout explaining the split (venv noted as alternative).

## Disclaimer removal (confirmed)

Removed the verbatim trademark disclaimer blockquote ("Unofficial personal
project. Not affiliated with...") from every docs file that had it:
architecture.md, running-locally.md, security.md, packaging-release.md.
No standalone "Name note: the import / package name is pyspark_connect_web..."
paragraph existed in docs/ (it lived in README, owned by the README agent). Also
cleaned two dangling references it would have left in packaging-release.md:
removed the checklist item asserting the disclaimer is present, and trimmed the
"see the README note on the split" publish step. `grep -rn -i` over docs/ now
returns only the one-line Apache-2.0 trademark courtesy in index.md's license
footer (kept; normal attribution, not the disclaimer block).

## Validation (no network / no mkdocs build here)

- python yaml.safe_load(mkdocs.yml) -> OK
- YAML-parsed .github/workflows/docs.yml -> OK
- All 10 nav targets exist on disk
- mkdocstrings docstring_style: sphinx to match the :func:/:class: roles in patch.py
- Could NOT run `mkdocs build` (offline)

## For the integrator

1. Enable GitHub Pages: Settings -> Pages -> Source = GitHub Actions (NOT
   "Deploy from a branch"). The deploy-pages artifact flow requires it; until
   flipped, the deploy job fails.
2. First deploy on main (or dispatch) brings the site live at
   https://hyukjinkwon.github.io/pyspark-client-wasm/
3. mkdocstrings imports the package at build time; the workflow installs .[dev].
4. mkdocs build --strict is intentional (fails on broken links / missing nav);
   first CI run is the real validation since I'm offline.
