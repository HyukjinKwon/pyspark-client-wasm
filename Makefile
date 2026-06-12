# SPDX-License-Identifier: Apache-2.0
#
# Makefile for pyspark-connect-web (the owns packaging/release entrypoints).
# Thin wrappers over the scripts; `make help` lists targets.

SHELL := /usr/bin/env bash
PCW_OUTPUT_DIR ?= _output

.DEFAULT_GOAL := help

.PHONY: help
help: ## Show this help
	@grep -hE '^[a-zA-Z0-9_.-]+:.*?## ' $(MAKEFILE_LIST) \
	  | awk 'BEGIN{FS=":.*?## "}{printf "  \033[36m%-18s\033[0m %s\n", $$1, $$2}'

# --- dev / test ------------------------------------------------------------
.PHONY: install
install: ## Install the package + dev deps into the active venv
	python -m pip install --upgrade pip
	pip install -e ".[dev]"

.PHONY: test
test: ## Run unit tests (no browser, no grpcio)
	pytest -q --ignore=tests/e2e

.PHONY: lint
lint: ## Lint + format-check (ruff). Install with: pip install ruff==0.6.9
	ruff check .
	ruff format --check .

.PHONY: fmt
fmt: ## Auto-format with ruff
	ruff format .

# --- packaging / release ---------------------------------------------------
.PHONY: wheel
wheel: ## Build the wheel into dist/
	python -m build --wheel --outdir dist

.PHONY: sdist
sdist: ## Build the source distribution into dist/
	python -m build --sdist --outdir dist

.PHONY: dist
dist: wheel sdist ## Build wheel + sdist

.PHONY: site
site: ## Build the wheel + JupyterLite site into $(PCW_OUTPUT_DIR)
	PCW_OUTPUT_DIR=$(PCW_OUTPUT_DIR) scripts/build_site.sh

# --- deploy ----------------------------------------------------------------
.PHONY: up
up: ## Bring up the DEV stack (Spark Connect + Envoy + static), foreground
	docker compose -f deploy/compose.yaml up

.PHONY: up-prod
up-prod: ## Bring up the PROD-hardened stack (TLS/auth/tight CORS), detached
	docker compose -f deploy/compose.yaml -f deploy/compose.prod.yaml up -d

.PHONY: down
down: ## Tear down the stack
	docker compose -f deploy/compose.yaml -f deploy/compose.prod.yaml down

# --- e2e -------------------------------------------------------------------
.PHONY: reference
reference: ## Generate native reference results (needs Spark Connect on :15002 + grpcio)
	python tests/e2e/reference.py --remote sc://localhost:15002 --out tests/e2e/reference.json

.PHONY: e2e
e2e: ## Run the Playwright e2e harness (skips if stack down)
	cd tests/e2e && npm install && npx playwright install chromium && npx playwright test

.PHONY: validate-deploy
validate-deploy: ## Static-validate deploy configs (YAML parse + COOP/COEP guard)
	python scripts/validate_deploy.py
