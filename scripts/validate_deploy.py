#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
"""Static validation of the deploy/ configs - no Docker, no network.

Checks that every Envoy/compose YAML parses, and that the mandatory
cross-origin-isolation headers (DECISIONS.md #4) are present in BOTH the dev and
prod Envoy configs. Also asserts the prod config did not regress into a wildcard
CORS origin. Used by CI (the headers/deploy guard) and `make validate-deploy`.
"""

from __future__ import annotations

import sys

import yaml

YAML_FILES = [
    "deploy/envoy.yaml",
    "deploy/envoy.prod.yaml",
    "deploy/compose.yaml",
    "deploy/compose.prod.yaml",
]

# Files that must carry COOP/COEP for SharedArrayBuffer (DECISIONS.md #4).
COI_FILES = ["deploy/envoy.yaml", "deploy/envoy.prod.yaml"]


def main() -> int:
    failed = False

    for f in YAML_FILES:
        try:
            with open(f, encoding="utf-8") as fh:
                yaml.safe_load(fh)
            print(f"OK   parse        {f}")
        except FileNotFoundError:
            print(f"FAIL missing      {f}")
            failed = True
        except yaml.YAMLError as exc:
            print(f"FAIL yaml-error   {f}: {exc}")
            failed = True

    for f in COI_FILES:
        try:
            text = open(f, encoding="utf-8").read()
        except FileNotFoundError:
            print(f"FAIL missing      {f}")
            failed = True
            continue
        for needle in (
            "Cross-Origin-Opener-Policy",
            "same-origin",
            "Cross-Origin-Embedder-Policy",
            "credentialless",
        ):
            if needle not in text:
                print(f"FAIL coi-header   {f}: missing {needle!r} (DECISIONS.md #4)")
                failed = True
        if all(
            n in text
            for n in (
                "Cross-Origin-Opener-Policy",
                "same-origin",
                "Cross-Origin-Embedder-Policy",
                "credentialless",
            )
        ):
            print(f"OK   coi-headers  {f}")

    # Prod must NOT use a wildcard CORS origin (regex ".*"). Tightened CORS is a
    # hard requirement for prod (lane 5 brief). The dev file may use ".*".
    prod = open("deploy/envoy.prod.yaml", encoding="utf-8").read()
    if 'regex: ".*"' in prod:
        print("FAIL prod-cors    deploy/envoy.prod.yaml uses wildcard CORS regex '.*'")
        failed = True
    else:
        print("OK   prod-cors    deploy/envoy.prod.yaml has no wildcard CORS origin")

    if failed:
        print("\nvalidate_deploy: FAILED")
        return 1
    print("\nvalidate_deploy: all checks passed")
    return 0


if __name__ == "__main__":
    sys.exit(main())
