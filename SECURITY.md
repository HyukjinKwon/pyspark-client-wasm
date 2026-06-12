<!-- SPDX-License-Identifier: Apache-2.0 -->

# Security Policy

## Supported Versions

This project is in early development (pre-1.0). Security fixes are applied to the
latest released version on the `main` branch only.

| Version | Supported |
|---------|-----------|
| latest `0.x` | yes |
| older `0.x`  | no |

## Reporting a Vulnerability

Please **do not** open a public issue for security vulnerabilities.

Instead, report privately via GitHub's
[private vulnerability reporting](https://docs.github.com/en/code-security/security-advisories/guidance-on-reporting-and-writing-information-about-vulnerabilities/privately-reporting-a-security-vulnerability)
("Report a vulnerability" under the repository's **Security** tab), or contact
the maintainer directly.

When reporting, please include:

- a description of the issue and its impact,
- steps to reproduce (a minimal proof of concept if possible),
- affected version(s) and environment (browser/Pyodide, proxy, server).

We aim to acknowledge reports within a few days and to coordinate a fix and
disclosure timeline with you.

## Threat Model

Because this client runs in the browser and connects to a Spark Connect server
through a grpc-web proxy, it has a security surface worth understanding before
deploying it publicly: cross-origin isolation / `SharedArrayBuffer`, CORS, proxy
authentication to a server that has none of its own, untrusted-server responses,
and notebook-output handling.

A detailed threat model with mitigations and a public-deployment checklist lives
in [`docs/security.md`](docs/security.md). Read it before exposing a deployment
to untrusted users or networks.
