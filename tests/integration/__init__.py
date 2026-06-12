# SPDX-License-Identifier: Apache-2.0
"""Integration tests: real Spark Connect server, real grpc-web framing round-trip.

Test-only code. ``grpcio`` IS allowed in this package (the DECISIONS.md #1 ban is
scoped to ``pyspark_connect_web/``, not the tests). See ``conftest.py`` for the
local server fixture and ``bridge.py`` for the pure-Python grpc-web <-> gRPC
bridge that stands in for Envoy.
"""
