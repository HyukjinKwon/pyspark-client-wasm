# SPDX-License-Identifier: Apache-2.0
"""Lane 4: Arrow result decoding + request-side Arrow.

This package owns everything that turns Spark Connect ``arrow_batch`` chunks into
a pandas ``DataFrame`` (result side) and a pandas ``DataFrame`` into the Arrow IPC
stream bytes that ``createDataFrame`` / ``LocalRelation`` carries (request side).

It deliberately does NOT touch the gRPC stub (lane 1), the monkey-patch (lane 2),
or the sync bridge (lane 3). It only consumes the response protos those lanes
produce. It never imports ``grpcio``.

See ``team/findings-lane4-arrow.md`` for the reuse-vs-reimplement investigation
and the SPARK-53525 chunking notes.
"""
from __future__ import annotations

from .results import decode_arrow_batches, encode_local_relation, reassemble_record_batches

__all__ = [
    "decode_arrow_batches",
    "encode_local_relation",
    "reassemble_record_batches",
]
