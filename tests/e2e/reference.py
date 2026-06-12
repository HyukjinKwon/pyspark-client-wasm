# SPDX-License-Identifier: Apache-2.0
"""Reference-result generator for the e2e harness (the components).

Runs the SAME queries the browser e2e runs, but against a **native** PySpark
Connect client (plain gRPC, not grpc-web), and writes the results to a JSON file
the browser run compares against. This is the ground truth for 
("Arrow correctness over speed - byte/row-exact vs a reference run of the same
query on plain PySpark Connect").

IMPORTANT - grpcio scoping:
    This file uses PySpark's normal Connect client, which imports ``grpcio``.
    That is allowed *here* because this file lives under ``tests/`` and is NOT
    part of the ``pyspark_connect_web/`` package. The CI grpcio-guard checks
    only ``pyspark_connect_web/``. Never import grpcio inside the package.

Usage:
    python tests/e2e/reference.py \
        --remote sc://localhost:15002 \
        --out tests/e2e/reference.json

If a Spark Connect server is not reachable, this exits non-zero with a clear
message; the e2e spec treats a missing reference.json as a skip (unless
E2E_REQUIRE_STACK=1).

Keep the queries here in lockstep with v0-checklist.spec.ts.
"""

from __future__ import annotations

import argparse
import json
import sys
from typing import Any, Dict


def build_reference(remote: str) -> Dict[str, Any]:
    """Run the v0 queries against a native Spark Connect client and collect
    JSON-serializable reference results."""
    # Imported lazily so `--help` and import of this module don't require pyspark.
    from pyspark.sql import SparkSession
    from pyspark.sql import functions as F

    spark = SparkSession.builder.remote(remote).getOrCreate()
    try:
        ref: Dict[str, Any] = {}

        # 2. range(10).collect() -> row count
        ref["range_collect_count"] = len(spark.range(10).collect())

        # 3. filter/select/groupBy/agg -> toPandas as records.
        #    Mirror this EXACTLY in v0-checklist.spec.ts.
        df = (
            spark.range(100)
            .filter("id % 2 = 0")
            .select((F.col("id") % 10).alias("bucket"), F.col("id"))
            .groupBy("bucket")
            .agg(F.count("*").alias("n"), F.sum("id").alias("sum_id"))
            .orderBy("bucket")
        )
        ref["filter_groupby_agg"] = df.toPandas().to_dict(orient="records")

        # 4. createDataFrame round-trip (record the input + collected output)
        import pandas as pd

        pdf = pd.DataFrame({"a": [1, 2, 3], "b": ["x", "y", "z"]})
        out = spark.createDataFrame(pdf).toPandas()
        ref["create_dataframe_roundtrip"] = {
            "input": pdf.to_dict(orient="records"),
            "output": out.to_dict(orient="records"),
            "equal": bool(out.equals(pdf)),
        }

        # 5. spark.sql
        ref["sql_select_1"] = [r.asDict() for r in spark.sql("select 1 as x").collect()]

        return ref
    finally:
        spark.stop()


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument(
        "--remote",
        default="sc://localhost:15002",
        help="native Spark Connect endpoint (plain gRPC; NOT grpc-web)",
    )
    p.add_argument(
        "--out",
        default="tests/e2e/reference.json",
        help="path to write reference JSON",
    )
    args = p.parse_args(argv)

    try:
        ref = build_reference(args.remote)
    except Exception as exc:  # noqa: BLE001 - CLI: surface any failure clearly
        print(
            f"[reference.py] could not generate reference from {args.remote}: {exc}\n"
            f"  Is a Spark Connect server up? (docker compose -f deploy/compose.yaml up)",
            file=sys.stderr,
        )
        return 2

    with open(args.out, "w", encoding="utf-8") as fh:
        json.dump(ref, fh, indent=2, sort_keys=True)
        fh.write("\n")
    print(f"[reference.py] wrote reference results to {args.out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
