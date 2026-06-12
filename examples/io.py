# SPDX-License-Identifier: Apache-2.0
"""I/O: createDataFrame from pandas + schema round-trip via Arrow.

Exercises lane 4's encode_local_relation (request side) and Arrow decode
(result side) - the same path the browser uses.

Run locally:  SPARK_REMOTE=sc://localhost:15002 python io.py
"""
import os

import pandas as pd

from pyspark.sql import SparkSession

REMOTE = os.environ.get("SPARK_REMOTE", "sc://localhost:15002")
spark = SparkSession.builder.remote(REMOTE).getOrCreate()

pdf = pd.DataFrame(
    {
        "id": [1, 2, 3],
        "name": ["alice", "bob", "carol"],
        "score": [9.5, 8.0, 7.25],
        "active": [True, False, True],
    }
)

sdf = spark.createDataFrame(pdf)
print("schema:", sdf.schema.simpleString())
sdf.show()

# round-trip back to pandas and confirm it matches what we sent
roundtrip = sdf.orderBy("id").toPandas()
print(roundtrip)
assert list(roundtrip["name"]) == ["alice", "bob", "carol"]
print("round-trip OK")

spark.stop()
