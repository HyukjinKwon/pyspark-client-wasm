# SPDX-License-Identifier: Apache-2.0
"""Window functions: row_number, rank, running sum over a partition.

Run locally:  SPARK_REMOTE=sc://localhost:15002 python window.py
"""
import os

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

REMOTE = os.environ.get("SPARK_REMOTE", "sc://localhost:15002")
spark = SparkSession.builder.remote(REMOTE).getOrCreate()

df = spark.createDataFrame(
    [
        ("a", 10), ("a", 30), ("a", 20),
        ("b", 5), ("b", 15),
    ],
    ["grp", "value"],
)

w = Window.partitionBy("grp").orderBy(F.col("value").desc())
running = Window.partitionBy("grp").orderBy("value").rowsBetween(
    Window.unboundedPreceding, Window.currentRow
)

result = df.select(
    "grp",
    "value",
    F.row_number().over(w).alias("rn"),
    F.rank().over(w).alias("rank"),
    F.sum("value").over(running).alias("running_sum"),
).orderBy("grp", F.col("value").desc())

result.show()
print(result.toPandas())

spark.stop()
