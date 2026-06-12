# SPDX-License-Identifier: Apache-2.0
"""Transformations: select, filter, withColumn, column expressions.

Run locally:  SPARK_REMOTE=sc://localhost:15002 python transformations.py
"""
import os

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

REMOTE = os.environ.get("SPARK_REMOTE", "sc://localhost:15002")
spark = SparkSession.builder.remote(REMOTE).getOrCreate()

df = spark.range(10).select(
    F.col("id"),
    (F.col("id") * 2).alias("doubled"),
    (F.col("id") % 2 == 0).alias("is_even"),
)

result = (
    df.filter(F.col("is_even"))
    .withColumn("label", F.concat(F.lit("n="), F.col("id").cast("string")))
    .orderBy(F.col("doubled").desc())
)

result.show()
print(result.toPandas())

spark.stop()
