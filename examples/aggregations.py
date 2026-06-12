# SPDX-License-Identifier: Apache-2.0
"""Aggregations: groupBy + multiple aggregate functions.

Run locally:  SPARK_REMOTE=sc://localhost:15002 python aggregations.py
"""
import os

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

REMOTE = os.environ.get("SPARK_REMOTE", "sc://localhost:15002")
spark = SparkSession.builder.remote(REMOTE).getOrCreate()

df = spark.range(1000).withColumn("bucket", F.col("id") % 5)

agg = (
    df.groupBy("bucket")
    .agg(
        F.count("*").alias("n"),
        F.sum("id").alias("sum_id"),
        F.avg("id").alias("avg_id"),
        F.min("id").alias("min_id"),
        F.max("id").alias("max_id"),
    )
    .orderBy("bucket")
)

agg.show()
print(agg.toPandas())

spark.stop()
