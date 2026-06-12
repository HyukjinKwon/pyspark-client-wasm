# SPDX-License-Identifier: Apache-2.0
"""Quickstart: connect and run the simplest queries.

Run locally:  SPARK_REMOTE=sc://localhost:15002 python quickstart.py
In JupyterLite: see ../examples/README.md (prepend `pcw.install()`).
"""
import os

from pyspark.sql import SparkSession

REMOTE = os.environ.get("SPARK_REMOTE", "sc://localhost:15002")
spark = SparkSession.builder.remote(REMOTE).getOrCreate()

# A tiny distributed range, materialized to the client.
print("collect:", spark.range(10).collect())
print("count:", spark.range(1000).filter("id % 7 = 0").count())

# Arrow result -> pandas (the path lane 4 decodes in the browser).
print(spark.range(5).withColumnRenamed("id", "n").toPandas())

spark.stop()
