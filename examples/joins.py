# SPDX-License-Identifier: Apache-2.0
"""Joins: inner and left joins across two DataFrames.

Run locally:  SPARK_REMOTE=sc://localhost:15002 python joins.py
"""
import os

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

REMOTE = os.environ.get("SPARK_REMOTE", "sc://localhost:15002")
spark = SparkSession.builder.remote(REMOTE).getOrCreate()

people = spark.createDataFrame(
    [(1, "alice"), (2, "bob"), (3, "carol")], ["id", "name"]
)
orders = spark.createDataFrame(
    [(1, 100), (1, 250), (2, 75)], ["person_id", "amount"]
)

inner = people.join(orders, people.id == orders.person_id, "inner").select(
    "name", "amount"
)
print("-- inner --")
inner.orderBy("name").show()

left = (
    people.join(orders, people.id == orders.person_id, "left")
    .groupBy("name")
    .agg(F.coalesce(F.sum("amount"), F.lit(0)).alias("total"))
    .orderBy("name")
)
print("-- left + rollup (carol has no orders) --")
left.show()

spark.stop()
