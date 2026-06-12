# SPDX-License-Identifier: Apache-2.0
"""SQL: spark.sql over a temp view.

Run locally:  SPARK_REMOTE=sc://localhost:15002 python sql.py
"""
import os

from pyspark.sql import SparkSession

REMOTE = os.environ.get("SPARK_REMOTE", "sc://localhost:15002")
spark = SparkSession.builder.remote(REMOTE).getOrCreate()

print("scalar:", spark.sql("select 1 as x, 'hi' as y").collect())

spark.range(100).createOrReplaceTempView("nums")
top = spark.sql(
    """
    select id % 3 as bucket, count(*) as n, sum(id) as total
    from nums
    group by id % 3
    order by bucket
    """
)
top.show()
print(top.toPandas())

spark.stop()
