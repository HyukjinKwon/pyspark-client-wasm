<!-- SPDX-License-Identifier: Apache-2.0 -->

# Examples

These are ordinary **PySpark Connect** programs. That's the whole point of
`pyspark-connect-web`: the DataFrame/SQL code you write is unchanged - only
*where it runs* and *how it reaches the server* differ.

Two ways to run each example:

### A. In the browser (JupyterLite) - the project's reason to exist

Paste the body of any example into a JupyterLite notebook cell, after:

```python
import pyspark_connect_web as pcw
pcw.install()
from pyspark.sql import SparkSession
spark = SparkSession.builder.remote("sc://localhost:8081/;transport=grpcweb").getOrCreate()
```

`pcw.install()` routes Spark Connect over grpc-web; the rest is identical PySpark.

### B. Locally with native PySpark - to run these `.py` files as-is

Set up an environment with **conda** and point at a running Spark Connect server
(see [`../deploy/`](../deploy/) or [`../docs/running-locally.md`](../docs/running-locally.md)):

```bash
conda create -n pcw python=3.11 && conda activate pcw
pip install "pyspark[connect]>=4.0" pyarrow pandas

# bring up a server (or use your own):  docker compose -f ../deploy/compose.yaml up -d
export SPARK_REMOTE="sc://localhost:15002"      # native gRPC endpoint
python quickstart.py
```

Each script reads the connection string from `$SPARK_REMOTE`
(default `sc://localhost:15002`), so the same file works against any Connect server.

## The tour

| File | Shows |
|------|-------|
| `quickstart.py`      | connect, `range`, `collect`, `toPandas` |
| `transformations.py` | `select`, `filter`, `withColumn`, column expressions |
| `aggregations.py`    | `groupBy`, `agg`, multiple aggregate functions |
| `joins.py`           | inner / left joins across DataFrames |
| `window.py`          | window functions (`row_number`, `rank`, running sums) |
| `sql.py`             | `spark.sql`, temp views |
| `io.py`              | `createDataFrame` from pandas, schema round-trip |
