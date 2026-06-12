<!-- SPDX-License-Identifier: Apache-2.0 -->

# pyspark-connect-web — PySpark in JupyterLite

Run the **real** PySpark Connect Python client inside a browser
(JupyterLite/Pyodide), talking to a Spark Connect server through a grpc-web
transport. Your existing PySpark code runs unchanged — no reimplementation, no
local JVM, no Python backend server.

```python
import pyspark_connect_web as pcw
pcw.install()

from pyspark.sql import SparkSession
spark = SparkSession.builder.remote("sc://localhost:8081/;transport=grpcweb").getOrCreate()
spark.range(10).filter("id % 2 = 0").toPandas()   # runs in your browser tab
```

## How it works
PySpark's Connect client is pure Python above a single gRPC stub: it builds
protobuf plans and ships them to the server. We **monkey-patch only that stub**
with a grpc-web/`fetch` transport, and make calls blocking via a Web Worker +
`Atomics`/`SharedArrayBuffer` bridge so `.collect()` returns data synchronously.
Everything above the stub — DataFrame, Column, functions — is untouched.

**This is a thin client, not local compute.** You still need a running Spark
Connect server (Spark 4.x) behind an Envoy grpc-web proxy. The win is: no Python
*backend*, real PySpark API, anywhere a browser runs.

## Status
Early development. See `COORDINATION.md` for the build plan and `DECISIONS.md`
for invariants. Built by a team of cooperating agents — lane map in `COORDINATION.md`.

> _Lane 5 owns this file — expand with install/quickstart/proxy setup as v0 lands._
