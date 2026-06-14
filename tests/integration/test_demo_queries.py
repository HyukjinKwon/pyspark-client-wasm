# SPDX-License-Identifier: Apache-2.0
"""The embedded BI demo's queries, run against a REAL Spark Connect server over
the grpc-web bridge (Envoy stand-in).

This is the cheap, no-browser regression guard for demo/index.html: it executes
the exact CTE prelude and example queries the demo ships, so a broken demo query
(for example the SEED_EXPRESSION_IS_UNFOLDABLE that ``rand(id)`` triggered) fails
here in the ci.yml `integration` job instead of only in the heavy browser e2e.

Keep _CTES and EXAMPLES in sync with demo/index.html.
"""
from __future__ import annotations

import pytest

from .bridge import GrpcWebBridgeChannel
from pyspark.sql.connect.session import SparkSession as ConnectSparkSession
import pyspark_connect_web as pcw


# Mirrors the CTE prelude in demo/index.html (the synthetic retail dataset).
_CTES = """
  products AS (
    SELECT id AS product_id,
           element_at(array('Widget','Gadget','Gizmo','Doohickey','Sprocket','Cog','Lever','Valve'), cast(pmod(id,8) as int)+1) || ' #' || cast(id as string) AS product_name,
           element_at(array('Electronics','Home','Toys','Sports','Office'), cast(pmod(id,5) as int)+1) AS category,
           round(5 + pmod(id*7919, 19500)/100.0, 2) AS price
    FROM range(50)),
  customers AS (
    SELECT id AS customer_id,
           'Customer ' || cast(id as string) AS name,
           element_at(array('US','UK','DE','FR','JP','IN','BR','CA'), cast(pmod(id,8) as int)+1) AS country,
           date_add(date'2023-01-01', cast(pmod(id*7,700) as int)) AS signup_date
    FROM range(200)),
  orders AS (
    SELECT id AS order_id,
           cast(pmod(id*31+7, 200) as bigint) AS customer_id,
           cast(pmod(id*17+3, 50) as bigint) AS product_id,
           cast(1 + pmod(id*13, 5) as int) AS quantity,
           date_add(date'2024-01-01', cast(pmod(id*3, 365) as int)) AS order_date
    FROM range(5000))
"""


def _wrap(sql: str) -> str:
    s = sql.strip().rstrip(";").strip()
    if s[:5].lower() == "with ":
        return "WITH " + _CTES + ",\n" + s[5:]
    return "WITH " + _CTES + "\n" + s


# The four example queries shipped in demo/index.html, plus the basics the e2e
# asserts. (label, sql, expected-columns).
EXAMPLES = [
    (
        "top_products",
        "SELECT p.product_name, p.category, round(sum(o.quantity*p.price),2) AS revenue, sum(o.quantity) AS units "
        "FROM orders o JOIN products p ON o.product_id = p.product_id "
        "GROUP BY p.product_name, p.category ORDER BY revenue DESC LIMIT 10",
        ["product_name", "category", "revenue", "units"],
    ),
    (
        "revenue_by_country",
        "SELECT c.country, count(*) AS orders, round(sum(o.quantity*p.price),2) AS revenue "
        "FROM orders o JOIN customers c ON o.customer_id = c.customer_id "
        "JOIN products p ON o.product_id = p.product_id GROUP BY c.country ORDER BY revenue DESC",
        ["country", "orders", "revenue"],
    ),
    (
        "monthly_revenue",
        "SELECT date_trunc('month', o.order_date) AS month, count(*) AS orders, "
        "round(sum(o.quantity*p.price),2) AS revenue "
        "FROM orders o JOIN products p ON o.product_id = p.product_id GROUP BY 1 ORDER BY 1",
        ["month", "orders", "revenue"],
    ),
    (
        "top_customers",
        "SELECT c.name, c.country, count(*) AS orders, round(sum(o.quantity*p.price),2) AS spend "
        "FROM orders o JOIN customers c ON o.customer_id = c.customer_id "
        "JOIN products p ON o.product_id = p.product_id GROUP BY c.name, c.country ORDER BY spend DESC LIMIT 10",
        ["name", "country", "orders", "spend"],
    ),
]


@pytest.fixture()
def web_spark(connect_server):
    pcw.install()
    host, port, token = connect_server
    target = f"{host}:{port}"
    auth = [("authorization", f"Bearer {token}")]
    bridges = []

    def factory(_endpoint):
        ch = GrpcWebBridgeChannel(target, extra_metadata=auth)
        bridges.append(ch)
        return ch

    pcw.set_channel_factory(factory)
    spark = ConnectSparkSession.builder.remote(
        f"sc://localhost:{port}/;transport=grpcweb"
    ).getOrCreate()
    try:
        yield spark
    finally:
        try:
            spark.stop()
        except Exception:
            pass
        for ch in bridges:
            ch.close()
        pcw.set_channel_factory(None)


def test_seed_row_counts(web_spark):
    """The CTE dataset has the expected sizes (and the CTEs analyze cleanly:
    a regression guard against SEED_EXPRESSION_IS_UNFOLDABLE and friends)."""
    for name, expected in [("products", 50), ("customers", 200), ("orders", 5000)]:
        pdf = web_spark.sql(_wrap(f"SELECT count(*) AS n FROM {name}")).toPandas()
        assert int(pdf["n"].iloc[0]) == expected, name


def test_select_star_limit(web_spark):
    pdf = web_spark.sql(_wrap("SELECT * FROM orders LIMIT 100")).toPandas()
    assert len(pdf) == 100
    assert list(pdf.columns) == [
        "order_id",
        "customer_id",
        "product_id",
        "quantity",
        "order_date",
    ]


@pytest.mark.parametrize("label,sql,cols", EXAMPLES, ids=[e[0] for e in EXAMPLES])
def test_example_queries(web_spark, label, sql, cols):
    pdf = web_spark.sql(_wrap(sql)).toPandas()
    assert list(pdf.columns) == cols
    assert len(pdf) > 0
