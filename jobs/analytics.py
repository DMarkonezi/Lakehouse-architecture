"""
Sloj ANALYTICS - zvezdasta sema.
  dim_customer (SCD2 po loyalty_tier), dim_product (SCD2 po ceni), dim_date
  fact_order_items  - jedan red po stavci porudzbine, months(order_ts), copy-on-write, inkrementalno MERGE
  daily_sales, conversion_funnel, customer_360, payment_reconciliation - CREATE OR REPLACE (atomicna zamena)
"""
from __future__ import annotations

from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F

from common import COPY_ON_WRITE, create_table, exists, log, upsert

FACT = "lake.analytics.fact_order_items"
OPEN_END = "TIMESTAMP '9999-12-31 00:00:00'"


# ------------------------------------------------------------------ SCD tipa 2
def scd2(spark: SparkSession, table: str, src, key: str, sk: str, tracked: list[str]) -> None:
    """
    Promena pracene kolone (tracked) -> stara verzija se zatvara (valid_to, is_current=false), dodaje se nova.
    Promena ostalih kolona -> azurira se tekuca verzija (SCD1). Sve u jednom MERGE-u = jedan snapshot.
    """
    biz = [c for c in src.columns if c not in ("created_at", "updated_at")]
    if not exists(spark, table):
        dim = src.select(F.xxhash64(key, "created_at").alias(sk), *biz,
                         F.col("created_at").alias("valid_from"), F.expr(OPEN_END).alias("valid_to"),
                         F.lit(True).alias("is_current"))
        create_table(spark, dim, table)
        log(f"{table}: kreirana, {dim.count()} verzija")
        return

    cur = spark.table(table).where("is_current")
    differs = " OR ".join(f"NOT (s.{c} <=> d.{c})" for c in biz if c != key)
    tracked_differs = " OR ".join(f"NOT (s.{c} <=> d.{c})" for c in tracked)
    src.createOrReplaceTempView("__src")
    cur.createOrReplaceTempView("__cur")
    changed = spark.sql(f"""SELECT s.*, d.{key} IS NOT NULL AS known,
                                   d.{key} IS NOT NULL AND ({tracked_differs}) AS new_version
                            FROM __src s LEFT JOIN __cur d ON s.{key} = d.{key}
                            WHERE d.{key} IS NULL OR {differs}""").localCheckpoint()
    if changed.isEmpty():
        log(f"{table}: nema promena")
        return
    changed.createOrReplaceTempView("__changed")
    spark.sql(f"""
        CREATE OR REPLACE TEMP VIEW __staged AS
        SELECT {key} AS merge_key, * FROM __changed                  -- zatvara staru ili azurira tekucu verziju
        UNION ALL
        SELECT NULL AS merge_key, * FROM __changed WHERE new_version -- otvara novu verziju""")
    type1 = ", ".join(f"d.{c} = s.{c}" for c in biz if c != key and c not in tracked)
    tracked_cond = " OR ".join(f"NOT (d.{c} <=> s.{c})" for c in tracked)
    cols = [sk, *biz, "valid_from", "valid_to", "is_current"]
    vf = "CASE WHEN s.merge_key IS NULL THEN s.updated_at ELSE s.created_at END"
    spark.sql(f"""
        MERGE INTO {table} d USING __staged s
        ON d.{key} = s.merge_key AND d.is_current
        WHEN MATCHED AND ({tracked_cond}) THEN UPDATE SET d.is_current = false, d.valid_to = s.updated_at
        WHEN MATCHED THEN UPDATE SET {type1}
        WHEN NOT MATCHED THEN INSERT ({', '.join(cols)})
          VALUES (xxhash64(s.{key}, {vf}), {', '.join('s.' + c for c in biz)}, {vf}, {OPEN_END}, true)""")
    stats = changed.groupBy("known", "new_version").count().collect()
    s = {(r["known"], r["new_version"]): r["count"] for r in stats}
    log(f"{table}: SCD2 -> {s.get((True, True), 0)} novih verzija, {s.get((True, False), 0)} SCD1 izmena, "
        f"{s.get((False, False), 0) + s.get((False, True), 0)} novih")


def build_dims(spark: SparkSession) -> None:
    customers = spark.table("lake.raw.customers").select(
        "customer_id", "country", "city", "customer_type", "loyalty_tier", "created_at", "updated_at")
    scd2(spark, "lake.analytics.dim_customer", customers, "customer_id", "customer_key", ["loyalty_tier"])

    products = spark.table("lake.raw.products").select(
        "product_id", "sku", "name", "category", "brand", "list_price_eur", "created_at", "updated_at")
    scd2(spark, "lake.analytics.dim_product", products, "product_id", "product_key", ["list_price_eur"])

    if not exists(spark, "lake.analytics.dim_date"):
        spark.sql("""
            CREATE TABLE lake.analytics.dim_date USING iceberg AS
            SELECT d AS date_key, year(d) AS year, quarter(d) AS quarter, month(d) AS month,
                   date_format(d, 'yyyy-MM') AS year_month, weekofyear(d) AS week, dayofmonth(d) AS day,
                   date_format(d, 'EEEE') AS weekday_name, dayofweek(d) IN (1, 7) AS is_weekend
            FROM (SELECT explode(sequence(DATE '2024-01-01', DATE '2027-12-31')) AS d)""")
        log("lake.analytics.dim_date: kreirana")


# ------------------------------------------------------------------ fact
def fact_source(spark: SparkSession, fx_bug: bool = False):
    items = spark.table("lake.clean.order_items")
    fx = (spark.table("lake.raw.fx_rates")
          .withColumn("_rn", F.row_number().over(Window.partitionBy("rate_date", "currency").orderBy(F.col("_ingested_at").desc())))
          .where("_rn = 1"))
    fx.createOrReplaceTempView("__fx")
    rate = "fx.rate_to_eur * fx.rate_to_eur" if fx_bug else "fx.rate_to_eur"   # bad_deploy: kurs primenjen dva puta
    extra = ", i.discount_pct" if "discount_pct" in items.columns else ""
    return spark.sql(f"""
        SELECT i.order_item_id, i.order_id, dc.customer_key, dp.product_key,
               CAST(o.order_ts AS DATE) AS date_key, o.order_ts, o.sales_channel, o.country,
               i.quantity, i.unit_price, i.discount, i.net_amount, o.currency,
               CAST(i.net_amount * {rate} AS DECIMAL(12,2)) AS net_amount_eur,
               o.status AS order_status {extra}
        FROM lake.clean.order_items i
        JOIN lake.clean.orders o ON i.order_id = o.order_id
        LEFT JOIN __fx fx ON fx.rate_date = to_date(o.order_ts) AND fx.currency = o.currency
        LEFT JOIN lake.analytics.dim_customer dc
               ON dc.customer_id = o.customer_id AND o.order_ts >= dc.valid_from AND o.order_ts < dc.valid_to
        LEFT JOIN lake.analytics.dim_product dp
               ON dp.product_id = i.product_id AND o.order_ts >= dp.valid_from AND o.order_ts < dp.valid_to""")


def build_fact(spark: SparkSession, fx_bug: bool = False) -> None:
    df = fact_source(spark, fx_bug)
    if fx_bug:
        log("!!! BAD DEPLOY: transformacija primenjuje kurs dva puta")
    upsert(spark, df, FACT, ["order_item_id"], partition_by=[F.months("order_ts")], props=COPY_ON_WRITE,
           write_order="order_ts", evolve=False, renames={"discount": "discount_amount"})


# ------------------------------------------------------------------ izvedene tabele
MARTS = {
    "daily_sales": """
        PARTITIONED BY (months(sales_date)) AS
        SELECT to_date(f.order_ts) AS sales_date, f.country, p.category, f.sales_channel,
               COUNT(DISTINCT f.order_id) AS orders, SUM(f.quantity) AS items,
               CAST(SUM(f.net_amount_eur) AS DECIMAL(14,2)) AS revenue_eur
        FROM lake.analytics.fact_order_items f
        JOIN lake.analytics.dim_product p ON f.product_key = p.product_key
        WHERE f.order_status IN ('paid', 'shipped', 'delivered')
        GROUP BY 1, 2, 3, 4""",
    "conversion_funnel": """
        AS
        SELECT to_date(event_ts) AS event_date, channel,
               COUNT(DISTINCT session_id) AS sessions,
               COUNT(DISTINCT CASE WHEN event_type = 'product_view'   THEN session_id END) AS product_view,
               COUNT(DISTINCT CASE WHEN event_type = 'add_to_cart'    THEN session_id END) AS add_to_cart,
               COUNT(DISTINCT CASE WHEN event_type = 'begin_checkout' THEN session_id END) AS begin_checkout,
               COUNT(DISTINCT CASE WHEN event_type = 'purchase'       THEN session_id END) AS purchase
        FROM lake.clean.clickstream
        GROUP BY 1, 2""",
    "payment_reconciliation": """
        AS
        WITH totals AS (SELECT order_id, SUM(net_amount) AS order_total FROM lake.clean.order_items GROUP BY order_id),
             pay AS (SELECT order_id,
                            SUM(CASE WHEN status = 'captured'   THEN amount END) AS captured,
                            SUM(CASE WHEN status = 'refunded'   THEN amount END) AS refunded,
                            SUM(CASE WHEN status = 'chargeback' THEN amount END) AS chargeback,
                            MAX(method) AS method
                     FROM lake.clean.payments GROUP BY order_id)
        SELECT o.order_id, to_date(o.order_ts) AS order_date, o.country, o.currency, o.status AS order_status,
               p.method, t.order_total, p.captured, p.refunded, p.chargeback,
               CAST(COALESCE(p.captured, 0) - t.order_total AS DECIMAL(14,2)) AS difference,
               CASE WHEN p.captured IS NULL AND o.status IN ('paid', 'shipped') THEN 'awaiting_payment'
                    WHEN p.captured IS NULL THEN 'missing_payment'
                    WHEN ABS(p.captured - t.order_total) > 0.01 * t.order_total THEN 'amount_mismatch'
                    WHEN o.status = 'returned' AND p.refunded IS NULL THEN 'refund_pending'
                    ELSE 'ok' END AS recon_status
        FROM lake.clean.orders o
        JOIN totals t ON o.order_id = t.order_id
        LEFT JOIN pay p ON o.order_id = p.order_id
        WHERE o.status NOT IN ('pending', 'cancelled')""",
}


def customer_360_sql(spark: SparkSession) -> str:
    seg = exists(spark, "lake.ml.customer_segments")
    return f"""
        AS
        WITH sales AS (
            SELECT dc.customer_id, COUNT(DISTINCT f.order_id) AS orders,
                   CAST(SUM(f.net_amount_eur) AS DECIMAL(14,2)) AS revenue_eur, MAX(f.order_ts) AS last_order_ts,
                   mode(f.sales_channel) AS preferred_channel
            FROM lake.analytics.fact_order_items f
            JOIN lake.analytics.dim_customer dc ON f.customer_key = dc.customer_key
            WHERE f.order_status IN ('paid', 'shipped', 'delivered')
            GROUP BY dc.customer_id),
        web AS (
            SELECT customer_id, COUNT(DISTINCT session_id) AS sessions, MAX(event_ts) AS last_seen_ts
            FROM lake.clean.clickstream WHERE customer_id IS NOT NULL GROUP BY customer_id)
        SELECT c.customer_id, c.country, c.city, c.loyalty_tier,
               COALESCE(s.orders, 0) AS orders, COALESCE(s.revenue_eur, 0) AS revenue_eur, s.last_order_ts,
               s.preferred_channel, COALESCE(w.sessions, 0) AS sessions, w.last_seen_ts
               {', m.segment_name' if seg else ', CAST(NULL AS STRING) AS segment_name'}
        FROM lake.analytics.dim_customer c
        LEFT JOIN sales s ON c.customer_id = s.customer_id
        LEFT JOIN web w ON c.customer_id = w.customer_id
        {'LEFT JOIN lake.ml.customer_segments m ON c.customer_id = m.customer_id' if seg else ''}
        WHERE c.is_current"""


def build_marts(spark: SparkSession) -> None:
    for name, body in {**MARTS, "customer_360": customer_360_sql(spark)}.items():
        spark.sql(f"CREATE OR REPLACE TABLE lake.analytics.{name} USING iceberg {body}")
        log(f"lake.analytics.{name}: {spark.table(f'lake.analytics.{name}').count()} redova")


def run(spark: SparkSession, fx_bug: bool = False) -> None:
    build_dims(spark)
    build_fact(spark, fx_bug)
    build_marts(spark)
