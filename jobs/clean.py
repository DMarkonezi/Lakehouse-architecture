"""
Sloj CLEAN: ocisceni i uskladjeni podaci.
  clean.orders, clean.order_items   - normalizovani statusi i valute, izracunat net_amount
  clean.clickstream                 - ispravni dogadjaji bez duplikata, izveden kanal (web/app)
  clean.clickstream_rejected        - odbaceni dogadjaji sa razlogom (kvarantin)
  clean.payments                    - puni ga wap_payments.py (upis-provera-objava)
"""
from __future__ import annotations

from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F

from common import append, exists, log, upsert

EVENT_TYPES = ["page_view", "search", "product_view", "add_to_cart", "begin_checkout", "purchase"]


def clean_orders(spark: SparkSession) -> None:
    df = spark.table("lake.raw.orders").select(
        "order_id", "customer_id", "order_ts",
        F.lower(F.trim("sales_channel")).alias("sales_channel"),
        F.upper(F.trim("country")).alias("country"),
        F.upper(F.trim("currency")).alias("currency"),
        F.lower(F.trim("status")).alias("status"),
        "updated_at")
    upsert(spark, df, "lake.clean.orders", ["order_id"], partition_by=[F.months("order_ts")])


def clean_order_items(spark: SparkSession) -> None:
    raw = spark.table("lake.raw.order_items")
    extra = ["discount_pct"] if "discount_pct" in raw.columns else []      # stize tek sa promotions_engine
    df = raw.select(
        "order_item_id", "order_id", "product_id",
        F.col("quantity").cast("int").alias("quantity"),
        F.col("unit_price").cast("decimal(10,2)").alias("unit_price"),
        F.coalesce("discount", F.lit(0)).cast("decimal(10,2)").alias("discount"),
        (F.col("quantity") * F.col("unit_price") - F.coalesce("discount", F.lit(0))).cast("decimal(12,2)").alias("net_amount"),
        F.lower(F.trim("item_status")).alias("item_status"),
        "updated_at", *extra)
    upsert(spark, df, "lake.clean.order_items", ["order_item_id"])


def clean_clickstream(spark: SparkSession) -> None:
    raw = spark.table("lake.raw.clickstream")
    wm = None
    for t in ("lake.clean.clickstream", "lake.clean.clickstream_rejected"):
        if exists(spark, t):
            v = spark.table(t).agg(F.max("_raw_ingested_at")).first()[0]
            wm = v if wm is None or (v and v > wm) else wm
    new = raw.where(F.col("_ingested_at") > F.lit(wm)) if wm else raw
    new = new.withColumnRenamed("_ingested_at", "_raw_ingested_at").localCheckpoint()
    if new.isEmpty():
        log("lake.clean.clickstream: nema novih dogadjaja")
        return

    dup_rank = F.row_number().over(Window.partitionBy("event_id").orderBy("_source_file", "event_ts"))
    checked = new.withColumn("_rn", dup_rank).withColumn("reason", F.expr(f"""
        CASE WHEN event_id IS NULL THEN 'missing_event_id'
             WHEN event_ts IS NULL THEN 'invalid_event_ts'
             WHEN event_type IS NULL THEN 'missing_event_type'
             WHEN event_type NOT IN ({', '.join(repr(e) for e in EVENT_TYPES)}) THEN 'unknown_event_type'
             WHEN _rn > 1 THEN 'duplicate'
        END""")).drop("_rn").localCheckpoint()

    valid = (checked.where("reason IS NULL").drop("reason")
             .withColumn("channel", F.when(F.col("device.app_version").isNotNull(), "app").otherwise("web")))
    rejected = checked.where("reason IS NOT NULL").select(
        "event_id", "session_id", "reason",
        F.to_json(F.struct(*[c for c in new.columns if not c.startswith("_")])).alias("raw_event"),
        "_source_file", "_raw_ingested_at")

    append(spark, valid, "lake.clean.clickstream", partition_by=[F.days("event_ts")])
    if not rejected.isEmpty():
        reasons = {r["reason"]: r["count"] for r in rejected.groupBy("reason").count().collect()}
        log(f"odbaceno {sum(reasons.values())} dogadjaja: {reasons}")
        append(spark, rejected, "lake.clean.clickstream_rejected")


def run(spark: SparkSession) -> None:
    clean_orders(spark)
    clean_order_items(spark)
    clean_clickstream(spark)
