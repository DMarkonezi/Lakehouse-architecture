"""
Sloj RAW: podaci u obliku u kome su stigli iz izvora.
  PostgreSQL   -> raw.customers, raw.products, raw.orders, raw.order_items   (MERGE, inkrementalno po updated_at)
  CSV          -> raw.payments, raw.fx_rates                                 (append, svaki fajl jednom)
  JSON Lines   -> raw.clickstream                                            (append, particija days(event_ts))
"""
from __future__ import annotations

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from common import (MERGE_ON_READ, PG_PROPS, PG_URL, append, exists, log, new_source_files, source_file_col,
                    upsert)

POSTGRES_TABLES = [  # (tabela, kljuc, svojstva)
    ("customers", ["customer_id"], {}),
    ("products", ["product_id"], {}),
    ("orders", ["order_id"], MERGE_ON_READ),          # statusi se stalno menjaju -> merge-on-read
    ("order_items", ["order_item_id"], MERGE_ON_READ),
]


def ingest_postgres(spark: SparkSession) -> None:
    for name, keys, props in POSTGRES_TABLES:
        table = f"lake.raw.{name}"
        where = ""
        
        if exists(spark, table):
            wm = spark.table(table).agg(F.max("updated_at")).first()[0]
            if wm:
                where = f" WHERE updated_at > TIMESTAMP '{wm:%Y-%m-%d %H:%M:%S}'"      # watermark
        
        df = spark.read.jdbc(PG_URL, f"(SELECT * FROM {name}{where}) src", properties=PG_PROPS)
        
        df = df.withColumn("_ingested_at", F.current_timestamp())
        
        if df.isEmpty():
            log(f"{table}: no changes in source {where}")
            continue
        upsert(spark, df, table, keys, props=props)


def ingest_payments(spark: SparkSession) -> None:
    table = "lake.raw.payments"
    files = new_source_files(spark, table, "payments", "*.csv")
    if not files:
        log(f"{table}: nema novih fajlova")
        return
    df = (spark.read.option("header", True)
          .schema("payment_id STRING, order_id BIGINT, amount DECIMAL(14,2), currency STRING, method STRING, "
                  "status STRING, processed_at TIMESTAMP")
          .csv([f"file:///home/iceberg/sources/{f}" for f in files])
          .withColumn("_source_file", source_file_col())
          .withColumn("_ingested_at", F.current_timestamp()))
    log(f"{table}: {len(files)} novih fajlova ({files[0]} .. {files[-1]})")
    append(spark, df, table)


def ingest_fx(spark: SparkSession) -> None:
    table = "lake.raw.fx_rates"
    files = new_source_files(spark, table, "fx_rates", "*.csv")
    if not files:
        log(f"{table}: nema novih fajlova")
        return
    df = (spark.read.option("header", True)
          .schema("rate_date DATE, currency STRING, rate_to_eur DECIMAL(18,8)")
          .csv([f"file:///home/iceberg/sources/{f}" for f in files])
          .withColumn("_source_file", source_file_col())
          .withColumn("_ingested_at", F.current_timestamp()))
    append(spark, df, table)


def ingest_clickstream(spark: SparkSession) -> None:
    table = "lake.raw.clickstream"
    files = new_source_files(spark, table, "clickstream", "date=*/*.jsonl")
    if not files:
        log(f"{table}: nema novih fajlova")
        return
    log(f"{table}: {len(files)} novih fajlova po satu")
    # sema se otkriva iz samih podataka: novo polje (npr. experiment) postaje nova kolona tabele
    df = spark.read.json([f"file:///home/iceberg/sources/{f}" for f in files])
    for c in ("customer_id", "product_id", "order_id"):
        df = df.withColumn(c, F.col(c).cast("bigint")) if c in df.columns else df.withColumn(c, F.lit(None).cast("bigint"))
    for c in ("search_query", "event_type"):
        if c not in df.columns:
            df = df.withColumn(c, F.lit(None).cast("string"))
    df = (df.withColumn("event_ts", F.to_timestamp("event_ts", "yyyy-MM-dd'T'HH:mm:ss'Z'"))   # neispravno -> NULL
          .withColumn("_source_file", source_file_col())
          .withColumn("_ingested_at", F.current_timestamp()))
    first = ["event_id", "session_id", "customer_id", "event_ts", "event_type", "product_id", "search_query", "order_id"]
    df = df.select(*first, *[c for c in df.columns if c not in first])
    append(spark, df, table, partition_by=[F.days("event_ts")])


def run(spark: SparkSession) -> None:
    ingest_postgres(spark)
    ingest_fx(spark)
    ingest_payments(spark)
    ingest_clickstream(spark)
