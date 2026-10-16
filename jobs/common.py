"""Zajednicke pomocne funkcije za Spark poslove (Spark je masina koja pise u lakehouse)."""
from __future__ import annotations

import os
import time
from datetime import datetime, timezone
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T

SOURCES = Path("/home/iceberg/sources")
PG_URL = f"jdbc:postgresql://postgres:5432/{os.environ.get('PG_DB', 'shop')}"
PG_PROPS = {"user": os.environ.get("PG_USER", "shop"), "password": os.environ.get("PG_PASSWORD", ""),
            "driver": "org.postgresql.Driver"}

# tabele koje se cesto menjaju na nivou reda: brisanje se belezi u delete fajlovima umesto prepisivanja
MERGE_ON_READ = {"write.delete.mode": "merge-on-read", "write.update.mode": "merge-on-read",
                 "write.merge.mode": "merge-on-read"}
COPY_ON_WRITE = {"write.delete.mode": "copy-on-write", "write.update.mode": "copy-on-write",
                 "write.merge.mode": "copy-on-write"}


def get_spark(app_name: str) -> SparkSession:
    spark = SparkSession.builder.appName(app_name).getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    return spark


def log(msg: str) -> None:
    print(f"  {msg}", flush=True)


def exists(spark: SparkSession, table: str) -> bool:
    return spark.catalog.tableExists(table)


def ensure_namespace(spark: SparkSession, table: str) -> None:
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {table.rsplit('.', 1)[0]}")


def target_schema(spark: SparkSession, table: str) -> T.StructType:
    spark.catalog.refreshTable(table)
    return spark.table(table).schema


def create_table(spark: SparkSession, df: DataFrame, table: str, partition_by: list | None = None,
                 props: dict | None = None, write_order: str | None = None) -> None:
    ensure_namespace(spark, table)
    w = df.writeTo(table).using("iceberg").tableProperty("format-version", "2")
    for k, v in (props or {}).items():
        w = w.tableProperty(k, v)
    if partition_by:
        w = w.partitionedBy(*partition_by)
    w.create()
    if write_order:
        spark.sql(f"ALTER TABLE {table} WRITE ORDERED BY {write_order}")


def align(df: DataFrame, schema: T.StructType, renames: dict | None = None) -> DataFrame:
    """Prilagodi DataFrame semi tabele: preimenovanja, tipovi, redosled; kolone kojih nema -> NULL."""
    for old, new in (renames or {}).items():
        if old in df.columns and new not in df.columns and new in schema.names:
            df = df.withColumnRenamed(old, new)
    dropped = [c for c in df.columns if c not in schema.names]
    if dropped:
        log(f"kolone {dropped} ne postoje u ciljnoj tabeli - preskacem")
    return df.select([(F.col(f"`{f.name}`") if f.name in df.columns else F.lit(None)).cast(f.dataType).alias(f.name)
                      for f in schema])


def evolve_schema(spark: SparkSession, df: DataFrame, table: str) -> DataFrame:
    """Evolucija seme: nova kolona iz izvora se dodaje u tabelu (samo promena metapodataka, bez prepisivanja)."""
    schema = target_schema(spark, table)
    for f in df.schema:
        if f.name not in schema.names:
            spark.sql(f"ALTER TABLE {table} ADD COLUMN `{f.name}` {f.dataType.simpleString()}")
            log(f"EVOLUCIJA SEME: {table} + kolona {f.name} {f.dataType.simpleString()}")
    return align(df, target_schema(spark, table))


def last_commit(spark: SparkSession, table: str) -> dict:
    row = spark.sql(f"SELECT snapshot_id, operation, summary FROM {table}.snapshots "
                    f"ORDER BY committed_at DESC LIMIT 1").collect()
    if not row:
        return {}
    return {"snapshot_id": row[0]["snapshot_id"], "operation": row[0]["operation"], **row[0]["summary"]}


def describe_commit(c: dict) -> str:
    keys = ["added-records", "deleted-records", "added-data-files", "added-delete-files", "total-records"]
    return f"snapshot {c.get('snapshot_id')} ({c.get('operation')}) " + ", ".join(
        f"{k}={c[k]}" for k in keys if k in c)


def upsert(spark: SparkSession, df: DataFrame, table: str, keys: list[str], partition_by: list | None = None,
           props: dict | None = None, write_order: str | None = None, evolve: bool = True,
           renames: dict | None = None) -> DataFrame | None:
    """
    MERGE INTO samo novih i izmenjenih redova (poredi se hash poslovnih kolona; kolone sa '_' su tehnicke).
    Jedan MERGE = jedna atomicna transakcija = jedan snapshot. Ako promena nema, nema ni commit-a.
    Vraca skup izmena (DataFrame) ili None.
    """
    if not exists(spark, table):
        create_table(spark, df, table, partition_by, props, write_order)
        log(f"{table}: kreirana, {spark.table(table).count()} redova")
        return df

    df = evolve_schema(spark, df, table) if evolve else align(df, target_schema(spark, table), renames)
    biz = [c for c in df.columns if c not in keys and not c.startswith("_")]
    h = F.xxhash64(*[F.col(f"`{c}`") for c in biz]) if biz else F.lit(0)
    current = spark.table(table).select(*keys, h.alias("__h"))
    changes = (df.withColumn("__h_new", h).join(current, keys, "left")
               .where(F.col("__h").isNull() | (F.col("__h") != F.col("__h_new")))
               .drop("__h", "__h_new")
               .localCheckpoint())        # MERGE trazi deterministican izvor
    n = changes.count()
    if n == 0:
        log(f"{table}: nema novih ni izmenjenih redova - nema commit-a")
        return None

    changes.createOrReplaceTempView("__changes")
    on = " AND ".join(f"t.`{k}` = s.`{k}`" for k in keys)
    spark.sql(f"MERGE INTO {table} t USING __changes s ON {on} "
              f"WHEN MATCHED THEN UPDATE SET * WHEN NOT MATCHED THEN INSERT *")
    log(f"{table}: MERGE {n} redova -> {describe_commit(last_commit(spark, table))}")
    return changes


def append(spark: SparkSession, df: DataFrame, table: str, partition_by: list | None = None,
           props: dict | None = None) -> None:
    if not exists(spark, table):
        create_table(spark, df, table, partition_by, props)
        log(f"{table}: kreirana, {spark.table(table).count()} redova")
        return
    evolve_schema(spark, df, table).writeTo(table).append()
    log(f"{table}: append -> {describe_commit(last_commit(spark, table))}")


def new_source_files(spark: SparkSession, table: str, subdir: str, pattern: str) -> list[str]:
    """Fajlovi iz izvora koji jos nisu ucitani (tabela pamti _source_file svakog reda)."""
    files = sorted(str(p.relative_to(SOURCES)) for p in (SOURCES / subdir).glob(pattern))
    if not files or not exists(spark, table):
        return files
    done = {r[0] for r in spark.table(table).select("_source_file").distinct().collect()}
    return [f for f in files if f not in done]


def source_file_col() -> F.Column:
    return F.regexp_replace(F.input_file_name(), r"^.*/sources/", "")


def now_utc() -> datetime:
    return datetime.now(timezone.utc).replace(tzinfo=None)


def ts_literal(dt: datetime) -> str:
    return f"TIMESTAMP '{dt:%Y-%m-%d %H:%M:%S}'"


class Timer:
    def __init__(self, label: str):
        self.label = label

    def __enter__(self):
        self.t0 = time.perf_counter()
        print(f"\n=== {self.label}", flush=True)
        return self

    def __exit__(self, *exc):
        print(f"=== {self.label}: {time.perf_counter() - self.t0:.1f}s", flush=True)


# ------------------------------------------------------------------ audit tabele (citaju ih konzola i izvestaji)
WAP_LOG_SCHEMA = ("run_id STRING, checked_at TIMESTAMP, table_name STRING, branch STRING, audit_snapshot_id BIGINT, "
                  "check_name STRING, passed BOOLEAN, observed STRING, threshold STRING, published BOOLEAN")
MAINT_LOG_SCHEMA = ("run_id STRING, run_at TIMESTAMP, table_name STRING, phase STRING, snapshots BIGINT, "
                    "manifests BIGINT, data_files BIGINT, delete_files BIGINT, data_bytes BIGINT, "
                    "all_files_bytes BIGINT, orphan_candidates BIGINT")


def ensure_audit_tables(spark: SparkSession) -> None:
    spark.sql("CREATE NAMESPACE IF NOT EXISTS lake.audit")
    spark.sql(f"CREATE TABLE IF NOT EXISTS lake.audit.wap_log ({WAP_LOG_SCHEMA}) USING iceberg")
    spark.sql(f"CREATE TABLE IF NOT EXISTS lake.audit.maintenance_log ({MAINT_LOG_SCHEMA}) USING iceberg")
