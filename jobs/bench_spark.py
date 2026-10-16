"""
Merenja (faza 4), Spark deo. Svako vreme je medijana 5 ponavljanja; uz vreme se uvek belezi i broj fajlova / bajtova,
jer oni ne zavise od masine na kojoj se meri. Radi nad sintetickim tabelama u imenskom prostoru lake.bench
(ponovljivo, ne dira tabele prodavnice), osim kontrolnog zbira koji se racuna nad analytics.fact_order_items.

  small_files  50 mikro-serija -> 50 malih fajlova; upit pre i posle kompakcije
  cow_vs_mor   azuriranje K rasutih redova: vreme i bajtovi upisa, vreme citanja posle
  rollback     vreme rollback_to_snapshot za tabele od 0,1 / 1 / 10 miliona redova (naspram vracanja kopiranjem)
  storage      rast skladista kroz 10 azuriranja i pad posle expire_snapshots
  checksum     kontrolni zbir septembra iz fact tabele (Trino i DuckDB ga porede u bench_py.py)
  v3           da li Spark 3.5 / Iceberg 1.8.1 moze da pise format v3

    spark-submit bench_spark.py [deo ...]      (bez argumenata: svi delovi)
"""
from __future__ import annotations

import json
import os
import statistics
import sys
import time
from pathlib import Path

import pyarrow.fs as pafs
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from common import COPY_ON_WRITE, MERGE_ON_READ, get_spark, log, now_utc, ts_literal

OUT = Path("/home/iceberg/results/bench_spark.json")
RUNS = 5


# ------------------------------------------------------------------ pomocne
def median_time(fn, runs: int = RUNS) -> float:
    times = []
    for _ in range(runs):
        t0 = time.perf_counter()
        fn()
        times.append(time.perf_counter() - t0)
    return round(statistics.median(times), 3)


def s3_bytes(spark: SparkSession, table: str) -> dict:
    """Sve sto tabela zauzima na S3 (data + metadata), bez obzira na to koji snimak sta referencira."""
    loc = spark.sql(f"DESCRIBE TABLE EXTENDED {table}").where("col_name = 'Location'").first()["data_type"]
    fs = pafs.S3FileSystem(endpoint_override="minio:9000", scheme="http", region="us-east-1",
                           access_key=os.environ["AWS_ACCESS_KEY_ID"], secret_key=os.environ["AWS_SECRET_ACCESS_KEY"])
    infos = [i for i in fs.get_file_info(pafs.FileSelector(loc.removeprefix("s3://"), recursive=True))
             if i.type == pafs.FileType.File]
    data = [i for i in infos if "/data/" in i.path]
    return {"s3_files": len(infos), "s3_bytes": sum(i.size for i in infos),
            "s3_data_files": len(data), "s3_data_bytes": sum(i.size for i in data)}


def current_files(spark: SparkSession, table: str) -> dict:
    r = spark.sql(f"""SELECT COUNT_IF(content = 0) AS data_files, COUNT_IF(content > 0) AS delete_files,
                             COALESCE(SUM(file_size_in_bytes), 0) AS bytes FROM {table}.files""").first()
    return {"data_files": r.data_files, "delete_files": r.delete_files, "bytes": r.bytes}


def last_summary(spark: SparkSession, table: str) -> dict:
    return spark.sql(f"SELECT summary FROM {table}.snapshots ORDER BY committed_at DESC LIMIT 1").first()["summary"]


def synthetic(spark: SparkSession, n: int, start: int = 0, seed: int = 7):
    return (spark.range(start, start + n)
            .select(F.col("id"), (F.col("id") % 50).cast("int").alias("category"),
                    F.round(F.rand(seed) * 100, 2).cast("decimal(10,2)").alias("amount"),
                    F.timestamp_seconds(F.lit(1788220800) + F.col("id") % 2592000).alias("event_ts"),
                    F.substring(F.sha2(F.col("id").cast("string"), 256), 1, 24).alias("payload")))


def fresh(spark: SparkSession, table: str, df, props: dict, files: int = 8) -> None:
    spark.sql(f"DROP TABLE IF EXISTS {table} PURGE")
    w = df.repartition(files).writeTo(table).using("iceberg").tableProperty("format-version", "2")
    for k, v in props.items():
        w = w.tableProperty(k, v)
    w.create()


AGG = "SELECT category, COUNT(*), SUM(amount) FROM {t} GROUP BY category"


# ------------------------------------------------------------------ merenja
def small_files(spark: SparkSession) -> dict:
    t = "lake.bench.events"
    spark.sql(f"DROP TABLE IF EXISTS {t} PURGE")
    synthetic(spark, 0).writeTo(t).using("iceberg").tableProperty("format-version", "2").create()
    batches, per_batch = 50, 4000
    t0 = time.perf_counter()
    for b in range(batches):   # svaka mikro-serija je zaseban upis -> zaseban fajl, manifest i snimak
        synthetic(spark, per_batch, start=b * per_batch, seed=b).coalesce(1).writeTo(t).append()
    write_s = time.perf_counter() - t0
    q = lambda: spark.sql(AGG.format(t=t)).collect()  # noqa: E731
    q()
    manifests = lambda: spark.sql(f"SELECT COUNT(*) FROM {t}.manifests").first()[0]  # noqa: E731
    before = {**current_files(spark, t), "manifests": manifests(), "query_s": median_time(q)}
    t0 = time.perf_counter()
    spark.sql(f"CALL lake.system.rewrite_data_files(table => 'bench.events', options => map('min-input-files', '2'))").collect()
    spark.sql("CALL lake.system.rewrite_manifests('bench.events')").collect()
    compact_s = time.perf_counter() - t0
    q()
    after = {**current_files(spark, t), "manifests": manifests(), "query_s": median_time(q)}
    res = {"batches": batches, "rows": batches * per_batch, "write_all_batches_s": round(write_s, 2),
           "compaction_s": round(compact_s, 2), "before": before, "after": after}
    log(f"small_files: {res}")
    return res


def cow_vs_mor(spark: SparkSession) -> dict:
    n = 2_000_000
    base = synthetic(spark, n).localCheckpoint()
    out = []
    for k in (10, 1_000, 100_000):
        step = n // k
        for mode, props in (("copy-on-write", COPY_ON_WRITE), ("merge-on-read", MERGE_ON_READ)):
            t = f"lake.bench.upd_{'cow' if mode.startswith('copy') else 'mor'}"
            fresh(spark, t, base, props)
            times, written = [], []
            for r in range(RUNS):   # 5 azuriranja razlicitih (rasutih) skupova od K redova
                t0 = time.perf_counter()
                spark.sql(f"UPDATE {t} SET amount = amount + 1 WHERE pmod(id, {step}) = {r}")
                times.append(time.perf_counter() - t0)
                s = last_summary(spark, t)
                written.append(int(s.get("added-files-size", 0)))
            files = current_files(spark, t)
            read_s = median_time(lambda: spark.sql(AGG.format(t=t)).collect())
            row = {"k_rows": k, "mode": mode, "update_s": round(statistics.median(times), 3),
                   "bytes_written_per_update": int(statistics.median(written)), "read_s_after": read_s, **files}
            log(f"cow_vs_mor: {row}")
            out.append(row)
    return {"table_rows": n, "table_files": 8, "results": out}


def rollback(spark: SparkSession) -> dict:
    out = []
    for n in (100_000, 1_000_000, 10_000_000):
        t = "lake.bench.rb"
        fresh(spark, t, synthetic(spark, n), COPY_ON_WRITE, files=max(1, n // 1_000_000))
        good = spark.sql(f"SELECT snapshot_id FROM {t}.snapshots").first()[0]
        synthetic(spark, n // 10, start=n).writeTo(t).append()            # "losa" serija
        bad = spark.sql(f"SELECT snapshot_id FROM {t}.snapshots ORDER BY committed_at DESC LIMIT 1").first()[0]
        times = []
        for _ in range(RUNS):
            t0 = time.perf_counter()
            spark.sql(f"CALL lake.system.rollback_to_snapshot('bench.rb', {good})").collect()
            times.append(time.perf_counter() - t0)
            spark.sql(f"CALL lake.system.set_current_snapshot('bench.rb', {bad})").collect()
        copy_s = median_time(lambda: spark.sql(
            f"CREATE OR REPLACE TABLE lake.bench.rb_copy USING iceberg AS SELECT * FROM {t} VERSION AS OF {good}"), runs=3)
        row = {"rows": n, "bytes": current_files(spark, t)["bytes"], "rollback_s": round(statistics.median(times), 3),
               "restore_by_copy_s": copy_s}
        log(f"rollback: {row}")
        out.append(row)
    spark.sql("DROP TABLE IF EXISTS lake.bench.rb_copy PURGE")
    return {"results": out, "note": "restore_by_copy_s = medijana 3 ponavljanja"}


def storage(spark: SparkSession) -> dict:
    t = "lake.bench.growth"
    fresh(spark, t, synthetic(spark, 1_000_000), COPY_ON_WRITE)
    series = [{"step": "pocetno stanje", **current_files(spark, t), **s3_bytes(spark, t)}]
    for r in range(10):   # svako azuriranje dira 1% rasutih redova -> copy-on-write prepisuje sve fajlove
        spark.sql(f"UPDATE {t} SET amount = amount + 1 WHERE pmod(id, 100) = {r}")
        series.append({"step": f"azuriranje {r + 1}", **current_files(spark, t), **s3_bytes(spark, t)})
    spark.sql(f"CALL lake.system.expire_snapshots(table => 'bench.growth', "
              f"older_than => {ts_literal(now_utc())}, retain_last => 1)").collect()
    series.append({"step": "posle expire_snapshots", **current_files(spark, t), **s3_bytes(spark, t)})
    for s in series:
        log(f"storage: {s}")
    return {"series": series}


def checksum(spark: SparkSession) -> dict:
    r = spark.sql("""
        SELECT COUNT(*) AS rows, CAST(SUM(net_amount_eur) AS DECIMAL(18,2)) AS revenue_eur,
               SUM(quantity) AS quantity, SUM(order_item_id) AS id_sum
        FROM lake.analytics.fact_order_items
        WHERE order_ts >= TIMESTAMP '2026-09-01 00:00:00' AND order_ts < TIMESTAMP '2026-10-01 00:00:00'""").first()
    res = {"rows": r.rows, "revenue_eur": str(r.revenue_eur), "quantity": r.quantity, "id_sum": r.id_sum,
           "snapshot_id": spark.sql("SELECT snapshot_id FROM lake.analytics.fact_order_items.snapshots "
                                    "ORDER BY committed_at DESC LIMIT 1").first()[0]}
    log(f"checksum: {res}")
    return res


def v3(spark: SparkSession) -> dict:
    spark.sql("DROP TABLE IF EXISTS lake.bench.v3_probe PURGE")
    spark.sql("CREATE TABLE lake.bench.v3_probe (id BIGINT, note STRING) USING iceberg TBLPROPERTIES ('format-version' = '3')")
    try:
        spark.sql("INSERT INTO lake.bench.v3_probe VALUES (1, 'v3')")
        res = {"create": True, "insert": True, "error": None}
    except Exception as e:
        cause = getattr(e, "java_exception", None)
        while cause is not None and cause.getCause() is not None:   # najdublji Java uzrok, ne py4j omotac
            cause = cause.getCause()
        res = {"create": True, "insert": False, "error": (cause.toString() if cause is not None else str(e))[:300]}
    log(f"v3: {res}")
    return res


PARTS = {"small_files": small_files, "cow_vs_mor": cow_vs_mor, "rollback": rollback, "storage": storage,
         "checksum": checksum, "v3": v3}


def main() -> None:
    parts = sys.argv[1:] or list(PARTS)
    spark = get_spark("bench")
    spark.sql("CREATE NAMESPACE IF NOT EXISTS lake.bench")
    results = json.loads(OUT.read_text()) if OUT.exists() else {}
    for p in parts:
        log(f"=== {p}")
        results[p] = PARTS[p](spark)
        OUT.parent.mkdir(parents=True, exist_ok=True)
        OUT.write_text(json.dumps(results, indent=2, default=str))
    log(f"rezultati: results/{OUT.name}")


if __name__ == "__main__":
    main()
