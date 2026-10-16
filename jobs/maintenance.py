"""
Nocno odrzavanje (nightly_maintenance):
  1. rewrite_data_files       - kompakcija malih fajlova; fact se sortira po order_ts;
                                kod merge-on-read tabela delete fajlovi se "upeku" u nove data fajlove
  2. rewrite_position_delete_files - uklanja delete fajlove koji vise nemaju na sta da se odnose
  3. expire_snapshots         - brise stare snapshot-ove (i fajlove koje samo oni koriste); tagovi se cuvaju
  4. rewrite_manifests        - preuredjuje manifeste radi brzeg planiranja upita
  5. remove_orphan_files      - SAMO dry_run: Iceberg odbija interval kraci od 24h jer bi mogao da obrise
                                fajlove upisa koji je upravo u toku

Stanje pre i posle se upisuje u lake.audit.maintenance_log (prikazuje ga konzola).
"""
from __future__ import annotations

import argparse
import os
import uuid

from pyspark.sql import SparkSession

from common import MAINT_LOG_SCHEMA, Timer, ensure_audit_tables, exists, get_spark, log, now_utc, ts_literal

TABLES = [  # (tabela, strategija kompakcije)
    ("raw.orders", "mor"),
    ("raw.order_items", "mor"),
    ("raw.clickstream", "binpack-days"),
    ("clean.clickstream", "binpack"),
    ("clean.payments", "binpack"),
    ("analytics.fact_order_items", "sort"),
]


def table_stats(spark: SparkSession, t: str) -> dict:
    files = spark.sql(f"SELECT content, COUNT(*) AS n, COALESCE(SUM(file_size_in_bytes), 0) AS b "
                      f"FROM lake.{t}.files GROUP BY content").collect()
    by = {r["content"]: r for r in files}
    all_bytes = spark.sql(f"SELECT COALESCE(SUM(file_size_in_bytes), 0) FROM "
                          f"(SELECT DISTINCT file_path, file_size_in_bytes FROM lake.{t}.all_files)").first()[0]
    return {
        "snapshots": spark.sql(f"SELECT COUNT(*) FROM lake.{t}.snapshots").first()[0],
        "manifests": spark.sql(f"SELECT COUNT(*) FROM lake.{t}.manifests").first()[0],
        "data_files": by[0]["n"] if 0 in by else 0,
        "delete_files": sum(by[k]["n"] for k in (1, 2) if k in by),
        "data_bytes": by[0]["b"] if 0 in by else 0,
        "all_files_bytes": all_bytes,
    }


def s3_listing(spark: SparkSession, t: str) -> str:
    """Listing fajlova tabele direktno sa S3 (pyarrow), kao view za remove_orphan_files(file_list_view)."""
    import pyarrow.fs as pafs
    location = spark.sql(f"DESCRIBE TABLE EXTENDED lake.{t}").where("col_name = 'Location'").first()["data_type"]
    s3 = pafs.S3FileSystem(endpoint_override="minio:9000", scheme="http", region="us-east-1",
                           access_key=os.environ["AWS_ACCESS_KEY_ID"], secret_key=os.environ["AWS_SECRET_ACCESS_KEY"])
    infos = s3.get_file_info(pafs.FileSelector(location.removeprefix("s3://"), recursive=True))
    rows = [("s3://" + i.path, i.mtime.replace(tzinfo=None)) for i in infos if i.type == pafs.FileType.File]
    spark.createDataFrame(rows, "file_path STRING, last_modified TIMESTAMP").createOrReplaceTempView("__listing")
    return "__listing"


def maintain(spark: SparkSession, t: str, strategy: str, retain_last: int, run_id: str) -> None:
    if not exists(spark, f"lake.{t}"):
        return
    print(f"\n--- lake.{t}", flush=True)
    before = table_stats(spark, t)

    # jedna grupa fajlova u isto vreme: svaki task drzi Parquet row group u memoriji, a heap je 3 GB
    opts = "'min-input-files', '2', 'max-concurrent-file-group-rewrites', '1'"
    if strategy == "mor":
        opts += ", 'delete-file-threshold', '1'"
    if strategy == "binpack-days":
        # posle black_friday (days -> hours) kompakcija bi po pravilu pisala u tekucu, satnu specifikaciju i
        # rasparcala stare dnevne fajlove na 24 sitna; zato se spaja u prvobitnu dnevnu specifikaciju (spec 0):
        # satne particije za sveze podatke, nocu se spajaju u dnevne fajlove
        opts += ", 'output-spec-id', '0'"
    if strategy == "sort":
        r = spark.sql(f"CALL lake.system.rewrite_data_files(table => '{t}', strategy => 'sort', "
                      f"sort_order => 'order_ts ASC NULLS LAST', options => map({opts}))").first()
    else:
        r = spark.sql(f"CALL lake.system.rewrite_data_files(table => '{t}', options => map({opts}))").first()
    log(f"rewrite_data_files: {r['rewritten_data_files_count']} fajlova -> {r['added_data_files_count']}")
    if strategy == "mor":
        r = spark.sql(f"CALL lake.system.rewrite_position_delete_files(table => '{t}', "
                      f"options => map('rewrite-all', 'true'))").first()
        log(f"rewrite_position_delete_files: uklonjeno {r['rewritten_delete_files_count'] - r['added_delete_files_count']} delete fajlova")

    r = spark.sql(f"CALL lake.system.expire_snapshots(table => '{t}', older_than => {ts_literal(now_utc())}, "
                  f"retain_last => {retain_last})").first()
    log(f"expire_snapshots: obrisano {r['deleted_data_files_count']} data, {r['deleted_position_delete_files_count']} "
        f"delete fajlova i {r['deleted_manifest_files_count']} manifesta")
    r = spark.sql(f"CALL lake.system.rewrite_manifests('{t}')").first()
    log(f"rewrite_manifests: {r['rewritten_manifests_count']} -> {r['added_manifests_count']}")

    orphans = spark.sql(f"CALL lake.system.remove_orphan_files(table => '{t}', dry_run => true, "
                        f"file_list_view => '{s3_listing(spark, t)}')").count()
    log(f"remove_orphan_files (dry_run, starije od 3 dana): {orphans} kandidata")

    after = table_stats(spark, t)
    log(f"data fajlova {before['data_files']} -> {after['data_files']}, delete fajlova {before['delete_files']} -> "
        f"{after['delete_files']}, snapshot-ova {before['snapshots']} -> {after['snapshots']}, "
        f"skladiste {before['all_files_bytes'] / 1e6:.1f} MB -> {after['all_files_bytes'] / 1e6:.1f} MB")
    at = now_utc()
    rows = [(run_id, at, t, phase, s["snapshots"], s["manifests"], s["data_files"], s["delete_files"], s["data_bytes"],
             s["all_files_bytes"], orphans if phase == "after" else None)
            for phase, s in (("before", before), ("after", after))]
    spark.createDataFrame(rows, MAINT_LOG_SCHEMA).writeTo("lake.audit.maintenance_log").append()


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--retain-last", type=int, default=5)
    ap.add_argument("--tables", default=",".join(t for t, _ in TABLES))
    args = ap.parse_args()
    spark = get_spark("maintenance")
    ensure_audit_tables(spark)
    run_id = str(uuid.uuid4())[:8]
    with Timer("nightly_maintenance"):
        for t, strategy in TABLES:
            if t in args.tables.split(","):
                maintain(spark, t, strategy, args.retain_last, run_id)
    spark.stop()
