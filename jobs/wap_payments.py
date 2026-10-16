"""
Upis-provera-objava (Write-Audit-Publish) za clean.payments.

  1. grana `audit` se postavi na trenutno stanje grane `main`
  2. novi i izmenjeni redovi se upisuju SAMO u granu audit (spark.wap.branch=audit)
  3. provere se izvrsavaju nad granom audit; izvestaji i dalje citaju main i nista ne vide
  4. ako sve provere prodju -> CALL fast_forward(main -> audit): objava je jedna promena pokazivaca
     ako ne prodju        -> main ostaje netaknut, paket ostaje u grani audit na uvid

Provere: nema negativnih iznosa | svaka uplata ima porudzbinu | iznos uplate = iznos porudzbine |
         dnevni promet u EUR ne odstupa neuobicajeno od uobicajenog (npr. iznosi u centima, x100)
"""
from __future__ import annotations

import uuid

from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F

from common import WAP_LOG_SCHEMA, create_table, ensure_audit_tables, exists, log, now_utc, upsert

TABLE = "lake.clean.payments"
BRANCH = "audit"


def latest_payments(spark: SparkSession):
    """Poslednja primljena verzija svake uplate (ispravljen fajl _v2 pobedjuje pogresan, duplikati nestaju)."""
    w = Window.partitionBy("payment_id").orderBy(F.col("_ingested_at").desc(), F.col("_source_file").desc())
    fx = (spark.table("lake.raw.fx_rates")
          .withColumn("_rn", F.row_number().over(Window.partitionBy("rate_date", "currency").orderBy(F.col("_ingested_at").desc())))
          .where("_rn = 1").select("rate_date", "currency", "rate_to_eur"))
    p = (spark.table("lake.raw.payments")
         .withColumn("_rn", F.row_number().over(w)).where("_rn = 1")
         .select("payment_id", "order_id", "amount", F.upper("currency").alias("currency"), F.lower("method").alias("method"),
                 F.lower("status").alias("status"), "processed_at", "_source_file"))
    return (p.join(fx, (F.to_date(p.processed_at) == fx.rate_date) & (p.currency == fx.currency), "left")
            .select(p["*"], (p.amount * fx.rate_to_eur).cast("decimal(14,2)").alias("amount_eur"))
            .select("payment_id", "order_id", "amount", "currency", "amount_eur", "method", "status", "processed_at",
                    "_source_file"))


def run_checks(spark: SparkSession, changes) -> list[dict]:
    audit = spark.sql(f"SELECT * FROM {TABLE} VERSION AS OF '{BRANCH}'")
    batch = audit.join(changes.select("payment_id"), "payment_id")           # ono sto bi bilo objavljeno
    batch.createOrReplaceTempView("__batch")
    checks = []

    neg = batch.where("amount <= 0").count()
    checks.append(dict(check_name="no_negative_amounts", passed=neg == 0, observed=f"{neg} redova", threshold="0"))

    orphans = batch.join(spark.table("lake.clean.orders"), "order_id", "left_anti").count()
    checks.append(dict(check_name="orders_exist", passed=orphans == 0, observed=f"{orphans} uplata bez porudzbine",
                       threshold="0"))

    rec = spark.sql("""
        WITH totals AS (SELECT order_id, SUM(net_amount) AS total FROM lake.clean.order_items GROUP BY order_id)
        SELECT COUNT(*) AS n, SUM(CASE WHEN ABS(b.amount - t.total) <= 0.01 * t.total THEN 1 ELSE 0 END) AS ok
        FROM __batch b JOIN totals t USING (order_id) WHERE b.status = 'captured'
    """).first()
    share = (rec["ok"] / rec["n"]) if rec["n"] else 1.0
    checks.append(dict(check_name="amount_matches_order", passed=share >= 0.98,
                       observed=f"{share:.1%} od {rec['n']} naplata", threshold=">= 98%"))

    daily = spark.sql("""SELECT to_date(processed_at) AS d, SUM(amount_eur) AS eur FROM __batch
                         WHERE status = 'captured' GROUP BY 1""").collect()
    base = spark.sql(f"""SELECT percentile_approx(eur, 0.5) AS median FROM (
                           SELECT to_date(processed_at) AS d, SUM(amount_eur) AS eur FROM {TABLE}
                           WHERE status = 'captured' GROUP BY 1 ORDER BY d DESC LIMIT 14)""").first()["median"]
    if base and daily:
        worst = max(daily, key=lambda r: abs(float(r["eur"]) / float(base) - 1))
        ratio = float(worst["eur"]) / float(base)
        # hvata pogresan red velicine (centi umesto evra = 100x), a propusta legitimne vrhove (akcija, Black Friday ~5x)
        checks.append(dict(check_name="daily_volume_normal", passed=0.1 <= ratio <= 20,
                           observed=f"{worst['d']}: {ratio:.1f}x medijane ({float(worst['eur']):,.0f} EUR)",
                           threshold="0.1x - 20x"))
    else:
        checks.append(dict(check_name="daily_volume_normal", passed=True, observed="nema istorije za poredjenje",
                           threshold="0.1x - 20x"))
    return checks


def publish(spark: SparkSession) -> bool:
    ensure_audit_tables(spark)
    src = latest_payments(spark)
    if not exists(spark, TABLE):
        # prazna tabela sa jednim snapshot-om, da bi grana audit imala od cega da krene
        create_table(spark, src.limit(0), TABLE, props={"write.wap.enabled": "true"})
        spark.sql(f"INSERT INTO {TABLE} SELECT * FROM {TABLE}")
        log(f"{TABLE}: kreirana (write.wap.enabled=true)")

    spark.sql(f"ALTER TABLE {TABLE} CREATE OR REPLACE BRANCH {BRANCH}")
    spark.conf.set("spark.wap.branch", BRANCH)
    try:
        log(f"upis u granu '{BRANCH}' (izvestaji citaju 'main' i ne vide ovaj upis)")
        changes = upsert(spark, src, TABLE, ["payment_id"])
    finally:
        spark.conf.unset("spark.wap.branch")
    if changes is None:
        return True

    checks = run_checks(spark, changes)
    ok = all(c["passed"] for c in checks)
    audit_snapshot = spark.sql(f"SELECT snapshot_id FROM {TABLE}.refs WHERE name = '{BRANCH}'").first()[0]
    for c in checks:
        log(f"  [{'OK  ' if c['passed'] else 'PADA'}] {c['check_name']:<22} {c['observed']}  (prag {c['threshold']})")

    if ok:
        r = spark.sql(f"CALL lake.system.fast_forward('{TABLE.removeprefix('lake.')}', 'main', '{BRANCH}')").first()
        log(f"OBJAVLJENO: main {r['previous_ref']} -> {r['updated_ref']} (fast_forward, bez kopiranja podataka)")
    else:
        log(f"NIJE OBJAVLJENO: paket ostaje u grani '{BRANCH}' (snapshot {audit_snapshot}), main je nepromenjen")

    run_id, at = str(uuid.uuid4())[:8], now_utc()
    rows = [(run_id, at, TABLE, BRANCH, audit_snapshot, c["check_name"], c["passed"], c["observed"], c["threshold"], ok)
            for c in checks]
    spark.createDataFrame(rows, WAP_LOG_SCHEMA).writeTo("lake.audit.wap_log").append()
    return ok


def run(spark: SparkSession) -> bool:
    return publish(spark)
