"""
Spark strana scenarija (poziva ih scenario.ps1):
    promotions_engine | black_friday | bad_deploy | rollback | month_close | erasure_request [id]
    atomicity_prepare | atomicity_write
"""
from __future__ import annotations

import sys
import time

from pyspark.sql import SparkSession

import analytics
from common import exists, get_spark, log, now_utc, ts_literal

FACT = "lake.analytics.fact_order_items"


def show(spark: SparkSession, sql: str, n: int = 20) -> None:
    # pandas umesto DataFrame.show(): bez '|' okvira koje scenario.ps1 filtrira kao Ivy sum
    pdf = spark.sql(sql).limit(n).toPandas()
    print("\n".join("    " + line for line in pdf.to_string(index=False).splitlines()) + "\n")


def data_files(spark: SparkSession, t: str) -> int:
    return spark.sql(f"SELECT COUNT(*) FROM {t}.files").first()[0]


# ------------------------------------------------------------------ evolucija seme
def promotions_engine(spark: SparkSession) -> None:
    cols = spark.table(FACT).columns
    if "discount_amount" in cols:
        log("evolucija je vec uradjena")
        return
    files_before = data_files(spark, FACT)
    snap_before = spark.sql(f"SELECT snapshot_id FROM {FACT}.refs WHERE name = 'main'").first()[0]
    print("PRE:", ", ".join(f"{f.name} {f.dataType.simpleString()}" for f in spark.table(FACT).schema
                            if f.name in ("unit_price", "discount")))

    spark.sql(f"ALTER TABLE {FACT} RENAME COLUMN discount TO discount_amount")            # dvosmislen naziv
    spark.sql(f"ALTER TABLE {FACT} ADD COLUMN discount_pct DECIMAL(5,2) AFTER discount_amount")
    spark.sql(f"ALTER TABLE {FACT} ALTER COLUMN unit_price TYPE DECIMAL(12,2)")           # B2B: vece cene

    spark.catalog.refreshTable(FACT)
    print("POSLE:", ", ".join(f"{f.name} {f.dataType.simpleString()}" for f in spark.table(FACT).schema
                              if f.name in ("unit_price", "discount_amount", "discount_pct")))
    snap_after = spark.sql(f"SELECT snapshot_id FROM {FACT}.refs WHERE name = 'main'").first()[0]
    log(f"data fajlova pre/posle: {files_before}/{data_files(spark, FACT)}, snapshot pre/posle: {snap_before}/{snap_after}")
    log("=> promenjeni su samo metapodaci; stari Parquet fajlovi se citaju preko ID-ja kolona, ne imena")
    show(spark, f"SELECT COUNT(*) AS stavki, SUM(discount_amount) AS popust_ukupno, COUNT(discount_pct) AS sa_procentom FROM {FACT}")


def black_friday(spark: SparkSession) -> None:
    t = "lake.raw.clickstream"
    try:
        spark.sql(f"ALTER TABLE {t} REPLACE PARTITION FIELD event_ts_day WITH hours(event_ts) AS event_ts_hour")
        log(f"{t}: particionisanje days(event_ts) -> hours(event_ts)")
    except Exception as e:  # noqa: BLE001
        log(f"particionisanje je vec promenjeno ({str(e).splitlines()[0][:80]})")
    log("postojeci fajlovi ostaju u dnevnim particijama (spec 0), novi idu u satne (spec 1) - bez prepisivanja")
    show(spark, f"SELECT spec_id, COUNT(*) AS particija, SUM(file_count) AS fajlova FROM {t}.partitions GROUP BY spec_id")


# ------------------------------------------------------------------ pogresan deploy i vracanje
REVENUE_BY_CCY = """SELECT currency, CAST(SUM(net_amount_eur) AS DECIMAL(14,2)) AS revenue_eur
                    FROM {t} {ver} GROUP BY currency ORDER BY currency"""


def bad_deploy(spark: SparkSession) -> None:
    spark.sql(f"ALTER TABLE {FACT} CREATE OR REPLACE TAG `pre-deploy`")
    log("tag `pre-deploy` postavljen na trenutni snapshot fact tabele")
    analytics.build_fact(spark, fx_bug=True)
    analytics.build_marts(spark)
    print("\nPrihod u EUR - pre deploy-a:")
    show(spark, REVENUE_BY_CCY.format(t=FACT, ver="VERSION AS OF 'pre-deploy'"))
    print("Prihod u EUR - posle deploy-a (RSD prihod je ~117x manji):")
    show(spark, REVENUE_BY_CCY.format(t=FACT, ver=""))


def rollback(spark: SparkSession) -> None:
    snap = spark.sql(f"SELECT snapshot_id FROM {FACT}.refs WHERE name = 'pre-deploy'").first()
    if not snap:
        raise SystemExit("nema taga pre-deploy - prvo pokreni bad_deploy")
    t0 = time.perf_counter()
    spark.sql(f"CALL lake.system.rollback_to_snapshot('analytics.fact_order_items', {snap[0]})")
    log(f"rollback_to_snapshot({snap[0]}) za {(time.perf_counter() - t0) * 1000:.0f} ms - menja se samo pokazivac")
    analytics.build_marts(spark)
    show(spark, REVENUE_BY_CCY.format(t=FACT, ver=""))
    show(spark, f"SELECT made_current_at, snapshot_id, is_current_ancestor FROM {FACT}.history ORDER BY made_current_at DESC", 4)


def month_close(spark: SparkSession, tag: str = "close-2026-09") -> None:
    for t in (FACT, "lake.analytics.daily_sales", "lake.analytics.payment_reconciliation", "lake.clean.payments"):
        spark.sql(f"ALTER TABLE {t} CREATE OR REPLACE TAG `{tag}` RETAIN 365 DAYS")
        log(f"{t}: tag `{tag}` (cuva se 365 dana, i kada expire_snapshots obrise ostale snimke)")
    print(f"\nSeptembar 2026 - zakljucan broj za finansije (VERSION AS OF '{tag}'):")
    show(spark, f"""SELECT currency, COUNT(*) AS stavki, CAST(SUM(net_amount_eur) AS DECIMAL(14,2)) AS revenue_eur
                    FROM {FACT} VERSION AS OF '{tag}'
                    WHERE order_ts >= '2026-09-01' AND order_ts < '2026-10-01' GROUP BY currency""")
    log("isto u Trino-u:  SELECT ... FROM analytics.fact_order_items FOR VERSION AS OF 'close-2026-09'")


# ------------------------------------------------------------------ GDPR brisanje
def erasure_request(spark: SparkSession, cid: str | None) -> None:
    if not cid:
        cid = str(spark.sql("""SELECT customer_id FROM lake.analytics.customer_360
                               WHERE orders > 0 AND sessions > 0 ORDER BY orders DESC, sessions DESC LIMIT 1""").first()[0])
    cid = int(cid)
    with open("/home/iceberg/results/erasure_customer.txt", "w") as f:
        f.write(str(cid))
    orders = [r[0] for r in spark.sql(f"SELECT order_id FROM lake.raw.orders WHERE customer_id = {cid}").collect()]
    keys = [r[0] for r in spark.sql(f"SELECT customer_key FROM lake.analytics.dim_customer WHERE customer_id = {cid}").collect()]
    in_orders = f"order_id IN ({', '.join(map(str, orders)) or 'NULL'})"
    targets = [
        ("lake.raw.customers", f"customer_id = {cid}"), ("lake.raw.orders", f"customer_id = {cid}"),
        ("lake.raw.order_items", in_orders), ("lake.raw.payments", in_orders),
        ("lake.raw.clickstream", f"customer_id = {cid}"),
        ("lake.clean.orders", f"customer_id = {cid}"), ("lake.clean.order_items", in_orders),
        ("lake.clean.payments", in_orders), ("lake.clean.clickstream", f"customer_id = {cid}"),
        ("lake.analytics.dim_customer", f"customer_id = {cid}"),
        ("lake.analytics.fact_order_items", f"customer_key IN ({', '.join(map(str, keys)) or 'NULL'})"),
        ("lake.analytics.customer_360", f"customer_id = {cid}"),
        ("lake.analytics.payment_reconciliation", in_orders),
        ("lake.ml.customer_segments", f"customer_id = {cid}"),
    ]
    targets = [(t, p) for t, p in targets if exists(spark, t)]
    print(f"\nZahtev za brisanje: kupac {cid} ({len(orders)} porudzbina)")

    before = {}
    for t, pred in targets:
        before[t] = spark.sql(f"SELECT snapshot_id FROM {t}.refs WHERE name = 'main'").first()[0]
        n = spark.sql(f"SELECT COUNT(*) FROM {t} WHERE {pred}").first()[0]
        if n:
            spark.sql(f"DELETE FROM {t} WHERE {pred}")
        log(f"{t}: obrisano {n} redova")
    if exists(spark, "lake.clean.payments"):
        spark.sql("ALTER TABLE lake.clean.payments CREATE OR REPLACE BRANCH audit")   # i grana sme da drzi stare podatke

    t, pred = "lake.raw.customers", f"customer_id = {cid}"
    print("\n1) Posle DELETE - tekuce stanje vs. vremensko putovanje na snimak pre brisanja:")
    show(spark, f"""SELECT 'sada' AS verzija, COUNT(*) AS redova FROM {t} WHERE {pred}
                    UNION ALL SELECT 'VERSION AS OF {before[t]}', COUNT(*) FROM {t} VERSION AS OF {before[t]} WHERE {pred}""")
    log("=> DELETE samo pravi nov snimak; stari fajlovi (i podaci kupca) i dalje postoje")

    print("2) Fizicko uklanjanje: merge-on-read tabele se kompaktuju (delete fajlovi se primene), "
          "pa expire_snapshots brise stare snimke i fajlove koje samo oni koriste")
    now = ts_literal(now_utc())
    for tt, _ in targets:
        name = tt.removeprefix("lake.")
        mor = spark.sql(f"SHOW TBLPROPERTIES {tt} ('write.delete.mode')").first()["value"] == "merge-on-read"
        if mor:
            spark.sql(f"CALL lake.system.rewrite_data_files(table => '{name}', options => map('delete-file-threshold', '1'))")
            spark.sql(f"CALL lake.system.rewrite_position_delete_files(table => '{name}', options => map('rewrite-all', 'true'))")
        r = spark.sql(f"CALL lake.system.expire_snapshots(table => '{name}', older_than => {now}, retain_last => 1)").first()
        log(f"{tt}: expire_snapshots -> obrisano {r['deleted_data_files_count']} data fajlova{' (posle kompakcije)' if mor else ''}")

    print("\n3) Vremensko putovanje na snimak pre brisanja:")
    try:
        spark.sql(f"SELECT COUNT(*) FROM {t} VERSION AS OF {before[t]} WHERE {pred}").collect()
        log("snimak je jos dostupan?!")
    except Exception as e:  # noqa: BLE001
        log(f"vise nije moguce: {str(e).splitlines()[0][:110]}")

    print("\n4) Imenovane reference (tagovi, grane) cuvaju svoje snimke i posle expire_snapshots:")
    for tt, pred in targets:
        for ref in spark.sql(f"SELECT name, type FROM {tt}.refs WHERE name <> 'main'").collect():
            n = spark.sql(f"SELECT COUNT(*) FROM {tt} VERSION AS OF '{ref['name']}' WHERE {pred}").first()[0]
            if n:
                log(f"PAZNJA {tt} @ {ref['type'].lower()} '{ref['name']}': jos {n} redova kupca "
                    f"-> politika brisanja mora da obuhvati i tagove (npr. ALTER TABLE ... DROP TAG)")


# ------------------------------------------------------------------ atomicnost
DEMO = "lake.demo.clickstream_copy"


def atomicity_prepare(spark: SparkSession) -> None:
    spark.sql("CREATE NAMESPACE IF NOT EXISTS lake.demo")
    spark.sql(f"DROP TABLE IF EXISTS {DEMO} PURGE")
    spark.sql(f"CREATE TABLE {DEMO} USING iceberg PARTITIONED BY (days(event_ts)) "
              f"AS SELECT * FROM lake.clean.clickstream LIMIT 10000")
    snap = spark.sql(f"SELECT snapshot_id FROM {DEMO}.refs WHERE name = 'main'").first()[0]
    log(f"{DEMO}: 10000 redova, snapshot {snap}")


def atomicity_write(spark: SparkSession) -> None:
    # namerno dug upis (~30x clickstream) da bi stigao da se prekine
    spark.sql(f"INSERT INTO {DEMO} SELECT c.* FROM lake.clean.clickstream c CROSS JOIN range(30) r")


if __name__ == "__main__":
    name = sys.argv[1]
    spark = get_spark(f"scenario-{name}")
    fn = {"promotions_engine": promotions_engine, "black_friday": black_friday, "bad_deploy": bad_deploy,
          "rollback": rollback, "month_close": month_close, "atomicity_prepare": atomicity_prepare,
          "atomicity_write": atomicity_write}.get(name)
    if name == "erasure_request":
        erasure_request(spark, sys.argv[2] if len(sys.argv) > 2 and sys.argv[2] else None)
    elif fn:
        fn(spark)
    else:
        raise SystemExit(f"nepoznat scenario {name}")
    spark.stop()
