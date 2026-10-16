"""
Razdvajanje skladistenja i obrade: Spark je zaustavljen, a isti upit daje isti rezultat u Trino-u i u DuckDB-u
(PyIceberg cita metapodatke iz REST kataloga i Parquet fajlove direktno sa S3).
"""
import os
import time

import trino

import lake

QUERY = """SELECT COUNT(*) AS stavki, CAST(SUM(net_amount_eur) AS DECIMAL(14,2)) AS revenue_eur, SUM(quantity) AS komada
           FROM {t} WHERE order_ts >= TIMESTAMP '2026-09-01 00:00:00 UTC' AND order_ts < TIMESTAMP '2026-10-01 00:00:00 UTC'"""


def trino_query(sql: str):
    conn = trino.dbapi.connect(host=os.environ.get("TRINO_HOST", "trino"), port=int(os.environ.get("TRINO_PORT", 8080)),
                               user="explorer", catalog="lake", schema="analytics")
    cur = conn.cursor()
    cur.execute(sql)
    return cur.fetchall()


def main() -> None:
    t0 = time.perf_counter()
    tr = trino_query(QUERY.format(t="fact_order_items"))[0]
    t_trino = time.perf_counter() - t0

    t0 = time.perf_counter()
    con = lake.duck({"fact": "analytics.fact_order_items"},
                    row_filters={"fact": "order_ts >= '2026-09-01T00:00:00+00:00' AND order_ts < '2026-10-01T00:00:00+00:00'"})
    dk = con.sql(QUERY.format(t="fact").replace(" UTC'", "+00'")).fetchone()
    t_duck = time.perf_counter() - t0

    print("Upit: septembar 2026 iz analytics.fact_order_items\n")
    print(f"  Trino           {tr[0]:>8} stavki  {float(tr[1]):>14,.2f} EUR  {tr[2]:>8} komada   ({t_trino:.2f}s)")
    print(f"  PyIceberg+DuckDB{dk[0]:>8} stavki  {float(dk[1]):>14,.2f} EUR  {dk[2]:>8} komada   ({t_duck:.2f}s)")
    same = (tr[0], float(tr[1]), tr[2]) == (dk[0], float(dk[1]), dk[2])
    print(f"\n  Isti rezultat: {'DA' if same else 'NE'}")
    print("  Spark (masina koja je podatke upisala) za to vreme ne radi; podaci zive u S3 skladistu, ne u masini.")


if __name__ == "__main__":
    main()
