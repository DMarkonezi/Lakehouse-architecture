"""
Merenja (faza 4), deo bez Spark-a: PyIceberg, DuckDB i Trino.

  pruning   koliko fajlova i bajtova upit zaista cita: PyIceberg plan_files (pre citanja ijednog bajta podataka)
            i Trino statistika izvrsenja (physicalInputBytes, processedRows); vreme je medijana 5 ponavljanja.
            EXPLAIN ANALYZE upita za jedan dan se cuva u results/explain_analyze.txt
  checksum  isti kontrolni zbir septembra iz Trino-a i DuckDB-a, poredjen sa Spark-ovim (bench_spark.json)
  v3        da li Trino 470 cita tabelu formata v3

    python bench_py.py [deo ...]
"""
from __future__ import annotations

import json
import os
import statistics
import sys
import time
from pathlib import Path

import trino

import lake

RESULTS = Path("/app/results")
OUT = RESULTS / "bench_py.json"
RUNS = 5

FILTERS = {  # (opis, PyIceberg filter, Trino uslov)
    "cela tabela": (None, "TRUE"),
    "jedan mesec (sept.)": ("{c} >= '2026-09-01T00:00:00+00:00' AND {c} < '2026-10-01T00:00:00+00:00'",
                            "{c} >= TIMESTAMP '2026-09-01 00:00:00 UTC' AND {c} < TIMESTAMP '2026-10-01 00:00:00 UTC'"),
    "jedan dan (15.9.)": ("{c} >= '2026-09-15T00:00:00+00:00' AND {c} < '2026-09-16T00:00:00+00:00'",
                          "{c} >= TIMESTAMP '2026-09-15 00:00:00 UTC' AND {c} < TIMESTAMP '2026-09-16 00:00:00 UTC'"),
}
TABLES = {  # tabela -> (kolona vremena, opis rasporeda)
    "raw.clickstream": ("event_ts", "particionisana po danu (days(event_ts))"),
    "analytics.fact_order_items": ("order_ts", "particionisana po mesecu, sortirana po order_ts"),
}


def cursor():
    return trino.dbapi.connect(host=os.environ.get("TRINO_HOST", "trino"), port=int(os.environ.get("TRINO_PORT", 8080)),
                               user="bench", catalog="lake").cursor()


def trino_run(sql: str) -> tuple[list, dict]:
    cur = cursor()
    cur.execute(sql)
    rows = cur.fetchall()
    return rows, cur.stats


def pruning() -> dict:
    out = []
    for name, (col, layout) in TABLES.items():
        tbl = lake.table(name)
        total = list(tbl.scan().plan_files())
        total_bytes = sum(t.file.file_size_in_bytes for t in total)
        for label, (py_f, trino_f) in FILTERS.items():
            tasks = list(tbl.scan(row_filter=py_f.format(c=col)).plan_files() if py_f else total)
            sql = f"SELECT COUNT(*), COUNT(DISTINCT {'session_id' if 'click' in name else 'order_id'}) FROM lake.{name} " \
                  f"WHERE {trino_f.format(c=col)}"
            times, stats = [], {}
            for _ in range(RUNS):
                t0 = time.perf_counter()
                rows, stats = trino_run(sql)
                times.append(time.perf_counter() - t0)
            row = {"table": name, "layout": layout, "filter": label,
                   "files_total": len(total), "bytes_total": total_bytes,
                   "files_planned": len(tasks), "bytes_planned": sum(t.file.file_size_in_bytes for t in tasks),
                   "delete_files_planned": sum(len(t.delete_files) for t in tasks),
                   "trino_rows_read": stats.get("processedRows"), "trino_physical_input_bytes": stats.get("physicalInputBytes"),
                   "trino_splits": stats.get("totalSplits"), "result_rows": rows[0][0],
                   "trino_query_s": round(statistics.median(times), 3)}
            print(row)
            out.append(row)
    col = TABLES["raw.clickstream"][0]
    plan = trino_run(f"EXPLAIN ANALYZE SELECT COUNT(*) FROM lake.raw.clickstream "
                     f"WHERE {FILTERS['jedan dan (15.9.)'][1].format(c=col)}")[0][0][0]
    (RESULTS / "explain_analyze.txt").write_text(plan)
    return {"results": out}


CHECK_TRINO = """SELECT COUNT(*), CAST(SUM(net_amount_eur) AS DECIMAL(18,2)), SUM(quantity), SUM(order_item_id)
                 FROM lake.analytics.fact_order_items FOR VERSION AS OF {sid}
                 WHERE order_ts >= TIMESTAMP '2026-09-01 00:00:00 UTC' AND order_ts < TIMESTAMP '2026-10-01 00:00:00 UTC'"""
CHECK_DUCK = """SELECT COUNT(*), CAST(SUM(net_amount_eur) AS DECIMAL(18,2)), SUM(quantity), SUM(order_item_id) FROM fact"""


def checksum() -> dict:
    spark_file = RESULTS / "bench_spark.json"
    spark = json.loads(spark_file.read_text()).get("checksum") if spark_file.exists() else None
    sid = int(spark["snapshot_id"]) if spark else lake.table("analytics.fact_order_items").current_snapshot().snapshot_id
    t = trino_run(CHECK_TRINO.format(sid=sid))[0][0]
    con = lake.duck({"fact": "analytics.fact_order_items"}, ref={"fact": sid},
                    row_filters={"fact": "order_ts >= '2026-09-01T00:00:00+00:00' AND order_ts < '2026-10-01T00:00:00+00:00'"})
    d = con.sql(CHECK_DUCK).fetchone()
    norm = lambda r: {"rows": int(r[0]), "revenue_eur": f"{float(r[1]):.2f}", "quantity": int(r[2]), "id_sum": int(r[3])}  # noqa: E731
    engines = {"Trino 470": norm(t), "PyIceberg + DuckDB": norm(d)}
    if spark:
        engines = {"Spark 3.5 (Iceberg 1.8.1)": norm((spark["rows"], spark["revenue_eur"], spark["quantity"], spark["id_sum"])),
                   **engines}
    same = len({json.dumps(v, sort_keys=True) for v in engines.values()}) == 1
    res = {"snapshot_id": sid, "engines": engines, "identical": same}
    print(res)
    return res


def v3() -> dict:
    try:
        rows, _ = trino_run("SELECT COUNT(*) FROM lake.bench.v3_probe")
        res = {"trino_read": True, "rows": rows[0][0], "error": None,
               "note": "tabela je prazna jer Spark/Iceberg 1.8.1 ne moze da potvrdi upis u v3"}
    except Exception as e:
        res = {"trino_read": False, "error": str(e)[:300]}
    print(res)
    return res


PARTS = {"pruning": pruning, "checksum": checksum, "v3": v3}


def main() -> None:
    parts = sys.argv[1:] or list(PARTS)
    results = json.loads(OUT.read_text()) if OUT.exists() else {}
    for p in parts:
        print(f"=== {p}")
        results[p] = PARTS[p]()
        OUT.write_text(json.dumps(results, indent=2, default=str))
    print(f"rezultati: results/{OUT.name}")


if __name__ == "__main__":
    main()
