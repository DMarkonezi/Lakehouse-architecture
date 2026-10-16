"""Ponovljivost: isti seed mora dati iste brojeve.  python fingerprint.py <oznaka>"""
import json, os, sys
from pathlib import Path
import trino

DIR = Path("/app/fingerprints")
CHECKS = {  # samo poslovne kolone; tehnicke (_ingested_at) se razlikuju izmedju pokretanja
    "raw.orders": "COUNT(*), SUM(order_id)",
    "raw.payments": "COUNT(*), SUM(amount)",
    "raw.clickstream": "COUNT(*), COUNT(DISTINCT event_id)",
    "clean.payments": "COUNT(*), SUM(amount)",
    "analytics.fact_order_items": "COUNT(*), SUM(net_amount_eur), SUM(quantity)",
    "analytics.daily_sales": "COUNT(*), SUM(revenue_eur)",
}

label = sys.argv[1]
cur = trino.dbapi.connect(host=os.environ.get("TRINO_HOST", "trino"), port=int(os.environ.get("TRINO_PORT", 8080)),
                          user="fingerprint", catalog="lake").cursor()
fp = {}
for t, expr in CHECKS.items():
    cur.execute(f"SELECT {expr} FROM lake.{t}")
    fp[t] = [str(v) for v in cur.fetchone()]
DIR.mkdir(exist_ok=True)
(DIR / f"{label}.json").write_text(json.dumps(fp, indent=2))
for other in sorted(DIR.glob("*.json")):
    if other.stem != label:
        print(f"{label} vs {other.stem}: {'ISTO' if json.loads(other.read_text()) == fp else 'RAZLIKA'}")
