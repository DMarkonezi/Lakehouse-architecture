"""
ML segmentacija kupaca (RFM + k-means) u cistom Python-u, nad zamrznutim snimkom.

  1. tag ml-train-<datum> na fact i dim tabeli -> skup za ucenje je tacno odredjen i ponovljiv
     (i mesecima kasnije: SELECT ... VERSION AS OF 'ml-train-...')
  2. RFM (recency, frequency, monetary) racuna DuckDB nad PyIceberg skeniranjem tog taga
  3. k-means (k=4), klasteri se imenuju po proseku RFM-a
  4. rezultat se upisuje u lake.ml.customer_segments preko PyIceberg-a (bez Spark-a);
     snimak nosi svojstvo ml.training-tag, pa se iz tabele zna na kojim podacima je model ucen
"""
from __future__ import annotations

import warnings
from datetime import datetime, timezone

import numpy as np
import pyarrow as pa
from sklearn.cluster import KMeans
from sklearn.preprocessing import StandardScaler

import lake

FACT, DIM, OUT = "analytics.fact_order_items", "analytics.dim_customer", "ml.customer_segments"
warnings.filterwarnings("ignore", message="Delete operation did not match any records")
NAMES = ["champions", "loyal", "promising", "at_risk"]

RFM_SQL = """
    WITH s AS (
        SELECT d.customer_id, MAX(f.order_ts) AS last_order_ts, COUNT(DISTINCT f.order_id) AS frequency,
               SUM(f.net_amount_eur)::DOUBLE AS monetary
        FROM fact f JOIN dim d ON f.customer_key = d.customer_key
        WHERE f.order_status IN ('paid', 'shipped', 'delivered')
        GROUP BY d.customer_id)
    SELECT customer_id, date_diff('day', last_order_ts, (SELECT MAX(last_order_ts) FROM s)) AS recency_days,
           frequency, monetary
    FROM s ORDER BY customer_id"""


def tag(name: str, tag_name: str) -> int:
    tbl = lake.table(name)
    sid = tbl.current_snapshot().snapshot_id
    existing = tbl.metadata.refs.get(tag_name)
    if existing is None:
        tbl.manage_snapshots().create_tag(sid, tag_name).commit()
    elif existing.snapshot_id != sid:
        tbl.manage_snapshots().remove_tag(tag_name).create_tag(sid, tag_name).commit()
    return sid


def main() -> None:
    now = datetime.now(timezone.utc)
    tag_name = f"ml-train-{now:%Y%m%d}"
    for n in (FACT, DIM):
        print(f"tag {tag_name} -> {n} snimak {tag(n, tag_name)}")

    con = lake.duck({"fact": FACT, "dim": DIM}, ref={"fact": tag_name, "dim": tag_name})
    rfm = con.sql(RFM_SQL).df()
    print(f"RFM za {len(rfm)} kupaca sa bar jednom placenom porudzbinom")

    x = np.column_stack([rfm.recency_days, np.log1p(rfm.frequency), np.log1p(rfm.monetary)])
    x = StandardScaler().fit_transform(x)
    km = KMeans(n_clusters=4, n_init=10, random_state=42).fit(x)
    rfm["cluster"] = km.labels_

    # Ime klastera: sto skoriji, cesci i vredniji kupci -> bolji segment
    score = x[:, 1] + x[:, 2] - x[:, 0]
    order = sorted(range(4), key=lambda c: -score[km.labels_ == c].mean())
    rfm["segment_name"] = rfm.cluster.map({c: NAMES[i] for i, c in enumerate(order)})

    data = pa.table({
        "customer_id": pa.array(rfm.customer_id, pa.int32()),
        "recency_days": pa.array(rfm.recency_days, pa.int32()),
        "frequency": pa.array(rfm.frequency, pa.int32()),
        "monetary_eur": pa.array(rfm.monetary.round(2), pa.float64()),
        "segment_name": pa.array(rfm.segment_name, pa.string()),
        "training_tag": pa.array([tag_name] * len(rfm), pa.string()),
        "scored_at": pa.array([now] * len(rfm), pa.timestamp("us", tz="UTC")),
    })
    cat = lake.catalog()
    cat.create_namespace_if_not_exists("ml")
    tbl = cat.load_table(OUT) if cat.table_exists(OUT) else cat.create_table(OUT, data.schema)
    tbl.overwrite(data, snapshot_properties={"ml.training-tag": tag_name, "ml.model": "kmeans-k4-rfm"})
    print(f"lake.{OUT}: {len(data)} redova, snimak {tbl.current_snapshot().snapshot_id}\n")

    summary = (rfm.groupby("segment_name")
               .agg(kupaca=("customer_id", "count"), recency=("recency_days", "median"),
                    porudzbina=("frequency", "mean"), eur=("monetary", "mean"))
               .reindex(NAMES).round(1))
    print(summary.to_string())


if __name__ == "__main__":
    main()
