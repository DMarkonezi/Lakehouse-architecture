"""
Pokretanje toka podataka u jednoj Spark sesiji:
    spark-submit pipeline.py                         # svi koraci: raw clean payments analytics
    spark-submit pipeline.py --steps analytics       # samo deo
    spark-submit pipeline.py --steps analytics --fx-bug   # scenario bad_deploy
"""
import argparse

import analytics
import clean
import raw_ingest
import wap_payments
from common import Timer, get_spark

STEPS = ["raw", "clean", "payments", "analytics"]

if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--steps", default=",".join(STEPS))
    ap.add_argument("--fx-bug", action="store_true")
    args = ap.parse_args()
    steps = args.steps.split(",")

    spark = get_spark("pipeline")
    with Timer("pipeline"):
        if "raw" in steps:
            with Timer("RAW - ucitavanje iz izvora"):
                raw_ingest.run(spark)
        if "clean" in steps:
            with Timer("CLEAN - ciscenje"):
                clean.run(spark)
        if "payments" in steps:
            with Timer("CLEAN.PAYMENTS - upis-provera-objava (WAP)"):
                wap_payments.run(spark)
        if "analytics" in steps:
            with Timer("ANALYTICS - dimenzije, fact, izvestaji"):
                analytics.run(spark, fx_bug=args.fx_bug)
    spark.stop()
