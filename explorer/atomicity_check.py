"""
Atomicnost: Spark se ubije (docker kill) usred upisa u lake.demo.clickstream_copy.

  wait   - ceka da Spark pocne da pise data fajlove (fajlovi postoje na S3, ali jos nisu potvrdjeni)
  verify - posle prekida: tabela je i dalje na prethodnom snimku, a napisani fajlovi su "siroce"
           (postoje na S3, ali ih nijedan snimak ne referencira, pa ih citaoci nikada ne vide)
"""
import json
import sys
import time
from pathlib import Path

import lake

NAME = "demo.clickstream_copy"
STATE = Path("/app/results/atomicity_state.json")


def wait() -> None:
    tbl = lake.table(NAME)
    snap = tbl.current_snapshot()
    referenced = lake.referenced_files(tbl)
    STATE.write_text(json.dumps({"snapshot_id": snap.snapshot_id, "records": int(snap.summary["total-records"])}))
    print(f"pre upisa: snimak {snap.snapshot_id}, {snap.summary['total-records']} redova, {len(referenced)} data fajlova")
    t0 = time.time()
    while time.time() - t0 < 300:
        new = [p for p in lake.list_objects(f"{tbl.location()}/data") if p not in referenced]
        if len(new) >= 3:
            print(f"Spark je poceo upis: {len(new)} novih data fajlova na S3 (jos nepotvrdjenih)")
            return
        time.sleep(0.5)
    sys.exit("Spark nije poceo upis u roku od 5 minuta")


def verify() -> None:
    before = json.loads(STATE.read_text())
    tbl = lake.table(NAME)
    snap = tbl.current_snapshot()
    rows = tbl.scan().count() if hasattr(tbl.scan(), "count") else len(tbl.scan().to_arrow())
    print(f"posle prekida: snimak {snap.snapshot_id} ({'ISTI' if snap.snapshot_id == before['snapshot_id'] else 'PROMENJEN'}), "
          f"{rows} redova (pre: {before['records']})")
    referenced = lake.referenced_files(tbl)
    listing = lake.list_objects(f"{tbl.location()}/data")
    orphans = {p: s for p, s in listing.items() if p not in referenced}
    print(f"na S3 je {len(listing)} data fajlova, snimci referenciraju {len(referenced)}, "
          f"siroce: {len(orphans)} fajlova ({sum(orphans.values()) / 1e6:.1f} MB)")
    for p in list(orphans)[:3]:
        print("   ", p.split("/data/")[-1])
    print("=> citaoci (Trino, DuckDB, Spark) vide samo potvrdjeni snimak; polovicnog upisa nema.\n"
          "=> siroce uklanja remove_orphan_files, ali tek kada su starije od zadatog praga (podrazumevano 3 dana;\n"
          "   Iceberg odbija prag kraci od 24h jer bi mogao da obrise fajlove upisa koji je upravo u toku).")


if __name__ == "__main__":
    {"wait": wait, "verify": verify}[sys.argv[1]]()
