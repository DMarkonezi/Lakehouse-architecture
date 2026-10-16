"""
Konflikt konkurentnih upisa (slika 3.6 uzivo).

Dva klijenta ucitaju tabelu u istom trenutku, tj. oba vide isti snimak S0.
  1. klijent A upise red -> katalog atomicno pomeri pokazivac S0 -> S1
  2. klijent B pokusa da upise na osnovu S0 -> katalog odbija (ocekivao je S0, a tekuci je S1)
  3. B osvezi tabelu (vidi S1) i ponovi upis -> prolazi kao S2
Katalog prihvata potvrdu samo ako se pretpostavljeni tekuci snimak poklapa (optimisticka konkurentnost).
"""
from datetime import datetime, timezone

import pandas as pd
import pyarrow as pa
from pyiceberg.exceptions import CommitFailedException
from pyiceberg.schema import Schema
from pyiceberg.types import NestedField, StringType, TimestamptzType

import lake

NAME = "demo.concurrent_writes"
SCHEMA = Schema(NestedField(1, "writer", StringType(), required=False),
                NestedField(2, "written_at", TimestamptzType(), required=False))


def row(writer: str) -> pa.Table:
    return pa.table({"writer": [writer], "written_at": [datetime.now(timezone.utc)]},
                    schema=pa.schema([("writer", pa.string()), ("written_at", pa.timestamp("us", tz="UTC"))]))


def main() -> None:
    cat = lake.catalog()
    cat.create_namespace_if_not_exists("demo")
    if cat.table_exists(NAME):
        cat.drop_table(NAME)
    cat.create_table(NAME, SCHEMA).append(row("init"))

    a = cat.load_table(NAME)
    b = cat.load_table(NAME)
    s0 = a.current_snapshot().snapshot_id
    print(f"A i B su ucitali tabelu; oba vide snimak S0 = {s0}")

    a.append(row("A"))
    print(f"A: upis prihvacen  -> S1 = {a.current_snapshot().snapshot_id}")

    try:
        b.append(row("B"))
        print("B: upis prihvacen bez greske (ova verzija klijenta sama ponavlja potvrdu)")
    except CommitFailedException as e:
        print(f"B: upis ODBIJEN    -> {type(e).__name__}: {str(e)[:160]}")
        b.refresh()
        print(f"B: osvezio tabelu, sada vidi S1 = {b.current_snapshot().snapshot_id}")
        b.append(row("B"))
        print(f"B: ponovljen upis prihvacen -> S2 = {b.current_snapshot().snapshot_id}")

    print("\nIstorija tabele (snimci se nadovezuju, nijedan upis nije izgubljen):")
    snaps = (b.inspect.snapshots().select(["committed_at", "snapshot_id", "parent_id", "operation"])
             .to_pandas(types_mapper={pa.int64(): pd.Int64Dtype()}.get))   # bez prolaska kroz float (gubi cifre)
    print(snaps.to_string(index=False))
    print("\nSadrzaj:", sorted(b.scan().to_arrow().column("writer").to_pylist()))


if __name__ == "__main__":
    main()
