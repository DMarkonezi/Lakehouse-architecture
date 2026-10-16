"""Pristup lakehouse-u iz cistog Python-a (PyIceberg + DuckDB), bez Spark-a i bez JVM-a."""
from __future__ import annotations

import os
from functools import lru_cache

import duckdb
import pyarrow.fs as pafs
from pyiceberg.catalog import Catalog, load_catalog
from pyiceberg.table import Table

S3_ENDPOINT = "http://minio:9000"


@lru_cache(maxsize=1)
def catalog() -> Catalog:
    # Isti REST katalog i isto skladiste kao Spark i Trino. Za Amazon S3 dovoljno je ukloniti s3.endpoint.
    return load_catalog("lake", **{
        "type": "rest",
        "uri": "http://iceberg-rest:8181",
        "s3.endpoint": S3_ENDPOINT,
        "s3.access-key-id": os.environ["S3_ACCESS_KEY"],
        "s3.secret-access-key": os.environ["S3_SECRET_KEY"],
        "s3.region": os.environ.get("AWS_REGION", "us-east-1"),
    })


def table(name: str) -> Table:
    return catalog().load_table(name)


def s3() -> pafs.S3FileSystem:
    return pafs.S3FileSystem(endpoint_override=S3_ENDPOINT.removeprefix("http://"), scheme="http",
                             access_key=os.environ["S3_ACCESS_KEY"], secret_key=os.environ["S3_SECRET_KEY"],
                             region=os.environ.get("AWS_REGION", "us-east-1"))


def list_objects(prefix: str) -> dict[str, int]:
    """Svi objekti pod s3://... prefiksom -> velicina u bajtovima."""
    infos = s3().get_file_info(pafs.FileSelector(prefix.removeprefix("s3://"), recursive=True, allow_not_found=True))
    return {"s3://" + i.path: i.size for i in infos if i.type == pafs.FileType.File}


def referenced_files(tbl: Table) -> set[str]:
    """Data i delete fajlovi na koje pokazuje bilo koji snimak tabele (ono sto se NE sme obrisati)."""
    paths = set()
    for snap in tbl.metadata.snapshots:
        for m in snap.manifests(tbl.io):
            for e in m.fetch_manifest_entry(tbl.io, discard_deleted=False):
                paths.add(e.data_file.file_path)
    return paths


def snapshot_id(tbl: Table, ref: str | int | None) -> int | None:
    """'close-2026-09' (tag), 'main' (grana) ili broj snimka -> id snimka."""
    if ref is None:
        return tbl.current_snapshot().snapshot_id if tbl.current_snapshot() else None
    if isinstance(ref, int) or str(ref).lstrip("-").isdigit():
        return int(ref)
    snap = tbl.snapshot_by_name(ref)
    if snap is None:
        raise KeyError(f"{tbl.name()} nema referencu '{ref}'")
    return snap.snapshot_id


def duck(tables: dict[str, str], ref: dict[str, str] | None = None, row_filters: dict[str, str] | None = None
         ) -> duckdb.DuckDBPyConnection:
    """DuckDB konekcija sa Iceberg tabelama registrovanim pod zadatim imenima: {"fact": "analytics.fact_order_items"}."""
    con = duckdb.connect()
    for alias, name in tables.items():
        tbl = table(name)
        kwargs = {}
        if ref and alias in ref:
            kwargs["snapshot_id"] = snapshot_id(tbl, ref[alias])
        if row_filters and alias in row_filters:
            kwargs["row_filter"] = row_filters[alias]
        tbl.scan(**kwargs).to_duckdb(alias, connection=con)
    return con
