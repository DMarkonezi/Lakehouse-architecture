"""Spaja bench_spark.json i bench_py.json u results/benchmarks.json i citljiv results/benchmarks.md."""
from __future__ import annotations

import json
import platform
from datetime import datetime, timezone
from pathlib import Path

R = Path("/app/results")


def mb(b) -> str:
    return f"{(b or 0) / 1e6:,.2f} MB"


def table(header: list[str], rows: list[list]) -> str:
    out = ["| " + " | ".join(header) + " |", "|" + "|".join("---" for _ in header) + "|"]
    out += ["| " + " | ".join(str(c) for c in r) + " |" for r in rows]
    return "\n".join(out)


def main() -> None:
    sp = json.loads((R / "bench_spark.json").read_text()) if (R / "bench_spark.json").exists() else {}
    py = json.loads((R / "bench_py.json").read_text()) if (R / "bench_py.json").exists() else {}
    meta = {"generated_at": datetime.now(timezone.utc).isoformat(timespec="seconds"), "python": platform.python_version(),
            "note": "vremena su medijana 5 ponavljanja na lokalnom Docker okruzenju; brojevi fajlova i bajtova ne zavise od masine"}
    (R / "benchmarks.json").write_text(json.dumps({"meta": meta, "spark": sp, "python": py}, indent=2))

    md = [f"# Merenja\n\nGenerisano {meta['generated_at']}. {meta['note'][0].upper() + meta['note'][1:]}.\n"]

    if "pruning" in py:
        md.append("## 1. Odsecanje particija i fajlova (pruning)\n\nKoliko fajlova upit planira i koliko bajtova Trino "
                  "zaista procita, u zavisnosti od filtera po vremenu.\n")
        md.append(table(["tabela", "filter", "fajlova (plan / ukupno)", "procitano (Trino)", "redova procitano",
                         "vreme Trino"],
                        [[r["table"], r["filter"], f"{r['files_planned']} / {r['files_total']}",
                          f"{mb(r['trino_physical_input_bytes'])} od {mb(r['bytes_total'])}",
                          f"{r['trino_rows_read']:,}", f"{r['trino_query_s']} s"] for r in py["pruning"]["results"]]))
        md.append("\nEXPLAIN ANALYZE upita za jedan dan: `results/explain_analyze.txt`.\n")

    if "small_files" in sp:
        s = sp["small_files"]
        b, a = s["before"], s["after"]
        md.append(f"## 2. Mali fajlovi i kompakcija\n\n{s['batches']} mikro-serija ({s['rows']:,} redova ukupno), "
                  f"zatim `rewrite_data_files` + `rewrite_manifests` ({s['compaction_s']} s).\n")
        md.append(table(["", "data fajlova", "manifesta", "velicina", "agregatni upit (Spark)"],
                        [["posle 50 mikro-serija", b["data_files"], b["manifests"], mb(b["bytes"]), f"{b['query_s']} s"],
                         ["posle kompakcije", a["data_files"], a["manifests"], mb(a["bytes"]), f"{a['query_s']} s"]]))
        md.append("")

    if "cow_vs_mor" in sp:
        c = sp["cow_vs_mor"]
        md.append(f"## 3. Copy-on-write naspram merge-on-read\n\nTabela od {c['table_rows']:,} redova u "
                  f"{c['table_files']} fajlova; azurira se K rasutih redova (5 puta, razliciti redovi).\n")
        md.append(table(["K redova", "rezim", "vreme azuriranja", "upisano po azuriranju", "data / delete fajlova posle",
                         "citanje posle"],
                        [[f"{r['k_rows']:,}", r["mode"], f"{r['update_s']} s", mb(r["bytes_written_per_update"]),
                          f"{r['data_files']} / {r['delete_files']}", f"{r['read_s_after']} s"] for r in c["results"]]))
        md.append("\nCopy-on-write prepisuje svaki fajl u kome je bar jedan izmenjen red; merge-on-read upisuje samo "
                  "male delete fajlove, a cenu placa citalac (spajanje) dok kompakcija ne \"upece\" izmene.\n")

    if "rollback" in sp:
        md.append("## 4. Rollback ne zavisi od velicine tabele\n")
        md.append(table(["redova", "velicina", "rollback_to_snapshot", "vracanje kopiranjem (CTAS)"],
                        [[f"{r['rows']:,}", mb(r["bytes"]), f"{r['rollback_s']} s", f"{r['restore_by_copy_s']} s"]
                         for r in sp["rollback"]["results"]]))
        md.append("\nRollback samo pomera pokazivac na tekuci snimak u katalogu; nijedan data fajl se ne cita ni pise.\n")

    if "storage" in sp:
        md.append("## 5. Rast skladista sa snimcima i expire_snapshots\n\nTabela od 1.000.000 redova (copy-on-write), "
                  "10 azuriranja po 1% rasutih redova.\n")
        md.append(table(["korak", "tekuca verzija tabele", "ukupno na S3", "data fajlova na S3"],
                        [[s["step"], mb(s["bytes"]), mb(s["s3_bytes"]), s["s3_data_files"]] for s in sp["storage"]["series"]]))
        md.append("\nStari snimci cuvaju stare fajlove (to omogucava time travel); expire_snapshots ih brise.\n")

    if "checksum" in py:
        c = py["checksum"]
        md.append(f"## 6. Interoperabilnost: isti kontrolni zbir iz tri masine\n\nSeptembar iz "
                  f"analytics.fact_order_items, snimak {c['snapshot_id']}.\n")
        md.append(table(["masina", "redova", "prihod EUR", "komada", "zbir order_item_id"],
                        [[k, f"{v['rows']:,}", v["revenue_eur"], f"{v['quantity']:,}", v["id_sum"]]
                         for k, v in c["engines"].items()]))
        md.append(f"\nIdentican rezultat: **{'DA' if c['identical'] else 'NE'}**\n")

    if "v3" in sp or "v3" in py:
        s3, p3 = sp.get("v3", {}), py.get("v3", {})
        md.append("## 7. Format v3\n")
        md.append(table(["provera", "rezultat"],
                        [["Spark 3.5 / Iceberg 1.8.1: CREATE TABLE v3", "da" if s3.get("create") else "ne"],
                         ["Spark 3.5 / Iceberg 1.8.1: INSERT u v3", "da" if s3.get("insert") else f"ne: `{s3.get('error')}`"],
                         ["Trino 470: citanje v3 tabele", "da" if p3.get("trino_read") else f"ne: `{p3.get('error')}`"]]))
        md.append("\nZato sve tabele projekta koriste format v2.\n")

    (R / "benchmarks.md").write_text("\n".join(md), encoding="utf-8")
    print("rezultati: results/benchmarks.md i results/benchmarks.json")


if __name__ == "__main__":
    main()
