"""
Stvarna struktura metapodataka Iceberg tabele (pandan slici 3.3), procitana preko PyIceberg-a:

  katalog -> metadata.json -> snimci -> lista manifesta -> manifesti -> data / delete fajlovi

    python metadata_graph.py analytics.fact_order_items [broj_snimaka]
Rezultat: results/figures/metadata_<tabela>.svg i .png
Manifest na koji pokazuje vise lista manifesta je ponovo iskoriscen izmedju snimaka (nije prepisan).
"""
from __future__ import annotations

import sys
from pathlib import Path

import graphviz

import lake

OUT = Path("/app/results/figures")
COLORS = {"catalog": "#e8eef9", "meta": "#dbe7f7", "snap": "#fdf0d5", "current": "#f6c56b", "list": "#efe3f7",
          "manifest": "#e0f2e9", "deletes": "#fbe0e0", "data": "#ffffff", "delfile": "#fdeaea", "ref": "#d9f0f5"}


def short(path: str, n: int = 22) -> str:
    name = path.rsplit("/", 1)[-1]
    return name if len(name) <= n else name[:n - 9] + "…" + name[-8:]


def partition_label(tbl, spec_id: int, partition) -> str:
    """Record[order_ts_month=680] -> 'order_ts_month=2026-09' (vrednost transformacije u citljivom obliku)."""
    spec = tbl.specs().get(spec_id)
    if spec is None or not spec.fields or partition is None:
        return ""
    out = []
    for f in spec.fields:
        v = getattr(partition, f.name, None)
        try:
            v = f.transform.to_human_string(tbl.schema().find_type(f.source_id), v)
        except Exception:
            pass
        out.append(f"{f.name}={v}")
    return ", ".join(out)


def build(name: str, max_snapshots: int = 3, files_per_manifest: int = 3) -> graphviz.Digraph:
    tbl = lake.table(name)
    md = tbl.metadata
    g = graphviz.Digraph(name, graph_attr={"rankdir": "LR", "fontname": "Helvetica", "fontsize": "11", "nodesep": "0.25",
                                           "ranksep": "0.6", "label": f"Iceberg tabela {name}", "labelloc": "t"},
                         node_attr={"shape": "box", "style": "rounded,filled", "fontname": "Helvetica", "fontsize": "9"},
                         edge_attr={"fontname": "Helvetica", "fontsize": "8", "color": "#555555"})

    g.node("catalog", f"REST katalog\n{name}", fillcolor=COLORS["catalog"], shape="cylinder")
    g.node("meta", f"{short(tbl.metadata_location, 30)}\nformat v{md.format_version} | sema {md.current_schema_id} | "
                   f"particija spec {md.default_spec_id}\nsnimaka ukupno: {len(md.snapshots)}", fillcolor=COLORS["meta"])
    g.edge("catalog", "meta", label="pokazivac na\ntekuci metadata.json", penwidth="2")

    current = md.current_snapshot_id
    snaps = sorted(md.snapshots, key=lambda s: s.timestamp_ms)[-max_snapshots:]
    shown = {s.snapshot_id for s in snaps}
    seen_manifests: set[str] = set()
    for s in snaps:
        sid = f"s{s.snapshot_id}"
        summ = s.summary
        added = (summ.get("added-records") or "0") if summ else "?"
        deleted = (summ.get("deleted-records") or "0") if summ else "?"
        pos_del = (summ.get("added-position-deletes") or "0") if summ else "0"
        extra = f"\n{pos_del} position delete-ova (merge-on-read)" if pos_del != "0" else ""
        g.node(sid, f"snimak {s.snapshot_id}\n{summ.operation.value if summ else ''}  +{added} / -{deleted} redova{extra}",
               fillcolor=COLORS["current" if s.snapshot_id == current else "snap"],
               penwidth="2" if s.snapshot_id == current else "1")
        g.edge("meta", sid, style="bold" if s.snapshot_id == current else "solid",
               label="current-snapshot-id" if s.snapshot_id == current else "")
        if s.parent_snapshot_id in shown:
            g.edge(sid, f"s{s.parent_snapshot_id}", style="dashed", label="parent", constraint="false")

        lid = f"l{s.snapshot_id}"
        g.node(lid, f"lista manifesta\n{short(s.manifest_list)}", fillcolor=COLORS["list"], shape="note")
        g.edge(sid, lid)
        for m in s.manifests(tbl.io):
            mid = "m" + str(abs(hash(m.manifest_path)))
            is_del = m.content.name == "DELETES"
            if m.manifest_path not in seen_manifests:
                seen_manifests.add(m.manifest_path)
                entries = m.fetch_manifest_entry(tbl.io, discard_deleted=False)
                live = [e for e in entries if e.status.name != "DELETED"]
                removed = len(entries) - len(live)   # copy-on-write: zapis da je fajl uklonjen iz tabele
                g.node(mid, f"manifest ({'delete' if is_del else 'data'})\n{short(m.manifest_path)}\n"
                            f"{len(live)} fajlova" + (f", {removed} uklonjen" if removed else "")
                            + f", napisan u snimku ..{str(m.added_snapshot_id)[-5:]}",
                       fillcolor=COLORS["deletes" if is_del else "manifest"])
                for e in live[:files_per_manifest]:
                    fid = "f" + str(abs(hash(e.data_file.file_path)))
                    part = partition_label(tbl, m.partition_spec_id, e.data_file.partition)
                    g.node(fid, f"{short(e.data_file.file_path)}\n{e.data_file.record_count} redova"
                                + (f" | particija {part}" if part else ""),
                           fillcolor=COLORS["delfile" if is_del else "data"], shape="component" if is_del else "box",
                           fontsize="8")
                    g.edge(mid, fid)
                if len(live) > files_per_manifest:
                    more = mid + "_more"
                    g.node(more, f"... jos {len(live) - files_per_manifest}", shape="plaintext", style="", fontsize="8")
                    g.edge(mid, more, style="dotted")
            g.edge(lid, mid)

    for ref_name, ref in md.refs.items():
        if ref_name != "main" and ref.snapshot_id in shown:
            rid = f"r{ref_name}"
            g.node(rid, f"{ref.snapshot_ref_type.value}: {ref_name}", shape="cds", fillcolor=COLORS["ref"])
            g.edge(rid, f"s{ref.snapshot_id}", style="dotted")
    return g


def main() -> None:
    name = sys.argv[1] if len(sys.argv) > 1 else "analytics.fact_order_items"
    n = int(sys.argv[2]) if len(sys.argv) > 2 else 3
    OUT.mkdir(parents=True, exist_ok=True)
    g = build(name, n)
    base = OUT / f"metadata_{name.replace('.', '_')}"
    for fmt in ("svg", "png"):
        g.render(base, format=fmt, cleanup=True)
    print(f"struktura metapodataka: results/figures/{base.name}.svg / .png")


if __name__ == "__main__":
    main()
