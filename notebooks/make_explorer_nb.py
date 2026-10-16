# Pravi notebooks/explorer.ipynb (pokrece se u explorer kontejneru):  python /app/notebooks/make_explorer_nb.py
# Izvrsavanje: jupyter nbconvert --to notebook --execute --inplace /app/notebooks/explorer.ipynb
import nbformat as nbf
md, code = nbf.v4.new_markdown_cell, nbf.v4.new_code_cell
cells = [
md("""# Iceberg iz cistog Python-a (PyIceberg + DuckDB)

Ovaj notebook ne koristi Spark ni JVM. PyIceberg cita **isti REST katalog** i **iste Parquet fajlove na S3** koje je napisao Spark,
a DuckDB nad njima izvrsava SQL. Ovo je dokaz da je tabela otvoren format, a ne interni format jedne masine."""),
code("""import sys; sys.path.insert(0, "/app/explorer")
import pandas as pd
import lake
pd.set_option("display.max_colwidth", 80)
cat = lake.catalog()
{ns[0]: [t[1] for t in cat.list_tables(ns)] for ns in cat.list_namespaces()}"""),
md("## 1. Tabela = metapodaci\nSema, specifikacija particija, redosled sortiranja i svojstva zapisani su u `metadata.json`, ne u masini za obradu."),
code("""fact = lake.table("analytics.fact_order_items")
print("metadata.json:", fact.metadata_location)
print("format:", fact.format_version)
print(fact.schema())
print("particije:", fact.spec())
print("sortiranje:", fact.sort_order())
{k: v for k, v in fact.properties.items() if k.startswith("write.")}"""),
md("## 2. Snimci (snapshots) i reference (grane, tagovi)\nSvaki upis je novi snimak; tagovi trajno imenuju snimak (npr. zatvaranje meseca, skup za ucenje modela)."),
code("""fact.inspect.snapshots().select(["committed_at", "snapshot_id", "parent_id", "operation"]).to_pandas()"""),
code("""fact.inspect.refs().to_pandas()"""),
md("""## 3. Planiranje upita: odsecanje particija i fajlova (pruning)
Pre citanja ijednog bajta podataka, PyIceberg iz manifesta zna koje fajlove treba procitati:
particija (mesec) i min/max statistike kolona po fajlu."""),
code("""all_files = list(fact.scan().plan_files())
sept = list(fact.scan(row_filter="order_ts >= '2026-09-01T00:00:00+00:00' AND order_ts < '2026-10-01T00:00:00+00:00'").plan_files())
one_day = list(fact.scan(row_filter="order_ts >= '2026-09-15T00:00:00+00:00' AND order_ts < '2026-09-16T00:00:00+00:00'").plan_files())
pd.DataFrame({"upit": ["cela tabela", "septembar", "jedan dan"],
              "fajlova za citanje": [len(all_files), len(sept), len(one_day)],
              "MB": [round(sum(t.file.file_size_in_bytes for t in x) / 1e6, 2) for x in (all_files, sept, one_day)]})"""),
md("## 4. SQL nad Iceberg tabelama u DuckDB-u"),
code("""con = lake.duck({"fact": "analytics.fact_order_items", "product": "analytics.dim_product"})
con.sql(\"\"\"
    SELECT p.category, COUNT(DISTINCT f.order_id) AS orders, ROUND(SUM(f.net_amount_eur), 2) AS revenue_eur
    FROM fact f JOIN product p ON f.product_key = p.product_key
    WHERE f.order_status IN ('paid', 'shipped', 'delivered')
    GROUP BY 1 ORDER BY revenue_eur DESC\"\"\").df()"""),
md("## 5. Time travel\nIsti upit nad prvim snimkom tabele i nad tekucim stanjem."),
code("""first = min(fact.metadata.snapshots, key=lambda s: s.timestamp_ms).snapshot_id
q = "SELECT COUNT(*) AS stavki, ROUND(SUM(net_amount_eur), 2) AS revenue_eur FROM fact"
con_then = lake.duck({"fact": "analytics.fact_order_items"}, ref={"fact": first})
con_now = lake.duck({"fact": "analytics.fact_order_items"})
then = con_then.sql(q).df().assign(verzija=f"snimak {first}")
now = con_now.sql(q).df().assign(verzija="tekuca")
pd.concat([then, now])"""),
md("""## 6. Fajlovi i manifesti
`inspect.files()` je sadrzaj manifesta tekuceg snimka: putanja, broj redova, velicina i statistike kolona."""),
code("""fact.inspect.files().select(["content", "file_path", "record_count", "file_size_in_bytes"]).to_pandas()"""),
code("""fact.inspect.manifests().select(["path", "added_snapshot_id", "added_data_files_count", "existing_data_files_count", "deleted_data_files_count"]).to_pandas()"""),
md("""## 7. Merge-on-read: delete fajlovi
`raw.orders` je merge-on-read: izmena reda ne prepisuje data fajl, vec dodaje mali *position delete* fajl
(\"red 17 u fajlu X vise ne vazi\"). Citalac spaja data i delete fajlove u letu; kompakcija ih kasnije spaja trajno."""),
code("""orders = lake.table("raw.orders")
f = orders.inspect.files().to_pandas()
f["vrsta"] = f.content.map({0: "data", 1: "position deletes", 2: "equality deletes"})
f.groupby("vrsta").agg(fajlova=("file_path", "count"), redova=("record_count", "sum"), bajtova=("file_size_in_bytes", "sum"))"""),
md("## 8. Struktura metapodataka (pandan slici 3.3)\nkatalog -> metadata.json -> snimci -> lista manifesta -> manifesti -> data/delete fajlovi"),
code("""import metadata_graph
metadata_graph.build("raw.orders", 3)"""),
]
nb = nbf.v4.new_notebook(cells=cells, metadata={"kernelspec": {"name": "python3", "display_name": "Python 3", "language": "python"}})
nbf.write(nb, "/app/notebooks/explorer.ipynb")
print("ok")
