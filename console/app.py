"""
Konzola lakehouse-a: poslovni prikazi + pogled "ispod haube" Iceberg tabela.

Sve se cita preko Trino-a (korisnik samo za citanje: Trino-ov S3 nalog ima readonly politiku u MinIO-u),
osim grafa metapodataka koji PyIceberg cita direktno iz kataloga i S3 skladista (isti readonly nalog).
Konzola ne zna nista o Spark-u: radi i kada je Spark zaustavljen.
"""
from __future__ import annotations

import os
import sys

import altair as alt
import pandas as pd
import streamlit as st
import trino

sys.path.insert(0, "/app/explorer")

st.set_page_config(page_title="Lakehouse konzola", page_icon="🧊", layout="wide")

TRINO = dict(host=os.environ.get("TRINO_HOST", "trino"), port=int(os.environ.get("TRINO_PORT", 8080)),
             user="console", catalog="lake")
PAID = "('paid', 'shipped', 'delivered')"


# ------------------------------------------------------------------ pristup podacima
@st.cache_data(ttl=30, show_spinner=False)
def q(sql: str) -> pd.DataFrame:
    with trino.dbapi.connect(**TRINO) as conn:
        cur = conn.cursor()
        cur.execute(sql)
        rows = cur.fetchall()
        return pd.DataFrame(rows, columns=[d[0] for d in cur.description])


def safe(sql: str) -> pd.DataFrame | None:
    try:
        return q(sql)
    except Exception as e:  # tabela jos ne postoji (scenario nije pokrenut) ili Trino nije dostupan
        st.info(f"Nema podataka: {str(e).split('message=')[-1][:200]}")
        return None


def tables() -> pd.DataFrame:
    return q("""SELECT table_schema, table_name FROM lake.information_schema.tables
                WHERE table_schema NOT IN ('information_schema') AND table_type = 'BASE TABLE'
                ORDER BY 1, 2""")


def has_column(schema: str, table: str, column: str) -> bool:
    return not q(f"""SELECT 1 FROM lake.information_schema.columns
                     WHERE table_schema = '{schema}' AND table_name = '{table}' AND column_name = '{column}'""").empty


def version_picker(table: str, key: str) -> str:
    """Izbor verzije tabele (grane i tagovi) -> SQL dodatak za time travel."""
    refs = safe(f'SELECT name, type, snapshot_id FROM lake.{table.split(".")[0]}."{table.split(".")[1]}$refs" ORDER BY type, name')
    if refs is None or len(refs) <= 1:
        return ""
    labels = ["tekuca (main)"] + [f"{r.type.lower()}: {r.name}" for r in refs.itertuples() if r.name != "main"]
    choice = st.selectbox("Verzija podataka (time travel)", labels, key=key)
    if choice.startswith("tekuca"):
        return ""
    return f" FOR VERSION AS OF '{choice.split(': ', 1)[1]}'"


def money(x) -> str:
    return f"{float(x or 0):,.0f} €"


# ------------------------------------------------------------------ stranice
def page_sales() -> None:
    st.header("Prodaja")
    v = version_picker("analytics.daily_sales", "sales_ver")
    df = safe(f"SELECT * FROM lake.analytics.daily_sales{v}")
    if df is None or df.empty:
        return
    df["revenue_eur"] = df.revenue_eur.astype(float)
    c1, c2, c3 = st.columns(3)
    countries = c1.multiselect("Zemlja", sorted(df.country.unique()), default=sorted(df.country.unique()))
    channels = c2.multiselect("Kanal", sorted(df.sales_channel.unique()), default=sorted(df.sales_channel.unique()))
    lo, hi = df.sales_date.min(), df.sales_date.max()
    rng = c3.date_input("Period", (lo, hi), min_value=lo, max_value=hi)
    d = df[df.country.isin(countries) & df.sales_channel.isin(channels)]
    if isinstance(rng, tuple) and len(rng) == 2:
        d = d[(d.sales_date >= rng[0]) & (d.sales_date <= rng[1])]

    k1, k2, k3, k4 = st.columns(4)
    k1.metric("Prihod", money(d.revenue_eur.sum()))
    k2.metric("Porudzbine", f"{d.orders.sum():,}")
    k3.metric("Komada", f"{d['items'].sum():,}")
    k4.metric("Prosecna porudzbina", money(d.revenue_eur.sum() / max(d.orders.sum(), 1)))

    daily = d.groupby(["sales_date", "sales_channel"], as_index=False).revenue_eur.sum()
    st.altair_chart(alt.Chart(daily).mark_line(point=True).encode(
        x=alt.X("sales_date:T", title=None), y=alt.Y("revenue_eur:Q", title="Prihod (EUR)"),
        color=alt.Color("sales_channel:N", title="Kanal"), tooltip=["sales_date:T", "sales_channel", "revenue_eur:Q"]),
        use_container_width=True)
    a, b = st.columns(2)
    cat = d.groupby("category", as_index=False).revenue_eur.sum().sort_values("revenue_eur", ascending=False)
    a.altair_chart(alt.Chart(cat).mark_bar().encode(x=alt.X("revenue_eur:Q", title="Prihod (EUR)"),
                                                    y=alt.Y("category:N", sort="-x", title=None)), use_container_width=True)
    ctry = d.groupby("country", as_index=False).agg(orders=("orders", "sum"), revenue_eur=("revenue_eur", "sum"))
    b.dataframe(ctry.sort_values("revenue_eur", ascending=False), hide_index=True, use_container_width=True)


def page_funnel() -> None:
    st.header("Konverzija")
    df = safe("SELECT * FROM lake.analytics.conversion_funnel")
    if df is None or df.empty:
        return
    steps = ["sessions", "product_view", "add_to_cart", "begin_checkout", "purchase"]
    tot = df.groupby("channel")[steps].sum().reset_index().melt("channel", var_name="step", value_name="sessions_n")
    st.altair_chart(alt.Chart(tot).mark_bar().encode(
        x=alt.X("step:N", sort=steps, title=None), y=alt.Y("sessions_n:Q", title="Sesija"),
        color="channel:N", xOffset="channel:N", tooltip=["channel", "step", "sessions_n"]), use_container_width=True)

    df["conversion_pct"] = 100 * df.purchase / df.sessions.where(df.sessions > 0)
    st.subheader("Stopa konverzije po danu")
    st.altair_chart(alt.Chart(df).mark_line(point=True).encode(
        x=alt.X("event_date:T", title=None), y=alt.Y("conversion_pct:Q", title="% sesija sa kupovinom"),
        color="channel:N"), use_container_width=True)

    st.subheader("A/B eksperiment checkout_v2 (aplikacija 3.0)")
    if not has_column("clean", "clickstream", "experiment"):
        st.caption("Kolona experiment jos ne postoji: pojavljuje se tek kad aplikacija 3.0 pocne da je salje "
                   "(scenario app_release_3); sema tabele se tada prosiruje bez prepisivanja podataka.")
        return
    ab = safe("""
        SELECT experiment.variant AS variant, COUNT(DISTINCT session_id) AS sessions,
               COUNT(DISTINCT CASE WHEN event_type = 'purchase' THEN session_id END) AS purchases
        FROM lake.clean.clickstream WHERE experiment IS NOT NULL GROUP BY 1 ORDER BY 1""")
    if ab is not None and not ab.empty:
        ab["conversion_pct"] = (100 * ab.purchases / ab.sessions).round(2)
        st.dataframe(ab, hide_index=True)


def page_finance() -> None:
    st.header("Finansije: uparivanje uplata")
    v = version_picker("analytics.payment_reconciliation", "fin_ver")
    df = safe(f"SELECT * FROM lake.analytics.payment_reconciliation{v}")
    if df is None or df.empty:
        return
    for c in ("order_total", "captured", "refunded", "chargeback", "difference"):
        df[c] = pd.to_numeric(df[c], errors="coerce")
    s = df.groupby("recon_status", as_index=False).agg(orders=("order_id", "count"), amount=("order_total", "sum"))
    a, b = st.columns([1, 2])
    a.dataframe(s, hide_index=True, use_container_width=True)
    b.altair_chart(alt.Chart(s).mark_arc(innerRadius=50).encode(theta="orders:Q", color="recon_status:N",
                                                                tooltip=["recon_status", "orders"]), use_container_width=True)
    st.subheader("Promet po valuti (originalni iznosi)")
    st.dataframe(df.groupby(["currency", "method"], as_index=False).captured.sum(), hide_index=True)
    st.subheader("Odstupanja")
    bad = df[df.recon_status.isin(["amount_mismatch", "missing_payment", "refund_pending"])]
    st.dataframe(bad.sort_values("difference", key=abs, ascending=False).head(200), hide_index=True,
                 use_container_width=True)


def page_customers() -> None:
    st.header("Kupci")
    df = safe("SELECT * FROM lake.analytics.customer_360")
    if df is None or df.empty:
        return
    df["revenue_eur"] = df.revenue_eur.astype(float)
    if df.segment_name.notna().any():
        seg = df.groupby("segment_name", as_index=False).agg(kupaca=("customer_id", "count"),
                                                              prihod=("revenue_eur", "sum"))
        a, b = st.columns(2)
        a.altair_chart(alt.Chart(seg).mark_bar().encode(x="kupaca:Q", y=alt.Y("segment_name:N", sort="-x", title=None)),
                       use_container_width=True)
        b.altair_chart(alt.Chart(seg).mark_bar().encode(x="prihod:Q", y=alt.Y("segment_name:N", sort="-x", title=None)),
                       use_container_width=True)
        # poslednji snimak koji je upisao model (posle njega mogu doci npr. GDPR brisanja bez tog svojstva)
        tag = safe("SELECT element_at(summary, 'ml.training-tag') AS tag, committed_at "
                   'FROM lake.ml."customer_segments$snapshots" '
                   "WHERE element_at(summary, 'ml.training-tag') IS NOT NULL ORDER BY committed_at DESC LIMIT 1")
        if tag is not None and not tag.empty:
            st.caption(f"Segmente je izracunao PyIceberg + scikit-learn nad tagom **{tag.tag[0]}** "
                       f"(upisano {tag.committed_at[0]:%Y-%m-%d %H:%M} UTC)")
    else:
        st.caption("Segmenti jos nisu izracunati (scenario segments).")

    st.subheader("Najvredniji kupci")
    st.dataframe(df.sort_values("revenue_eur", ascending=False).head(50), hide_index=True, use_container_width=True)

    st.subheader("Istorija kupca (SCD tip 2)")
    cid = st.number_input("customer_id", min_value=1, value=int(df.sort_values("orders").customer_id.iloc[-1]))
    hist = safe(f"""SELECT customer_key, loyalty_tier, city, valid_from, valid_to, is_current
                    FROM lake.analytics.dim_customer WHERE customer_id = {int(cid)} ORDER BY valid_from""")
    if hist is not None:
        st.dataframe(hist, hide_index=True, use_container_width=True)


def page_internals() -> None:
    st.header("Tabele ispod haube")
    t = tables()
    names = [f"{r.table_schema}.{r.table_name}" for r in t.itertuples()]
    default = names.index("analytics.fact_order_items") if "analytics.fact_order_items" in names else 0
    name = st.selectbox("Tabela", names, index=default)
    schema, tbl = name.split(".")
    meta = lambda suffix: f'lake.{schema}."{tbl}${suffix}"'  # noqa: E731

    # parent_id kao tekst: kolona sa NULL vrednoscu bi u pandas-u postala float i izgubila poslednje cifre ID-ja
    snaps = q(f"""SELECT committed_at, snapshot_id, CAST(parent_id AS VARCHAR) AS parent_id, operation, summary
                  FROM {meta('snapshots')} ORDER BY committed_at""")
    files = q(f"SELECT content, record_count, file_size_in_bytes FROM {meta('files')}")
    manifests = q(f"SELECT path FROM {meta('manifests')}")
    data = files[files.content == 0]
    deletes = files[files.content != 0]
    k = st.columns(5)
    k[0].metric("Snimaka", len(snaps))
    k[1].metric("Manifesta", len(manifests))
    k[2].metric("Data fajlova", len(data))
    k[3].metric("Delete fajlova", len(deletes), help="merge-on-read: brisanja se pamte u posebnim fajlovima")
    k[4].metric("Prosecna velicina", f"{data.file_size_in_bytes.mean() / 1e6:.2f} MB" if len(data) else "-")

    tab1, tab2, tab3, tab4, tab5 = st.tabs(["Snimci i reference", "Fajlovi i particije", "Time travel",
                                            "Odrzavanje", "Graf metapodataka"])
    with tab1:
        s = snaps.copy()
        for f in ("added-records", "deleted-records", "added-data-files", "added-delete-files", "total-records"):
            s[f] = s.summary.map(lambda m, f=f: (m or {}).get(f))
        st.dataframe(s.drop(columns="summary").iloc[::-1], hide_index=True, use_container_width=True)
        st.dataframe(q(f"SELECT * FROM {meta('refs')}"), hide_index=True)
    with tab2:
        if len(files):
            files["MB"] = files.file_size_in_bytes / 1e6
            files["vrsta"] = files.content.map({0: "data", 1: "position deletes", 2: "equality deletes"})
            st.altair_chart(alt.Chart(files).mark_bar().encode(
                x=alt.X("MB:Q", bin=alt.Bin(maxbins=30), title="Velicina fajla (MB)"), y=alt.Y("count()", title="Fajlova"),
                color="vrsta:N"), use_container_width=True)
        try:
            parts = q(f"""SELECT json_format(CAST(partition AS JSON)) AS partition, record_count, file_count,
                                 total_size FROM {meta('partitions')} ORDER BY 1""")
            st.dataframe(parts, hide_index=True, use_container_width=True)
        except Exception:
            st.caption("Tabela nije particionisana.")
    with tab3:
        if len(snaps) > 1:
            opts = {f"{r.committed_at:%Y-%m-%d %H:%M:%S}  {r.operation}  ({r.snapshot_id})": r.snapshot_id
                    for r in snaps.itertuples()}
            pick = st.select_slider("Snimak", list(opts), value=list(opts)[-1])
            sid = opts[pick]
            then = q(f"SELECT COUNT(*) AS n FROM lake.{schema}.{tbl} FOR VERSION AS OF {sid}").n[0]
            now = q(f"SELECT COUNT(*) AS n FROM lake.{schema}.{tbl}").n[0]
            st.metric("Redova u izabranom snimku", f"{then:,}", delta=f"{then - now:,} u odnosu na tekuce stanje")
            st.code(f"SELECT * FROM lake.{schema}.{tbl} FOR VERSION AS OF {sid}", language="sql")
            st.dataframe(q(f"SELECT * FROM lake.{schema}.{tbl} FOR VERSION AS OF {sid} LIMIT 100"),
                         hide_index=True, use_container_width=True)
        else:
            st.caption("Tabela ima samo jedan snimak.")
    with tab4:
        log = safe(f"""SELECT run_at, phase, snapshots, manifests, data_files, delete_files,
                              ROUND(data_bytes / 1e6, 2) AS data_mb, ROUND(all_files_bytes / 1e6, 2) AS all_files_mb,
                              orphan_candidates
                       FROM lake.audit.maintenance_log WHERE table_name = 'lake.{name}' ORDER BY run_at, phase DESC""")
        if log is not None and not log.empty:
            st.dataframe(log, hide_index=True, use_container_width=True)
            st.caption("before/after: kompakcija spaja male fajlove i primenjuje delete fajlove, expire_snapshots "
                       "brise stare snimke (i fajlove koje vise niko ne referencira), rewrite_manifests sredjuje manifeste.")
        else:
            st.caption("Odrzavanje jos nije pokretano za ovu tabelu (scenario nightly_maintenance).")
    with tab5:
        n = st.slider("Broj poslednjih snimaka", 1, 6, 3)
        if st.button("Nacrtaj"):
            import metadata_graph
            st.graphviz_chart(metadata_graph.build(name, n), use_container_width=True)


def page_audit() -> None:
    st.header("Kontrola kvaliteta: Write-Audit-Publish")
    st.caption("Uplate se prvo upisu na granu audit tabele clean.payments; provere se izvrse nad tom granom, "
               "a na main se objavljuju (fast_forward) samo ako sve prodju. Citaoci main grane losu seriju nikada ne vide.")
    df = safe("SELECT * FROM lake.audit.wap_log ORDER BY checked_at DESC")
    if df is None or df.empty:
        return
    runs = (df.groupby("run_id").agg(checked_at=("checked_at", "max"), passed=("passed", "all"),
                                     published=("published", "max"), audit_snapshot_id=("audit_snapshot_id", "max"))
            .sort_values("checked_at", ascending=False).reset_index())
    runs["status"] = runs.published.map({True: "✅ objavljeno", False: "⛔ zadrzano na grani audit"})
    st.dataframe(runs[["checked_at", "run_id", "status", "audit_snapshot_id"]], hide_index=True, use_container_width=True)
    run = st.selectbox("Pokretanje", runs.run_id)
    det = df[df.run_id == run][["check_name", "passed", "observed", "threshold"]]
    st.dataframe(det.style.map(lambda v: "background-color: #fbe0e0" if v is False else "", subset=["passed"]),
                 hide_index=True, use_container_width=True)
    refs = safe('SELECT name, type, snapshot_id FROM lake.clean."payments$refs"')
    if refs is not None:
        st.subheader("Reference tabele clean.payments")
        st.dataframe(refs, hide_index=True)
        if len(refs[refs.name.isin(["main", "audit"])].snapshot_id.unique()) > 1:
            st.warning("Grana audit je ispred main-a: postoji serija koja nije prosla proveru i ceka ispravku.")


PAGES = {"Prodaja": page_sales, "Konverzija": page_funnel, "Finansije": page_finance, "Kupci": page_customers,
         "Tabele ispod haube": page_internals, "Kontrola kvaliteta": page_audit}

with st.sidebar:
    st.title("🧊 Lakehouse")
    page = st.radio("Stranica", list(PAGES), label_visibility="collapsed")
    st.divider()
    if st.button("Osvezi"):
        st.cache_data.clear()
    st.caption("Citanje: Trino 470 (readonly S3 nalog)\nKatalog: Iceberg REST\nSkladiste: MinIO (S3 API)")

PAGES[page]()
