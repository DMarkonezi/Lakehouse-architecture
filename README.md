# Apache Iceberg lakehouse – onlajn prodavnica elektronike

Lokalno lakehouse okruženje (Docker, bez ikakvih naloga) za regionalnu onlajn prodavnicu elektronike sa
web i mobilnim kanalom, u Srbiji (RSD) i evrozoni (EUR). Podaci iz četiri vrste izvora stižu u Iceberg
tabele na S3 skladištu, prolaze kroz slojeve **raw → clean → analytics (+ ml)** i čitaju ih **četiri
različite mašine**: Spark, Trino, DuckDB i PyIceberg. Scenariji pokreću mehanizme Iceberg-a jedan po
jedan (WAP, merge-on-read, rollback, evolucija šeme i particija, GDPR brisanje, održavanje...), a
merenja ih kvantifikuju.

## Od nule do demonstracije u tri komande

Potreban je samo Docker Desktop (8 GB RAM-a za Docker).

```powershell
.\reset.ps1                      # podiže ceo stek i učitava istoriju (~3 min, prvi put duže zbog image-a)
.\scenario.ps1 normal_week       # nova nedelja poslovanja: porudžbine, uplate, klikovi
start http://localhost:8501      # konzola: prodaja, konverzija, finansije, kupci, tabele ispod haube
```

Ceo tok (reset, svi scenariji redom, merenja) jednom komandom: `.\run_all.ps1` (~30 min).

`reset.ps1` briše sve volumene: korisnici baze i S3 skladišta, šema tabela i podaci prave se samo na praznom
okruženju. Ako `.env` već postoji, ne prepisuje se; nove promenljive iz `.env.example` tada treba dodati ručno.

## Arhitektura

```
 IZVORI                           OBRADA                  ICEBERG (format v2)                  ČITANJE
 ─────────────────────────────    ──────────────────      ───────────────────────────────      ─────────────────────
 PostgreSQL  customers, products ┐                        raw        (kopija izvora)           Trino 470  ─► konzola
             orders, order_items ├─► Spark 3.5 ─ MERGE ─► clean      (očišćeno, WAP)           (samo čitanje)  :8501
 CSV/dan     payments, fx_rates  │   (spark-submit,       analytics  (zvezdasta šema)
 JSONL/sat   clickstream         ┘    local režim)        ml         (segmenti kupaca)  ◄──── PyIceberg + DuckDB
                                                                │                           (bez Spark-a i JVM-a)
                                       REST katalog ◄───────────┤  "koji metadata.json je tekući"
                                       MinIO (S3 API) ◄─────────┘  Parquet + Avro + JSON fajlovi
```

| Komponenta | Uloga | Adresa |
|---|---|---|
| **MinIO** | S3 skladište: data fajlovi (Parquet) i metapodaci (JSON, Avro) | http://localhost:9001 (`admin` / lozinka iz `.env`) |
| **Iceberg REST katalog** | jedini izvor istine o tekućem stanju tabele; atomična zamena pokazivača; jedini upisuje `metadata.json` | http://localhost:8181 |
| **Spark + Iceberg 1.8.1** | ETL, MERGE, procedure održavanja; Jupyter | http://localhost:8888, Spark UI :4040 |
| **Trino 470** | SQL za konzolu; S3 nalog **samo za čitanje** | `docker exec -it trino trino --catalog lake` |
| **Explorer** (PyIceberg 0.9.1, DuckDB 1.2.2) | čisti Python: notebook, ML, merenja, graf metapodataka | http://localhost:8889 |
| **Konzola** (Streamlit) | poslovni prikazi + Iceberg ispod haube | http://localhost:8501 |
| **PostgreSQL 16** | izvorna OLTP baza prodavnice | localhost:5433 (`shop` / lozinka iz `.env`) |

Sve verzije image-a su fiksirane; ograničenja memorije su u `docker-compose.yml` (Spark 4 GB, Trino 2 GB, MinIO 1 GB, ...).

## Domen i model podataka

Generator (`generator/generate.py`, `--seed`, `--scale small|demo`) je deterministički: isti seed daje iste podatke
(vidi „Ponovljivost"). Svaka porudžbina ima svoj tok (pending → paid → shipped → delivered; ~3% otkazano, ~4% vraćeno,
0,3% chargeback), cene u RSD se računaju iz EUR po dnevnom kursu, a klikovi u sesijama koje se završe kupovinom vode do
stvarnih porudžbina. Izvori namerno sadrže greške: neispravno vreme, događaj bez tipa, duplikati, ponovo poslat fajl uplata.

| Sloj | Tabele | Napomena |
|---|---|---|
| **raw** | customers, products, orders, order_items, payments, fx_rates, clickstream | Postgres inkrementalno po `updated_at`; fajlovi se prate po `_source_file`; `orders` i `order_items` su **merge-on-read**; jedini sloj sa ličnim podacima (ime, e-pošta) |
| **clean** | orders, order_items, payments, clickstream, clickstream_rejected | normalizacija, odbačeni događaji sa razlogom; `payments` ide kroz **WAP** |
| **analytics** | fact_order_items, dim_customer, dim_product, dim_date, daily_sales, conversion_funnel, customer_360, payment_reconciliation | zvezdasta šema bez ličnih podataka (kupac je samo `customer_id`); fact: jedan red po stavci, strani ključevi `customer_key`, `product_key` i `date_key` (→ `dim_date`), `months(order_ts)`, copy-on-write, sortiran po `order_ts`; SCD2 dimenzije (nivo lojalnosti, cena) |
| **ml** | customer_segments | RFM + k-means, upisuje PyIceberg nad zamrznutim tagom `ml-train-*` |
| **audit** | wap_log, maintenance_log | rezultati provera i stanje tabela pre/posle održavanja |

## Scenariji

`.\scenario.ps1 <ime>`, redosled kao u `run_all.ps1`:

| Scenario | Mehanizam | Šta se vidi |
|---|---|---|
| `normal_week` | MERGE, inkrementalno učitavanje, SCD2 | samo izmenjeni redovi ulaze u MERGE; nove verzije kupaca |
| `payments_in_cents` | **Write-Audit-Publish** (grana `audit`) | provera pada (iznosi ×100), `main` ostaje netaknut; konzola → Kontrola kvaliteta |
| `payments_fix` | `fast_forward` | ispravljen fajl prolazi proveru i objavljuje se bez kopiranja podataka |
| `app_release_3` | evolucija šeme (nova struct kolona) | stari fajlovi se ne prepisuju; A/B test u konzoli → Konverzija |
| `promotions_engine` | rename, nova kolona, proširenje tipa | `discount → discount_amount`, `decimal(10,2) → (12,2)`; broj fajlova i snimak isti |
| `black_friday` | evolucija particionisanja | `days(event_ts) → hours(event_ts)`; stari fajlovi ostaju u spec 0 |
| `segments` | PyIceberg upis, tag | ML bez Spark-a; snimak nosi `ml.training-tag` |
| `bad_deploy` / `rollback` | tag `pre-deploy`, `rollback_to_snapshot` | RSD prihod 117× manji → vraćanje za manje od sekunde |
| `month_close` | tag `close-2026-09` (RETAIN 365 DAYS) | zaključan broj za finansije, i u Trino-u `FOR VERSION AS OF 'close-2026-09'` |
| `nightly_maintenance` | `rewrite_data_files` (binpack/sort), delete fajlovi, `expire_snapshots`, `rewrite_manifests`, `remove_orphan_files` (dry run) | konzola → Tabele ispod haube → Održavanje |
| `conflict` | optimistička konkurentnost | drugi PyIceberg upis na istom snimku se odbija, posle osvežavanja prolazi |
| `atomicity` | atomičnost potvrde | `docker kill` usred upisa: tabela na starom snimku, napisani fajlovi su siročad |
| `erasure_request` | GDPR: DELETE → kompakcija → expire | brisanje po `customer_id` kroz sve slojeve; time travel pre brisanja radi, posle ne; upozorenje za tagove koji i dalje čuvaju podatke |
| `separation` / `start_spark` | razdvajanje skladišta i obrade | Spark ugašen; Trino i DuckDB daju isti zbir, konzola radi |
| `metadata` | struktura metapodataka | `results/figures/metadata_*.svg`: katalog → metadata.json → snimci → manifesti → fajlovi |

## Merenja

`.\run_benchmarks.ps1` → `results/benchmarks.md` (tabele) i `results/benchmarks.json`. Vremena su medijana 5 ponavljanja;
uz svako vreme beleže se i broj fajlova i bajtova, jer oni ne zavise od računara.

| Merenje | Rezultat (skala small, ovaj računar) |
|---|---|
| Pruning (Trino, clickstream) | upit za jedan dan čita 24 od 1012 fajlova (0,48 od 23 MB), 0,29 s naspram 3,1 s; fact: mesec čita 1 od 2 fajla |
| 50 mikro-serija → kompakcija | 50 fajlova / 50 manifesta → 1 / 1; upit 0,91 s → 0,29 s |
| CoW naspram MoR (2 M redova, 10 rasutih redova) | CoW upiše 43 MB po izmeni, MoR 11 KB; 4,7 s naspram 1,3 s |
| Rollback | ~0,06 s i za 100 hiljada i za 10 miliona redova; vraćanje kopiranjem 0,5 s → 9,2 s |
| Rast skladišta | 10 izmena: 24 MB → 269 MB na S3; posle `expire_snapshots` ponovo 24 MB |
| Interoperabilnost | Spark, Trino i DuckDB: identičan kontrolni zbir istog snimka |
| Format v3 | Spark/Iceberg 1.8.1 pravi v3 tabelu, ali upis pada (`first-row-id is null`); Trino 470 je čita → projekat koristi v2 |

## Ponovljivost

Isti seed mora dati iste brojeve. `explorer/fingerprint.py <oznaka>` preko Trino-a računa kontrolne zbirove poslovnih
kolona (tehničke kolone kao `_ingested_at` se razlikuju između pokretanja), čuva ih u `fingerprints/` (folder koji
`reset.ps1` ne briše) i poredi sa prethodnim:

```powershell
.\reset.ps1;  docker exec explorer python /app/explorer/fingerprint.py run1
.\reset.ps1;  docker exec explorer python /app/explorer/fingerprint.py run2     # run2 vs run1: ISTO
```

## Explorer: Iceberg iz čistog Python-a

- `notebooks/explorer.ipynb` (http://localhost:8889): katalog, šema, snimci, `plan_files` pruning, SQL u DuckDB-u, time travel, delete fajlovi.
- `explorer/metadata_graph.py <tabela>`: crta stvarnu strukturu metapodataka (pandan slici 3.3 u radu); isti graf je u konzoli.
- `explorer/lake.py`: pristup katalogu, S3 i DuckDB-u u ~80 linija.

## Bezbednost: najmanje privilegije

`minio/setup.sh` pravi zasebne S3 korisnike:

| Korisnik | Prava | Ko ga koristi |
|---|---|---|
| `catalog` | čitanje i pisanje | REST katalog (jedini upisuje `metadata.json`) |
| `spark` | čitanje i pisanje | Spark poslovi |
| `explorer` | čitanje svega, pisanje **samo u `ml/` i `demo/`** (politika `minio/explorer-policy.json`) | PyIceberg: segmentacija (`ml`), scenario `conflict` (`demo`); tagove na analitičkim tabelama pravi preko kataloga |
| `trino` | **samo čitanje** | Trino i konzola |

Spark čita izvornu bazu korisnikom `lake_reader`, koji sme samo `SELECT` (pravi ga `sources/postgres/reader.sh`);
generator i scenario `erasure_request` pišu u bazu kao `shop`. Provere (sve tri treba da padnu):

```powershell
docker exec explorer python -c "import lake; f=lake.s3().open_output_stream('warehouse/analytics/proba.txt'); f.write(b'x'); f.close()"   # AccessDenied
docker exec postgres psql -U lake_reader -d shop -c "DELETE FROM customers WHERE false"                                                    # permission denied
docker exec trino trino --catalog lake --execute "INSERT INTO demo.concurrent_writes VALUES ('x', current_timestamp)"                     # pada na S3
```

Lični podaci kupaca (ime, e-pošta) postoje samo u `raw.customers`; analitički sloj i konzola vide samo `customer_id`.
Lozinke su u `.env` (pravi se iz `.env.example`).

## Argument „oblak"

MinIO govori isti S3 API kao Amazon S3, a Iceberg ne zavisi od mašine za obradu: stanje tabele je u katalogu i u fajlovima.
Prelazak na pravi oblak je promena konfiguracije, ne koda:

| Gde | Danas (MinIO) | Amazon S3 |
|---|---|---|
| `spark/spark-defaults.conf` | `s3.endpoint=http://minio:9000`, `s3.path-style-access=true` | ukloniti obe linije; akreditivi iz IAM uloge |
| `trino/iceberg.properties` | `s3.endpoint`, `s3.path-style-access`, ključevi iz `.env` | ukloniti endpoint i path-style; `s3.region` |
| `explorer/lake.py` | `s3.endpoint` | ukloniti |
| katalog | REST fixture (SQLite) | bilo koji REST katalog (npr. AWS Glue, Polaris, Nessie) |

Scenario `separation` to pokazuje uživo: Spark, mašina koja je podatke upisala, se zaustavi, a Trino i DuckDB i dalje
čitaju iste tabele i dobijaju isti rezultat.

## Svesno izostavljeno

Bez cloud naloga, bez strimovanja (Kafka/Flink) i bez Delta Lake-a i Hudi-ja: fokus je na mehanizmima Iceberg-a.
Format v3 je izmeren, ali ne i korišćen (vidi Merenja). REST katalog je referentna implementacija, a ne produkcijski katalog.
`remove_orphan_files` radi samo kao dry run, jer Iceberg odbija prag kraći od 24 h (fajlovi upisa koji je u toku).

## Struktura

```
docker-compose.yml, .env.example     stek i lozinke
minio/                               setup.sh (S3 korisnici), explorer-policy.json
spark/ trino/ pytools/               konfiguracija servisa (pytools = image za explorer i konzolu)
sources/postgres/                    init.sql (šema izvorne baze), reader.sh (korisnik lake_reader)
generator/generate.py                deterministički generator izvora (init / next / fix-payments)
jobs/                                Spark: raw_ingest, clean, wap_payments, analytics, pipeline,
                                     maintenance, scenarios, bench_spark, common
explorer/                            PyIceberg + DuckDB: lake, metadata_graph, segment_customers, conflict_demo,
                                     atomicity_check, check_separation, bench_py, bench_report, fingerprint
console/app.py                       Streamlit konzola (čita samo preko Trino-a)
notebooks/explorer.ipynb             Iceberg iz čistog Python-a
reset.ps1  scenario.ps1  run_benchmarks.ps1  run_all.ps1
docs/DEFENSE.md                      scenario odbrane (~10 min)
results/                             merenja, grafovi (generiše se, nije u git-u)
fingerprints/                        otisci za proveru ponovljivosti (nije u git-u)
```
