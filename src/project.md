# Apache Iceberg lakehouse – kratak pregled projekta

## Cilj
Praktični deo rada: lokalno lakehouse okruženje (Docker, bez cloud naloga) na kome se mehanizmi Apache Iceberg
formata pokreću kroz realne poslovne scenarije jedne onlajn prodavnice i kvantifikuju merenjima. Teza koju projekat
dokazuje: tabela je **otvoren format nezavisan od mašine**: jedna mašina piše, više različitih čita isti snimak,
a stanje tabele (snimci, grane, tagovi) rešava probleme kvaliteta, oporavka i usklađenosti bez kopiranja podataka.

## Okruženje (Docker Compose, fiksirane verzije)
| Komponenta | Uloga |
|---|---|
| MinIO (S3 API) | skladište: Parquet data fajlovi, JSON/Avro metapodaci |
| Iceberg REST katalog 1.9.2 | tekući `metadata.json` svake tabele, atomična potvrda |
| Spark 3.5.5 + Iceberg 1.8.1 | ETL (MERGE), procedure održavanja |
| Trino 470 | SQL za konzolu, S3 nalog samo za čitanje |
| PyIceberg 0.9.1 + DuckDB 1.2.2 | čisti Python: ML, merenja, analiza metapodataka |
| Streamlit | konzola sa poslovnim prikazima i prikazom "ispod haube" |
| PostgreSQL 16 | izvorna OLTP baza |

## Domen i podaci
Regionalna prodavnica elektronike (web i aplikacija; RSD i EUR). Deterministički generator (`--seed`, `--scale`)
pravi četiri vrste izvora: Postgres (kupci, proizvodi, porudžbine, stavke; inkrementalno po `updated_at`), dnevni CSV
uplata i kurseva i JSONL klikove po satu sa ugnježdenim poljima. Podaci sadrže tipične greške (neispravno vreme,
duplikati, ponovo poslat fajl).

Slojevi: **raw** (kopija izvora; porudžbine u merge-on-read režimu) → **clean** (normalizacija, odbačeni zapisi sa
razlogom, uplate kroz Write-Audit-Publish) → **analytics** (zvezdasta šema: `fact_order_items` particionisan po mesecu i
sortiran, SCD2 dimenzije kupca i proizvoda, izveštajne tabele) → **ml** (RFM segmentacija kupaca, k-means, upis preko
PyIceberg-a nad zamrznutim tagom).

## Demonstrirani mehanizmi (scenariji)
Write-Audit-Publish sa granom `audit` i `fast_forward`; merge-on-read i delete fajlovi; evolucija šeme (nova kolona,
rename, proširenje tipa) i particionisanja (dan → sat) bez prepisivanja; tagovi (zatvaranje meseca, skup za učenje
modela, stanje pre deploy-a); rollback; atomičnost (prekid Spark-a usred upisa ostavlja samo siroče fajlove);
optimistička konkurentnost (konflikt dva upisa); GDPR brisanje (DELETE → kompakcija → expire, uz upozorenje za
tagove); održavanje (kompakcija binpack/sort, expire_snapshots, rewrite_manifests, remove_orphan_files); razdvajanje
skladišta i obrade (Spark ugašen, Trino i DuckDB daju isti rezultat); najmanje privilegije na S3.

## Merenja (skala small, lokalno; vremena su medijana 5 ponavljanja)
- **Pruning:** upit za jedan dan čita 24 od 1012 fajlova clickstream tabele (0,48 od 23 MB), 10× brže od cele tabele.
- **Mali fajlovi:** 50 mikro-serija = 50 fajlova i 50 manifesta; posle kompakcije 1 i 1, upit 3× brži.
- **CoW naspram MoR:** izmena 10 rasutih redova u tabeli od 2 M redova: CoW upiše 43 MB, MoR 11 KB.
- **Rollback:** ~0,06 s nezavisno od veličine (0,1 – 10 M redova); vraćanje kopiranjem raste od 0,5 do 9,2 s.
- **Skladište:** 10 izmena podiže zauzeće sa 24 na 269 MB; `expire_snapshots` ga vraća na 24 MB.
- **Interoperabilnost:** Spark, Trino i DuckDB daju identičan kontrolni zbir istog snimka.
- **Format v3:** Spark/Iceberg 1.8.1 ne može da potvrdi upis u v3 tabelu, pa projekat koristi v2.

## Pokretanje
`.\reset.ps1` (stek + istorija), `.\scenario.ps1 <ime>` (pojedinačni scenario), `.\run_benchmarks.ps1` (merenja),
`.\run_all.ps1` (sve od nule, ~30 min). Konzola: http://localhost:8501. Scenario odbrane: `docs/DEFENSE.md`.

## Ograničenja
Sve radi na jednom računaru; REST katalog je referentna implementacija (SQLite); obrada je paketna (bez strimovanja);
Delta Lake i Hudi su namerno izostavljeni. Apsolutna vremena zavise od računara; zato se uz njih beleže i broj fajlova i
bajtova. `remove_orphan_files` se izvršava samo kao dry run (Iceberg ne dozvoljava prag kraći od 24 h).
