# Scenario odbrane (~10 minuta)

Cilj: uživo pokazati da je Iceberg **format tabele nezavisan od mašine** i da njegovi mehanizmi
(snimci, grane, atomična potvrda, metapodaci) rešavaju konkretne poslovne probleme prodavnice.

## Priprema (dan ranije, ~15 min)

```powershell
.\reset.ps1
.\scenario.ps1 normal_week
.\scenario.ps1 app_release_3          # A/B eksperiment u konzoli
.\scenario.ps1 segments               # ML segmenti
.\scenario.ps1 month_close            # tag close-2026-09
.\scenario.ps1 nightly_maintenance    # pre/posle u konzoli
.\run_benchmarks.ps1                  # results\benchmarks.md
.\scenario.ps1 metadata               # results\figures\metadata_analytics_fact_order_items.svg
```

Otvoriti unapred: konzolu (http://localhost:8501), `results\benchmarks.md`, `README.md` (dijagram) i PowerShell u
folderu projekta. Docker Desktop sme da ostane uključen preko noći; podaci su u volumenima.

## Tok

| Vreme | Korak | Komanda / ekran | Šta reći |
|---|---|---|---|
| 0:00 | Arhitektura | README, dijagram | Četiri izvora, slojevi raw → clean → analytics → ml. Skladište (MinIO, S3 API) i katalog su odvojeni od mašina za obradu: Spark piše, Trino, DuckDB i PyIceberg čitaju. |
| 1:00 | Posao kao korisnik | konzola → **Prodaja** | Prihod po kanalu i zemlji. Konzola čita isključivo preko Trino-a, sa S3 nalogom koji ima samo pravo čitanja. |
| 1:30 | **WAP** | `.\scenario.ps1 payments_in_cents` (~90 s) | Provajder je poslao jedan dan u centima. Upis ide na granu `audit`; provere padaju (99× veći obim), `main` se ne menja. Izveštaji nikada ne vide lošu seriju. |
| 3:00 | | konzola → **Kontrola kvaliteta** | Rezultati provera; grana `audit` je ispred `main`. Ispravka bi bila `payments_fix` → `fast_forward`, bez kopiranja podataka. |
| 3:30 | **Loš deploy i rollback** | `.\scenario.ps1 bad_deploy` → konzola Prodaja (RSD pao 117×) → `.\scenario.ps1 rollback` | Rollback je promena pokazivača u katalogu: ispod sekunde, bez obzira na veličinu tabele. |
| 5:00 | **Ispod haube** | konzola → **Tabele ispod haube**: snimci, time travel klizač, graf metapodataka, održavanje | Svaki upis je novi snimak; graf je stvarna struktura (katalog → metadata.json → lista manifesta → manifesti → fajlovi). Održavanje: kompakcija, expire, manifesti. |
| 6:30 | **Atomičnost** | `.\scenario.ps1 atomicity` (~60 s) | Spark se ubija usred upisa. Tabela ostaje na prethodnom snimku; napisani fajlovi su siročad koju niko ne vidi. Nema polovičnog stanja. |
| 7:45 | **Razdvajanje skladišta i obrade** | `.\scenario.ps1 separation`, pa osvežiti konzolu | Mašina koja je upisala podatke je ugašena; Trino i DuckDB daju identičan zbir, konzola radi. Podaci žive u formatu, ne u mašini. |
| 8:45 | **Merenja** | `results\benchmarks.md`: rollback, CoW/MoR, pruning | Rollback ~0,06 s i za 10 miliona redova (kopiranje 9,2 s). CoW prepisuje 43 MB za 10 izmenjenih redova, MoR 11 KB. Upit za jedan dan čita 24 od 1012 fajlova (0,48 od 23 MB). |
| 9:30 | **Oblak** | README → „Argument oblak" | MinIO govori S3 API; prelazak na Amazon S3 je uklanjanje `s3.endpoint` iz tri konfiguracije. Format v2, jer v3 upis u Iceberg 1.8.1 još ne radi (izmereno). |
| 10:00 | | `.\scenario.ps1 start_spark` | |

## Rezervni odgovori (ako ostane vremena ili dođu pitanja)

| Pitanje | Pokazati |
|---|---|
| Kako se menja šema bez prepisivanja? | `.\scenario.ps1 promotions_engine`: rename, nova kolona, širenje tipa; broj fajlova i snimak isti (kolone se prate po ID-ju) |
| A promena particionisanja? | `.\scenario.ps1 black_friday`: days → hours; stari fajlovi ostaju u spec 0; upiti rade preko obe specifikacije |
| GDPR / pravo na zaborav? | `.\scenario.ps1 erasure_request`: DELETE nije dovoljan (time travel), tek kompakcija + expire; tagovi i dalje čuvaju podatke |
| Šta ako dva upisa krenu istovremeno? | `.\scenario.ps1 conflict`: katalog odbija drugu potvrdu, posle osvežavanja prolazi |
| Kako ML zna na kojim podacima je učen? | konzola → Kupci: tag `ml-train-*`, svojstvo snimka `ml.training-tag` |
| Bezbednost? | Trino: `INSERT INTO lake.demo.concurrent_writes VALUES ('x', current_timestamp)` → pada na S3 |

## Ako nešto zakaže

- Pipeline traje duže nego obično: dok radi, prikazati konzolu ili graf metapodataka.
- Spark ne odgovara: `.\scenario.ps1 start_spark`.
- Sve ostalo: `results\benchmarks.md` i `results\figures\` su statički dokazi istih tvrdnji.
