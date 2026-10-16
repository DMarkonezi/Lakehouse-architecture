# Scenariji koji pokrecu mehanizme iz poglavlja 3.   Upotreba:  .\scenario.ps1 <ime> [-Arg <vrednost>]
#
#   init                 pocetno ucitavanje istorije (poziva ga reset.ps1)
#   normal_week          nova nedelja: porudzbine, promene statusa, povracaji, clickstream po satu
#   payments_in_cents    provajder posalje jedan dan u centima (x100) -> WAP provera pada, nista se ne objavljuje
#   payments_fix         stize ispravljen fajl -> provera prolazi, objava preko fast_forward
#   app_release_3        aplikacija salje experiment{id, variant} -> nova kolona bez prepisivanja podataka
#   promotions_engine    discount -> discount_amount, nova discount_pct, unit_price decimal(10,2) -> (12,2)
#   black_friday         particionisanje clickstream-a days -> hours, petak 5x vise saobracaja
#   bad_deploy           transformacija primeni kurs dva puta (izvestaj postaje pogresan)
#   rollback             vracanje fact tabele na snimak pre bad_deploy
#   month_close          tag close-2026-09 + vremensko putovanje
#   erasure_request      GDPR brisanje kupca (-Arg <customer_id>, podrazumevano se bira automatski)
#   nightly_maintenance  kompakcija, expire_snapshots, rewrite_manifests, remove_orphan_files (dry_run)
#   segments             PyIceberg (bez Spark-a) racuna RFM segmente i upisuje lake.ml.customer_segments
#   atomicity            Spark se ubije usred upisa -> tabela ostaje na prethodnom snimku
#   conflict             dva PyIceberg upisa zasnovana na istom snimku -> drugi se odbija
#   separation           Spark se zaustavi, Trino i DuckDB i dalje citaju iste tabele
#   start_spark          ponovo pokrece Spark posle 'separation'
#   metadata             crta stvarnu strukturu metapodataka tabele (-Arg <tabela>)
param(
    [Parameter(Mandatory = $true, Position = 0)][string]$Name,
    [string]$Arg = "",
    [ValidateSet("small", "demo")][string]$Scale = "small",
    [int]$Seed = 42
)
$ErrorActionPreference = "Continue"
Set-Location $PSScriptRoot

function Step($text) { Write-Host "`n>>> $text" -ForegroundColor Cyan }

function Gen {
    docker exec explorer python /app/generator/generate.py @args
    if ($LASTEXITCODE -ne 0) { throw "generator nije uspeo" }
}

function Load-Postgres {
    $batch = (Get-ChildItem sources\files\postgres -Directory | Sort-Object Name | Select-Object -Last 1).Name
    Step "PostgreSQL: ucitavam $batch (nove porudzbine, promene statusa, cena i nivoa lojalnosti)"
    docker exec postgres sh -c "psql -q -U `"`$POSTGRES_USER`" -d `"`$POSTGRES_DB`" -f /sources/postgres/$batch/load.sql"
    if ($LASTEXITCODE -ne 0) { throw "ucitavanje u PostgreSQL nije uspelo" }
}

$SparkNoise = "ivy|::|in central|from central|added as a dependency|confs:|artifacts|^\s*\||^\s*-{10,}|The jars for|" +
              "WARN NativeCodeLoader|WARN Utils|Setting default log level|To adjust logging level| INFO |^\s*$"

function Spark {
    # a,b,c je u PowerShell-u niz: vraca se u jedan argument "a,b,c"
    $script = $args[0]; $rest = @(); for ($i = 1; $i -lt $args.Count; $i++) { $rest += , ($args[$i] -join ",") }
    # sakrivamo logove pokretanja Spark-a i Ivy-ja; ostaje izlaz poslova i greske
    docker exec spark-iceberg spark-submit "/home/iceberg/jobs/$script" @rest 2>&1 | ForEach-Object { "$_" } |
        Where-Object { $_ -notmatch $SparkNoise }
    if ($LASTEXITCODE -ne 0) { throw "Spark posao '$script' nije uspeo" }
}

function Py {
    # a,b,c je u PowerShell-u niz: vraca se u jedan argument "a,b,c"
    $script = $args[0]; $rest = @(); for ($i = 1; $i -lt $args.Count; $i++) { $rest += , ($args[$i] -join ",") }
    docker exec explorer python "/app/explorer/$script" @rest
    if ($LASTEXITCODE -ne 0) { throw "explorer skripta '$script' nije uspela" }
}

function Pipeline { Step "Spark pipeline: raw -> clean -> WAP -> analytics"; Spark pipeline.py @args }

switch ($Name) {
    "init" {
        Step "generator: istorija ($Scale, seed $Seed)"
        Gen init --seed $Seed --scale $Scale
        Load-Postgres
        Pipeline
    }
    "normal_week"       { Step "generator: nova nedelja"; Gen next; Load-Postgres; Pipeline }
    "payments_in_cents" { Step "generator: nova nedelja, jedan dan uplata u centima (x100)"; Gen next --payments-in-cents; Load-Postgres; Pipeline }
    "payments_fix"      { Step "provajder salje ispravljen fajl"; Gen fix-payments; Pipeline --steps raw,payments,analytics }
    "app_release_3"     { Step "generator: aplikacija 3.0 salje experiment{id, variant}"; Gen next --app-release-3; Load-Postgres; Pipeline }
    "promotions_engine" {
        Step "evolucija seme fact tabele"
        Spark scenarios.py promotions_engine
        Step "generator: izvor dobija discount_pct"
        Gen next --promotions; Load-Postgres; Pipeline
    }
    "black_friday" {
        Step "evolucija particionisanja raw.clickstream: days -> hours"
        Spark scenarios.py black_friday
        Gen next --black-friday; Load-Postgres; Pipeline
    }
    "bad_deploy"          { Spark scenarios.py bad_deploy }
    "rollback"            { Spark scenarios.py rollback }
    "month_close"         { Spark scenarios.py month_close }
    "nightly_maintenance" { Spark maintenance.py }
    "segments"            { Step "PyIceberg + DuckDB + scikit-learn (Spark nije potreban)"; Py segment_customers.py; Pipeline --steps analytics }
    "metadata"            { Py metadata_graph.py $(if ($Arg) { $Arg } else { "analytics.fact_order_items" }) }
    "conflict"            { Py conflict_demo.py }
    "erasure_request" {
        Spark scenarios.py erasure_request $Arg
        $cid = (Get-Content results\erasure_customer.txt).Trim()
        Step "brisanje kupca $cid i u izvornoj bazi"
        docker exec postgres sh -c "psql -q -U `"`$POSTGRES_USER`" -d `"`$POSTGRES_DB`" -c 'DELETE FROM order_items WHERE order_id IN (SELECT order_id FROM orders WHERE customer_id = $cid); DELETE FROM orders WHERE customer_id = $cid; DELETE FROM customers WHERE customer_id = $cid;'"
    }
    "atomicity" {
        Step "priprema tabele lake.demo.clickstream_copy"
        Spark scenarios.py atomicity_prepare
        Step "pokrecem dug upis u pozadini"
        $job = Start-Job { docker exec spark-iceberg spark-submit /home/iceberg/jobs/scenarios.py atomicity_write 2>&1 | Out-Null }
        Py atomicity_check.py wait
        Step "docker kill spark-iceberg  (usred upisa)"
        docker kill spark-iceberg | Out-Null
        Stop-Job $job -ErrorAction SilentlyContinue; Remove-Job $job -Force -ErrorAction SilentlyContinue
        Py atomicity_check.py verify
        Step "ponovo pokrecem Spark"
        docker start spark-iceberg | Out-Null
        docker compose up -d --wait spark-iceberg 2>&1 | Out-Null
    }
    "separation" {
        Step "docker stop spark-iceberg"
        docker stop spark-iceberg | Out-Null
        Py check_separation.py
        Write-Host "`nSpark je zaustavljen; konzola na http://localhost:8501 i dalje radi.  Vracanje: .\scenario.ps1 start_spark" -ForegroundColor Yellow
    }
    "start_spark" { docker start spark-iceberg | Out-Null; docker compose up -d --wait spark-iceberg 2>&1 | Out-Null; Write-Host "Spark ponovo radi" }
    default { Write-Host "Nepoznat scenario '$Name'. Spisak je na vrhu scenario.ps1." -ForegroundColor Red; exit 1 }
}

