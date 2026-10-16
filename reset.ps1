# Od nule do demonstracije: brise sve podatke, podize ceo stek i radi pocetno ucitavanje.
#   .\reset.ps1                    # mala kolicina podataka (1 nedelja istorije), ~3 min
#   .\reset.ps1 -Scale demo        # 3 meseca istorije, ~150 hiljada stavki, ~1 milion dogadjaja
#   .\reset.ps1 -NoLoad            # samo prazan stek
param(
    [ValidateSet("small", "demo")][string]$Scale = "small",
    [int]$Seed = 42,
    [switch]$NoLoad
)
$ErrorActionPreference = "Continue"
Set-Location $PSScriptRoot

if (-not (Test-Path .env)) {
    Copy-Item .env.example .env
    Write-Host "napravljen .env iz .env.example" -ForegroundColor Yellow
}

Write-Host "=== brisem kontejnere, volumene i generisane fajlove" -ForegroundColor Cyan
docker compose down -v --remove-orphans 2>&1 | Out-Null
Remove-Item -Recurse -Force sources\files, results -ErrorAction SilentlyContinue
New-Item -ItemType Directory -Force sources\files, results | Out-Null

Write-Host "=== podizem stek (prvi put traje duze zbog preuzimanja image-a)" -ForegroundColor Cyan
docker compose up -d --build --wait
if ($LASTEXITCODE -ne 0) { throw "stek se nije podigao - pogledaj: docker compose ps" }
docker compose ps --format "table {{.Name}}\t{{.Status}}"

if (-not $NoLoad) {
    & "$PSScriptRoot\scenario.ps1" init -Scale $Scale -Seed $Seed
}

Write-Host "`nSpremno:" -ForegroundColor Green
Write-Host "  Lakehouse Console  http://localhost:8501"
Write-Host "  MinIO konzola      http://localhost:9001   (admin / lozinka iz .env)"
Write-Host "  Spark Jupyter      http://localhost:8888"
Write-Host "  Explorer Jupyter   http://localhost:8889   (PyIceberg + DuckDB, bez Spark-a)"
Write-Host "  Trino              docker exec -it trino trino --catalog lake"
