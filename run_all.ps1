# Ceo tok od nule: reset, svi scenariji redom i merenja (oko 30 minuta na skali small).
# Upotreba:  .\run_all.ps1 [-Scale small|demo] [-Seed 42] [-SkipBenchmarks]
# Zaustavlja se na prvoj gresci; na kraju ispisuje trajanje svakog koraka.
param(
    [ValidateSet("small", "demo")][string]$Scale = "small",
    [int]$Seed = 42,
    [switch]$SkipBenchmarks
)
$ErrorActionPreference = "Stop"
Set-Location $PSScriptRoot

$steps = @(
    "normal_week", "payments_in_cents", "payments_fix", "app_release_3", "promotions_engine", "black_friday",
    "segments", "bad_deploy", "rollback", "month_close", "nightly_maintenance", "conflict", "atomicity",
    "erasure_request", "metadata"
)
$times = [ordered]@{}

$t = [Diagnostics.Stopwatch]::StartNew()
.\reset.ps1 -Scale $Scale -Seed $Seed
$times["reset + init"] = $t.Elapsed

foreach ($s in $steps) {
    Write-Host "`n==================== $s ====================" -ForegroundColor Magenta
    $t = [Diagnostics.Stopwatch]::StartNew()
    .\scenario.ps1 $s -Scale $Scale -Seed $Seed
    $times[$s] = $t.Elapsed
}
if (-not $SkipBenchmarks) {
    Write-Host "`n==================== merenja ====================" -ForegroundColor Magenta
    $t = [Diagnostics.Stopwatch]::StartNew()
    .\run_benchmarks.ps1
    $times["benchmarks"] = $t.Elapsed
}
Write-Host "`n==================== separation ====================" -ForegroundColor Magenta
.\scenario.ps1 separation
.\scenario.ps1 start_spark

Write-Host "`nTrajanje koraka:" -ForegroundColor Green
$times.GetEnumerator() | ForEach-Object { "  {0,-22} {1:mm\:ss}" -f $_.Key, $_.Value }
