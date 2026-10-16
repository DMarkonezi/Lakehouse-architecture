# Merenja (faza 4).   Upotreba:  .\run_benchmarks.ps1 [-Parts small_files,cow_vs_mor,...]
# Rezultat: results\benchmarks.md (tabele za rad) i results\benchmarks.json (sirovi brojevi).
# Spark deo radi nad sintetickim tabelama lake.bench.*; traje oko 10 minuta.
param([string[]]$Parts = @())
$ErrorActionPreference = "Continue"
Set-Location $PSScriptRoot

$SparkNoise = "ivy|::|in central|from central|added as a dependency|confs:|artifacts|^\s*\||^\s*-{10,}|The jars for|" +
              "WARN NativeCodeLoader|WARN Utils|Setting default log level|To adjust logging level| INFO |^\s*$|ERROR AppendDataExec"
$sparkParts = @("small_files", "cow_vs_mor", "rollback", "storage", "checksum", "v3")
$pyParts = @("pruning", "checksum", "v3")
$sel = if ($Parts.Count) { $Parts } else { $sparkParts + $pyParts | Select-Object -Unique }

$sp = @($sel | Where-Object { $sparkParts -contains $_ })
if ($sp.Count) {
    Write-Host "`n>>> Spark merenja: $($sp -join ', ')" -ForegroundColor Cyan
    docker exec spark-iceberg spark-submit /home/iceberg/jobs/bench_spark.py @sp 2>&1 | ForEach-Object { "$_" } |
        Where-Object { $_ -notmatch $SparkNoise }
    if ($LASTEXITCODE -ne 0) { throw "Spark merenja nisu uspela" }
}
$py = @($sel | Where-Object { $pyParts -contains $_ })
if ($py.Count) {
    Write-Host "`n>>> PyIceberg / DuckDB / Trino merenja: $($py -join ', ')" -ForegroundColor Cyan
    docker exec explorer python /app/explorer/bench_py.py @py
    if ($LASTEXITCODE -ne 0) { throw "Python merenja nisu uspela" }
}
docker exec explorer python /app/explorer/bench_report.py
Write-Host "`nIzvestaj: results\benchmarks.md" -ForegroundColor Green
