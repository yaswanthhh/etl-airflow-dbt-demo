param(
    [switch]$Yes
)

# Adjust these if your folders differ
$csvPaths = @(
    ".\data\incoming\listings\*.csv",
    ".\data\processed\listings\*.csv",
    ".\data\incoming\weather\*.csv",
    ".\data\processed\weather\*.csv"
)

if (-not $Yes) {
    Write-Host "This will:"
    Write-Host "  1) Delete CSV files under data/incoming and data/processed"
    Write-Host "  2) Delete Docker volumes (Postgres data + Airflow metadata)"
    $confirm = Read-Host "Type YES to continue"
    if ($confirm -ne "YES") { Write-Host "Cancelled."; exit 1 }
}

# 1) Delete CSVs (ignore missing paths)
foreach ($p in $csvPaths) {
    Remove-Item $p -Force -ErrorAction SilentlyContinue
}

# Optional: delete empty folders too (keep root /data)
Get-ChildItem .\data -Recurse -Directory -ErrorAction SilentlyContinue |
Sort-Object FullName -Descending |
Remove-Item -Force -Recurse -ErrorAction SilentlyContinue

# 2) Reset containers + volumes
docker compose down --volumes
docker compose up -d --build
docker compose ps
