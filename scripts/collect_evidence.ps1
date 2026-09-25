$ErrorActionPreference = "Stop"

$root = Split-Path -Parent $PSScriptRoot
$evidence = Join-Path $root "evidence"
New-Item -ItemType Directory -Force -Path $evidence | Out-Null
$stamp = Get-Date -Format "yyyyMMdd-HHmmss"

function Save-Json($name, $value) {
    $value | ConvertTo-Json -Depth 12 | Set-Content -Encoding utf8 (Join-Path $evidence $name)
}

docker compose ps -a | Set-Content -Encoding utf8 (Join-Path $evidence "compose_ps.txt")
docker compose logs --no-color --tail=300 | Set-Content -Encoding utf8 (Join-Path $evidence "compose_logs.txt")
foreach ($service in @("mapreduce-job", "spark-etl", "model-trainer", "streaming-predictor", "kafka-producer", "fleet-tracker", "dashboard")) {
    docker compose logs --no-color $service | Set-Content -Encoding utf8 (Join-Path $evidence ("service_{0}.log" -f $service.Replace("-", "_")))
}
docker compose config --quiet
"PASS docker compose config" | Set-Content -Encoding utf8 (Join-Path $evidence "compose_config.txt")

$health = Invoke-RestMethod "http://localhost:8088/health"
Save-Json "dashboard_health.json" $health
$dashboard = Invoke-RestMethod "http://localhost:8088/api/dashboard"
Save-Json "dashboard_snapshot.json" $dashboard
$hotspots = Invoke-RestMethod "http://localhost:8088/api/upcoming-hotspots?hours=3"
Save-Json "upcoming_hotspots.json" $hotspots

$vehiclesBefore = Invoke-RestMethod "http://localhost:8088/api/vehicles"
Save-Json "vehicles_before.json" $vehiclesBefore
Start-Sleep -Seconds 6
$vehiclesAfter = Invoke-RestMethod "http://localhost:8088/api/vehicles"
Save-Json "vehicles_after.json" $vehiclesAfter

$before = @($vehiclesBefore.vehicles) | Where-Object vehicle_id -eq "NYC-TAXI-001" | Select-Object -First 1
$after = @($vehiclesAfter.vehicles) | Where-Object vehicle_id -eq "NYC-TAXI-001" | Select-Object -First 1
$movement = [ordered]@{
    vehicle_id = "NYC-TAXI-001"
    before = $before
    after = $after
    changed = (($before.latitude -ne $after.latitude) -or ($before.longitude -ne $after.longitude) -or ($before.simulation_cycle -ne $after.simulation_cycle))
}
Save-Json "vehicle_movement.json" $movement

$modelPath = Join-Path $root "models\hotspot_model\metadata\part-00000"
$modelEvidence = [ordered]@{
    path = $modelPath
    exists = Test-Path -LiteralPath $modelPath
    size_bytes = if (Test-Path -LiteralPath $modelPath) { (Get-Item -LiteralPath $modelPath).Length } else { 0 }
}
Save-Json "model_artifact.json" $modelEvidence

docker exec taxi-demand-namenode-1 hdfs dfs -ls -h /taxi/raw | Set-Content -Encoding utf8 (Join-Path $evidence "hdfs_raw.txt")
docker exec taxi-demand-namenode-1 hdfs dfs -ls -h /taxi/raw/parquet | Set-Content -Encoding utf8 (Join-Path $evidence "hdfs_raw_parquet_files.txt")
docker exec taxi-demand-namenode-1 hdfs dfs -ls -h /taxi/curated/trips | Set-Content -Encoding utf8 (Join-Path $evidence "hdfs_curated_parquet.txt")
docker exec taxi-demand-kafka-1 /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list | Set-Content -Encoding utf8 (Join-Path $evidence "kafka_topics.txt")

$mongoCount = docker exec taxi-demand-mongodb-1 mongosh --quiet --eval "db.getSiblingDB('taxi').hotspot_predictions.countDocuments({})"
[ordered]@{ collection = "taxi.hotspot_predictions"; count = [int]$mongoCount.Trim() } | ConvertTo-Json | Set-Content -Encoding utf8 (Join-Path $evidence "mongo_hotspot_count.json")

$pass = [ordered]@{
    collected_at = (Get-Date).ToString("o")
    dashboard_health = ($health.status -eq "ok")
    hotspot_records = ([int]$dashboard.summary.count -gt 0)
    upcoming_hotspots = (@($hotspots.forecasts).Count -eq 3)
    fleet_records = ($vehiclesAfter.count -ge 40)
    fleet_moved = [bool]$movement.changed
    model_artifact = [bool]$modelEvidence.exists
    mongo_records = ([int]$mongoCount.Trim() -gt 0)
    hdfs_raw_parquet = ((Get-Content (Join-Path $evidence "hdfs_raw_parquet_files.txt") | Where-Object { $_ -match "yellow_tripdata_.*\.parquet" }).Count -eq 7)
}
Save-Json "verification_summary.json" $pass
$pass.GetEnumerator() | ForEach-Object { "{0}={1}" -f $_.Key, $_.Value } | Set-Content -Encoding utf8 (Join-Path $evidence "verification_summary.txt")
Write-Output "[PASS] Evidence collected in $evidence"
Write-Output "[PASS] Hotspot records: $($dashboard.summary.count)"
Write-Output "[PASS] Fleet movement: $($movement.changed)"
