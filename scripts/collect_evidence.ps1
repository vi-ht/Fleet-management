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

docker exec -e "SPARK_MASTER_URL=local[2]" taxi-demand-spark-master-1 /opt/spark/bin/spark-submit --master "local[2]" /workspace/scripts/profile_data_evidence.py | Set-Content -Encoding utf8 (Join-Path $evidence "data_profile.json")
if ($LASTEXITCODE -ne 0) { throw "Spark data profile failed with exit code $LASTEXITCODE" }

$health = Invoke-RestMethod "http://localhost:8088/health"
Save-Json "dashboard_health.json" $health
$dashboard = Invoke-RestMethod "http://localhost:8088/api/dashboard"
Save-Json "dashboard_snapshot.json" $dashboard
$dispatch = Invoke-RestMethod "http://localhost:8088/api/dispatch"
Save-Json "dispatch_hotspots.json" $dispatch
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
$modelDir = Join-Path $root "models\hotspot_model"
$modelEvidence = [ordered]@{
    path = $modelPath
    exists = Test-Path -LiteralPath $modelPath
    size_bytes = if (Test-Path -LiteralPath $modelPath) { (Get-Item -LiteralPath $modelPath).Length } else { 0 }
    last_write_time = if (Test-Path -LiteralPath $modelPath) { (Get-Item -LiteralPath $modelPath).LastWriteTime.ToString("o") } else { $null }
    metadata_sha256 = if (Test-Path -LiteralPath $modelPath) { (Get-FileHash -LiteralPath $modelPath -Algorithm SHA256).Hash } else { $null }
    artifact_file_count = if (Test-Path -LiteralPath $modelDir) { @(Get-ChildItem -LiteralPath $modelDir -File -Recurse).Count } else { 0 }
    artifact_total_bytes = if (Test-Path -LiteralPath $modelDir) { (Get-ChildItem -LiteralPath $modelDir -File -Recurse | Measure-Object -Property Length -Sum).Sum } else { 0 }
}
Save-Json "model_artifact.json" $modelEvidence

$metricsPath = Join-Path $root "data\results\model_metrics.json"
if (Test-Path -LiteralPath $metricsPath) {
    Save-Json "model_metrics.json" (Get-Content -Raw -LiteralPath $metricsPath | ConvertFrom-Json)
}

docker exec taxi-demand-namenode-1 hdfs dfs -ls -h /taxi/raw | Set-Content -Encoding utf8 (Join-Path $evidence "hdfs_raw.txt")
docker exec taxi-demand-namenode-1 hdfs dfs -ls -h /taxi/raw/parquet | Set-Content -Encoding utf8 (Join-Path $evidence "hdfs_raw_parquet_files.txt")
docker exec taxi-demand-namenode-1 hdfs dfs -checksum /taxi/raw/parquet/yellow_tripdata_*.parquet | Set-Content -Encoding utf8 (Join-Path $evidence "hdfs_raw_checksums.txt")
docker exec taxi-demand-namenode-1 hdfs dfs -ls -h /taxi/curated/trips | Set-Content -Encoding utf8 (Join-Path $evidence "hdfs_curated_parquet.txt")
docker exec taxi-demand-kafka-1 /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list | Set-Content -Encoding utf8 (Join-Path $evidence "kafka_topics.txt")

$mongoCount = docker exec taxi-demand-mongodb-1 mongosh --quiet --eval "db.getSiblingDB('taxi').hotspot_predictions.countDocuments({})"
[ordered]@{ collection = "taxi.hotspot_predictions"; count = [int]$mongoCount.Trim() } | ConvertTo-Json | Set-Content -Encoding utf8 (Join-Path $evidence "mongo_hotspot_count.json")

$pass = [ordered]@{
    collected_at = (Get-Date).ToString("o")
    dashboard_health = ($health.status -eq "ok")
    hotspot_records = ([int]$dashboard.summary.count -gt 0)
    hotspot_explanations = (@($dispatch.hotspots).Count -gt 0 -and @($dispatch.hotspots | Where-Object { [int]$_.sample_count -le 0 }).Count -eq 0)
    upcoming_hotspots = (@($hotspots.forecasts).Count -eq 3)
    open_meteo_forecast = [bool]$hotspots.weather_available
    weather_model_selected = [bool]$hotspots.weather_used_by_model
    fleet_records = ($vehiclesAfter.count -ge 40)
    fleet_moved = [bool]$movement.changed
    model_artifact = [bool]$modelEvidence.exists
    mongo_records = ([int]$mongoCount.Trim() -gt 0)
    hdfs_raw_parquet = ((Get-Content (Join-Path $evidence "hdfs_raw_parquet_files.txt") | Where-Object { $_ -match "yellow_tripdata_.*\.parquet" }).Count -eq 7)
    hadoop_streaming_output_verified = ((Get-Content (Join-Path $root "data\results\hadoop_streaming.log") -Raw) -match "independent Counter baseline")
    spark_query_outputs_present = ((Test-Path (Join-Path $root "data\results\spark_query_results.json")) -and (Test-Path (Join-Path $root "data\results\spark_query_plans.txt")))
    performance_outputs_identical = [bool](Get-Content -Raw -LiteralPath (Join-Path $root "data\results\performance_benchmark.json") | ConvertFrom-Json).outputs_identical
}
Save-Json "verification_summary.json" $pass
python (Join-Path $PSScriptRoot "summarize_evidence.py") $root
if ($LASTEXITCODE -ne 0) { throw "Evidence summary failed with exit code $LASTEXITCODE" }
Write-Output "[PASS] Evidence collected in $evidence"
