"""Build runtime summaries and checksums from collected evidence JSON/text."""

from __future__ import annotations

import hashlib
import json
import sys
from pathlib import Path


def read_json(path: Path):
    return json.loads(path.read_text(encoding="utf-8-sig"))


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def main(root: Path) -> None:
    evidence = root / "evidence"
    output = root / "output" / "playwright"
    output.mkdir(parents=True, exist_ok=True)

    verified = read_json(evidence / "verification_summary.json")
    metrics = read_json(evidence / "model_metrics.json")
    profile = read_json(evidence / "data_profile.json")
    dashboard = read_json(evidence / "dashboard_snapshot.json")
    dispatch = read_json(evidence / "dispatch_hotspots.json")
    upcoming = read_json(evidence / "upcoming_hotspots.json")
    before = read_json(evidence / "vehicles_before.json")
    after = read_json(evidence / "vehicles_after.json")
    movement = read_json(evidence / "vehicle_movement.json")
    artifact = read_json(evidence / "model_artifact.json")
    mongo = read_json(evidence / "mongo_hotspot_count.json")

    runtime = {
        "collected_at": verified["collected_at"],
        "dashboard_health": verified["dashboard_health"],
        "prediction_records": int(dashboard["summary"]["count"]),
        "zones_with_data": int(dashboard["summary"]["zones"]),
        "latest_prediction_time": dashboard["summary"].get("latest"),
        "mongo_prediction_records": int(mongo["count"]),
        "fleet_vehicle_count": int(after["count"]),
        "fleet_movement_verified": bool(movement["changed"]),
        "upcoming_forecast_horizons": [row["label"] for row in upcoming["forecasts"]],
        "hotspot_zone_ranking": [
            {"zone": row["zone"], "score": row["hotspot_score"], "samples": row["sample_count"]}
            for row in dispatch["hotspots"][:5]
        ],
        "model_evaluation": metrics,
        "hdfs_raw_parquet_file_count": sum(
            "yellow_tripdata_" in line and ".parquet" in line
            for line in (evidence / "hdfs_raw_parquet_files.txt").read_text(encoding="utf-8-sig").splitlines()
        ),
        "hdfs_raw_trip_rows": int(profile["raw"]["row_count"]),
        "hdfs_curated_trip_rows": int(profile["curated"]["row_count"]),
        "source_pickup_date_min": profile["raw"]["pickup_datetime_min"],
        "source_pickup_date_max": profile["raw"]["pickup_datetime_max"],
        "curated_distinct_pickup_zones": int(profile["curated"]["distinct_pickup_zones"]),
        "kafka_topics": [
            item for item in (evidence / "kafka_topics.txt").read_text(encoding="utf-8-sig").splitlines()
            if item and item != "__consumer_offsets"
        ],
        "model_artifact": artifact,
        "vehicle_id_checked": movement["vehicle_id"],
        "vehicle_before": before["vehicles"][0] if before.get("vehicles") else None,
        "vehicle_after": after["vehicles"][0] if after.get("vehicles") else None,
    }
    (evidence / "runtime_metrics.json").write_text(
        json.dumps(runtime, ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
    )

    summary = [
        f"collected_at={runtime['collected_at']}",
        f"dashboard_health={runtime['dashboard_health']}",
        f"prediction_records={runtime['prediction_records']}",
        f"zones_with_data={runtime['zones_with_data']}",
        f"mongo_prediction_records={runtime['mongo_prediction_records']}",
        f"fleet_vehicle_count={runtime['fleet_vehicle_count']}",
        f"fleet_movement_verified={runtime['fleet_movement_verified']}",
        f"upcoming_forecast_horizons={', '.join(runtime['upcoming_forecast_horizons'])}",
        f"hdfs_raw_parquet_file_count={runtime['hdfs_raw_parquet_file_count']}",
        f"hdfs_raw_trip_rows={runtime['hdfs_raw_trip_rows']}",
        f"hdfs_curated_trip_rows={runtime['hdfs_curated_trip_rows']}",
        f"source_pickup_date_range={runtime['source_pickup_date_min']}..{runtime['source_pickup_date_max']}",
        f"curated_distinct_pickup_zones={runtime['curated_distinct_pickup_zones']}",
        f"kafka_topics={', '.join(runtime['kafka_topics'])}",
        f"model_evaluation={metrics.get('evaluation_method')}",
        f"weather_source={metrics.get('weather_source')}",
        f"weather_coverage={metrics.get('weather_coverage')}",
        f"weather_model_selected={metrics.get('weather_selected_by_temporal_holdout')}",
        f"calendar_rmse={metrics.get('temporal_holdout_comparison', {}).get('calendar_baseline', {}).get('RMSE')}",
        f"weather_candidate_rmse={metrics.get('temporal_holdout_comparison', {}).get('weather_candidate', {}).get('RMSE')}",
        f"test_period={metrics.get('test_start_date')}..{metrics.get('test_end_date')}",
        f"training_rows={metrics.get('training_rows')}",
        f"evaluation_rows={metrics.get('evaluation_rows')}",
        f"rmse={metrics.get('rmse')}",
        f"mae={metrics.get('mae')}",
    ]
    (output / "verification-summary.txt").write_text("\n".join(summary) + "\n", encoding="utf-8")
    (output / "dispatch-hotspots.json").write_bytes((evidence / "dispatch_hotspots.json").read_bytes())

    checksum_paths = (
        "evidence/verification_summary.json", "evidence/runtime_metrics.json",
        "evidence/model_metrics.json", "evidence/data_profile.json", "evidence/forced_training.log",
        "evidence/hdfs_raw_parquet_files.txt", "evidence/hdfs_raw_checksums.txt",
        "data/results/hadoop_streaming.log", "data/results/mapreduce_actual.csv",
        "data/results/spark_query_results.json", "data/results/spark_query_plans.txt",
        "data/results/performance_benchmark.json", "evidence/mongo_hotspot_count.json",
        "output/playwright/verification-summary.txt", "output/playwright/runtime-dashboard-2026-10-03.png",
        "output/playwright/map-hotspots.png", "output/playwright/dispatch-hotspots.json",
        "output/playwright/weather-model-dashboard-2026-10-03.png",
        "output/playwright/weather-model-map-2026-10-03.png",
        "output/playwright/weather-model-mobile-2026-10-03.png",
        "output/playwright/weather-model-dashboard-2026-10-04.png",
        "output/playwright/weather-model-mobile-2026-10-04.png",
    )
    manifest = [
        {"path": item, "sha256": sha256(root / item)}
        for item in checksum_paths if (root / item).is_file()
    ]
    (evidence / "artifact_checksums.json").write_text(
        json.dumps(manifest, ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
    )
    print(f"[PASS] Evidence summary refreshed at {runtime['collected_at']}")
    print(f"[PASS] Hotspot records: {runtime['prediction_records']}")
    print(f"[PASS] Fleet movement: {runtime['fleet_movement_verified']}")


if __name__ == "__main__":
    main(Path(sys.argv[1]).resolve())
