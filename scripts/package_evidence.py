"""Copy reproducible run evidence into a small, versionable project bundle."""

from __future__ import annotations

import hashlib
import json
from pathlib import Path
from shutil import copy2


ROOT = Path(__file__).resolve().parents[1]
DESTINATION = ROOT / "output" / "evidence"

SOURCES = (
    "evidence/runtime_capture_2026-10-04.json",
    "evidence/fleet_movement_capture_2026-10-04.json",
    "evidence/model_metrics.json",
    "evidence/runtime_metrics.json",
    "evidence/data_profile.json",
    "evidence/verification_summary.json",
    "evidence/docx_render_check.json",
    "evidence/hdfs_raw_parquet_files.txt",
    "evidence/hdfs_raw_checksums.txt",
    "evidence/hdfs_curated_parquet.txt",
    "evidence/kafka_topics.txt",
    "evidence/mongo_hotspot_count.json",
    "evidence/dispatch_hotspots.json",
    "evidence/upcoming_hotspots.json",
    "evidence/vehicles_before.json",
    "evidence/vehicles_after.json",
    "evidence/vehicle_movement.json",
    "evidence/dashboard_health.json",
    "evidence/dashboard_snapshot.json",
    "data/results/model_metrics.json",
    "data/results/hadoop_streaming.log",
    "data/results/mapreduce_actual.csv",
    "data/results/mapreduce_expected.csv",
    "data/results/spark_query_results.json",
    "data/results/spark_query_plans.txt",
    "data/results/performance_benchmark.json",
)

PRESENTATION_ARTIFACTS = (
    "output/playwright/weather-model-dashboard-2026-10-04.png",
    "output/playwright/weather-model-map-2026-10-04.png",
    "output/playwright/weather-model-mobile-2026-10-04.png",
    "output/playwright/console-log-evidence-2026-10-04.png",
    "output/playwright/browser-qa-375.png",
    "output/playwright/browser-qa-768.png",
    "output/playwright/browser-qa-1440.png",
    "output/evidence/browser_qa_summary_2026-10-04.json",
    "output/evidence/dependency_audit.json",
    "output/documents/BDA501_Final_Project_Report_Submission.docx",
)


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def main() -> None:
    DESTINATION.mkdir(parents=True, exist_ok=True)
    for relative in SOURCES:
        source = ROOT / relative
        if not source.is_file():
            raise FileNotFoundError(f"Required evidence is missing: {relative}")
        destination = DESTINATION / source.name
        copy2(source, destination)
        if source.suffix in {".log", ".txt"}:
            # Drop only insignificant line-end whitespace from tool output so
            # evidence remains readable in diffs; counter values stay intact.
            lines = destination.read_text(encoding="utf-8").splitlines()
            while lines and not lines[-1]:
                lines.pop()
            destination.write_text(
                "\n".join(line.rstrip(" \t") for line in lines) + "\n",
                encoding="utf-8",
            )

    paths = [DESTINATION / Path(item).name for item in SOURCES]
    paths.extend(ROOT / item for item in PRESENTATION_ARTIFACTS)
    artifacts = []
    for path in paths:
        if path.is_file():
            artifacts.append(
                {
                    "path": path.relative_to(ROOT).as_posix(),
                    "sha256": sha256(path),
                }
            )

    manifest = DESTINATION / "artifact_checksums.json"
    manifest.write_text(
        json.dumps(artifacts, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )
    print(f"[PASS] Packaged {len(paths)} runtime/presentation evidence files")
    print(f"[PASS] SHA-256 manifest: {manifest.relative_to(ROOT).as_posix()}")


if __name__ == "__main__":
    main()
