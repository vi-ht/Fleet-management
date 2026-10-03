"""Open-Meteo hourly weather features for NYC taxi demand forecasting."""

from __future__ import annotations

import json
import os
import threading
import time
from datetime import date, timedelta
from pathlib import Path

import requests


LATITUDE = 40.7128
LONGITUDE = -74.0060
TIMEZONE = "America/New_York"
HISTORICAL_URL = "https://historical-forecast-api.open-meteo.com/v1/forecast"
FORECAST_URL = "https://api.open-meteo.com/v1/forecast"
WEATHER_FIELDS = (
    "temperature_2m",
    "precipitation",
    "snowfall",
    "wind_speed_10m",
    "relative_humidity_2m",
    "weather_code",
)
_forecast_lock = threading.Lock()
_forecast_cached_at = 0.0
_forecast_cache: dict[str, dict] = {}
_FORECAST_TTL_SECONDS = 1800


def _hourly_rows(payload: dict) -> list[dict]:
    hourly = payload.get("hourly") or {}
    times = hourly.get("time") or []
    return [
        {"hour": timestamp[:13], **{name: hourly.get(name, [None] * len(times))[index] for name in WEATHER_FIELDS}}
        for index, timestamp in enumerate(times)
    ]


def _request_hourly(url: str, params: dict, timeout: int = 45) -> dict:
    response = requests.get(url, params=params, timeout=timeout)
    response.raise_for_status()
    payload = response.json()
    if not payload.get("hourly", {}).get("time"):
        raise ValueError("Open-Meteo returned no hourly observations")
    return payload


def fetch_historical_weather(start_date: str, end_date: str, cache_dir: str | Path) -> list[dict]:
    """Fetch/cache historical forecast features, using local NYC hours.

    The Historical Forecast API starts in 2021/2022. Dates before 2022 are
    intentionally excluded by the caller so training uses features available
    from the same operational forecast family used at inference time.
    """
    start = max(date.fromisoformat(start_date), date(2022, 1, 1))
    end = date.fromisoformat(end_date)
    if end < start:
        return []
    cache_root = Path(cache_dir)
    cache_root.mkdir(parents=True, exist_ok=True)
    all_rows: dict[str, dict] = {}
    for year in range(start.year, end.year + 1):
        year_start = max(start, date(year, 1, 1))
        year_end = min(end, date(year, 12, 31))
        cache_file = cache_root / f"historical_forecast_{year}_{year_start}_{year_end}.json"
        if cache_file.exists():
            payload = json.loads(cache_file.read_text(encoding="utf-8"))
        else:
            payload = _request_hourly(
                HISTORICAL_URL,
                {
                    "latitude": LATITUDE,
                    "longitude": LONGITUDE,
                    "start_date": year_start.isoformat(),
                    "end_date": year_end.isoformat(),
                    "hourly": ",".join(WEATHER_FIELDS),
                    "timezone": TIMEZONE,
                    "temperature_unit": "celsius",
                    "wind_speed_unit": "kmh",
                    "precipitation_unit": "mm",
                },
            )
            temporary = cache_file.with_suffix(".tmp")
            temporary.write_text(json.dumps(payload), encoding="utf-8")
            temporary.replace(cache_file)
        for row in _hourly_rows(payload):
            all_rows[row["hour"]] = row
    return [all_rows[key] for key in sorted(all_rows)]


def get_forecast(force_refresh: bool = False) -> dict[str, dict]:
    """Return cached hourly NYC forecast data, refreshing at most every 30 min."""
    global _forecast_cached_at, _forecast_cache
    with _forecast_lock:
        now = time.monotonic()
        if not force_refresh and _forecast_cache and now - _forecast_cached_at < _FORECAST_TTL_SECONDS:
            return dict(_forecast_cache)
        try:
            payload = _request_hourly(
                FORECAST_URL,
                {
                    "latitude": LATITUDE,
                    "longitude": LONGITUDE,
                    "hourly": ",".join(WEATHER_FIELDS),
                    "timezone": TIMEZONE,
                    "forecast_days": 16,
                    "temperature_unit": "celsius",
                    "wind_speed_unit": "kmh",
                    "precipitation_unit": "mm",
                },
                timeout=15,
            )
            _forecast_cache = {row["hour"]: row for row in _hourly_rows(payload)}
            _forecast_cached_at = now
        except (requests.RequestException, ValueError, KeyError, TypeError):
            if not _forecast_cache:
                raise
        return dict(_forecast_cache)


def weather_values_for_hour(hour_key: str, forecast: dict[str, dict], fallback: dict | None = None) -> dict:
    """Resolve model inputs; mark and fill forecast gaps with train-set medians."""
    row = forecast.get(hour_key)
    missing = row is None or any(row.get(name) is None for name in WEATHER_FIELDS)
    values = {}
    fallback = fallback or {}
    for name in WEATHER_FIELDS:
        value = row.get(name) if row else None
        if value is None:
            value = fallback.get(name, 0.0)
        values[name] = float(value)
    values["weather_missing"] = 1.0 if missing else 0.0
    values["weather_source"] = "historical_fallback" if missing else "Open-Meteo forecast"
    return values


def historical_baseline_threshold(baseline: dict, zone: str, hour: int, dow: int) -> tuple[float, float, float, int]:
    """Return mean, standard deviation, and conservative 3-sigma score limit."""
    row = baseline.get(f"{zone}|{hour}|{dow}")
    if not row or int(row.get("count", 0)) < 5:
        return 100.0, 0.0, 100.0, int((row or {}).get("count", 0))
    mean = float(row.get("mean", 0.0))
    stddev = float(row.get("stddev", 0.0))
    return mean, stddev, min(100.0, mean + 3.0 * stddev), int(row["count"])


def weather_description(code: int | float | None) -> str:
    """Human-readable label for WMO weather interpretation codes."""
    if code is None:
        return "Không có dữ liệu"
    labels = {
        0: "Trời quang",
        1: "Ít mây",
        2: "Có mây rải rác",
        3: "Nhiều mây",
        45: "Sương mù",
        48: "Sương mù đóng băng",
        51: "Mưa phùn nhẹ",
        53: "Mưa phùn vừa",
        55: "Mưa phùn dày",
        56: "Mưa phùn đóng băng nhẹ",
        57: "Mưa phùn đóng băng dày",
        61: "Mưa nhẹ",
        63: "Mưa vừa",
        65: "Mưa lớn",
        66: "Mưa đóng băng nhẹ",
        67: "Mưa đóng băng lớn",
        71: "Tuyết nhẹ",
        73: "Tuyết vừa",
        75: "Tuyết lớn",
        77: "Hạt tuyết",
        80: "Mưa rào nhẹ",
        81: "Mưa rào vừa",
        82: "Mưa rào mạnh",
        85: "Mưa tuyết nhẹ",
        86: "Mưa tuyết mạnh",
        95: "Dông",
        96: "Dông, mưa đá nhẹ",
        99: "Dông, mưa đá mạnh",
    }
    return labels.get(int(code), "Điều kiện thời tiết khác")
