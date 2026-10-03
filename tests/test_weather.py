import unittest

from weather import (
    _hourly_rows,
    historical_baseline_threshold,
    weather_description,
    weather_values_for_hour,
)


class WeatherFeatureTests(unittest.TestCase):
    def test_hourly_rows_use_local_hour_keys_and_keep_nulls(self):
        payload = {
            "hourly": {
                "time": ["2025-01-01T00:00", "2025-01-01T01:00"],
                "temperature_2m": [1.0, None],
                "precipitation": [0.0, 2.0],
            }
        }
        rows = _hourly_rows(payload)
        self.assertEqual(rows[0]["hour"], "2025-01-01T00")
        self.assertEqual(rows[1]["temperature_2m"], None)

    def test_missing_forecast_uses_training_medians_and_is_marked(self):
        result = weather_values_for_hour(
            "2025-01-01T01", {}, {"temperature_2m": 10, "weather_code": 3}
        )
        self.assertEqual(result["temperature_2m"], 10.0)
        self.assertEqual(result["weather_code"], 3.0)
        self.assertEqual(result["weather_missing"], 1.0)
        self.assertEqual(result["weather_source"], "historical_fallback")

    def test_present_forecast_is_marked_as_live_source(self):
        values = {"temperature_2m": 4, "precipitation": 1, "snowfall": 0,
                  "wind_speed_10m": 5, "relative_humidity_2m": 80, "weather_code": 61}
        result = weather_values_for_hour("2025-01-01T01", {"2025-01-01T01": values})
        self.assertEqual(result["weather_missing"], 0.0)
        self.assertEqual(result["weather_source"], "Open-Meteo forecast")

    def test_anomaly_limit_uses_three_sigma_and_minimum_support(self):
        baseline = {"132|8|1": {"mean": 20, "stddev": 10, "count": 20}}
        self.assertEqual(historical_baseline_threshold(baseline, "132", 8, 1), (20, 10, 50, 20))
        sparse = {"132|8|1": {"mean": 20, "stddev": 2, "count": 4}}
        self.assertEqual(historical_baseline_threshold(sparse, "132", 8, 1)[2:], (100.0, 4))

    def test_weather_code_has_readable_label(self):
        self.assertEqual(weather_description(71), "Tuyết nhẹ")
        self.assertEqual(weather_description(0), "Trời quang")


if __name__ == "__main__":
    unittest.main()
