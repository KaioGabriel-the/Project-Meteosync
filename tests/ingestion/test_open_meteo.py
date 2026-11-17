import unittest
from datetime import date, datetime, timezone
from unittest.mock import Mock, patch

from config.cities import CITIES
from scripts.ingest_open_meteo import (
    BASE_URL,
    MAX_HISTORICAL_DAYS,
    fetch_weather_with_telemetry,
    get_resilient_session,
    ingest_city,
    resolve_date_range,
)


class IngestionRangeTests(unittest.TestCase):
    def test_default_range_is_thirty_complete_utc_days(self):
        run_at = datetime(2024, 1, 31, 12, tzinfo=timezone.utc)

        start, end = resolve_date_range(now=run_at)

        self.assertEqual((end - start).days + 1, MAX_HISTORICAL_DAYS)
        self.assertEqual((start, end), (date(2024, 1, 1), date(2024, 1, 30)))

    def test_incremental_range_uses_current_utc_date(self):
        run_at = datetime(2024, 1, 1, 2, tzinfo=timezone.utc)

        self.assertEqual(
            resolve_date_range(incremental=True, now=run_at),
            (date(2024, 1, 1), date(2024, 1, 1)),
        )

    def test_rejects_ranges_longer_than_thirty_days(self):
        with self.assertRaises(ValueError):
            resolve_date_range(date(2024, 1, 1), date(2024, 1, 31))

    def test_rejects_naive_run_timestamp(self):
        with self.assertRaises(ValueError):
            resolve_date_range(now=datetime(2024, 1, 1))


class OpenMeteoRequestTests(unittest.TestCase):
    def test_request_records_telemetry_and_requests_utc(self):
        response = Mock(
            status_code=200,
            content=b'{"hourly":{"time":["2024-01-01T00:00"]}}',
        )
        response.json.return_value = {"hourly": {"time": ["2024-01-01T00:00"]}}
        session = Mock()
        session.get.return_value = response

        payload, telemetry = fetch_weather_with_telemetry(
            CITIES[0],
            date(2024, 1, 1),
            date(2024, 1, 30),
            session=session,
        )

        self.assertEqual(payload["hourly"]["time"], ["2024-01-01T00:00"])
        self.assertEqual(session.get.call_args.kwargs["params"]["timezone"], "UTC")
        self.assertEqual(telemetry["status_code"], 200)
        self.assertEqual(telemetry["payload_size_bytes"], len(response.content))
        self.assertIn("latency_ms", telemetry)
        self.assertTrue(telemetry["request_timestamp"].endswith("Z"))

    def test_incremental_request_asks_for_last_available_hour(self):
        response = Mock(
            status_code=200,
            content=b'{"hourly":{"time":["2024-01-01T00:00"]}}',
        )
        response.json.return_value = {"hourly": {"time": ["2024-01-01T00:00"]}}
        session = Mock()
        session.get.return_value = response

        fetch_weather_with_telemetry(
            CITIES[0],
            None,
            None,
            session=session,
            incremental=True,
        )

        params = session.get.call_args.kwargs["params"]
        self.assertEqual(params["past_hours"], 1)
        self.assertEqual(params["forecast_hours"], 0)
        self.assertNotIn("start_date", params)
        self.assertNotIn("end_date", params)

    def test_request_rejects_more_than_thirty_days(self):
        with self.assertRaises(ValueError):
            fetch_weather_with_telemetry(CITIES[0], date(2024, 1, 1), date(2024, 1, 31))

    def test_request_session_retries_transient_http_errors(self):
        session = get_resilient_session()

        retries = session.get_adapter(BASE_URL).max_retries

        self.assertEqual(retries.total, 5)
        self.assertEqual(retries.backoff_factor, 2)
        self.assertIn(429, retries.status_forcelist)

    def test_incremental_ingestion_does_not_pass_historical_dates(self):
        storage = Mock()
        storage.upload_json.return_value = "s3a://bronze/open_meteo/sample.json"
        run_at = datetime(2024, 1, 1, 2, tzinfo=timezone.utc)
        response_data = {"hourly": {"time": ["2024-01-01T01:00"]}}
        telemetry = {"latency_ms": 1}

        with patch(
            "scripts.ingest_open_meteo.fetch_weather_with_telemetry",
            return_value=(response_data, telemetry),
        ) as fetch:
            succeeded = ingest_city(
                CITIES[0],
                storage,
                incremental=True,
                run_at=run_at,
            )

        self.assertTrue(succeeded)
        self.assertIsNone(fetch.call_args.args[1])
        self.assertIsNone(fetch.call_args.args[2])
        self.assertTrue(fetch.call_args.kwargs["incremental"])
