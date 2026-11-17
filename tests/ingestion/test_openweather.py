import unittest
from datetime import datetime, timezone
from unittest.mock import Mock, patch

import requests

from config.cities import CITIES
from scripts.ingest_openweather import fetch_openweather, ingest_city


class OpenWeatherRequestTests(unittest.TestCase):
    def test_http_401_and_429_are_raised(self):
        for status_code in (401, 429):
            with self.subTest(status_code=status_code):
                response = Mock(status_code=status_code)
                response.raise_for_status.side_effect = requests.HTTPError(
                    f"HTTP {status_code}"
                )
                session = Mock()
                session.get.return_value = response

                with self.assertRaises(requests.HTTPError):
                    fetch_openweather(CITIES[0], api_key="test-key", session=session)

    def test_ingestion_preserves_openweather_payload(self):
        storage = Mock()
        storage.upload_json.return_value = "s3a://bronze/openweather/sample.json"
        run_at = datetime(2024, 1, 1, tzinfo=timezone.utc)
        raw_data = {
            "main": {"temp": 300},
            "wind": {"speed": 3},
            "weather": [{"id": 800, "main": "Clear"}],
            "sys": {"country": "BR"},
        }
        telemetry = {"latency_ms": 1}

        with patch(
            "scripts.ingest_openweather.fetch_openweather",
            return_value=(raw_data, telemetry),
        ):
            succeeded = ingest_city(CITIES[0], storage, run_at)

        self.assertTrue(succeeded)
        payload = storage.upload_json.call_args.args[0]
        self.assertEqual(payload["data"], raw_data)
        self.assertEqual(payload["telemetry"], telemetry)
        self.assertTrue(storage.upload_json.call_args.args[1].startswith("openweather/"))
