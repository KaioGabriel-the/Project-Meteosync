import re
import unittest
from datetime import date, datetime, timezone
from unittest.mock import Mock, patch
from zoneinfo import ZoneInfo

from config.cities import CITIES
from scripts.ingest_open_meteo import fetch_weather_with_telemetry
from scripts.ingest_openweather import fetch_openweather
from scripts.utils.date_ranges import previous_complete_date_range


class CityCatalogTests(unittest.TestCase):
    def test_catalog_contains_the_target_capitals(self):
        expected = {
            "Teresina",
            "São Paulo",
            "Rio de Janeiro",
            "Brasília",
            "Salvador",
            "Recife",
            "Manaus",
            "Porto Alegre",
            "Curitiba",
            "Belém",
        }
        self.assertEqual(len(CITIES), 10)
        self.assertEqual({city["city"] for city in CITIES}, expected)

    def test_city_ids_are_unique_slugs(self):
        city_ids = [city["city_id"] for city in CITIES]
        self.assertEqual(len(city_ids), len(set(city_ids)))
        for city_id in city_ids:
            self.assertRegex(city_id, re.compile(r"^[a-z0-9]+(?:_[a-z0-9]+)*$"))

    def test_coordinates_are_numeric_and_in_range(self):
        for city in CITIES:
            with self.subTest(city=city["city"]):
                self.assertIsInstance(city["latitude"], (int, float))
                self.assertIsInstance(city["longitude"], (int, float))
                self.assertGreaterEqual(city["latitude"], -90)
                self.assertLessEqual(city["latitude"], 90)
                self.assertGreaterEqual(city["longitude"], -180)
                self.assertLessEqual(city["longitude"], 180)

    def test_timezones_are_valid_iana_names(self):
        for city in CITIES:
            with self.subTest(city=city["city"]):
                ZoneInfo(city["timezone"])

    def test_date_range_uses_each_city_local_timezone(self):
        utc_instant = datetime(2024, 1, 1, 3, 30, tzinfo=timezone.utc)

        sao_paulo_start, sao_paulo_end = previous_complete_date_range(
            "America/Sao_Paulo",
            now=utc_instant,
        )
        manaus_start, manaus_end = previous_complete_date_range(
            "America/Manaus",
            now=utc_instant,
        )

        self.assertEqual(sao_paulo_end.isoformat(), "2023-12-31")
        self.assertEqual(manaus_end.isoformat(), "2023-12-30")
        self.assertEqual((sao_paulo_end - sao_paulo_start).days, 9)
        self.assertEqual((manaus_end - manaus_start).days, 9)


class IngestionRequestTests(unittest.TestCase):
    def test_open_meteo_request_uses_utc_timezone(self):
        response = Mock(status_code=200, content=b"{}")
        response.json.return_value = {}
        city = next(city for city in CITIES if city["city_id"] == "manaus")
        session = Mock()
        session.get.return_value = response

        with patch("scripts.ingest_open_meteo.get_resilient_session", return_value=session):
            fetch_weather_with_telemetry(city, date(2024, 1, 1), date(2024, 1, 10))

        self.assertEqual(
            session.get.call_args.kwargs["params"]["timezone"],
            "UTC",
        )

    def test_openweather_request_uses_kelvin_and_city_coordinates(self):
        response = Mock(status_code=200, content=b"{}")
        response.json.return_value = {"main": {"temp": 300}, "weather": [{"id": 800}]}
        session = Mock()
        session.get.return_value = response
        city = next(city for city in CITIES if city["city_id"] == "manaus")

        fetch_openweather(city, api_key="test-key", session=session)

        params = session.get.call_args.kwargs["params"]
        self.assertEqual(params["lat"], city["latitude"])
        self.assertEqual(params["lon"], city["longitude"])
        self.assertEqual(params["appid"], "test-key")
        self.assertEqual(params["units"], "standard")


if __name__ == "__main__":
    unittest.main()
