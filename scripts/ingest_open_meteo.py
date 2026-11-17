import argparse
import time
from datetime import date, datetime, timezone
from pathlib import Path
import requests
from requests.adapters import HTTPAdapter
from urllib3.util import Retry

from botocore.exceptions import BotoCoreError, ClientError

from config.cities import CITIES
from scripts.utils.date_ranges import previous_complete_date_range
from scripts.utils.logging_config import configure_ingestion_logger
from scripts.utils.s3_client import S3ClientWrapper

BASE_URL = "https://api.open-meteo.com/v1/forecast"
PROJECT_ROOT = Path(__file__).resolve().parents[1]
MAX_HISTORICAL_DAYS = 30

HOURLY_VARIABLES = [
    "temperature_2m",
    "relative_humidity_2m",
    "precipitation",
    "surface_pressure",
    "wind_speed_10m",
    "weather_code",
]

logger = configure_ingestion_logger(
    __name__,
    PROJECT_ROOT / "logs" / "ingest_open_meteo.log",
)


def get_resilient_session() -> requests.Session:
    """Configura sessão HTTP com políticas de Retry e Exponential Backoff."""
    session = requests.Session()
    retries = Retry(
        total=5,
        backoff_factor=2,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=frozenset({"GET"}),
        respect_retry_after_header=True,
        raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retries)
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    return session


def fetch_weather_with_telemetry(
    city: dict,
    start_date: date | None,
    end_date: date | None,
    session: requests.Session | None = None,
    incremental: bool = False,
) -> tuple[dict, dict]:
    session = session or get_resilient_session()
    params = {
        "latitude": city["latitude"],
        "longitude": city["longitude"],
        "hourly": ",".join(HOURLY_VARIABLES),
        "timezone": "UTC",
        "temperature_unit": "celsius",
        "wind_speed_unit": "kmh",
        "precipitation_unit": "mm",
    }
    if incremental:
        if start_date is not None or end_date is not None:
            raise ValueError("Não combine --incremental com datas históricas.")
        params.update({"past_hours": 1, "forecast_hours": 0})
    else:
        if start_date is None or end_date is None:
            raise ValueError("Datas são obrigatórias para a ingestão histórica.")
        if start_date > end_date:
            raise ValueError("start_date não pode ser posterior a end_date.")
        if (end_date - start_date).days + 1 > MAX_HISTORICAL_DAYS:
            raise ValueError(f"A janela de ingestão não pode exceder {MAX_HISTORICAL_DAYS} dias.")
        params.update(
            {
                "start_date": start_date.isoformat(),
                "end_date": end_date.isoformat(),
            }
        )

    request_timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    start_time = time.monotonic()
    response = session.get(BASE_URL, params=params, timeout=60)
    latency_ms = int((time.monotonic() - start_time) * 1000)

    try:
        response.raise_for_status()
    except requests.HTTPError:
        logger.error("Open-Meteo respondeu HTTP %s.", response.status_code)
        raise
    payload = response.json()
    if not isinstance(payload, dict):
        raise ValueError("A API Open-Meteo retornou um payload JSON inválido.")

    telemetry = {
        "request_timestamp": request_timestamp,
        "status_code": response.status_code,
        "latency_ms": latency_ms,
        "payload_size_bytes": len(response.content),
    }
    return payload, telemetry


def resolve_date_range(
    start_date: date | None = None,
    end_date: date | None = None,
    incremental: bool = False,
    now: datetime | None = None,
) -> tuple[date, date]:
    current_time = now or datetime.now(timezone.utc)
    if current_time.tzinfo is None:
        raise ValueError("now precisa incluir fuso horário.")
    utc_today = current_time.astimezone(timezone.utc).date()

    if incremental:
        if start_date is not None or end_date is not None:
            raise ValueError("Não combine --incremental com datas históricas.")
        return utc_today, utc_today

    if (start_date is None) != (end_date is None):
        raise ValueError("Informe start_date e end_date juntos.")
    if start_date is not None and end_date is not None:
        if start_date > end_date:
            raise ValueError("start_date não pode ser posterior a end_date.")
        if (end_date - start_date).days + 1 > MAX_HISTORICAL_DAYS:
            raise ValueError(f"A janela de ingestão não pode exceder {MAX_HISTORICAL_DAYS} dias.")
        return start_date, end_date

    return previous_complete_date_range("UTC", days=MAX_HISTORICAL_DAYS, now=current_time)


def ingest_city(
    city: dict,
    s3_wrapper: S3ClientWrapper,
    start_date: date | None = None,
    end_date: date | None = None,
    incremental: bool = False,
    run_at: datetime | None = None,
    session: requests.Session | None = None,
) -> bool:
    city_name = city["city"]
    try:
        range_start, range_end = resolve_date_range(start_date, end_date, incremental, run_at)
        ingestion_time = run_at or datetime.now(timezone.utc)
        if ingestion_time.tzinfo is None:
            raise ValueError("run_at precisa incluir fuso horário.")
        ingestion_time = ingestion_time.astimezone(timezone.utc)
        logger.info("Iniciando coleta Open-Meteo: %s | %s até %s", city_name, range_start, range_end)

        data, telemetry = fetch_weather_with_telemetry(
            city,
            None if incremental else range_start,
            None if incremental else range_end,
            session=session,
            incremental=incremental,
        )

        hourly_data = data.get("hourly")
        if not isinstance(hourly_data, dict) or not hourly_data.get("time"):
            raise ValueError("A resposta da API não contém dados horários.")

        enrichment_payload = {
            "telemetry": telemetry,
            "metadata": {
                "city_id": city["city_id"],
                "city_name": city_name,
                "timezone": "UTC",
                "start_date": range_start.isoformat(),
                "end_date": range_end.isoformat(),
            },
            "data": data,
        }

        s3_key = (
            f"open_meteo/year={ingestion_time.year}/month={ingestion_time.month:02d}/"
            f"day={ingestion_time.day:02d}/open_meteo_{city['city_id']}_"
            f"{ingestion_time.strftime('%Y%m%dT%H%M%SZ')}.json"
        )

        destination = s3_wrapper.upload_json(enrichment_payload, s3_key)
        logger.info(
            "Coleta Open-Meteo concluída para %s | Latência: %dms | Destino: %s",
            city_name,
            telemetry["latency_ms"],
            destination,
        )
        return True

    except (
        requests.RequestException,
        BotoCoreError,
        ClientError,
        ValueError,
        KeyError,
        OSError,
    ) as error:
        logger.error("Erro ao processar Open-Meteo para %s: %s", city_name, error)
        return False


def main(
    start_date: date | None = None,
    end_date: date | None = None,
    incremental: bool = False,
) -> None:
    logger.info("=" * 50)
    logger.info("METEOSYNC | OPEN-METEO BRONZE INGESTION")
    logger.info("=" * 50)

    run_at = datetime.now(timezone.utc)
    range_start, range_end = resolve_date_range(start_date, end_date, incremental, run_at)
    s3_wrapper = S3ClientWrapper()
    successful, failed = 0, 0

    with get_resilient_session() as session:
        for city in CITIES:
            if ingest_city(
                city,
                s3_wrapper,
                None if incremental else range_start,
                None if incremental else range_end,
                incremental=incremental,
                run_at=run_at,
                session=session,
            ):
                successful += 1
            else:
                failed += 1

    logger.info("Resumo Open-Meteo -> Sucessos: %d | Falhas: %d", successful, failed)
    if failed > 0:
        raise RuntimeError(f"Ingestão finalizada com {failed} falha(s).")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Ingestão Bronze de dados da Open-Meteo.")
    parser.add_argument("--start-date", type=date.fromisoformat)
    parser.add_argument("--end-date", type=date.fromisoformat)
    parser.add_argument(
        "--incremental",
        action="store_true",
        help="Coleta a última hora disponível em UTC.",
    )
    arguments = parser.parse_args()
    main(arguments.start_date, arguments.end_date, arguments.incremental)