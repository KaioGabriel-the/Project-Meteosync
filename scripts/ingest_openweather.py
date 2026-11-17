import os
import time
from datetime import datetime, timezone
from pathlib import Path

import requests
from botocore.exceptions import BotoCoreError, ClientError
from dotenv import load_dotenv
from requests.adapters import HTTPAdapter
from urllib3.util import Retry

from config.cities import CITIES
from scripts.utils.logging_config import configure_ingestion_logger
from scripts.utils.s3_client import S3ClientWrapper

PROJECT_ROOT = Path(__file__).resolve().parents[1]
load_dotenv(PROJECT_ROOT / ".env")

BASE_URL = "https://api.openweathermap.org/data/2.5/weather"

logger = configure_ingestion_logger(
    __name__,
    PROJECT_ROOT / "logs" / "ingest_openweather.log",
)


def get_resilient_session() -> requests.Session:
    session = requests.Session()
    retries = Retry(
        total=3,
        backoff_factor=1,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=frozenset({"GET"}),
        respect_retry_after_header=True,
        raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retries)
    session.mount("https://", adapter)
    return session


def fetch_openweather(
    city: dict,
    api_key: str | None = None,
    session: requests.Session | None = None,
) -> tuple[dict, dict]:
    api_key = api_key or os.getenv("OPENWEATHER_API_KEY")
    if not api_key:
        raise ValueError("OPENWEATHER_API_KEY não encontrada nas variáveis de ambiente.")

    params = {
        "lat": city["latitude"],
        "lon": city["longitude"],
        "appid": api_key,
        "units": "standard",
    }

    session = session or get_resilient_session()
    request_timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    start_time = time.monotonic()
    response = session.get(BASE_URL, params=params, timeout=30)
    latency_ms = int((time.monotonic() - start_time) * 1000)

    if response.status_code == 429:
        logger.error("Rate Limit excedido (HTTP 429) na OpenWeather para a cidade %s", city["city"])
    elif response.status_code == 401:
        logger.error("Chave de API inválida (HTTP 401) configurada para OpenWeather.")
    response.raise_for_status()
    payload = response.json()
    if not isinstance(payload, dict):
        raise ValueError("A API OpenWeather retornou um payload JSON inválido.")

    telemetry = {
        "request_timestamp": request_timestamp,
        "status_code": response.status_code,
        "latency_ms": latency_ms,
        "payload_size_bytes": len(response.content),
    }
    return payload, telemetry


def ingest_city(
    city: dict,
    s3_wrapper: S3ClientWrapper,
    run_at: datetime | None = None,
    session: requests.Session | None = None,
) -> bool:
    city_name = city["city"]
    try:
        ingestion_time = run_at or datetime.now(timezone.utc)
        if ingestion_time.tzinfo is None:
            raise ValueError("run_at precisa incluir fuso horário.")
        ingestion_time = ingestion_time.astimezone(timezone.utc)
        logger.info("Iniciando coleta OpenWeather: %s", city_name)
        data, telemetry = fetch_openweather(city, session=session)

        enrichment_payload = {
            "telemetry": telemetry,
            "metadata": {"city_id": city["city_id"], "city_name": city_name},
            "data": data,
        }

        s3_key = (
            f"openweather/year={ingestion_time.year}/month={ingestion_time.month:02d}/"
            f"day={ingestion_time.day:02d}/openweather_{city['city_id']}_"
            f"{ingestion_time.strftime('%Y%m%dT%H%M%SZ')}.json"
        )

        destination = s3_wrapper.upload_json(enrichment_payload, s3_key)
        logger.info(
            "Coleta OpenWeather concluída para %s | Latência: %dms | Destino: %s",
            city_name,
            telemetry["latency_ms"],
            destination,
        )
        return True

    except requests.RequestException as error:
        status_code = getattr(error.response, "status_code", None)
        logger.error(
            "Falha na requisição OpenWeather para %s (%s; HTTP %s).",
            city_name,
            type(error).__name__,
            status_code,
        )
        return False
    except (BotoCoreError, ClientError, ValueError, KeyError, OSError) as error:
        logger.error("Erro ao processar OpenWeather para %s: %s", city_name, error)
        return False


def main() -> None:
    logger.info("=" * 50)
    logger.info("METEOSYNC | OPENWEATHER BRONZE INGESTION")
    logger.info("=" * 50)

    run_at = datetime.now(timezone.utc)
    s3_wrapper = S3ClientWrapper()
    successful, failed = 0, 0

    with get_resilient_session() as session:
        for city in CITIES:
            if ingest_city(city, s3_wrapper, run_at, session):
                successful += 1
            else:
                failed += 1

    logger.info("Resumo OpenWeather -> Sucessos: %d | Falhas: %d", successful, failed)
    if failed > 0:
        raise RuntimeError(f"Ingestão finalizada com {failed} falha(s).")


if __name__ == "__main__":
    main()