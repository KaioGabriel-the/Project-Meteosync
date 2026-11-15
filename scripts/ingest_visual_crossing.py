
import json
import logging
import os

from datetime import date
from pathlib import Path

import requests
from dotenv import load_dotenv

from config.cities import CITIES
from scripts.utils.date_ranges import previous_complete_date_range


# ==========================================
# CONFIGURAÇÕES
# ==========================================

BASE_URL = (
    "https://weather.visualcrossing.com/"
    "VisualCrossingWebServices/rest/services/timeline"
)

PROJECT_ROOT = Path(__file__).resolve().parents[1]

OUTPUT_DIR = (
    PROJECT_ROOT
    / "data"
    / "raw"
    / "visual_crossing"
)

# Carrega as variáveis de ambiente
load_dotenv(PROJECT_ROOT / ".env")

API_KEY = os.getenv("VISUAL_CROSSING_API_KEY")

# ==========================================
# LOGGING
# ==========================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)

logger = logging.getLogger(__name__)


# ==========================================
# UTILITÁRIOS
# ==========================================

# ==========================================
# INGESTÃO
# ==========================================

def fetch_weather(city: dict, start_date: date, end_date: date) -> dict:
    """
    Consulta os dados meteorológicos de uma cidade
    para os últimos 10 dias completos.
    """

    latitude = city["latitude"]
    longitude = city["longitude"]

    url = (
        f"{BASE_URL}/"
        f"{latitude},{longitude}/"
        f"{start_date.isoformat()}/"
        f"{end_date.isoformat()}"
    )

    params = {
        "unitGroup": "metric",
        "include": "hours",
        "contentType": "json",
        "timezone": city["timezone"],
    }

    # A chave é opcional para consultas gratuitas
    # que permitem acesso anônimo.
    if API_KEY:
        params["key"] = API_KEY

    response = requests.get(
        url,
        params=params,
        timeout=60,
    )

    response.raise_for_status()

    return response.json()


# ==========================================
# ARMAZENAMENTO BRONZE
# ==========================================

def save_raw_data(
    city: dict,
    data: dict,
    start_date: date,
    end_date: date,
) -> Path:
    """
    Salva a resposta original da API em formato JSON.
    """

    output_path = (
        OUTPUT_DIR
        / f"{start_date}_{end_date}"
    )

    output_path.mkdir(
        parents=True,
        exist_ok=True,
    )

    file_path = output_path / f"{city['city_id']}.json"

    with open(
        file_path,
        "w",
        encoding="utf-8",
    ) as file:

        json.dump(
            data,
            file,
            ensure_ascii=False,
            indent=4,
        )

    return file_path


# ==========================================
# PROCESSAMENTO POR CIDADE
# ==========================================

def ingest_city(city: dict) -> bool:
    """
    Executa a ingestão de uma cidade.
    """

    city_name = city["city"]

    try:
        start_date, end_date = previous_complete_date_range(
            city["timezone"],
        )

        logger.info(
            "Iniciando coleta: %s (%s), %s até %s",
            city_name,
            city["timezone"],
            start_date,
            end_date,
        )

        weather_data = fetch_weather(city, start_date, end_date)

        days = weather_data.get("days", [])

        if not days:
            raise ValueError(
                "A resposta da API não contém dados diários."
            )

        # Conta os registros horários de todos os dias.
        hourly_records = sum(
            len(day.get("hours", []))
            for day in days
        )

        if hourly_records == 0:
            raise ValueError(
                "A resposta da API não contém dados horários."
            )

        expected_hours = 10 * 24

        if len(days) != 10:
            logger.warning(
                "%s retornou %d dias; eram esperados 10.",
                city_name,
                len(days),
            )

        if hourly_records != expected_hours:
            logger.warning(
                "%s retornou %d registros horários; "
                "eram esperados %d.",
                city_name,
                hourly_records,
                expected_hours,
            )

        file_path = save_raw_data(
            city,
            weather_data,
            start_date,
            end_date,
        )

        logger.info(
            "Coleta concluída: %s | %d registros horários | %s",
            city_name,
            hourly_records,
            file_path,
        )

        return True

    except requests.RequestException as error:
        logger.error(
            "Erro na requisição para %s: %s",
            city_name,
            error,
        )

    except (ValueError, KeyError, OSError) as error:
        logger.error(
            "Erro ao processar %s: %s",
            city_name,
            error,
        )

    return False


# ==========================================
# EXECUÇÃO PRINCIPAL
# ==========================================

def main():

    logger.info("=" * 50)
    logger.info("METEOSYNC | VISUAL CROSSING INGESTION")
    logger.info("=" * 50)

    logger.info(
        "Período da coleta: últimos 10 dias completos no fuso de cada cidade",
    )

    logger.info(
        "Total de cidades: %d",
        len(CITIES),
    )

    logger.info(
        "Autenticação: %s",
        "API Key configurada" if API_KEY else "Acesso gratuito sem chave",
    )

    successful = 0
    failed = 0

    for city in CITIES:

        if ingest_city(city):
            successful += 1
        else:
            failed += 1

    logger.info("=" * 50)
    logger.info("RESUMO DA INGESTÃO")
    logger.info("Cidades processadas: %d", len(CITIES))
    logger.info("Sucessos: %d", successful)
    logger.info("Falhas: %d", failed)
    logger.info("=" * 50)

    if failed > 0:
        raise RuntimeError(
            f"Ingestão finalizada com {failed} falha(s)."
        )


if __name__ == "__main__":
    main()
