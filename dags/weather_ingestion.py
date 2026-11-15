
from datetime import timedelta

import pendulum

from airflow import DAG
from airflow.operators.python import PythonOperator


# ==========================================
# CONFIGURAÇÕES
# ==========================================

DEFAULT_ARGS = {
    "owner": "meteosync",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}


# ==========================================
# FUNÇÕES DE INGESTÃO
# ==========================================

def run_open_meteo_ingestion():
    """
    Executa a ingestão da API Open-Meteo.
    """

    from scripts.ingest_open_meteo import main

    main()


def run_visual_crossing_ingestion():
    """
    Executa a ingestão da API Visual Crossing.
    """

    from scripts.ingest_visual_crossing import main

    main()


# ==========================================
# DEFINIÇÃO DA DAG
# ==========================================

with DAG(
    dag_id="meteosync_weather_ingestion",
    description="Ingestão de dados meteorológicos de múltiplas APIs",
    default_args=DEFAULT_ARGS,
    start_date=pendulum.datetime(
        2026, 1, 1,
        tz="America/Sao_Paulo",
    ),
    schedule_interval=None,
    catchup=False,
    max_active_runs=1,
    tags=[
        "meteosync",
        "weather",
        "ingestion",
        "bronze",
    ],
) as dag:

    # --------------------------------------
    # Tarefa 1: Open-Meteo
    # --------------------------------------

    ingest_open_meteo = PythonOperator(
        task_id="ingest_open_meteo",
        python_callable=run_open_meteo_ingestion,
    )

    # --------------------------------------
    # Tarefa 2: Visual Crossing
    # --------------------------------------

    ingest_visual_crossing = PythonOperator(
        task_id="ingest_visual_crossing",
        python_callable=run_visual_crossing_ingestion,
    )
