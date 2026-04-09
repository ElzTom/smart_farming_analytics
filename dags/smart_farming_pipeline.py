from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

PROJ = "/opt/airflow/project"
PYTHON = "python"

default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="smart_farming_pipeline",
    description="End-to-end soil sensor pipeline: ingest → silver → gold → PostgreSQL",
    schedule="0 6 * * *",        # Daily at 6am
    start_date=datetime(2026, 1, 1),
    catchup=False,
    default_args=default_args,
    tags=["smart_farming", "soil", "weather"],
) as dag:

    ingest_soil = BashOperator(
        task_id="ingest_soil_parquet",
        bash_command=f"cd {PROJ} && {PYTHON} -m etl.ingest_soil_readings_parquet",
    )

    ingest_weather_rainfall = BashOperator(
        task_id="ingest_weather_rainfall",
        bash_command=f"cd {PROJ} && {PYTHON} -m etl.ingest_weather_rainfall_csv",
    )

    ingest_weather_temperature = BashOperator(
        task_id="ingest_weather_temperature",
        bash_command=f"cd {PROJ} && {PYTHON} -m etl.ingest_weather_temperature_csv",
    )

    ingest_weather_solar = BashOperator(
        task_id="ingest_weather_solar",
        bash_command=f"cd {PROJ} && {PYTHON} -m etl.ingest_weather_solar",
    )

    silver_soil = BashOperator(
        task_id="transform_silver_soil",
        bash_command=f"cd {PROJ} && {PYTHON} -m etl.transform_silver_soil",
    )

    silver_weather = BashOperator(
        task_id="transform_silver_weather",
        bash_command=f"cd {PROJ} && {PYTHON} -m etl.transform_silver_weather",
    )

    gold_daily = BashOperator(
        task_id="transform_gold_daily",
        bash_command=f"cd {PROJ} && {PYTHON} -m etl.transform_gold_daily",
    )

    gold_hourly = BashOperator(
        task_id="transform_gold_hourly",
        bash_command=f"cd {PROJ} && {PYTHON} -m etl.transform_gold_hourly",
    )

    load_postgres = BashOperator(
        task_id="load_gold_to_postgres",
        bash_command=f"cd {PROJ} && {PYTHON} -m etl.load_gold_to_postgres",
    )

    # ----------------------------------------------------------------
    # Pipeline dependencies
    # ----------------------------------------------------------------

    # Silver waits for its respective ingest
    ingest_soil >> silver_soil
    [ingest_weather_rainfall, ingest_weather_temperature, ingest_weather_solar] >> silver_weather

    # Gold waits for both silver layers
    [silver_soil, silver_weather] >> gold_daily
    silver_soil >> gold_hourly

    # Load to Postgres after both gold tables are ready
    [gold_daily, gold_hourly] >> load_postgres
