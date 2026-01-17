from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import sys


from src.orchestration.main_pipeline_orchestration import run_healthcare_data_pipeline   # import your ETL function

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2026, 1, 10),
    "retries": 0
}

with DAG(
    dag_id="healthcare_etl_orc_python_operator17",
    default_args=default_args,
    schedule=None,
    catchup=False
    
) as dag:

    run_main_pipeline = PythonOperator(
        task_id="run_main_pipeline",
        python_callable=run_healthcare_data_pipeline
    )

    run_main_pipeline