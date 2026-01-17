from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from src.orchestration.main_pipeline_airflow import Run_pipline

r=Run_pipline()

# Default DAG arguments
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2026, 1, 16),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

# Define DAG
with DAG(
    dag_id="healthcare_etl_pipeline_task1",
    default_args=default_args,
    description="ETL Pipeline for Healthcare Data - Bronze/Silver/Gold",
    schedule=None,   # manual trigger
    catchup=False,
    max_active_runs=1,
    tags=["healthcare", "etl", "spark"],
) as dag:

    # Task functions
    def bronze_task():
        r.runnging_data_extracting_step()

    def validation_task():
        r.runnging_data_validation_step()

    def silver_task():
        r.runnging_silver_transformations_step()

    def gold_task():
        r.runnging_silver_transformations_step()

    # PythonOperators
    t1_bronze = PythonOperator(
        task_id="bronze_extraction",
        python_callable=bronze_task
    )

    t2_validation = PythonOperator(
        task_id="bronze_validation",
        python_callable=validation_task
    )

    t3_silver = PythonOperator(
        task_id="silver_transformation",
        python_callable=silver_task
    )

    t4_gold = PythonOperator(
        task_id="gold_transformation",
        python_callable=gold_task
    )

    # Set task dependencies
    t1_bronze >> t2_validation >> t3_silver >> t4_gold
