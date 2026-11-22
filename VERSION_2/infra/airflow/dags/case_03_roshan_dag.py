import sys
if "/opt/spark-apps" not in sys.path: sys.path.insert(0, "/opt/spark-apps")

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup
from jobs.extract_raw import main as extract_raw_main
from jobs.case_3.stage import main as stage_main
from jobs.case_3.mart import main as mart_main
from jobs.case_3.dq_checks import run_dq_checks
import os

default_args = {
    "owner": "NorvegianForestCat",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="case_03_roshan_pipeline",
    default_args=default_args,
    description="ETL pipeline for case 3: Correlation between Roshan kill time and match outcome",
    schedule="0 */2 * * *",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["case_3", "roshan", "etl", "dota2"],
) as dag:

    with TaskGroup("raw_layer", tooltip="Extraction of raw data from PostgreSQL and MongoDB") as raw_group:
        extract_raw_task = PythonOperator(
            task_id="extract_raw_data",
            python_callable=extract_raw_main,
            execution_timeout=timedelta(hours=1),
        )

    with TaskGroup("stage_layer", tooltip="Cleaning, deduplication and validation of data") as stage_group:
        stage_task = PythonOperator(
            task_id="stage_clean_data",
            python_callable=stage_main,
            execution_timeout=timedelta(hours=1),
        )

    with TaskGroup("mart_layer", tooltip="Building analytical views and aggregations") as mart_group:
        mart_task = PythonOperator(
            task_id="build_mart_roshan",
            python_callable=mart_main,
            execution_timeout=timedelta(hours=1),
        )

    with TaskGroup("quality_layer", tooltip="Data Quality checks") as quality_group:
        dq_checks_task = PythonOperator(
            task_id="run_dq_checks",
            python_callable=run_dq_checks,
            op_kwargs={
                "spark": None,
                "run_id": "{{ run_id }}",
            },
            execution_timeout=timedelta(minutes=30),
        )

    raw_group >> stage_group >> mart_group >> quality_group