from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.utils.task_group import TaskGroup

default_args = {
    "owner": "NorvegianForestCat",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

SPARK_COMMON = {
    "conn_id": "spark_default",
    "env_vars": {
        "PYTHONPATH": "/opt:/opt/spark-apps:/opt/spark-apps/common",
    },
    "verbose": True,
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
        extract_raw_task = SparkSubmitOperator(
            task_id="extract_raw_data",
            application="/opt/spark-apps/jobs/extract_raw.py",
            name="case03_extract_raw",
            execution_timeout=timedelta(hours=1),
            **SPARK_COMMON,
        )

    with TaskGroup("stage_layer", tooltip="Cleaning, deduplication and validation of data") as stage_group:
        stage_task = SparkSubmitOperator(
            task_id="stage_clean_data",
            application="/opt/spark-apps/jobs/case_3/stage.py",
            name="case03_stage_clean",
            execution_timeout=timedelta(hours=1),
            **SPARK_COMMON,
        )

    with TaskGroup("mart_layer", tooltip="Building analytical views and aggregations") as mart_group:
        mart_task = SparkSubmitOperator(
            task_id="build_mart_roshan",
            application="/opt/spark-apps/jobs/case_3/mart.py",
            name="case03_build_mart",
            execution_timeout=timedelta(hours=1),
            **SPARK_COMMON,
        )

    with TaskGroup("quality_layer", tooltip="Data Quality checks") as quality_group:
        dq_checks_task = SparkSubmitOperator(
            task_id="run_dq_checks",
            application="/opt/spark-apps/jobs/case_3/dq_checks.py",
            name="case03_dq_checks",
            application_args=["{{ run_id }}"],
            execution_timeout=timedelta(minutes=30),
            **SPARK_COMMON,
        )

    raw_group >> stage_group >> mart_group >> quality_group
