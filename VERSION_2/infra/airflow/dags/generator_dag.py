import os
from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator

default_args = {
    "owner": "NorvegianForestCat",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="generator_dag",
    default_args=default_args,
    description="Periodic Dota 2 data generation",
    schedule="0 */4 * * *",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["generator", "base"],
) as dag:

    pg_user = os.getenv("PG_USER")
    pg_pass = os.getenv("PG_PASSWORD")
    mongo_user = os.getenv("MONGO_USER")
    mongo_pass = os.getenv("MONGO_PASSWORD")

    generate_data = DockerOperator(
        task_id="generate_data",
        image="ghcr.io/norvegianforestcat/data-gen-bootcamp2026:latest",
        command="--matches 10000 --seed 42",
        docker_url="unix://var/run/docker.sock",
        network_mode="dota-lakehouse_default",
        auto_remove="success",
        mount_tmp_dir=False,
        environment={
            "PG_HOST": "postgres",
            "PG_PORT": "5432",
            "PG_DB": "dota",
            "PG_USER": pg_user,
            "PG_PASSWORD": pg_pass,
            "MONGO_HOST": "mongo",
            "MONGO_PORT": "27017",
            "MONGO_DB": "dota_events",
            "MONGO_USER": mongo_user,
            "MONGO_PASSWORD": mongo_pass,
            "POSTGRES_URL": f"postgresql://{pg_user}:{pg_pass}@postgres:5432/dota",
            "MONGO_URL": f"mongodb://{mongo_user}:{mongo_pass}@mongo:27017/dota_events?authSource=admin",
        },
    )
