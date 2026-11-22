from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'data-engineering',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'generator_dag',
    default_args=default_args,
    description='Periodic Dota 2 data generation',
    schedule_interval='@every 4h',
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=['generator', 'base'],
) as dag:

    generate_data = BashOperator(
        task_id='generate_data',
        bash_command='docker run --rm --network dota-lakehouse_default dota-generator:latest --matches 10000 --seed 42',
    )

    generate_data