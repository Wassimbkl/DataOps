from datetime import timedelta
import pendulum

from airflow import DAG
from airflow.operators.bash import BashOperator

with DAG(
    dag_id="sales_pipeline_dag",
    start_date=pendulum.datetime(2025, 1, 1, tz="Europe/Paris"),
    schedule="@daily",
    catchup=False,
    default_args={
        "retries": 2,
        "retry_delay": timedelta(minutes=5),
    },
    tags=["dataops"],
) as dag:

    run_pipeline = BashOperator(
        task_id="run_pipeline_docker",
        bash_command="""
        docker run --rm \
          mon_projet_dataops:latest
        """,
    )
